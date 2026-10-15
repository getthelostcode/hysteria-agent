# 桥接：Go 控制面（agent 原生通道） × Laravel 控制面（面板/数据源）

> 方案 1：**Go 控制面只做通道终结，数据与身份全部落在 Laravel 的 MySQL。**
> 面板继续做唯一的 UI / 审计 / 权限 / 主题；agent 继续用原生 Envelope 协议；两者之间由 Go 控制面
> 以「节点代理」身份调用 Laravel 的 **既有** Agent API（HMAC 签名 REST），因此 Laravel 侧几乎零改动、
> 无第二套 schema、无跨语言直接写库。

## 1. 拓扑与职责

```
        agent（本仓库）                    Go 控制面（本仓库 cmd/controlplane）              Laravel 控制面
   ┌────────────────────┐       ┌──────────────────────────────────────────┐      ┌──────────────────────┐
   │ hysteria-agent run │       │ /api/v1/agent/enroll   ← 透明代理          │      │ /api/v1/agent/enroll │
   │  · credentials 0400│──WS──▶│ /api/v1/agent/control  ← Envelope 终结     │──REST│ /api/agent/v1/*      │
   │  · 子进程守护      │  proto│  · 桥接器 internal/cpapi / internal/bridge │ HMAC │ (注册/心跳/配置/指令/ │
   │  · 遥测/日志       │       │  · admin/healthz/readyz/metrics            │      │  指标/日志/诊断/升级) │
   └────────────────────┘       └──────────────────────────────────────────┘      └──────────┬───────────┘
                                                                                        MySQL │ Redis │ Horizon
                                                                                              ▼
                                                                                    Filament 面板（唯一 UI）
```

职责边界：

| 能力 | 归属 |
|---|---|
| 节点身份（node_id / node_secret）、注册令牌 | **Laravel**（唯一权威） |
| Envelope WS、心跳/遥测/日志/指令的 agent 原生协议 | **Go 控制面** |
| 配置版本、发布/回滚、指令队列、指标时序、审计、权限 | **Laravel** |
| 面板实时刷新（浏览器） | **Laravel Reverb**（与 agent 通道互不相干） |

## 2. 身份权威：enroll 代理

Go 控制面不再自行派生 node_secret（现有 `master_key` 派生逻辑保留，仅供 `identity.authority: local` 的独立部署模式）：

```
agent ──POST /api/v1/agent/enroll {enroll_token,hostname,machine_id,...}──▶ Go 控制面
                                                                              │ 原样转发（同字段契约）
                                                                              ▼
                                                          Laravel POST /api/v1/agent/enroll
                                                          （校验 enroll_token → 生成 node_id+node_secret
                                                            → 写 nodes/node_credentials/audit_logs）
                                                                              │ 200 {node_id,node_secret,...}
                                                                              ▼
Go 控制面：把 node_secret 存进 state.json（0600，per-node），把响应原样回给 agent
           并补 ws_path=/api/v1/agent/control
agent：写入 /etc/hysteria-agent/credentials.json（0400），完成
```

* 409 `node_exists` / `replace:true`、401 令牌语义：Go 控制面**直接透传** Laravel 的状态码与 body，agent 侧无需改动。
* 令牌一次性、并发注册：Go 控制面只做转发，不做本地令牌记账，避免双份真相。
* `node_secret` 在 Go 控制面留一份是必要的（它要替该节点签 REST 请求）；文件权限 0600，且与 Laravel 侧的 `secret_hash` 校验一致（同值）。

## 3. Go 控制面 → Laravel：接口映射（HMAC 签名，等同 agent）

签名规范与 Laravel `App\Support\SignatureVerifier` 一致（**务必逐字节对齐**）：

```
X-Node-Id: <node_id>      X-Timestamp: <unix>      X-Nonce: <uuid v4>
X-Node-Signature = HMAC-SHA256(secret, METHOD + "\n" + PATH + "\n" + sha256_hex(rawBody) + "\n" + X-Timestamp + "\n" + X-Nonce)
```

* `PATH` **不含查询串**；gzip 体签的是压缩后字节；
* 时间偏移 ≤ `AGENT_SIGNATURE_TTL`（300s）；nonce 每个请求重新生成（重放会 409）。

S4 已实现的部分（`internal/cp/bridge.go`）标 ✅：

| agent 侧（Envelope） | Go 控制面调用的 Laravel 接口 | 触发时机 | 说明 |
|---|---|---|---|
| `Hello` ✅ | `POST /api/agent/v1/heartbeat` | 每次 WS 建连 | 顺带取回最新配置版本与待执行指令数；**额外立刻转一次心跳**，面板无需等 30s 就能看到节点在线 |
| `HelloAck` 协商 | — | — | 间隔值来自 Laravel `settings.heartbeat_interval`（桥接器启动时拉一次，缓存 5 分钟） |
| `Heartbeat` ✅ | `POST /api/agent/v1/heartbeat` | 30s（可协商） | Laravel 侧写库按节点 15s 门控 |
| `ConfigPush` 需求 | `GET /api/agent/v1/config/latest?current_version=N` | 2s 轮询 + Reverb 事件立即触发 | Laravel 是配置发布方 |
| `ConfigAck` | `POST /api/agent/v1/config/ack` | agent 回执后 | 失败 → Laravel **自动回滚**并置 node=error |
| `MetricBatch` ✅ | `POST /api/agent/v1/metrics`（gzip） | 15s | 单批 ≤500，超出切分；`cpu/mem/disk` 压到 0..100（多核 CPU>100 会被 Laravel 422 掉整批），hysteria 明细进 `extra.hysteria` |
| `LogBatch` ✅ | `POST /api/agent/v1/logs` | 按 agent 采样批次 | agent 侧已做采样/截断；按 500 条切分，level 做归一（`warn→warning`、`fatal/panic→critical`），否则 Laravel 按 info 存 |
| `CommandRequest` | `POST /api/agent/v1/commands/poll` | 10s 兜底 + Reverb 立即触发 | 取到后转成 Envelope 下发 |
| `CommandResult` | `POST /api/agent/v1/commands/{command_id}/ack` | agent 回执后 | `OK/REJECTED→ok`，`FAILED/TIMEOUT→failed` |
| `CommandChunk`（tail_logs 流） | 缓存后随 `command/ack` 的 `result.chunks[]` 一并回传 | tail 结束 | 面板查看流式日志 |
| 诊断报告 | `POST /api/agent/v1/diagnostics` | `RUN_DIAGNOSTIC` 完成后 | 进 `node_logs(level=diagnostic)` |
| 自升级 | `GET /api/agent/v1/upgrade/check?arch=&version=` | 12h 或面板指令 | 命中后下发 `COMMAND_TYPE_UPGRADE_AGENT{url,sha256,signature}`；agent 自己验签 |

Laravel 侧**不需要新增任何 agent 接口**。

## 4. 指令类型映射表

Laravel `node_commands.type`（下拉框来自 `NodeCommand::TYPES`）↔ agent `CommandType`：

| Laravel type | agent CommandType | 方向 | 备注 |
|---|---|---|---|
| `reload_config` | `RELOAD_CONFIG(1)` | ⇄ | 支持 `payload.revision` |
| `restart_hysteria` | `RESTART_HYSTERIA(2)` | ⇄ | |
| `upgrade` | `UPGRADE_AGENT(3)` | ⇄ | payload 带 url/sha256/signature |
| `collect_diagnostics` | `RUN_DIAGNOSTIC(4)` | ⇄ | |
| `tail_logs` ★ | `TAIL_LOGS(5)` | ⇄ | 最长 5 分钟；结果按 chunk 汇总回传 |
| `kick_user` | `KICK_USERS(6)` | ⇄ | payload.user_ids |
| `get_state` ★ | `GET_STATE(7)` | ⇄ | agent+hysteria 快照 |
| `cancel_tail` ★ | `CANCEL_TAIL(8)` | ⇄ | payload.command_id |
| `rollback_config` ★ | `ROLLBACK_CONFIG(9)` | ⇄ | Go 控制面同时把 Laravel 侧配置回滚（`POST /nodes/{id}/configs/{v}/rollback`，需 CP 的 admin token） |
| `self_restart` ★ | `SELF_RESTART(10)` | ⇄ | systemd 接管重启 |
| `clear_outbox` ★ | `CLEAR_OUTBOX(11)` | ⇄ | 危险：仅 super-admin，需 `payload.confirm=true` |
| `exec` | — | ✗ | **不转发**：agent 白名单拒收任意 shell |
| `traffic_reset` | — | ✗ | agent 暂无对应指令（后续可加 hyapi 清零） |
| `rotate_secret` | — | 仅 Laravel | 由 Go 控制面监听 Laravel 侧密钥轮换事件，更新 state.json 并触发 agent 重连 |

★ = 需要在 Laravel `NodeCommand::TYPES` 里新增的 6 个类型（面板下拉自动出现）。

## 5. Laravel 侧改动清单（约 60 行 + 测试）

1. **`NodeCommand::TYPES` 增加 6 个类型**（`tail_logs / get_state / cancel_tail / rollback_config / self_restart / clear_outbox`）及中文标签；
2. **危险类型限制**：`NodeCommandPolicy::issue` 对 `clear_outbox`、`exec` 只放行 super-admin/admin（Filament action 与 admin API 校验共用）；
3. **通道标记（可选）**：桥接器在 enroll 时带 `protocol: "bridge/v1"`（或 `X-Channel: go-cp`），enroll 兼容面已把 `protocol` 写进 `nodes.meta.protocol` → 面板可加一列「通道」区分 Go-CP 节点与直连节点；
4. **文档**：`docs/API.md` 增加「桥接消费方」一节（即本文档的摘要与链接）。

不需要：新表、新 agent 接口、Redis 新键。

## 6. 时序图

### 6.1 注册

```
agent                    Go CP                        Laravel
  │ POST enroll            │                              │
  ├───────────────────────▶│ POST /api/v1/agent/enroll     │
  │                        ├─────────────────────────────▶│ 校验 token / 建节点 / 审计
  │                        │◀──── 200 {node_id,secret} ───┤
  │                        │ 存 state.json(0600)           │
  │◀ 200 +ws_path ─────────┤                              │
  │ 写 credentials.json 0400│                              │
  │ ══ WS Hello ══════════▶│ POST /heartbeat（HMAC）       │
  │◀═ HelloAck(30s/15s) ═══┤◀─ 200 ───────────────────────┤
```

### 6.2 配置发布 → 热重载 →（失败）自动回滚

```
面板发布 v3 → Laravel 写 node_configs + audit
      │ Reverb private-fleet: config.published
      ▼
Go CP（订阅 fleet，或 2s 轮询 config/latest）
      │ Envelope ConfigPush{v3, sha256, yaml}
      ▼
agent：schema 校验 → 端口/证书检查 → 原子写 0600 → 热重载 → 保留 5 版历史
      │ Envelope ConfigAck{ok|failed, error}
      ▼
Go CP → POST /api/agent/v1/config/ack
      ├─ ok      → Laravel: config.status=applied
      └─ failed  → Laravel: 自动 rollback(生成 v4=旧内容) + node.status=error + 审计
```

### 6.3 指令往返（目标 <2s）

```
面板「重启 Hysteria」→ Laravel node_commands(pending) + audit + Reverb command.issued
      ▼
Go CP：Reverb 事件（<100ms）或 10s 兜底轮询 commands/poll
      │ Envelope CommandRequest{RESTART_HYSTERIA}
      ▼
agent：白名单校验 → supervisor.Restart → 记录 exit code
      │ Envelope CommandResult{OK/Failed}
      ▼
Go CP → POST /commands/{id}/ack → Laravel: acked + audit（面板 5s 内可见）
```

## 7. 降级与错误语义

| 场景 | 行为 |
|---|---|
| Laravel 不可达 | agent 的 WS 不受影响；遥测/日志/回执进入 Go CP 自身 outbox（`events/<node>.jsonl`），指数退避补发（1s→300s）；配置与指令暂停并在恢复后补做 |
| Go CP 重启 | 从 `state.json` 恢复节点表；agent 按退避重连；未送达的指令在 Laravel 侧仍是 `pending`，重新 poll 即拿到 |
| 重启期间漏掉的指令 | Laravel `commands/poll` 会把仍未 ack 的指令再给一次（`sent` 状态也会重投），Go CP 按 command_id 去重 |
| 面板删除节点 | Go CP 下次 heartbeat 收到 404 `unknown_node` → 关闭该 agent 的 WS 并删除本地凭据 |
| 密钥轮换 | Laravel 轮换后旧 secret 失效 → Go CP 收 401 → 重读 Laravel 侧新密钥需走重注册（面板 →「轮换密钥」+ agent 重新 enroll），文档中显式说明 |
| Reverb 不可用 | 纯轮询模式仍可用（延迟由 <100ms 变为 ≤10s），不影响正确性 |

## 8. 规模与限流预算（5000 节点）

每节点每秒的 Laravel 请求数：

| 来源 | 频率 | req/s |
|---|---|---|
| heartbeat | 30s | 0.033 |
| metrics | 15s | 0.067 |
| config 轮询 | 2s | 0.5（**仅当未订阅 Reverb 时**） |
| commands 轮询 | 10s | 0.1 |
| 合计（订阅 Reverb，配置事件驱动） | — | **≈0.2 req/s × 5000 ≈ 1000 req/s** |
| 合计（纯轮询） | — | ≈0.7 → 3500 req/s（需横向扩 Web 层；不推荐） |

结论：**默认应订阅 Reverb**（Go CP 用 operator 创建的 Sanctum token 订阅 `private-fleet`），轮询只做兜底。
Laravel 侧已有：每节点 1200 req/min 限流、心跳写库 15s 门控、指标批量入库、聚合走队列。

## 9. Go 控制面新增配置（`deploy/controlplane.yaml`）

```yaml
identity:
  authority: "laravel"        # laravel | local（local = 旧的 master_key 派生独立模式）
laravel:
  base_url: "https://cp.example.com"   # Laravel 控制面
  admin_token: ""                      # 运维/面板 token：订阅 fleet、触发配置回滚
  enroll_path: "/api/v1/agent/enroll"  # 与 Laravel 兼容面同一契约
  request_timeout_seconds: 15
  retry: { initial_seconds: 1, max_seconds: 300, multiplier: 2, jitter: 0.2 }
poll:
  config_seconds: 2           # 仅在未订阅 Reverb 时生效
  commands_seconds: 10
  fallback_only: true
reverb:
  enabled: true
  key: ""                     # REVERB_APP_KEY
  host: "cp.example.com"
  port: 443
  scheme: "https"
bridge:
  gzip_metrics: true
  max_metric_batch: 500
  outbox_dir: "/var/lib/hysteria-controlplane/outbox"
```

## 10. 实施步骤与验收

| 步骤 | 内容 | 验收 | 状态 |
|---|---|---|
| S1 | Laravel：6 个新指令类型 + 危险类型限权 + 测试 | `php artisan test` 全绿（新增 ≥3 用例） |✅ 完成（107 用例/491 断言，8 个新用例）
| S2 | Go：`internal/cpapi`（签名 + 客户端）+ 单测 | 单测用 httptest 校验签名原文/重放/超时；与 Laravel e2e 脚本对拍 |✅ 完成（11 单测含跨语言黄金向量；`cmd/cpapi-smoke` 对真实控制面 10/10）
| S3 | Go：enroll 代理 + state.json 凭据存储 | e2e：agent 注册 → Laravel `nodes` 出现该节点（同一 node_id/secret） |✅ 完成（真机验证：同一 node_id；secret 两边互认；409 透传；state.json 0600）
| S4 | Go：心跳/遥测/日志转发 | 面板 5s 内看到指标曲线与日志 |
| S5 | Go：配置下发 + 回执（含失败回滚） | 面板发布 → agent 热重载 → 面板 `applied`；注入坏配置 → 面板自动 rollback |
| S6 | Go：指令双向 + Reverb 订阅 + 轮询兜底 | 面板点「重启」→ 2s 内 ack；断掉 Reverb 仍在 ≤10s 内送达 |
| S7 | Go：降级 outbox 补发 | 停 Laravel → 产生遥测/回执 → 恢复后全部落到面板，无丢无重 |
| S8 | 文档 + 部署（systemd 两份 unit、cloudflared 路径路由） | `install-controlplane.sh` 一键后 agent 可直接 enroll |

## 11. 桥接的消息处理模型（为什么不会拖慢 Agent 路径）

每次转发都是一个 `bridgeJob`，进 per-node 的**有界队列**（256）后由**单个后台 worker 顺序执行**；
Agent 的消息处理路径只做 `select { case ch <- job: default: }`，**永不阻塞**：

* 队列满 → 丢弃 + `cp_bridge_failures_total{result="dropped"}`（S7 换成落盘 outbox 补发）；
* 上游 5xx/超时 → 失败计数 + WARN 日志，worker 继续处理后续消息（重试策略在 `internal/cpapi`）；
* 非桥接模式（`identity.authority: local`）→ 连转发器都不建，**零出站请求**（有单测守护）。

观测：`GET /metrics` 的 `cp_bridge_total{kind=heartbeat|metrics|logs|diagnostics}` 与
`cp_bridge_failures_total{result=failed|dropped}`。面板 5 秒内出现曲线/日志 = 验收通过。

## 12. 真机验收（S4）

```bash
# 先用 hysteria-control-plane 起面板（8001/8088 均可）
PANEL_URL=http://127.0.0.1:8001 \
PANEL_DB=/root/hysteria-control-plane/database/database.sqlite \   # 可选：额外校验 node_logs 入库
bash tests/bridge_e2e.sh
```

`tests/bridge_e2e.sh` 起真实 agent + 真实 Go 控制面（laravel 模式）+ 真实 Laravel 面板，
断言 14 项：注册 → `credentials.json` 0400 → WS 握手完成 → 面板可见且 `online` →
`meta.protocol=bridge/v1` → 指标曲线有样本 → `cp_bridge_total` 三类计数增长 → 日志入库。

**踩坑（前两条都会让面板"看起来没数据"）**：

1. **通道 JWT 的 issuer/audience 必须由控制面（校验方）决定**，不能透传 Laravel 注册响应里的值。
   Laravel 兼容面曾返回 `jwt_audience=hysteria-agent`，而控制面按 `creds.DefaultAudience`
   (`hysteria-control`) 校验 → WS 握手 401，agent 侧只能看到 "bad handshake"。
   现在 enroll 代理一律回填 `creds.DefaultIssuer/DefaultAudience`，Laravel 兼容面也用同一组值。
2. **401 分支必须打日志**。"缺头"与"audience 不匹配"两条分支原来只写响应不写日志，排查时 CP
   日志干干净净，只能抓包/手工 curl；现在两条都带 `node_id`/`got`/`want`。
3. `enroll --replace` 是**同一 node_id 轮换凭据**，面板侧保留原节点名 —— 脚本按
   `credentials.json` 的 `node_id` 查询，而不是按新名字搜。

## 13. 进度

- [x] **S1** Laravel：6 个 agent 原生指令类型 + 危险类型限权 + 通道列（`hysteria-control-plane@97b6df1`）
- [x] **S2** Go：`internal/cpapi` 签名客户端 + 11 单测（跨语言黄金向量）+ `cmd/cpapi-smoke`
- [x] **S3** Go：`identity.authority: laravel` —— enroll 代理、per-node secret 入库（state.json 0600）、
      通道鉴权改用下发密钥、启动探活、409/401 原样透传
- [x] **S4** Go：心跳/遥测/日志/诊断转发（`internal/cp/bridge.go` + 6 单测 + 真机 14 项断言全绿）
       · [ ] S5 配置下发+回执（含失败自动回滚） · [ ] S6 指令双向 + Reverb 事件驱动
- [ ] S7 Laravel 不可达时的 outbox 补发 · [ ] S8 部署（cloudflared 路径路由 + 两份 unit）

约定：**路径与字段以 Laravel 侧为准**（它是契约发布方）；Go 侧只做映射，任何字段变动先改 Laravel 的 `docs/API.md` 与 OpenAPI（`/docs/api.json`），再同步 Go。
