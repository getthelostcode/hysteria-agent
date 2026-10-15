# hysteria-agent 设计文档

> 版本：0.1.0 ｜ 协议版本：`PROTOCOL_VERSION_V1` ｜ 传输：WSS + Protobuf

本文是接口契约与实现依据。改协议先改这里，再改 `proto/agent.proto` 与两个实现（Agent / `cmd/mockcontrol`）。

---

## 1. 定位与总体架构

`hysteria-agent` 是跑在**节点机**上的常驻守护进程，负责：

1. **身份**：用一次性 `enroll_token` 换长期身份（`node_id` + `node_secret`），之后凭它连控制面（对标 cloudflared 的 `cert.pem` 机制）；
2. **通道**：与控制面维持一条带心跳、可重连、断线不丢事件的 WSS 长连接；
3. **配置**：接收控制面下发的 Hysteria2 配置，校验 → 原子落盘 → 热更新 → 失败自动回滚（保留最近 5 个版本）；
4. **进程**：接管 Hysteria2 进程/服务的生命周期，崩溃自动重启、记录退出码与日志；
5. **遥测**：15s 上报主机与 Hysteria 指标，日志按采样率远程采集；
6. **指令**：执行白名单内的远程指令（重载/重启/自升级/诊断/日志流/踢人）。

```
                        ┌──────────────────────── 控制面 (任选语言) ────────────────────────┐
                        │  enroll API   │  Control WebSocket  │  配置存储 / 指标 / 审计  │
                        └───┬───────────┴──────────┬──────────┴──────────────┬───────────┘
                            │ HTTPS/JSON           │ WSS + Protobuf          │
                            │ (一次性)              │ 心跳30s/超时90s          │
┌───────────────────────────┴──────────────────────┴─────────────────────────┴───────────┐
│ 节点机 / hysteria-agent (systemd: hysteria-agent.service)                              │
│                                                                                        │
│  ┌────────────┐  ┌───────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐   │
│  │ creds      │  │ control client│  │ hyconfig     │  │ supervisor│  │ telemetry    │   │
│  │ 0400 凭据  │→ │ 退避重连/心跳 │→ │ 校验/版本管理│→ │ 子进程守护│  │ /proc + API  │   │
│  └────────────┘  │ outbox 补发   │  │ 原子写/回滚  │  │ 崩溃重启  │  └──────────────┘   │
│                  └───────┬───────┘  └──────┬───────┘  └─────┬─────┘  ┌──────────────┐   │
│  ┌────────────┐  ┌───────┴───────┐  ┌──────┴───────┐  ┌─────┴─────┐  │ logs 采样/   │   │
│  │ upgrader   │  │ commands      │  │ metrics 9100 │  │ diag      │  │ tail 流      │   │
│  │ ed25519 验签│  │ 白名单分发    │  │ /healthz     │  │ 端口/证书 │  └──────────────┘   │
│  └────────────┘  └───────────────┘  └──────────────┘  │ DNS/出口IP│                     │
│                                                        └───────────┘                     │
│                     │ SIGTERM/SIGKILL │ /traffic /online /kick │ stdout/stderr          │
│             ┌───────┴─────────────────┴───────────────────────┴────────┐                 │
│             │ Hysteria2 服务端（process / systemd / external 三种模式） │                 │
│             └────────────────────────────────────────────────────────┘                 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 目录与权限（mandatory）

| 路径 | 权限 | 说明 |
|---|---|---|
| `/etc/hysteria-agent/config.yaml` | 0600 | Agent 自身配置 |
| `/etc/hysteria-agent/credentials.json` | **0400** | `node_id` / `node_secret`（长期身份密钥） |
| `/etc/hysteria-agent/history/` | 0700 | 配置历史（最近 5 个版本 + index.json） |
| `/etc/hysteria/config.yaml` | **0600** | Hysteria 实际读取的配置，由 Agent 原子写入 |
| `/var/lib/hysteria-agent/` | 0750 | `outbox.jsonl`、`state.json`、`upgrade/` |
| `/run/hysteria-agent/` | 0755 | 运行时文件 |
| `/usr/local/bin/hysteria-agent` | 0755 | 自身二进制（自升级时原子替换，保留备份） |

---

## 2. 控制通道协议

### 2.1 为什么是 WebSocket + Protobuf，而不是 gRPC bidi

* 控制面是多语言环境（Go / Python-FastAPI / Laravel），gRPC 需要在每种语言里生成 stub 并维护 HTTP/2 栈；WebSocket + protobuf 任何语言都能实现（`tests/dumpq.py` + `cmd/mockcontrol` 就是两个独立实现）。
* 节点与控制面之间常经过 **Cloudflare Tunnel**：`cloudflared` 默认 `http` 协议下 gRPC 需要额外 `--protocol http2`，且对 100s 空闲超时敏感；WebSocket 在隧道里开箱即用，配合 30s 心跳不会被判空闲。
* 需求允许多路复用的逻辑流：用 `Envelope.type` + `seq` + `command_id` 在单条 TCP 上实现心跳流、事件流、指令流与日志流。

### 2.2 分帧

一条 WebSocket **二进制帧** = 一个 `Envelope` 的 protobuf 序列化结果，不加额外长度前缀（WebSocket 自带消息边界）。
`Envelope.type` 与 `oneof body` 冗余声明同一件事，方便没有 oneof 惯用法的语言先 `switch type` 再解析。

### 2.3 消息表

| 消息 | 方向 | `seq` | 说明 |
|---|---|---|---|
| `Hello` | Agent→CP | 0 | 握手：版本、主机信息、`resume_from_seq`、`last_acked_seq`、能力白名单、当前配置哈希 |
| `HelloAck` | CP→Agent | 0 | 心跳/遥测间隔、`resend_from_seq`（要求从哪条开始补发）、期望配置版本 |
| `Heartbeat` | Agent→CP | 0 | 每 30s：运行时长、待发事件数、Hysteria 是否在跑、是否 ready、当前配置版本 |
| `HeartbeatAck` | CP→Agent | 0 | 服务端时间（用于时钟偏差检测）、累积确认 `acked_seq` |
| `ConfigPush` | CP→Agent | 0 | `revision` / `format` / `payload` / `sha256` / 可选 `signature` / `dry_run` / `force` |
| `ConfigAck` | Agent→CP | >0 | `ok`、`error`、`checks[]`、`warnings[]`、`rolled_back`、耗时 |
| `CommandRequest` | CP→Agent | 0 | `command_id`（幂等键）、`type`（白名单枚举）、`params_json`、`trace_id` |
| `CommandResult` | Agent→CP | >0 | 状态（ok/failed/rejected/timeout）、`result_json`、起止时间 |
| `CommandChunk` | Agent→CP | >0 | 流式指令分片（`tail_logs`），`index` 递增、最后一片 `eof=true` |
| `LogBatch` | Agent→CP | >0 | 采样后的日志行 |
| `MetricBatch` | Agent→CP | >0 | 主机指标 + Hysteria 指标 + 配置版本 |
| `NodeEvent` | Agent→CP | >0 | 语义事件：子进程起停/崩溃（含退出码与日志尾巴）、配置应用/回滚、升级、放弃重启 |
| `EventAck` | CP→Agent | 0 | **累积确认**：`seq <= acked_seq` 的事件可以本地丢弃 |
| `ErrorMsg` | 双向 | 0 | 协议级错误提示 |
| `UpgradeOffer` | CP→Agent | 0 | 升级提议（`url` + `sha256` + ed25519 签名） |

### 2.4 可靠投递语义（at-least-once）

* `seq > 0` 的消息**先写本地 outbox**（`/var/lib/hysteria-agent/outbox.jsonl`，JSONL + fsync）再发送；控制面 `EventAck` 后从 outbox 删除；
* outbox 里有确认水位线旁路文件（`outbox.jsonl.ack`），重启后 `Hello.last_acked_seq` 不会退回 0；
* 重连时 `Hello.resume_from_seq` = 本地最小未确认 seq；控制面可回 `HelloAck.resend_from_seq` 覆盖起点；
* 因此**语义是 at-least-once**：控制面必须按 `(node_id, seq)` 幂等入库；重复是允许的，丢事件不允许；
* outbox 超限（默认 32MB / 20000 条）时按 `log → metric → 其它` 的顺序丢最旧的，并计入 `hysteria_agent_events_dropped_total` + 上报 `outbox_dropped` 事件。

### 2.5 心跳与断线判定

* Agent 每 30s 发 `Heartbeat`（可用 `HelloAck` 覆盖为 3s 等）；
* Agent 侧：`ReadDeadline = 90s`，90s 内没收到**任何**服务端消息（含 Ping/Pong）即判定断线、关闭连接重连；
* 控制面侧：90s 没收到 `Heartbeat` 即可把节点标 offline（本 Agent 的 `/readyz` 也会因控制通道断开而告警，但不影响 Hysteria 继续服务，见 §7）；
* 重连退避：`1s → ×2 → 上限 300s`，带 ±20% 抖动；连接存活超过 30s 后退避复位。

---

## 3. 认证与安全模型

### 3.1 注册（一次性）

```
Agent                                             控制面
  │  POST {control_url}/api/v1/agent/enroll          │
  │  {enroll_token, hostname, machine_id,            │
  │   boot_id, os, arch, agent_version}              │
  │ ─────────────────────────────────────────────────▶
  │  200 {node_id, node_secret, pin_sha256[],        │
  │       ca_cert_pem?, client_cert_pem?,            │
  │       jwt_issuer?, jwt_audience?, ws_path?}      │
  │ ◀─────────────────────────────────────────────────
  │  写入 credentials.json (0400)，token 不再使用      │
```

* `enroll_token` 只用于注册，**不落盘**（日志里也只打印前 6 位）；
* `node_secret` 由控制面生成（≥16 字节随机，建议 32）；它是长期身份密钥，泄漏等于节点被冒充，因此本地 0400、控制面侧应支持一键吊销重签；
* 注册接口的错误语义：`401/403` = 令牌无效/已用/过期，`409` = 节点已存在（要重注册需控制面先删或在请求里带 `replace: true`）。

### 3.2 通道认证（每次连接）

优先级：

1. **mTLS**：若注册响应下发了 `client_cert_pem` + `client_key_pem`，Agent 用它做客户端证书认证（`creds.TLSConfig`）；
2. **JWT（HS256）**：用 `node_secret` 作为 HMAC 密钥**本地自签**，与 `Authorization: Bearer <jwt>` 头一起发送。

JWT 声明：

```json
{"iss":"hysteria-agent","sub":"<node_id>","aud":"hysteria-control",
 "iat":…,"nbf":…-30s,"exp":…,"jti":"<8字节随机>","ver":"v1"}
```

* 本地自签的理由：`node_secret` 双方共享，控制面本地即可验签，节点**断网重启后不需要额外往返**就能连上；
* 控制面只需要：查 `node_id` → 取 `node_secret` → 校验 `HS256` 签名 + `exp/nbf` + `aud`；`jti` 用于防重放（可选做 5 分钟去重）；
* `kid` 头里带 `node_id`，方便控制面定位密钥。

### 3.3 证书 pinning（防中间人 / 防隧道被劫持）

* `control.pin_sha256`（配置）与注册响应里的 `pin_sha256`（凭据）会被合并成允许集合；
* 校验方式是 **SPKI SHA-256 pin**：`base64(sha256(cert.RawSubjectPublicKeyInfo))`，链上任意一张证书匹配即通过（支持 pin 中间 CA）；
* pin 与标准证书链校验**同时生效**（除 `insecure_skip_verify: true`，该选项必须在有 pin 时才允许开启，否则启动即报错）。

### 3.4 升级验签

* ed25519；公钥来自编译期注入的 `version.UpgradePublicKey` 与配置 `upgrade.pubkeys`（可多把，支持轮换）；
* **没有可用公钥时 `upgrade.enabled` 必须为 false**，否则配置校验失败（禁止"静默不验签"）；
* 签名对象见 §5.3，验签失败一律拒绝且不改动原二进制。

### 3.5 指令白名单

Agent 只实现 `agent.proto` 里 `CommandType` 枚举的 11 个指令；未知枚举值直接 `REJECTED`。
所有需要操作系统能力的地方都用**固定二进制 + 固定参数**的 `exec`：

| 动作 | 实际执行 | 参数来源 |
|---|---|---|
| 重启 Agent | `systemctl --no-block restart hysteria-agent` | 配置里的单元名（不接受指令参数） |
| 管理 Hysteria | `systemctl {start,stop,restart,reload,is-active,status,show} <unit>` | 动词白名单 + 配置里的单元名 |
| Hysteria 日志 | `journalctl -u <unit> -f -n 0 --output=cat` | 配置里的单元名 |
| 升级冒烟测试 | `<新二进制> version` | 固定子命令 |

**不存在**任何把指令参数拼进 shell 的路径（没有 `sh -c`），这是本设计对"禁止执行任意 shell 命令"的落地方式。

---

## 4. 关键时序

### 4.1 启动与握手

```
Agent                              systemd               控制面
  │ ExecStart: hysteria-agent run    │                     │
  │ ├ 读 config.yaml (0600)          │                     │
  │ ├ 读 credentials.json (0400)     │                     │
  │ ├ 打开 outbox（恢复未投递事件）   │                     │
  │ ├ 启动 127.0.0.1:9100 (/metrics /healthz /readyz)      │
  │ ├ 读当前 /etc/hysteria/config.yaml 的 hash/revision     │
  │ ├ supervisor.Start → 拉起 hysteria 子进程               │
  │ │    └ 子进程 stdout/stderr → 日志 Hub（本地环 + 远程） │
  │ └ 循环：control.Run / telemetry / logs / state / events │
  │                                                         │
  │ WSS 连接（TLS + pinning + Bearer JWT）                  │
  │ ───────────────────────────────────────────────────────▶│
  │ Hello{resume_from_seq, last_acked_seq, capabilities…}   │
  │ ───────────────────────────────────────────────────────▶│
  │ HelloAck{intervals, resend_from_seq, config_revision}   │
  │ ◀───────────────────────────────────────────────────────│
  │ 补发 outbox 中 seq >= resend_from_seq 的事件            │
  │ ───────────────────────────────────────────────────────▶│
  │ 之后：每 30s Heartbeat，每 15s MetricBatch，日志按批推送 │
```

### 4.2 配置下发与热更新（含自动回滚）

```
Agent                                            控制面
  │ ConfigPush{revision, payload, sha256}            │
  │ ◀─────────────────────────────────────────────────│
  │ 0. sha256 与当前配置相同且非 force → 直接 ack(unchanged)
  │ 1. 校验（五层，见 §6.1）                          │
  │      ✗ → ConfigAck{ok:false, checks, error} + NodeEvent(config_failed)，不落盘
  │ 2. 备份当前配置进 history（回滚依据）             │
  │ 3. 新版本写 history/rev-NNNNNN-<revision>.yaml    │
  │ 4. 原子写 /etc/hysteria/config.yaml（0600）       │
  │ 5. 热更新：SIGTERM 优雅停止 → 新配置拉起 → 等就绪  │
  │      ✗（进程起不来/端口不通）                     │
  │         → 恢复上一版本 → 重启 → ConfigAck{ok:false,
  │           rolled_back:true} + NodeEvent(config_rolled_back)
  │      ✓ → ConfigAck{ok:true, checks}              │
  │           + NodeEvent(config_applied)             │
```

关于"平滑重载"的**事实说明**：Hysteria 2 只处理 `SIGINT/SIGTERM`（上游源码 `app/cmd/server.go` 没有 SIGHUP 处理），不支持配置热重载。
因此 Agent 的 `reload` = **优雅重启**：`SIGTERM` 让进程收尾（连接排空）→ 立刻以新配置拉起，断连窗口通常是进程启动时间（几十到几百毫秒），QUIC 客户端会自动重连。
`supervisor.Reload` 对 `systemd` 模式会先尝试 `systemctl reload`，unit 没定义 `ExecReload` 时回退为 `restart`。
不做"SIGHUP 假封装"，避免用户以为改配置不断线。

### 4.3 子进程崩溃重启

```
hysteria 进程崩溃 → reap 拿到 exit code（含信号 128+N 语义）
   ├ NodeEvent{type:exited, exit_code, log_tail[20]}
   ├ metrics: hysteria_restarts_total++, last_exit_code
   ├ 退避重启：1s → ×2 → 上限 60s，带抖动
   │    成功 → restarts++，退避复位，NodeEvent{started}
   └ 窗口限流：300s 内超过 10 次 → 放弃重启
        NodeEvent{restart_gave_up} + /readyz 变 not_ready（需人工介入）
```

### 4.4 断线缓冲与补发

```
T0 控制面宕机
T0..T1 Agent 照常工作；所有 seq>0 事件写入 outbox 并 fsync（日志/指标/回滚回执都不丢）
T1 控制面恢复
T1+ Agent 重连（退避 ≤300s）→ Hello{resume_from_seq = 本地最小未确认 seq}
   控制面 HelloAck{resend_from_seq = 自己已确认的最大 seq + 1}
   Agent 按序补发（每轮 ≤512 条），收到 EventAck 后删除已确认部分
```

### 4.5 自升级

```
控制面 → CommandRequest{type:upgrade_agent, params:{version,url,sha256,signature,size,force}}
Agent:
 1. 下载（带 pinning 的 TLS 客户端，≤120s，≤64MB）
 2. 校验 sha256；ed25519 验签（签名对象 = sha256 的 hex 字符串）
 3. 写入 upgrade/ 目录 + chmod 0755
 4. 冒烟测试：`<新二进制> version` 必须输出包含 hysteria-agent 且退出码 0
 5. 备份当前二进制 → 同目录 rename 原子替换 /usr/local/bin/hysteria-agent
 6. **重启前先优雅停掉被守护的 Hysteria**（exec 模式下 syscall.Exec 不会带走子进程，
    不停就会留下占着 443 的孤儿 → 新进程启动 Hysteria 直接端口冲突）
 7. 重启：systemd 模式 `systemctl --no-block restart`（避免自己杀掉自己的重启动作）；
    exec 模式 `syscall.Exec` 原地换镜像
 8. CommandResult 回执（含备份路径）+ NodeEvent{upgrade}
```

### 4.6 tail_logs 流式日志

```
CommandRequest{tail_logs, params:{duration_seconds≤300, backlog_lines, source, level}}
  ├ 先回放 Hub 环形缓冲最近 N 行（JSON Lines 分片）
  ├ 订阅实时日志（Agent 自身 + hysteria 子进程 stdout/stderr 都在 Hub 里）
  ├ systemd/external 模式下额外挂 `journalctl -u <unit> -f`（固定参数）
  ├ 每 100 行或 200ms 一批 → CommandChunk{index, data}
  └ 到时限/被 CANCEL_TAIL 取消/订阅关闭 → 最后一片 eof=true + CommandResult
```

---

## 5. 接口契约明细

### 5.1 控制面必须实现的 HTTP 接口

| 方法 | 路径（可配） | 请求 | 响应 |
|---|---|---|---|
| POST | `/api/v1/agent/enroll` | §3.1 的 JSON（也接受 `X-Enroll-Token` 头） | `{node_id, node_secret, pin_sha256?, ca_cert_pem?, client_cert_pem?, client_key_pem?, jwt_issuer?, jwt_audience?, heartbeat_interval_seconds?, telemetry_interval_seconds?, config_revision?}` |
| GET | `/api/v1/agent/control` | WebSocket 升级请求，头：`Authorization: Bearer <JWT>`、`X-Node-ID`、`X-Agent-Version`、`X-Agent-Protocol` | WebSocket（二进制帧 = Envelope） |
| GET | `/api/v1/agent/token` | 可选：Agent 自检用（未实现则允许 404） | 任意 |

### 5.2 指令参数与结果

| 指令 | params（JSON） | result（JSON） |
|---|---|---|
| `reload_config` | `{revision?,format?,payload?(base64 或原文),reason?,force?,dry_run?}`（不带 payload 表示"用磁盘上的配置重载"） | ConfigAck 同构：`{revision,ok,error,checks,warnings,rolled_back,duration_ms}` |
| `rollback_config` | `{revision?,reason?}` | 同上（`revision` 为恢复到的版本） |
| `restart_hysteria` | `{}` | `{pid,running,restarts}` |
| `upgrade_agent` | `{version,url,sha256,signature,size?,force?,min_version?,restart_mode?}` | `{version,old_version,sha256,backup,steps[],restarted,binary_path}` |
| `run_diagnostic` | `{}` | 诊断报告：`{node_id,status,checks:[{name,status,detail,data,duration_ms}]}` |
| `tail_logs` | `{duration_seconds?,backlog_lines?,source?(all\|agent\|hysteria),level?}` | `{lines_sent,duration_seconds,stopped}` + N 个 `CommandChunk` |
| `cancel_tail` | `{command_id}` | `{cancelled}` |
| `kick_users` | `{user_ids:[…],reason?}` | `{kicked:[…],note}`（提示"踢 ≠ 封禁"） |
| `get_state` | `{}` | 状态快照（见下） |
| `self_restart` | `{}` | `{restarting:<unit>}` |
| `clear_outbox` | `{confirm:"yes"}` | `{cleared:N}`（危险操作，必须显式确认） |

`get_state` 结果结构：

```json
{
  "node_id": "node_000123", "agent_version": "0.1.0", "uptime_seconds": 3600, "ready": true,
  "config": {"path":"/etc/hysteria/config.yaml","revision":"20261015-3","sha256":"…",
             "last_apply_at":"…","last_apply_error":"","history":["20261015-3(ok)","20261015-2(ok)"]},
  "control": {"connected":true,"reconnects":1,"last_acked_seq":1024,"pending_events":0,
              "server_config_revision":"20261015-3","server_skew_ms":12},
  "hysteria": {"mode":"process","running":true,"pid":12345,"uptime_seconds":3000,
               "version":"v2.6.5","restart_count":1,"last_exit_code":137},
  "logs": {"remote_enabled":true,"sent":1203,"sampled_out":88}
}
```

### 5.3 自升级签名规范

```
sha256_hex = hex(sha256(binary))
signature  = base64( ed25519_sign(privkey, sha256_hex 的 ASCII 字节) )
```

* 控制面侧参考实现：`cmd/agent-sign`（`keygen` / `sign` / `pubkey`）；
* 也接受 hex 形式的签名，以及"直接对文件内容签名"的实现（验签时两种都试，见 `upgrade.VerifyPayload`）；
* 防降级：`version` 与当前相同且 `force=false` 时拒绝；如需严格防降级，控制面用 `min_version` 把关。

---

## 6. 配置校验（五层）与历史管理

### 6.1 校验分层

| 层 | 检查内容 | 失败处理 |
|---|---|---|
| schema | YAML/JSON 语法；`listen`（**省略时按 Hysteria 默认 `:443` 处理并告警**，因为上游允许省略，诊断老配置不能因此报假错）；`tls`/`acme` 二选一；`auth.type ∈ {password,userpass,http,command}`；**未知顶层键告警**（专治 `masqueradee` 这类拼写错误） | error → 拒绝落盘 |
| 端口 | `listen` 端口在 TCP/UDP 上的占用情况；**占用者是正在守护的 hysteria（pid）或同名可执行文件时视为合法**（热更新 / 在有实例的机器上校验新配置） | error（UDP 被别的进程抢是致命的） |
| 证书 | 证书可解析、公私钥配对、未过期/未生效、临近过期（默认 14 天）告警、SAN 覆盖 acme 域名 | error / warn |
| 一致性 | `trafficStats.secret` 必须与 Agent 的 `hysteria.api_secret` 一致（否则遥测与踢人会静默失效）；`trafficStats.listen` 必须存在 | error |
| 引用 | `acl.file` / `mmdb` / `resolver.file` 等引用文件是否存在 | warn |

校验结果会以 `checks[]`（如 `schema:ok`、`cert:expires_in=45d`、`port:udp/443=owned_by_hysteria`）形式回执给控制面。

### 6.2 版本历史与回滚

* 每个版本落盘为 `history/rev-NNNNNN-<revision>.yaml`（0600），索引在 `history/index.json`；
* 保留最近 `history_keep`（默认 5）个版本，多余快照会连文件一起删除；
* 序号取自目录内最大序号 +1（**不能用索引长度**，裁剪后序号会回退并覆盖旧文件——这是实现里真实踩过的坑，已在 `store.nextSeq` 修正）；
* 应用失败自动回滚：恢复"第一个 sha 与当前文件不同、且结果不是 rejected/failed"的版本；回滚动作本身也写进历史（`result=rolled_back`），便于审计。

---

## 7. 可观测性

### 7.1 结构化日志字段

```json
{"ts":"2026-10-15T09:00:00.000Z","level":"INFO","node_id":"node_1","component":"control",
 "msg":"控制通道断开","trace_id":"…","error":"…"}
```

* `component ∈ {agent, control, supervisor, hysteria, hyconfig, telemetry, logs, commands, upgrade, diag}`；
* `trace_id` 来自控制面（`Envelope.trace_id` 或指令参数），会一路带进该指令产生的所有日志；
* 远程采集带采样：默认 `200 行/秒 + 500 突发`，warn/error 用独立小桶（20 行/秒）避免被 info 饿死；超限丢弃计数进 `hysteria_agent_logs_sampled_out_total`。

### 7.2 Prometheus 指标（127.0.0.1:9100/metrics）

| 指标 | 类型 | 说明 |
|---|---|---|
| `hysteria_agent_info{version,node_id}` | gauge | 恒为 1，版本信息 |
| `hysteria_agent_uptime_seconds` | gauge | Agent 运行时长 |
| `hysteria_agent_control_connected` | gauge | 控制通道是否连接 |
| `hysteria_agent_control_reconnects_total` | counter | 重连次数 |
| `hysteria_agent_heartbeat_sent_total` / `_acked_total` | counter | 心跳收发 |
| `hysteria_agent_events_sent_total` / `_resent_total` / `_buffered` / `_dropped_total` | counter/gauge | 事件投递与积压 |
| `hysteria_agent_outbox_bytes` | gauge | outbox 占用 |
| `hysteria_agent_config_apply_total{result}` / `_seconds{result}` | counter/histogram | 配置应用结果与耗时（ok/rejected/failed/rolled_back/dry_run） |
| `hysteria_agent_config_revision_info{revision,sha256}` | gauge | 当前生效配置 |
| `hysteria_agent_hysteria_running` | gauge | 被守护进程是否在跑 |
| `hysteria_agent_hysteria_restarts_total` / `_last_exit_code` / `_uptime_seconds` | counter/gauge | 子进程健康度 |
| `hysteria_agent_hysteria_active_users` / `_online_devices` | gauge | 用户与设备数 |
| `hysteria_agent_hysteria_tx_bytes_total` / `_rx_bytes_total` | counter | Hysteria 上报的流量 |
| `hysteria_agent_cpu_percent` / `_mem_percent` / `_mem_used_bytes` / `_disk_percent` / `_disk_used_bytes` / `_load1` | gauge | 主机资源 |
| `hysteria_agent_net_rx_bytes_total` / `_tx_bytes_total` | counter | 主机网络 |
| `hysteria_agent_commands_total{command,status}` / `_running` | counter/gauge | 指令执行 |
| `hysteria_agent_logs_sent_total` / `_sampled_out_total` / `_tail_dropped_total` / `_tail_active` | counter/gauge | 日志链路 |
| `hysteria_agent_upgrade_total{result}` | counter | 升级尝试 |
| `hysteria_agent_last_heartbeat_timestamp` | gauge | 最近心跳成功时间 |

### 7.3 健康端点语义

* `/healthz`（liveness）：**进程活着就 200**——systemd 用它判断是否重启进程；
* `/readyz`（readiness）：`Hysteria 在运行 && 配置已应用` 才 200，另外把 `control_channel` 状态放进 `checks` 里（控制通道断开不影响已连接用户的正常使用，所以不算 not ready，但运维一眼能看到）。

---

## 8. 控制面侧需要落库的数据（建议）

节点侧只上报，落库责任在控制面。参考 `hysteria-node-agent` 仓库 `sql/` 里已有的 19 表设计（用户/服务商/节点/流量/计费/结算），Agent 相关的关键字段：

| 表/字段 | 说明 |
|---|---|
| `nodes(node_id, node_name, node_secret_hash, status, last_seen_at, agent_version, hysteria_version, config_revision)` | 节点主表；`node_secret` 只存哈希或加密值 |
| `node_enroll_tokens(token_hash, expires_at, used_by_node_id, used_at)` | 注册令牌，一次性 |
| `node_events(node_id, seq, type, exit_code, reason, payload_json, created_at)` | `NodeEvent` 落库，**唯一键 (node_id, seq) 保证 at-least-once 幂等** |
| `node_metrics(node_id, ts, cpu_percent, mem_used_bytes, …)` | 指标（按月分区，参考 `sql/08_partition_maintenance.sql`） |
| `node_logs(node_id, ts, level, component, msg, trace_id)` | 采样后的日志（建议保留 7~30 天） |
| `node_config_versions(node_id, revision, sha256, payload, applied_at, result, error)` | 配置下发审计（节点侧只保留 5 个版本，长期历史在控制面） |
| `node_commands(command_id, node_id, type, params_json, status, result_json, created_at, finished_at)` | 指令下发与结果，`command_id` 唯一 |

---

## 9. 失败模式与处理

| 失败模式 | Agent 行为 | 运维可见性 |
|---|---|---|
| 控制面不可达 | 指数退避重连（≤5min），事件进 outbox，Hysteria 不受影响 | `/readyz` 的 `control_channel=disconnected`、`events_buffered` 增长、`NodeEvent(control_disconnected)` |
| 控制面证书 pin 不匹配 | 连接失败并明确报错，不降级 | journalctl 里 `证书 pinning 校验失败：…` |
| 下发配置非法 | 拒绝落盘并回执 checks | `ConfigAck{ok:false}` + `NodeEvent(config_failed)` |
| 下发配置让服务起不来 | 自动回滚到上一版本并重启 | `ConfigAck{rolled_back:true}` + `NodeEvent(config_rolled_back)` |
| 回滚也失败 | 明确上报"回滚失败"，保持原文件不动，等待人工 | `NodeEvent(config_failed)` + `config.last_apply_error` |
| Hysteria 反复崩溃 | 退避重启，窗口内超限后放弃 | `NodeEvent(restart_gave_up)`、`/readyz` not ready、`hysteria_running=0` |
| outbox 满了 | 按 log→metric→其它 顺序丢最旧，计数上报 | `events_dropped_total`、`outbox_dropped` 事件 |
| 升级验签失败 | 拒绝替换，保留原二进制 | `CommandResult{status:failed}`、`upgrade_total{result=verify_failed}` |
| 新二进制跑不起来 | 冒烟测试拦在替换前 | `upgrade_total{result=smoke_test_failed}` |
| 证书临近过期 | 配置下发时告警，`diag` 报告里体现 | `cert:expires_in=…d`、诊断 `certificate=warn` |
| Agent 自身崩溃 | systemd `Restart=always` 拉起；outbox 里的事件不丢 | `journalctl -u hysteria-agent`、`state.json` 的 pid |
| 收到 SIGTERM | 先优雅停止 Hysteria（SIGTERM→等待→SIGKILL），再关闭指标端点、flush outbox 后退出；**第二个**信号才强退。控制通道的连接会在 ctx 结束时立即关闭，避免读循环拖到读超时（90s）才退出 | `journalctl` 里 `收到信号，开始优雅停止` / `hysteria-agent 已退出` |
| 多进程共用同一 state_dir | outbox 的水位线与条目可能不一致：载入时 `lastSeq = max(文件最大 seq, 水位线)`，发送起点取 `min(水位线+1, 本地最旧 seq)`，宁可重发也不漏发 | 重复事件会出现在控制面（按 `(node_id,seq)` 幂等入库即可） |

---

## 10. 需求对照表

| 需求条目 | 实现位置 | 验证方式 |
|---|---|---|
| 1 注册/身份（enroll_token → node_id+node_secret，0400） | `internal/enroll`, `internal/creds`, `cmd/hysteria-agent` (`enroll`) | E2E §1；`internal/creds` 单测（权限、JWT） |
| 1b 后续用 node_secret 做 mTLS 或 JWT | `internal/creds.TLSConfig/BearerHeader`，`internal/control.session` | E2E：WSS 用 JWT 连接成功；单测：pin/mTLS |
| 2 长连接控制通道（WebSocket+Protobuf） | `proto/agent.proto`, `internal/control`, `internal/wire` | E2E §2、`cmd/mockcontrol`（第二实现） |
| 2b 自动重连（指数退避，上限 5 分钟） | `internal/control.Run` | E2E §6；指标 `control_reconnects_total` |
| 2c 心跳 30s / 超时 90s | `internal/control.session`（ReadDeadline）、`config.Control` | E2E：3s 心跳配置下 heartbeat 计数增长 |
| 2d 断线缓存 + 重连补发 | `internal/outbox` + `control.flushPending` | E2E §6（`dumpq check` 断言 seq 连续无重复）；outbox 单测 |
| 3 配置下发（YAML/JSON）+ 校验 | `internal/hyconfig.Validate`（五层） | `hyconfig` 单测 12 例 + E2E §3 |
| 3b 原子写 /etc/hysteria/config.yaml + 热更新 | `hyconfig.Store.Apply`（tmp+fsync+rename）、`supervisor.Reload` | E2E §3（权限 0600、内容断言） |
| 3c 保留最近 5 个版本 + 回滚 | `hyconfig.Store`（history/index.json、Prune） | `store` 单测（Prune=3 场景）+ E2E §3c |
| 3d 失败自动回滚并上报错误 | `agent.ApplyPayload` 回滚分支 | E2E §3c（毒配置 → `rolledBack:true`） |
| 4 systemd 管理 Agent + 单元模板 | `deploy/hysteria-agent.service`、`install.sh` | 真机安装（见 README「验证记录」） |
| 4b 拉起/监控 Hysteria 子进程 | `internal/supervisor`（process/systemd/external） | E2E §4（kill -9 后自动重启） |
| 4c 崩溃自动重启 + 记录 exit code/日志 | `supervisor.reap/scheduleRestart` | E2E §4 + `NodeEvent(exited)` 断言 |
| 4d status / restart / upgrade 命令 | `cmd/hysteria-agent` | E2E §8 |
| 5 每 15s 上报 CPU/内存/磁盘/网络 | `internal/sysinfo` + `internal/telemetry` | E2E：metricBatch 到达控制面 |
| 5b Hysteria 连接数/流量/活跃用户/运行时长/版本 | `internal/hyapi` + `telemetry` | E2E：`/traffic`、`/online` 被调用（mock 记录请求次数） |
| 5c 日志远程采集 + 采样率限制 | `internal/logs.Forwarder` + `logging.Sampler` | E2E：logBatch 到达；采样计数指标 |
| 6 六类远程指令 | `internal/commands`（11 个白名单指令） | E2E §5、§7（每条指令都有断言） |
| 6b tail_logs 限时 5 分钟 | `commands.tailLogs`（`tail_logs.max_duration_seconds=300`） | E2E §5（分片 + eof） |
| 6c upgrade_agent 验签 + 原子替换 + 重启 | `internal/upgrade` | E2E §7（正确签名成功、错误签名拒绝）+ upgrade 单测 6 例 |
| 6d run_diagnostic（端口/证书/DNS/出口 IP） | `internal/diag`（12 项检查） | E2E §5（报告含 checks）+ `diag` CLI |
| 7 TLS + 证书 pinning | `internal/creds.TLSConfig` / `pinVerifier` | creds 单测（pin 匹配/不匹配）；`diag` 输出当前 SPKI pin |
| 7b 配置 0600 / 凭据 0400 | `hyconfig.atomicWrite`、`creds.Save` | 单测 + E2E 权限断言 |
| 7c 自升级必须 ed25519 验签 | `internal/upgrade.VerifyPayload` | upgrade 单测 + E2E §7 |
| 7d 禁止任意 shell（白名单） | `commands`（固定 exec）+ `supervisor.allowedSystemctlVerbs` | 代码审计 + `wire.ParseCommandType` 拒绝未知指令 |
| 8 结构化 JSON 日志（ts/level/node_id/component/msg/trace_id） | `internal/logging` | E2E：dump 里的 logBatch 字段 |
| 8b `/metrics` + `/healthz` + `/readyz` | `internal/metrics` | E2E §2（三个端点 200） |
