# hysteria-agent

Hysteria2 节点的**管理守护进程**：一条 WebSocket + Protobuf 长连接把节点接进控制面，负责注册、配置下发与热更新、进程守护、遥测上报、远程指令（含自升级与诊断）。

> 协议与设计细节见 [DESIGN.md](DESIGN.md)；接口契约（消息表、指令参数、时序图）都在那里，本文偏使用与运维。

## 它解决什么问题

一台跑 Hysteria2 的节点机，运维上通常要面对：配置改了要上去手改、进程挂了没人拉、流量/负载看不到、要重启得 SSH、要升级得逐台登录。这个 Agent 把这几件事都收进控制面：

```
控制面 ──WSS(Protobuf)+HTTPS(JSON)──▶ hysteria-agent ──► Hysteria2 进程
   ▲                                        │
   └── 心跳 / 指标 / 日志 / 事件 / 指令结果 ──┘
```

| 能力 | 一句话 |
|---|---|
| 注册与身份 | `enroll --token=…` 一次性换取 `node_id` + `node_secret`，存 `credentials.json`(0400)；之后用 `node_secret` 自签 JWT（或 mTLS）连接 |
| 长连接 | 心跳 30s / 超时 90s；指数退避重连（上限 5 分钟）；**断线期间事件落盘，重连后按 seq 补发** |
| 配置下发 | 五层校验（schema/端口/证书/一致性/引用）→ 原子写 `/etc/hysteria/config.yaml`(0600) → 热更新 → 失败自动回滚；保留最近 5 个版本 |
| 进程守护 | `process`（Agent 直接拉起子进程）/ `systemd` / `external` 三种模式；崩溃退避重启、记录 exit code、窗口内超限则放弃并告警 |
| 遥测 | 15s 上报 CPU/内存/磁盘/网络 + Hysteria 在线设备/活跃用户/流量/运行时长/版本；日志按采样率远程采集 |
| 远程指令 | `reload_config`、`restart_hysteria`、`upgrade_agent`（ed25519 验签+原子替换）、`run_diagnostic`、`tail_logs`（限时 5 分钟）、`kick_users`、`get_state`、`rollback_config`、`cancel_tail`、`self_restart`、`clear_outbox` |
| 安全 | 出站 TLS + SPKI 证书 pinning；配置 0600 / 凭据 0400；升级强制验签；**只执行白名单指令，永不 `sh -c` 任意命令** |
| 可观测 | 结构化 JSON 日志（ts/level/node_id/component/msg/trace_id）；`127.0.0.1:9100` 的 `/metrics`、`/healthz`、`/readyz` |

与同仓库/同系列的早期组件 `hysteria-node-agent`（只做"拉踢人列表 + 上报流量"的最小脚本）不同：本程序是**常驻守护 + 双向控制通道**形态，能管进程、管配置、管升级。

## 环境要求

* 节点机：Linux（systemd 推荐）；root 或等价权限
* Hysteria 2 服务端（Agent 可以代为安装/守护，也可以只管已有的实例）
* 控制面：实现 [DESIGN.md §5.1](DESIGN.md) 的三个接口即可（Go / Python / PHP 都行；`cmd/mockcontrol` 是一份 200 行的参考实现）

## 快速开始

### 一键安装（推荐）

```bash
# 1) 在节点机上（脚本会自己装 Hysteria、生成配置、装 systemd 单元、注册节点）
bash install.sh https://cp.example.com --token=<enroll_token> --name=hk-node-1

# 常用参数
#   --no-hysteria        不动 Hysteria（外部已装好）
#   --hysteria-config P  指定 Hysteria 配置路径（默认 /etc/hysteria/config.yaml）
#   --skip-enroll        只装不注册
#   --verify-only        只做巡检，不改任何文件
#   --uninstall          卸载（Hysteria 本身不动）
#
# 关键环境变量
#   HYSTERIA_DOMAIN / HYSTERIA_ACME_EMAIL   有域名就用 ACME，否则自签证书
#   UPGRADE_PUBKEY                          自升级验签公钥（不给则关闭自升级）
#   AGENT_BINARY / AGENT_BINARY_URL         二进制来源
```

### 手动安装

```bash
make build                                  # 产出 bin/hysteria-agent
install -m0755 bin/hysteria-agent /usr/local/bin/
install -d -m0700 /etc/hysteria-agent
install -m0600 deploy/config.yaml /etc/hysteria-agent/config.yaml
$EDITOR /etc/hysteria-agent/config.yaml     # 至少改 control.server_url
hysteria-agent enroll --token=<enroll_token>
install -m0644 deploy/hysteria-agent.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now hysteria-agent
```

### 常用命令

```bash
hysteria-agent status [--json]     # 运行状态（读 state.json + 本地端点 + systemd）
hysteria-agent diag [--json]       # 12 项健康检查：端口/证书/DNS/出口 IP/控制面/API/磁盘/时钟…
hysteria-agent restart             # 重启 Agent（--hysteria-only 只重启 Hysteria）
hysteria-agent config history      # 配置历史（最近 5 个版本）
hysteria-agent config rollback --rev=<revision>
hysteria-agent config validate -f /tmp/new-config.yaml
hysteria-agent upgrade --url=… --sha256=… --signature=…   # 手动升级（同样强制验签）
hysteria-agent version
curl -s 127.0.0.1:9100/metrics | head      # Prometheus
curl -s 127.0.0.1:9100/readyz              # 就绪检查
```

## 配置表

`/etc/hysteria-agent/config.yaml`（0600）。下划线项为默认值，最小配置只需要 `control.server_url`。

| 键 | 默认值 | 说明 |
|---|---|---|
| `control.server_url` | 必填 | 控制面地址（`https://` 或本地联调的 `http://`） |
| `control.ws_path` | `/api/v1/agent/control` | 控制通道路径 |
| `control.enroll_path` | `/api/v1/agent/enroll` | 注册接口路径 |
| `control.token_path` | `/api/v1/agent/token` | 可选自检接口 |
| `control.user_agent` | `hysteria-agent` | HTTP User-Agent |
| `control.ca_file` | 空 | 私有 CA（PEM） |
| `control.pin_sha256` | `[]` | 证书 SPKI pin（base64），非空则强制 pinning |
| `control.insecure_skip_verify` | `false` | 仅当有 pin 时才允许为 true |
| `control.allow_insecure_ws` | `false` | 允许 `ws://`（仅本地联调） |
| `control.connect_timeout_seconds` | `15` | 拨号超时 |
| `control.handshake_timeout_seconds` | `20` | WebSocket 握手超时 |
| `control.write_timeout_seconds` / `read_timeout_seconds` | `15` / `120` | 读写超时 |
| `control.heartbeat_interval_seconds` | `30` | 心跳间隔（控制面可在 HelloAck 覆盖） |
| `control.heartbeat_timeout_seconds` | `90` | 超时判定断线 |
| `control.telemetry_interval_seconds` | `15` | 遥测间隔（可被覆盖） |
| `control.jwt_expiry_seconds` | `3600` | 自签 JWT 有效期 |
| `control.max_message_bytes` | `4194304` | 单帧上限 |
| `control.token_refresh_leeway_seconds` | `300` | 重连前提前续签的余量 |
| `control.reconnect.{initial_seconds,max_seconds,multiplier,jitter}` | `1 / 300 / 2 / 0.2` | 退避策略 |
| `credentials_file` | `/etc/hysteria-agent/credentials.json` | 凭据（0400） |
| `state_dir` | `/var/lib/hysteria-agent` | outbox / state.json / 升级目录 |
| `runtime_dir` | `/run/hysteria-agent` | 运行时目录 |
| `hysteria.mode` | `process` | `process` / `systemd` / `external` |
| `hysteria.binary` | `/usr/local/bin/hysteria` | Hysteria 可执行文件 |
| `hysteria.config_path` | `/etc/hysteria/config.yaml` | Agent 负责原子写入的目标配置 |
| `hysteria.service_name` | `hysteria-server` | systemd 模式下的单元名 |
| `hysteria.work_dir` | 空 | 子进程工作目录 |
| `hysteria.env` / `hysteria.extra_args` | `[]` | 子进程环境变量/附加参数 |
| `hysteria.api_base_url` | `http://127.0.0.1:8080` | Hysteria 的 `trafficStats.listen` |
| `hysteria.api_secret` | 空 | 必须与 `trafficStats.secret` 一致（否则校验会拒绝下发） |
| `hysteria.log_paths` / `journal_unit` | `[]` / 空 | 附加日志来源 |
| `hysteria.startup_timeout_seconds` / `shutdown_timeout_seconds` | `20` / `15` | 启动就绪等待 / SIGTERM 优雅停止上限 |
| `hysteria.restart.{initial,max,multiplier,jitter}` | `1 / 60 / 2 / 0.2` | 崩溃重启退避 |
| `hysteria.restart_max_in_window` / `restart_window_seconds` | `10` / `300` | 窗口限流，超限放弃重启并上报 |
| `hysteria.history_dir` / `history_keep` | `/etc/hysteria-agent/history` / `5` | 配置历史与保留数量 |
| `hysteria.validate_port` / `validate_cert` / `cert_expiry_warn_days` | `true` / `true` / `14` | 下发校验开关与证书告警阈值 |
| `telemetry.interval_seconds` | `15` | 采集间隔 |
| `telemetry.disk_paths` | `["/", "/var/lib/hysteria-agent"]` | 磁盘统计路径 |
| `telemetry.net_interfaces` | `[]`（全部非 lo） | 网络统计接口 |
| `telemetry.hysteria_enabled` | `true` | 是否读 Hysteria 本地 API |
| `logs.level` / `format` | `info` / `json` | 日志级别与格式 |
| `logs.remote_enabled` | `true` | 远程日志采集 |
| `logs.ring_size` | `2000` | 本地环形缓冲（tail 回放） |
| `logs.include_hysteria` | `true` | 采集子进程 stdout/stderr |
| `logs.sample.{max_lines_per_second,burst,min_level,max_batch,flush_interval_ms,max_line_bytes}` | `200/500/info/100/500/8192` | 采样与限速 |
| `metrics.enabled` / `listen` / `path` | `true` / `127.0.0.1:9100` / `/metrics` | 指标端点 |
| `metrics.health_path` / `ready_path` | `/healthz` / `/readyz` | 健康端点 |
| `outbox.path` | `/var/lib/hysteria-agent/outbox.jsonl` | 事件缓冲 |
| `outbox.max_bytes` / `max_events` / `fsync` / `compact_min_drops` | `32MiB` / `20000` / `true` / `64` | 缓冲上限与落盘策略 |
| `upgrade.enabled` | `true` | 自升级开关（无公钥时必须 false） |
| `upgrade.pubkeys` | `[]` | ed25519 公钥（base64），可多把轮换 |
| `upgrade.binary_path` / `work_dir` | `/usr/local/bin/hysteria-agent` / `/var/lib/hysteria-agent/upgrade` | 目标二进制与工作目录 |
| `upgrade.allow_downgrade` | `false` | 是否允许降级 |
| `upgrade.download_timeout_seconds` / `smoke_test_seconds` | `120` / `10` | 下载与冒烟测试 |
| `upgrade.restart_mode` / `service_name` | `systemd` / `hysteria-agent` | 升级后的重启方式（`systemd`/`exec`） |
| `diag.dns_hosts` / `public_ip_urls` | `[]` / `["https://api.ipify.org"]` | 诊断的 DNS 与出口 IP 探测 |
| `diag.port_check_timeout_ms` / `http_timeout_ms` / `check_control_plane` | `800` / `5000` / `true` | 诊断超时与开关 |
| `tail_logs.max_duration_seconds` | `300` | `tail_logs` 硬上限（5 分钟） |

## 控制面接口契约（最小实现要点）

```
POST /api/v1/agent/enroll    → {node_id, node_secret, pin_sha256?, ...}
GET  /api/v1/agent/control   → WebSocket（Authorization: Bearer <JWT(HS256, key=node_secret)>）
```

* JWT claims：`iss=hysteria-agent`、`sub=<node_id>`、`aud=hysteria-control`、`exp/iat/nbf/jti`；用该节点的 `node_secret` 做 HS256 验签即可。
* 收到 `Hello` 要回 `HelloAck{resend_from_seq: 已确认最大 seq+1, heartbeat_interval_seconds, telemetry_interval_seconds}`。
* 所有 `seq>0` 的消息都要按 `(node_id, seq)` **幂等入库**，并回 `EventAck{acked_seq}`（累积确认）。
* 帧格式：WebSocket 二进制帧 = `Envelope` 的 protobuf 序列化（`proto/agent.proto`）。

FastAPI 骨架（节选）：

```python
from fastapi import FastAPI, WebSocket, Header
import jwt  # PyJWT

@app.post("/api/v1/agent/enroll")
def enroll(body: dict, x_enroll_token: str = Header(default="")):
    token = x_enroll_token or body["enroll_token"]
    node = cp.consume_enroll_token(token)          # 一次性令牌
    return {"node_id": node.id, "node_secret": node.secret,
            "pin_sha256": [cp.spki_pin], "heartbeat_interval_seconds": 30,
            "telemetry_interval_seconds": 15}

@app.websocket("/api/v1/agent/control")
async def control(ws: WebSocket, authorization: str = Header(default=""),
                  x_node_id: str = Header(default="")):
    node = cp.get_node(x_node_id)
    jwt.decode(authorization.removeprefix("Bearer "), node.secret, algorithms=["HS256"],
               audience="hysteria-control", issuer="hysteria-agent")   # 校验
    await ws.accept()
    async for raw in ws.iter_bytes():
        env = Envelope.FromString(raw)             # agent_pb2
        cp.handle(node, env)                       # 幂等入库 + ack
```

更多参考实现与端到端测试：`cmd/mockcontrol`（控制面）、`cmd/mockhysteria`（假 Hysteria）、`cmd/agent-sign`（升级签名工具）、`tests/e2e.sh`。

## 升级签名公式

```
sha256_hex = hex(sha256(binary))
signature  = base64(ed25519_sign(private_key, sha256_hex))     # 对哈希的 hex 字符串签名
```

```bash
./bin/agent-sign keygen -out /etc/hysteria-agent/upgrade.key     # 打印 base64 公钥
# 公钥填进 Agent 的 upgrade.pubkeys（或用 -ldflags 注入 version.UpgradePublicKey）
./bin/agent-sign sign -key /etc/hysteria-agent/upgrade.key -file ./hysteria-agent \
  -version 0.2.0 -url https://cp.example.com/dl/hysteria-agent-0.2.0
# 输出的 JSON 直接作为 upgrade_agent 指令的 params
```

## 工作方式与可靠性

* **投递语义 at-least-once**：`seq>0` 的消息先写 outbox（fsync）再发送，控制面累积确认后才删除；断线重连从 `resume_from_seq` 补发。控制面必须按 `(node_id, seq)` 幂等入库。
* **配置热更新是"优雅重启"**：Hysteria 2 上游只处理 `SIGINT/SIGTERM`（没有 SIGHUP 处理），所以 Agent 的做法是 SIGTERM 优雅收尾 → 以新配置拉起，断连窗口通常是进程启动时间，客户端会自动重连。这一点在 [DESIGN.md §4.2](DESIGN.md) 有明确说明，不做"看起来支持热重载"的封装。
* **自动回滚**：新配置校验通过但服务起不来（端口冲突、证书问题、上游拒绝启动）→ 自动恢复上一个版本并重启，回执里 `rolled_back=true`，同时上报 `config_rolled_back` 事件。
* **`/readyz` 的语义**：`Hysteria 在跑 && 配置已应用`。控制通道断开**不会**让它变 not ready——已连接的用户仍在正常使用，控制面侧看 `Heartbeat` 超时即可判定节点离线。
* **踢人 ≠ 封禁**：`kick_users` 只调 Hysteria `/kick`，客户端会自动重连。真正阻断必须在控制面认证后端把用户置为禁用。

## 验证记录

代码量：Go 源码 ~11.2k 行（不含 protoc 生成）+ 协议 330 行 + 脚本/文档 ~2k 行。

### 单元测试（`make test`，5 个包 36 个函数 / 41 个用例，全绿）

| 包 | 覆盖内容 |
|---|---|
| `outbox` (6) | 递增 seq、累积确认、重启恢复、上限丢弃、坏行容错、**水位线高于条目时不搁置事件**（回归） |
| `hyconfig` (13) | 五层校验各失败分支（缺 listen/非法 auth/acme 无域名/secret 不一致/证书过期/证书私钥不匹配/临期告警/端口被占 vs 被自己占）、JSON 格式、垃圾输入；历史快照、原子写 0600、回滚、Prune 保留 3 个、索引损坏自愈、rejected 版本不参与回滚 |
| `upgrade` (6) | 合法签名、篡改内容、错误密钥、缺签名、原子替换 + 备份 + 0755、下载失败/验签失败/冒烟失败时**不动原二进制** |
| `creds` (7) | 凭据 0400 写入与加载收紧、JWT 往返/坏密钥/篡改/过期、SPKI pin 匹配与不匹配、insecure 无 pin 报错 |
| `wire` (3) | 15 种信封往返、协议版本不兼容拒绝、白名单解析与未知指令拒绝 |

### 端到端（`make e2e`：真实 Agent + 假控制面 + 假 Hysteria，断言"控制面收到了什么"）

73 项断言，覆盖需求逐条：

| 场景 | 断言要点 |
|---|---|
| 注册 | enroll 成功；`credentials.json` 0400；错误令牌被拒 |
| 长连接 | Hello 带能力白名单与 `resume_from_seq`；心跳与指标到达；`/healthz`、`/readyz`、`/metrics` 均 200 |
| 配置下发 | 目标文件 0600 且内容为新版；`ConfigAck{ok:true}`；revision 写入历史与 state.json；Hysteria 以新配置运行 |
| 非法配置 | 回执 `ok=false` 且**磁盘文件 sha256 未变**；上报 `config_failed` 事件 |
| 起不来的配置 | 自动回滚（`rolledBack:true`）、Hysteria 重新运行、revision 回退、上报 `config_rolled_back` |
| 进程守护 | `kill -9` 后新 pid 拉起且与 state.json 一致；`restart_count` 增长；上报 `exited` 事件且 `exitCode=137` |
| 远程指令 | get_state / run_diagnostic（报告含端口、证书、DNS 检查项并落盘）/ kick_users（真实调到 Hysteria `/kick` 且回执含用户）/ tail_logs（分片 + `eof` + 内容含日志行）/ reload_config 全部有断言；**重复 command_id 只执行一次** |
| 断线补发 | 断线期间 `pending_events>0`、state 显示断开；重连后从 `resume_from_seq` 起 seq **连续、无缺口、无重复**，断线期间的事件确实补发 |
| 自升级 | 错误签名被拒且二进制不变；正确签名 → 版本变 9.9.9、保留 .bak 备份、exec 重启后 Agent 仍在线、控制通道重连、Hysteria 重新拉起 |
| 优雅停止 | SIGTERM 后先停 Hysteria 再退出（`hysteria-agent 已退出`），子进程不残留 |

复现：`make e2e`（本机实测 PASS=73 FAIL=0）。

### 真机安装（`tests/install_test.sh`）

在装有生产 Hysteria 的机器上，用独立单元名 + 生产配置的**副本**跑 `install.sh`：安装 → enroll → 服务 active → `/healthz`、`/readyz` 200 → 隔离实例的 `trafficStats` API 200（不带 secret 的负例 401）→ `--verify-only` 退出码 0 → `--uninstall` 清理干净；并用 `md5sum` 与 `MainPID` 证明生产的 `/etc/hysteria/config.yaml` 与 `hysteria-server` 既未被改动也未被重启（结果见提交说明）。

## 排错表

| 现象 | 定位 | 处理 |
|---|---|---|
| 启动即退出：`control.server_url 使用明文` | 用了 `http://` | 生产用 `https://`；联调加 `control.allow_insecure_ws: true` |
| 启动即退出：`没有可用验签公钥` | `upgrade.enabled: true` 但没配公钥 | `upgrade.enabled: false` 或填 `upgrade.pubkeys` |
| `JWT 校验失败` / WS 握手 401 | 控制面侧 `node_secret` 与本地不一致，或时钟偏差过大 | 对比 `credentials.json`、查 `diag` 的时钟检查、同步 NTP |
| `证书 pinning 校验失败：…SPKI pin = …` | 控制面证书换了或 pin 配错 | 用报错里的实际 pin 更新 `control.pin_sha256` 或注册响应 |
| 连接一直重连（`control_reconnects_total` 涨） | 网络/隧道/控制面不可用 | 看 `diag` 的 `control_plane` 检查，确认 `X-Node-ID` 与 JWT 一致 |
| 配置下发被拒：`trafficStats.secret 与 Agent 的 hysteria.api_secret 不一致` | Hysteria 配置里的 secret 与 Agent 配置不一致 | 改一处即可（改 Hysteria 配置要重启 Hysteria） |
| 配置下发被拒：`udp/443 已被进程 … 占用` | 端口被别人抢了 | `ss -ulnp \| grep 443` 查持有者；若就是自己的 hysteria，检查 `hysteria.api_base_url`/进程归属 |
| 回滚循环（`rolled_back` 反复出现） | 下发的新配置本身起不来 | 看 `config.last_apply_error` 与 journalctl 里 hysteria 的报错；修正后重发 |
| `hysteria 反复崩溃`、`restart_gave_up` | 日志里 hysteria 的报错 | 修配置/证书；修好后 `hysteria-agent restart` |
| 升级后旧版本仍在跑 | `restart_mode: systemd` 但单元名不对 | 对齐 `upgrade.service_name`；或看 `/metrics` 的 `upgrade_total{result}` |
| `tail_logs` 收不到 hysteria 日志 | `systemd`/`external` 模式下需要 `hysteria.journal_unit` | 填单元名；`process` 模式下 Agent 直接抓子进程 stdout |
| 指标端点起不来 | 9100 被占 | 改 `metrics.listen`（默认只监听 127.0.0.1） |
| Agent 日志说"目标配置不存在"但文件确实在 | systemd 单元带 `PrivateTmp=true`，服务进程看不到宿主机的 `/tmp` | 配置必须放在 `/etc`、`/var/lib` 等真实路径，不要放 `/tmp` |
| 升级后 Hysteria 起不来、日志报端口被占 | exec 模式重启留下的孤儿子进程还占着 443 | 本版本已在重启前先停被守护进程；若仍出现，`ss -ulnp` 找孤儿并 kill |

## 目录结构

```
.
├── cmd/
│   ├── hysteria-agent/     # 主程序（CLI：enroll/run/status/restart/upgrade/diag/config/version）
│   ├── agent-sign/         # 控制面侧升级签名工具（keygen/sign/pubkey）
│   ├── mockcontrol/        # 假控制面（协议第二实现 + E2E 驱动接口）
│   └── mockhysteria/       # 假 Hysteria2（监听端口 + /traffic /online /kick + 毒配置）
├── internal/
│   ├── agent/       # 装配层：子系统编排、配置应用管线（校验→落盘→热更新→回滚）
│   ├── commands/    # 远程指令白名单分发与执行（含 tail_logs 流式）
│   ├── config/      # Agent 自身配置（默认值 + 校验 + 路径常量）
│   ├── control/     # WebSocket 控制通道（握手、心跳、重连、补发、分发）
│   ├── creds/       # 凭据 0400、JWT 自签、mTLS、SPKI pinning
│   ├── diag/        # 12 项健康检查
│   ├── enroll/      # 一次性注册
│   ├── hyapi/       # Hysteria 本地 API 客户端（/traffic /online /kick）
│   ├── hyconfig/    # Hysteria 配置校验（五层）+ 版本历史 + 原子写 + 回滚
│   ├── logging/     # slog JSON + 环形缓冲 + 采样器 + trace_id
│   ├── logs/        # 远程日志批量投递
│   ├── metrics/     # Prometheus 指标 + /healthz /readyz
│   ├── outbox/      # 断线事件缓冲（JSONL + 确认水位线）
│   ├── supervisor/  # Hysteria 进程/服务守护（process/systemd/external）
│   ├── sysinfo/     # /proc 采集 + 端口/PID 归因（不引第三方库）
│   ├── telemetry/   # 指标采集与投递
│   ├── upgrade/     # 验签 + 冒烟测试 + 原子替换 + 重启
│   ├── version/     # 构建期注入的版本与升级公钥
│   └── wire/        # 信封编解码与枚举命名
├── proto/agent.proto        # 控制通道协议（唯一权威定义）
├── gen/agentpb/             # protoc 生成代码
├── deploy/                  # systemd 单元 + 配置模板
├── tests/e2e.sh, dumpq.py   # 端到端联调与断言工具
├── install.sh               # 一键安装
└── Makefile
```

## 开发

```bash
make proto     # 重新生成 protobuf（需要 protoc + protoc-gen-go）
make test      # 单元测试
make e2e       # 端到端联调（需要 openssl、python3、root）
make tools     # 构建 mockcontrol / mockhysteria / agent-sign
make dist      # 产出带 sha256 的发布产物
```

协议改动流程：改 `proto/agent.proto` → `make proto` → 改 `internal/wire` → 同步 `cmd/mockcontrol`（第二实现）→ `DESIGN.md` 的消息表 → 跑 `make e2e`。
