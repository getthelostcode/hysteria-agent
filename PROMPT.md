# hysteria-agent 提示词与路径

本文件是给人/给别的 AI 复用的：上半部分是**最初的原始需求提示词**（逐字保留，就是这个项目的起点），
下半部分是**交接提示词**（换一个会话/换一个 agent 时，贴这一段就能接着干）与**路径清单**。

---

## 一、原始需求提示词（逐字）

```text
角色设定
你是一名资深 Go 语言后端工程师，熟悉 Cloudflare Tunnel（cloudflared）、Hysteria2、gRPC、WebSocket 长连接、systemd 服务管理。请根据以下需求，设计并实现一个名为 hysteria-agent 的节点管理守护进程。

功能需求
1. 节点注册与身份认证

首次启动时，通过 hysteria-agent enroll --token=<enroll_token> 向控制平面注册

控制平面返回 node_id + node_secret，持久化到 /etc/hysteria-agent/credentials.json

后续启动使用 node_secret 进行双向 TLS 或 JWT 认证

参考 cloudflared 的 cert.pem 机制

2. 长连接控制通道

与 Control Plane 建立 gRPC bidirectional stream（或 WebSocket + Protobuf）持久连接

支持自动重连（指数退避，最大 5 分钟）

心跳间隔 30s，超时 90s 视为断线

断线期间本地缓存事件，重连后补发

3. 配置下发与热更新

Control Plane 通过控制通道下发 Hysteria2 配置（YAML/JSON）

Agent 校验配置合法性（schema + 端口占用 + 证书有效性）

原子写入 /etc/hysteria/config.yaml，通过 SIGHUP 或 API 触发 hysteria 平滑重载

保留最近 5 个版本配置用于回滚

失败时自动回滚并上报错误

4. 进程生命周期管理

Agent 本身由 systemd 管理（提供 unit 文件模板）

Agent 负责拉起/监控 hysteria server 子进程（或作为 systemd 依赖）

子进程崩溃自动重启，记录 exit code 和日志

提供 hysteria-agent status / restart / upgrade 命令

5. 遥测与健康上报

每 15s 上报一次指标：
CPU / 内存 / 磁盘 / 网络 IO
Hysteria 连接数、上下行流量、活跃用户数
进程运行时长、版本号
通过控制通道的独立 stream 或 Prometheus remote write

支持日志远程采集（stdout → 控制平面，带采样率限制）

6. 远程指令
reload_config — 重载配置
restart_hysteria — 重启 Hysteria
upgrade_agent — 自升级（下载校验 sha256 + 签名，原子替换二进制，exec 重启）
run_diagnostic — 执行健康检查（端口、证书、DNS、出口 IP）并返回报告
tail_logs — 实时流式推送日志（限时 5 分钟）

7. 安全要求
所有出站连接使用 TLS，证书 pinning
配置文件权限 0600，credentials 权限 0400
自升级二进制必须验签（ed25519）
禁止 Agent 执行任意 shell 命令（只允许白名单指令）

8. 可观测性
结构化日志（JSON），字段：ts, level, node_id, component, msg, trace_id
内置 /metrics Prometheus endpoint（本地 127.0.0.1:9100）
/healthz 和 /readyz
```

---

## 二、交接提示词（换会话/换 agent 时贴这段）

```text
继续维护 hysteria-agent 项目（Hysteria2 节点管理守护进程 + 控制面，Go 1.23）。

仓库：/root/hysteria-agent（GitHub: git@github.com:getthelostcode/hysteria-agent.git, 分支 master）
文档：README.md（使用/运维/排错）、DESIGN.md（协议、时序图、失败模式）、PROMPT.md（本文件）

当前状态（都在仓库里，不要重复造）：
- 节点侧 Agent：WSS+Protobuf 长连接、断线补发、配置下发（五层校验+原子写 0600+失败回滚+保留 5 版）、
  子进程守护（三种模式）、15s 遥测、11 条白名单远程指令（含 ed25519 验签自升级、诊断、tail_logs、kick）、
  /metrics /healthz /readyz（127.0.0.1:9100）。
- 控制面：cmd/controlplane + internal/cp（注册、控制通道、配置/指令下发、事件归档、Prometheus、只读仪表盘）；
  node_secret = HMAC-SHA256(master_key, "node_secret:"+node_id)，控制面不保存共享密钥。
- 安装：install.sh（节点）、install-controlplane.sh（控制面）；发布包 release/*.tar.gz（amd64/arm64 静态）。

硬约束（改代码时不要破坏）：
1) Agent 只认白名单指令，永不执行任意 shell；对外操作一律固定二进制 + 固定参数（systemctl/journalctl）的 exec。
2) 配置 0600 / credentials 0400；自升级必须 ed25519 验签，没配公钥就禁用自升级，绝不退化成不验签。
3) 明文 http/ws 只允许显式 control.allow_insecure_ws: true（仅联调）。
4) 应用配置后失败必须原子回滚，并上报 config_failed / config_rolled_back 事件。
5) 真机测试不得干扰生产：用独立服务名 + 生产配置的副本，并证明 /etc/hysteria/config.yaml 的 md5 与
   hysteria-server 的 MainPID 未变。

验证命令（改完必须跑，全绿才算完成）：
  export PATH=$PATH:/usr/local/go/bin
  go test ./... -count=1                  # 6 个包
  make build && make release              # 产出 bin/ 与 release/*.tar.gz
  bash tests/e2e.sh                       # 74 断言（真 Agent + mock 控制面）
  bash tests/controlplane_smoke.sh        # 37 断言（真 Agent + 真控制面）
  bash tests/smoke_release.sh             # 28 断言（用发布包里的二进制跑）
  bash tests/install_test.sh              # 21 断言（真机隔离安装，不碰生产）
  bash tests/install_from_clone.sh        # 13 断言（bare clone 直接 install.sh）

已知坑（详见技能 hysteria-node-daemon 与 README 排错表）：
- Hysteria2 上游没有 SIGHUP，"热更新"= SIGTERM 优雅停止 + 立即以新配置拉起。
- outbox 用 JSONL + 旁路 ack 水位线；水位线高于文件最大 seq 会让积压永不投递（已修：取 max/min）。
- 控制面必须对重复 seq 仍然回 EventAck，否则 Agent 的 outbox 水位线卡住。
- HelloAck 协商的心跳间隔必须立刻生效（已改成可重置 Timer）。
- 测试脚本里不要用 pkill -f 的模式串出现在自己的命令行里（会自杀）；用 flock + 端口清理。
- systemd 单元带 PrivateTmp=true 时，Agent 看不到宿主机的 /tmp，配置要放 /etc、/var/lib。
```

---

## 三、路径清单

### 仓库（开发机 `/root/hysteria-agent`）

| 路径 | 内容 |
|---|---|
| `cmd/hysteria-agent/` | Agent 主程序（CLI：`enroll`/`run`/`status`/`restart`/`upgrade`/`diag`/`config`/`version`） |
| `cmd/controlplane/` | 控制面（`init`/`serve`/`token`/`nodes`/`node`/`push-config`/`command`/`events`/`telemetry`/`delete-node`） |
| `cmd/agent-sign/` | 自升级签名工具（`keygen`/`sign`/`pubkey`） |
| `cmd/mockcontrol/`、`cmd/mockhysteria/` | 测试用假控制面 / 假 Hysteria |
| `internal/` | `agent`(主循环) `config` `control`(长连接) `commands`(白名单) `cp`(控制面) `creds` `diag` `enroll` `hyapi` `hyconfig` `logging` `logs` `metrics` `outbox` `supervisor` `sysinfo` `telemetry` `upgrade` `version` `wire` |
| `proto/agent.proto`、`gen/agentpb/` | 协议定义与生成的 Go 代码（改 proto 后 `make proto`，勿手改 gen） |
| `bin/` | 本机构建产物（不入库） |
| `dist/hysteria-agent-linux-amd64`(+`.sha256`) | 入库的单文件发布二进制 |
| `release/hysteria-agent-0.1.0-linux-{amd64,arm64}.tar.gz` + `SHA256SUMS` | 入库的发布包（含 `bin/{hysteria-agent,controlplane,mockcontrol,mockhysteria,agent-sign}`、`install.sh`、`deploy/`、`README.md`、`DESIGN.md`） |
| `deploy/` | `config.yaml` + `hysteria-agent.service`（节点）、`controlplane.yaml` + `controlplane.service`（控制面） |
| `install.sh`、`install-controlplane.sh` | 一键安装（节点 / 控制面），支持 `--verify-only` / `--uninstall` |
| `tests/` | `e2e.sh`(74) `controlplane_smoke.sh`(37) `smoke_release.sh`(28) `install_test.sh`(21) `install_from_clone.sh`(13) `cleanup.sh` `dumpq.py` |
| `README.md`、`DESIGN.md`、`PROMPT.md` | 使用/运维、协议与设计、本文件 |

### 节点机上的运行时路径（`install.sh` 装完之后）

| 路径 | 说明 |
|---|---|
| `/usr/local/bin/hysteria-agent` | Agent 二进制（旧版本备份为 `.bak`） |
| `/etc/hysteria-agent/config.yaml` | Agent 配置（0600） |
| `/etc/hysteria-agent/credentials.json` | `node_id` + `node_secret`（**0400**，enroll 时写入） |
| `/etc/hysteria-agent/history/` | 配置版本历史（保留最近 5 个，供回滚） |
| `/var/lib/hysteria-agent/state.json` | 运行状态快照（`status` 命令读它） |
| `/var/lib/hysteria-agent/outbox.jsonl`(+`.ack`) | 断线期间缓存的事件与 ack 水位线 |
| `/etc/hysteria/config.yaml` | **Hysteria2 服务端配置**（Agent 下发时原子写，0600） |
| `/etc/systemd/system/hysteria-agent.service` | Agent 的 systemd 单元 |
| `127.0.0.1:9100/{metrics,healthz,readyz}` | 本地可观测端点 |

### 控制面机器上的运行时路径（`install-controlplane.sh` 装完之后）

| 路径 | 说明 |
|---|---|
| `/usr/local/bin/controlplane` | 控制面二进制 |
| `/etc/hysteria-controlplane/config.yaml` | 控制面配置（0600，含 `admin_token`；`master_key` 可放这里或 state_dir） |
| `/var/lib/hysteria-controlplane/master.key` | 主密钥（0600，node_secret 的派生种子；**备份它**） |
| `/var/lib/hysteria-controlplane/state.json` | 节点表 + 注册令牌 + 待确认配置（0600） |
| `/var/lib/hysteria-controlplane/events/<node_id>.jsonl` | 事件归档（0600，按 `retention` 裁剪） |
| `/etc/systemd/system/controlplane.service` | 控制面 systemd 单元 |
| `127.0.0.1:8443/api/v1/agent/enroll` | 注册端点（对外路径，经隧道/反代暴露） |
| `127.0.0.1:8443/api/v1/agent/control` | 控制通道（WebSocket） |
| `127.0.0.1:8443/admin/*` | 管理接口（`Authorization: Bearer <admin_token>`） |
| `127.0.0.1:8443/` | 只读仪表盘（浏览器打开） |
