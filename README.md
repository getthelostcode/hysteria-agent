# hysteria-agent

Hysteria2 节点的**管理守护进程**：注册、守护、遥测、**配置下发与自动收敛**、远程指令、自升级 —— 一个静态二进制，装完就不用再登机器改配置。

```
控制面/面板 ──WSS(Protobuf) + HTTPS(JSON)──▶ hysteria-agent ──▶ Hysteria2（含 systemd 那份）
      ▲                                            │
      └──── 心跳 / 指标 / 日志 / 事件 / 回执 ────────┘
```

| 能力 | 一句话 |
|---|---|
| 注册与身份 | `enroll --token=…` 一次性换取 `node_id` + `node_secret` → `credentials.json`(0400)；之后用 `node_secret` 自签 JWT 连控制通道 |
| 控制通道 | 心跳 30s / 超时 90s 判离线；指数退避重连（上限 5 分钟）；断线期间事件落盘、重连按 seq 补发（at-least-once） |
| **配置下发** | 控制面是权威：按 **sha256** 判断本机内容与面板是否同一份，不一致就取回 → 校验 → 快照 → 原子写 0600 → **重启 Hysteria** → 回执；起不来自动回滚，保留最近 5 个版本 |
| 进程守护 | `systemd`（管现有 `hysteria-server`）/ `process`（自己拉起）/ `external`（只观测）；崩溃退避重启、窗口限流后放弃并上报 |
| 遥测 | 15s 上报 CPU/内存/磁盘/网络 + Hysteria 在线用户/流量/运行时长/版本；日志按采样率远程采集 |
| 远程指令 | `reload_config` / `restart_hysteria` / `rollback_config` / `run_diagnostic` / `tail_logs`（≤5 分钟）/ `kick_users` / `get_state` / `upgrade_agent` / `self_restart` / `cancel_tail` / `clear_outbox` |
| 安全 | 出站 TLS + SPKI pinning；配置 0600 / 凭据 0400；升级强制 ed25519 验签；**只执行白名单指令，永不 `sh -c`** |
| 可观测 | JSON 日志（`ts/level/node_id/component/msg/trace_id`）+ `127.0.0.1:9100` 的 `/metrics` `/healthz` `/readyz` |

> 协议与设计细节见 [DESIGN.md](DESIGN.md)；控制面最小契约见 [附录 A](#附录-a控制面最小契约)；
> 与 Laravel 面板的桥接见 [docs/BRIDGE-TO-CONTROL-PLANE.md](docs/BRIDGE-TO-CONTROL-PLANE.md)。

## 目录

- [1. 快速开始](#1-快速开始)
- [2. 运行机制](#2-运行机制)
- [3. 配置下发与同步](#3-配置下发与同步)
- [4. agent 怎么管 Hysteria](#4-agent-怎么管-hysteria)
- [5. 鉴权：默认 HTTP](#5-鉴权默认-http)
- [6. 安装 / 升级 / 回退 / 卸载](#6-安装--升级--回退--卸载)
- [7. 安全](#7-安全)
- [8. 可观测](#8-可观测)
- [9. 配置参考](#9-配置参考)
- [10. 排错](#10-排错)
- [11. 测试与验证](#11-测试与验证)
- [12. 目录结构与开发](#12-目录结构与开发)

## 1. 快速开始

**环境要求**：节点机 Linux（systemd 推荐）、root 或等价权限；Hysteria 2 服务端（agent 可以代为安装/守护，也可以只管已有实例）；控制面满足 [附录 A](#附录-a控制面最小契约) 的三个接口即可（本仓库自带完整实现，见 [附录 C](#附录-c自带控制面--桥接--反代)）。

### 1.1 一键安装（推荐）

```bash
# 节点机上（root）：装 agent + Hysteria、生成配置、装 systemd 单元、注册、并把面板最新配置同步下来
sudo -E bash install.sh https://node.ipx.sarl --token=<面板签发的一次性令牌> --name=hk-node-1

# 升级 / 只同步（节点已在面板里）
cd ~/hysteria-agent && git pull && sudo -E bash install.sh https://node.ipx.sarl --skip-enroll
```

常用参数（完整表见 [6.1](#61-installsh-参数与环境变量)）：

| 参数 | 作用 |
|---|---|
| `--token=<T>` / `--name=<N>` | 一次性注册令牌 / 节点名（默认主机名） |
| `--skip-enroll` | 只装/升级，不重新注册 |
| `--hysteria-mode=auto\|systemd\|process\|external` | agent 怎么管 Hysteria（默认 `auto`，见 [§4](#4-agent-怎么管-hysteria)） |
| `--no-sync-config` / `SYNC_CONFIG_SOURCE=node` | 不跑「装完同步配置」/ 指定同步来源 |
| `--verify-only` | 只巡检，不改任何文件（退出码非 0 表示有问题） |
| `--no-rebuild` | 只用现成发布产物，绝不源码重建（版本可复现） |
| `--uninstall` | 卸载 agent（Hysteria 本身不动） |

### 1.2 用发布包（目标机没装 Go）

```bash
tar -xzf hysteria-agent-0.1.0-linux-amd64.tar.gz && cd hysteria-agent-0.1.0-linux-amd64
sha256sum -c ../SHA256SUMS      # 校验 tarball
sha256sum -c SHA256SUMS         # 校验包内二进制
bash install.sh https://node.ipx.sarl --token=<T> --name=hk-node-1
```

包内含 `bin/{hysteria-agent,controlplane,mockcontrol,mockhysteria,agent-sign}`（`CGO_ENABLED=0` 静态链接）、`install.sh`、`deploy/`、`README.md`、`DESIGN.md`。

### 1.3 手动安装

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

### 1.4 装完怎么确认

```bash
hysteria-agent version                    # 版本 + 构建提交 + 时间（对账用）
hysteria-agent status [--json]            # 运行态（读 state.json + 本地端点）
hysteria-agent diag [--json]              # 12 项健康检查：端口/证书/DNS/出口 IP/控制面/API/磁盘/时钟…
curl -s 127.0.0.1:9100/healthz; curl -s 127.0.0.1:9100/readyz; curl -s 127.0.0.1:9100/metrics | head
```

面板上应能看到：节点在线、「最后心跳」持续前移、「配置版本」= 它**实盘那份**配置对应版本管理里的第几版（见 [3.5](#35-节点把实盘配置报给面板)）。

## 2. 运行机制

| 周期 | 默认 | 说明 |
|---|---|---|
| 心跳 | 30s（超时 90s 判离线） | 控制面可在 `HelloAck` 里覆盖 |
| 遥测 | 15s | CPU/内存/磁盘/网络 + Hysteria 用户与流量 |
| **配置轮询** | 5s（`control.config_poll_seconds`） | 控制通道不可用时走 REST；带上本机 `current_sha256` 由控制面判定 |
| 日志采集 | 采样 ≤200 行/s（突发 500） | 环形缓冲 2000 行，`tail_logs` 回放 |

**配置闭环**（面板改一次，节点自己跟上）：

```
面板改默认版本 / 发布新版本
        │
        ▼
agent 每 5s：GET /api/v1/agent/config/default?current_version=N&current_sha256=<本机文件 sha>
        │        （库里没有默认版本时自动退回「本节点发布的版本」）
        ▼
控制面按 sha256 判定「是不是同一份内容」──一致──▶ 什么都不做（幂等）
        │不一致
        ▼
下发正文 → 节点：校验（问题只作警告）→ 留历史快照 → 原子写 0600
        → 重启 Hysteria（systemd：systemctl restart hysteria-server；process：SIGTERM 优雅停止后拉起）
        → 就绪检查 → 回执 applied
        └── 起不来 → 自动回滚到上一版 + 回报失败原因（含 Hysteria 最后几行输出）
```

**可靠性语义**

* **at-least-once**：`seq>0` 的消息先写 outbox（fsync）再发，控制面累积确认后才删；重连从 `resume_from_seq` 补发。控制面必须按 `(node_id, seq)` 幂等入库。
* **配置热更新 = 优雅重启**：Hysteria 2 只处理 `SIGINT/SIGTERM`（没有 SIGHUP），所以是「优雅停止 → 以新配置拉起」，断连窗口 ≈ 进程启动时间，客户端会自动重连。不做「假装支持热重载」的封装（见 [DESIGN.md §4.2](DESIGN.md)）。
* **`/readyz` 语义** = `Hysteria 在跑 && 配置已应用`；控制通道断开**不会**让它 not ready（已连接的用户还在用，控制面靠心跳超时判离线）。
* **踢人 ≠ 封禁**：`kick_users` 只调 Hysteria `/kick`，客户端会自动重连；真正阻断要在认证后端把用户禁用。
* **REST 降级只认 404/405/501**（对端没实现该端点）；`401/403` **不会**降级 —— 那是确定的凭据问题，降级只会把它掩盖掉。

## 3. 配置下发与同步

### 3.1 两份来源，取哪一份是显式开关

| 来源 | 含义 | 谁默认用它 |
|---|---|---|
| `default` | 「配置管理 → 版本管理」里那份**默认版本文件**（与节点无关，全部节点共用同一份正文） | `config sync`（CLI）与守护进程（`control.config_source` 默认 `default`） |
| `node` | **本节点**发布的第几版 | 显式 `config_source: node` / `--source node` |

* `control.config_source` 留空 = 不指定，由调用方决定默认值；
* **库里还没有默认版本时自动退回 `node`** 并如实打印来源 —— 否则会出现「面板明明发布了配置、节点一无所获」；
* 节点按文件库同步时，回执里的版本号在 `node_configs` 里没有对应行，控制面会补记一条（审计 `config.applied_from_library`），节点历史仍然完整；
* 命令行临时覆盖：`hysteria-agent config sync --source node`。

### 3.2 判定按 sha256，不按版本号

每次同步都取回**最新正文**与磁盘文件比 sha256：一致 → 明确说「已是最新（与面板 vN 内容一致）」且**不写盘**；不一致 → 写。
版本号只是控制面的记账，会被「被拒绝的版本」「就地改过的正文」「手工改过的文件」「回滚过的状态」骗过。
「本机当前是哪一版」同样按**文件哈希与历史记录精确匹配**，匹配不到就按未知处理（取最新正文重写并说明原因），绝不退回「最新一条记录」。

### 3.3 校验只作警告 + 自动回滚

* **唯一的硬底线是「连 YAML/结构都解析不出来」** —— 那才拒收（写下去只会让 Hysteria 起不来）。
* 其余问题（证书/私钥路径不存在、`trafficStats.secret` 与 `hysteria.api_secret` 不一致、端口被占…）**只作警告**，写进日志与回执，**仍然以面板为准写入**，由写后就绪检查 + 自动回滚兜底。
  > 这条曾经是硬门禁，后果是节点**永远不更新**：控制面下发的正文与节点现状稍有不符就整份拒收，磁盘一直停在旧内容（真实现场：节点每 4 秒回执一次 `证书/私钥无法加载: open /etc/hysteria/cert.pem: no such file or directory`）。
* 想恢复「不过就不写」：`config sync --strict`（CLI）或 `--strict-sync-config`（install.sh）。
* **自动回滚的三道护栏**（防止「失败 → 回滚 → 再失败」变成版本风暴）：① 最新版本已是回滚目标内容（sha256 一致）→ 跳过；② 同一节点 10 分钟内自动回滚 ≥3 次 → 跳过（判定为环境问题）；③ 回滚目标与失败版本 sha256 相同 → 跳过。
  跳过时审计 `config.auto_rollback_skipped`，节点置 `error`，错误留在面板上让人去修。

### 3.4 手工同步 / 历史 / 回滚 / 校验

```bash
hysteria-agent config sync --dry-run     # 只拉取 + 校验 + 打印会写什么（含来源）
hysteria-agent config sync               # 落盘（0600，原子写，留历史）
hysteria-agent config sync --reload      # 落盘后顺带重启被守护的 Hysteria
hysteria-agent config sync --source node # 临时按「本节点发布的版本」取
hysteria-agent config history            # 历史快照（默认保留 5 个）
hysteria-agent config rollback [-rev=<revision>]
hysteria-agent config validate -f /tmp/new.yaml
```

`install.sh` 的第 5 步就是跑一次 `config sync`：**装完/升级完，本机配置立刻就是面板上那一份**（已是最新则不动文件；面板不可达只 WARN，不算安装失败）。不想同步用 `--no-sync-config`。

### 3.5 节点把「实盘配置」报给面板

每次心跳带上本机 `/etc/hysteria/config.yaml` 的 `config_version` / `config_sha256` / `config_path`，控制面收养进节点元数据。
面板的「配置版本」列**按 `config_sha256` 反查版本号**（① 版本管理里 sha 命中 → ② 该节点历史里 sha 命中 → ③ 都没命中才退回自报版本号并注明）；只有文件与某条记录**逐字节一致**时才报版本号，被手工改过就报 0 + 实际 sha256，面板显示「未识别」，**不拿面板的期望版本糊上去**。

## 4. agent 怎么管 Hysteria

| `hysteria.mode` | 行为 | 适用 |
|---|---|---|
| `systemd` | Hysteria 由 systemd 管理；agent 只调白名单动词，配置更新后 **`systemctl restart <service>`** | 本机已有 `hysteria-server.service`（install.sh 默认自动选中） |
| `process` | agent 直接拉起/守护 `hysteria` 子进程 | 没有 systemd 单元的机器 |
| `external` | 只观测，不管理 | 别人起的实例 / `install.sh --no-hysteria` |

* `install.sh` 默认 **auto**：`systemctl list-unit-files` 里有 `<HYSTERIA_SERVICE>.service`（默认 `hysteria-server`）就写 `mode: systemd`，否则 `process`；也可显式 `--hysteria-mode=` / `HYSTERIA_MODE=` / `HYSTERIA_SERVICE=`。
* **为什么必须检测**：agent 再拉起一份 Hysteria 会和 systemd 那份抢 `udp :443`，起来就退出、反复失败回滚（真实现场：`listen udp :443: bind: address already in use`）。
* 重启语义：systemd 模式直接 `systemctl restart <service>`（先试 `systemctl reload`，单元定义了 `ExecReload` 才有效）；process 模式 SIGTERM 优雅停止后用新配置拉起。
* 崩溃重启：退避 `1s→60s`（×2，抖动 0.2），窗口 300s 内超过 10 次则放弃并上报 `restart_gave_up`。

## 5. 鉴权：默认 HTTP

静态密码写在节点上，用户离开平台依然能连、也没法按流量/到期回收，所以新生成的配置默认走 **HTTP 鉴权**：

```yaml
auth:
  type: http
  http:
    url: https://auth.ipx.sarl    # HYSTERIA_AUTH_URL
    insecure: false               # HYSTERIA_AUTH_INSECURE=1 才跳过证书校验
```

* 新装就用上面这段；想保留老行为设 `HYSTERIA_AUTH_MODE=password`。
* **已有节点不会被自动改**（install.sh 默认只读现有 `trafficStats`）；要换必须显式：

```bash
sudo bash scripts/set-hysteria-auth-http.sh https://auth.ipx.sarl      # 只替换顶层 auth 段，自动备份 + 重启
# 或 sudo -E bash install.sh https://node.ipx.sarl --set-hysteria-auth --hysteria-auth-url https://auth.ipx.sarl
```

  两种方式都**只动顶层 `auth` 段**，证书 / `masquerade` / `trafficStats` 一字不改。
* ⚠️ 切换前先探活鉴权后端，否则**所有用户都会连不上**：

```bash
curl -si -X POST -H 'Content-Type: application/json' \
  -d '{"addr":"1.2.3.4:1","auth":"a:b","tx":0,"rx":0}' https://auth.ipx.sarl/auth
# 期望 200 + {"ok":false,"id":""}
```

## 6. 安装 / 升级 / 回退 / 卸载

### 6.1 install.sh 参数与环境变量

| 参数 | 说明 |
|---|---|
| `<控制面地址>`（位置参数） | 例如 `https://node.ipx.sarl` |
| `--token=<T>` / `--name=<N>` | 注册令牌 / 节点名 |
| `--hysteria-mode=<m>` | `auto`(默认) / `systemd` / `process` / `external` |
| `--hysteria-config=<P>` | Hysteria 配置路径（默认 `/etc/hysteria/config.yaml`） |
| `--set-hysteria-auth` `--hysteria-auth-url=<U>` `--hysteria-auth-insecure` | 显式把 Hysteria 切成 HTTP 鉴权 |
| `--no-hysteria` | 不动 Hysteria（`mode` 取 `external`） |
| `--skip-enroll` | 只装不注册 |
| `--no-sync-config` / `--strict-sync-config` | 跳过「装完同步配置」/ 同步时严格校验 |
| `--no-rebuild` / `--use-dist` | 只用现成发布产物，绝不源码重建 |
| `--verify-only` / `--uninstall` | 只巡检 / 卸载 |

关键环境变量：`HYSTERIA_SERVICE`、`HYSTERIA_MODE`、`SYNC_CONFIG_SOURCE`、`HYSTERIA_DOMAIN`/`HYSTERIA_ACME_EMAIL`、`UPGRADE_PUBKEY`、`AGENT_BINARY`/`AGENT_BINARY_URL`、`SYSTEMD_UNIT_DIR`、`HYSTERIA_AGENT_MACHINE_ID`。

### 6.2 更新节点：装到的必须是新构建

`install.sh` 优先用仓库里入库的 `dist/hysteria-agent-linux-<arch>`，而 **`git pull` 不会更新它** —— 会出现「代码是新的、行为是旧的」（真实现场：装到还没有配置轮询的构建，面板发布的配置永远停在 `published`）。

* 发布产物旁边有 `.commit`（构建来源提交）与 `.sha256`；按 **提交** 判断是否过期（不是 mtime：`git pull` 会把 mtime 全刷成「现在」，每次都误判）；
* 过期 → 用当前源码重建（`go build`，ldflags 注入真实 commit）；没有 Go 工具链则**明确 WARN**，不静默装旧的；
* 安装来源优先级：`AGENT_BINARY` > `dist/` > `release/` 包 > `bin/`（本机遗留构建，最后）。真实现场：遗留的 `bin/hysteria-agent` 曾把发布产物顶掉；
* 装完打印 `hysteria-agent 0.1.0 (<commit>, <time>)`，到节点上对一下就知道装的哪一版。

### 6.3 版本标识与「回到某一版」

* 版本标签 = **最后改动 agent 源码的那个提交**（不是打包提交）：打包/文档提交一行代码没改，却会让源码重建的二进制自报另一个 commit —— 同一份代码两个身份，事后对账全是问号；源码脏了标签带 `-dirty`。
* 重打发布产物：`make dist` 或 `bash scripts/release-dist.sh`（编译 + `.sha256` + `.commit`）。

```bash
# 回到某个已发布版本（例如 0.1.0 (61145e9)）
cd ~/hysteria-agent && git pull
git log --oneline -5 -- dist/hysteria-agent-linux-amd64        # 找那一版的打包提交
git checkout <打包提交> -- dist/hysteria-agent-linux-amd64 \
    dist/hysteria-agent-linux-amd64.sha256 dist/hysteria-agent-linux-amd64.commit
sudo -E bash install.sh https://node.ipx.sarl --skip-enroll --no-rebuild
hysteria-agent version                                          # 应显示 (61145e9, …)
```

### 6.4 自升级（强制验签）

远程 `upgrade_agent` 或手工 `hysteria-agent upgrade --url=… --sha256=… --signature=…`：下载 → 校验 sha256 → 验 ed25519 签名 → 冒烟测试 → 原子替换（保留 `.bak`）→ 重启（`upgrade.restart_mode`）。任一步失败**都不动原二进制**。签名公式见 [附录 B](#附录-b升级签名公式)。

### 6.5 克隆镜像与 machine_id

控制面按 `machine_id` 认节点：**克隆出来的 VPS 镜像 machine-id 相同**，两台会互相顶掉注册。给每台一个固定值：

```yaml
enroll:
  machine_id: "jp8-ip2-one"     # 或临时：HYSTERIA_AGENT_MACHINE_ID=xxx hysteria-agent enroll …
```

## 7. 安全

* 出站 `https`/`wss` + 可选 **SPKI 证书 pinning**（`control.pin_sha256`）；`insecure_skip_verify` 只在有 pin 时允许。
* 文件权限：配置 0600、凭据 0400（写入时收紧并校验）。
* 凭据 = `node_id` + `node_secret`（HS256 自签 JWT，`iss=hysteria-agent`、`aud=hysteria-control`）；也可走 mTLS。
* **远程指令白名单**：`internal/commands` 只认识固定类型，参数按键校验；没有「执行任意 shell」这条路。
* 自升级必须验签；公钥可在配置里（`upgrade.pubkeys`，支持多把轮换）或编译期注入。

## 8. 可观测

| 端点 | 说明 |
|---|---|
| `GET 127.0.0.1:9100/healthz` | 存活 |
| `GET 127.0.0.1:9100/readyz` | 就绪 = Hysteria 在跑 && 配置已应用 |
| `GET 127.0.0.1:9100/metrics` | Prometheus |

常用指标：`hysteria_agent_config_apply_total{result}`、`…_config_apply_seconds`、`hysteria_agent_rest_heartbeats_total`、`…_rest_heartbeat_failures_total`、`hysteria_agent_rest_config_polls_total`、`…_rest_config_failures_total`、`hysteria_agent_upgrade_total{result}`、`hysteria_agent_control_reconnects_total`、`hysteria_agent_outbox_pending`，以及 Hysteria 进程/重启计数。

日志：slog JSON，字段 `ts, level, node_id, component, msg, trace_id`（+ 场景字段如 `revision`、`sha256`、`error`、`rolled_back`）。

## 9. 配置参考

`/etc/hysteria-agent/config.yaml`（0600）。最小配置只需要 `control.server_url`。

| 键 | 默认值 | 说明 |
|---|---|---|
| `control.server_url` | 必填 | 控制面地址（`https://`；联调可 `http://` + `allow_insecure_ws`） |
| `control.ws_path` / `enroll_path` / `token_path` | `/api/v1/agent/control` / `…/enroll` / `…/token` | 通道与注册端点 |
| `control.rest_heartbeat_path` / `rest_config_path` / `rest_default_config_path` | `/api/v1/agent/heartbeat` / `…/config/latest` / `…/config/default` | REST 兜底面 |
| `control.config_source` | `default` | `default`（版本文件库默认版本）/ `node`（本节点发布的版本） |
| `control.config_poll_seconds` / `config_apply_timeout_seconds` | `5` / `180` | 配置轮询间隔 / 单次应用上限 |
| `control.rest_fallback_seconds` | `300` | 降级窗口，之后重试控制通道 |
| `control.user_agent` / `ca_file` / `pin_sha256` / `insecure_skip_verify` | `hysteria-agent` / 空 / `[]` / `false` | HTTP 身份与 TLS |
| `control.allow_insecure_ws` | `false` | 允许 `ws://`（仅本地联调） |
| `control.connect_timeout_seconds` / `handshake_timeout_seconds` | `15` / `20` | 拨号 / 握手超时 |
| `control.write_timeout_seconds` / `read_timeout_seconds` | `15` / `120` | 读写超时 |
| `control.heartbeat_interval_seconds` / `heartbeat_timeout_seconds` | `30` / `90` | 心跳与离线判定 |
| `control.telemetry_interval_seconds` | `15` | 遥测间隔（可被 HelloAck 覆盖） |
| `control.jwt_expiry_seconds` / `max_message_bytes` / `token_refresh_leeway_seconds` | `3600` / `4194304` / `300` | JWT 与帧上限 |
| `control.reconnect.{initial_seconds,max_seconds,multiplier,jitter}` | `1 / 300 / 2 / 0.2` | 重连退避 |
| `credentials_file` | `/etc/hysteria-agent/credentials.json` | 凭据（0400） |
| `state_dir` / `runtime_dir` | `/var/lib/hysteria-agent` / `/run/hysteria-agent` | outbox / state.json / 升级目录 |
| `enroll.machine_id` | 空（用本机 `/etc/machine-id`） | 克隆镜像时给每台一个固定值 |
| `hysteria.mode` | `process`（install.sh 自动检测后常写 `systemd`） | `systemd` / `process` / `external` |
| `hysteria.binary` / `config_path` / `service_name` | `/usr/local/bin/hysteria` / `/etc/hysteria/config.yaml` / `hysteria-server` | 被管对象 |
| `hysteria.api_base_url` / `api_secret` | `http://127.0.0.1:8080` / 空 | 必须与 Hysteria 配置里的 `trafficStats.listen` / `.secret` 一致；不一致只是**警告**（仍写入），但 agent 读不到流量、踢不了人 |
| `hysteria.work_dir` / `env` / `extra_args` | 空 / `[]` / `[]` | 子进程（process 模式） |
| `hysteria.log_paths` / `journal_unit` | `[]` / `hysteria-server`（`mode: systemd` 时自动启用） | 额外日志来源；建议与单元名一致 |
| `hysteria.startup_timeout_seconds` / `shutdown_timeout_seconds` | `20` / `15` | 就绪等待 / 优雅停止上限 |
| `hysteria.restart.{initial,max,multiplier,jitter}` | `1 / 60 / 2 / 0.2` | 崩溃重启退避 |
| `hysteria.restart_max_in_window` / `restart_window_seconds` | `10` / `300` | 窗口限流，超限放弃并上报 |
| `hysteria.history_dir` / `history_keep` | `/etc/hysteria-agent/history` / `5` | 配置历史与保留数 |
| `hysteria.validate_port` / `validate_cert` / `cert_expiry_warn_days` | `true` / `true` / `14` | 下发校验开关与证书告警阈值（结果只作警告） |
| `telemetry.interval_seconds` / `disk_paths` / `net_interfaces` / `hysteria_enabled` | `15` / `["/", "/var/lib/hysteria-agent"]` / `[]`（全部非 lo） / `true` | 遥测 |
| `logs.level` / `format` / `remote_enabled` / `ring_size` / `include_hysteria` | `info` / `json` / `true` / `2000` / `true` | 日志 |
| `logs.sample.{max_lines_per_second,burst,min_level,max_batch,flush_interval_ms,max_line_bytes}` | `200/500/info/100/500/8192` | 远程日志采样与限速 |
| `metrics.enabled` / `listen` / `path` / `health_path` / `ready_path` | `true` / `127.0.0.1:9100` / `/metrics` / `/healthz` / `/readyz` | 指标端点 |
| `outbox.path` / `max_bytes` / `max_events` / `fsync` / `compact_min_drops` | `/var/lib/hysteria-agent/outbox.jsonl` / `32MiB` / `20000` / `true` / `64` | 断线缓冲 |
| `upgrade.enabled` / `pubkeys` | `true` / `[]`（无公钥时必须 false） | 自升级 |
| `upgrade.binary_path` / `work_dir` / `allow_downgrade` | `/usr/local/bin/hysteria-agent` / `…/upgrade` / `false` | 升级目标 |
| `upgrade.download_timeout_seconds` / `smoke_test_seconds` | `120` / `10` | 下载与冒烟测试 |
| `upgrade.restart_mode` / `service_name` | `systemd` / `hysteria-agent` | 升级后重启方式 |
| `diag.dns_hosts` / `public_ip_urls` | `[]` / `["https://api.ipify.org"]` | 诊断的 DNS 与出口 IP 探测 |
| `diag.port_check_timeout_ms` / `http_timeout_ms` / `check_control_plane` | `800` / `5000` / `true` | 诊断超时与开关 |
| `tail_logs.max_duration_seconds` | `300` | `tail_logs` 硬上限（5 分钟） |

## 10. 排错

先跑 `hysteria-agent diag --json` 与 `journalctl -u hysteria-agent -n 50`，再看下表。

| 现象 | 定位 | 处理 |
|---|---|---|
| 面板发配置了，节点磁盘**一动不动** | 看回执里 agent 的话：`无法解析` → 配置坏了；`配置已写入但 Hysteria 未能以新配置就绪` → 起不来（已自动回滚） | ① 改面板正文；② 看回执里的「Hysteria 最后输出」，多半是下面两条 |
| 回执：`证书/私钥无法加载: open /etc/hysteria/cert.pem: no such file or directory` | 面板正文引用的证书路径在节点上不存在 | 让路径真实存在（`ln -sf <真实证书> /etc/hysteria/cert.pem` + key），或把面板正文改成节点真实路径 |
| 回执：`listen udp :443: bind: address already in use` | 已有一份 Hysteria 占着端口（多为 systemd 那份），而 agent 又拉起一份 | 切 `hysteria.mode: systemd`（`install.sh --hysteria-mode=systemd`），让 agent 用 `systemctl restart hysteria-server` 管那一份；`ss -lntup \| grep ':443'` 确认持有者 |
| 版本号疯涨 / 面板一堆同内容版本 | 「失败 → 自动回滚 → 再失败」循环（环境问题） | 已内建三道护栏（见 [3.3](#33-校验只作警告--自动回滚)），审计里 `config.auto_rollback_skipped`；根因通常是上面两条 |
| 面板「配置版本」显示 `未识别` | 节点上报的 sha256 与控制面任何版本都对不上（被面板之外改过） | 让节点按面板同步一次（`config sync`），或在面板里为这份内容建一版 |
| `config sync` 说「已是最新」但磁盘里是旧值 | 它比的是内容 sha，旧值与面板当前那版**逐字节相同** | 面板改内容（或换默认版本）→ sha 变化 → 下次同步写入 |
| 面板列出的版本号与节点自报的不一样 | 列表按 `config_sha256` 反查，不信任自报版本号（自报可能因回滚/手工改而失真） | 以列表为准；悬停里写明「按版本管理 / 本节点历史 / 节点自报认定」 |
| `TLS 预检失败: … missing port in address` | 地址没写端口 | 写全 `https://host:443` |
| `注册失败(404)：<!DOCTYPE html>…` | 打到网页而不是 API：路径不对，或控制面没实现 Agent 接口 | `hysteria-agent enroll --check` 打印实际 URL/状态码/结论；确认对端有 `POST /api/v1/agent/enroll` |
| `注册失败(401)：未知的 enroll_token` | 令牌写错 / 一次性已用 / 过期 / 被吊销 | 面板重新签发 |
| `enroll --check` 里 TLS 通过但 WS 连不上 | 反代没转发 WebSocket | nginx 加 `Upgrade`/`Connection` + `proxy_read_timeout 3600s`；cloudflared 用带 path 的 ingress（见 [附录 C](#附录-c自带控制面--桥接--反代)） |
| 启动即退出：`control.server_url 使用明文` | 用了 `http://` | 生产用 `https://`；联调加 `control.allow_insecure_ws: true` |
| 启动即退出：`没有可用验签公钥` | `upgrade.enabled: true` 但没配公钥 | `upgrade.enabled: false` 或填 `upgrade.pubkeys` |
| WS 握手 401 / `JWT 校验失败` | 控制面 `node_secret` 不一致，或时钟偏差大 | 对比 `credentials.json`、看 `diag` 时钟项、同步 NTP；控制面换 `master_key` 会让所有节点必须重新注册 |
| 断开后一直重连（`control_reconnects_total` 涨） | 网络/隧道/控制面不可用 | 看 `diag` 的 `control_plane` 项，确认 `X-Node-ID` 与 JWT 一致 |
| 「最后心跳」不动 | 控制通道不可用但 REST 兜底没生效 | 确认 `control.rest_heartbeat_path` 可用；看 `rest_heartbeat_failures_total` |
| `tail_logs` 收不到 Hysteria 日志 | `systemd`/`external` 模式需要 `hysteria.journal_unit` | 填单元名；`process` 模式 agent 直接抓子进程 stdout |
| 指标端点起不来 | 9100 被占 | 改 `metrics.listen`（默认只监听 127.0.0.1） |
| 日志说「目标配置不存在」但文件确实在 | systemd 单元带 `PrivateTmp=true`，服务看不到宿主机 `/tmp` | 配置放 `/etc`、`/var/lib` 等真实路径，别放 `/tmp` |
| 升级后版本没变 | 单元名不匹配 / 没权限重启 | 对齐 `upgrade.service_name`；看 `/metrics` 的 `upgrade_total{result}` |
| 两台节点互相顶掉注册 | 克隆镜像 `machine-id` 相同 | 见 [6.5](#65-克隆镜像与-machine_id) |
| `restart_gave_up`：Hysteria 反复崩溃 | Hysteria 自身报错（配置/证书/端口） | 看 `config.last_apply_error` 与 journal；修好后 `hysteria-agent restart` |

## 11. 测试与验证

```bash
make build                      # bin/hysteria-agent + bin/controlplane
make test                       # go test ./...（各内部包单测）
make vet                        # go vet
bash tests/e2e.sh               # 真实 agent + 假控制面 + 假 Hysteria，端到端 75 项
make smoke-release              # 用发布包里的二进制重跑一遍全流程（验完发布产物再发版）
make smoke-cp                   # 跑自带的 cmd/controlplane 全流程
```

需要真实控制面/面板的端到端（本机跑）：

```bash
PANEL_URL=http://127.0.0.1:8001 bash tests/rest_config_e2e.sh    # 19 项：按节点发布 → 生效 → applied / 坏配置回滚
PANEL_URL=http://127.0.0.1:8001 bash tests/rest_fallback_e2e.sh  #  9 项：REST 兜底心跳持续前移
PANEL_URL=http://127.0.0.1:8001 bash tests/install_sync_e2e.sh   # 16 项：跑真 install.sh，装完/再跑都跟上面板
PANEL_URL=http://127.0.0.1:8001 bash tests/bridge_e2e.sh         # 14 项：Go 控制面桥接 Laravel（身份权威在面板）
```

不需要外部依赖的回归（本轮全部实测通过）：

| 测试 | 断言数 | 覆盖 |
|---|---|---|
| `tests/e2e.sh` | 75 | 注册 / 长连接 / 配置下发 / **无法解析才拒收、可解析仍写入** / 自动回滚 / 进程守护 / 远程指令 / 断线补发 / 自升级 / 优雅停止 |
| `tests/auto_converge_e2e.sh` | 18 | 本机与面板默认版本不一致 → 自动更新 + **重启 Hysteria**；面板改默认版本 → 再跟一轮；内容与节点现状不符（secret 不一致）仍写入 |
| `tests/config_sync_test.sh` | 33 | `config sync` 全部分支（来源、dry-run、strict、幂等、被手工改过、历史快照） |
| `tests/install_version_identity_test.sh` | 21 | 版本标识取源码提交 / 发布产物 `.commit` / 来源优先级（`dist/` > `bin/`）/ `--no-rebuild` / `hysteria.mode` 自动检测 |
| `tests/install_binary_freshness_test.sh` | 10 | 发布产物过期判定（提交比对 + mtime 兜底）与源码重建 |
| `tests/install_sync_e2e.sh` | 16 | 跑真 install.sh：装完跟上 / 已是最新不重写 / `--no-sync-config` 跳过 |
| `tests/hysteria_auth_http_test.sh` | 37 | HTTP 鉴权生成逻辑 + 「两份实现输出一致」防漂移断言 |
| `go test ./...` + `go vet` | — | `outbox`/`hyconfig`/`upgrade`/`creds`/`wire`/`control`/`supervisor`/`cp` 等包单测 |

真机安装（`tests/install_test.sh`）：在装有生产 Hysteria 的机器上用独立单元名 + 生产配置的**副本**跑 install.sh，安装 → enroll → `/healthz`、`/readyz` 200 → `trafficStats` API 200（无 secret 负例 401）→ `--verify-only` 退出码 0 → `--uninstall` 清理；并用 `md5sum` 与 `MainPID` 证明生产的配置与 `hysteria-server` 未被改动、未被重启。

## 12. 目录结构与开发

```
.
├── cmd/
│   ├── hysteria-agent/       # 主程序（enroll/run/status/diag/restart/upgrade/config/version）
│   ├── controlplane/         # 自带控制面：注册/控制通道/配置下发/指令/事件归档 + CLI + 只读仪表盘
│   ├── agent-sign/           # 升级签名工具（keygen/sign/pubkey）
│   ├── mockcontrol/          # 假控制面（协议第二实现 + REST 兜底面 + E2E 驱动接口）
│   └── mockhysteria/         # 假 Hysteria2（端口 + /traffic /online /kick + 毒配置）
├── internal/
│   ├── agent/       # 装配层与配置应用管线（校验→快照→原子写→重启→回滚）
│   ├── commands/    # 远程指令白名单分发（含 tail_logs 流式）
│   ├── config/      # agent 自身配置（默认值/校验/路径常量）
│   ├── control/     # 控制通道（握手/心跳/重连/补发）+ REST 兜底（心跳/配置/回执）
│   ├── creds/       # 凭据 0400、JWT 自签、SPKI pinning
│   ├── diag/        # 12 项健康检查
│   ├── enroll/      # 一次性注册（含 machine_id 覆盖）
│   ├── hyapi/       # Hysteria 本地 API（/traffic /online /kick）
│   ├── hyconfig/    # 配置校验 + 历史/原子写/回滚 + DiskIdentity（实盘身份）
│   ├── logging/ logs/ metrics/ outbox/ telemetry/
│   ├── supervisor/  # Hysteria 守护（process/systemd/external + 重启策略）
│   ├── sysinfo/     # /proc 采集与端口/PID 归因
│   ├── upgrade/     # 验签 + 冒烟 + 原子替换 + 重启
│   ├── version/     # 构建期注入的版本与升级公钥
│   └── wire/        # 信封编解码与枚举命名
├── proto/agent.proto        # 控制通道协议（唯一权威定义）
├── gen/agentpb/             # protoc 生成代码
├── deploy/                  # systemd 单元 + 配置模板
├── scripts/                 # release-dist.sh（重打发布产物）、set-hysteria-auth-http.sh
├── tests/                   # e2e 与各专项回归
├── install.sh  Makefile
└── DESIGN.md  docs/BRIDGE-TO-CONTROL-PLANE.md
```

```bash
make            # = make build：bin/hysteria-agent + bin/controlplane（+ 工具）
make tools      # mockcontrol / mockhysteria / agent-sign / cpapi-smoke
make proto      # 重新生成 protobuf（需 protoc + protoc-gen-go）
make test       # go test ./...
make vet        # go vet ./...
make dist       # 重打发布产物 dist/*（含 .sha256 与 .commit）
make release    # 打 tarball + SHA256SUMS
make smoke-release / smoke-cp / clean
```

协议改动流程：改 `proto/agent.proto` → `make proto` → 改 `internal/wire` → 同步 `cmd/mockcontrol`（第二实现）→ 更新 `DESIGN.md` 消息表 → `make e2e`。

## 附录 A：控制面最小契约

```
POST /api/v1/agent/enroll   → {node_id, node_secret, pin_sha256?, heartbeat_interval_seconds, …}
GET  /api/v1/agent/control  → WebSocket（Authorization: Bearer <自签 JWT> 或 ?key=node_secret）

# 控制通道不可用（404/405/501，例如 PHP 控制面）时走 REST 兜底：
POST /api/v1/agent/heartbeat                                          ← 心跳（含 config_version/config_sha256/config_path 现状）
GET  /api/v1/agent/config/default?current_version=N&current_sha256=X   ← 默认版本文件（与节点无关）
GET  /api/v1/agent/config/latest?current_version=N&current_sha256=X    ← 本节点发布的版本
POST /api/v1/agent/config/ack                                         ← {version, status: applied|failed, error}
```

* JWT claims：`iss=hysteria-agent`、`sub=<node_id>`、`aud=hysteria-control`、`exp/iat/nbf/jti`；用该节点 `node_secret` 做 HS256 验签。
* `Hello` → 回 `HelloAck{resend_from_seq: 已确认最大 seq+1, heartbeat_interval_seconds, telemetry_interval_seconds}`。
* 所有 `seq>0` 的消息按 `(node_id, seq)` **幂等入库**，回 `EventAck{acked_seq}`（累积确认）。
* 帧格式：WebSocket 二进制帧 = `Envelope` 的 protobuf（`proto/agent.proto`）。
* 取配置端点对「空数据」必须给安全默认（`up_to_date: true` + `config: null`）：老节点拿到只会照常跳过，不会乱动本地文件；同时**库里没有默认版本时要能退回单节点版本**，否则「面板发布了配置但节点一无所获」。

FastAPI 骨架（节选）：

```python
@app.post("/api/v1/agent/enroll")
def enroll(body: dict, x_enroll_token: str = Header(default="")):
    node = cp.consume_enroll_token(x_enroll_token or body["enroll_token"])   # 一次性令牌
    return {"node_id": node.id, "node_secret": node.secret, "pin_sha256": [cp.spki_pin],
            "heartbeat_interval_seconds": 30, "telemetry_interval_seconds": 15}

@app.websocket("/api/v1/agent/control")
async def control(ws: WebSocket, authorization: str = Header(default=""), x_node_id: str = Header(default="")):
    node = cp.get_node(x_node_id)
    jwt.decode(authorization.removeprefix("Bearer "), node.secret,
               algorithms=["HS256"], audience="hysteria-control", issuer="hysteria-agent")
    await ws.accept()
    async for raw in ws.iter_bytes():
        cp.handle(node, Envelope.FromString(raw))     # 幂等入库 + ack
```

参考实现：`cmd/mockcontrol`（假控制面，200 行级，含 REST 兜底面与测试驱动接口）、`cmd/controlplane`（完整实现）、`cmd/mockhysteria`（假 Hysteria）。

## 附录 B：升级签名公式

```
sha256_hex = hex(sha256(binary))
signature  = base64(ed25519_sign(private_key, sha256_hex))     # 对哈希的 hex 字符串签名
```

```bash
./bin/agent-sign keygen -out /etc/hysteria-agent/upgrade.key     # 打印 base64 公钥 → upgrade.pubkeys
./bin/agent-sign sign -key /etc/hysteria-agent/upgrade.key -file ./hysteria-agent \
  -version 0.2.0 -url https://cp.example.com/dl/hysteria-agent-0.2.0
# 输出的 JSON 直接作为 upgrade_agent 指令的 params
```

## 附录 C：自带控制面 / 桥接 / 反代

**内置控制面**（`cmd/controlplane`，开箱可用）：

```bash
bash install-controlplane.sh --public-url https://cp.example.com   # 装二进制 + 0600 配置（随机 admin_token 与主密钥）+ systemd + 现场验证
controlplane token new --name hk-node-1        # 一次性注册令牌
controlplane nodes | node <id> | events <id> --limit 50
controlplane push-config <id> /etc/hysteria/config.yaml --reason "调 masquerade" --wait 25s
controlplane command <id> run_diagnostic
controlplane delete-node <id>                  # 吊销：node_secret 由 master_key 派生，删记录即失效
```

设计要点：`node_secret = HMAC-SHA256(master_key, "node_secret:"+node_id)`（控制面不存密钥）；按 `(node_id, seq)` 幂等入库；配置下发 `ConfigPush → ConfigAck` 按 revision 幂等重发；状态在 `state_dir/state.json`(0600) 与 `events/<node>.jsonl`(0600)。管理侧接口：`GET /admin/nodes`、`…/{id}`、`POST …/{id}/config`、`POST …/{id}/commands?type=…`、`GET …/{id}/events|telemetry`、`POST /admin/tokens`、`DELETE …`；浏览器 `/` 是只读仪表盘。

**桥接模式**（Go 控制面只终结通道，身份/数据全落 Laravel 面板，见 [docs/BRIDGE-TO-CONTROL-PLANE.md](docs/BRIDGE-TO-CONTROL-PLANE.md)）：

```yaml
# /etc/hysteria-controlplane/config.yaml
identity: { authority: "laravel" }
laravel:  { base_url: "https://cp.example.com", enroll_path: "/api/v1/agent/enroll" }
poll:     { config_seconds: 2, commands_seconds: 10, fallback_only: true }
reverb:   { enabled: true, key: "<REVERB_APP_KEY>", host: "cp.example.com", port: 443, scheme: "https" }
```

注册被原样代理到 Laravel（同一个 `node_id` 立刻出现在面板里）；心跳/遥测/日志/诊断由控制面**转发**到 Laravel（per-node 后台队列，agent 侧永不阻塞）。自检：`go run ./cmd/cpapi-smoke -cp https://cp.example.com -email <admin> -password <pw>`。

**反代**（控制通道是长连接，必须转发 `Upgrade`/`Connection`）：

```yaml
# ~/.cloudflared/config.yml
ingress:
  - hostname: node.ipx.sarl
    path: /api/v1/agent/.*
    service: http://127.0.0.1:8443      # 控制面（WS 同路径，cloudflared 自动转发 Upgrade）
  - hostname: node.ipx.sarl
    service: http://127.0.0.1:8080      # 原有面板/站点
  - service: http_status:404
```

```nginx
location ~ ^/api/v1/agent/ {
    proxy_pass http://127.0.0.1:8443;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;
    proxy_set_header X-Node-ID $http_x_node_id;
}
```
