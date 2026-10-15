#!/usr/bin/env bash
# smoke_release.sh —— 用"发布包里的二进制"真跑一遍，证明解压即用。
#
# 覆盖：version / enroll（凭据 0400）/ 长连接 + 心跳 /healthz /readyz /metrics /
#       配置下发（真实写盘并重启 Hysteria）/ 远程指令 run_diagnostic / 优雅停止。
#
# 用法：make release && make smoke-release   （或直接 bash tests/smoke_release.sh）
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${SMOKE_WORK:-/var/lib/hysteria-agent-smoke}"
CP_PORT=18085
HY_LISTEN="127.0.0.1:18543"
HY_API=18084
METRICS=19101
API_SECRET="smoke-stats-secret"
TOKEN="smoke-enroll-token"

PASS=0; FAIL=0
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
check(){ local d="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi; }
log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

AGENT_PID=""; CP_PID=""
cleanup() {
  [[ -n "$AGENT_PID" ]] && kill "$AGENT_PID" 2>/dev/null
  [[ -n "$CP_PID" ]] && kill "$CP_PID" 2>/dev/null
  sleep 1
  pkill -9 -f "$WORK/pkg/bin/[m]ockhysteria" 2>/dev/null
  sleep 0.5
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- 解包
log "解包发布产物到 $WORK"
TARBALL="$(ls -1 "$ROOT"/release/hysteria-agent-*-linux-amd64.tar.gz 2>/dev/null | head -1)"
[[ -n "$TARBALL" ]] || { echo "找不到发布包，请先 make release"; exit 1; }
echo "使用发布包: $TARBALL"
( cd "$ROOT/release" && sha256sum -c SHA256SUMS --ignore-missing >/dev/null 2>&1 ) \
  && ok "发布包 sha256 校验通过" || bad "发布包 sha256 校验失败"

rm -rf "$WORK"; mkdir -p "$WORK/pkgdir"
tar -xzf "$TARBALL" -C "$WORK/pkgdir"
PKG="$(find "$WORK/pkgdir" -maxdepth 1 -mindepth 1 -type d | head -1)"
mv "$PKG" "$WORK/pkg"
BIN="$WORK/pkg/bin"
mkdir -p "$WORK/etc" "$WORK/var"

check "包内有 agent 二进制" test -x "$BIN/hysteria-agent"
check "包内有 install.sh" test -f "$WORK/pkg/install.sh"
check "包内有 systemd 单元模板" test -f "$WORK/pkg/deploy/hysteria-agent.service"
check "包内有配置模板" test -f "$WORK/pkg/deploy/config.yaml"
check "包内有文档" test -f "$WORK/pkg/README.md"

log "二进制自检（静态可执行）"
"$BIN/hysteria-agent" version || bad "version 失败"
VER_OUT="$("$BIN/hysteria-agent" version 2>&1)"
check "版本号正确（$(echo "$VER_OUT" | head -1)）" bash -c "echo '$VER_OUT' | grep -q '0\.1\.0'"
check "agent 二进制是静态链接（无动态依赖）" bash -c "! ldd '$BIN/hysteria-agent' 2>&1 | grep -q 'libc'"
check "agent-sign 可用" bash -c "'$BIN/agent-sign' pubkey -key /dev/null 2>&1 | grep -qi 'base64\|私钥\|错误'"

# ---------------------------------------------------------------- 造一份 Hysteria 配置（假 Hysteria 用）
log "准备假 Hysteria 与 Agent 配置"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$WORK/etc/key.pem" -out "$WORK/etc/cert.pem" -days 30 \
  -subj "/CN=smoke.example.com" >/dev/null 2>&1
cat > "$WORK/etc/hysteria.yaml" <<EOF
listen: $HY_LISTEN
tls:
  cert: $WORK/etc/cert.pem
  key: $WORK/etc/key.pem
auth:
  type: password
  password: smoke-password-123456
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:$HY_API
  secret: $API_SECRET
EOF

cat > "$WORK/etc/agent.yaml" <<EOF
control:
  server_url: "http://127.0.0.1:$CP_PORT"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 3
  heartbeat_timeout_seconds: 20
  telemetry_interval_seconds: 2
  reconnect:
    initial_seconds: 1
    max_seconds: 5
credentials_file: "$WORK/etc/credentials.json"
state_dir: "$WORK/var"
hysteria:
  mode: process
  binary: "$BIN/mockhysteria"
  config_path: "$WORK/etc/hysteria.yaml"
  api_base_url: "http://127.0.0.1:$HY_API"
  api_secret: "$API_SECRET"
  history_dir: "$WORK/etc/history"
  startup_timeout_seconds: 10
  shutdown_timeout_seconds: 5
  restart:
    initial_seconds: 1
    max_seconds: 5
telemetry:
  interval_seconds: 2
  disk_paths: ["$WORK"]
logs:
  level: info
metrics:
  listen: "127.0.0.1:$METRICS"
outbox:
  path: "$WORK/var/outbox.jsonl"
upgrade:
  enabled: false
diag:
  dns_hosts: ["127.0.0.1"]
  public_ip_urls: []
  check_control_plane: false
EOF

log "启动假控制面并注册节点"
"$BIN/mockcontrol" -listen "127.0.0.1:$CP_PORT" -enroll-token "$TOKEN" \
  -dump "$WORK/var/dump.jsonl" -heartbeat-interval 3 -telemetry-interval 2 \
  > "$WORK/mock.log" 2>&1 &
CP_PID=$!
sleep 1
check "假控制面就绪" curl -sf "http://127.0.0.1:$CP_PORT/healthz"

"$BIN/hysteria-agent" enroll -config "$WORK/etc/agent.yaml" --token="$TOKEN" --name=smoke-node \
  > "$WORK/enroll.log" 2>&1
check "enroll 成功" grep -q "注册成功" "$WORK/enroll.log"
check "凭据权限 0400" test "$(stat -c %a "$WORK/etc/credentials.json")" = "400"

log "启动 Agent 并等待就绪"
"$BIN/hysteria-agent" -config "$WORK/etc/agent.yaml" run > "$WORK/agent.log" 2>&1 &
AGENT_PID=$!
for i in $(seq 1 40); do
  curl -sf "http://127.0.0.1:$METRICS/readyz" >/dev/null 2>&1 && break
  sleep 0.5
done
check "/healthz 返回 200" curl -sf "http://127.0.0.1:$METRICS/healthz"
check "/readyz 返回 200（Hysteria 已被拉起且配置已应用）" curl -sf "http://127.0.0.1:$METRICS/readyz"
check "/metrics 有 agent 指标" bash -c "curl -s http://127.0.0.1:$METRICS/metrics | grep -q hysteria_agent_info"
# state.json 每 5s 刷新一次，这里等一下再断言（单次检查会踩到首帧）
STATE_OK=1
for i in $(seq 1 30); do
  if python3 -c "import json,sys;d=json.load(open('$WORK/var/state.json'));sys.exit(0 if d['control']['connected'] and d['hysteria']['running'] else 1)" 2>/dev/null; then
    STATE_OK=0; break
  fi
  sleep 0.5
done
check "state.json 显示控制通道已连、Hysteria 在跑" test "$STATE_OK" = "0"
check "心跳已到达假控制面" bash -c "grep -q 'Hello:' '$WORK/mock.log'"

log "配置下发（真实写盘 + 重启 Hysteria）"
REV="smoke-$(date +%s)"
sed 's/smoke-password-123456/smoke-password-abcdef/' "$WORK/etc/hysteria.yaml" > "$WORK/push.yaml"
curl -sf -X POST --data-binary @"$WORK/push.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=$REV" >/dev/null
sleep 6
check "目标配置已更新（0600）" test "$(stat -c %a "$WORK/etc/hysteria.yaml")" = "600"
check "配置内容为新版本" grep -q 'smoke-password-abcdef' "$WORK/etc/hysteria.yaml"
check "配置回执成功" bash -c "grep -q \"配置回执: revision=$REV ok=true\" '$WORK/mock.log'"
check "配置历史已记录" bash -c "grep -q '$REV' '$WORK/etc/history/index.json'"

log "远程指令 run_diagnostic"
curl -sf -X POST -d '{}' "http://127.0.0.1:$CP_PORT/_test/command?type=run_diagnostic&id=smoke-diag" >/dev/null
sleep 8
check "诊断指令返回 ok" bash -c "grep -q 'type=run_diagnostic status=ok' '$WORK/mock.log'"
check "诊断报告已落盘" test -f "$WORK/var/last-diagnostic.json"
check "诊断里 Hysteria 进程正常" bash -c \
  "python3 -c \"import json;d=json.load(open('$WORK/var/last-diagnostic.json'));c={x['name']:x['status'] for x in d['checks']};assert c['hysteria_process']=='ok' and c['hysteria_api']=='ok'\""

log "CLI 子命令"
check "status 可用" bash -c "'$BIN/hysteria-agent' status -config '$WORK/etc/agent.yaml' | grep -q node_id"
check "config history 可用" bash -c "'$BIN/hysteria-agent' config history -config '$WORK/etc/agent.yaml' | grep -q '$REV'"

log "优雅停止"
kill -TERM "$AGENT_PID"
for i in $(seq 1 40); do kill -0 "$AGENT_PID" 2>/dev/null || break; sleep 0.5; done
check "Agent 已退出" bash -c "! kill -0 $AGENT_PID 2>/dev/null"
check "Hysteria 子进程未残留" bash -c "! pgrep -f '$WORK/pkg/bin/[m]ockhysteria server' >/dev/null"
AGENT_PID=""

log "汇总"
echo "PASS=$PASS FAIL=$FAIL   （工作目录 $WORK）"
[[ $FAIL -eq 0 ]] || { echo "--- agent 日志尾部 ---"; tail -15 "$WORK/agent.log"; exit 1; }
