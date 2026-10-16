#!/usr/bin/env bash
#
# tests/auto_converge_e2e.sh —— 节点自动发现「与面板的版本不一样」→ 自动更新为默认版本 → 重启 Hysteria
#
# 需求（用户原话）：节点自动发现与 plane 的版不一样，并自动更新为默认版本，并重启 hysteria。
#
# 真实链路：真 hysteria-agent ──► mockcontrol（-rest-only：/control 返回 501，对齐真实 Laravel 面板）
#                                 /api/v1/agent/config/default  ← 配置版本文件库的默认版本
#           agent 轮询发现本机 sha256 ≠ 默认版本 sha256 → 取回正文 → 原子写 0600
#                                               → 校验 → 重启被守护的 Hysteria（mockhysteria）
#
# 断言两轮收敛：① 首次（本机是手工配置）② 面板改了默认版本后（v2 → v3）再次自动跟上。
#
# 用法：bash tests/auto_converge_e2e.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK:-/tmp/auto-converge-e2e}"
export PATH="$PATH:/usr/local/go/bin"
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }
sha()  { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }

cleanup() {
  [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null
  [[ -n "${CP_PID:-}" ]] && kill "$CP_PID" 2>/dev/null
  sleep 0.5
  return 0
}
trap cleanup EXIT

log "0) 构建 agent / mockcontrol / mockhysteria"
rm -rf "$WORK"; mkdir -p "$WORK"/{bin,etc/agent,etc/hysteria,var,logs,certs}
cd "$ROOT" || exit 1
go build -trimpath -o "$WORK/bin/hysteria-agent" ./cmd/hysteria-agent || exit 1
go build -o "$WORK/bin/mockcontrol" ./cmd/mockcontrol || exit 1
go build -o "$WORK/bin/mockhysteria" ./cmd/mockhysteria || exit 1
ok "三个二进制构建完成"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=auto-converge" \
  -keyout "$WORK/certs/server.key" -out "$WORK/certs/server.crt" >/dev/null 2>&1

# 面板侧「配置版本文件库的默认版本」（第一版）
cat > "$WORK/plane-default-v1.yaml" <<EOF
listen: 127.0.0.1:18501
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: plane-default-v1
trafficStats:
  listen: 127.0.0.1:18502
  secret: e2e-stats-secret
# marker: plane-default-v1
EOF

log "1) 起 mockcontrol（-rest-only，模拟真实面板：WS 501 + REST 配置面）"
CP_PORT="${CP_PORT:-18490}"
"$WORK/bin/mockcontrol" -listen "127.0.0.1:$CP_PORT" -enroll-token "auto-converge-token" \
  -rest-only -default-config "$WORK/plane-default-v1.yaml" -dump "$WORK/logs/dump.jsonl" \
  >"$WORK/logs/mockcontrol.log" 2>&1 &
CP_PID=$!
for _ in $(seq 1 40); do curl -sS -m 2 "http://127.0.0.1:$CP_PORT/healthz" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sS -m 3 "http://127.0.0.1:$CP_PORT/_test/state" >/dev/null || { echo "mockcontrol 没起来"; exit 2; }
ok "mockcontrol 在 127.0.0.1:$CP_PORT（WS 面返回 501，配置只走 REST）"

PLANE_SHA1="$(sha "$WORK/plane-default-v1.yaml")"

log "2) 本机先放一份**与面板不一样**的配置（模拟人工改过 / 旧版本）"
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18503
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: local-before
# marker: local-before
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"
LOCAL_SHA="$(sha "$WORK/etc/hysteria/config.yaml")"
[[ "$LOCAL_SHA" != "$PLANE_SHA1" ]] && ok "本机与面板默认版本确实不一样（sha 不同）" || bad "夹具没造对：两边 sha 相同"

cat > "$WORK/etc/agent/config.yaml" <<EOF
control:
  server_url: "http://127.0.0.1:$CP_PORT"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 2
  rest_fallback_seconds: 120
  config_poll_seconds: 1
  config_source: default
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
enroll:
  machine_id: "auto-converge-$$-$(date +%s)"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  config_path: "$WORK/etc/hysteria/config.yaml"
  history_dir: "$WORK/etc/hysteria/history"
  api_base_url: "http://127.0.0.1:18502"
  api_secret: "e2e-stats-secret"
logs: { level: info, remote_enabled: false }
metrics: { listen: "127.0.0.1:19221" }
upgrade: { enabled: false }
EOF

log "3) 注册 + 启动守护进程"
"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="auto-converge-token" --name="auto-converge-$$" >"$WORK/logs/enroll.log" 2>&1
[[ -f "$WORK/etc/agent/credentials.json" ]] && ok "注册成功" || { bad "注册失败：$(tail -3 "$WORK/logs/enroll.log")"; exit 1; }

export MOCK_HYSTERIA_START_LOG="$WORK/logs/mock-starts.log"; : > "$MOCK_HYSTERIA_START_LOG"
MOCK_HYSTERIA_START_LOG="$MOCK_HYSTERIA_START_LOG" \
  "$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" run >"$WORK/logs/agent.log" 2>&1 &
AGENT_PID=$!

wait_for() { # wait_for <期望 sha> <秒数>
  local want="$1" limit="${2:-60}" i=0
  while [[ $i -lt $limit ]]; do
    [[ "$(sha "$WORK/etc/hysteria/config.yaml")" == "$want" ]] && return 0
    sleep 1
    i=$((i+1))
  done
  return 1
}

log "4) 断言：自动发现不一致 → 更新为面板默认版本 → 重启 Hysteria"
if wait_for "$PLANE_SHA1" 60; then ok "本机配置已自动跟上面板默认版本（sha=$PLANE_SHA1）"; else
  bad "60s 内没跟上：本机 sha=$(sha "$WORK/etc/hysteria/config.yaml")  期望 $PLANE_SHA1"; fi
check "落盘权限仍是 600" "[ \"\$(stat -c %a '$WORK/etc/hysteria/config.yaml')\" = '600' ]"
check "内容就是面板那份" "grep -q 'marker: plane-default-v1' '$WORK/etc/hysteria/config.yaml'"
STARTS1="$(wc -l < "$MOCK_HYSTERIA_START_LOG")"
check "Hysteria 被（重新）拉起过：启动记录 ≥2 次（首次 + 换配置后）" "[ \"$STARTS1\" -ge 2 ]"
check "agent 日志里报了来源" "grep -qE 'REST 兜底：收到新配置|配置已更新|来源' '$WORK/logs/agent.log'"
sleep 2
check "控制面收到了配置回执（applied）" "curl -sS -m 3 'http://127.0.0.1:$CP_PORT/_test/state' | grep -q 'applied'"

log "5) 面板把默认版本改成 v2 → 节点应当再次自动跟上并重启"
sed 's/plane-default-v1/plane-default-v2/g; s/18501/18511/' "$WORK/plane-default-v1.yaml" > "$WORK/plane-default-v2.yaml"
curl -sS -m 5 -X POST --data-binary "@$WORK/plane-default-v2.yaml" "http://127.0.0.1:$CP_PORT/_test/set_default_config" >/dev/null
PLANE_SHA2="$(sha "$WORK/plane-default-v2.yaml")"
if wait_for "$PLANE_SHA2" 60; then ok "改动默认版本后节点自动跟上（sha=$PLANE_SHA2）"; else
  bad "60s 内没跟上 v2：本机 sha=$(sha "$WORK/etc/hysteria/config.yaml")"; fi
check "内容是 v2 那份" "grep -q 'marker: plane-default-v2' '$WORK/etc/hysteria/config.yaml'"
STARTS2="$(wc -l < "$MOCK_HYSTERIA_START_LOG")"
check "又重启了一次 Hysteria（启动记录增加）" "[ \"$STARTS2\" -gt \"$STARTS1\" ]"
check "历史里留了可回滚的快照" "ls '$WORK/etc/hysteria/history' 2>/dev/null | grep -q ."
check "MOCK_HYSTERIA_START_LOG 明细" "[ -s '$MOCK_HYSTERIA_START_LOG' ]"

echo
printf '结果：%d 通过 / %d 失败\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
