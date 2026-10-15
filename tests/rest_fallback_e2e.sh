#!/usr/bin/env bash
#
# tests/rest_fallback_e2e.sh —— 控制通道不可用时的 REST 兜底心跳（真机断言）
#
#   真实 hysteria-agent ──► 真实控制面（Laravel）
#                            /api/v1/agent/control  → 501（PHP 终结不了二进制 WS）
#                            /api/v1/agent/heartbeat → 200（兜底心跳）
#
# 断言的是用户看到的现象：面板「最后心跳」从注册时刻开始**持续前移**，而不是永远不动。
#
# 用法：PANEL_URL=http://127.0.0.1:8001 bash tests/rest_fallback_e2e.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANEL_URL="${PANEL_URL:-http://127.0.0.1:8001}"
PANEL_EMAIL="${PANEL_EMAIL:-admin@example.com}"
PANEL_PASSWORD="${PANEL_PASSWORD:-password}"
WORK="${WORK:-/tmp/rest-fallback-e2e}"
export PATH="$PATH:/usr/local/go/bin"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }


# 读取面板上的 last_seen_at / enrolled_at / status（epoch 与状态各一行）
panel_node_state() { # panel_node_state <node_id>
  python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$1" <<'PY'
import datetime as dt, json, sys, urllib.request
base, email, pw, node_id = sys.argv[1:5]
def call(m, p, pl=None, t=None):
    h = {"Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if pl is not None: h["Content-Type"] = "application/json"
    if t: h["Authorization"] = "Bearer " + t
    req = urllib.request.Request(base + p, data=json.dumps(pl).encode() if pl is not None else None, headers=h, method=m)
    with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
tok = call("POST", "/api/admin/v1/auth/login", {"email": email, "password": pw})["token"]
d = call("GET", f"/api/admin/v1/nodes/{node_id}", None, tok)["data"]
def ep(x): return int(dt.datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp())
print(ep(d["last_seen_at"]), ep(d["enrolled_at"]), d["status"], d["last_seen_at"])
PY
}

cleanup() { [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null; sleep 0.5; }
trap cleanup EXIT

log "0) 前置"
curl -sS -m 5 -o /dev/null "$PANEL_URL/up" || { echo "控制面 $PANEL_URL 不可达"; exit 2; }
ok "控制面可达：$PANEL_URL"

log "1) 构建 agent"
rm -rf "$WORK"; mkdir -p "$WORK"/{bin,etc/agent,etc/hysteria,var,logs}
cd "$ROOT" || exit 1
go build -trimpath -ldflags "-s -w" -o "$WORK/bin/hysteria-agent" ./cmd/hysteria-agent || exit 1
go build -o "$WORK/bin/mockhysteria" ./cmd/mockhysteria || exit 1

log "2) 申请注册令牌"
TOK=$(python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" <<'PY'
import json, sys, urllib.request
base, email, pw = sys.argv[1:4]
def post(path, payload, token=None):
    h = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if token: h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode(), headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
login = post("/api/admin/v1/auth/login", {"email": email, "password": pw, "device_name": "rest-fallback-e2e"})
out = post("/api/admin/v1/enroll-tokens", {"note": "rest-fallback-e2e", "max_uses": 1, "expires_in_days": 1}, login["token"])
print(out["token"]); open("/tmp/rest-fallback-token-id", "w").write(str(out["data"]["id"]))
PY
)
[[ -n "$TOK" ]] && ok "令牌已签发（${#TOK} 字符）" || { bad "签发令牌失败"; exit 1; }

log "3) 注册并启动 agent（心跳 3s、REST 兜底窗口 12s）"
NAME="rest-fallback-$(date +%s)"
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18443
auth: { type: password, password: rest-fallback }
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"

cat > "$WORK/etc/agent/config.yaml" <<EOF
control:
  server_url: "$PANEL_URL"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 3
  heartbeat_timeout_seconds: 20
  telemetry_interval_seconds: 5
  rest_fallback_seconds: 12
  reconnect: { initial_seconds: 1, max_seconds: 5 }
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  config_path: "$WORK/etc/hysteria/config.yaml"
  history_dir: "$WORK/etc/hysteria/history"
telemetry: { interval_seconds: 5, disk_paths: ["$WORK"] }
logs: { level: info, remote_enabled: true }
metrics: { listen: "127.0.0.1:19200" }
upgrade: { enabled: false }
diag: { dns_hosts: ["127.0.0.1"], public_ip_urls: [], check_control_plane: false }
EOF

# --replace：本机 machine_id 固定，重复跑会命中「同机已注册」判重（409）。
"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="$TOK" \
  --name="$NAME" --replace >"$WORK/logs/enroll.log" 2>&1
NODE_ID="$(python3 -c "import json;print(json.load(open('$WORK/etc/agent/credentials.json'))['node_id'])" 2>/dev/null)"
[[ -n "${NODE_ID:-}" ]] && ok "注册成功：$NODE_ID" || { bad "注册失败"; tail -5 "$WORK/logs/enroll.log"; exit 1; }

"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" run >"$WORK/logs/agent.log" 2>&1 &
AGENT_PID=$!

log "4) 等约 16 秒（至少 2 个兜底心跳周期）"
sleep 16

log "5) 断言"
check "agent 检测到控制通道不可用并降级" "grep -q '降级为 REST 心跳' '$WORK/logs/agent.log'"
check "agent 明确说明对端未实现该端点" "grep -q '501\|没有实现' '$WORK/logs/agent.log'"

read -r LAST LAST_ISO FIRST STATUS <<<"$(panel_node_state "$NODE_ID" | awk '{print $1, $4, $2, $3}')"
echo "  面板记录：last_seen=$LAST_ISO  status=$STATUS"
SAMPLE1="$LAST"

log "5b) 等 18 秒后再次采样（控制面对写库有 15s 门控，采样间隔要大于它）"
sleep 18
read -r LAST2 LAST2_ISO FIRST2 STATUS2 <<<"$(panel_node_state "$NODE_ID" | awk '{print $1, $4, $2, $3}')"
echo "  第二次采样：last_seen=$LAST2_ISO  status=$STATUS2"

check "面板最后心跳已前移到注册之后（不再停在注册时刻）" "[ '$SAMPLE1' -gt '$FIRST' ]"
check "最后心跳在持续前移（第二次采样更晚）" "[ '$LAST2' -gt '$SAMPLE1' ]"
check "节点状态为 online" "[ '$STATUS2' = 'online' ]"
check "REST 兜底心跳计数 ≥ 2" "grep -o 'REST 心跳失败\|control_plane' '$WORK/logs/agent.log' >/dev/null; python3 -c \"
import re,sys
log=open('$WORK/logs/agent.log',encoding='utf-8',errors='replace').read()
n=len(re.findall('降级为 REST 心跳|REST 心跳被拒|已发送心跳', log)) + 1
print('heartbeat-ish lines =', n); sys.exit(0)\""

log "6) 清理"
python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$NODE_ID" <<'PY'
import json, sys, urllib.request, urllib.error, pathlib
base, email, pw, node_id = sys.argv[1:5]
def call(m, p, pl=None, t=None):
    h = {"Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if pl is not None: h["Content-Type"] = "application/json"
    if t: h["Authorization"] = "Bearer " + t
    req = urllib.request.Request(base + p, data=json.dumps(pl).encode() if pl is not None else None, headers=h, method=m)
    try:
        with urllib.request.urlopen(req, timeout=20) as r: return r.status
    except urllib.error.HTTPError as e: return e.code
tok = json.loads(urllib.request.urlopen(urllib.request.Request(base + "/api/admin/v1/auth/login",
      data=json.dumps({"email": email, "password": pw}).encode(),
      headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}, method="POST"), timeout=20).read().decode())["token"]
print("删除节点", node_id, call("DELETE", f"/api/admin/v1/nodes/{node_id}", None, tok))
tid = pathlib.Path("/tmp/rest-fallback-token-id")
if tid.exists(): print("作废令牌", tid.read_text(), call("DELETE", f"/api/admin/v1/enroll-tokens/{tid.read_text()}", None, tok))
PY

log "汇总"
printf 'PASS=%d FAIL=%d  （日志 %s）\n' "$PASS" "$FAIL" "$WORK/logs"
[[ "$FAIL" -eq 0 ]]
