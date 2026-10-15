#!/usr/bin/env bash
#
# tests/bridge_e2e.sh —— 桥接真机验证（S4）
#
#   真实 hysteria-agent ──Envelope WS──▶ Go 控制面(identity.authority=laravel)
#                                          └──HMAC REST──▶ 真实 Laravel 控制面/面板
#
# 断言：agent 经桥接注册后，Laravel 面板能看到 同一个 node_id、在线心跳、指标曲线与日志。
#
# 用法：
#   PANEL_URL=http://127.0.0.1:8001 bash tests/bridge_e2e.sh
#   PANEL_URL=http://127.0.0.1:8001 PANEL_DB=/path/to/database.sqlite bash tests/bridge_e2e.sh  # 额外校验日志入库
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANEL_URL="${PANEL_URL:-http://127.0.0.1:8001}"
PANEL_EMAIL="${PANEL_EMAIL:-admin@example.com}"
PANEL_PASSWORD="${PANEL_PASSWORD:-password}"
PANEL_DB="${PANEL_DB:-}"

CP_PORT="${CP_PORT:-8443}"
AGENT_METRICS_PORT="${AGENT_METRICS_PORT:-19100}"
WORK="${WORK:-/tmp/bridge-e2e}"
CP_STATE="$WORK/cpstate"
NODE_NAME="bridge-e2e-$(date +%s)"
export PATH="$PATH:/usr/local/go/bin"

PASS=0; FAIL=0
log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }

cleanup() {
  [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null
  [[ -n "${CP_PID:-}" ]] && kill "$CP_PID" 2>/dev/null
  pkill -f "$WORK/bin/mockhysteria" 2>/dev/null
  sleep 0.5
}
trap cleanup EXIT

log "0) 前置检查"
if ! curl -sS -m 5 -o /dev/null "$PANEL_URL/up" 2>/dev/null; then
  echo "控制面（Laravel）$PANEL_URL 不可达；先启动它再跑本脚本（hysteria-control-plane: php artisan serve）"
  exit 2
fi
ok "Laravel 控制面可达：$PANEL_URL"
command -v python3 >/dev/null || { echo "需要 python3"; exit 2; }

log "1) 构建（agent / mockhysteria / 控制面）"
rm -rf "$WORK"; mkdir -p "$WORK"/{bin,etc/agent,etc/hysteria,var,logs,files}
cd "$ROOT" || exit 1
go build -o "$WORK/bin/mockhysteria" ./cmd/mockhysteria || exit 1
go build -o "$WORK/bin/controlplane" ./cmd/controlplane || exit 1
go build -trimpath -ldflags "-s -w -X github.com/getthelostcode/hysteria-agent/internal/version.Version=0.1.0-bridge" \
  -o "$WORK/bin/hysteria-agent" ./cmd/hysteria-agent || exit 1
ok "构建完成：$(ls "$WORK/bin" | tr '\n' ' ')"

log "2) 从面板申请一次性注册令牌"
TOKEN=$(python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" <<'PY'
import json, sys, urllib.request
base, email, pw = sys.argv[1], sys.argv[2], sys.argv[3]
def post(path, payload, token=None):
    h = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "bridge-e2e/1.0"}
    if token: h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode(), headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())
login = post("/api/admin/v1/auth/login", {"email": email, "password": pw, "device_name": "bridge-e2e"})
issued = post("/api/admin/v1/enroll-tokens", {"note": "bridge e2e", "max_uses": 1, "expires_in_days": 1}, login["token"])
print(issued["token"])
PY
)
[[ -n "$TOKEN" ]] && ok "注册令牌：${TOKEN:0:12}…" || { bad "申请注册令牌失败"; exit 1; }

log "3) 启动 Go 控制面（identity.authority=laravel）"
cat > "$WORK/cp.yaml" <<EOF
listen: "127.0.0.1:$CP_PORT"
state_dir: "$CP_STATE"
admin_token: "adm_bridge_e2e"
public_url: "http://127.0.0.1:$CP_PORT"
enroll_path: "/api/v1/agent/enroll"
ws_path: "/api/v1/agent/control"
identity:
  authority: "laravel"
laravel:
  base_url: "$PANEL_URL"
  enroll_path: "/api/v1/agent/enroll"
  request_timeout_seconds: 10
poll: { config_seconds: 2, commands_seconds: 10, fallback_only: true }
reverb: { enabled: false }
bridge: { gzip_metrics: true, max_metric_batch: 500 }
EOF
if ss -lntp 2>/dev/null | grep -q ":$CP_PORT\b"; then
  bad "端口 $CP_PORT 已被占用（可能是上一轮残留的控制面）；请先释放再跑，否则断言会测到旧实例"
  exit 1
fi
"$WORK/bin/controlplane" serve -config "$WORK/cp.yaml" >"$WORK/logs/controlplane.log" 2>&1 &
CP_PID=$!
for _ in $(seq 1 30); do curl -sS -m 2 -o /dev/null "http://127.0.0.1:$CP_PORT/healthz" 2>/dev/null && break; sleep 0.3; done
check "控制面健康检查通过" "curl -sS -m 3 -o /dev/null http://127.0.0.1:$CP_PORT/healthz"
check "控制面确认走 Laravel 身份权威" "grep -q '身份权威：laravel' '$WORK/logs/controlplane.log'"

log "4) Agent 经控制面注册 + 起控制通道"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$WORK/etc/hysteria/key.pem" -out "$WORK/etc/hysteria/cert.pem" -days 5 -subj "/CN=bridge.e2e" >/dev/null 2>&1
API_SECRET="bridge-e2e-$RANDOM"
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18443
tls:
  cert: $WORK/etc/hysteria/cert.pem
  key: $WORK/etc/hysteria/key.pem
auth:
  type: password
  password: bridge-e2e-password
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:19999
  secret: $API_SECRET
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"

cat > "$WORK/etc/agent/config.yaml" <<EOF
control:
  server_url: "http://127.0.0.1:$CP_PORT"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 3
  heartbeat_timeout_seconds: 20
  telemetry_interval_seconds: 2
  reconnect: { initial_seconds: 1, max_seconds: 5 }
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  config_path: "$WORK/etc/hysteria/config.yaml"
  api_base_url: "http://127.0.0.1:19999"
  api_secret: "$API_SECRET"
  history_dir: "$WORK/etc/agent/history"
  history_keep: 5
  startup_timeout_seconds: 10
  shutdown_timeout_seconds: 5
  restart: { initial_seconds: 1, max_seconds: 5 }
  restart_max_in_window: 5
  restart_window_seconds: 60
telemetry:
  interval_seconds: 2
  disk_paths: ["$WORK"]
logs:
  level: info
  remote_enabled: true
metrics:
  listen: "127.0.0.1:$AGENT_METRICS_PORT"
outbox:
  path: "$WORK/var/outbox.jsonl"
upgrade:
  enabled: false
diag:
  dns_hosts: ["127.0.0.1"]
  public_ip_urls: []
  check_control_plane: false
EOF

# --replace：本机 machine_id 固定，重复跑会命中 Laravel 的「同机已注册」判重（409）。
# 这也是运维重装后的常规路径：同一个 node_id 轮换密钥，而不是新建僵尸节点。
"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="$TOKEN" \
  --name="$NODE_NAME" --replace >"$WORK/logs/enroll.log" 2>&1
check "Agent 注册成功（经 Go 控制面代理到 Laravel）" "[ -s '$WORK/etc/agent/credentials.json' ]"
check "credentials.json 权限 0400" "[ \"\$(stat -c %a '$WORK/etc/agent/credentials.json')\" = '400' ]"

"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" run >"$WORK/logs/agent.log" 2>&1 &
AGENT_PID=$!

# 等控制通道建立 + 至少两轮心跳/遥测（agent 侧 3s/2s）
sleep 12
check "Agent 控制通道握手完成" "grep -q '握手完成' '$WORK/logs/agent.log'"

log "5) 面板侧断言（这是 S4 的验收口径）"
# 注意：用 credentials.json 里的 node_id 查，而不是节点名 —— enroll --replace 走的是
# 「同一个节点轮换凭据」，面板侧保留原名字。
NODE_ID="$(python3 -c "import json;print(json.load(open('$WORK/etc/agent/credentials.json'))['node_id'])")"
read -r STATUS PROTOCOL <<<"$(python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$NODE_ID" <<'PY'
import json, sys, urllib.request
base, email, pw, node_id = sys.argv[1:5]
def call(method, path, payload=None, token=None):
    h = {"Accept": "application/json", "User-Agent": "bridge-e2e/1.0"}
    if payload is not None: h["Content-Type"] = "application/json"
    if token: h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode() if payload is not None else None, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())
tok = call("POST", "/api/admin/v1/auth/login", {"email": email, "password": pw})["token"]
try:
    n = call("GET", f"/api/admin/v1/nodes/{node_id}", None, tok)["data"]
except Exception:
    print("- -"); raise SystemExit(0)
print(n["status"], (n.get("meta") or {}).get("protocol") or "-")
PY
)"
[[ -n "${STATUS:-}" && "$STATUS" != "-" ]] && ok "面板可见该节点：$NODE_ID（status=$STATUS）" || bad "面板未找到节点 $NODE_ID"
check "节点状态 online（心跳已被桥接转发）" "[ '${STATUS:-}' = 'online' ]"

check "面板登记通道为 bridge/v1" "[ '${PROTOCOL:-}' = 'bridge/v1' ]"

check "面板能看到指标曲线（遥测已转发）" "python3 - '$PANEL_URL' '$PANEL_EMAIL' '$PANEL_PASSWORD' '$NODE_ID' <<'PY'
import json, sys, time, urllib.request
base, email, pw, node_id = sys.argv[1:5]
def call(method, path, payload=None, token=None):
    h = {'Accept': 'application/json', 'User-Agent': 'bridge-e2e/1.0'}
    if payload is not None: h['Content-Type'] = 'application/json'
    if token: h['Authorization'] = 'Bearer ' + token
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode() if payload is not None else None, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())
tok = call('POST', '/api/admin/v1/auth/login', {'email': email, 'password': pw})['token']
for _ in range(20):   # 最多等 20s
    series = call('GET', f'/api/admin/v1/nodes/{node_id}/metrics?range=1h', None, tok).get('series', [])
    if series and any(s.get('mem') is not None for s in series):
        print('samples=%d cpu=%s mem=%s' % (len(series), series[-1].get('cpu'), series[-1].get('mem')))
        sys.exit(0)
    time.sleep(1)
sys.exit(1)
PY"

check "控制面桥接计数在增长（心跳/指标/日志）" "python3 - <<'PY'
import re, sys, urllib.request
text = urllib.request.urlopen('http://127.0.0.1:$CP_PORT/metrics', timeout=10).read().decode()
def val(kind):
    m = re.search(r'cp_bridge_total\{kind=\"%s\"\} (\d+)' % kind, text)
    return int(m.group(1)) if m else 0
hb, mt, lg = val('heartbeat'), val('metrics'), val('logs')
print('heartbeat=%d metrics=%d logs=%d' % (hb, mt, lg))
sys.exit(0 if hb > 0 and mt > 0 and lg > 0 else 1)
PY"

if [[ -n "$PANEL_DB" ]]; then
  check "节点日志已入库（node_logs）" "python3 - '$PANEL_DB' '$NODE_ID' <<'PY'
import sqlite3, sys
db, node_id = sys.argv[1], sys.argv[2]
con = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
row = con.execute('select count(*) from node_logs where node_id = (select id from nodes where node_id = ?)', (node_id,)).fetchone()
print('node_logs rows =', row[0])
sys.exit(0 if row and row[0] > 0 else 1)
PY"
fi

log "汇总"
printf 'PASS=%d FAIL=%d\n' "$PASS" "$FAIL"
printf '日志：%s\n' "$WORK/logs"
[[ "$FAIL" -eq 0 ]]
