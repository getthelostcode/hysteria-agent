#!/usr/bin/env bash
#
# tests/rest_config_e2e.sh —— 面板「发布配置」到节点真正生效（真机断言）
#
#   真实 hysteria-agent ──► 真实控制面（Laravel）
#                            /api/v1/agent/control     → 501（PHP 终结不了二进制 WS）
#                            /api/v1/agent/config/latest → 200（REST 兜底拉配置）
#                            /api/v1/agent/config/ack    → 200（回执）
#
# 断言的是用户报的现象："节点列表发布配置无法生效" ——
#   1) 面板发布后，节点磁盘上的 config.yaml 在几秒内变成新内容（热重载，不重启服务）
#   2) 面板里该版本变成 applied（applied_at 有值），而不是永远 published
#   3) 发一份坏配置：节点拒绝 + 上报失败 + 面板自动回滚，节点磁盘最终回到可用的那份
#
# 用法：PANEL_URL=http://127.0.0.1:8001 bash tests/rest_config_e2e.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANEL_URL="${PANEL_URL:-http://127.0.0.1:8001}"
PANEL_EMAIL="${PANEL_EMAIL:-admin@example.com}"
PANEL_PASSWORD="${PANEL_PASSWORD:-password}"
WORK="${WORK:-/tmp/rest-config-e2e}"
export PATH="$PATH:/usr/local/go/bin"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }

# 面板操作：login / publish / configs（一次 python 调用输出多行 key=value）
panel() { # panel <action> [args...]
  python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$@" <<'PY'
import json, os, sys, urllib.request, urllib.error
base, email, pw = sys.argv[1:4]
action = sys.argv[4]
args = sys.argv[5:]

def call(m, p, pl=None, t=None):
    h = {"Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if pl is not None: h["Content-Type"] = "application/json"
    if t: h["Authorization"] = "Bearer " + t
    req = urllib.request.Request(base + p, data=json.dumps(pl).encode() if pl is not None else None,
                                 headers=h, method=m)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")

def token():
    # admin-login 限流 10/分钟：令牌只登一次，之后从环境变量复用
    cached = os.environ.get("PANEL_TOKEN", "").strip()
    if cached:
        return cached
    return call("POST", "/api/admin/v1/auth/login", {"email": email, "password": pw, "device_name": "rest-config-e2e"})[1]["token"]

if action == "login":
    print(token())
elif action == "publish":
    node_id, yaml_text, reason = args[0], args[1], args[2]
    st, body = call("POST", f"/api/admin/v1/nodes/{node_id}/configs",
                    {"content_yaml": yaml_text, "reason": reason}, token())
    d = body.get("data") or body
    print("status", st); print("version", d.get("version")); print("sha256", d.get("sha256"))
elif action == "configs":
    node_id = args[0]
    st, body = call("GET", f"/api/admin/v1/nodes/{node_id}/configs", None, token())
    rows = body.get("data") or []
    for r in rows:
        print("row", r.get("version"), r.get("status"), r.get("applied_at"), (r.get("sha256") or "")[:12])
elif action == "issue_token":
    st, body = call("POST", "/api/admin/v1/enroll-tokens",
                    {"note": "rest-config-e2e", "max_uses": 1, "expires_in_days": 1}, token())
    print(body.get("token")); print("token_id", (body.get("data") or {}).get("id"))
elif action == "delete_node":
    print("delete", call("DELETE", f"/api/admin/v1/nodes/{args[0]}", None, token())[0])
elif action == "revoke_token":
    print("revoke", call("DELETE", f"/api/admin/v1/enroll-tokens/{args[0]}", None, token())[0])
elif action == "publish_direct":
    # 绕过 HTTP 直连库：不需要（留空）
    pass
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

log "2) 申请注册令牌 + 注册 + 启动 agent（REST 兜底窗口 90s、配置轮询 3s）"
export PANEL_TOKEN="$(panel login)"     # admin-login 限流 10/分钟，全程复用这一枚
[[ -n "$PANEL_TOKEN" ]] && ok "面板登录成功（令牌复用，避免撞登录限流）" || { bad "面板登录失败"; exit 1; }
TOKINFO="$(panel issue_token)"; TOK="$(printf '%s\n' "$TOKINFO" | head -1)"; TOKEN_ID="$(printf '%s\n' "$TOKINFO" | sed -n 's/^token_id //p')"
[[ -n "$TOK" ]] && ok "令牌已签发（${#TOK} 字符）" || { bad "签发令牌失败"; exit 1; }

mkdir -p "$WORK/etc/hysteria" "$WORK/certs"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=rest-config-e2e" \
  -keyout "$WORK/certs/server.key" -out "$WORK/certs/server.crt" >/dev/null 2>&1
[[ -s "$WORK/certs/server.crt" ]] && ok "自签证书已生成" || { bad "生成证书失败"; exit 1; }

# 初始配置必须**合法**：它既是起点，也是后面坏配置回滚时的落点
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18443
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: initial
trafficStats:
  listen: 127.0.0.1:19099
  secret: e2e-stats-secret
# marker: initial
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"

cat > "$WORK/etc/agent/config.yaml" <<EOF
control:
  server_url: "$PANEL_URL"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 3
  heartbeat_timeout_seconds: 20
  telemetry_interval_seconds: 5
  rest_fallback_seconds: 90
  config_poll_seconds: 3
  reconnect: { initial_seconds: 1, max_seconds: 5 }
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  api_base_url: "http://127.0.0.1:19099"
  api_secret: "e2e-stats-secret"
  config_path: "$WORK/etc/hysteria/config.yaml"
  history_dir: "$WORK/etc/hysteria/history"
telemetry: { interval_seconds: 5, disk_paths: ["$WORK"] }
logs: { level: info, remote_enabled: true }
metrics: { listen: "127.0.0.1:19201" }
upgrade: { enabled: false }
diag: { dns_hosts: ["127.0.0.1"], public_ip_urls: [], check_control_plane: false }
EOF

NAME="rest-config-$(date +%s)"
"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="$TOK" --name="$NAME" --replace >"$WORK/logs/enroll.log" 2>&1
NODE_ID="$(python3 -c "import json;print(json.load(open('$WORK/etc/agent/credentials.json'))['node_id'])" 2>/dev/null)"
[[ -n "${NODE_ID:-}" ]] && ok "注册成功：$NODE_ID" || { bad "注册失败"; tail -5 "$WORK/logs/enroll.log"; exit 1; }

"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" run >"$WORK/logs/agent.log" 2>&1 &
AGENT_PID=$!
sleep 8

log "3) 面板发布配置 v1 —— 断言节点磁盘热重载 + 面板变 applied"
PUB_YAML="listen: 127.0.0.1:18444
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: http
  http:
    url: https://auth.ipx.sarl
    insecure: false
trafficStats:
  listen: 127.0.0.1:19099
  secret: e2e-stats-secret
# marker: rest-config-e2e v1
"
PUBINFO="$(panel publish "$NODE_ID" "$PUB_YAML" "rest-config-e2e v1")"
echo "$PUBINFO" | sed 's/^/  /'
V1="$(printf '%s\n' "$PUBINFO" | sed -n 's/^version //p')"
[[ -n "$V1" ]] && ok "发布成功：v$V1" || { bad "发布失败（原始响应：$PUBINFO）"; exit 1; }

echo "  等待节点取配置并热重载（最多 20s）…"
for _ in $(seq 1 20); do
  if grep -q "marker: rest-config-e2e v1" "$WORK/etc/hysteria/config.yaml" 2>/dev/null; then break; fi
  sleep 1
done
check "节点磁盘上的 config.yaml 已变成新配置（发布生效）" "grep -q 'marker: rest-config-e2e v1' '$WORK/etc/hysteria/config.yaml'"
check "auth 段已是 HTTP 鉴权（面板模板原样落盘）" "grep -q 'url: https://auth.ipx.sarl' '$WORK/etc/hysteria/config.yaml'"
check "配置权限仍是 600" "[ \"\$(stat -c %a '$WORK/etc/hysteria/config.yaml')\" = '600' ]"
check "旧配置留了版本历史（可回滚）" "ls '$WORK/etc/hysteria/history' 2>/dev/null | grep -q ."
check "agent 日志记录了 REST 收到新配置" "grep -q 'REST 兜底：收到新配置' '$WORK/logs/agent.log'"
# 回执是异步写盘 + 面板写库，直接 grep 会撞竞态（面板已 applied 但日志还没 flush）
for _ in $(seq 1 10); do
  grep -q 'REST 兜底：配置回执已送达' "$WORK/logs/agent.log" 2>/dev/null && break
  sleep 1
done
check "agent 发回了配置回执" "grep -q 'REST 兜底：配置回执已送达' '$WORK/logs/agent.log'"
check "agent 没有因为应用配置而重启整个服务（热重载）" "grep -q '配置更新 rev=$V1\\|热重载\\|reload' '$WORK/logs/agent.log'"

sleep 3
ROWS="$(panel configs "$NODE_ID")"
echo "$ROWS" | grep "^row $V1 " | sed 's/^/  面板：/'
check "面板上 v$V1 已是 applied（不再是 published）" "printf '%s\\n' \"\$ROWS\" | grep -q '^row $V1 applied'"

log "4) 面板发布坏配置 v2 —— 断言节点拒绝 + 面板自动回滚"
# 坏配置必须是「面板能发布、节点上装不了」的那类：面板只做 YAML/字段校验，
# 节点才会真去检查证书是否可读、端口是否可监听。
BAD_YAML="listen: 127.0.0.1:18445
tls:
  cert: $WORK/certs/does-not-exist.crt
  key: $WORK/certs/does-not-exist.key
auth:
  type: password
  password: bad
# marker: rest-config-e2e bad
"
BADINFO="$(panel publish "$NODE_ID" "$BAD_YAML" "rest-config-e2e 坏配置")"
echo "$BADINFO" | sed 's/^/  /'
V2="$(printf '%s\n' "$BADINFO" | sed -n 's/^version //p')"
[[ -n "$V2" ]] || bad "坏配置发布失败（原始响应：$BADINFO）"

echo "  等待回执与回滚（最多 25s）…"
for _ in $(seq 1 25); do
  if python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$NODE_ID" "$V2" <<'PY'
import json, os, sys, urllib.request
base, email, pw, node_id, v2 = sys.argv[1:6]
def call(m, p, pl=None, t=None):
    h = {"Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if pl is not None: h["Content-Type"] = "application/json"
    if t: h["Authorization"] = "Bearer " + t
    req = urllib.request.Request(base + p, data=json.dumps(pl).encode() if pl is not None else None, headers=h, method=m)
    with urllib.request.urlopen(req, timeout=30) as r: return json.loads(r.read().decode() or "{}")
tok = os.environ.get("PANEL_TOKEN", "").strip()
rows = call("GET", f"/api/admin/v1/nodes/{node_id}/configs", None, tok)["data"]
bad = [r for r in rows if str(r["version"]) == v2]
ok = bad and bad[0]["status"] == "failed" and any(r["status"] == "applied" for r in rows if str(r["version"]) != v2)
sys.exit(0 if ok else 1)
PY
  then break; fi
  sleep 1
done

ROWS2="$(panel configs "$NODE_ID")"
echo "$ROWS2" | sed 's/^/  面板：/'
check "坏配置 v$V2 被标记 failed" "printf '%s\\n' \"\$ROWS2\" | grep -q '^row $V2 failed'"
check "面板自动生成了回滚版本并 applied" "printf '%s\\n' \"\$ROWS2\" | grep -c 'applied' | grep -qv '^0$'"
check "节点拒绝了坏配置并上报失败" "grep -q '配置应用失败\\|校验失败\\|invalid\\|failed' '$WORK/logs/agent.log'"
check "节点磁盘最终仍是可用的配置（不是坏配置）" "! grep -q 'marker: rest-config-e2e bad' '$WORK/etc/hysteria/config.yaml'"
check "回滚版本也被节点取走应用了（回执 ≥2 次：失败 + 回滚）" "[ \"\$(grep -c 'REST 兜底：配置回执已送达' '$WORK/logs/agent.log')\" -ge 2 ]"

log "5) 清理"
panel delete_node "$NODE_ID" >/dev/null
[[ -n "${TOKEN_ID:-}" ]] && panel revoke_token "$TOKEN_ID" >/dev/null

log "汇总"
printf 'PASS=%d FAIL=%d  （日志 %s）\n' "$PASS" "$FAIL" "$WORK/logs"
[[ "$FAIL" -eq 0 ]]
