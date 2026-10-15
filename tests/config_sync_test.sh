#!/usr/bin/env bash
#
# tests/config_sync_test.sh —— `hysteria-agent config sync`：把面板最新配置立刻落到节点
#
# 需求：`sudo -E bash install.sh <url> --skip-enroll` 之后，VPS 上的 /etc/hysteria/config.yaml
# 应当**立刻**是面板里最新发布的配置（而不是等 agent 起来轮询，更不是永远保持旧文件）。
#
# 断言：写入 / 已是最新时不动 / 校验不过就不写（不能把能用的配置换坏）/ dry-run 不落盘 /
#       权限 600 / 留下可回滚的历史快照。
#
# 用法：PANEL_URL=http://127.0.0.1:8001 bash tests/config_sync_test.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANEL_URL="${PANEL_URL:-http://127.0.0.1:8001}"
PANEL_EMAIL="${PANEL_EMAIL:-admin@example.com}"
PANEL_PASSWORD="${PANEL_PASSWORD:-password}"
WORK="${WORK:-/tmp/config-sync-test}"
export PATH="$PATH:/usr/local/go/bin"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }

panel() { # panel <action> [args...]
  python3 - "$PANEL_URL" "$PANEL_EMAIL" "$PANEL_PASSWORD" "$@" <<'PY'
import json, os, sys, urllib.request, urllib.error
base, email, pw = sys.argv[1:4]
action, args = sys.argv[4], sys.argv[5:]
def call(m, p, pl=None, t=None):
    h = {"Accept": "application/json", "User-Agent": "hysteria-agent/1.0.0"}
    if pl is not None: h["Content-Type"] = "application/json"
    if t: h["Authorization"] = "Bearer " + t
    req = urllib.request.Request(base + p, data=json.dumps(pl).encode() if pl is not None else None, headers=h, method=m)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e: return e.code, json.loads(e.read().decode() or "{}")
def token():
    return os.environ.get("PANEL_TOKEN","").strip() or call("POST","/api/admin/v1/auth/login",
        {"email":email,"password":pw,"device_name":"config-sync-test"})[1]["token"]
if action == "login": print(token())
elif action == "issue_token":
    st, body = call("POST","/api/admin/v1/enroll-tokens",{"note":"config-sync-test","max_uses":1,"expires_in_days":1},token())
    print(body.get("token")); print("token_id", (body.get("data") or {}).get("id"))
elif action == "publish":
    st, body = call("POST", f"/api/admin/v1/nodes/{args[0]}/configs", {"content_yaml": args[1], "reason": args[2]}, token())
    d = body.get("data") or body
    print("status", st); print("version", d.get("version"))
elif action == "delete_node": print("delete", call("DELETE", f"/api/admin/v1/nodes/{args[0]}", None, token())[0])
elif action == "revoke_token": print("revoke", call("DELETE", f"/api/admin/v1/enroll-tokens/{args[0]}", None, token())[0])
PY
}

cleanup() { :; }
trap cleanup EXIT

log "0) 前置"
curl -sS -m 5 -o /dev/null "$PANEL_URL/up" || { echo "控制面 $PANEL_URL 不可达"; exit 2; }
ok "控制面可达：$PANEL_URL"
export PANEL_TOKEN="$(panel login)"
[[ -n "$PANEL_TOKEN" ]] && ok "面板登录成功" || { bad "面板登录失败"; exit 1; }

log "1) 构建 agent 并注册一个一次性节点"
rm -rf "$WORK"; mkdir -p "$WORK"/{bin,etc/agent,etc/hysteria,certs,logs}
cd "$ROOT" || exit 1
go build -trimpath -o "$WORK/bin/hysteria-agent" ./cmd/hysteria-agent || exit 1
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=config-sync-test" \
  -keyout "$WORK/certs/server.key" -out "$WORK/certs/server.crt" >/dev/null 2>&1
ok "自签证书已生成"

TOKINFO="$(panel issue_token)"; TOK="$(printf '%s\n' "$TOKINFO" | head -1)"; TOKEN_ID="$(printf '%s\n' "$TOKINFO" | sed -n 's/^token_id //p')"

cat > "$WORK/etc/agent/config.yaml" <<EOF
control:
  server_url: "$PANEL_URL"
  allow_insecure_ws: true
  heartbeat_interval_seconds: 3
  rest_fallback_seconds: 30
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  config_path: "$WORK/etc/hysteria/config.yaml"
  history_dir: "$WORK/etc/hysteria/history"
  api_base_url: "http://127.0.0.1:19210"
  api_secret: "e2e-stats-secret"
logs: { level: info, remote_enabled: false }
metrics: { listen: "127.0.0.1:19211" }
upgrade: { enabled: false }
EOF

# 起点：一份能用的本地配置（sync 不该在没有新版本时动它）
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18453
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: local-original
# marker: local-original
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"
cp "$WORK/etc/hysteria/config.yaml" "$WORK/original.yaml"

"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="$TOK" --name="config-sync-$(date +%s)" --replace >"$WORK/logs/enroll.log" 2>&1
NODE_ID="$(python3 -c "import json;print(json.load(open('$WORK/etc/agent/credentials.json'))['node_id'])" 2>/dev/null)"
[[ -n "${NODE_ID:-}" ]] && ok "注册成功：$NODE_ID" || { bad "注册失败"; tail -5 "$WORK/logs/enroll.log"; exit 1; }

log "2) 控制面还没发布配置 → sync 不该动本地文件"
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync >"$WORK/logs/sync0.log" 2>&1
SYNC0=$?
grep -q "已是最新\|没有已发布" "$WORK/logs/sync0.log" && ok "如实报告「没有更新的版本」" || { bad "输出不符：$(cat "$WORK/logs/sync0.log")"; }
diff -q "$WORK/etc/hysteria/config.yaml" "$WORK/original.yaml" >/dev/null && ok "本地配置未被改动" || bad "本地配置被改了"

log "3) 面板发布 v1 → sync 立刻落盘"
V1_YAML="listen: 127.0.0.1:18454
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: http
  http:
    url: https://auth.ipx.sarl
    insecure: false
trafficStats:
  listen: 127.0.0.1:19210
  secret: e2e-stats-secret
# marker: from-panel-v1
"
V1="$(panel publish "$NODE_ID" "$V1_YAML" "config-sync v1" | sed -n 's/^version //p')"
[[ -n "$V1" ]] && ok "发布 v$V1" || { bad "发布失败"; exit 1; }

"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync >"$WORK/logs/sync1.log" 2>&1
SYNCR=$?
[[ "$SYNCR" == "0" ]] && ok "sync 成功退出" || { bad "sync 失败：$(cat "$WORK/logs/sync1.log")"; }
cat "$WORK/logs/sync1.log" | sed 's/^/    /'
check "磁盘已是面板配置" "grep -q 'marker: from-panel-v1' '$WORK/etc/hysteria/config.yaml'"
check "auth 段是 HTTP 鉴权" "grep -q 'url: https://auth.ipx.sarl' '$WORK/etc/hysteria/config.yaml'"
check "权限 600" "[ \"\$(stat -c %a '$WORK/etc/hysteria/config.yaml')\" = '600' ]"
check "留了历史快照（可回滚）" "ls '$WORK/etc/hysteria/history' 2>/dev/null | grep -q ."
check "输出里带了版本与 sha" "grep -q '已同步控制面 v$V1' '$WORK/logs/sync1.log'"

log "4) 再 sync 一次：已是最新，不该重复写"
BEFORE="$(stat -c %Y "$WORK/etc/hysteria/config.yaml")"
sleep 1
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync >"$WORK/logs/sync2.log" 2>&1
grep -q "已是最新" "$WORK/logs/sync2.log" && ok "识别为已是最新" || bad "没有识别为已是最新：$(cat "$WORK/logs/sync2.log")"
[[ "$(stat -c %Y "$WORK/etc/hysteria/config.yaml")" == "$BEFORE" ]] && ok "文件没被重复写" || bad "文件被重复写了"

log "5) 坏配置（面板能发布、节点装不了）→ 校验不过就不写"
BAD_YAML="listen: 127.0.0.1:18455
tls:
  cert: $WORK/certs/nope.crt
  key: $WORK/certs/nope.key
# marker: from-panel-bad
"
panel publish "$NODE_ID" "$BAD_YAML" "config-sync 坏配置" >/dev/null
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync >"$WORK/logs/sync3.log" 2>&1
[[ $? -ne 0 ]] && ok "坏配置被拒（非 0 退出）" || bad "坏配置竟然同步成功"
check "坏配置没落盘" "! grep -q 'marker: from-panel-bad' '$WORK/etc/hysteria/config.yaml'"
check "仍是上一份可用配置" "grep -q 'marker: from-panel-v1' '$WORK/etc/hysteria/config.yaml'"

log "6) dry-run 不落盘"
V4_YAML="listen: 127.0.0.1:18456
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: from-panel-v4
# marker: from-panel-v4
"
panel publish "$NODE_ID" "$V4_YAML" "config-sync v4" >/dev/null
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync --dry-run >"$WORK/logs/sync4.log" 2>&1
check "dry-run 明确说了 --dry-run" "grep -q -- '--dry-run' '$WORK/logs/sync4.log'"
check "dry-run 没落盘" "! grep -q 'marker: from-panel-v4' '$WORK/etc/hysteria/config.yaml'"
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" config sync >"$WORK/logs/sync5.log" 2>&1
check "去掉 dry-run 后正常落盘" "grep -q 'marker: from-panel-v4' '$WORK/etc/hysteria/config.yaml'"

log "7) 清理"
panel delete_node "$NODE_ID" >/dev/null
[[ -n "${TOKEN_ID:-}" ]] && panel revoke_token "$TOKEN_ID" >/dev/null

log "汇总"
printf 'PASS=%d FAIL=%d  （日志 %s）\n' "$PASS" "$FAIL" "$WORK/logs"
[[ "$FAIL" -eq 0 ]]
