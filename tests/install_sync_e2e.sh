#!/usr/bin/env bash
#
# tests/install_sync_e2e.sh —— 跑**真的 install.sh**：装完就该用上面板里最新发布的配置
#
# 需求（用户原话）：`sudo -E bash install.sh https://node.ipx.sarl --skip-enroll`
# 要求 VPS 上的 /etc/hysteria/config.yaml 自动更新成控制面上最新的那份。
#
# 这里在沙箱里跑真实 install.sh（systemctl 用假 shim，不碰宿主服务）：
#   ① 首次安装（带注册令牌）→ 凭据建立 + 配置同步落盘
#   ② 面板发布新版本 → 再跑一次（正是用户的 --skip-enroll 命令）→ 磁盘自动变成最新
#   ③ 已是最新时不动文件；--no-sync-config 则跳过
#
# 用法：PANEL_URL=http://127.0.0.1:8001 bash tests/install_sync_e2e.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANEL_URL="${PANEL_URL:-http://127.0.0.1:8001}"
PANEL_EMAIL="${PANEL_EMAIL:-admin@example.com}"
PANEL_PASSWORD="${PANEL_PASSWORD:-password}"
WORK="${WORK:-/tmp/install-sync-e2e}"
export PATH="$PATH:/usr/local/go/bin"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }

panel() {
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
        {"email":email,"password":pw,"device_name":"install-sync-e2e"})[1]["token"]
if action == "login": print(token())
elif action == "issue_token":
    st, body = call("POST","/api/admin/v1/enroll-tokens",{"note":"install-sync-e2e","max_uses":1,"expires_in_days":1},token())
    print(body.get("token")); print("token_id", (body.get("data") or {}).get("id"))
elif action == "publish":
    st, body = call("POST", f"/api/admin/v1/nodes/{args[0]}/configs", {"content_yaml": args[1], "reason": args[2]}, token())
    d = body.get("data") or body
    print("status", st); print("version", d.get("version"))
elif action == "delete_node": print("delete", call("DELETE", f"/api/admin/v1/nodes/{args[0]}", None, token())[0])
elif action == "revoke_token": print("revoke", call("DELETE", f"/api/admin/v1/enroll-tokens/{args[0]}", None, token())[0])
PY
}

log "0) 前置：控制面 + 沙箱（假 systemctl，绝不碰宿主服务）"
curl -sS -m 5 -o /dev/null "$PANEL_URL/up" || { echo "控制面 $PANEL_URL 不可达"; exit 2; }
ok "控制面可达：$PANEL_URL"

rm -rf "$WORK"; mkdir -p "$WORK"/{shim,bin,etc/hysteria/history,etc/agent,var,logs,certs}
cat > "$WORK/shim/systemctl" <<'EOF'
#!/bin/bash
printf '[shim] systemctl %s\n' "$*" >> "${SHIM_LOG:-/dev/null}"
exit 0
EOF
cat > "$WORK/shim/hysteria" <<'EOF'
#!/bin/bash
# 只应付 `hysteria version` 这类探测
if [[ "${1:-}" == "version" ]]; then echo "Version: v2.6.5 (mock)"; exit 0; fi
echo "mock hysteria"; exit 0
EOF
chmod +x "$WORK/shim/systemctl" "$WORK/shim/hysteria"
export PATH="$WORK/shim:$PATH"
export SHIM_LOG="$WORK/systemctl.log"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=install-sync-e2e" \
  -keyout "$WORK/certs/server.key" -out "$WORK/certs/server.crt" >/dev/null 2>&1
ok "自签证书已生成"
export PANEL_TOKEN="$(panel login)"

# 起点：节点上已有一份能用的旧配置（sync 应当用面板的配置把它更新掉）
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: 127.0.0.1:18460
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: node-side-old
trafficStats:
  listen: 127.0.0.1:19220
  secret: install-sync-secret
# marker: node-side-old
EOF
chmod 600 "$WORK/etc/hysteria/config.yaml"

log "1) 首次安装（真实 install.sh + 注册令牌）"
TOKINFO="$(panel issue_token)"; TOK="$(printf '%s\n' "$TOKINFO" | head -1)"; TOKEN_ID="$(printf '%s\n' "$TOKINFO" | sed -n 's/^token_id //p')"

run_install() { # run_install <额外参数...>
  AGENT_BIN="$WORK/bin/hysteria-agent" \
  AGENT_CFG_DIR="$WORK/etc/agent" \
  AGENT_STATE="$WORK/var" \
  AGENT_SERVICE="ha-install-sync-test" \
  HYSTERIA_CONFIG="$WORK/etc/hysteria/config.yaml" \
  HYSTERIA_LISTEN="127.0.0.1:18461" \
    SYNC_CONFIG_SOURCE="${SYNC_CONFIG_SOURCE:-node}" \
    bash "$ROOT/install.sh" "$PANEL_URL" --no-hysteria "$@" >"$WORK/logs/install-$1.log" 2>&1
}

if run_install --token="$TOK"; then ok "install.sh 成功退出"; else bad "install.sh 失败：$(tail -6 "$(ls -t "$WORK"/logs/install-*.log | head -1)" 2>/dev/null)"; fi
INSTALL_LOG="$(ls -t "$WORK"/logs/install-*.log | head -1)"
check "走了「同步面板配置」这一步" "grep -q '步骤 5/6' '$INSTALL_LOG'"
check "凭据已写入且 0400" "[ \"\$(stat -c %a '$WORK/etc/agent/credentials.json')\" = '400' ]"
NODE_ID="$(python3 -c "import json;print(json.load(open('$WORK/etc/agent/credentials.json'))['node_id'])" 2>/dev/null)"
[[ -n "${NODE_ID:-}" ]] && ok "注册成功：$NODE_ID" || { bad "拿不到 node_id"; exit 1; }

log "2) 面板发布 v1 → 再跑一次（就是用户那条 --skip-enroll 命令）→ 磁盘必须自动变成最新"
V1_YAML="listen: 127.0.0.1:18461
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: http
  http:
    url: https://auth.ipx.sarl
    insecure: false
trafficStats:
  listen: 127.0.0.1:19220
  secret: install-sync-secret
# marker: from-panel-v1
"
V1="$(panel publish "$NODE_ID" "$V1_YAML" "install-sync v1" | sed -n 's/^version //p')"
[[ -n "$V1" ]] && ok "发布 v$V1" || { bad "发布失败"; exit 1; }

run_install --skip-enroll
INSTALL_LOG="$(ls -t "$WORK"/logs/install-*.log | head -1)"
cat "$INSTALL_LOG" | grep -E '步骤 5/6|已同步|WARN|FAIL' | sed 's/^/    /'
check "磁盘已是面板配置（自动更新成最新）" "grep -q 'marker: from-panel-v1' '$WORK/etc/hysteria/config.yaml'"
check "旧配置被换掉" "! grep -q 'node-side-old' '$WORK/etc/hysteria/config.yaml'"
check "权限仍是 600" "[ \"\$(stat -c %a '$WORK/etc/hysteria/config.yaml')\" = '600' ]"
HIST="$(awk '/^[[:space:]]*history_dir:/{print $2}' "$WORK/etc/agent/config.yaml" | tr -d '"')"
check "留下可回滚的历史（$HIST）" "[ -n '$HIST' ] && ls '$HIST' | grep -q rev-"

log "3) 已是最新：再跑一次不该重复写"
BEFORE="$(stat -c %Y "$WORK/etc/hysteria/config.yaml")"
sleep 1
run_install --skip-enroll
check "识别为已是最新" "grep -q '已是最新' \"\$(ls -t '$WORK'/logs/install-*.log | head -1)\""
[[ "$(stat -c %Y "$WORK/etc/hysteria/config.yaml")" == "$BEFORE" ]] && ok "文件没被重复写" || bad "文件被重复写了"

log "4) --no-sync-config 应跳过同步"
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" --version >/dev/null 2>&1
V2_YAML="listen: 127.0.0.1:18461
tls:
  cert: $WORK/certs/server.crt
  key: $WORK/certs/server.key
auth:
  type: password
  password: from-panel-v2
# marker: from-panel-v2
"
panel publish "$NODE_ID" "$V2_YAML" "install-sync v2" >/dev/null
run_install --skip-enroll --no-sync-config
INSTALL_LOG="$(ls -t "$WORK"/logs/install-*.log | head -1)"
check "明确说了跳过同步" "grep -q '跳过配置同步' '$INSTALL_LOG'"
check "跳过时确实没改成 v2" "! grep -q 'marker: from-panel-v2' '$WORK/etc/hysteria/config.yaml'"

log "5) 再跑一次（不禁用同步）→ 立刻跟上 v2"
run_install --skip-enroll
check "磁盘跟上 v2" "grep -q 'marker: from-panel-v2' '$WORK/etc/hysteria/config.yaml'"

log "6) 清理（含装上机器的那份 unit 文件与临时目录）"
panel delete_node "$NODE_ID" >/dev/null
[[ -n "${TOKEN_ID:-}" ]] && panel revoke_token "$TOKEN_ID" >/dev/null
rm -f /etc/systemd/system/ha-install-sync-test.service
printf '  systemctl 调用（全部被 shim 拦下，未触碰宿主服务）：\n'
sed 's/^/    /' "$WORK/systemctl.log" 2>/dev/null | head -6

log "汇总"
printf 'PASS=%d FAIL=%d  （日志 %s）\n' "$PASS" "$FAIL" "$WORK/logs"
[[ "$FAIL" -eq 0 ]]
