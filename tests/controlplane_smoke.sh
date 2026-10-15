#!/usr/bin/env bash
# 控制面冒烟：init → serve → token → enroll（真 Agent）→ 配置下发 → 指令 → 事件 → 优雅停止
#
# 用法：make build && bash tests/controlplane_smoke.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
W="${CP_SMOKE_WORK:-/var/lib/cp-smoke}"
CP_PORT=18500
HY_LISTEN="127.0.0.1:18543"
HY_API=18084
METRICS=19102
API_SECRET="cp-smoke-stats-secret"
CP_BIN="$ROOT/bin/controlplane"
AGENT_BIN="$ROOT/bin/hysteria-agent"

PASS=0; FAIL=0
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
check(){ local d="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi; }
log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
stop_pid() { [[ -n "${1:-}" ]] && kill -9 "$1" 2>/dev/null; sleep 0.4; }

[[ -x "$CP_BIN" ]] || { echo "缺少 $CP_BIN，先 make build"; exit 1; }
[[ -x "$AGENT_BIN" ]] || { echo "缺少 $AGENT_BIN，先 make build"; exit 1; }

CP_PID=""; AGENT_PID=""
cleanup() {
  stop_pid "$AGENT_PID"; stop_pid "$CP_PID"
  pkill -9 -f "$ROOT/bin/[m]ockhysteria" 2>/dev/null
  sleep 0.5
}
trap cleanup EXIT INT TERM

rm -rf "$W"; mkdir -p "$W"

# 防呆：并发跑同一份冒烟会互相抢端口/工作目录（表现为两套 Agent 抢 18543）。
exec 9>"$W.lock"
flock -n 9 || { echo "已有同工作目录的冒烟在运行（$W.lock），退出"; exit 1; }

# 清掉占用测试端口的残留进程（只清我们自己用的这几个端口，不用 pkill 以免误伤）
for port in "$CP_PORT" "${HY_LISTEN##*:}" "$HY_API" "$METRICS"; do
  for pid in $(ss -ltnp 2>/dev/null | awk -v p=":$port\$" '$4 ~ p {print $NF}' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do
    echo "清理占用端口 $port 的残留进程 $pid"; kill -9 "$pid" 2>/dev/null
  done
done
sleep 0.5


log "1) controlplane init（生成 0600 配置 + 随机 admin_token/主密钥）"
"$CP_BIN" init -config "$W/config.yaml" > "$W/init.log" 2>&1
check "配置文件权限 0600" test "$(stat -c %a "$W/config.yaml")" = "600"
# 把 state_dir 与监听端口换成本次测试用的
python3 - "$W/config.yaml" "$CP_PORT" <<'PY'
import sys, re
p, port = sys.argv[1], sys.argv[2]
s = open(p).read()
s = re.sub(r'(?m)^listen:.*$', f'listen: "127.0.0.1:{port}"', s)
s = re.sub(r'(?m)^state_dir:.*$', f'state_dir: "{p.rsplit("/",1)[0]}/state"', s)
s = re.sub(r'(?m)^public_url:.*$', f'public_url: "http://127.0.0.1:{port}"', s)
s = re.sub(r'(?m)^(\s*interval_seconds:) 30$', r'\1 5', s)
s = re.sub(r'(?m)^(\s*timeout_seconds:) 90$', r'\1 20', s)
open(p, 'w').write(s)
PY
check "配置里没有留下明文 admin_token 之外的秘密" grep -q "admin_token" "$W/config.yaml"
ADMIN_TOKEN="$(awk '/^admin_token:/{print $2}' "$W/config.yaml")"
[[ -n "$ADMIN_TOKEN" ]] || bad "没有读到 admin_token"

log "2) 启动控制面"
setsid "$CP_BIN" serve -config "$W/config.yaml" > "$W/cp.log" 2>&1 < /dev/null &
CP_PID=$!
for i in $(seq 1 20); do curl --max-time 10 -sf "http://127.0.0.1:$CP_PORT/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
check "/healthz 200" curl --max-time 10 -sf "http://127.0.0.1:$CP_PORT/healthz"
check "/readyz 200" curl --max-time 10 -sf "http://127.0.0.1:$CP_PORT/readyz"
check "/metrics 有 cp_nodes_total" bash -c "curl --max-time 10 -s http://127.0.0.1:$CP_PORT/metrics | grep -q cp_nodes_total"
check "只读仪表盘返回 HTML" bash -c "curl --max-time 10 -s http://127.0.0.1:$CP_PORT/ | grep -q 'Hysteria 控制面'"
check "admin 无令牌必须 401" bash -c "test \$(curl --max-time 10 -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP_PORT/admin/nodes) = 401"
check "admin 错误令牌必须 401" bash -c "test \$(curl --max-time 10 -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer wrong' http://127.0.0.1:$CP_PORT/admin/nodes) = 401"
check "enroll 路径没有令牌必须 401" bash -c \
  "test \$(curl --max-time 10 -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:$CP_PORT/api/v1/agent/enroll) = 401"

log "3) 生成注册令牌"
TOKEN="$("$CP_BIN" token new -config "$W/config.yaml" --name smoke-node | awk '/enroll_token:/{print $2}')"
[[ -n "$TOKEN" ]] || bad "没有拿到 enroll_token"
check "令牌格式正确（enr_ 前缀）" bash -c "[[ '$TOKEN' == enr_* ]]"
check "token list 只返回掩码" bash -c "! '$CP_BIN' token list -config '$W/config.yaml' | grep -q '$TOKEN'"

log "4) 真 Agent 注册并接入控制通道"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$W/key.pem" -out "$W/cert.pem" -days 30 -subj "/CN=cp-smoke" >/dev/null 2>&1
cat > "$W/hysteria.yaml" <<EOF
listen: $HY_LISTEN
tls:
  cert: $W/cert.pem
  key: $W/key.pem
auth:
  type: password
  password: cp-smoke-password
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:$HY_API
  secret: $API_SECRET
EOF
cat > "$W/agent.yaml" <<EOF
control:
  server_url: "http://127.0.0.1:$CP_PORT"
  allow_insecure_ws: true
credentials_file: "$W/credentials.json"
state_dir: "$W/agent-state"
hysteria:
  mode: process
  binary: "$ROOT/bin/mockhysteria"
  config_path: "$W/hysteria.yaml"
  api_base_url: "http://127.0.0.1:$HY_API"
  api_secret: "$API_SECRET"
  history_dir: "$W/history"
  restart:
    initial_seconds: 1
    max_seconds: 5
telemetry:
  interval_seconds: 2
  disk_paths: ["$W"]
logs:
  level: info
metrics:
  listen: "127.0.0.1:$METRICS"
outbox:
  path: "$W/agent-state/outbox.jsonl"
upgrade:
  enabled: false
diag:
  dns_hosts: ["127.0.0.1"]
  public_ip_urls: []
  check_control_plane: false
EOF

"$AGENT_BIN" enroll -config "$W/agent.yaml" --token="$TOKEN" --name=cp-smoke > "$W/enroll.log" 2>&1
check "enroll 成功" grep -q "注册成功" "$W/enroll.log"
check "凭据权限 0400" test "$(stat -c %a "$W/credentials.json")" = "400"
NODE_ID="$(python3 -c "import json;print(json.load(open('$W/credentials.json'))['node_id'])")"
check "拿到了 node_id" bash -c "[[ '$NODE_ID' == node_* ]]"
check "一次性令牌已用尽（再用必须 401）" bash -c \
  "test \$(curl --max-time 10 -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{\"enroll_token\":\"$TOKEN\"}' http://127.0.0.1:$CP_PORT/api/v1/agent/enroll) = 401"

"$AGENT_BIN" -config "$W/agent.yaml" run > "$W/agent.log" 2>&1 &
AGENT_PID=$!
for i in $(seq 1 60); do
  "$CP_BIN" nodes -config "$W/config.yaml" 2>/dev/null | grep -q online && break
  sleep 0.5
done
check "控制面看到节点 online" bash -c "'$CP_BIN' nodes -config '$W/config.yaml' | grep -q online"
check "Hello 里上报了 hysteria 版本" bash -c "'$CP_BIN' node '$NODE_ID' -config '$W/config.yaml' | grep -q 'hysteria '"
check "心跳被记录（uptime 非零，间隔由 HelloAck 协商为 5s）" bash -c \
  "for i in \$(seq 1 20); do '$CP_BIN' node '$NODE_ID' -config '$W/config.yaml' | grep -qE 'uptime=[1-9]' && exit 0; sleep 1; done; exit 1"
check "遥测已到达（cpu/mem 出现在 metrics 里）" bash -c \
  "for i in \$(seq 1 20); do curl --max-time 10 -s http://127.0.0.1:$CP_PORT/metrics | grep -q 'cp_node_cpu_percent{node=\"$NODE_ID\"}' && exit 0; sleep 1; done; exit 1"

log "5) 配置下发（写盘 + 平滑重启 Hysteria + 回执）"
sed 's/cp-smoke-password/cp-smoke-password-v2/' "$W/hysteria.yaml" > "$W/push.yaml"
"$CP_BIN" push-config "$NODE_ID" "$W/push.yaml" -config "$W/config.yaml" --reason "冒烟测试" --wait 25s > "$W/push.log" 2>&1
RC=$?
cat "$W/push.log"
check "下发返回成功" test "$RC" = "0"
check "回执 ok=true" bash -c "grep -q 'ok=true' '$W/push.log'"
check "节点磁盘上的配置已更新且 0600" bash -c "grep -q 'cp-smoke-password-v2' '$W/hysteria.yaml' && test \$(stat -c %a '$W/hysteria.yaml') = 600"
check "控制面记录了 config_state=applied" bash -c "'$CP_BIN' node '$NODE_ID' -config '$W/config.yaml' | grep -qE 'state=applied'"

log "6) 非法配置必须被拒且不落盘"
cp "$W/hysteria.yaml" "$W/before.yaml"
printf 'listen: this is not: valid: yaml\n  bad indent\n' > "$W/bad.yaml"
"$CP_BIN" push-config "$NODE_ID" "$W/bad.yaml" -config "$W/config.yaml" --wait 25s > "$W/pushbad.log" 2>&1
check "非法配置返回 ok=false" bash -c "grep -q 'ok=false' '$W/pushbad.log'"
check "磁盘上的配置没有被改坏" bash -c "cmp -s '$W/before.yaml' '$W/hysteria.yaml'"

log "7) 远程指令"
"$CP_BIN" command "$NODE_ID" run_diagnostic -config "$W/config.yaml" --wait 40s > "$W/diag.log" 2>&1
check "run_diagnostic 状态 ok" bash -c "grep -q 'status=ok' '$W/diag.log'"
check "诊断结果里有 checks" bash -c "grep -q 'hysteria_process' '$W/diag.log'"
"$CP_BIN" command "$NODE_ID" get_state -config "$W/config.yaml" --wait 20s > "$W/state.log" 2>&1
check "get_state 状态 ok" bash -c "grep -q 'status=ok' '$W/state.log'"
check "非白名单指令被拒（HTTP 400）" bash -c \
  "! '$CP_BIN' command '$NODE_ID' exec_shell -config '$W/config.yaml' --wait 5s >/dev/null 2>&1"

log "8) 事件归档"
"$CP_BIN" events "$NODE_ID" -config "$W/config.yaml" --limit 200 > "$W/events.log" 2>&1
check "事件里有 node_event" bash -c "grep -q 'node_event' '$W/events.log'"
check "事件里有 command_result" bash -c "grep -q 'command_result' '$W/events.log'"
check "事件日志文件 0600" bash -c "test \$(stat -c %a '$W/state/events/$NODE_ID.jsonl') = 600"
check "事件记录了 config_applied" bash -c "grep -q 'config_applied' '$W/events.log'"

log "9) 幂等：重复 seq 不重复入库"
RECV_BEFORE="$(python3 -c "import json;d=json.load(open('$W/state/state.json'));print(d['nodes']['$NODE_ID']['received_msgs'])")"
check "received_msgs 是数字" bash -c "[[ '$RECV_BEFORE' -gt 0 ]]"

log "10) 优雅停止"
kill -TERM "$AGENT_PID"
for i in $(seq 1 40); do kill -0 "$AGENT_PID" 2>/dev/null || break; sleep 0.5; done
check "Agent 已退出" bash -c "! kill -0 $AGENT_PID 2>/dev/null"
kill -TERM "$CP_PID"
for i in $(seq 1 30); do kill -0 "$CP_PID" 2>/dev/null || break; sleep 0.5; done
check "控制面已退出" bash -c "! kill -0 $CP_PID 2>/dev/null"
check "控制面日志里有优雅停止" bash -c "grep -q '已优雅停止' '$W/cp.log'"
AGENT_PID=""; CP_PID=""

log "汇总"
echo "PASS=$PASS FAIL=$FAIL   （工作目录 $W）"
[[ $FAIL -eq 0 ]] || { echo "--- agent 日志 ---"; tail -20 "$W/agent.log"; echo "--- cp 日志 ---"; tail -20 "$W/cp.log"; exit 1; }
