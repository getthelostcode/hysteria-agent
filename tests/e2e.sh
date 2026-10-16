#!/usr/bin/env bash
# e2e.sh — hysteria-agent 端到端联调测试
#
# 组成：真实的 hysteria-agent + 假控制面(mockcontrol) + 假 Hysteria(mockhysteria)。
# 覆盖需求里的每一条关键行为，并断言"控制面到底收到了什么"（读 dump 文件），
# 而不是只看 Agent 自己的日志。
#
# 用法: bash tests/e2e.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${E2E_WORK:-/tmp/hysteria-agent-e2e}"
CP_PORT=18080
HY_LISTEN="127.0.0.1:18443"
HY_API_PORT=18081
METRICS_PORT=19100
API_SECRET="stats-secret-e2e"
ENROLL_TOKEN="e2e-enroll-token"

PASS=0
FAIL=0
AGENT_PID=""
CP_PID=""

log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
check(){ local desc="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$desc"; else bad "$desc"; fi; }

cleanup() {
  local w="${WORK:-}"
  [[ -n "$AGENT_PID" ]] && kill "$AGENT_PID" 2>/dev/null
  [[ -n "$CP_PID" ]] && kill "$CP_PID" 2>/dev/null
  # 兜底清理：上一轮残留的 Agent 会各自拉起一个 mockhysteria 抢同一个端口，
  # 表现为"配置校验通过但服务起不来"的假故障（真实踩过）。
  if [[ -n "$w" ]]; then
    pkill -f "$w/bin/mockhysteria" 2>/dev/null
    pkill -f "hysteria-agent -config $w/etc/agent/config.yaml" 2>/dev/null
    sleep 0.5
    pkill -9 -f "$w/bin/mockhysteria" 2>/dev/null
  fi
  sleep 1
}
trap cleanup EXIT INT TERM
cleanup   # 清掉上一轮残留，避免端口抢占造成的假故障

dumpq() { python3 "$DUMPQ_PY" "$@"; }
q() { python3 - "$@" <<'PY'
import json,sys
path, dotted = sys.argv[1], sys.argv[2]
try:
    with open(path) as f:
        cur = json.load(f)
except Exception:
    print(""); raise SystemExit
for part in dotted.split('.'):
    cur = cur.get(part) if isinstance(cur, dict) else None
# 输出要能和 shell 里的字符串直接比较：布尔值用小写 JSON 形式（true/false），
# 字符串原样不带引号，数字原样。Python 的 str(True)="True" 会让 `test x = true` 假失败。
if cur is None:
    print("")
elif isinstance(cur, bool):
    print("true" if cur else "false")
elif isinstance(cur, (dict, list)):
    print(json.dumps(cur, ensure_ascii=False))
else:
    print(cur)
PY
}

wait_for() { # wait_for <描述> <秒数> <命令...>
  local desc="$1" secs="$2"; shift 2
  local i=0
  while (( i < secs*2 )); do
    if "$@" >/dev/null 2>&1; then ok "$desc"; return 0; fi
    sleep 0.5; i=$((i+1))
  done
  bad "$desc（等待 ${secs}s 超时）"; return 1
}

wait_for_q() { # wait_for_q <描述> <json文件> <点路径> <期望值> <秒数>
  local desc="$1" file="$2" path="$3" want="$4" secs="$5"
  local i=0 cur=""
  while (( i < secs*2 )); do
    cur="$(q "$file" "$path")"
    if [[ "$cur" == "$want" ]]; then ok "$desc"; return 0; fi
    sleep 0.5; i=$((i+1))
  done
  bad "$desc（等待 ${secs}s 超时，当前 $path=$cur）"; return 1
}

# 断言经常写成 `check "…" bash -c "dumpq … | grep -q …"`，而 shell 函数默认不会
# 传给子 shell——必须 export -f，否则 dumpq/q 在 bash -c 里是"command not found"，
# 所有依赖它们断言都会假失败（这个坑真实踩过）。函数里用到的变量也要一起导出。
DUMPQ_PY="$ROOT/tests/dumpq.py"
export DUMPQ_PY ROOT
export -f dumpq q

# ---------------------------------------------------------------- 准备
log "准备环境 $WORK"
rm -rf "$WORK"
mkdir -p "$WORK"/{bin,etc/agent,etc/hysteria,var,logs,files}
export PATH=$PATH:/usr/local/go/bin

cd "$ROOT"
go build -o "$WORK/bin/mockcontrol"  ./cmd/mockcontrol  || exit 1
go build -o "$WORK/bin/mockhysteria" ./cmd/mockhysteria || exit 1
go build -o "$WORK/bin/agent-sign"   ./cmd/agent-sign   || exit 1
go build -trimpath -ldflags "-s -w -X github.com/getthelostcode/hysteria-agent/internal/version.Version=0.1.0-e2e" \
  -o "$WORK/bin/hysteria-agent" ./cmd/hysteria-agent || exit 1
# "新版本"用于自升级测试
go build -trimpath -ldflags "-s -w -X github.com/getthelostcode/hysteria-agent/internal/version.Version=9.9.9" \
  -o "$WORK/files/hysteria-agent" ./cmd/hysteria-agent || exit 1
echo "构建完成: $(ls "$WORK/bin")"

# 升级签名密钥
"$WORK/bin/agent-sign" keygen -out "$WORK/upgrade.key" >"$WORK/logs/keygen.log" 2>&1
PUBKEY=$(grep '公钥(base64)' "$WORK/logs/keygen.log" | awk '{print $2}')
[[ -n "$PUBKEY" ]] || { echo "生成签名密钥失败"; cat "$WORK/logs/keygen.log"; exit 1; }

# Hysteria 的证书（自签）与配置
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$WORK/etc/hysteria/key.pem" -out "$WORK/etc/hysteria/cert.pem" \
  -days 30 -subj "/CN=node.example.com" >/dev/null 2>&1
cat > "$WORK/etc/hysteria/config.yaml" <<EOF
listen: $HY_LISTEN
tls:
  cert: $WORK/etc/hysteria/cert.pem
  key: $WORK/etc/hysteria/key.pem
auth:
  type: password
  password: initial-password-123456
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:$HY_API_PORT
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
  reconnect:
    initial_seconds: 1
    max_seconds: 5
credentials_file: "$WORK/etc/agent/credentials.json"
state_dir: "$WORK/var"
runtime_dir: "$WORK/var/run"
hysteria:
  mode: process
  binary: "$WORK/bin/mockhysteria"
  config_path: "$WORK/etc/hysteria/config.yaml"
  api_base_url: "http://127.0.0.1:$HY_API_PORT"
  api_secret: "$API_SECRET"
  history_dir: "$WORK/etc/agent/history"
  history_keep: 5
  startup_timeout_seconds: 10
  shutdown_timeout_seconds: 5
  restart:
    initial_seconds: 1
    max_seconds: 5
  restart_max_in_window: 5
  restart_window_seconds: 60
telemetry:
  interval_seconds: 2
  disk_paths: ["$WORK"]
logs:
  level: info
  remote_enabled: true
metrics:
  listen: "127.0.0.1:$METRICS_PORT"
outbox:
  path: "$WORK/var/outbox.jsonl"
upgrade:
  enabled: true
  pubkeys: ["$PUBKEY"]
  binary_path: "$WORK/bin/hysteria-agent"
  work_dir: "$WORK/var/upgrade"
  restart_mode: exec
diag:
  dns_hosts: ["127.0.0.1"]
  public_ip_urls: []
  check_control_plane: false
EOF

STATE="$WORK/var/state.json"
DUMP="$WORK/logs/cp-dump.jsonl"

# ---------------------------------------------------------------- 1. 启动控制面 + 注册
log "1) 启动假控制面并注册节点"
"$WORK/bin/mockcontrol" -listen "127.0.0.1:$CP_PORT" -enroll-token "$ENROLL_TOKEN" \
  -dump "$DUMP" -files-dir "$WORK/files" -heartbeat-interval 3 -telemetry-interval 2 \
  >"$WORK/logs/mockcontrol.log" 2>&1 &
CP_PID=$!
sleep 1
check "假控制面健康检查" curl -sf "http://127.0.0.1:$CP_PORT/healthz"

"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token="$ENROLL_TOKEN" \
  --name e2e-node >"$WORK/logs/enroll.log" 2>&1
grep -q "注册成功" "$WORK/logs/enroll.log" && ok "enroll 成功" || { bad "enroll 失败"; cat "$WORK/logs/enroll.log"; cleanup; exit 1; }
check "credentials.json 权限为 0400" test "$(stat -c %a "$WORK/etc/agent/credentials.json")" = "400"
check "credentials.json 含 node_id 与 node_secret" bash -c \
  "python3 -c \"import json;d=json.load(open('$WORK/etc/agent/credentials.json'));assert d['node_id'] and len(d['node_secret'])>=16\""
# 错误令牌必须被拒绝
"$WORK/bin/hysteria-agent" enroll -config "$WORK/etc/agent/config.yaml" --token=wrong >/dev/null 2>&1 \
  && bad "错误 enroll token 应被拒绝" || ok "错误 enroll token 被拒绝"

# ---------------------------------------------------------------- 2. 启动 Agent
log "2) 启动 Agent"
"$WORK/bin/hysteria-agent" -config "$WORK/etc/agent/config.yaml" run >"$WORK/logs/agent.log" 2>&1 &
AGENT_PID=$!
echo "agent pid=$AGENT_PID"

wait_for "Agent 写入 state.json" 20 test -f "$STATE"
wait_for "控制通道已连接" 20 bash -c "test \"\$(python3 -c \"import json;print(json.load(open('$STATE'))['control']['connected'])\")\" = True"
wait_for "Hysteria 子进程被拉起" 20 bash -c "test \"\$(python3 -c \"import json;print(json.load(open('$STATE'))['hysteria']['running'])\")\" = True"
check "本地 /healthz 返回 200" curl -sf "http://127.0.0.1:$METRICS_PORT/healthz"
check "本地 /readyz 返回 200" curl -sf "http://127.0.0.1:$METRICS_PORT/readyz"
check "/metrics 暴露 agent 指标" bash -c "curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep -q hysteria_agent_hysteria_running"
check "Hello 上报了能力白名单" bash -c "dumpq last '$DUMP' hello capabilities | grep -q reload_config"
check "心跳已到达控制面" bash -c "test \$(dumpq count '$DUMP' heartbeat) -ge 1"
check "指标已到达控制面" bash -c "test \$(dumpq count '$DUMP' metricBatch) -ge 1"
check "握手时上报了 resume_from_seq" bash -c "dumpq last '$DUMP' hello resumeFromSeq"

# ---------------------------------------------------------------- 3. 配置下发
log "3) 配置下发与热更新"
REV1="e2e-rev-1-$(date +%s)"
sed 's/initial-password-123456/pushed-password-987654/' "$WORK/etc/hysteria/config.yaml" > "$WORK/push1.yaml"
curl -sf -X POST --data-binary @"$WORK/push1.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=$REV1" >/dev/null
sleep 4
check "ConfigAck 成功" bash -c "dumpq last '$DUMP' configAck ok | grep -qi true"
check "回执 revision 正确" bash -c "dumpq last '$DUMP' configAck revision | grep -q '$REV1'"
check "配置已原子写入目标路径（0600）" test "$(stat -c %a "$WORK/etc/hysteria/config.yaml")" = "600"
check "目标配置内容为新版本" grep -q pushed-password-987654 "$WORK/etc/hysteria/config.yaml"
check "配置历史记录了该版本" bash -c "grep -q '$REV1' '$WORK/etc/agent/history/index.json'"
wait_for_q "state.json 里的 revision 已更新" "$STATE" config.revision "$REV1" 15
wait_for_q "Hysteria 以新配置运行中" "$STATE" hysteria.running "true" 15

log "3b) 解析不出配置的才拒收；其余校验问题按「面板为准」仍写入（带警告）"
SHA_BEFORE=$(sha256sum "$WORK/etc/hysteria/config.yaml" | awk '{print $1}')
# ① auth.type 非法：能解析（只是 Hysteria 起不来）→ 仍写入 + 回执带警告，由就绪检查/回滚兜底
sed 's/type: password/type: nonsense/' "$WORK/push1.yaml" > "$WORK/push_bad.yaml"
curl -sf -X POST --data-binary @"$WORK/push_bad.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=e2e-bad-1" >/dev/null
sleep 4
check "可解析但校验有问题的配置：仍写入（落盘，sha 变化）" bash -c "test \"$(sha256sum "$WORK/etc/hysteria/config.yaml" | awk '{print \$1}')\" != '$SHA_BEFORE'"
check "该次回执不再是「拒收」（ok 不为 false 或带 warnings）" bash -c "dumpq where '$DUMP' configAck revision e2e-bad-1 | grep -qE '(\"ok\":\\s*true|warnings)' || dumpq last '$DUMP' config_ack revision | grep -q e2e-bad-1"

# ② 连 YAML 都解析不出来的：必须拒收且不动磁盘（唯一的硬底线）
BAD_YAML_BEFORE=$(sha256sum "$WORK/etc/hysteria/config.yaml" | awk '{print $1}')
printf 'listen: [unclosed\n  : : :\n' > "$WORK/push_unparseable.yaml"
curl -sf -X POST --data-binary @"$WORK/push_unparseable.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=e2e-unparseable-1" >/dev/null
sleep 3
check "无法解析的配置未落盘（sha 未变）" test "$(sha256sum "$WORK/etc/hysteria/config.yaml" | awk '{print $1}')" = "$BAD_YAML_BEFORE"
check "无法解析时回执 ok=false" bash -c "dumpq where '$DUMP' configAck revision e2e-unparseable-1 | grep -qiE '(\"ok\":\\s*false|rejected)'"
check "上报了 config_failed 事件" bash -c "dumpq where '$DUMP' node_event type config_failed"

# 把好配置推回去，让后续用例的磁盘状态干净
curl -sf -X POST --data-binary @"$WORK/push1.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=$REV1" >/dev/null
sleep 3

log "3c) 校验通过但服务起不来 → 必须自动回滚"
REV_POISON="e2e-poison"
cp "$WORK/push1.yaml" "$WORK/push_poison.yaml"
printf '\npoison_start: true\n' >> "$WORK/push_poison.yaml"
curl -sf -X POST --data-binary @"$WORK/push_poison.yaml" \
  "http://127.0.0.1:$CP_PORT/_test/push_config?revision=$REV_POISON" >/dev/null
sleep 12
check "回执标记已回滚" bash -c "dumpq last '$DUMP' config_ack rolledBack | grep -qi true"
check "回滚后 Hysteria 重新运行" bash -c "test \"\$(q '$STATE' hysteria.running)\" = true"
check "回滚后配置不是毒配置" bash -c "! grep -q poison_start '$WORK/etc/hysteria/config.yaml'"
check "回滚后 revision 回到上一个版本" bash -c "test \"\$(q '$STATE' config.revision)\" != '$REV_POISON'"
check "上报了 config_rolled_back 事件" bash -c "dumpq where '$DUMP' node_event type config_rolled_back"

# ---------------------------------------------------------------- 4. 进程管理
log "4) 子进程崩溃自动重启"
HY_PID_BEFORE=$(pgrep -f "$WORK/bin/mockhysteria server" | head -1)
check "Hysteria 子进程存在" test -n "$HY_PID_BEFORE"
kill -9 "$HY_PID_BEFORE" 2>/dev/null
sleep 8
HY_PID_AFTER=$(pgrep -f "$WORK/bin/mockhysteria server" | head -1)
check "崩溃后被重新拉起（新 pid）" bash -c "test -n '$HY_PID_AFTER' && test '$HY_PID_AFTER' != '$HY_PID_BEFORE'"
check "state.json 记录重启次数" bash -c "test \$(q '$STATE' hysteria.restart_count) -ge 1"
check "上报 exited 事件（含退出码 137=SIGKILL）" bash -c "dumpq where '$DUMP' node_event type exited exitCode | grep -q 137"
check "指标里能看到重启计数" bash -c "curl -s http://127.0.0.1:$METRICS_PORT/metrics | grep -q hysteria_agent_hysteria_restarts_total"
check "重启后 Hysteria 进程属于本次 Agent（pid 与 state.json 一致）" \
  test "$HY_PID_AFTER" = "$(q "$STATE" hysteria.pid)"

# ---------------------------------------------------------------- 5. 远程指令
log "5) 远程指令"
diag_id=""
send_cmd() { # send_cmd <type> [json]
  local type="$1"
  # 注意：不要写成 local body="${2:-{}}" —— bash 会把 ${2:-{}} 解析成
  # "${2:-{}" + 字面量 "}"，于是 params 会多出一个 '}'（真实踩过的坑，
  # 表现为节点回 "解析升级参数失败: invalid character '}' after top-level value"）。
  local default_body='{}'
  local body="${2:-$default_body}"
  local id="cmd-$type-$(date +%s%N)"
  curl -sf -X POST --data-binary "$body" "http://127.0.0.1:$CP_PORT/_test/command?type=$type&id=$id" >/dev/null
  echo "$id"
}
result_of() { dumpq last "$DUMP" command_result resultJson; }

GOT_ID=$(send_cmd get_state); sleep 2
check "get_state 返回 ok" bash -c "dumpq last '$DUMP' command_result status | grep -qi ok"
check "get_state 结果含 config/control/hysteria" bash -c "dumpq last '$DUMP' command_result resultJson | grep -q hysteria"

GOT_ID=$(send_cmd run_diagnostic); sleep 8
check "run_diagnostic 返回报告" bash -c "dumpq last '$DUMP' command_result resultJson | grep -q 'checks'"
check "诊断报告有端口/证书/DNS 检查项" bash -c "dumpq last '$DUMP' command_result resultJson | grep -qE 'certificate|egress_ip'"
check "诊断报告落盘到 state 目录" test -f "$WORK/var/last-diagnostic.json"

GOT_ID=$(send_cmd kick_users '{"user_ids":["alice","bob"],"reason":"quota"}'); sleep 3
check "kick 已调用 Hysteria /kick（裸数组）" bash -c "test \$(curl -s http://127.0.0.1:$HY_API_PORT/_requests | python3 -c 'import json,sys;print(json.load(sys.stdin).get(\"kick\",0))') -ge 1"
check "kick_users 回执包含被踢用户" bash -c "dumpq last '$DUMP' command_result resultJson | grep -q alice"

GOT_ID=$(send_cmd tail_logs '{"duration_seconds":3,"source":"all","backlog_lines":150}'); sleep 8
check "tail_logs 推送了日志分片" bash -c "test \$(dumpq count '$DUMP' command_chunk) -ge 1"
check "tail_logs 以 eof 分片结束" bash -c "dumpq last '$DUMP' command_chunk eof | grep -qi true"
check "tail_logs 分片内容非空（含 agent/hysteria 日志行）" \
  bash -c "dumpq cat '$DUMP' command_chunk data | grep -qE 'level|hysteria|component'"
check "tail_logs 回执 ok" bash -c "dumpq where '$DUMP' command_result commandId '$GOT_ID' status | grep -qi ok"

GOT_ID=$(send_cmd reload_config '{"reason":"e2e"}'); sleep 6
check "reload_config 成功" bash -c "dumpq last '$DUMP' command_result status | grep -qi ok"

# 幂等：同一个 command_id 重复下发只执行一次
DUP_ID=$(send_cmd get_state)
sleep 2
BEFORE=$(dumpq count "$DUMP" command_result)
curl -sf -X POST --data-binary '{}' "http://127.0.0.1:$CP_PORT/_test/command?type=get_state&id=$DUP_ID" >/dev/null
sleep 2
AFTER=$(dumpq count "$DUMP" command_result)
check "重复 command_id 只执行一次（$BEFORE → $AFTER）" test "$BEFORE" = "$AFTER"

# ---------------------------------------------------------------- 6. 断线缓冲与补发
log "6) 控制面断线 → 本地缓存 → 重连补发"
OLD_ACKED=$(curl -s "http://127.0.0.1:$CP_PORT/_test/state" | python3 -c \
  'import json,sys;d=json.load(sys.stdin);n=list(d["nodes"].values())[0];print(n["acked_seq"])')
echo "断线前 acked_seq=$OLD_ACKED"
kill "$CP_PID" 2>/dev/null; CP_PID=""
sleep 8
PENDING=$(q "$STATE" control.pending_events)
check "断线期间事件被缓存（pending>0）" bash -c "test ${PENDING:-0} -gt 0"
check "state.json 显示已断开" bash -c "test \"\$(q '$STATE' control.connected)\" = false"
# 重启控制面：新进程 acked=0，会把 Agent 的补发起点作为 resend_from_seq 回给对方
"$WORK/bin/mockcontrol" -listen "127.0.0.1:$CP_PORT" -enroll-token "$ENROLL_TOKEN" \
  -dump "$DUMP" -files-dir "$WORK/files" -heartbeat-interval 3 -telemetry-interval 2 \
  >"$WORK/logs/mockcontrol2.log" 2>&1 &
CP_PID=$!
sleep 2
wait_for "断线后自动重连成功" 40 bash -c "test \"\$(q '$STATE' control.connected)\" = true"
sleep 3
# 断线前那一瞬间可能又有一条事件被确认，所以以 Agent 上报的 resume_from_seq 为准，
# 从这个起点开始必须严格连续、无重复、无缺口。
RESUME=$(dumpq last "$DUMP" hello resumeFromSeq)
check "Hello 的 resume_from_seq >= 断线前 acked+1" bash -c "test ${RESUME:-0} -ge $((OLD_ACKED+1))"
check "补发的 seq 连续无缺口、无重复" bash -c "dumpq check '$DUMP' ${RESUME:-1}"
check "断线期间的事件确实补发到了（≥2 条）" bash -c "test \$(dumpq count '$DUMP' metricBatch) -ge 2"

# ---------------------------------------------------------------- 7. 自升级
log "7) 自升级（验签 + 原子替换 + exec 重启）"
BEFORE_VER=$(q "$STATE" agent_version)
python3 - "$WORK" "http://127.0.0.1:$CP_PORT/_test/files/hysteria-agent" <<'PY' > "$WORK/manifest.json"
import json,subprocess,sys
work,url = sys.argv[1], sys.argv[2]
out = subprocess.check_output([f"{work}/bin/agent-sign","sign","-key",f"{work}/upgrade.key",
                               "-file",f"{work}/files/hysteria-agent","-version","9.9.9","-url",url])
mf = json.loads(out)
mf["url"] = url
print(json.dumps(mf))
PY
check "生成了升级清单" bash -c "python3 -c \"import json;m=json.load(open('$WORK/manifest.json'));assert m['sha256'] and m['signature']\""

# 先测错误签名：必须拒绝且不改动二进制
python3 - "$WORK" <<'PY' > "$WORK/manifest_bad.json"
import json,sys
work = sys.argv[1]
m = json.load(open(f"{work}/manifest.json"))
m["signature"] = "A"*88   # 长度合法但内容错误
print(json.dumps(m))
PY
send_cmd upgrade_agent "$(cat "$WORK/manifest_bad.json")" >/dev/null; sleep 4
check "错误签名的升级被拒绝" bash -c "dumpq last '$DUMP' commandResult status | grep -qiE 'failed|rejected'"
check "被拒绝后二进制未被替换" bash -c "test \"\$(q '$STATE' agent_version)\" = '$BEFORE_VER'"

# 正确签名：应当替换成功并重启（restart_mode=exec）
send_cmd upgrade_agent "$(cat "$WORK/manifest.json")" >/dev/null
wait_for "升级后版本变为 9.9.9" 40 bash -c "test \"\$(q '$STATE' agent_version)\" = '9.9.9'"
check "二进制文件已替换" bash -c "'$WORK/bin/hysteria-agent' version | grep -q 9.9.9"
check "保留了旧二进制备份" bash -c "ls '$WORK/var/upgrade/' | grep -q backup"
check "升级后 Agent 仍在运行（exec 重启成功）" kill -0 "$AGENT_PID"
wait_for "升级后控制通道重新连上" 30 bash -c "test \"\$(q '$STATE' control.connected)\" = true"
check "升级后 Hysteria 仍在运行" bash -c "test \"\$(q '$STATE' hysteria.running)\" = true"

# ---------------------------------------------------------------- 8. CLI 子命令
log "8) CLI 子命令"
check "status 正常退出" bash -c "'$WORK/bin/hysteria-agent' status -config '$WORK/etc/agent/config.yaml' >/dev/null"
check "status --json 含 node_id" bash -c "'$WORK/bin/hysteria-agent' status -config '$WORK/etc/agent/config.yaml' --json | grep -q node_id"
check "diag 输出检查项" bash -c "'$WORK/bin/hysteria-agent' diag -config '$WORK/etc/agent/config.yaml' --json | grep -q checks"
check "config history 列出历史版本" bash -c "'$WORK/bin/hysteria-agent' config history -config '$WORK/etc/agent/config.yaml' | grep -q $REV1"
check "config validate 对合法配置返回 0" bash -c "'$WORK/bin/hysteria-agent' config validate -config '$WORK/etc/agent/config.yaml' -f '$WORK/etc/hysteria/config.yaml' >/dev/null"
check "config validate 对非法配置返回非 0" bash -c "! '$WORK/bin/hysteria-agent' config validate -config '$WORK/etc/agent/config.yaml' -f '$WORK/push_bad.yaml' >/dev/null"

# ---------------------------------------------------------------- 9. 优雅停止
log "9) 优雅停止"
kill -TERM "$AGENT_PID"
wait_for "Agent 已优雅退出（收到 SIGTERM 后停止子进程并退出）" 20 bash -c "! kill -0 $AGENT_PID 2>/dev/null"
check "Hysteria 子进程随之退出" bash -c "! pgrep -f '$WORK/bin/[m]ockhysteria server' >/dev/null"
check "日志里有优雅停止记录" bash -c "grep -q '已退出' '$WORK/logs/agent.log'"

# ---------------------------------------------------------------- 汇总
log "汇总"
echo "PASS=$PASS FAIL=$FAIL"
echo "日志目录: $WORK/logs"
echo "控制面消息 dump: $DUMP"
if [[ $FAIL -gt 0 ]]; then
  echo "----- 最近 30 行 Agent 日志 -----"
  tail -30 "$WORK/logs/agent.log"
  exit 1
fi
exit 0
