#!/usr/bin/env bash
# 真机安装测试：用 install.sh 在本机装一套"隔离实例"（独立服务名 + 独立 Hysteria 配置副本），
# 验证完再卸载，并证明生产用的 /etc/hysteria/config.yaml 与 hysteria-server 未受影响。
#
# 用法：bash tests/install_test.sh
#
# 注意：工作目录**不要**放在 /tmp 下——生产的 systemd 单元带 PrivateTmp=true，
# 服务进程看到的 /tmp 是私有 tmpfs，会出现"配置文件明明存在，Agent 却说找不到"
# 的假故障（真机安装测试里踩过）。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${INSTALL_TEST_WORK:-/var/lib/hysteria-agent-install-test}"
CP_PORT=18080
HY_PORT=18443
HY_API=18082
AGENT_SERVICE=hysteria-agent-test
REAL_CFG=/etc/hysteria/config.yaml

PASS=0; FAIL=0
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
check(){ local d="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi; }
log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

cleanup() {
  AGENT_SERVICE="$AGENT_SERVICE" bash "$ROOT/install.sh" --uninstall >/dev/null 2>&1
  pkill -f "mockcontrol -listen 127.0.0.1:$CP_PORT" 2>/dev/null
  sleep 1
}
trap cleanup EXIT INT TERM

[[ $EUID -eq 0 ]] || { echo "需要 root"; exit 1; }
command -v systemctl >/dev/null || { echo "需要 systemd"; exit 1; }

REAL_MD5_BEFORE="$(md5sum "$REAL_CFG" 2>/dev/null | awk '{print $1}')"
REAL_ACTIVE_BEFORE="$(systemctl is-active hysteria-server 2>/dev/null || true)"
REAL_HY_PID_BEFORE="$(systemctl show -p MainPID --value hysteria-server 2>/dev/null || true)"
echo "生产环境基线: config md5=$REAL_MD5_BEFORE hysteria-server=$REAL_ACTIVE_BEFORE pid=$REAL_HY_PID_BEFORE"

log "准备隔离环境 $WORK"
rm -rf "$WORK"; mkdir -p "$WORK"
pkill -f "mockcontrol -listen 127.0.0.1:$CP_PORT" 2>/dev/null
sleep 0.5
setsid "$ROOT/bin/mockcontrol" -listen "127.0.0.1:$CP_PORT" -enroll-token install-test-token \
  -dump "$WORK/dump.jsonl" > "$WORK/mock.log" 2>&1 < /dev/null &
sleep 1

# 复制生产 Hysteria 配置，只改 listen / trafficStats（证书路径保持原样，root 可读）
# 注意：生产配置里 listen 可能是注释掉的（Hysteria 默认 :443），所以要显式"取消注释并改端口"，
# 否则隔离实例会去抢 443 和生产实例冲突（这里真实踩到过）。
python3 - "$REAL_CFG" "$WORK/hysteria.yaml" "$HY_PORT" "$HY_API" <<'PY'
import re, sys
src, dst, port, api = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
text = open(src).read()

lines = text.splitlines()
out, done = [], False
for line in lines:
    if re.match(r'^\s*#?\s*listen\s*:', line) and not done:
        out.append(f'listen: 127.0.0.1:{port}')
        done = True
        continue
    out.append(line)
if not done:
    out.insert(0, f'listen: 127.0.0.1:{port}')
text = "\n".join(out) + "\n"

if 'trafficStats:' in text:
    def fix(block):
        return re.sub(r'(?m)^(\s*)listen:\s*.*$',
                      lambda m: f'{m.group(1)}listen: 127.0.0.1:{api}', block, count=1)
    text = re.sub(r'(?s)(trafficStats:\n(?:[ \t]+.*\n)+)', lambda m: fix(m.group(1)), text, count=1)
else:
    text += f'\ntrafficStats:\n  listen: 127.0.0.1:{api}\n  secret: install-test-secret\n'
open(dst, 'w').write(text)
print("--- 隔离配置 ---")
print(text)
PY
TS_SECRET="$(awk '/trafficStats:/{f=1} f&&/secret:/{sub(/^[[:space:]]*secret:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$WORK/hysteria.yaml")"
echo "隔离配置里的 trafficStats secret: ${TS_SECRET:-(空)}"

log "运行 install.sh"
AGENT_SERVICE="$AGENT_SERVICE" bash "$ROOT/install.sh" http://127.0.0.1:$CP_PORT \
  --token=install-test-token --name install-test --hysteria-config "$WORK/hysteria.yaml" \
  > "$WORK/install.log" 2>&1
INSTALL_RC=$?
tail -25 "$WORK/install.log"
check "install.sh 退出码为 0" test "$INSTALL_RC" = "0"

log "验证安装结果"
check "Agent 服务在运行（$AGENT_SERVICE）" systemctl is-active "$AGENT_SERVICE"
check "Agent 配置权限 0600" test "$(stat -c %a /etc/hysteria-agent/config.yaml)" = "600"
check "凭据文件权限 0400" test "$(stat -c %a /etc/hysteria-agent/credentials.json)" = "400"
check "/healthz 返回 200" curl -sf http://127.0.0.1:9100/healthz
check "/readyz 返回 200" curl -sf http://127.0.0.1:9100/readyz
check "/metrics 有 agent 指标" bash -c "curl -s http://127.0.0.1:9100/metrics | grep -q hysteria_agent_info"
check "控制通道已连上（state.json）" bash -c \
  "python3 -c \"import json;d=json.load(open('/var/lib/hysteria-agent/state.json'));assert d['control']['connected'] and d['hysteria']['running']\""
check "隔离实例的 Hysteria 在跑（agent 拉起的进程）" \
  bash -c "pgrep -f '[h]ysteria server -c $WORK/hysteria.yaml' >/dev/null"
check "隔离实例的 trafficStats API 可读（200）" \
  bash -c "test \$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: $TS_SECRET' http://127.0.0.1:$HY_API/traffic) = 200"
check "负例：不带 secret 必须 401" \
  bash -c "test \$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$HY_API/traffic) = 401"
check "hysteria-agent status 可用" bash -c "'$ROOT/bin/hysteria-agent' status | grep -q node_id"
check "hysteria-agent diag 通过" bash -c "'$ROOT/bin/hysteria-agent' diag >/dev/null || true"

log "验证 --verify-only"
AGENT_SERVICE="$AGENT_SERVICE" bash "$ROOT/install.sh" http://127.0.0.1:$CP_PORT --verify-only \
  > "$WORK/verify.log" 2>&1
check "--verify-only 退出码为 0" test "$?" = "0"
grep -E "^\s*(http|Hysteria|控制通道|node_id|就绪|配置|\[)" "$WORK/verify.log" | head -20

log "卸载并确认生产环境未受影响"
AGENT_SERVICE="$AGENT_SERVICE" bash "$ROOT/install.sh" --uninstall > "$WORK/uninstall.log" 2>&1 || true
sleep 2
check "服务已停用" bash -c "! systemctl is-active $AGENT_SERVICE >/dev/null 2>&1"
check "单元文件已删除" bash -c "! test -f /etc/systemd/system/$AGENT_SERVICE.service"
check "配置目录已删除" bash -c "! test -d /etc/hysteria-agent"
check "隔离实例的 Hysteria 进程已退出" bash -c "! pgrep -f '[h]ysteria server -c $WORK/hysteria.yaml' >/dev/null"

REAL_MD5_AFTER="$(md5sum "$REAL_CFG" 2>/dev/null | awk '{print $1}')"
REAL_ACTIVE_AFTER="$(systemctl is-active hysteria-server 2>/dev/null || true)"
REAL_HY_PID_AFTER="$(systemctl show -p MainPID --value hysteria-server 2>/dev/null || true)"
echo "生产环境现状: config md5=$REAL_MD5_AFTER hysteria-server=$REAL_ACTIVE_AFTER pid=$REAL_HY_PID_AFTER"
check "生产 Hysteria 配置未被改动" test "$REAL_MD5_BEFORE" = "$REAL_MD5_AFTER"
check "生产 hysteria-server 仍在运行" test "$REAL_ACTIVE_AFTER" = "active"
check "生产 hysteria-server 进程未重启（pid 不变）" test "$REAL_HY_PID_BEFORE" = "$REAL_HY_PID_AFTER"

log "汇总"
echo "PASS=$PASS FAIL=$FAIL"
echo "日志: $WORK/install.log / $WORK/verify.log"
[[ $FAIL -eq 0 ]] || exit 1
