#!/usr/bin/env bash
# 复现用户场景：从 GitHub 克隆下来的仓库（没有 bin/），直接跑 install.sh 应当能用
# 仓库里自带的编译产物（dist/ 或 release/）装起来。
#
# 用法：bash tests/install_from_clone.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLONE="${CLONE_DIR:-/var/lib/hysteria-agent-clone-test}"
WORK="$CLONE/work"
CP_PORT=18086
HY_PORT=18544
HY_API=18087
AGENT_SERVICE=hysteria-agent-clonetest
REAL_CFG=/etc/hysteria/config.yaml

# 工具二进制（mockcontrol 等）：优先 bin/（make tools），否则从 release/ 包里取
pick_tool() {
  local name="$1" t
  [[ -x "$ROOT/bin/$name" ]] && { echo "$ROOT/bin/$name"; return 0; }
  for t in "$ROOT"/release/*/bin/"$name"; do
    [[ -x "$t" ]] && { echo "$t"; return 0; }
  done
  return 1
}
MOCKCONTROL="$(pick_tool mockcontrol || true)"
[[ -n "$MOCKCONTROL" ]] || { echo "找不到 mockcontrol：先 make tools，或解包 release/*.tar.gz"; exit 1; }

PASS=0; FAIL=0
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
check(){ local d="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi; }
log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

cleanup() {
  AGENT_SERVICE="$AGENT_SERVICE" bash "$CLONE/install.sh" --uninstall >/dev/null 2>&1
  pkill -f "mockcontrol -listen 127.0.0.1:$CP_PORT" 2>/dev/null
  sleep 1
}
trap cleanup EXIT INT TERM

REAL_MD5_BEFORE="$(md5sum "$REAL_CFG" | awk '{print $1}')"

log "1) 全新 clone（模拟用户环境：没有 bin/ 目录）"
rm -rf "$CLONE"; mkdir -p "$CLONE"
git clone -q "$ROOT" "$CLONE/repo"
CLONE_REPO="$CLONE/repo"
check "clone 里没有 bin/hysteria-agent（复现用户遇到的场景）" bash -c "! test -x '$CLONE_REPO/bin/hysteria-agent'"
check "clone 里有仓库自带的编译产物 dist/" test -x "$CLONE_REPO/dist/hysteria-agent-linux-amd64"
check "clone 里有发布包 release/*.tar.gz" bash -c "ls '$CLONE_REPO'/release/hysteria-agent-*-linux-amd64.tar.gz >/dev/null"
mkdir -p "$WORK"

log "2) 起假控制面（工具从工作树拿，被测的是 install.sh）"
"$MOCKCONTROL" -listen "127.0.0.1:$CP_PORT" -enroll-token clone-test-token \
  -dump "$WORK/dump.jsonl" > "$WORK/mock.log" 2>&1 &
sleep 1
check "假控制面就绪" curl -sf "http://127.0.0.1:$CP_PORT/healthz"

log "3) 造一份隔离的 Hysteria 配置（基于生产配置副本）"
python3 - "$REAL_CFG" "$WORK/hysteria.yaml" "$HY_PORT" "$HY_API" <<'PY'
import re, sys
src, dst, port, api = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
out, done = [], False
for line in open(src).read().splitlines():
    if re.match(r'^\s*#?\s*listen\s*:', line) and not done:
        out.append(f'listen: 127.0.0.1:{port}'); done = True; continue
    out.append(line)
if not done:
    out.insert(0, f'listen: 127.0.0.1:{port}')
text = "\n".join(out) + "\n"
text = re.sub(r'(?s)(trafficStats:\n(?:[ \t]+.*\n)+)',
              lambda m: re.sub(r'(?m)^(\s*)listen:\s*.*$', lambda x: f'{x.group(1)}listen: 127.0.0.1:{api}', m.group(1), count=1),
              text, count=1)
open(dst, 'w').write(text)
PY
TS_SECRET="$(awk '/trafficStats:/{f=1} f&&/secret:/{sub(/^[[:space:]]*secret:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$WORK/hysteria.yaml")"

log "4) 直接跑 install.sh（不加任何 AGENT_BINARY）"
AGENT_SERVICE="$AGENT_SERVICE" AGENT_NO_DOWNLOAD=1 bash "$CLONE_REPO/install.sh" "http://127.0.0.1:$CP_PORT" \
  --token=clone-test-token --name=clone-test --hysteria-config "$WORK/hysteria.yaml" \
  > "$WORK/install.log" 2>&1
RC=$?
tail -20 "$WORK/install.log"
check "install.sh 退出码 0" test "$RC" = "0"
check "日志显示使用了仓库自带的二进制" bash -c "grep -qE '使用本地二进制|已从 GitHub 下载' '$WORK/install.log'"
check "日志里没有'找不到 agent 二进制'的错误" bash -c "! grep -qE '(没有)?找到 agent 二进制' '$WORK/install.log'"

log "5) 安装结果"
check "Agent 服务在运行" systemctl is-active "$AGENT_SERVICE"
check "/readyz 返回 200" bash -c "for i in \$(seq 1 20); do curl -sf http://127.0.0.1:9100/readyz >/dev/null && exit 0; sleep 1; done; exit 1"
check "enroll 成功（凭据 0400）" test "$(stat -c %a /etc/hysteria-agent/credentials.json)" = "400"
check "隔离实例的 trafficStats API 可读" bash -c \
  "test \$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: $TS_SECRET' http://127.0.0.1:$HY_API/traffic) = 200"

log "6) 卸载并确认生产环境未受影响"
AGENT_SERVICE="$AGENT_SERVICE" bash "$CLONE_REPO/install.sh" --uninstall >/dev/null 2>&1
sleep 2
check "服务已停用" bash -c "! systemctl is-active $AGENT_SERVICE >/dev/null 2>&1"
check "生产 Hysteria 配置未被改动" test "$(md5sum "$REAL_CFG" | awk '{print $1}')" = "$REAL_MD5_BEFORE"

log "汇总"
echo "PASS=$PASS FAIL=$FAIL   （日志 $WORK/install.log）"
[[ $FAIL -eq 0 ]] || exit 1
