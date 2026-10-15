#!/usr/bin/env bash
#
# tests/install_binary_freshness_test.sh —— 「别把仓库里那份旧二进制装上」的回归
#
# 现场：只 git pull 了新代码就重跑 install.sh，脚本会优先用仓库里入库的
# dist/hysteria-agent-linux-amd64 —— 那份可能还是几个月前的构建（缺新功能），
# 而且版本号看着一样，极难察觉（表现：面板发布的配置一直 applied_at 为空）。
#
# 用法：bash tests/install_binary_freshness_test.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
trap 'rm -rf "${WORK}"' EXIT

extract_fn() {
  awk -v fn="$1" '
    index($0, fn "()") == 1 {
      print
      if ($0 ~ /\}[[:space:]]*$/) exit
      capturing = 1; next
    }
    capturing && /^}/ { print; exit }
    capturing { print }
  ' "${ROOT}/install.sh"
}

LIB="${WORK}/lib.sh"; : > "${LIB}"
{
  printf 'log()  { :; }\ninfo() { :; }\nwarn() { printf "[WARN] %%s\\n" "$*"; }\ndie()  { printf "[FAIL] %%s\\n" "$*" >&2; exit 1; }\n'
  for fn in agent_binary_stale agent_go build_agent_from_source; do
    extract_fn "${fn}" >> "${LIB}"; echo >> "${LIB}"
  done
} 
bash -n "${LIB}" || { echo "抽取的函数有语法错误"; exit 1; }
# shellcheck disable=SC1090
source "${LIB}"

echo "== 造一个假仓库（源码 + 一份旧 dist 二进制）"
REPO="${WORK}/repo"
mkdir -p "${REPO}/dist" "${REPO}/cmd/hysteria-agent" "${REPO}/internal/version" "${WORK}/bin"
cat > "${REPO}/cmd/hysteria-agent/main.go" <<'GO'
package main
func main() {}
GO
cat > "${REPO}/internal/version/version.go" <<'GO'
package version
var Version, Commit, BuildTime string
GO
printf '#!/bin/sh\necho "hysteria-agent 0.1.0 (OLDOLD, 2026-10-15T12:52:37Z)"\n' > "${REPO}/dist/hysteria-agent-linux-amd64"
chmod +x "${REPO}/dist/hysteria-agent-linux-amd64"
OLD="${REPO}/dist/hysteria-agent-linux-amd64"

# go shim：记录参数，并把「新二进制」写到 -o 指定的路径
cat > "${WORK}/bin/go" <<'SHIM'
#!/bin/bash
printf '%s\n' "$*" >> "${SHIM_LOG}"
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[ -n "$out" ] || exit 3
printf '#!/bin/sh\necho "hysteria-agent 0.1.0 (NEWNEW, 2026-10-15T15:30:00Z)"\n' > "$out"
chmod +x "$out"
SHIM
chmod +x "${WORK}/bin/go"
export SHIM_LOG="${WORK}/go-args.log"
export GO="${WORK}/bin/go"
export SCRIPT_DIR="${REPO}"

echo "== 1) 过期判定"
touch -d '2026-10-15 15:00:00' "${REPO}/internal/version/version.go"
touch -d '2026-10-15 12:52:00' "${OLD}"
if agent_binary_stale "${OLD}"; then ok "源码比二进制新 → 判为过期"; else bad "应该判为过期却没判"; fi

echo "== 2) 二进制比源码新（刚构建过的检出）→ 不该重建"
touch -d '2026-10-15 16:00:00' "${OLD}"
if agent_binary_stale "${OLD}"; then bad "不该判为过期"; else ok "二进制更新 → 不重建"; fi
touch -d '2026-10-15 12:52:00' "${OLD}"

echo "== 3) 找 Go 工具链（GO 环境变量 / PATH / /usr/local/go）"
[[ "$(agent_go)" == "${WORK}/bin/go" ]] && ok "按 GO 变量找到工具链" || bad "agent_go 没返回 GO 指定的路径"
env -u GO bash -c "PATH='${WORK}/bin:/usr/bin:/bin' SCRIPT_DIR='${REPO}' bash -c '
  source ${LIB}; [ \"\$(agent_go)\" = \"${WORK}/bin/go\" ]'" \
  && ok "GO 未设时能从 PATH 找到 go" || bad "PATH 里找不到 go"

echo "== 4) 从源码重建：产物落在指定路径、带上真实 commit"
OUT="${WORK}/out/hysteria-agent"
mkdir -p "${WORK}/out"
mkdir -p "${REPO}/.git"; git -C "${REPO}" init -q 2>/dev/null
if build_agent_from_source "${OUT}"; then ok "重建成功"; else bad "重建失败"; fi
if grep -q -- "-o ${OUT} ./cmd/hysteria-agent" "${SHIM_LOG}"; then ok "调用了 go build -o <out> ./cmd/hysteria-agent"; else bad "构建命令不对：$(cat "${SHIM_LOG}" 2>/dev/null)"; fi
if grep -q "internal/version.Commit=" "${SHIM_LOG}"; then ok "ldflags 注入了 commit"; else bad "ldflags 没带 commit"; fi
if grep -q "internal/version.BuildTime=" "${SHIM_LOG}"; then ok "ldflags 注入了构建时间"; else bad "ldflags 没带构建时间"; fi
"${OUT}" version 2>/dev/null | head -1 | grep -q "NEWNEW" && ok "产物可用且是新构建" || bad "产物不可用"

echo "== 5) 没有 Go 工具链时如实失败（好让 install.sh 走 warn 分支，而不是静默装旧的）"
if ( GO=/nonexistent/go PATH=/usr/bin:/bin; build_agent_from_source "${WORK}/out/x" ) 2>/dev/null; then
  bad "没有工具链却报成功"
else
  ok "没有工具链时返回失败"
fi

printf '\nPASS=%d FAIL=%d\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
