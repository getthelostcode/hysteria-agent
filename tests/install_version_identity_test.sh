#!/usr/bin/env bash
#
# tests/install_version_identity_test.sh —— 「同一份代码只有一个版本身份」
#
# 现场：仓库里发布的 dist 二进制自报 61145e9，但节点上 install.sh 走源码重建时注入的是
# `git rev-parse --short HEAD`（= 打包提交 2f7cc90，只有 dist/ 变化）—— 同一份代码两个版本号，
# 事后对账（面板/节点/发布）全对不上。用户原话：「退回到这个版本 hysteria-agent 0.1.0 (61145e9…)」。
#
# 这里断言：
#   ① 版本标识取「最后改动 agent 源码的提交」，不是 HEAD；源码脏了补 -dirty
#   ② 发布产物带 .commit 记录，install.sh 按它判断过期（打包提交不再被误判成「有新版」）
#   ③ 真仓库里 dist 的 .commit 与二进制自报的 commit 一致
#   ④ 沙箱里跑真 install.sh：--no-rebuild 原样装发布产物；过期时才源码重建（且标签是源码提交）
#
# 用法：bash tests/install_version_identity_test.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1  →  $2"; fi; }
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

echo "== 1) 造一个真 git 仓库：源码提交 c1 → 打包提交 c2（HEAD，只动 dist/）"
REPO="${WORK}/repo"; mkdir -p "${REPO}/dist" "${REPO}/cmd/hysteria-agent" "${REPO}/internal/version" "${WORK}/bin"
cd "${REPO}"
git init -q .
git config user.email t@t; git config user.name t
cat > cmd/hysteria-agent/main.go <<'GO'
package main
func main() {}
GO
cat > internal/version/version.go <<'GO'
package version
var Version, Commit, BuildTime string
GO
git add -A && git commit -qm "feat: 源码提交"
SRC_COMMIT="$(git rev-parse --short HEAD)"
printf '#!/bin/sh\necho "hysteria-agent 0.1.0 (%s, 2026-10-16T00:43:27Z)"\n' "$SRC_COMMIT" > dist/hysteria-agent-linux-amd64
chmod +x dist/hysteria-agent-linux-amd64
printf '%s\n' "$SRC_COMMIT" > dist/hysteria-agent-linux-amd64.commit
git add -A && git commit -qm "chore(dist): 重建发布二进制（只有 dist/ 变化）"
HEAD_COMMIT="$(git rev-parse --short HEAD)"
[[ "$HEAD_COMMIT" != "$SRC_COMMIT" ]] && ok "夹具：打包提交 ${HEAD_COMMIT} ≠ 源码提交 ${SRC_COMMIT}" || bad "夹具没造对"

echo "== 2) 抽取 install.sh 的版本相关函数并断言（不碰真仓库）"
LIB="${WORK}/lib.sh"; : > "${LIB}"
{
  printf 'log()  { printf "[OK] %%s\\n" "$*"; }\ninfo() { printf "[INFO] %%s\\n" "$*"; }\nwarn() { printf "[WARN] %%s\\n" "$*"; }\ndie()  { printf "[FAIL] %%s\\n" "$*" >&2; exit 1; }\n'
  printf 'SCRIPT_DIR="%s"\nAGENT_VERSION=0.1.0\n' "${REPO}"
  for fn in agent_source_commit dist_recorded_commit agent_binary_stale agent_go build_agent_from_source; do
    extract_fn "${fn}"; echo
  done
} > "${LIB}"
bash -n "${LIB}" || { echo "抽取的函数有语法错误"; exit 1; }
# shellcheck disable=SC1090
source "${LIB}"

check "① 版本标识 = 最后改动源码的提交（不是 HEAD 那个打包提交）" \
      "[ \"\$(agent_source_commit)\" = '${SRC_COMMIT}' ]"

printf '\n// 未提交的改动\n' >> cmd/hysteria-agent/main.go
check "① 源码有未提交改动 → 标 -dirty（不谎报成干净提交）" \
      "[ \"\$(agent_source_commit)\" = '${SRC_COMMIT}-dirty' ]"
git checkout -q -- cmd/hysteria-agent/main.go

check "② .commit 与源码提交一致 → 发布产物不算过期" \
      "! agent_binary_stale '${REPO}/dist/hysteria-agent-linux-amd64'"
printf 'deadbee\n' > dist/hysteria-agent-linux-amd64.commit
check "② .commit 与源码提交不一致 → 判定过期（该重建）" \
      "agent_binary_stale '${REPO}/dist/hysteria-agent-linux-amd64'"
rm -f dist/hysteria-agent-linux-amd64.commit
touch cmd/hysteria-agent/main.go
check "② 没有 .commit 记录 → 退回 mtime 兜底（源码更新就算过期）" \
      "agent_binary_stale '${REPO}/dist/hysteria-agent-linux-amd64'"
printf '%s\n' "$SRC_COMMIT" > dist/hysteria-agent-linux-amd64.commit

echo "== 3) 源码重建注入的 commit 必须是源码提交（go shim 抓 ldflags）"
cat > "${WORK}/bin/go" <<'SHIM'
#!/bin/bash
printf '%s\n' "$*" >> "${SHIM_LOG}"
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in -o) out="$2"; shift 2 ;; *) shift ;; esac
done
[ -n "$out" ] || exit 3
printf '#!/bin/sh\necho "hysteria-agent 0.1.0 (REBUILT, 2026-01-01T00:00:00Z)"\n' > "$out"
chmod +x "$out"
SHIM
chmod +x "${WORK}/bin/go"
export SHIM_LOG="${WORK}/go-argv.log"; : > "${SHIM_LOG}"
export GO="${WORK}/bin/go"
export PATH="${WORK}/bin:${PATH}"
build_agent_from_source "${WORK}/rebuilt" >/dev/null 2>&1
check "③ 重建命令带 -X ...Commit=${SRC_COMMIT}" "grep -q -- '-X github.com/getthelostcode/hysteria-agent/internal/version.Commit=${SRC_COMMIT}' '${SHIM_LOG}'"
check "③ 重建命令**不带**打包提交 ${HEAD_COMMIT}" "! grep -q -- \"Commit=${HEAD_COMMIT}\" '${SHIM_LOG}'"

echo "== 4) 真仓库自洽：dist 的 .commit 与二进制自报的 commit 一致"
REAL_COMMIT_FILE="$(cat "${ROOT}/dist/hysteria-agent-linux-amd64.commit" 2>/dev/null | tr -d '[:space:]')"
check "④ dist/*.commit 存在且非空" "[ -n '${REAL_COMMIT_FILE}' ]"
check "④ 二进制自报的 commit == .commit 文件" \
      "${ROOT}/dist/hysteria-agent-linux-amd64 version 2>/dev/null | grep -qF \"(${REAL_COMMIT_FILE},\""
check "④ .commit == 最后改动源码的提交（不是打包提交）" \
      "[ '${REAL_COMMIT_FILE}' = \"\$(cd '${ROOT}' && git log -1 --format=%h -- cmd internal '*.go')\" ]"

echo "== 4b) 解析顺序：发布产物 dist/ 必须优先于本地遗留的 bin/"
mkdir -p "${REPO}/bin"
printf '#!/bin/sh\necho "hysteria-agent 0.1.0 (STALE01, 2026-10-15T12:52:37Z)"\n' > "${REPO}/bin/hysteria-agent"
chmod +x "${REPO}/bin/hysteria-agent"
{
  printf 'log() { :; }\ninfo() { :; }\nwarn() { :; }\ndie() { exit 1; }\n'
  printf 'SCRIPT_DIR="%s"\nAGENT_VERSION=0.1.0\nARCH=amd64\nAGENT_BINARY=""\n' "${REPO}"
  extract_fn resolve_local_agent; echo
} > "${WORK}/lib2.sh"
bash -n "${WORK}/lib2.sh" || { echo "resolve_local_agent 抽取失败"; exit 1; }
# shellcheck disable=SC1090
( source "${WORK}/lib2.sh"; resolve_local_agent ) > "${WORK}/picked.txt"
check "④b 选中发布产物 dist/（不是遗留的 bin/）" \
      "grep -q 'dist/hysteria-agent-linux-amd64$' '${WORK}/picked.txt'"

echo "== 5) 沙箱里跑真 install.sh（假 systemctl + 单元目录指向沙箱，绝不碰宿主）"
cat > "${WORK}/bin/systemctl" <<'SHIM'
#!/bin/bash
printf '[shim] systemctl %s\n' "$*" >> "${SHIM_LOG_SYSTEMCTL:-/dev/null}"
exit 0
SHIM
chmod +x "${WORK}/bin/systemctl"
export SHIM_LOG_SYSTEMCTL="${WORK}/systemctl.log"; : > "${SHIM_LOG_SYSTEMCTL}"

SANDBOX="${WORK}/sandbox"; mkdir -p "${SANDBOX}/bin" "${SANDBOX}/etc" "${SANDBOX}/units" "${SANDBOX}/var" "${SANDBOX}/logs"
run_install() { # run_install <标签> [额外参数...]
  local tag="$1"; shift
  AGENT_BIN="${SANDBOX}/bin/hysteria-agent" \
  AGENT_CFG_DIR="${SANDBOX}/etc" \
  AGENT_STATE="${SANDBOX}/var" \
  SYSTEMD_UNIT_DIR="${SANDBOX}/units" \
  AGENT_SERVICE="ha-identity-test" \
  HYSTERIA_CONFIG="${SANDBOX}/etc/hysteria.yaml" \
    bash "${ROOT}/install.sh" "http://127.0.0.1:9" --no-hysteria --skip-enroll --no-sync-config "$@" \
    > "${SANDBOX}/logs/${tag}.log" 2>&1
}

if run_install pinned --no-rebuild; then ok "沙箱安装（--no-rebuild）成功退出"; else bad "沙箱安装失败：$(tail -4 "${SANDBOX}/logs/pinned.log")"; fi
check "⑤ 装的是现成产物、没有源码重建" "grep -q '使用本地二进制' '${SANDBOX}/logs/pinned.log'"
check "⑤ 没有任何源码重建动作" "! grep -q '已改用当前源码重新构建' '${SANDBOX}/logs/pinned.log'"
check "⑤ 装出来的二进制自报发布版本（${REAL_COMMIT_FILE}）" \
      "'${SANDBOX}/bin/hysteria-agent' version 2>/dev/null | grep -qF '(${REAL_COMMIT_FILE},'"
check "⑤ 全程没有往宿主单元目录写东西" "[ ! -e /etc/systemd/system/ha-identity-test.service ]"

echo
printf '结果：%d 通过 / %d 失败\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
