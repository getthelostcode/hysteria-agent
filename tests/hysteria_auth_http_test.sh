#!/usr/bin/env bash
#
# tests/hysteria_auth_http_test.sh —— 校验「Hysteria 配置走 HTTP 鉴权」这件事
#
# 背景：节点上的 /etc/hysteria/config.yaml 若是 auth.type=password 的静态密码，用户离开平台后
# 依然能连，也没法按流量/到期回收。改成 HTTP 鉴权时**只能动 auth 段** —— 重新生成整份配置会
# 顺手改掉运维自己的证书路径与 masquerade。
#
# 用法：bash tests/hysteria_auth_http_test.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
trap 'rm -rf "${WORK}"' EXIT

# 只抽取需要的函数（不执行 install.sh 主流程：它会装二进制、写 systemd 单元、跑注册）
#
# 注意单行定义 `log() { ...; }`：它没有独立的 `}` 行，若只等 `^}` 会一路吃到文件末尾，
# 把主流程（解析参数、die 退出）一起拖进来 —— 实测会把测试脚本当场杀掉。
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
for fn in log info warn die emit_auth_block set_hysteria_auth_http; do
  extract_fn "${fn}" >> "${LIB}"; echo >> "${LIB}"
done
cat >> "${LIB}" <<'PRELUDE'
HAS_SYSTEMD=0        # 测试里不碰 systemd
HY_SERVICE="hysteria-server"
PRELUDE

bash -n "${LIB}" || { echo "抽取出的函数有语法错误"; exit 1; }
# shellcheck disable=SC1090
source "${LIB}"
# 抽取来的 log/info/warn/die 覆盖测试自己的 ok/bad，这里装回来（顺便让函数内部日志可见）
ok()   { PASS=$((PASS+1)); printf '[PASS] %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '[FAIL] %s\n' "$*"; }
log()  { printf '[ OK ] %s\n' "$*"; }
info() { printf '[INFO] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*"; }
die()  { printf '[FAIL] %s\n' "$*" >&2; exit 1; }

CFG="${WORK}/config.yaml"
cat > "${CFG}" <<'YAML'
listen: :443

tls:
  cert: /etc/hysteria/certs/server.crt
  key: /etc/hysteria/certs/server.key

auth:
  type: password
  password: 5634e41494f5

trafficStats:
  listen: 127.0.0.1:8080
  secret: 9da0f1431930a0e9ded898ed4e6663c6dea707df034268f8

masquerade:
  type: proxy
  proxy:
    url: https://news.ycombinator.com/
    rewriteHost: true
YAML
cp "${CFG}" "${WORK}/original.yaml"

echo "== 1) 生成时默认就是 HTTP 鉴权（emit_auth_block）"
gen="$(emit_auth_block http "" "https://auth.ipx.sarl" "0")"
[[ "$(printf '%s\n' "$gen" | wc -l)" == "5" ]] && ok "HTTP 鉴权块 5 行" || bad "HTTP 鉴权块行数不对"
grep -q "type: http" <<<"$gen" && ok "type: http" || bad "缺 type: http"
grep -q "url: https://auth.ipx.sarl" <<<"$gen" && ok "url 正确" || bad "url 不对"
grep -q "insecure: false" <<<"$gen" && ok "insecure: false" || bad "insecure 不对"
grep -q "type: password" <<<"$gen" && bad "HTTP 模式下不该出现密码" || ok "HTTP 模式下无密码字段"

echo "== 1b) 生成配置时用的 auth 必须来自 emit_auth_block（不然测的和跑的不是同一段）"
if grep -q 'AUTH_BLOCK="$(emit_auth_block' "${ROOT}/install.sh" && grep -qE '^\$AUTH_BLOCK$' "${ROOT}/install.sh"; then
  ok "生成路径复用 emit_auth_block"
else
  bad "生成路径没有复用 emit_auth_block（测试就失效了）"
fi

echo "== 2) password 模式仍可用（兼容老节点）"
legacy="$(emit_auth_block password "deadbeefcafe" "" "0")"
grep -q "type: password" <<<"$legacy" && grep -q "password: deadbeefcafe" <<<"$legacy" \
  && ok "password 模式保留" || bad "password 模式被破坏"

echo "== 3) 已有配置：只换 auth 段（https）"
set_hysteria_auth_http "${CFG}" "https://auth.ipx.sarl" "0" >/dev/null
grep -q "type: http" "${CFG}" && ok "auth.type 已是 http" || bad "auth.type 没改"
grep -q "type: password" "${CFG}" && bad "静态密码仍在" || ok "静态密码已移除"

echo "== 4) 其余内容一字不改"
for probe in "cert: /etc/hysteria/certs/server.crt" "key: /etc/hysteria/certs/server.key" \
             "listen: 127.0.0.1:8080" "secret: 9da0f1431930a0e9ded898ed4e6663c6dea707df034268f8" \
             "url: https://news.ycombinator.com/" "rewriteHost: true" "listen: :443"; do
  grep -qF "${probe}" "${CFG}" && ok "保留：${probe}" || bad "丢失：${probe}"
done

echo "== 5) auth 段结构 / 位置 / 权限 / 备份"
expected='auth:
  type: http
  http:
    url: https://auth.ipx.sarl
    insecure: false'
actual="$(awk '/^auth:/{f=1} f{print; if (f && /^[^[:space:]]/ && !/^auth:/) exit}' "${CFG}" | sed '/^[[:space:]]*$/d' | head -5)"
[[ "${actual}" == "${expected}" ]] && ok "auth 段逐行匹配" || { bad "auth 段不符"; printf '%s\n' "${actual}" | sed 's/^/      实际: /'; }

python3 - "${CFG}" <<'PY' && ok "auth 段在 trafficStats 之前" || bad "auth 段位置不对"
import sys
keys = [l.split(":")[0] for l in open(sys.argv[1]) if l.strip() and not l[0].isspace() and not l.startswith("#")]
assert keys.index("auth") < keys.index("trafficStats"), keys
PY

BAK="$(ls "${CFG}".bak.* 2>/dev/null | tail -1)"
[[ -n "${BAK}" ]] && diff -q "${BAK}" "${WORK}/original.yaml" >/dev/null && ok "备份存在且与原文一致" || bad "备份不对"
[[ "$(stat -c %a "${CFG}")" == "600" ]] && ok "权限 600" || bad "权限不是 600"

echo "== 6) 幂等 + insecure"
set_hysteria_auth_http "${CFG}" "https://auth.ipx.sarl" "0" >/dev/null
[[ "$(grep -cE '^auth:' "${CFG}")" == "1" ]] && ok "auth 段唯一" || bad "auth 段重复"
set_hysteria_auth_http "${CFG}" "https://auth.ipx.sarl" "1" >/dev/null
grep -q "insecure: true" "${CFG}" && ok "insecure 可打开" || bad "insecure 未生效"

echo "== 7) 非法输入被拒（subshell：函数里的 die 会 exit）"
if (set_hysteria_auth_http "${CFG}" "" "0" >/dev/null 2>&1); then bad "空 url 应报错"; else ok "空 url 被拒"; fi
if (set_hysteria_auth_http "${WORK}/nope.yaml" "https://x" "0" >/dev/null 2>&1); then bad "缺文件应报错"; else ok "缺文件被拒"; fi
if (set_hysteria_auth_http "${CFG}" "auth.ipx.sarl" "0" >/dev/null 2>&1); then bad "缺 scheme 应报错"; else ok "缺 scheme 被拒"; fi

echo "== 8) 独立脚本与 install.sh 内实现输出必须一致（防漂移）"
A="${WORK}/standalone.yaml"; B="${WORK}/inline.yaml"
cp "${WORK}/original.yaml" "${A}"; cp "${WORK}/original.yaml" "${B}"
bash "${ROOT}/scripts/set-hysteria-auth-http.sh" "https://auth.ipx.sarl" --config "${A}" --no-restart >/dev/null 2>&1 \
  && ok "独立脚本执行成功" || bad "独立脚本执行失败"
( set_hysteria_auth_http "${B}" "https://auth.ipx.sarl" "0" ) >/dev/null 2>&1
if diff -q <(grep -v '^#' "${A}") <(grep -v '^#' "${B}") >/dev/null; then
  ok "两份实现输出一致"
else
  bad "两份实现输出不一致"; diff <(grep -v '^#' "${A}") <(grep -v '^#' "${B}") | head -8
fi

echo "== 9) 独立脚本边界 + 明文 http 提醒"
if (bash "${ROOT}/scripts/set-hysteria-auth-http.sh" "auth.ipx.sarl" --config "${A}" --no-restart) >/dev/null 2>&1; then
  bad "缺 scheme 应被拒"; else ok "缺 scheme 被拒"; fi
out="$(bash "${ROOT}/scripts/set-hysteria-auth-http.sh" "http://auth.ipx.sarl" --config "${A}" --no-restart 2>&1 || true)"
if grep -q "明文 http" <<<"${out}"; then ok "明文 http 有提醒"; else bad "明文 http 没提醒"; fi

# NB: 不要写 `cmd | grep -q`：set -o pipefail 下 grep 提前关管道会让上游 SIGPIPE(141)，条件变假。
echo "== 10) install.sh 里不要残留 pipefail 陷阱写法"
# 只看代码行，别把自己的说明性注释也算进去
hits="$(grep -nE '^[^#]*\| *grep -q' "${ROOT}/install.sh" || true)"
if [[ -n "${hits}" ]]; then
  bad "install.sh 里仍有 '| grep -q'（pipefail 下会误判）"; printf '%s\n' "${hits}" | sed 's/^/      /'
else
  ok "没有 '| grep -q' 写法"
fi

printf '\nPASS=%d FAIL=%d\n' "${PASS}" "${FAIL}"
[[ "${FAIL}" -eq 0 ]]
