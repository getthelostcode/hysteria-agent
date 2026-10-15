#!/usr/bin/env bash
#
# scripts/set-hysteria-auth-http.sh —— 把**已有**节点的 Hysteria 配置改成 HTTP 鉴权
#
# 为什么要单独一个脚本：节点上的 /etc/hysteria/config.yaml 可能是很久以前生成的静态密码
# （auth.type=password：用户离开平台后依然能连，也无法按流量/到期回收），而重跑 install.sh
# 会顺带装二进制、写 systemd 单元…… 这台机器上我们只想改 auth 段，别的原样不动。
#
# 用法：
#   sudo bash scripts/set-hysteria-auth-http.sh [https://auth.ipx.sarl] [--insecure] \
#        [--config /etc/hysteria/config.yaml] [--service hysteria-server] [--no-restart]
#
# 行为：
#   1. 备份原配置到 <config>.bak.<时间戳>
#   2. 只替换顶层 auth 段（证书 / masquerade / trafficStats 等一字不动）
#   3. 权限回到 600；有 PyYAML 时做一次语法校验
#   4. 重启 Hysteria 服务（--no-restart 跳过）
#
# 注意：切过去之前先确认鉴权后端可用，否则**所有用户都连不上**：
#   curl -si -X POST -H 'Content-Type: application/json' \
#     -d '{"addr":"1.2.3.4:1","auth":"a:b","tx":0,"rx":0}' https://auth.ipx.sarl/auth
#   期望 200 + {"ok":false,"id":""}
set -euo pipefail

AUTH_URL="${1:-https://auth.ipx.sarl}"
shift || true

INSECURE=0
CFG="${HYSTERIA_CONFIG:-/etc/hysteria/config.yaml}"
SERVICE="${HYSTERIA_SERVICE:-hysteria-server}"
RESTART=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --insecure)   INSECURE=1; shift ;;
    --config)     CFG="${2:-}"; shift 2 ;;
    --service)    SERVICE="${2:-}"; shift 2 ;;
    --no-restart) RESTART=0; shift ;;
    -h|--help)    sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)            echo "未知参数：$1（--help 看用法）" >&2; exit 2 ;;
  esac
done

GREEN=$'\033[1;32m'; BLUE=$'\033[1;36m'; YELLOW=$'\033[1;33m'; RED=$'\033[1;31m'; NC=$'\033[0m'
log()  { printf '%s[OK]%s %s\n'   "${GREEN}"  "${NC}" "$*"; }
info() { printf '%s[INFO]%s %s\n' "${BLUE}"   "${NC}" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "${YELLOW}" "${NC}" "$*"; }
die()  { printf '%s[FAIL]%s %s\n' "${RED}"    "${NC}" "$*" >&2; exit 1; }

[[ -f "$CFG" ]] || die "配置文件不存在：$CFG"
case "$AUTH_URL" in
  http://*|https://*) ;;
  *) die "鉴权地址必须是 http:// 或 https:// 开头：$AUTH_URL" ;;
esac
[[ "$AUTH_URL" == http://* ]] && warn "用的是明文 http：鉴权请求会带用户凭据，建议 https"

insecure_literal="false"
[[ "$INSECURE" == "1" ]] && insecure_literal="true"
bash_block="auth:
  type: http
  http:
    url: ${AUTH_URL}
    insecure: ${insecure_literal}"

backup="${CFG}.bak.$(date +%Y%m%d%H%M%S)"
tmp="$(mktemp)"; stripped="$(mktemp)"
awk '
  /^[^[:space:]#][^:]*:/ { in_auth = ($0 ~ /^auth:[[:space:]]*(#.*)?$/) ? 1 : 0 }
  in_auth == 0 { print }
' "$CFG" > "$stripped"

# 锚点与 install.sh 的 set_hysteria_auth_http 保持一致（tests 里断言两份实现输出相同）
insert_at="$(grep -nE '^(trafficStats|masquerade|bandwidth|acl|quic|resolver|outbounds):' "$stripped" | head -1 | cut -d: -f1 || true)"
if [[ -n "$insert_at" && "$insert_at" -gt 1 ]]; then
  head -n "$((insert_at - 1))" "$stripped" > "$tmp"
  printf '%s\n\n' "$bash_block" >> "$tmp"
  tail -n "+$insert_at" "$stripped" >> "$tmp"
else
  cat "$stripped" > "$tmp"; printf '\n%s\n' "$bash_block" >> "$tmp"
fi
rm -f "$stripped"

if command -v python3 >/dev/null && python3 -c 'import yaml' >/dev/null 2>&1; then
  python3 -c 'import sys,yaml; yaml.safe_load(open(sys.argv[1]))' "$tmp" \
    || die "YAML 语法校验失败（原配置未改动）：$tmp"
else
  warn "本机没有 PyYAML，跳过 YAML 语法校验"
fi

cp -a "$CFG" "$backup"
cat "$tmp" > "$CFG"; rm -f "$tmp"; chmod 600 "$CFG"

log "auth 段已改为 HTTP 鉴权：${AUTH_URL}（insecure=${insecure_literal}）"
info "  备份：$backup"
info "  其余内容（证书 / masquerade / trafficStats）未改动"
echo
info "现在的 auth 段："
awk '/^auth:/{f=1} f{print "      " $0; if (f && /^[^[:space:]]/ && !/^auth:/) exit}' "$CFG" | sed '$d'

if [[ "$RESTART" -eq 1 ]]; then
  units="$(systemctl list-unit-files 2>/dev/null || true)"
  if grep -q "^${SERVICE}.service" <<<"$units"; then
    systemctl restart "$SERVICE" && log "已重启 $SERVICE" \
      || die "重启 $SERVICE 失败（配置已改，备份在 $backup）"
  else
    warn "没找到 ${SERVICE}.service：请手动重启 Hysteria"
  fi
fi
