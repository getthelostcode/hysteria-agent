#!/usr/bin/env bash
# install-controlplane.sh —— 在控制面这台机器上一键装好控制面（systemd + 配置 + 首个令牌）。
#
# 用法：
#   bash install-controlplane.sh                                  # 监听 127.0.0.1:8443，public_url 自动留空
#   bash install-controlplane.sh --public-url https://cp.example.com
#   bash install-controlplane.sh --listen 0.0.0.0:8443 --public-url https://cp.example.com
#   bash install-controlplane.sh --uninstall
#
# 做完之后：
#   1) 起 Cloudflare Tunnel / nginx 把 <public_url>/api/v1/agent/* 指到 http://127.0.0.1:8443
#      （WebSocket 需要转发 Upgrade/Connection 头；cloudflared 写一条带 path 的 ingress 即可）
#   2) controlplane token new --name <节点名>
#   3) 在节点上 bash install.sh <public_url> --token=<令牌>
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_NAME=controlplane
BIN=/usr/local/bin/$BIN_NAME
CONF_DIR=/etc/hysteria-controlplane
CONF=$CONF_DIR/config.yaml
SERVICE=$BIN_NAME
UNIT=/etc/systemd/system/$SERVICE.service
STATE_DIR=/var/lib/hysteria-controlplane

PUBLIC_URL=""
LISTEN=""
UNINSTALL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --public-url) PUBLIC_URL="${2:?}"; shift 2 ;;
    --listen)     LISTEN="${2:?}"; shift 2 ;;
    --uninstall)  UNINSTALL=1; shift ;;
    --help|-h)    sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "未知参数: $1"; exit 2 ;;
  esac
done

c_green=$'\033[1;32m'; c_red=$'\033[1;31m'; c_blue=$'\033[1;36m'; c_off=$'\033[0m'
info(){ printf '%s[INFO]%s %s\n' "$c_blue" "$c_off" "$*"; }
ok(){   printf '%s[ OK ]%s %s\n' "$c_green" "$c_off" "$*"; }
die(){  printf '%s[FAIL]%s %s\n' "$c_red" "$c_off" "$*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "需要 root 运行"

if [[ $UNINSTALL -eq 1 ]]; then
  info "卸载控制面"
  systemctl disable --now "$SERVICE" 2>/dev/null || true
  rm -f "$UNIT" "$BIN"
  systemctl daemon-reload
  ok "已卸载（配置与状态保留在 $CONF_DIR 与 $STATE_DIR，如需清理请手动删除）"
  exit 0
fi

# ---------------- 1) 二进制 ----------------
info "步骤 1/4：安装 $BIN_NAME"
SRC=""
for c in "$SELF_DIR/bin/$BIN_NAME" "$SELF_DIR/$BIN_NAME" "$(command -v $BIN_NAME || true)"; do
  [[ -n "$c" && -x "$c" ]] && { SRC="$c"; break; }
done
[[ -n "$SRC" ]] || die "找不到 $BIN_NAME：把它放在脚本旁的 bin/ 里，或先 make build"
install -m 0755 "$SRC" "$BIN.tmp" && mv -f "$BIN.tmp" "$BIN"
ok "二进制：$BIN（来源 $SRC）"

# ---------------- 2) 配置 ----------------
info "步骤 2/4：生成配置"
mkdir -p "$CONF_DIR" "$STATE_DIR"
chmod 0700 "$CONF_DIR" "$STATE_DIR"
if [[ -f "$CONF" ]]; then
  ok "已存在配置，沿用：$CONF（要重建请先删掉它）"
else
  "$BIN" init -config "$CONF" >/dev/null
  ok "已生成配置（含随机 admin_token 与主密钥）"
fi
chmod 0600 "$CONF"
python3 - "$CONF" "$LISTEN" "$PUBLIC_URL" "$STATE_DIR" <<'PY'
import sys, re
path, listen, public, state = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
s = open(path).read()
if listen:
    s = re.sub(r'(?m)^listen:.*$', f'listen: "{listen}"', s)
if public:
    s = re.sub(r'(?m)^public_url:.*$', f'public_url: "{public}"', s)
s = re.sub(r'(?m)^state_dir:.*$', f'state_dir: "{state}"', s)
open(path, 'w').write(s)
PY

ADMIN_TOKEN="$(awk '/^admin_token:/{print $2}' "$CONF")"
CUR_LISTEN="$(awk '/^listen:/{print $2}' "$CONF" | tr -d '"')"
CUR_PUBLIC="$(awk '/^public_url:/{print $2}' "$CONF" | tr -d '"')"
ok "listen=$CUR_LISTEN  public_url=${CUR_PUBLIC:-（未设置，Agent 需填可直接访问的地址）}"

# ---------------- 3) systemd ----------------
info "步骤 3/4：安装 systemd 服务"
cat > "$UNIT" <<EOF
[Unit]
Description=Hysteria Agent Control Plane
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN serve -config $CONF
Restart=always
RestartSec=2s
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=$STATE_DIR
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$BIN_NAME

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now "$SERVICE" >/dev/null 2>&1 || die "启动失败：journalctl -u $SERVICE -n 50 --no-pager"
sleep 1

# ---------------- 4) 现场验证 ----------------
info "步骤 4/4：验证"
BASE="http://${CUR_LISTEN}"
[[ "$BASE" == "http://0.0.0.0"* ]] && BASE="http://127.0.0.1${CUR_LISTEN##*:}"
PORT="${CUR_LISTEN##*:}"
health="$(curl -fsS --max-time 5 "http://127.0.0.1:$PORT/healthz" || true)"
[[ -n "$health" ]] || die "健康检查失败：journalctl -u $SERVICE -n 50 --no-pager"
ok "服务已运行：$health"
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$PORT/admin/nodes")"
[[ "$code" == "401" ]] || die "/admin/nodes 期望 401（无令牌），实际 $code"
ok "管理接口鉴权正常（无令牌 401）"

echo
echo "${c_green}控制面已就绪${c_off}"
echo "  admin_token : $ADMIN_TOKEN   ← 只出现在这里和 $CONF，请自行保存"
echo "  配置        : $CONF (0600)"
echo "  状态目录    : $STATE_DIR"
echo "  管理接口    : ${CUR_PUBLIC:-http://127.0.0.1:$PORT}/admin/nodes（Authorization: Bearer <admin_token>）"
echo "  仪表盘      : ${CUR_PUBLIC:-http://127.0.0.1:$PORT}/"
echo
echo "下一步："
echo "  1) 生成注册令牌：$BIN_NAME token new --name node-1"
echo "  2) 让 ${CUR_PUBLIC:-<对外地址>} 的 /api/v1/agent/* 指到这里（cloudflared ingress 加 path 规则）"
echo "  3) 节点上：bash install.sh ${CUR_PUBLIC:-<对外地址>} --token=<令牌>"
