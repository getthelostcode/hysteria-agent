#!/usr/bin/env bash
# hysteria-agent 一键安装脚本
#
#   bash install.sh <控制面地址> [--token=<注册令牌>] [--name=<节点名>]
#
# 它做这几件事（幂等，可重复执行）：
#   1. 安装 hysteria-agent 二进制（优先用仓库里的 ./bin/，否则从 AGENT_BINARY_URL 下载）
#   2. 确保 Hysteria 2 服务端存在：没有二进制就从 GitHub Releases 下载；
#      没有 /etc/hysteria/config.yaml 就生成一份（自签证书或 ACME + HTTP 鉴权 + trafficStats）
#      已有配置时**不动它**，除非显式要求换鉴权（见 --set-hysteria-auth）
#   3. 生成 /etc/hysteria-agent/config.yaml，并把 hysteria.api_base_url / api_secret
#      与 Hysteria 配置里的 trafficStats 对齐（不改动 Hysteria 其它键与注释）
#   4. 安装并启动 systemd 单元，然后执行 enroll 注册节点
#   5. 现场验证：/healthz、/readyz、Hysteria /traffic、控制通道状态，最后打印 journalctl 末尾
#
# 环境变量：
#   ENROLL_TOKEN         注册令牌（等价 --token）
#   AGENT_BINARY         本地 agent 二进制路径
#   AGENT_BINARY_URL     下载 agent 二进制（可选）
#   AGENT_VERSION        版本号（仅日志）
#   UPGRADE_PUBKEY       自升级验签公钥（base64 ed25519；不给则关闭自升级）
#   HYSTERIA_VERSION     要下载的 Hysteria 版本（默认 v2.6.5）
#   HYSTERIA_LISTEN      监听地址（默认 :443）
#   HYSTERIA_DOMAIN      域名，给了就用 ACME 申请证书
#   HYSTERIA_ACME_EMAIL  ACME 邮箱
#   HYSTERIA_SERVICE     Hysteria 的 systemd 单元名（默认 hysteria-server）
#   AGENT_SERVICE        Agent 的 systemd 单元名（默认 hysteria-agent）
#
# 其它参数：
#   --no-hysteria        不动 Hysteria（假定外部已装好）
#   --hysteria-config    指定 Hysteria 配置文件路径（默认 /etc/hysteria/config.yaml）
#   --set-hysteria-auth  把**已有**配置的 auth 段换成 HTTP 鉴权（其余内容一字不动，会先备份）
#   --hysteria-auth-url=<url>   HTTP 鉴权地址（默认 https://auth.ipx.sarl；
#                              显式给出时也会触发上面的 auth 段重写）
#   HYSTERIA_AUTH_MODE   http（默认，走鉴权服务）| password（老的静态密码，用户离开平台也能连）
#   HYSTERIA_AUTH_INSECURE  1 = 跳过鉴权地址的证书校验（自签后端才用），默认 0
#   --skip-enroll        只装不注册
#   --verify-only        只做验证，不安装/不修改任何配置
#   --uninstall          卸载（停止服务、删单元与配置目录）
set -euo pipefail

# ---------------------------------------------------------------- 常量与日志
AGENT_BIN=/usr/local/bin/hysteria-agent
AGENT_CFG_DIR=/etc/hysteria-agent
AGENT_CFG=$AGENT_CFG_DIR/config.yaml
AGENT_STATE=/var/lib/hysteria-agent
AGENT_SERVICE="${AGENT_SERVICE:-hysteria-agent}"
HY_SERVICE="${HYSTERIA_SERVICE:-hysteria-server}"
HY_LISTEN="${HYSTERIA_LISTEN:-:443}"
HY_VERSION="${HYSTERIA_VERSION:-v2.6.5}"
HY_CFG="/etc/hysteria/config.yaml"
# HTTP 鉴权是默认：静态密码写在节点上，用户离开平台后依然能连，也无法按流量/到期回收。
HYSTERIA_AUTH_MODE="${HYSTERIA_AUTH_MODE:-http}"
[[ -n "${HYSTERIA_AUTH_URL:-}" ]] && HYSTERIA_AUTH_URL_PROVIDED=1
HYSTERIA_AUTH_URL="${HYSTERIA_AUTH_URL:-https://auth.ipx.sarl}"
HYSTERIA_AUTH_INSECURE="${HYSTERIA_AUTH_INSECURE:-0}"
UPGRADE_PUBKEY="${UPGRADE_PUBKEY:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\033[1;32m[OK]\033[0m %s\n' "$*" >&2; }
info() { printf '\033[1;36m[INFO]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

CONTROL_URL=""
ENROLL_TOKEN="${ENROLL_TOKEN:-}"
NODE_NAME=""
NO_HYSTERIA=0
SKIP_ENROLL=0
VERIFY_ONLY=0
UNINSTALL=0
SET_HYSTERIA_AUTH=0
HYSTERIA_AUTH_URL_PROVIDED=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --token=*)          ENROLL_TOKEN="${1#*=}" ;;
    --token)            shift; ENROLL_TOKEN="${1:-}" ;;
    --name=*)           NODE_NAME="${1#*=}" ;;
    --name)             shift; NODE_NAME="${1:-}" ;;
    --hysteria-config=*) HY_CFG="${1#*=}" ;;
    --hysteria-config)   shift; HY_CFG="${1:-}" ;;
    --set-hysteria-auth) SET_HYSTERIA_AUTH=1 ;;
    --hysteria-auth-url=*) HYSTERIA_AUTH_URL="${1#*=}"; HYSTERIA_AUTH_URL_PROVIDED=1 ;;
    --hysteria-auth-insecure) HYSTERIA_AUTH_INSECURE=1 ;;
    --no-hysteria)      NO_HYSTERIA=1 ;;
    --skip-enroll)      SKIP_ENROLL=1 ;;
    --verify-only)      VERIFY_ONLY=1 ;;
    --uninstall)        UNINSTALL=1 ;;
    -h|--help)          sed -n '2,40p' "$0"; exit 0 ;;
    http://*|https://*|ws://*|wss://*) CONTROL_URL="$1" ;;
    *) die "未知参数: $1（用 --help 查看用法）" ;;
  esac
  shift
done

[[ $EUID -eq 0 ]] || die "请用 root 运行（需要写 /etc 和安装 systemd 单元）"
HAS_SYSTEMD=1
command -v systemctl >/dev/null 2>&1 || { HAS_SYSTEMD=0; warn "没有 systemctl（容器？），将只安装文件，不启动服务"; }

# ---------------------------------------------------------------- 卸载
if [[ $UNINSTALL -eq 1 ]]; then
  info "卸载 hysteria-agent"
  if [[ $HAS_SYSTEMD -eq 1 ]]; then
    systemctl disable --now "$AGENT_SERVICE" 2>/dev/null || true
    rm -f "/etc/systemd/system/$AGENT_SERVICE.service" "/lib/systemd/system/$AGENT_SERVICE.service"
    systemctl daemon-reload
  fi
  rm -f "$AGENT_BIN"
  rm -rf "$AGENT_CFG_DIR" "$AGENT_STATE"
  log "已卸载（Hysteria 本身未动）"
  exit 0
fi

[[ -n "$CONTROL_URL" ]] || die "第一个参数必须是控制面地址，例如: bash install.sh https://cp.example.com --token=xxx"
CONTROL_URL="${CONTROL_URL%/}"

# 令牌形状：在动任何东西之前判死。面板列表里显示的是 12 位掩码（enr_GmCVlx2W…），
# 抄它注册一定失败，但会白白走完二进制安装 + 服务检查 + 配置生成，最后停在 401。
if [[ -n "$ENROLL_TOKEN" ]]; then
  TOKEN_TRIMMED="${ENROLL_TOKEN//[[:space:]]/}"
  if [[ "$TOKEN_TRIMMED" != "${ENROLL_TOKEN}" ]]; then
    ENROLL_TOKEN="$TOKEN_TRIMMED"
    warn "注册令牌两端带有空白字符，已自动去掉"
  fi
  if [[ "$ENROLL_TOKEN" == enr_* && "${#ENROLL_TOKEN}" -ne 44 ]]; then
    die "$(cat <<EOF
注册令牌被截断了：当前 ${#ENROLL_TOKEN} 字符，本控制面签发的完整令牌是 44 字符（enr_ + 40 位随机字符）。
  你给的是：${ENROLL_TOKEN}
  面板「注册令牌」列表里显示的 enr_xxxxxxxxxx… 是**掩码前缀**（控制面只保存 SHA-256 哈希，无法反查明文）。
  请到面板 → 注册令牌 → 该行点「重新签发」→「查看刚签发的令牌」→「复制完整令牌」，再重跑：
    bash install.sh $CONTROL_URL --token='<粘贴 44 字符完整令牌>'
EOF
)"
  fi
  if [[ "$ENROLL_TOKEN" != enr_* ]]; then
    warn "注册令牌不以 enr_ 开头（${#ENROLL_TOKEN} 字符）：若不是 hysteria-control-plane 签发的可忽略；否则请检查是否复制错了字段"
  fi
fi

# ---------------------------------------------------------------- 工具函数
write_file() { # write_file <path> <mode> ; 内容从 stdin 读
  local path="$1" mode="$2"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
  chmod "$mode" "$path"
  # 防止日志混进生成的文件（历史上这类脚本真踩过：配置文件里混进 [INFO] 行会让服务起不来）
  if grep -qE '^\x1b\[1;3[0-9]m\[(OK|INFO|WARN|FAIL)\]' "$path" 2>/dev/null; then
    die "生成的文件 $path 里混入了日志行，已中止"
  fi
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo amd64 ;;
    aarch64|arm64) echo arm64 ;;
    *) die "不支持的架构: $(uname -m)" ;;
  esac
}

read_yaml_value() { # read_yaml_value <file> <key>  —— 只取第一个匹配（顶层/缩进都行）
  local file="$1" key="$2"
  [[ -f "$file" ]] || return 1
  awk -v k="^[[:space:]]*$key[[:space:]]*:" '$0 ~ k { sub(/^[^:]*:[[:space:]]*/, ""); gsub(/^["'"'"']|["'"'"']$/, ""); print; exit }' "$file"
}

# ---------------------------------------------------------------- 0. verify-only
verify() {
  local rc=0
  info "验证 1/4：Agent 单元状态"
  if [[ $HAS_SYSTEMD -eq 1 ]]; then
    systemctl is-active "$AGENT_SERVICE" || rc=1
  fi
  info "验证 2/4：本地运维端点"
  local mlisten
  mlisten="$(read_yaml_value "$AGENT_CFG" listen || true)"
  [[ -n "$mlisten" ]] || mlisten="127.0.0.1:9100"
  for path in /healthz /readyz; do
    local code
    code="$(curl -s -o /tmp/.ha-verify -w '%{http_code}' "http://$mlisten$path" || echo 000)"
    printf '  %s %s -> %s %s\n' "http://$mlisten$path" "$path" "$code" "$(head -c 200 /tmp/.ha-verify 2>/dev/null)"
    [[ "$code" == "200" ]] || rc=1
  done
  rm -f /tmp/.ha-verify

  info "验证 3/4：Hysteria 本地 API（/traffic）"
  # 以 Agent 配置为准：它记录了 Hysteria 的配置路径与 trafficStats 的 listen/secret，
  # 验证的正是"Agent 眼里的那份"，而不是脚本默认假设的那份（--hysteria-config 换过路径时尤其重要）。
  local hy_cfg api_base api_secret ts_listen ts_secret base code
  hy_cfg="$(read_yaml_value "$AGENT_CFG" config_path || true)"
  [[ -n "$hy_cfg" ]] || hy_cfg="$HY_CFG"
  api_base="$(read_yaml_value "$AGENT_CFG" api_base_url || true)"
  api_secret="$(read_yaml_value "$AGENT_CFG" api_secret || true)"
  if [[ -f "$hy_cfg" ]]; then
    ts_listen="$(awk '/trafficStats:/{f=1} f&&/listen:/{sub(/^[[:space:]]*listen:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$hy_cfg")"
    ts_secret="$(awk '/trafficStats:/{f=1} f&&/secret:/{sub(/^[[:space:]]*secret:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$hy_cfg")"
    [[ -n "$api_base" ]] || api_base="http://${ts_listen#*://}"
    [[ -n "$api_secret" ]] || api_secret="$ts_secret"
    if [[ -n "$api_base" && "$api_base" != "http://" ]]; then
      code="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: $api_secret" "$api_base/traffic" || echo 000)"
      printf '  Agent 配置的 hysteria 配置路径: %s\n' "$hy_cfg"
      printf '  %s/traffic -> %s\n' "$api_base" "$code"
      case "$code" in
        200) : ;;
        401|403) warn "  Hysteria 拒绝鉴权：Agent 的 api_secret 与 Hysteria 配置里的 trafficStats.secret 不一致" ;;
        404) warn "  Hysteria 没启用 trafficStats（/traffic 不存在）" ;;
        000) warn "  连不上 ${api_base}（Hysteria 没在跑，或 api_base_url 不对）" ;;
      esac
      # 负例：不带 Authorization 必须 401/403，否则说明鉴权形同虚设
      local nega
      nega="$(curl -s -o /dev/null -w '%{http_code}' "$api_base/traffic" || echo 000)"
      printf '  负例（不带 Authorization）-> %s\n' "$nega"
      [[ "$nega" == "401" || "$nega" == "403" ]] || warn "  不带 secret 也能读 /traffic，检查 trafficStats.secret 是否为空"
    else
      warn "  配置里没有 trafficStats.listen，跳过"
    fi
  else
    warn "  找不到 $hy_cfg，跳过"
  fi

  info "验证 4/4：控制通道"
  if [[ -f "$AGENT_STATE/state.json" ]]; then
    python3 - "$AGENT_STATE/state.json" <<'PY' || rc=1
import json,sys
d=json.load(open(sys.argv[1]))
c=d.get("control",{}); h=d.get("hysteria",{}); cfg=d.get("config",{})
print(f"  node_id={d.get('node_id')} agent={d.get('agent_version')} ready={d.get('ready')}")
print(f"  控制通道: connected={c.get('connected')} pending={c.get('pending_events')} acked_seq={c.get('last_acked_seq')}")
print(f"  Hysteria: running={h.get('running')} pid={h.get('pid')} restarts={h.get('restart_count')} exit={h.get('last_exit_code')}")
print(f"  配置: revision={cfg.get('revision')} sha256={str(cfg.get('sha256'))[:12]}")
sys.exit(0 if c.get("connected") and h.get("running") else 1)
PY
  else
    warn "  还没有 $AGENT_STATE/state.json（Agent 未启动？）"
    rc=1
  fi

  if [[ -f "$AGENT_CFG" ]]; then
    info "Agent 自检（diag）"
    "$AGENT_BIN" diag -config "$AGENT_CFG" || true
  fi
  return $rc
}

if [[ $VERIFY_ONLY -eq 1 ]]; then
  verify && { log "验证通过"; exit 0; } || { warn "验证发现问题（见上）"; exit 1; }
fi

# ---------------------------------------------------------------- 1. 安装 Agent 二进制
info "步骤 1/5：安装 hysteria-agent 二进制"
if [[ -x "$AGENT_BIN" ]]; then
  info "已存在 $AGENT_BIN（将覆盖为新版本；旧文件备份为 $AGENT_BIN.bak）"
fi

ARCH="$(detect_arch)"
AGENT_REPO_SLUG="${AGENT_REPO_SLUG:-getthelostcode/hysteria-agent}"
AGENT_VERSION="${AGENT_VERSION:-0.1.0}"
RAW_BASE="https://raw.githubusercontent.com/${AGENT_REPO_SLUG}/master"

# 找一个可用的本地二进制来源。仓库里带的是编译好的发布产物：
#   dist/hysteria-agent-linux-<arch>                     单文件（amd64）
#   release/hysteria-agent-<ver>-linux-<arch>.tar.gz     完整包（amd64/arm64）
#   bin/hysteria-agent                                   本机 make build 的产物
resolve_local_agent() {
  local cands=(
    "${AGENT_BINARY:-}"
    "$SCRIPT_DIR/bin/hysteria-agent"
    "$SCRIPT_DIR/dist/hysteria-agent-linux-$ARCH"
    "$SCRIPT_DIR/release/hysteria-agent-$AGENT_VERSION-linux-$ARCH/bin/hysteria-agent"
  )
  local c
  for c in "${cands[@]}"; do
    [[ -n "$c" && -x "$c" ]] && { echo "$c"; return 0; }
  done
  # 从仓库里的发布包（tarball）里取
  local t tmp
  for t in "$SCRIPT_DIR"/release/hysteria-agent-*-linux-"$ARCH".tar.gz; do
    [[ -f "$t" ]] || continue
    tmp="$(mktemp -d)"
    if tar -xzf "$t" -C "$tmp" 2>/dev/null; then
      c="$(find "$tmp" -type f -name hysteria-agent -perm -u+x 2>/dev/null | head -1)"
      if [[ -n "$c" ]]; then echo "$c"; return 0; fi
    fi
    rm -rf "$tmp"
  done
  return 1
}

# 本地都没有时，从 GitHub 拉（单文件优先，其次发布包）。
download_agent() {
  [[ "${AGENT_NO_DOWNLOAD:-0}" == "1" ]] && return 1
  local url
  for url in "$RAW_BASE/dist/hysteria-agent-linux-$ARCH" \
             "$RAW_BASE/release/hysteria-agent-$AGENT_VERSION-linux-$ARCH.tar.gz"; do
    info "尝试下载 $url"
    if [[ "$url" == *.tar.gz ]]; then
      local tmp; tmp="$(mktemp -d)"
      if curl -fsSL "$url" -o "$tmp/pkg.tar.gz" && tar -xzf "$tmp/pkg.tar.gz" -C "$tmp" 2>/dev/null; then
        local found; found="$(find "$tmp" -type f -name hysteria-agent -perm -u+x | head -1)"
        if [[ -n "$found" ]]; then install -m 0755 "$found" "$AGENT_BIN.new"; rm -rf "$tmp"; return 0; fi
      fi
      rm -rf "$tmp"
    elif curl -fsSL "$url" -o "$AGENT_BIN.new"; then
      chmod 0755 "$AGENT_BIN.new"; return 0
    fi
  done
  return 1
}

SRC_BIN=""
if [[ -n "${AGENT_BINARY_URL:-}" ]]; then
  info "下载 AGENT_BINARY_URL=$AGENT_BINARY_URL"
  curl -fsSL "$AGENT_BINARY_URL" -o "$AGENT_BIN.new" || die "下载 $AGENT_BINARY_URL 失败"
  chmod 0755 "$AGENT_BIN.new"
elif SRC_BIN="$(resolve_local_agent)"; then
  info "使用本地二进制：$SRC_BIN"
  install -m 0755 "$SRC_BIN" "$AGENT_BIN.new"
elif download_agent; then
  info "已从 GitHub 下载 agent 二进制"
elif [[ -x "$AGENT_BIN" ]]; then
  info "没有新二进制来源，沿用已安装的 $AGENT_BIN"
else
  die "找不到 agent 二进制（架构 $ARCH）。三种办法任选：
   1) 直接下载发布包：curl -fsSLO ${RAW_BASE}/release/hysteria-agent-${AGENT_VERSION}-linux-${ARCH}.tar.gz && tar -xzf hysteria-agent-${AGENT_VERSION}-linux-${ARCH}.tar.gz && cd hysteria-agent-${AGENT_VERSION}-linux-${ARCH} && bash install.sh <控制面地址> --token=...
   2) 指定现成文件：AGENT_BINARY=/path/to/hysteria-agent bash install.sh <控制面地址> --token=...
   3) 指定下载地址：AGENT_BINARY_URL=https://... bash install.sh <控制面地址> --token=...
   4) 自行编译：make build（需要 Go 1.23+，产物在 bin/hysteria-agent）"
fi
if [[ -f "$AGENT_BIN.new" ]]; then
  if ! "$AGENT_BIN.new" version >/dev/null 2>&1; then
    rm -f "$AGENT_BIN.new"
    die "新二进制无法执行（本机架构 $ARCH，来源 $SRC_BIN）——多半是拿到了别的架构的包。
   本机应使用：${RAW_BASE}/release/hysteria-agent-${AGENT_VERSION}-linux-${ARCH}.tar.gz
   或自己编译：make build"
  fi
  [[ -f "$AGENT_BIN" ]] && cp -f "$AGENT_BIN" "$AGENT_BIN.bak"
  mv -f "$AGENT_BIN.new" "$AGENT_BIN"
  log "已安装 $AGENT_BIN（$("$AGENT_BIN" version)）"
fi
mkdir -p "$AGENT_CFG_DIR" "$AGENT_STATE" /run/hysteria-agent
chmod 700 "$AGENT_CFG_DIR"; chmod 750 "$AGENT_STATE"

# ---------------------------------------------------------------- 2. 确保 Hysteria 服务端
if [[ $NO_HYSTERIA -eq 0 ]]; then
  info "步骤 2/5：检查 Hysteria 服务端"
  if ! command -v hysteria >/dev/null 2>&1 && [[ ! -x /usr/local/bin/hysteria ]]; then
    warn "未找到 hysteria 二进制，尝试从 GitHub Releases 下载 $HY_VERSION"
    ARCH="$(detect_arch)"
    URL="https://github.com/apernet/hysteria/releases/download/app/$HY_VERSION/hysteria-linux-$ARCH"
    if curl -fsSL "$URL" -o /usr/local/bin/hysteria; then
      chmod 0755 /usr/local/bin/hysteria
      log "已安装 Hysteria $HY_VERSION"
    else
      warn "下载失败（$URL）；请自行安装 Hysteria 后重跑，或加 --no-hysteria"
    fi
  else
    log "Hysteria 已安装：$(hysteria version 2>/dev/null | head -2 | tr '\n' ' ' || true)"
  fi

  if [[ ! -f "$HY_CFG" ]]; then
    info "生成 $HY_CFG（自签证书 + 随机密码 + trafficStats）"
    mkdir -p "$(dirname "$HY_CFG")"
    TLS_BLOCK=""
    if [[ -n "${HYSTERIA_DOMAIN:-}" ]]; then
      TLS_BLOCK="acme:
  domains:
    - $HYSTERIA_DOMAIN
  email: ${HYSTERIA_ACME_EMAIL:-admin@$HYSTERIA_DOMAIN}
  type: http"
    else
      CERT_DIR="$(dirname "$HY_CFG")"
      openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
        -keyout "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.pem" -days 3650 \
        -subj "/CN=${HYSTERIA_DOMAIN:-node.local}" >/dev/null 2>&1 \
        || die "生成自签证书失败（需要 openssl）"
      chmod 600 "$CERT_DIR/key.pem"
      TLS_BLOCK="tls:
  cert: $CERT_DIR/cert.pem
  key: $CERT_DIR/key.pem"
    fi
    HY_PASSWORD="$(head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    HY_STATS_SECRET="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    AUTH_BLOCK="$(emit_auth_block "$HYSTERIA_AUTH_MODE" "$HY_PASSWORD" "$HYSTERIA_AUTH_URL" "$HYSTERIA_AUTH_INSECURE")"
    write_file "$HY_CFG" 600 <<EOF
listen: $HY_LISTEN
$TLS_BLOCK
$AUTH_BLOCK
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:8080
  secret: $HY_STATS_SECRET
EOF
    log "已生成 Hysteria 配置（密码与 trafficStats secret 均为随机值）"
  else
    log "复用已有 $HY_CFG（不会改动它的其它内容）"

    # auth 段升级：只有显式要求时才改（--set-hysteria-auth 或显式传了 HYSTERIA_AUTH_URL）。
    # 不默认改：鉴权后端不可达会把所有用户挡在门外，例行重跑不该有这种副作用。
    if [[ "$SET_HYSTERIA_AUTH" -eq 1 || "$HYSTERIA_AUTH_URL_PROVIDED" -eq 1 ]]; then
      if [[ "$HYSTERIA_AUTH_MODE" == "password" ]]; then
        warn "HYSTERIA_AUTH_MODE=password：跳过 auth 段重写"
      else
        set_hysteria_auth_http "$HY_CFG" "$HYSTERIA_AUTH_URL" "$HYSTERIA_AUTH_INSECURE"
      fi
    fi
  fi

  # 输出 auth 段：HTTP 鉴权（默认）或老的静态密码
# $1=mode $2=password $3=url $4=insecure
emit_auth_block() {
  local mode="$1" pass="$2" url="$3" insecure="$4"
  if [[ "$mode" == "password" ]]; then
    printf 'auth:\n  type: password\n  password: %s\n' "$pass"
  else
    printf 'auth:\n  type: http\n  http:\n    url: %s\n    insecure: %s\n' \
      "$url" "$([[ "$insecure" == "1" ]] && echo true || echo false)"
  fi
}

# 把**已有**配置的 auth 段就地换成 HTTP 鉴权：只动 auth，证书/masquerade/trafficStats 一字不动。
#
# 为什么需要它：老节点上写的是 auth.type=password 的静态密码 —— 用户离开平台后依然能连，
# 也没法按流量/到期回收；而重新生成整份配置会顺手改掉运维自己的 masquerade、证书路径。
# 为什么必须显式触发：鉴权后端不可达时切过去等于把所有人关在门外，例行重跑脚本不该有这种副作用。
# $1=配置路径 $2=url $3=insecure(0/1)
set_hysteria_auth_http() {
  local cfg="$1" url="$2" insecure="$3"
  local backup tmp stripped insert_at units

  [[ -f "$cfg" ]] || die "配置文件不存在：$cfg"
  [[ -n "$url" ]] || die "auth.http.url 不能为空"
  case "$url" in
    http://*|https://*) ;;
    *) die "auth.http.url 必须是 http:// 或 https:// 开头：$url" ;;
  esac

  backup="${cfg}.bak.$(date +%Y%m%d%H%M%S)"
  tmp="$(mktemp)"
  stripped="$(mktemp)"

  # 顶层的 auth: 段整体删掉（遇到下一个非缩进的 key 就恢复输出）
  awk '
    /^[^[:space:]#][^:]*:/ { in_auth = ($0 ~ /^auth:[[:space:]]*(#.*)?$/) ? 1 : 0 }
    in_auth == 0 { print }
  ' "$cfg" > "$stripped"

  insert_at="$(grep -nE '^(trafficStats|masquerade|bandwidth|acl|quic|resolver|outbounds):' "$stripped" | head -1 | cut -d: -f1 || true)"
  if [[ -n "$insert_at" && "$insert_at" -gt 1 ]]; then
    head -n "$((insert_at - 1))" "$stripped" > "$tmp"
    emit_auth_block http "" "$url" "$insecure" >> "$tmp"
    printf '\n' >> "$tmp"
    tail -n "+$insert_at" "$stripped" >> "$tmp"
  else
    cat "$stripped" > "$tmp"
    printf '\n' >> "$tmp"
    emit_auth_block http "" "$url" "$insecure" >> "$tmp"
  fi
  rm -f "$stripped"

  cp -a "$cfg" "$backup"
  cat "$tmp" > "$cfg"
  rm -f "$tmp"
  chmod 600 "$cfg"

  log "已把 auth 段换成 HTTP 鉴权：$url（insecure=$([[ "$insecure" == "1" ]] && echo true || echo false)）"
  info "  备份：$backup"
  info "  其余内容（证书 / masquerade / trafficStats）未改动"

  if [[ "$HAS_SYSTEMD" -eq 1 ]]; then
    units="$(systemctl list-unit-files 2>/dev/null || true)"
    if grep -q "^$HY_SERVICE.service" <<<"$units"; then
      systemctl restart "$HY_SERVICE" && log "已重启 $HY_SERVICE" \
        || warn "重启 $HY_SERVICE 失败：配置已改，请手动检查（备份 $backup）"
    else
      warn "没找到 $HY_SERVICE.service：请手动重启 Hysteria"
    fi
  fi
}

# 确保 systemd 单元存在
  # NB: 不要写 `systemctl list-unit-files | grep -q ...`：set -o pipefail 下 grep 提前退出会让
  # systemctl 收到 SIGPIPE(141)，整条管道判为失败 → 明明有单元也会被当成「不存在」而重写它。
  unit_files=""
  [[ $HAS_SYSTEMD -eq 1 ]] && unit_files="$(systemctl list-unit-files 2>/dev/null || true)"
  if [[ $HAS_SYSTEMD -eq 1 ]] && ! grep -q "^$HY_SERVICE.service" <<<"$unit_files"; then
    info "生成 Hysteria 的 systemd 单元 /etc/systemd/system/$HY_SERVICE.service"
    write_file "/etc/systemd/system/$HY_SERVICE.service" 644 <<EOF
[Unit]
Description=Hysteria2 Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/hysteria server -c $HY_CFG
WorkingDirectory=$(dirname "$HY_CFG")
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
  fi
  if [[ $HAS_SYSTEMD -eq 1 ]]; then
    systemctl enable --now "$HY_SERVICE" >/dev/null 2>&1 || warn "启动 $HY_SERVICE 失败，请查看 journalctl -u $HY_SERVICE"
  fi
else
  info "步骤 2/5：跳过 Hysteria（--no-hysteria）"
fi

# ---------------------------------------------------------------- 3. 生成 Agent 配置
info "步骤 3/5：生成 $AGENT_CFG"
# 从 Hysteria 配置里读 trafficStats（Agent 必须用同一个 secret 才能读流量/踢人）
TS_LISTEN="$(awk '/trafficStats:/{f=1} f&&/listen:/{sub(/^[[:space:]]*listen:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$HY_CFG" 2>/dev/null || true)"
TS_SECRET="$(awk '/trafficStats:/{f=1} f&&/secret:/{sub(/^[[:space:]]*secret:[[:space:]]*/,"");gsub(/["'"'"']/,"");print;exit}' "$HY_CFG" 2>/dev/null || true)"
[[ -n "$TS_LISTEN" ]] || TS_LISTEN="127.0.0.1:8080"
API_BASE="http://${TS_LISTEN#*://}"

if [[ -s "$AGENT_CFG" ]]; then
  cp -f "$AGENT_CFG" "$AGENT_CFG.bak-$(date +%Y%m%d%H%M%S)"
  info "已备份原配置"
fi

UPGRADE_BLOCK="upgrade:
  enabled: false
  pubkeys: []"
if [[ -n "$UPGRADE_PUBKEY" ]]; then
  UPGRADE_BLOCK="upgrade:
  enabled: true
  pubkeys:
    - $UPGRADE_PUBKEY
  binary_path: $AGENT_BIN
  work_dir: $AGENT_STATE/upgrade
  restart_mode: systemd
  service_name: $AGENT_SERVICE"
else
  warn "未提供 UPGRADE_PUBKEY：自升级已关闭（用 agent-sign keygen 生成密钥后可重跑本脚本并传 UPGRADE_PUBKEY=...）"
fi

# 明文 ws:// 只在本地联调时出现：配置里必须显式打开 allow_insecure_ws，否则启动会直接报错退出。
INSECURE_WS=""
if [[ "$CONTROL_URL" == http://* || "$CONTROL_URL" == ws://* ]]; then
  INSECURE_WS="  allow_insecure_ws: true   # 明文控制面（仅本地联调！生产请用 https/wss）"
  warn "控制面地址是明文的（$CONTROL_URL），已设置 control.allow_insecure_ws: true，仅供联调"
fi

write_file "$AGENT_CFG" 600 <<EOF
# 由 install.sh 生成于 $(date -u +%Y-%m-%dT%H:%M:%SZ)
control:
  server_url: "$CONTROL_URL"
$INSECURE_WS
  ws_path: "/api/v1/agent/control"
  enroll_path: "/api/v1/agent/enroll"
  heartbeat_interval_seconds: 30
  heartbeat_timeout_seconds: 90
  telemetry_interval_seconds: 15
  reconnect:
    initial_seconds: 1
    max_seconds: 300
    multiplier: 2
    jitter: 0.2

credentials_file: "$AGENT_CFG_DIR/credentials.json"
state_dir: "$AGENT_STATE"

hysteria:
  mode: process
  binary: /usr/local/bin/hysteria
  config_path: "$HY_CFG"
  service_name: "$HY_SERVICE"
  api_base_url: "$API_BASE"
  api_secret: "$TS_SECRET"
  history_dir: "$AGENT_CFG_DIR/history"
  history_keep: 5
  startup_timeout_seconds: 20
  shutdown_timeout_seconds: 15

telemetry:
  interval_seconds: 15
  disk_paths: ["/", "$AGENT_STATE"]

logs:
  level: info
  remote_enabled: true

metrics:
  listen: "127.0.0.1:9100"

outbox:
  path: "$AGENT_STATE/outbox.jsonl"

$UPGRADE_BLOCK
EOF
log "已写入 $AGENT_CFG（api_base_url=$API_BASE, api_secret=$([[ -n $TS_SECRET ]] && echo '已对齐' || echo '缺失'))"

# ---------------------------------------------------------------- 4. systemd + enroll
info "步骤 4/5：安装 systemd 单元"
if [[ $HAS_SYSTEMD -eq 1 ]]; then
  UNIT_SRC="$SCRIPT_DIR/deploy/$AGENT_SERVICE.service"
  [[ -f "$UNIT_SRC" ]] || UNIT_SRC="$SCRIPT_DIR/deploy/hysteria-agent.service"
  if [[ -f "$UNIT_SRC" ]]; then
    install -m 0644 "$UNIT_SRC" "/etc/systemd/system/$AGENT_SERVICE.service"
  else
    write_file "/etc/systemd/system/$AGENT_SERVICE.service" 644 <<EOF
[Unit]
Description=Hysteria2 Node Agent (hysteria-agent)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$AGENT_BIN run -config $AGENT_CFG
Restart=always
RestartSec=3
KillMode=control-group
TimeoutStopSec=30
LimitNOFILE=1048576
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=$AGENT_CFG_DIR /etc/hysteria $AGENT_STATE /run/hysteria-agent /usr/local/bin

[Install]
WantedBy=multi-user.target
EOF
  fi
  systemctl daemon-reload
  log "已安装 /etc/systemd/system/$AGENT_SERVICE.service"
fi

if [[ -f "$AGENT_CFG_DIR/credentials.json" && $SKIP_ENROLL -eq 0 ]]; then
  info "已有凭据，跳过注册（如需重新注册请先用 --skip-enroll 或删除 credentials.json）"
fi
if [[ ! -f "$AGENT_CFG_DIR/credentials.json" && $SKIP_ENROLL -eq 0 ]]; then
  if [[ -z "$ENROLL_TOKEN" ]]; then
    warn "没有注册令牌：用 ENROLL_TOKEN=xxx 或 --token=xxx 重跑，或稍后手工执行"
    warn "  $AGENT_BIN enroll -config $AGENT_CFG --token=<token>"
  else
    info "注册节点到控制面"
    ARGS=(enroll -config "$AGENT_CFG" --token="$ENROLL_TOKEN")
    [[ -n "$NODE_NAME" ]] && ARGS+=(--name "$NODE_NAME")
    if "$AGENT_BIN" "${ARGS[@]}"; then
      log "注册成功，凭据写入 $AGENT_CFG_DIR/credentials.json (0400)"
    else
      die "注册失败（检查控制面地址、令牌、TLS/pin 配置）"
    fi
  fi
fi

if [[ $HAS_SYSTEMD -eq 1 ]]; then
  systemctl enable "$AGENT_SERVICE" >/dev/null 2>&1 || true
  systemctl restart "$AGENT_SERVICE" || warn "启动 $AGENT_SERVICE 失败"
  sleep 3
fi

# ---------------------------------------------------------------- 5. 验证
info "步骤 5/5：验证"
sleep 2
verify || warn "验证未完全通过，请按上面的提示排查"
echo >&2
info "===== systemctl status ====="
if [[ $HAS_SYSTEMD -eq 1 ]]; then systemctl --no-pager -l status "$AGENT_SERVICE" | head -20 >&2 || true; fi
info "===== journalctl -u $AGENT_SERVICE（末尾 20 行）====="
if [[ $HAS_SYSTEMD -eq 1 ]]; then journalctl -u "$AGENT_SERVICE" -n 20 --no-pager >&2 || true; fi
echo >&2
log "安装完成。常用命令："
echo "  hysteria-agent status" >&2
echo "  hysteria-agent diag" >&2
echo "  hysteria-agent restart" >&2
echo "  bash $0 $CONTROL_URL --verify-only   # 只做巡检" >&2
