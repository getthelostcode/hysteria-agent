#!/usr/bin/env bash
#
# scripts/release-dist.sh —— 重打「仓库里那份发布二进制」：编译 + 写 .sha256 + 写 .commit
#
# .commit 记录这份二进制是用哪个提交编的（= **最后改动 agent 源码的提交**，不是打包提交）。
# install.sh 靠它判断「仓库里的发布产物过期没有」，节点装到的版本也因此与发布版本对得上。
#
# 用法：bash scripts/release-dist.sh [输出路径]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist/hysteria-agent-linux-amd64}"
GO="${GO:-}"
[[ -n "$GO" ]] || GO="$(command -v go || echo /usr/local/go/bin/go)"

commit="$(cd "$ROOT" && git log -1 --format=%h -- cmd internal '*.go')"
[[ -n "$(cd "$ROOT" && git status --porcelain -- cmd internal '*.go')" ]] && commit="${commit}-dirty"
build="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

"$GO" build -trimpath \
  -ldflags "-s -w \
    -X github.com/getthelostcode/hysteria-agent/internal/version.Version=${AGENT_VERSION:-0.1.0} \
    -X github.com/getthelostcode/hysteria-agent/internal/version.Commit=${commit} \
    -X github.com/getthelostcode/hysteria-agent/internal/version.BuildTime=${build}" \
  -o "$OUT" ./cmd/hysteria-agent

( cd "$ROOT" && sha256sum "$(basename "$OUT")" > /dev/null 2>&1 || true )
sha256="$(sha256sum "$OUT" | awk '{print $1}')"
printf '%s  %s\n' "$sha256" "$(basename "$OUT")" > "$OUT.sha256"
printf '%s\n' "$commit" > "$OUT.commit"
printf '[OK] 发布 %s\n     commit: %s\n     build:  %s\n     sha256: %s\n' "$OUT" "$commit" "$build" "$sha256"
