#!/usr/bin/env bash
# 清理测试残留进程。
#
# 用 ps + awk 选 PID 再 kill，而不是 pkill -f <模式>：pkill -f 会匹配到外层
# shell / Hermes 快照包装进程的命令行，把自己一起杀掉（真实踩过的坑）。
SELF=$$
list() {
  ps -eo pid=,args= 2>/dev/null | awk -v me="$SELF" '
    $1 == me { next }
    /cleanup\.sh/ { next }
    /mockhysteria|mockcontrol -listen|hysteria-agent -config|hysteria-agent .* run/ { print $1 }
  ' | sort -u
}

PIDS="$(list)"
if [[ -n "$PIDS" ]]; then
  # shellcheck disable=SC2086
  kill $PIDS 2>/dev/null
  sleep 1
  PIDS="$(list)"
  if [[ -n "$PIDS" ]]; then
    # shellcheck disable=SC2086
    kill -9 $PIDS 2>/dev/null
    sleep 0.5
  fi
fi
echo "清理完成，剩余相关进程: $(list | wc -l)"
