#!/usr/bin/env bash
# 手工调试脚本：起假控制面 + Agent，跑几条指令并打印诊断信息。
# 复用上一轮 e2e 的配置/凭据，并把凭据里的节点预置进 mockcontrol 的注册表（验证持久化路径）。
cd /tmp/hysteria-agent-e2e || exit 1
export PATH=$PATH:/usr/local/go/bin
pkill -f "[m]ockcontrol"; pkill -f "[m]ockhysteria"; sleep 1
rm -f /tmp/dbg-dump.jsonl /tmp/dbg-mock.log /tmp/dbg-agent.log

python3 - <<'PY'
import json
c = json.load(open('etc/agent/credentials.json'))
json.dump([{"NodeID": c["node_id"], "Secret": c["node_secret"], "Name": "e2e-node"}],
          open('/tmp/dbg-dump.jsonl.nodes.json', 'w'), indent=2)
print("预置节点:", c["node_id"])
PY

setsid ./bin/mockcontrol -listen 127.0.0.1:18080 -enroll-token e2e-enroll-token \
  -dump /tmp/dbg-dump.jsonl -files-dir ./files -heartbeat-interval 3 -telemetry-interval 2 \
  > /tmp/dbg-mock.log 2>&1 < /dev/null &
setsid ./bin/hysteria-agent -config ./etc/agent/config.yaml run > /tmp/dbg-agent.log 2>&1 < /dev/null &
sleep 6

echo "== dump lines before commands: $(wc -l < /tmp/dbg-dump.jsonl)"
curl -s -X POST --data-binary '{"user_ids":["alice","bob"],"reason":"quota"}' \
  "http://127.0.0.1:18080/_test/command?type=kick_users&id=kick1" >/dev/null
curl -s -X POST -d '{}' "http://127.0.0.1:18080/_test/command?type=run_diagnostic&id=diag1" >/dev/null
curl -s -X POST -d '{"duration_seconds":3}' "http://127.0.0.1:18080/_test/command?type=tail_logs&id=tail1" >/dev/null
sleep 12

echo "== mock log (tail 25) =="; tail -25 /tmp/dbg-mock.log
echo "== types =="; python3 /root/hysteria-agent/tests/dumpq.py types /tmp/dbg-dump.jsonl
echo "== command results =="
python3 - /tmp/dbg-dump.jsonl <<'PY'
import json, base64, sys
for line in open(sys.argv[1]):
    r = json.loads(line)
    if r.get("type") != "commandResult":
        continue
    b = r["body"].get("commandResult", {})
    raw = b.get("resultJson", "")
    try:
        txt = base64.b64decode(raw).decode("utf-8", "replace")
    except Exception:
        txt = "(非 base64)"
    print(f"  id={b.get('commandId')} status={b.get('status')} error={b.get('error')!r} len={len(txt)}")
    if b.get("status") == "failed" and txt:
        try:
            rep = json.loads(txt)
            for c in rep.get("checks", []):
                print(f"     [{c['status']:7}] {c['name']}: {c['detail'][:110]}")
        except Exception as e:
            print("     (解析失败)", e, txt[:300])
PY
echo "== tail chunks =="; python3 /root/hysteria-agent/tests/dumpq.py count /tmp/dbg-dump.jsonl commandChunk
echo "== kick requests on hysteria =="; curl -s http://127.0.0.1:18081/_requests; echo
echo "== agent warn/error =="; grep -E '"level":"(ERROR|WARN)"' /tmp/dbg-agent.log | tail -5
