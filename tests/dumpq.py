#!/usr/bin/env python3
"""dumpq.py — 查询 mockcontrol 落盘的 JSONL 消息流，供 e2e 脚本断言用。

用法:
  dumpq.py count <dump> <type>                # 某类型消息条数
  dumpq.py types <dump>                       # 各类型计数（JSON）
  dumpq.py last <dump> <type> [dotted.path]   # 最后一条该类型消息（可选取字段）
  dumpq.py seqs <dump>                        # 所有 seq>0 的消息序号
  dumpq.py check <dump> <start>               # 从 start 起是否连续且无重复
"""
import base64
import json
import sys


def load(path):
    out = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return out


def body(rec):
    b = rec.get("body") or {}
    # protojson 的 oneof 字段名是 camelCase，如 "configAck"
    for v in b.values():
        if isinstance(v, dict):
            return v
    return {}


BYTES_FIELDS = {"resultJson", "data", "payload", "fieldsJson", "paramsJson", "dataJson", "signature"}


def norm(name):
    """类型名归一化：dump 里存的是枚举名（command_result），断言里常写驼峰（commandResult）。"""
    return (name or "").replace("_", "").lower()


def match_type(rec, wanted):
    if rec.get("type") == wanted:
        return True
    return norm(rec.get("type")) == norm(wanted)


def get_path(obj, path):
    cur = obj
    parts = path.split(".")
    for i, part in enumerate(parts):
        key = part
        if isinstance(cur, dict) and part not in cur:
            snake = "".join("_" + c.lower() if c.isupper() else c for c in part)
            if snake in cur:
                key = snake
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        # protojson 把 bytes 编码成 base64：断言前先解码成原文
        if isinstance(cur, str) and key in BYTES_FIELDS:
            try:
                cur = base64.b64decode(cur).decode("utf-8", "replace")
            except Exception:
                pass
    return cur


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    cmd, path = sys.argv[1], sys.argv[2]
    recs = load(path)

    if cmd == "count":
        t = sys.argv[3]
        print(sum(1 for r in recs if match_type(r, t)))
    elif cmd == "types":
        counts = {}
        for r in recs:
            counts[r.get("type")] = counts.get(r.get("type"), 0) + 1
        print(json.dumps(counts, ensure_ascii=False))
    elif cmd == "last":
        t = sys.argv[3]
        want_path = sys.argv[4] if len(sys.argv) > 4 else None
        sel = [r for r in recs if match_type(r, t)]
        if not sel:
            print("MISSING")
            return 1
        rec = sel[-1]
        if want_path:
            val = get_path(body(rec), want_path)
            if val is None:
                print("MISSING")
                return 1
            print(json.dumps(val, ensure_ascii=False) if not isinstance(val, str) else val)
        else:
            print(json.dumps(rec.get("body"), ensure_ascii=False))
    elif cmd == "seqs":
        print(json.dumps([r.get("seq") for r in recs if r.get("seq")]))
    elif cmd == "where":
        # dumpq.py where <dump> <type> <path> <value> [<printpath>]
        t, want_path, want_val = sys.argv[3], sys.argv[4], sys.argv[5]
        print_path = sys.argv[6] if len(sys.argv) > 6 else None
        hits = [r for r in recs if match_type(r, t) and str(get_path(body(r), want_path)) == want_val]
        if not hits:
            print("NO_MATCH")
            return 1
        for r in hits:
            if print_path:
                v = get_path(body(r), print_path)
                print(json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v)
            else:
                print(json.dumps(r.get("body"), ensure_ascii=False))
    elif cmd == "cat":
        # dumpq.py cat <dump> <type> <path>  —— 把所有匹配记录的该字段按序拼接输出
        t, want_path = sys.argv[3], sys.argv[4]
        out = []
        for r in recs:
            if not match_type(r, t):
                continue
            v = get_path(body(r), want_path)
            if v is None:
                continue
            out.append(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
        print("".join(out))
    elif cmd == "check":
        start = int(sys.argv[3])
        seqs = [r["seq"] for r in recs if r.get("seq")]
        if not seqs:
            print("no-seqs")
            return 1
        uniq = sorted(set(seqs))
        problems = []
        if uniq[0] != start:
            problems.append(f"first={uniq[0]} 期望 {start}")
        for a, b in zip(uniq, uniq[1:]):
            if b != a + 1:
                problems.append(f"缺口 {a}->{b}")
        if len(uniq) != len(seqs):
            problems.append(f"有重复: {len(seqs)} 条消息 / {len(uniq)} 个唯一 seq")
        if problems:
            print("FAIL: " + "; ".join(problems))
            return 1
        print(f"OK: seq {uniq[0]}..{uniq[-1]} 连续无重复（{len(uniq)} 条）")
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
