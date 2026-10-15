package outbox

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

func metricEnv(ts int64) *agentpb.Envelope {
	return wire.New(agentpb.MsgType_MSG_TYPE_METRIC_BATCH, &agentpb.MetricBatch{
		Sample: &agentpb.MetricSample{TsMs: ts, CpuPercent: 1.5},
	})
}

func TestAppendAckAndResend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outbox.jsonl")
	ob, err := Open(path, 1<<20, 100, true, 2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer ob.Close()

	for i := 1; i <= 5; i++ {
		seq, err := ob.Append(metricEnv(int64(i)), "metric")
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq != uint64(i) {
			t.Fatalf("seq 应为 %d，得到 %d（seq 必须单调递增）", i, seq)
		}
	}
	pending, _, _, acked, last := ob.Stat()
	if pending != 5 || acked != 0 || last != 5 {
		t.Fatalf("stat 异常: pending=%d acked=%d last=%d", pending, acked, last)
	}

	// 累积确认 3 之后，1..3 必须被丢弃、4..5 保留（重连只补发未确认的部分）。
	if removed := ob.Ack(3); removed != 3 {
		t.Fatalf("Ack(3) 应移除 3 条，实际 %d", removed)
	}
	pending, _, _, acked, _ = ob.Stat()
	if pending != 2 || acked != 3 {
		t.Fatalf("Ack 后 stat 异常: pending=%d acked=%d", pending, acked)
	}
	entries := ob.PendingFrom(1)
	if len(entries) != 2 || entries[0].Seq != 4 || entries[1].Seq != 5 {
		t.Fatalf("PendingFrom(1) 应返回 seq 4,5，得到 %d 条", len(entries))
	}
	_ = entries[0].Envelope() // 必须能反序列化回原信封
}

// 重启恢复是这类程序最关键的可靠性属性：进程重启后未确认事件必须还在。
func TestRestoreAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outbox.jsonl")

	ob, err := Open(path, 1<<20, 100, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := ob.Append(metricEnv(int64(i)), "metric"); err != nil {
			t.Fatal(err)
		}
	}
	ob.Ack(1)
	ob.Close()

	ob2, err := Open(path, 1<<20, 100, true, 1)
	if err != nil {
		t.Fatalf("重启后 Open 失败: %v", err)
	}
	defer ob2.Close()
	pending, _, _, acked, last := ob2.Stat()
	if pending != 3 {
		t.Fatalf("重启后应恢复 3 条未确认事件，得到 %d", pending)
	}
	if acked != 1 || last != 4 {
		t.Fatalf("重启后 seq 状态丢失: acked=%d last=%d", acked, last)
	}
	// 恢复后继续追加必须是 5，不能与已有 seq 冲突。
	seq, err := ob2.Append(metricEnv(99), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Fatalf("继续追加的 seq 应为 5，得到 %d", seq)
	}
}

func TestLimitsDropOldestLogs(t *testing.T) {
	dir := t.TempDir()
	ob, err := Open(filepath.Join(dir, "o.jsonl"), 1<<20, 5, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	for i := 0; i < 8; i++ {
		if _, err := ob.Append(metricEnv(int64(i)), "log"); err != nil {
			t.Fatal(err)
		}
	}
	pending, _, dropped, _, last := ob.Stat()
	if pending > 5 {
		t.Fatalf("超出 max_events 后仍保留 %d 条", pending)
	}
	if dropped == 0 {
		t.Fatalf("应记录丢弃数量")
	}
	if last != 8 {
		t.Fatalf("seq 分配不应受丢弃影响, last=%d", last)
	}
}

func TestCorruptLineSkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "o.jsonl")
	ob, _ := Open(path, 1<<20, 10, false, 1)
	_, _ = ob.Append(metricEnv(1), "metric")
	ob.Close()

	// 模拟掉电写了一半的坏行
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"seq":2,"kind":"metric","env":"@@broken@@` + "\n")
	f.Close()

	ob2, err := Open(path, 1<<20, 10, false, 1)
	if err != nil {
		t.Fatalf("坏行不应该让 Open 失败: %v", err)
	}
	defer ob2.Close()
	if pending, _, _, _, _ := ob2.Stat(); pending != 1 {
		t.Fatalf("应只恢复 1 条完好事件，得到 %d", pending)
	}
}

// 回归：确认水位线高于文件里最大 seq（多进程共用 state_dir、或被篡改）时，
// 新事件不能因为"序号 ≤ 水位线"而被永久搁置。历史上真出过这个 bug：
// 积压 133 条事件（含指令结果）永远发不出去。
func TestWatermarkAheadOfEntriesDoesNotStrandEvents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "o.jsonl")
	ob, err := Open(path, 1<<20, 100, false, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := ob.Append(metricEnv(int64(i)), "metric"); err != nil {
			t.Fatal(err)
		}
	}
	ob.Close()

	// 伪造一个更高的确认水位线
	if err := os.WriteFile(path+".ack", []byte("999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ob2, err := Open(path, 1<<20, 100, false, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer ob2.Close()
	seq, err := ob2.Append(metricEnv(42), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if seq <= 999 {
		t.Fatalf("新事件的 seq 必须大于确认水位线，得到 %d", seq)
	}
	if pending, _, _, _, _ := ob2.Stat(); pending == 0 {
		t.Fatal("积压事件不应被丢弃")
	}
}

func TestClear(t *testing.T) {
	dir := t.TempDir()
	ob, _ := Open(filepath.Join(dir, "o.jsonl"), 1<<20, 10, false, 1)
	defer ob.Close()
	for i := 0; i < 3; i++ {
		ob.Append(metricEnv(int64(i)), "metric")
	}
	n, err := ob.Clear()
	if err != nil || n != 3 {
		t.Fatalf("Clear 应清掉 3 条，得到 %d err=%v", n, err)
	}
	if pending, _, _, _, _ := ob.Stat(); pending != 0 {
		t.Fatalf("Clear 后仍有 %d 条", pending)
	}
}
