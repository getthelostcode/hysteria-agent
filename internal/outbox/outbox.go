// Package outbox 实现"断线期间本地缓存事件、重连后补发"的可靠投递缓冲。
//
// 语义：
//   - 每个需要可靠投递的信封都被分配一个单调递增的 seq（agent 侧唯一）；
//   - 追加写入 <path>（JSON Lines，一行一个信封：{"seq":N,"kind":...,"env":base64}）；
//   - 控制面用 EventAck.acked_seq 做累积确认，Ack(seq) 之后本地可以丢弃；
//   - 文件是 append-only 的，被确认的前缀在 Compact() 时整体重写（原子 rename）；
//   - 超过 max_bytes / max_events 时丢弃最旧的事件（丢日志优先丢 info），并计数上报。
//
// 为什么不用 SQLite：单写入者、顺序追加、上限可控，一个 JSONL 文件就够，
// 且排障时人眼可读（cat 即可看到积压了什么）。
package outbox

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
)

// Entry 是缓冲里的一条待投递事件。
type Entry struct {
	Seq     uint64            `json:"seq"`
	Kind    string            `json:"kind"` // log|metric|config_ack|command_result|command_chunk|event
	TsMs    int64             `json:"ts"`
	TraceID string            `json:"trace_id,omitempty"`
	Env     string            `json:"env"` // base64(Envelope)
	env     *agentpb.Envelope // 反序列化缓存（不落盘）
}

// Outbox 是并发安全的投递缓冲。
type Outbox struct {
	mu    sync.Mutex
	path  string
	f     *os.File
	fsync bool

	entries         []*Entry
	bytes           int64
	maxBytes        int64
	maxEvents       int
	compactMinDrops int

	lastSeq     uint64
	ackedSeq    uint64
	dropped     uint64
	compactions uint64
}

// Open 打开（或创建）缓冲文件并恢复其中未确认的事件。
func Open(path string, maxBytes int64, maxEvents int, fsync bool, compactMinDrops int) (*Outbox, error) {
	o := &Outbox{
		path: path, maxBytes: maxBytes, maxEvents: maxEvents,
		fsync: fsync, compactMinDrops: compactMinDrops,
	}
	if path == "" {
		return o, nil // 纯内存模式（本地调试 / 单元测试）
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("创建 outbox 目录失败: %w", err)
	}
	if err := o.load(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("打开 outbox 失败: %w", err)
	}
	o.f = f
	o.loadAckWatermark()
	// 序号必须永远大于已确认水位线，否则新事件会被算成"已确认的旧事件"而永不投递。
	// （多进程共用同一个 state_dir 之类的异常场景会造出这种不一致，宁可让序号跳号。）
	if o.lastSeq < o.ackedSeq {
		o.lastSeq = o.ackedSeq
	}
	// 恢复出来的内容整体重写一次：丢掉坏行、让 seq 连续。
	if err := o.rewriteLocked(); err != nil {
		return nil, err
	}
	return o, nil
}

// ackPath 是确认水位线的旁路文件。
//
// 为什么需要它：outbox 文件里只保留"未确认"的事件，重启后无法从文件本身推断
// 已经确认到哪了。少了这个水位线，Hello.last_acked_seq 会退回 0，控制面可能
// 重复补发一整个窗口的事件。
func (o *Outbox) ackPath() string { return o.path + ".ack" }

func (o *Outbox) loadAckWatermark() {
	b, err := os.ReadFile(o.ackPath())
	if err != nil {
		return
	}
	var v uint64
	if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &v); err == nil {
		o.ackedSeq = v
	}
}

func (o *Outbox) saveAckWatermark() {
	if o.path == "" {
		return
	}
	tmp := o.ackPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", o.ackedSeq)), 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, o.ackPath())
}

func (o *Outbox) load() error {
	f, err := os.Open(o.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			// 坏行（掉电写了一半）不该让 agent 起不来：跳过并继续。
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(e.Env)
		if err != nil {
			continue
		}
		env := &agentpb.Envelope{}
		if err := proto.Unmarshal(raw, env); err != nil {
			continue
		}
		e.env = env
		o.entries = append(o.entries, &e)
		if e.Seq > o.lastSeq {
			o.lastSeq = e.Seq
		}
		o.bytes += int64(len(line))
	}
	return sc.Err()
}

// Append 分配 seq、序列化信封并落盘。env.Seq 会被就地覆盖为分配到的序号。
func (o *Outbox) Append(env *agentpb.Envelope, kind string) (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lastSeq++
	env.Seq = o.lastSeq
	raw, err := proto.Marshal(env)
	if err != nil {
		return 0, err
	}
	e := &Entry{
		Seq:     env.Seq,
		Kind:    kind,
		TsMs:    env.TsMs,
		TraceID: env.TraceId,
		Env:     base64.StdEncoding.EncodeToString(raw),
		env:     env,
	}
	o.entries = append(o.entries, e)
	o.bytes += int64(len(e.Env)) + 128 // 估算行开销

	if o.f != nil {
		line, err := json.Marshal(e)
		if err != nil {
			return 0, err
		}
		line = append(line, '\n')
		if _, err := o.f.Write(line); err != nil {
			return 0, err
		}
		if o.fsync {
			if err := o.f.Sync(); err != nil {
				return 0, err
			}
		}
	}

	dropped := o.enforceLimitsLocked()
	if dropped > 0 {
		if err := o.rewriteLocked(); err != nil {
			return env.Seq, err
		}
	}
	return env.Seq, nil
}

// enforceLimitsLocked 在超限时从最旧开始丢弃，返回本次丢弃数量。
func (o *Outbox) enforceLimitsLocked() int {
	dropped := 0
	for (o.maxEvents > 0 && len(o.entries) > o.maxEvents) || (o.maxBytes > 0 && o.bytes > o.maxBytes) {
		if len(o.entries) == 0 {
			break
		}
		// 优先丢"价值最低"的事件：日志 > 指标 > 命令结果/配置回执。
		idx := -1
		for i, e := range o.entries {
			if e.Kind == "log" {
				idx = i
				break
			}
		}
		if idx < 0 {
			for i, e := range o.entries {
				if e.Kind == "metric" {
					idx = i
					break
				}
			}
		}
		if idx < 0 {
			idx = 0 // 全都是不可丢的（命令结果/配置回执），只能丢最旧的
		}
		o.bytes -= int64(len(o.entries[idx].Env)) + 128
		o.entries = append(o.entries[:idx], o.entries[idx+1:]...)
		o.dropped++
		dropped++
	}
	return dropped
}

// Snapshot 返回当前待投递事件的拷贝（按 seq 升序）。
func (o *Outbox) Snapshot() []*Entry {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*Entry, len(o.entries))
	copy(out, o.entries)
	return out
}

// PendingFrom 返回 seq >= from 的事件（重连补发用）。
func (o *Outbox) PendingFrom(from uint64) []*Entry {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*Entry, 0, len(o.entries))
	for _, e := range o.entries {
		if e.Seq >= from {
			out = append(out, e)
		}
	}
	return out
}

// ackLocked 需要持有锁。
func (o *Outbox) Ack(seq uint64) (removed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if seq <= o.ackedSeq {
		return 0
	}
	o.ackedSeq = seq
	keep := o.entries[:0]
	for _, e := range o.entries {
		if e.Seq <= seq {
			o.bytes -= int64(len(e.Env)) + 128
			removed++
			continue
		}
		keep = append(keep, e)
	}
	o.entries = keep
	o.saveAckWatermark()
	if removed >= o.compactMinDrops {
		if err := o.rewriteLocked(); err != nil {
			return removed
		}
	}
	return removed
}

// rewriteLocked 原子重写文件（tmp + rename），只保留当前 entries。
func (o *Outbox) rewriteLocked() error {
	if o.path == "" {
		return nil
	}
	tmp := o.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 128*1024)
	for _, e := range o.entries {
		line, err := json.Marshal(e)
		if err != nil {
			continue
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, o.path); err != nil {
		return err
	}
	if o.f != nil {
		o.f.Close()
		nf, err := os.OpenFile(o.path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		o.f = nf
	}
	o.compactions++
	// 重算字节数
	o.bytes = 0
	for _, e := range o.entries {
		o.bytes += int64(len(e.Env)) + 128
	}
	return nil
}

// Clear 丢弃全部积压（对应 CLEAR_OUTBOX 指令，需控制面显式下发）。
func (o *Outbox) Clear() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(o.entries)
	o.entries = nil
	o.ackedSeq = o.lastSeq
	o.saveAckWatermark()
	if o.path != "" {
		o.bytes = 0
		if err := o.rewriteLocked(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Stat 返回 (待投递数, 占用字节, 已丢弃数, 已确认 seq, 已分配最大 seq)。
func (o *Outbox) Stat() (pending int, bytes int64, dropped uint64, acked, last uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries), o.bytes, o.dropped, o.ackedSeq, o.lastSeq
}

// SortedSeqs 返回当前待投递的 seq 列表（排障用）。
func (o *Outbox) SortedSeqs() []uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	seqs := make([]uint64, 0, len(o.entries))
	for _, e := range o.entries {
		seqs = append(seqs, e.Seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

// Close 关闭文件句柄。
func (o *Outbox) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.f == nil {
		return nil
	}
	err := o.f.Sync()
	if cerr := o.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	o.f = nil
	o.saveAckWatermark()
	return err
}

// Envelope 返回条目的信封（已反序列化）。
func (e *Entry) Envelope() *agentpb.Envelope { return e.env }
