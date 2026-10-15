// Package logging 提供结构化（JSON）日志、本地环形缓冲与远程日志投递。
//
// 输出格式（字段顺序即 slog JSONHandler 的写入顺序）：
//
//	{"ts":"2026-10-15T09:00:00.000Z","level":"INFO","node_id":"n_1","component":"control","msg":"已连接控制面","trace_id":"...","...":"..."}
//
// 三条链路：
//  1. stdout —— systemd/journald 收集，永远开启；
//  2. 环形缓冲 —— 给 tail_logs 指令做"最近 N 条"回放；
//  3. 远程投递 —— 经控制通道 LogBatch 发给控制面，带采样率限制（见 Sampler）。
package logging

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

type ctxKey string

const traceKey ctxKey = "trace_id"

// WithTrace 把 trace_id 放进 context，之后该 ctx 上打的所有日志都会带上它。
func WithTrace(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceKey, traceID)
}

// TraceOf 取出 context 里的 trace_id。
func TraceOf(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(traceKey).(string); ok {
		return v
	}
	return ""
}

// Options 日志初始化参数。
type Options struct {
	Level    string
	Format   string // json | text
	NodeID   string
	RingSize int
	Stdout   io.Writer
}

// attrsHandler 在每条记录上补 node_id，并把 context 里的 trace_id 注入。
type attrsHandler struct {
	inner slog.Handler
	base  []slog.Attr
}

func (h *attrsHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *attrsHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, a := range h.base {
		r.AddAttrs(a)
	}
	if tid := TraceOf(ctx); tid != "" {
		r.AddAttrs(slog.String("trace_id", tid))
	}
	return h.inner.Handle(ctx, r)
}

func (h *attrsHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &attrsHandler{inner: h.inner.WithAttrs(attrs), base: h.base}
}

func (h *attrsHandler) WithGroup(name string) slog.Handler {
	return &attrsHandler{inner: h.inner.WithGroup(name), base: h.base}
}

// New 构建 logger + Hub。返回的 Hub 用于本地回放与远程投递。
func New(opts Options) (*slog.Logger, *Hub) {
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.RingSize <= 0 {
		opts.RingSize = 2000
	}
	hub := NewHub(opts.RingSize)

	// 每行日志同时写 stdout 和 Hub（Hub 负责解析成 LogEntry）。
	w := io.MultiWriter(opts.Stdout, hub)

	level := parseLevel(opts.Level)
	handlerOpts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 {
				switch a.Key {
				case slog.TimeKey:
					a.Key = "ts"
				case slog.MessageKey:
					a.Key = "msg"
				}
			}
			return a
		},
	}
	var inner slog.Handler
	if opts.Format == "text" {
		inner = slog.NewTextHandler(w, handlerOpts)
	} else {
		inner = slog.NewJSONHandler(w, handlerOpts)
	}
	h := &attrsHandler{inner: inner}
	if opts.NodeID != "" {
		h.base = []slog.Attr{slog.String("node_id", opts.NodeID)}
	}
	return slog.New(h), hub
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Component 给 logger 打上 component 字段（agent|control|supervisor|hyconfig|upgrade|diag|...）。
func Component(l *slog.Logger, name string) *slog.Logger {
	return l.With(slog.String("component", name))
}

// LevelName 把 slog 级别转成小写字符串，与控制通道 LogEntry.level 对齐。
func LevelName(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "debug"
	case l < slog.LevelWarn:
		return "info"
	case l < slog.LevelError:
		return "warn"
	default:
		return "error"
	}
}

// ParseLevel 供命令参数解析用。
func ParseLevel(s string) slog.Level { return parseLevel(s) }

// ---------------------------- 采样 ----------------------------

// Sampler 是简单的令牌桶采样器：保护控制面不被日志风暴打爆。
// 约定：warn/error 使用一个独立的小桶，绝不被 info/debug 的流量饿死。
type Sampler struct {
	rate  float64 // tokens/second
	burst float64

	mu        sync.Mutex
	tokens    float64
	last      time.Time
	errRate   float64
	errBurst  float64
	errTokens float64
	errLast   time.Time
	dropped   uint64
}

// NewSampler 构建采样器。
func NewSampler(linesPerSecond, burst int) *Sampler {
	if linesPerSecond <= 0 {
		linesPerSecond = 200
	}
	if burst <= 0 {
		burst = linesPerSecond * 2
	}
	return &Sampler{
		rate:      float64(linesPerSecond),
		burst:     float64(burst),
		tokens:    float64(burst),
		last:      time.Now(),
		errRate:   float64(linesPerSecond) / 10,
		errBurst:  float64(burst) / 10,
		errTokens: float64(burst) / 10,
		errLast:   time.Now(),
	}
}

// Allow 返回该级别日志是否允许远程投递。
func (s *Sampler) Allow(level string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	high := level == "warn" || level == "error"
	rate, burst := s.rate, s.burst
	tokens, last := &s.tokens, &s.last
	if high {
		rate, burst = s.errRate, s.errBurst
		tokens, last = &s.errTokens, &s.errLast
	}
	*tokens += now.Sub(*last).Seconds() * rate
	if *tokens > burst {
		*tokens = burst
	}
	*last = now
	if *tokens >= 1 {
		*tokens--
		return true
	}
	s.dropped++
	return false
}

// Dropped 返回被采样丢弃的行数。
func (s *Sampler) Dropped() uint64 { return atomic.LoadUint64(&s.dropped) }

// ---------------------------- Hub ----------------------------

// Hub 既是 io.Writer（接收 slog 写出的 JSON 行），也是日志分发中心。
type Hub struct {
	mu      sync.RWMutex
	ring    []*agentpb.LogEntry
	cap     int
	nextID  uint64
	subs    map[int]*Sub
	nextSub int

	total   uint64
	dropped uint64
}

// Sub 是 tail_logs 用的订阅句柄。
type Sub struct {
	Ch   chan *agentpb.LogEntry
	hub  *Hub
	id   int
	stop sync.Once
}

// NewHub 构建环形缓冲 Hub。
func NewHub(size int) *Hub {
	if size <= 0 {
		size = 2000
	}
	return &Hub{cap: size, subs: map[int]*Sub{}}
}

// Write 实现 io.Writer：接收一行 JSON 日志，解析成 LogEntry 后入环并广播。
// 非 JSON（例如手写的 fmt.Fprintln 或子进程日志）会被包成 msg 原文的 info 记录。
func (h *Hub) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		entry := parseLogLine(line)
		h.push(entry)
	}
	return len(p), nil
}

func parseLogLine(line string) *agentpb.LogEntry {
	e := &agentpb.LogEntry{TsMs: time.Now().UnixMilli(), Level: "info", Component: "agent", Msg: line}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return e
	}
	if v, ok := m["ts"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			e.TsMs = t.UnixMilli()
		}
		delete(m, "ts")
	}
	if v, ok := m["level"].(string); ok {
		e.Level = strings.ToLower(v)
		delete(m, "level")
	}
	if v, ok := m["msg"].(string); ok {
		e.Msg = v
		delete(m, "msg")
	}
	if v, ok := m["component"].(string); ok {
		e.Component = v
		delete(m, "component")
	}
	if v, ok := m["trace_id"].(string); ok {
		e.TraceId = v
		delete(m, "trace_id")
	}
	delete(m, "node_id") // node_id 在信封上已经有，避免重复
	if len(m) > 0 {
		if b, err := json.Marshal(m); err == nil {
			e.FieldsJson = b
		}
	}
	return e
}

func (h *Hub) push(e *agentpb.LogEntry) {
	h.mu.Lock()
	h.nextID++
	e.Id = h.nextID
	h.total++
	if len(h.ring) >= h.cap {
		// 覆盖最旧的；如果恰好被 tail 订阅者正在读，也仍可从订阅通道拿到（环形只做回放）。
		copy(h.ring, h.ring[1:])
		h.ring[len(h.ring)-1] = e
	} else {
		h.ring = append(h.ring, e)
	}
	subs := make([]*Sub, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()

	for _, s := range subs {
		select {
		case s.Ch <- e:
		default:
			// 订阅者跟不上：丢弃并记账，绝不阻塞生产过程。
			atomic.AddUint64(&h.dropped, 1)
		}
	}
}

// Tail 返回最近 n 条日志的拷贝（n<=0 表示全部）。
func (h *Hub) Tail(n int) []*agentpb.LogEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if n <= 0 || n > len(h.ring) {
		n = len(h.ring)
	}
	out := make([]*agentpb.LogEntry, 0, n)
	out = append(out, h.ring[len(h.ring)-n:]...)
	return out
}

// Subscribe 订阅后续日志；调用 Sub.Close 取消。
func (h *Hub) Subscribe(buffer int) *Sub {
	if buffer <= 0 {
		buffer = 256
	}
	s := &Sub{Ch: make(chan *agentpb.LogEntry, buffer), hub: h}
	h.mu.Lock()
	s.id = h.nextSub
	h.nextSub++
	h.subs[s.id] = s
	h.mu.Unlock()
	return s
}

// Close 取消订阅。
func (s *Sub) Close() {
	s.stop.Do(func() {
		s.hub.mu.Lock()
		delete(s.hub.subs, s.id)
		s.hub.mu.Unlock()
		close(s.Ch)
	})
}

// Stats 返回总量与被丢弃量（丢给订阅者的行数）。
func (h *Hub) Stats() (total, dropped uint64) {
	return atomic.LoadUint64(&h.total), atomic.LoadUint64(&h.dropped)
}

// Ingest 供"非 slog 来源"（例如 hysteria 子进程日志）写入 Hub。
func (h *Hub) Ingest(level, component, msg, traceID string, fields []byte) {
	h.push(&agentpb.LogEntry{
		TsMs:       time.Now().UnixMilli(),
		Level:      level,
		Component:  component,
		Msg:        msg,
		TraceId:    traceID,
		FieldsJson: fields,
	})
}

// NewLogEnvelope 把一批日志包成控制通道信封（供远程日志采集使用）。
func NewLogEnvelope(entries []*agentpb.LogEntry, source string) *agentpb.Envelope {
	return wire.New(agentpb.MsgType_MSG_TYPE_LOG_BATCH, &agentpb.LogBatch{Entries: entries, Source: source})
}
