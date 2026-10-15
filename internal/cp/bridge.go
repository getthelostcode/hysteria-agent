package cp

// 桥接转发（S4）：把 Agent 的 Envelope 消息转成 Laravel 控制面的 HMAC REST。
//
//	Hello / Heartbeat → POST /api/agent/v1/heartbeat
//	MetricBatch       → POST /api/agent/v1/metrics   （gzip，单批 <= MaxMetricBatch）
//	LogBatch          → POST /api/agent/v1/logs      （单批 <= 500，与 Laravel 侧上限对齐）
//	诊断报告          → POST /api/agent/v1/diagnostics
//
// 只在 identity.authority=laravel 时启用；转发在后台 worker 里串行执行，
// Agent 的消息处理路径**永不阻塞**（队列满就丢弃并计数，S7 会补落盘 outbox）。
// 见 docs/BRIDGE-TO-CONTROL-PLANE.md §3。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/cpapi"
)

const (
	bridgeQueueSize   = 256
	bridgeMaxLogBatch = 500
)

type bridgeJob struct {
	kind string
	fn   func(context.Context) error
}

// forwarder 是一个节点的桥接转发器（每个节点一个后台 worker）。
type forwarder struct {
	nodeID  string
	client  *cpapi.Client
	cfg     *Config
	log     *slog.Logger
	metrics *Metrics

	queue chan bridgeJob
	stop  chan struct{}
	once  sync.Once
}

func newForwarder(cfg *Config, node *Node, log *slog.Logger, metrics *Metrics) (*forwarder, error) {
	if strings.TrimSpace(node.Secret) == "" {
		return nil, fmt.Errorf("节点 %s 缺少桥接密钥（仅在 identity.authority=laravel 下存在）", node.ID)
	}

	clientCfg := cfg.LaravelClientConfig()
	clientCfg.NodeID = node.ID
	clientCfg.Secret = node.Secret

	client, err := cpapi.New(clientCfg)
	if err != nil {
		return nil, err
	}

	f := &forwarder{
		nodeID:  node.ID,
		client:  client,
		cfg:     cfg,
		log:     log,
		metrics: metrics,
		queue:   make(chan bridgeJob, bridgeQueueSize),
		stop:    make(chan struct{}),
	}
	go f.worker()

	return f, nil
}

func (f *forwarder) submit(kind string, fn func(context.Context) error) {
	select {
	case f.queue <- bridgeJob{kind: kind, fn: fn}:
	default:
		f.metrics.bump("bridge_dropped")
		f.log.Warn("桥接队列已满，丢弃一条待转发消息", "node_id", f.nodeID, "kind", kind)
	}
}

func (f *forwarder) worker() {
	for {
		select {
		case <-f.stop:
			return
		case job := <-f.queue:
			ctx, cancel := context.WithTimeout(context.Background(), f.cfg.LaravelTimeout())
			err := job.fn(ctx)
			cancel()

			if err != nil {
				f.metrics.bump("bridge_failed")
				f.log.Warn("桥接转发失败", "node_id", f.nodeID, "kind", job.kind, "err", err)

				continue
			}
			f.metrics.bump("bridge_" + job.kind)
		}
	}
}

func (f *forwarder) close() {
	f.once.Do(func() { close(f.stop) })
}

// ---------------- Server 侧入口（onMessage 调用） ----------------

// bridgeForwarder 返回该节点的转发器；非桥接模式返回 nil（完全不发请求）。
func (s *Server) bridgeForwarder(nodeID string) *forwarder {
	if !s.cfg.UsesLaravelIdentity() {
		return nil
	}

	s.bridgeMu.Lock()
	defer s.bridgeMu.Unlock()

	if f, ok := s.bridges[nodeID]; ok {
		return f
	}

	node, ok := s.store.Node(nodeID)
	if !ok {
		return nil
	}

	f, err := newForwarder(s.cfg, node, s.log, s.metrics)
	if err != nil {
		s.log.Warn("桥接转发器不可用", "node_id", nodeID, "err", err)

		return nil
	}
	s.bridges[nodeID] = f
	s.log.Info("桥接转发器已建立", "node_id", nodeID, "laravel", s.cfg.Laravel.BaseURL)

	return f
}

// bridgeHeartbeat 上报心跳（Hello 之后立刻来一发，让面板马上看到节点在线）。
func (s *Server) bridgeHeartbeat(nodeID string, hb *agentpb.Heartbeat) {
	f := s.bridgeForwarder(nodeID)
	if f == nil {
		return
	}

	node, _ := s.store.Node(nodeID)
	req := heartbeatToLaravel(node, hb)
	f.submit("heartbeat", func(ctx context.Context) error {
		_, err := f.client.Heartbeat(ctx, req)

		return err
	})
}

// bridgeMetrics 转发一批遥测（单批一条样本，超出按 MaxMetricBatch 切分）。
func (s *Server) bridgeMetrics(nodeID string, m *agentpb.MetricBatch) {
	f := s.bridgeForwarder(nodeID)
	if f == nil || m == nil {
		return
	}

	sample := metricBatchToLaravel(m)
	f.submit("metrics", func(ctx context.Context) error {
		_, err := f.client.Metrics(ctx, []cpapi.MetricSample{sample})

		return err
	})
}

// bridgeLogs 转发日志批次（按 500 条切分）。
func (s *Server) bridgeLogs(nodeID string, l *agentpb.LogBatch) {
	f := s.bridgeForwarder(nodeID)
	if f == nil || l == nil || len(l.GetEntries()) == 0 {
		return
	}

	for _, chunk := range chunkLogEntries(l, bridgeMaxLogBatch) {
		entries := chunk
		f.submit("logs", func(ctx context.Context) error {
			_, err := f.client.Logs(ctx, entries)

			return err
		})
	}
}

// bridgeDiagnostics 转发诊断报告。
func (s *Server) bridgeDiagnostics(nodeID string, payload map[string]any) {
	f := s.bridgeForwarder(nodeID)
	if f == nil || payload == nil {
		return
	}

	f.submit("diagnostics", func(ctx context.Context) error {
		return f.client.Diagnostics(ctx, payload)
	})
}

// ---------------- 纯映射函数（可直接单测） ----------------

func heartbeatToLaravel(node *Node, hb *agentpb.Heartbeat) cpapi.HeartbeatRequest {
	req := cpapi.HeartbeatRequest{
		Meta: map[string]string{
			"hysteria_running": boolStr(hb.GetHysteriaRunning()),
			"ready":            boolStr(hb.GetReady()),
			"pending_events":   fmt.Sprintf("%d", hb.GetPendingEvents()),
			"sent_events":      fmt.Sprintf("%d", hb.GetSentEvents()),
			"last_acked_seq":   fmt.Sprintf("%d", hb.GetLastAckedSeq()),
			"source":           "go-controlplane-bridge",
		},
		Uptime: int64(hb.GetUptimeSeconds()),
	}
	if hb.GetConfigRevision() != "" {
		req.Meta["config_revision"] = hb.GetConfigRevision()
	}
	if node != nil {
		req.AgentVersion = node.AgentVersion
		req.Version = node.HysteriaVersion
		// 面板侧只看到本控制面的地址，公网出口靠这里转发。
		req.PublicIP = node.PublicIP
		if node.Kernel != "" {
			req.Meta["kernel"] = node.Kernel
		}
	}

	return req
}

func metricBatchToLaravel(m *agentpb.MetricBatch) cpapi.MetricSample {
	s := m.GetSample()
	h := m.GetHysteria()

	cpu := clampPercent(s.GetCpuPercent())
	mem := clampPercent(s.GetMemPercent())
	disk := clampPercent(s.GetDiskPercent())

	netRx := toInt64(h.GetRxBytes())
	netTx := toInt64(h.GetTxBytes())
	conns := toInt64(h.GetConns())
	users := toInt64(h.GetActiveUsers())

	extra := map[string]any{
		"load1":  s.GetLoad1(),
		"load5":  s.GetLoad5(),
		"load15": s.GetLoad15(),
	}
	if m.GetAgentVersion() != "" {
		extra["agent_version"] = m.GetAgentVersion()
	}
	if m.GetConfigRevision() != "" {
		extra["config_revision"] = m.GetConfigRevision()
	}
	if len(m.GetLabels()) > 0 {
		extra["labels"] = m.GetLabels()
	}
	if s.GetMemTotalBytes() > 0 {
		extra["mem_used_bytes"] = s.GetMemUsedBytes()
		extra["mem_total_bytes"] = s.GetMemTotalBytes()
	}
	if s.GetDiskTotalBytes() > 0 {
		extra["disk_used_bytes"] = s.GetDiskUsedBytes()
		extra["disk_total_bytes"] = s.GetDiskTotalBytes()
	}
	if s.GetProcCount() > 0 {
		extra["proc_count"] = s.GetProcCount()
	}

	extra["hysteria"] = map[string]any{
		"running":        h.GetRunning(),
		"pid":            h.GetPid(),
		"uptime_seconds": h.GetUptimeSeconds(),
		"version":        h.GetVersion(),
		"online_devices": h.GetOnlineDevices(),
		"last_exit_code": h.GetLastExitCode(),
		"restart_count":  h.GetRestartCount(),
	}

	recorded := msToRFC3339(s.GetTsMs())
	if recorded == "" {
		recorded = time.Now().UTC().Format(time.RFC3339)
	}

	return cpapi.MetricSample{
		CPU: &cpu, Mem: &mem, Disk: &disk,
		NetRx: &netRx, NetTx: &netTx, ConnCount: &conns, Users: &users,
		RecordedAt: recorded,
		Extra:      extra,
	}
}

func chunkLogEntries(l *agentpb.LogBatch, size int) [][]cpapi.LogEntry {
	if size <= 0 {
		size = bridgeMaxLogBatch
	}

	source := l.GetSource()
	entries := l.GetEntries()
	out := make([][]cpapi.LogEntry, 0, (len(entries)+size-1)/size)

	for start := 0; start < len(entries); start += size {
		end := start + size
		if end > len(entries) {
			end = len(entries)
		}

		chunk := make([]cpapi.LogEntry, 0, end-start)
		for _, e := range entries[start:end] {
			chunk = append(chunk, logEntryToLaravel(source, e))
		}
		out = append(out, chunk)
	}

	return out
}

func logEntryToLaravel(source string, e *agentpb.LogEntry) cpapi.LogEntry {
	ctx := map[string]any{}
	if e.GetComponent() != "" {
		ctx["component"] = e.GetComponent()
	}
	if e.GetTraceId() != "" {
		ctx["trace_id"] = e.GetTraceId()
	}
	if source != "" {
		ctx["source"] = source
	}
	if e.GetId() > 0 {
		ctx["local_seq"] = e.GetId()
	}
	if raw := e.GetFieldsJson(); len(raw) > 0 {
		var extra map[string]any
		if err := json.Unmarshal(raw, &extra); err == nil {
			for k, v := range extra {
				if _, taken := ctx[k]; !taken {
					ctx[k] = v
				}
			}
		} else {
			ctx["fields_raw"] = string(raw)
		}
	}

	recorded := msToRFC3339(e.GetTsMs())
	if recorded == "" {
		recorded = time.Now().UTC().Format(time.RFC3339)
	}

	return cpapi.LogEntry{
		Level:      normalizeLogLevel(e.GetLevel()),
		Message:    e.GetMsg(),
		Context:    ctx,
		RecordedAt: recorded,
	}
}

// 控制面对 level 的枚举是 debug|info|notice|warning|error|critical|diagnostic，
// agent 侧习惯用 warn —— 这里做一次归一，否则会被 Laravel 归为 info。
func normalizeLogLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "warn":
		return "warning"
	case "fatal", "panic":
		return "critical"
	case "debug", "info", "notice", "warning", "error", "critical", "diagnostic":
		return strings.ToLower(strings.TrimSpace(level))
	default:
		return "info"
	}
}

// clampPercent 把百分比压到 [0,100]：多核机器上 cpu_percent 可能 >100，
// 而控制面按 0..100 校验，不压会把整批指标 422 掉。
func clampPercent(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}

	return float64(int(v*100)) / 100
}

func toInt64(v uint64) int64 {
	if v > uint64(1<<62) {
		return int64(1 << 62)
	}

	return int64(v)
}

func msToRFC3339(ms int64) string {
	if ms <= 0 {
		return ""
	}

	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}

	return "false"
}
