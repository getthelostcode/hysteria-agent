package cp

import (
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/cpapi"
)

// fakeLaravelBridge 模拟 Laravel 的 Agent API：校验 HMAC 签名并记录收到的 body。
type fakeLaravelBridge struct {
	mu            sync.Mutex
	requests      []bridgeRequest
	secret        string
	status        int // 非 0 时对所有请求返回该状态
	statusOnce    int // 只对第一个请求返回该状态
	sleepPerReq   time.Duration
	badSignatures int
}

type bridgeRequest struct {
	path string
	body []byte
	gzip bool
	ok   bool
}

func (f *fakeLaravelBridge) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		ts, nonce := r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce")
		nodeID := r.Header.Get("X-Node-Id")
		mac := hmac.New(sha256.New, []byte(f.secret))
		mac.Write([]byte(cpapi.CanonicalString(r.Method, r.URL.Path, raw, ts, nonce)))
		want := hex.EncodeToString(mac.Sum(nil))
		valid := nodeID != "" && hmac.Equal([]byte(want), []byte(r.Header.Get("X-Node-Signature")))

		f.mu.Lock()
		f.requests = append(f.requests, bridgeRequest{
			path: r.URL.Path, body: raw,
			gzip: strings.Contains(r.Header.Get("Content-Encoding"), "gzip"),
			ok:   valid,
		})
		if !valid {
			f.badSignatures++
		}
		status := f.status
		if f.statusOnce != 0 && len(f.requests) == 1 {
			status = f.statusOnce
		}
		sleep := f.sleepPerReq
		f.mu.Unlock()

		if sleep > 0 {
			time.Sleep(sleep)
		}

		if status >= 400 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"upstream_failed"}`))

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"stored":1}`))
	})
}

func (f *fakeLaravelBridge) snapshot() []bridgeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]bridgeRequest, len(f.requests))
	copy(out, f.requests)

	return out
}

func (f *fakeLaravelBridge) badSigs() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.badSignatures
}

func (f *fakeLaravelBridge) waitFor(t *testing.T, n int, timeout time.Duration) []bridgeRequest {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := f.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}

	return f.snapshot()
}

func newBridgeServer(t *testing.T, upstream string, nodeID, secret string) (*Server, *Store) {
	t.Helper()

	store := newTestStore(t)
	if _, err := store.MirrorExternalNode(EnrollRequest{NodeName: "bridge-node", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0", HysteriaVersion: "2.6.0"},
		nodeID, secret, AuthorityLaravel, "laravel"); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.AdminToken = "adm"
	cfg.Identity = IdentityConfig{Authority: AuthorityLaravel}
	cfg.Laravel = LaravelConfig{BaseURL: upstream, EnrollPath: "/api/v1/agent/enroll", RequestTimeoutSeconds: 5,
		Retry: RetryConfig{InitialSeconds: 1, MaxSeconds: 2, Multiplier: 2}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	return New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil))), store
}

func TestBridgeForwardsHeartbeatMetricsAndLogs(t *testing.T) {
	up := &fakeLaravelBridge{secret: "nds_bridge_secret"}
	ts := httptest.NewServer(up.handler())
	defer ts.Close()

	srv, store := newBridgeServer(t, ts.URL, "nd_bridge_1", up.secret)
	_ = store

	now := time.Now().UnixMilli()
	srv.bridgeHeartbeat("nd_bridge_1", &agentpb.Heartbeat{
		TsMs: now, UptimeSeconds: 42, PendingEvents: 2, HysteriaRunning: true, Ready: true,
		ConfigRevision: "3", LastAckedSeq: 7, SentEvents: 9,
	})
	srv.bridgeMetrics("nd_bridge_1", &agentpb.MetricBatch{
		Sample: &agentpb.MetricSample{
			TsMs: now, CpuPercent: 250, MemPercent: 30.5, DiskPercent: 44,
			MemTotalBytes: 8 << 30, MemUsedBytes: 2 << 30, Load1: 1.5, ProcCount: 321,
		},
		Hysteria: &agentpb.HysteriaStats{
			Running: true, Pid: 99, UptimeSeconds: 3600, Version: "2.6.0",
			ActiveUsers: 3, OnlineDevices: 5, RxBytes: 1024, TxBytes: 2048, Conns: 7, RestartCount: 1,
		},
		AgentVersion:   "0.1.0",
		ConfigRevision: "3",
		Labels:         map[string]string{"region": "hk"},
	})
	srv.bridgeLogs("nd_bridge_1", &agentpb.LogBatch{
		Source: "agent",
		Entries: []*agentpb.LogEntry{{
			TsMs: now, Level: "warn", Component: "control", Msg: "重连中", TraceId: "trace-1",
			FieldsJson: []byte(`{"attempt":3}`), Id: 11,
		}},
	})

	got := up.waitFor(t, 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("期望 3 个转发请求，实际 %d", len(got))
	}
	if up.badSigs() != 0 {
		t.Fatalf("有 %d 个请求签名不可验证（与 Laravel 规范不一致）", up.badSigs())
	}

	byPath := map[string]bridgeRequest{}
	for _, r := range got {
		byPath[r.path] = r
	}

	// 心跳
	hb, ok := byPath["/api/agent/v1/heartbeat"]
	if !ok {
		t.Fatalf("未转发心跳: %v", byPath)
	}
	var hbBody struct {
		AgentVersion string            `json:"agent_version"`
		Uptime       int64             `json:"uptime"`
		Meta         map[string]string `json:"meta"`
	}
	_ = json.Unmarshal(hb.body, &hbBody)
	if hbBody.Meta["hysteria_running"] != "true" || hbBody.Meta["ready"] != "true" ||
		hbBody.Meta["config_revision"] != "3" || hbBody.Meta["pending_events"] != "2" {
		t.Fatalf("心跳 meta 映射错误: %+v", hbBody.Meta)
	}
	if hbBody.Uptime != 42 || hbBody.AgentVersion != "0.1.0" {
		t.Fatalf("心跳字段映射错误: %+v", hbBody)
	}

	// 指标：gzip + cpu 被压到 100
	mt, ok := byPath["/api/agent/v1/metrics"]
	if !ok {
		t.Fatalf("未转发指标")
	}
	if !mt.gzip {
		t.Fatal("指标应使用 gzip（控制面侧 AGENT_MAX_PAYLOAD_BYTES/带宽考虑）")
	}
	plain := gunzip(t, mt.body)
	var mtBody struct {
		Samples []struct {
			CPU        float64        `json:"cpu"`
			Mem        float64        `json:"mem"`
			Disk       float64        `json:"disk"`
			NetRx      int64          `json:"net_rx"`
			NetTx      int64          `json:"net_tx"`
			ConnCount  int64          `json:"conn_count"`
			Users      int64          `json:"users"`
			RecordedAt string         `json:"recorded_at"`
			Extra      map[string]any `json:"extra"`
		} `json:"samples"`
	}
	if err := json.Unmarshal(plain, &mtBody); err != nil {
		t.Fatalf("指标 body 解析失败: %v (%s)", err, string(plain))
	}
	if len(mtBody.Samples) != 1 {
		t.Fatalf("应有 1 条样本，实际 %d", len(mtBody.Samples))
	}
	s := mtBody.Samples[0]
	if s.CPU != 100 {
		t.Fatalf("cpu 应被压到 100（否则控制面 422 掉整批），实际 %v", s.CPU)
	}
	if s.Mem != 30.5 || s.Disk != 44 || s.NetRx != 1024 || s.NetTx != 2048 || s.ConnCount != 7 || s.Users != 3 {
		t.Fatalf("指标字段映射错误: %+v", s)
	}
	if s.RecordedAt == "" || !strings.Contains(s.RecordedAt, "T") {
		t.Fatalf("recorded_at 应是 RFC3339，实际 %q", s.RecordedAt)
	}
	hy, _ := s.Extra["hysteria"].(map[string]any)
	if hy == nil || hy["pid"].(float64) != 99 {
		t.Fatalf("extra.hysteria 应带 pid: %+v", s.Extra)
	}
	if s.Extra["load1"].(float64) != 1.5 {
		t.Fatalf("extra 应带 load1: %+v", s.Extra)
	}

	// 日志：warn -> warning，context 带 component/trace_id/fields
	lg, ok := byPath["/api/agent/v1/logs"]
	if !ok {
		t.Fatalf("未转发日志")
	}
	var lgBody struct {
		Entries []struct {
			Level      string         `json:"level"`
			Message    string         `json:"message"`
			Context    map[string]any `json:"context"`
			RecordedAt string         `json:"recorded_at"`
		} `json:"entries"`
	}
	_ = json.Unmarshal(lg.body, &lgBody)
	if len(lgBody.Entries) != 1 {
		t.Fatalf("日志条数错误: %+v", lgBody)
	}
	e := lgBody.Entries[0]
	if e.Level != "warning" {
		t.Fatalf("agent 的 warn 应归一为 warning（否则控制面当 info），实际 %q", e.Level)
	}
	if e.Context["component"] != "control" || e.Context["trace_id"] != "trace-1" ||
		e.Context["source"] != "agent" || e.Context["attempt"].(float64) != 3 {
		t.Fatalf("日志 context 映射错误: %+v", e.Context)
	}

	// 计数器
	if srv.metrics.BridgeHeartbeats != 1 || srv.metrics.BridgeMetrics != 1 || srv.metrics.BridgeLogs != 1 {
		t.Fatalf("桥接计数错误: hb=%d metrics=%d logs=%d",
			srv.metrics.BridgeHeartbeats, srv.metrics.BridgeMetrics, srv.metrics.BridgeLogs)
	}
}

func TestBridgeSplitsLargeLogBatch(t *testing.T) {
	up := &fakeLaravelBridge{secret: "nds_split_secret"}
	ts := httptest.NewServer(up.handler())
	defer ts.Close()

	srv, _ := newBridgeServer(t, ts.URL, "nd_bridge_split", up.secret)

	entries := make([]*agentpb.LogEntry, 1200)
	for i := range entries {
		entries[i] = &agentpb.LogEntry{TsMs: time.Now().UnixMilli(), Level: "info", Msg: "line"}
	}
	srv.bridgeLogs("nd_bridge_split", &agentpb.LogBatch{Source: "hysteria", Entries: entries})

	got := up.waitFor(t, 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("1200 条应按 500 切成 3 批，实际 %d", len(got))
	}

	sizes := make([]int, 0, len(got))
	for _, r := range got {
		var body struct {
			Entries []json.RawMessage `json:"entries"`
		}
		_ = json.Unmarshal(r.body, &body)
		sizes = append(sizes, len(body.Entries))
	}
	if sizes[0] != 500 || sizes[1] != 500 || sizes[2] != 200 {
		t.Fatalf("切分尺寸错误: %v", sizes)
	}
}

func TestBridgeIsInactiveInLocalMode(t *testing.T) {
	up := &fakeLaravelBridge{secret: "unused"}
	ts := httptest.NewServer(up.handler())
	defer ts.Close()

	store := newTestStore(t)
	cfg := DefaultConfig() // authority=local
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if srv.bridgeForwarder("any") != nil {
		t.Fatal("local 模式不应建立桥接转发器")
	}

	srv.bridgeHeartbeat("any", &agentpb.Heartbeat{TsMs: time.Now().UnixMilli()})
	srv.bridgeMetrics("any", &agentpb.MetricBatch{Sample: &agentpb.MetricSample{}})
	srv.bridgeLogs("any", &agentpb.LogBatch{Entries: []*agentpb.LogEntry{{Msg: "x"}}})

	time.Sleep(150 * time.Millisecond)
	if n := len(up.snapshot()); n != 0 {
		t.Fatalf("local 模式不应向 Laravel 发任何请求，实际 %d", n)
	}
}

func TestBridgeDropsOnQueueOverflowWithoutBlocking(t *testing.T) {
	srv, _ := newBridgeServer(t, "http://127.0.0.1:1", "nd_bridge_full", "nds_full_secret")

	start := time.Now()
	for i := 0; i < 2000; i++ {
		srv.bridgeHeartbeat("nd_bridge_full", &agentpb.Heartbeat{TsMs: time.Now().UnixMilli()})
	}
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("消息处理路径被桥接阻塞了（耗时 %s）：Agent 路径必须永不阻塞", elapsed)
	}
	if srv.metrics.BridgeDropped == 0 {
		t.Fatal("队列应发生丢弃并计数（S7 才会换成落盘 outbox）")
	}
}

func TestBridgeWorkerSurvivesUpstreamErrors(t *testing.T) {
	up := &fakeLaravelBridge{secret: "nds_errbridge", status: http.StatusInternalServerError}
	ts := httptest.NewServer(up.handler())
	defer ts.Close()

	srv, _ := newBridgeServer(t, ts.URL, "nd_bridge_err", up.secret)

	// 上游持续 5xx：转发失败要计数，但 worker 不能死（cpapi 自身会按策略重试）。
	srv.bridgeHeartbeat("nd_bridge_err", &agentpb.Heartbeat{TsMs: time.Now().UnixMilli()})

	if got := up.waitFor(t, 1, 2*time.Second); len(got) == 0 {
		t.Fatal("第一个请求应到达上游")
	}

	deadline := time.Now().Add(1250 * time.Second)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && srv.metrics.BridgeFailed == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.metrics.BridgeFailed == 0 {
		t.Fatal("5xx 应计入 bridge_failed")
	}

	// 上游恢复：同一个 worker 必须继续把后续消息送出去。
	up.mu.Lock()
	up.status = 0
	up.mu.Unlock()

	srv.bridgeHeartbeat("nd_bridge_err", &agentpb.Heartbeat{TsMs: time.Now().UnixMilli()})

	recovered := time.Now().Add(3 * time.Second)
	for time.Now().Before(recovered) && srv.metrics.BridgeHeartbeats == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.metrics.BridgeHeartbeats == 0 {
		t.Fatal("上游恢复后 worker 必须继续工作（成功计数未增长）")
	}
}

func TestBridgeMappingEdgeCases(t *testing.T) {
	// 未知 level 归一为 info；空 ts 用当前时间；负数百分位压到 0
	e := logEntryToLaravel("hysteria", &agentpb.LogEntry{Level: "TRACE", Msg: "x"})
	if e.Level != "info" || e.RecordedAt == "" {
		t.Fatalf("未知 level 归一失败: %+v", e)
	}
	if lvl := normalizeLogLevel("fatal"); lvl != "critical" {
		t.Fatalf("fatal 应归一为 critical，实际 %q", lvl)
	}

	if clampPercent(-5) != 0 || clampPercent(100) != 100 || clampPercent(99.999) > 100 {
		t.Fatal("clampPercent 边界错误")
	}

	if msToRFC3339(0) != "" {
		t.Fatal("0 时间戳应返回空字符串，由调用方回退当前时间")
	}

	// 大 uint64 不应溢出成负数
	if v := toInt64(^uint64(0)); v <= 0 {
		t.Fatalf("大数转换应被夹紧为正数，实际 %d", v)
	}
}

func gunzip(t *testing.T, raw []byte) []byte {
	t.Helper()

	zr, err := gzip.NewReader(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("gzip 解包失败: %v", err)
	}
	defer zr.Close()

	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip 读取失败: %v", err)
	}

	return plain
}
