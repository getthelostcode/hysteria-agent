package cpapi

import (
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 跨语言黄金向量 ----
//
// 期望值由 Laravel 侧独立算出（php -r 'hash_hmac(...)'，见 BRIDGE doc §3），
// 硬编码在这里，任何一侧改了签名规范都会立刻红。
func TestSignMatchesLaravelGoldenVector(t *testing.T) {
	const (
		secret  = "nds_golden_vector_secret"
		method  = "POST"
		path    = "/api/agent/v1/heartbeat"
		body    = `{"version":"2.6.0"}`
		ts      = "1700000000"
		nonce   = "11111111-2222-3333-4444-555555555555"
		wantSig = "2dc895ed329b4bb0d1b3a1f1900da595136b330cafcb941602d78a48756b44cb"
	)

	canonical := CanonicalString(method, path, []byte(body), ts, nonce)
	wantCanonical := "POST\n/api/agent/v1/heartbeat\n" +
		"e5ddaa56290d976f15390d323b9d513627070cf8935041e08542abf49a8d312e\n" +
		"1700000000\n11111111-2222-3333-4444-555555555555"

	if canonical != wantCanonical {
		t.Fatalf("签名原文不一致：\n got=%q\nwant=%q", canonical, wantCanonical)
	}

	if got := Sign(secret, method, path, []byte(body), ts, nonce); got != wantSig {
		t.Fatalf("签名与 Laravel 不一致：got=%s want=%s", got, wantSig)
	}
}

func TestNonceIsUniquePerCall(t *testing.T) {
	seen := map[string]struct{}{}
	signer := NewSigner("s")

	for i := 0; i < 200; i++ {
		h := signer.Headers(http.MethodPost, "/x", nil)
		nonce := h[HeaderNonce]
		if nonce == "" {
			t.Fatal("nonce 为空")
		}
		if _, dup := seen[nonce]; dup {
			t.Fatalf("nonce 重复: %s", nonce)
		}
		seen[nonce] = struct{}{}
	}
}

// ---- 服务端视角校验（模拟 Laravel SignatureVerifier） ----

type verifyingServer struct {
	secret string
	mu     sync.Mutex
	calls  []recordedCall
}

type recordedCall struct {
	method, path, nonce, timestamp, signature string
	body                                      []byte
	headers                                   http.Header
}

func (vs *verifyingServer) snapshot() []recordedCall {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	out := make([]recordedCall, len(vs.calls))
	copy(out, vs.calls)

	return out
}

// verify 复刻 Laravel 的校验：path 不含查询串、签名针对原始（可能已压缩的）字节。
func (vs *verifyingServer) verify(t *testing.T, r *http.Request) ([]byte, bool) {
	t.Helper()

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("读 body 失败: %v", err)
	}

	if strings.Contains(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("gzip 解包失败: %v", err)
		}
		if _, err := io.ReadAll(zr); err != nil {
			t.Fatalf("gzip 读取失败: %v", err)
		}
	}

	ts, nonce := r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderNonce)
	canonical := strings.Join([]string{
		r.Method,
		r.URL.Path,
		sha256Hex(raw),
		ts,
		nonce,
	}, "\n")

	mac := hmac.New(sha256.New, []byte(vs.secret))
	mac.Write([]byte(canonical))
	want := hex.EncodeToString(mac.Sum(nil))

	vs.mu.Lock()
	vs.calls = append(vs.calls, recordedCall{
		method: r.Method, path: r.URL.Path, nonce: nonce, timestamp: ts,
		signature: r.Header.Get(HeaderSignature), body: raw, headers: r.Header.Clone(),
	})
	vs.mu.Unlock()

	if r.Header.Get(HeaderNodeID) == "" {
		t.Error("缺少 X-Node-Id")
	}

	return raw, hmac.Equal([]byte(want), []byte(r.Header.Get(HeaderSignature)))
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newTestClient(t *testing.T, baseURL, secret string) *Client {
	t.Helper()

	client, err := New(Config{
		BaseURL: baseURL,
		NodeID:  "nd_test",
		Secret:  secret,
		Retry:   RetryPolicy{Attempts: 1, Initial: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2},
		Gzip:    true,
	})
	if err != nil {
		t.Fatalf("构造客户端失败: %v", err)
	}

	return client
}

func TestHeartbeatIsSignedAndDecoded(t *testing.T) {
	const secret = "nds_unit_secret"
	vs := &verifyingServer{secret: secret}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/v1/heartbeat" {
			t.Errorf("路径错误: %s", r.URL.Path)
		}
		raw, ok := vs.verify(t, r)
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad_signature"}`))
			return
		}

		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if body["agent_version"] != "0.1.0" {
			t.Errorf("body 未正确序列化: %s", string(raw))
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"node_id":"nd_test","server_time":"2026-10-15T10:00:00+00:00","heartbeat_interval":30,"config":{"version":3,"sha256":"abc","pending":true},"commands":{"pending":2}}`))
	}))
	defer srv.Close()

	resp, err := newTestClient(t, srv.URL, secret).Heartbeat(context.Background(), HeartbeatRequest{
		AgentVersion: "0.1.0",
		Version:      "2.6.0",
	})
	if err != nil {
		t.Fatalf("心跳失败: %v", err)
	}

	if resp.HeartbeatInterval != 30 || resp.Config.Version != 3 || !resp.Config.Pending || resp.Commands.Pending != 2 {
		t.Fatalf("响应解析错误: %+v", resp)
	}
}

func TestGzipMetricsSignCompressedBytes(t *testing.T) {
	const secret = "nds_gzip_secret"
	vs := &verifyingServer{secret: secret}

	var sawGzip, sawSignature atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Content-Encoding"), "gzip") {
			sawGzip.Store(true)
		}
		raw, ok := vs.verify(t, r)
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		sawSignature.Store(true)

		// 服务端（Laravel）侧的 DecodeAgentPayload 之后拿到的是解压后的 JSON。
		zr, err := gzip.NewReader(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("解压失败: %v", err)
		}
		defer zr.Close()
		plain, _ := io.ReadAll(zr)
		if !strings.Contains(string(plain), `"samples"`) {
			t.Errorf("解压后 body 不含 samples: %s", string(plain))
		}

		_, _ = w.Write([]byte(`{"ok":true,"stored":2}`))
	}))
	defer srv.Close()

	cpu := 12.5
	resp, err := newTestClient(t, srv.URL, secret).Metrics(context.Background(), []MetricSample{
		{CPU: &cpu, RecordedAt: "2026-10-15T10:00:00Z"},
		{CPU: &cpu, RecordedAt: "2026-10-15T10:00:15Z"},
	})
	if err != nil {
		t.Fatalf("指标上报失败: %v", err)
	}

	if !sawGzip.Load() || !sawSignature.Load() {
		t.Fatal("指标上报既未压缩也未通过签名校验")
	}
	if resp.Stored != 2 {
		t.Fatalf("stored=%d want=2", resp.Stored)
	}
}

func TestQueryStringIsNotSigned(t *testing.T) {
	const secret = "nds_query_secret"
	vs := &verifyingServer{secret: secret}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "" {
			t.Error("应当带上查询串")
		}
		// verify() 用 r.URL.Path（不含查询串）重算签名
		if _, ok := vs.verify(t, r); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		_, _ = w.Write([]byte(`{"ok":true,"up_to_date":false,"config":{"version":4,"sha256":"deadbeef","content_yaml":"listen: :443\n"}}`))
	}))
	defer srv.Close()

	resp, err := newTestClient(t, srv.URL, secret).ConfigLatest(context.Background(), 3)
	if err != nil {
		t.Fatalf("拉取配置失败: %v", err)
	}

	if resp.UpToDate || resp.Config == nil || resp.Config.Version != 4 || resp.Config.ContentYAML != "listen: :443\n" {
		t.Fatalf("响应解析错误: %+v", resp)
	}
}

func TestCommandsPollAndAckRoundTrip(t *testing.T) {
	const secret = "nds_cmd_secret"
	vs := &verifyingServer{secret: secret}

	var ackPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := vs.verify(t, r); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch {
		case r.URL.Path == "/api/agent/v1/commands/poll":
			_, _ = w.Write([]byte(`{"ok":true,"server_time":"t","commands":[{"command_id":"11111111-1111-1111-1111-111111111111","type":"tail_logs","payload":{"duration_seconds":60},"issued_at":"t","expires_at":"t"}]}`))
		case strings.HasSuffix(r.URL.Path, "/ack"):
			ackPath = r.URL.Path
			_, _ = w.Write([]byte(`{"ok":true,"command_id":"11111111-1111-1111-1111-111111111111","status":"acked"}`))
		default:
			t.Errorf("意外路径: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, secret)

	polled, err := client.CommandsPoll(context.Background(), 10)
	if err != nil {
		t.Fatalf("拉取指令失败: %v", err)
	}
	if len(polled.Commands) != 1 || polled.Commands[0].Type != "tail_logs" {
		t.Fatalf("指令解析错误: %+v", polled.Commands)
	}

	cmd := polled.Commands[0]
	acked, err := client.CommandAck(context.Background(), cmd.CommandID, "ok", map[string]any{"exit_code": 0}, "")
	if err != nil {
		t.Fatalf("回执失败: %v", err)
	}
	if acked.Status != "acked" {
		t.Fatalf("回执状态错误: %+v", acked)
	}
	if ackPath != "/api/agent/v1/commands/"+cmd.CommandID+"/ack" {
		t.Fatalf("回执路径错误: %s", ackPath)
	}
}

func TestTypedErrors(t *testing.T) {
	const secret = "nds_err_secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agent/v1/heartbeat":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad_signature","message":"Invalid signature or stale timestamp."}`))
		case "/api/agent/v1/config/latest":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unknown_node","message":"Unknown node."}`))
		default:
			w.WriteHeader(http.StatusTeapot)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, secret)

	_, err := client.Heartbeat(context.Background(), HeartbeatRequest{})
	if err == nil || !IsUnauthorized(err) {
		t.Fatalf("401 应被识别为认证失败: %v", err)
	}

	var apiErr *APIError
	if !errorsAs(err, &apiErr) || apiErr.Code != "bad_signature" {
		t.Fatalf("错误类型解析失败: %#v", apiErr)
	}

	_, err = client.ConfigLatest(context.Background(), 1)
	if err == nil || !IsUnknownNode(err) || IsUnauthorized(err) {
		t.Fatalf("404 unknown_node 应被识别为节点已消失: %v", err)
	}
}

// errorsAs 避免测试里重复 import errors 造成的命名冲突。
func errorsAs(err error, target **APIError) bool {
	e, ok := err.(*APIError)
	if !ok {
		return false
	}
	*target = e

	return true
}

func TestRetriesOn5xxWithFreshNonce(t *testing.T) {
	const secret = "nds_retry_secret"
	vs := &verifyingServer{secret: secret}
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := vs.verify(t, r); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"vendor_busy"}`))

			return
		}

		_, _ = w.Write([]byte(`{"ok":true,"stored":1}`))
	}))
	defer srv.Close()

	client, err := New(Config{
		BaseURL: srv.URL, NodeID: "nd_test", Secret: secret,
		Retry: RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2, Jitter: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Logs(context.Background(), []LogEntry{{Level: "info", Message: "x"}}); err != nil {
		t.Fatalf("重试后应当成功: %v", err)
	}

	calls := vs.snapshot()
	if len(calls) != 2 {
		t.Fatalf("期望 2 次尝试，实际 %d 次", len(calls))
	}
	if calls[0].nonce == calls[1].nonce {
		t.Fatal("重试必须使用新的 nonce，否则会被判定为重放")
	}
	if calls[0].timestamp == "" || calls[1].timestamp == "" {
		t.Fatal("缺少时间戳")
	}
}

func TestEnrollPassthroughUsesHeaderAndParsesContract(t *testing.T) {
	var gotHeader, gotBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/enroll" {
			t.Errorf("注册路径错误: %s", r.URL.Path)
		}
		if r.Header.Get(HeaderSignature) != "" {
			t.Error("注册不应带签名头")
		}
		gotHeader = r.Header.Get(HeaderEnrollTok)

		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)

		_, _ = w.Write([]byte(`{"node_id":"nd_abc","node_secret":"nds_xyz","node_name":"hk-edge-1","ws_path":"/api/v1/agent/control","config_revision":"0","heartbeat_interval_seconds":30,"pin_sha256":[],"jwt_issuer":"Hysteria Control Plane"}`))
	}))
	defer srv.Close()

	resp, err := Enroll(context.Background(), Config{UserAgent: "hysteria-controlplane-bridge/1.0"}, srv.URL, EnrollRequest{
		EnrollToken:  "enr_test",
		Hostname:     "hk-edge-1",
		OS:           "linux",
		Arch:         "amd64",
		AgentVersion: "0.1.0",
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	if gotHeader != "enr_test" {
		t.Fatalf("X-Enroll-Token 未发送: %q", gotHeader)
	}
	if !strings.Contains(gotBody, `"hostname":"hk-edge-1"`) || strings.Contains(gotBody, "replace") {
		t.Fatalf("注册 body 不符合契约: %s", gotBody)
	}
	if resp.NodeID != "nd_abc" || resp.NodeSecret != "nds_xyz" || resp.ConfigRevision != "0" || resp.HeartbeatIntervalSeconds != 30 {
		t.Fatalf("注册响应解析错误: %+v", resp)
	}
}

func TestEnrollSurfaces409WithNodeID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"node_exists","message":"该节点已注册","node_id":"nd_existing"}`))
	}))
	defer srv.Close()

	_, err := Enroll(context.Background(), Config{}, srv.URL, EnrollRequest{EnrollToken: "enr_x", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0"})
	if err == nil {
		t.Fatal("409 应当返回错误")
	}

	var apiErr *APIError
	if !errorsAs(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != "node_exists" {
		t.Fatalf("409 语义错误: %#v", apiErr)
	}
	if !strings.Contains(apiErr.Body, "nd_existing") {
		t.Fatalf("响应体应包含 node_id: %s", apiErr.Body)
	}
}

func TestContextCancellationStopsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client, err := New(Config{
		BaseURL: srv.URL, NodeID: "nd", Secret: "s",
		Retry: RetryPolicy{Attempts: 5, Initial: time.Hour, Multiplier: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := client.Heartbeat(ctx, HeartbeatRequest{}); err == nil {
		t.Fatal("应当返回错误")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("取消后不应继续等待退避: %s", elapsed)
	}
}
