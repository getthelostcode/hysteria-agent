package control

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/outbox"
)

// blockHandler 模拟「配置应用卡住」：HandleConfigPush 要很久才返回（等 hysteria 就绪的重试）。
type blockHandler struct {
	block    time.Duration
	applied  atomic.Int64
	blocked  atomic.Int64
	blockErr chan struct{}
}

func (h *blockHandler) HandleHelloAck(context.Context, *agentpb.HelloAck) {}
func (h *blockHandler) HandleConfigPush(ctx context.Context, cp *agentpb.ConfigPush) {
	h.blocked.Add(1)
	select {
	case <-time.After(h.block):
	case <-ctx.Done():
	}
	h.applied.Add(1)
}
func (h *blockHandler) HandleCommand(context.Context, *agentpb.CommandRequest)    {}
func (h *blockHandler) HandleUpgradeOffer(context.Context, *agentpb.UpgradeOffer) {}

// 回归：配置应用慢/卡住时，心跳必须照常上报（真实现场：面板「最后心跳不更新」）。
func TestRESTFallbackHeartbeatIsIndependentOfConfigApply(t *testing.T) {
	var heartbeats atomic.Int64
	var lastBody atomic.Value // 最近一次心跳的正文（JSON）

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/agent/heartbeat":
			heartbeats.Add(1)
			if raw, err := io.ReadAll(r.Body); err == nil {
				lastBody.Store(string(raw))
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		case r.URL.Path == "/api/v1/agent/config/latest",
			r.URL.Path == "/api/v1/agent/config/default":
			// 一直说有新配置：逼 agent 反复去应用（也就是一直卡在 blockHandler 上）。
			// 两个端点都要给：守护进程默认来源是「配置版本文件库的默认版本」(/config/default)，
			// 只实现 latest 会让轮询根本跑不到（这个用例曾经因此假过）。
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true, "up_to_date": false,
				"config": map[string]any{
					"version": 7, "sha256": "deadbeef", "revision": "7",
					"content_yaml": "listen: :443\n",
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.Control.ServerURL = srv.URL
	cfg.Control.AllowInsecureWS = true
	cfg.Control.HeartbeatIntervalSeconds = 1 // 每秒一次，便于断言
	cfg.Control.RESTFallbackSeconds = 60
	// 节点现状要上报给面板照抄（写死路径/secret 会让 agent 整份拒收配置）
	hyCfg := filepath.Join(dir, "hysteria.yaml")
	if err := os.WriteFile(hyCfg, []byte("tls:\n  cert: /etc/hysteria/cert.pem\n  key: /etc/hysteria/key.pem\ntrafficStats:\n  listen: 127.0.0.1:9999\n  secret: node-side-secret\n"), 0o600); err != nil {
		t.Fatalf("write hysteria cfg: %v", err)
	}
	cfg.Hysteria.ConfigPath = hyCfg
	// agent 侧的值与配置文件刻意不同：上报必须是 agent 自己的（校验比的就是它）
	cfg.Hysteria.APISecret = "agent-side-secret"
	cfg.Hysteria.APIBaseURL = "http://127.0.0.1:8080"
	cfg.Control.ConfigPollSeconds = 1
	cfg.Control.ConfigApplyTimeoutSeconds = 600 // 应用永远不会超时返回
	cfg.StateDir = dir
	cfg.CredentialsFile = filepath.Join(dir, "creds.json")

	ob, err := outbox.Open(filepath.Join(dir, "outbox.log"), 1<<20, 1000, false, 100)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	defer func() { _ = ob.Close() }()

	cr := writeCreds(t, cfg.CredentialsFile, "nd_test_node")

	h := &blockHandler{block: time.Hour}
	m := metrics.New("nd_test_node")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	cli := New(cfg, cr, log, m, ob, logging.NewHub(16), h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		cli.runRESTFallback(ctx, errHandshakeStub{})
		close(done)
	}()

	// 等 4.5 秒：心跳至少应该发了 3 次，而配置应用一直卡在 blockHandler 里
	time.Sleep(4500 * time.Millisecond)

	got := heartbeats.Load()
	if got < 3 {
		t.Fatalf("配置应用卡住时心跳被饿死：4.5s 内只发了 %d 次心跳（应 ≥3）", got)
	}
	if h.blocked.Load() == 0 {
		t.Fatalf("配置轮询没有跑到（测试没覆盖到目标场景）")
	}

	// 心跳里必须带上「面板该照抄什么」：证书路径与 trafficStats（否则面板只能猜）
	raw, _ := lastBody.Load().(string)
	if raw == "" {
		t.Fatalf("没有捕获到心跳正文")
	}
	for _, want := range []string{
		"/etc/hysteria/cert.pem", "/etc/hysteria/key.pem", // 证书路径来自配置文件（校验看文件在不在）
		"agent-side-secret", "127.0.0.1:8080", // secret/listen 必须来自 agent 配置（校验比的就是它）
		"traffic_stats_secret",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("心跳正文缺少 %q：%s", want, raw)
		}
	}
	if strings.Contains(raw, "node-side-secret") {
		t.Fatalf("不得把配置文件里的 secret 当权威值上报（会让面板抄回错值）：%s", raw)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("取消后 runRESTFallback 没有返回")
	}
}

// writeCreds 落一份最小可用的凭据文件（creds.Load 要求文件存在）。
func writeCreds(t *testing.T, path, nodeID string) *creds.Credentials {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw, _ := json.Marshal(map[string]any{
		"node_id": nodeID, "node_secret": "nds_test_secret", "protocol": "v1",
	})
	if err := os.WriteFile(path, raw, 0o400); err != nil {
		t.Fatalf("write creds: %v", err)
	}
	cr, err := creds.Load(path)
	if err != nil {
		t.Fatalf("creds: %v", err)
	}

	return cr
}

type errHandshakeStub struct{}

func (errHandshakeStub) Error() string { return "握手失败 HTTP 501: 对端没有实现该端点" }

// 窗口到期后必须自己退出（否则永远不会回去重试控制通道）。
func TestRESTFallbackExitsAfterWindow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ok":true,"up_to_date":true}`)
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.Control.ServerURL = srv.URL
	cfg.Control.AllowInsecureWS = true
	cfg.Control.HeartbeatIntervalSeconds = 1
	cfg.Control.RESTFallbackSeconds = 2
	cfg.Control.ConfigPollSeconds = 1
	cfg.StateDir = dir
	cfg.CredentialsFile = filepath.Join(dir, "creds.json")

	ob, err := outbox.Open(filepath.Join(dir, "outbox.log"), 1<<20, 1000, false, 100)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	defer func() { _ = ob.Close() }()

	cr := writeCreds(t, cfg.CredentialsFile, "nd_test_node2")

	cli := New(cfg, cr, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("nd_test_node2"),
		ob, logging.NewHub(16), &blockHandler{block: time.Millisecond})

	done := make(chan struct{})
	go func() {
		cli.runRESTFallback(context.Background(), errHandshakeStub{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("窗口（2s）到期后没有退出")
	}
	if cli.RESTFallbackActive() {
		t.Fatalf("退出后 restFallback 仍为 true")
	}
	_ = os.Remove(filepath.Join(dir, "creds.json"))
}

// 取配置的端点必须按 config_source 走：default=文件库默认版本；留空/node=本节点发布的版本。
func TestFetchLatestConfigUsesConfiguredSource(t *testing.T) {
	var lastPath atomic.Value

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath.Store(r.URL.Path)
		// 必须真给一份配置：库端点回「空」时会触发「退回 node 来源」的兜底，那就测不出端点选择了
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "up_to_date": false,
			"config": map[string]any{"version": 3, "sha256": "s3", "revision": "3", "content_yaml": "listen: :443\n"},
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.Control.ServerURL = srv.URL
	cfg.Control.AllowInsecureWS = true
	cfg.CredentialsFile = filepath.Join(dir, "creds.json")

	writeCreds(t, cfg.CredentialsFile, "nd_source_test")
	cr, err := creds.Load(cfg.CredentialsFile)
	if err != nil {
		t.Fatalf("creds: %v", err)
	}

	if _, err := FetchLatestConfig(context.Background(), cfg, cr, 0, "", ""); err != nil {
		t.Fatalf("node 来源失败: %v", err)
	}
	if got, _ := lastPath.Load().(string); got != "/api/v1/agent/config/latest" {
		t.Fatalf("node 来源应打 /config/latest，实际 %s", got)
	}

	if _, err := FetchLatestConfig(context.Background(), cfg, cr, 0, "", ConfigSourceDefault); err != nil {
		t.Fatalf("default 来源失败: %v", err)
	}
	if got, _ := lastPath.Load().(string); got != "/api/v1/agent/config/default" {
		t.Fatalf("default 来源应打 /config/default，实际 %s", got)
	}
}

// 文件库里还没有默认版本 → 自动退回本节点发布的版本（否则「面板发布了配置、节点一无所获」）。
func TestFetchLatestConfigFallsBackToNodeSourceWhenLibraryIsEmpty(t *testing.T) {
	var paths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "up_to_date": false,
			"config": map[string]any{
				"version": 7, "sha256": "abc", "revision": "7", "content_yaml": "listen: :443\n",
			},
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.Control.ServerURL = srv.URL
	cfg.Control.AllowInsecureWS = true
	cfg.CredentialsFile = filepath.Join(dir, "creds.json")
	writeCreds(t, cfg.CredentialsFile, "nd_fallback_test")

	cr, err := creds.Load(cfg.CredentialsFile)
	if err != nil {
		t.Fatalf("creds: %v", err)
	}

	// 库为空（config: null）时控制面默认端点会这么回
	emptySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "up_to_date": true, "config": nil})
	}))
	defer emptySrv.Close()

	cfg.Control.RESTDefaultConfigPath = "/api/v1/agent/config/default"
	cfg.Control.RESTConfigPath = "/api/v1/agent/config/latest"

	// 1) 库端点空 + node 端点有内容 → 内容来自 node，且标记回退
	cfg.Control.ServerURL = srv.URL
	withBoth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/api/v1/agent/config/default" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "up_to_date": true, "config": nil})

			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "up_to_date": false,
			"config": map[string]any{"version": 7, "sha256": "abc", "revision": "7", "content_yaml": "listen: :443\n"},
		})
	}))
	defer withBoth.Close()
	cfg.Control.ServerURL = withBoth.URL

	out, err := FetchLatestConfig(context.Background(), cfg, cr, 0, "", ConfigSourceDefault)
	if err != nil {
		t.Fatalf("取配置失败: %v", err)
	}
	if !out.FellBackFromDefault || out.Source != ConfigSourceNode {
		t.Fatalf("应标记「退回 node 来源」，实际 %+v", out)
	}
	if out.Version != 7 || strings.TrimSpace(out.Content) == "" {
		t.Fatalf("退回后应拿到 node 的配置，实际 %+v", out)
	}
	if len(paths) < 2 || paths[0] != "/api/v1/agent/config/default" || paths[1] != "/api/v1/agent/config/latest" {
		t.Fatalf("应先问 default 再问 latest，实际 %v", paths)
	}
}
