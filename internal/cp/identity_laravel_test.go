package cp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/creds"
)

// fakeLaravel 模拟 Laravel 控制面的 enroll 兼容面（/api/v1/agent/enroll）。
type fakeLaravel struct {
	status    int
	nodeID    string
	secret    string
	body      string
	gotToken  string
	gotBody   map[string]any
	gotPath   string
	reachable bool
}

func (f *fakeLaravel) start(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.gotPath = r.URL.Path
		f.gotToken = r.Header.Get("X-Enroll-Token")
		_ = json.NewDecoder(r.Body).Decode(&f.gotBody)

		if f.status != 0 && f.status != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(f.status)
			_, _ = w.Write([]byte(f.body))

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node_id":"` + f.nodeID + `","node_secret":"` + f.secret +
			`","node_name":"hk-edge-1","ws_path":"/api/v1/agent/control","config_revision":"7",` +
			`"heartbeat_interval_seconds":25,"telemetry_interval_seconds":10,"jwt_issuer":"Hysteria Control Plane","jwt_audience":"hysteria-agent"}`))
	}))
}

func newLaravelModeServer(t *testing.T, upstreamURL string) (*Server, *Store, *Config) {
	t.Helper()

	store := newTestStore(t)
	cfg := DefaultConfig()
	cfg.AdminToken = "adm_test"
	cfg.PublicURL = "https://node.example.com"
	cfg.Identity = IdentityConfig{Authority: AuthorityLaravel}
	cfg.Laravel = LaravelConfig{
		BaseURL:               upstreamURL,
		EnrollPath:            "/api/v1/agent/enroll",
		RequestTimeoutSeconds: 3,
		Retry:                 RetryConfig{InitialSeconds: 1, MaxSeconds: 2, Multiplier: 2},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("配置校验失败: %v", err)
	}

	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

	return srv, store, cfg
}

func enrollThroughServer(t *testing.T, ts *httptest.Server, path string, req EnrollRequest) (int, string) {
	t.Helper()

	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Enroll-Token", req.EnrollToken)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(raw)
}

func TestLaravelAuthorityProxiesEnrollAndStoresSecret(t *testing.T) {
	up := &fakeLaravel{nodeID: "nd_laravel_1", secret: "nds_laravel_secret_value"}
	ts := up.start(t)
	defer ts.Close()

	srv, store, cfg := newLaravelModeServer(t, ts.URL)
	agent := httptest.NewServer(srv.Handler())
	defer agent.Close()

	status, body := enrollThroughServer(t, agent, cfg.EnrollPath, EnrollRequest{
		EnrollToken:  "enr_bridge_token",
		NodeName:     "hk-edge-1",
		Hostname:     "hk-edge-1",
		OS:           "linux",
		Arch:         "amd64",
		MachineID:    "machine-abc",
		AgentVersion: "0.1.0",
		Protocol:     "1",
	})

	if status != http.StatusOK {
		t.Fatalf("注册应成功，实际 %d: %s", status, body)
	}

	var er EnrollResponse
	if err := json.Unmarshal([]byte(body), &er); err != nil {
		t.Fatal(err)
	}

	// 身份来自 Laravel（不是本地派生），控制面地址/路径来自本控制面。
	if er.NodeID != "nd_laravel_1" || er.NodeSecret != "nds_laravel_secret_value" {
		t.Fatalf("身份未透传: %+v", er)
	}
	if er.WSPath != cfg.WSPath || er.ControlURL != cfg.PublicURL {
		t.Fatalf("控制通道信息应来自本控制面: ws=%s url=%s", er.WSPath, er.ControlURL)
	}
	if er.HeartbeatIntervalSeconds != 25 || er.TelemetryIntervalSeconds != 10 {
		t.Fatalf("心跳/遥测应沿用 Laravel 的协商值: %+v", er)
	}

	// 上游收到了令牌与 machine_id（Laravel 用 machine_id 判重，丢了就 409 失效）。
	if up.gotToken != "enr_bridge_token" {
		t.Fatalf("X-Enroll-Token 未转发: %q", up.gotToken)
	}
	if up.gotBody["machine_id"] != "machine-abc" {
		t.Fatalf("machine_id 未转发: %v", up.gotBody)
	}
	if up.gotPath != "/api/v1/agent/enroll" {
		t.Fatalf("上游路径错误: %s", up.gotPath)
	}

	// 本地镜像：secret 落库、来源标记为 laravel，且通道鉴权用它。
	node, ok := store.Node("nd_laravel_1")
	if !ok {
		t.Fatal("本地未镜像该节点")
	}
	if node.Secret != "nds_laravel_secret_value" || node.Authority != AuthorityLaravel {
		t.Fatalf("本地镜像不正确: %+v", node)
	}
	if got := store.NodeSecret("nd_laravel_1"); got != "nds_laravel_secret_value" {
		t.Fatalf("NodeSecret 应返回 Laravel 的密钥，实际 %q", got)
	}
	if store.NodeSecret("nd_laravel_1") == store.DeriveSecret("nd_laravel_1") {
		t.Fatal("桥接节点不应回退到主密钥派生")
	}
}

func TestLaravelAuthorityPassesThrough409WithNodeID(t *testing.T) {
	up := &fakeLaravel{
		status: http.StatusConflict,
		body:   `{"error":"node_exists","message":"该节点已注册","node_id":"nd_existing"}`,
	}
	ts := up.start(t)
	defer ts.Close()

	srv, store, cfg := newLaravelModeServer(t, ts.URL)
	agent := httptest.NewServer(srv.Handler())
	defer agent.Close()

	status, body := enrollThroughServer(t, agent, cfg.EnrollPath, EnrollRequest{
		EnrollToken: "enr_x", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0",
	})

	if status != http.StatusConflict {
		t.Fatalf("应透传 409，实际 %d: %s", status, body)
	}
	if !strings.Contains(body, "nd_existing") {
		t.Fatalf("应保留上游响应体里的 node_id: %s", body)
	}
	if _, ok := store.Node("nd_existing"); ok {
		t.Fatal("被拒绝的注册不应在本地建节点")
	}
}

func TestLaravelAuthorityReturnsBadGatewayWhenUpstreamDown(t *testing.T) {
	// 指向一个已关闭的端口
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	srv, store, cfg := newLaravelModeServer(t, deadURL)
	agent := httptest.NewServer(srv.Handler())
	defer agent.Close()

	status, body := enrollThroughServer(t, agent, cfg.EnrollPath, EnrollRequest{
		EnrollToken: "enr_x", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0",
	})

	if status != http.StatusBadGateway {
		t.Fatalf("Laravel 不可达应返回 502，实际 %d: %s", status, body)
	}
	if !strings.Contains(body, "Laravel") {
		t.Fatalf("错误信息应点明上游: %s", body)
	}
	if _, ok := store.Node("nd_laravel_1"); ok {
		t.Fatal("上游失败时不应留下节点")
	}
}

func TestLocalAuthorityStillDerivesSecrets(t *testing.T) {
	store := newTestStore(t)
	cfg := DefaultConfig()
	if cfg.Identity.Authority != AuthorityLocal {
		t.Fatalf("默认应为 local 模式，实际 %q", cfg.Identity.Authority)
	}
	if cfg.UsesLaravelIdentity() {
		t.Fatal("默认不应走 Laravel 身份权威")
	}

	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	agent := httptest.NewServer(srv.Handler())
	defer agent.Close()

	tok := store.NewToken("local", time.Hour, 1)
	status, body := enrollThroughServer(t, agent, cfg.EnrollPath, EnrollRequest{
		EnrollToken: tok.Token, NodeName: "local-node", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0",
	})
	if status != http.StatusOK {
		t.Fatalf("本地模式注册应成功: %d %s", status, body)
	}

	var er EnrollResponse
	if err := json.Unmarshal([]byte(body), &er); err != nil {
		t.Fatal(err)
	}
	if er.NodeSecret != store.DeriveSecret(er.NodeID) {
		t.Fatal("本地模式仍应使用主密钥派生密钥")
	}
}

// TestChannelAuthUsesLaravelSecret：agent 用 Laravel 下发的 node_secret 自签 JWT，
// 控制通道必须接受；用主密钥派生的密钥自签则必须被拒。
func TestChannelAuthUsesLaravelSecret(t *testing.T) {
	const laravelSecret = "nds_laravel_channel_secret"
	up := &fakeLaravel{nodeID: "nd_laravel_chan", secret: laravelSecret}
	ts := up.start(t)
	defer ts.Close()

	srv, store, cfg := newLaravelModeServer(t, ts.URL)
	agent := httptest.NewServer(srv.Handler())
	defer agent.Close()

	if status, body := enrollThroughServer(t, agent, cfg.EnrollPath, EnrollRequest{
		EnrollToken: "enr_chan", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0",
	}); status != http.StatusOK {
		t.Fatalf("注册失败: %d %s", status, body)
	}

	// 用一个普通 GET 触发鉴权分支（没有 Upgrade 头，升级会失败，但鉴权结论清晰）。
	probe := func(jwt string) int {
		req, _ := http.NewRequest(http.MethodGet, agent.URL+cfg.WSPath, nil)
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("X-Node-ID", "nd_laravel_chan")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		defer resp.Body.Close()

		return resp.StatusCode
	}

	good := signTestJWT(t, laravelSecret, "nd_laravel_chan")
	if code := probe(good); code == http.StatusUnauthorized {
		t.Fatal("用 Laravel 下发的密钥自签 JWT 应通过鉴权")
	}

	derived := signTestJWT(t, store.DeriveSecret("nd_laravel_chan"), "nd_laravel_chan")
	if code := probe(derived); code != http.StatusUnauthorized {
		t.Fatalf("用主密钥派生的密钥应被拒，实际 %d", code)
	}
}

func TestValidateRequiresBaseURLInLaravelMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Identity = IdentityConfig{Authority: AuthorityLaravel}
	cfg.Laravel = LaravelConfig{}

	if err := cfg.Validate(); err == nil {
		t.Fatal("laravel 模式缺 base_url 应当报错")
	}

	cfg.Laravel.BaseURL = "node.example.com" // 缺 scheme
	if err := cfg.Validate(); err == nil {
		t.Fatal("非绝对 URL 应当报错")
	}

	cfg.Laravel.BaseURL = "https://cp.example.com/"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("合法配置不应报错: %v", err)
	}
	if cfg.Bridge.MaxMetricBatch != 500 || cfg.Poll.CommandsSeconds != 10 {
		t.Fatalf("Normalize 未补齐默认值: %+v %+v", cfg.Bridge, cfg.Poll)
	}
	if !strings.HasSuffix(cfg.Bridge.OutboxDir, "/outbox") {
		t.Fatalf("outbox 目录应默认落在 state_dir 下: %s", cfg.Bridge.OutboxDir)
	}
}

func TestMirrorExternalNodeRoundTripSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	key, _ := NewMasterKey()
	store, err := OpenStore(dir, key)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.MirrorExternalNode(EnrollRequest{NodeName: "n1", Hostname: "h1", OS: "linux"},
		"nd_persist", "nds_persist_secret", AuthorityLaravel, "laravel"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.NodeSecret("nd_persist"); got != "nds_persist_secret" {
		t.Fatalf("重新打开后密钥丢失: %q", got)
	}
	if n, ok := reopened.Node("nd_persist"); !ok || n.Authority != AuthorityLaravel {
		t.Fatal("重新打开后身份来源信息丢失")
	}
}

// signTestJWT 构造 agent 侧等价的自签 JWT（HS256，键为 node_secret）。
func signTestJWT(t *testing.T, secret, nodeID string) string {
	t.Helper()

	enc := func(v any) string {
		b, _ := json.Marshal(v)

		return base64.RawURLEncoding.EncodeToString(b)
	}

	header := enc(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload := enc(map[string]any{
		"iss": "hysteria-agent",
		"aud": creds.DefaultAudience,
		"sub": nodeID,
		"jti": "test-jti",
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(header + "." + payload))

	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
