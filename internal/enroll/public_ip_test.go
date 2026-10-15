package enroll

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getthelostcode/hysteria-agent/internal/config"
)

func TestDetectPublicIPReadsTheFirstHealthyEndpoint(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("203.0.113.42\n"))
	}))
	defer good.Close()

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	cfg := config.Default()
	cfg.Diag.PublicIPURLs = []string{bad.URL, good.URL} // 第一个坏掉，第二个能回答

	if got := detectPublicIP(context.Background(), cfg, nil); got != "203.0.113.42" {
		t.Fatalf("应取到第二个端点的地址，实际 %q", got)
	}
}

func TestDetectPublicIPRejectsNonPublicAndGarbageAnswers(t *testing.T) {
	cases := map[string]string{
		"私网地址": "192.168.1.1",
		"回环地址": "127.0.0.1",
		"非 IP": "not-an-ip",
		"空响应":  "",
	}

	for name, body := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))

		cfg := config.Default()
		cfg.Diag.PublicIPURLs = []string{srv.URL}

		if got := detectPublicIP(context.Background(), cfg, nil); got != "" {
			t.Fatalf("%s 不应被当作公网地址，实际 %q", name, got)
		}
		srv.Close()
	}
}

func TestDetectPublicIPIsSkippedWithoutConfiguredURLs(t *testing.T) {
	cfg := config.Default()
	cfg.Diag.PublicIPURLs = nil

	// 未配置时不发任何请求：注册是交互式操作，不能被 IP 查询拖住
	if got := detectPublicIP(context.Background(), cfg, nil); got != "" {
		t.Fatalf("未配置 public_ip_urls 时应直接跳过，实际 %q", got)
	}
}

// 注册请求必须带上 public_ip：控制面在 NAT/隧道后只能看到对端代理的地址。
func TestEnrollRequestCarriesTheDetectedPublicIP(t *testing.T) {
	ipSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("198.51.100.7"))
	}))
	defer ipSrv.Close()

	var body map[string]any

	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		// 密钥要够长：agent 会拒绝过短的 secret（这是个有用的防御，测试要按真实契约来）
		secret := "nds_" + strings.Repeat("s", 44)
		_, _ = w.Write([]byte(`{"node_id":"nd_test","node_secret":"` + secret + `"}`))
	}))
	defer cp.Close()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.Path = dir + "/config.yaml"
	cfg.Control.ServerURL = cp.URL
	cfg.Control.AllowInsecureWS = true
	cfg.Diag.PublicIPURLs = []string{ipSrv.URL}
	cfg.CredentialsFile = dir + "/credentials.json"
	cfg.StateDir = dir

	_, err := Run(context.Background(), cfg, Options{
		Token: "enr_" + strings.Repeat("a", 40),
	}, func(string, ...any) {})
	if err != nil {
		t.Fatalf("注册应成功: %v", err)
	}

	if body["public_ip"] != "198.51.100.7" {
		t.Fatalf("注册请求应带 public_ip，实际 body=%v", body["public_ip"])
	}
	if body["enroll_token"] == nil || body["protocol"] != "v1" {
		t.Fatalf("注册请求的既有字段不应被破坏: %v", body)
	}
}
