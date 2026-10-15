package creds

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
)

func TestSaveLoadAndPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	c := &Credentials{NodeID: "node_1", NodeSecret: "0123456789abcdef0123456789abcdef", NodeName: "test"}
	if err := c.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 需求：credentials 权限 0400
	if fi.Mode().Perm() != 0o400 {
		t.Fatalf("凭据权限应为 0400，得到 %v", fi.Mode().Perm())
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.NodeID != "node_1" || got.Issuer != DefaultIssuer || got.Audience != DefaultAudience {
		t.Fatalf("凭据内容/默认值不对: %+v", got)
	}
}

func TestLoadTightensLoosePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	raw, _ := json.Marshal(&Credentials{NodeID: "n", NodeSecret: "0123456789abcdef0123456789abcdef"})
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	fi, _ := os.Stat(path)
	if fi.Mode().Perm() != 0o400 {
		t.Fatalf("加载时应把权限收紧到 0400，得到 %v", fi.Mode().Perm())
	}
}

func TestLoadRejectsIncompleteCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	os.WriteFile(path, []byte(`{"node_id":"n"}`), 0o600)
	if _, err := Load(path); err == nil {
		t.Fatal("缺少 node_secret 必须报错")
	}
}

func TestJWTMintAndVerify(t *testing.T) {
	c := &Credentials{NodeID: "node_42", NodeSecret: "shared-secret-value", Issuer: DefaultIssuer, Audience: DefaultAudience}
	tok, exp, err := c.MintJWT(time.Hour)
	if err != nil {
		t.Fatalf("MintJWT: %v", err)
	}
	if time.Until(exp) < 50*time.Minute {
		t.Fatalf("过期时间不对: %v", exp)
	}
	claims, err := VerifyJWT(tok, c.NodeSecret)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}
	if claims["sub"] != "node_42" || claims["aud"] != DefaultAudience || claims["iss"] != DefaultIssuer {
		t.Fatalf("claims 异常: %v", claims)
	}
	if _, ok := claims["jti"].(string); !ok {
		t.Fatal("应带 jti 便于控制面防重放")
	}

	// 换密钥必须失败
	if _, err := VerifyJWT(tok, "another-secret"); err == nil {
		t.Fatal("错误密钥必须验签失败")
	}
	// 篡改 payload 必须失败
	parts := []byte(tok)
	if parts[10] == 'a' {
		parts[10] = 'b'
	} else {
		parts[10] = 'a'
	}
	if _, err := VerifyJWT(string(parts), c.NodeSecret); err == nil {
		t.Fatal("篡改后的 JWT 必须失败")
	}
}

func TestJWTExpired(t *testing.T) {
	// MintJWT 对 ttl<=0 会按防御性策略退化为 1 小时，所以这里手工构造一个已过期的令牌，
	// 直接验证 VerifyJWT 的过期分支（控制面对过期令牌必须拒绝）。
	c := &Credentials{NodeID: "n", NodeSecret: "s", Issuer: DefaultIssuer, Audience: DefaultAudience}
	hb, _ := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT"})
	cb, _ := json.Marshal(map[string]any{
		"iss": DefaultIssuer, "sub": "n", "aud": DefaultAudience,
		"exp": time.Now().Add(-time.Minute).Unix(),
	})
	signing := b64(hb) + "." + b64(cb)
	mac := hmac.New(sha256.New, []byte(c.NodeSecret))
	mac.Write([]byte(signing))
	tok := signing + "." + b64(mac.Sum(nil))
	if _, err := VerifyJWT(tok, c.NodeSecret); err == nil {
		t.Fatal("过期 JWT 必须失败")
	}
	if _, _, err := c.MintJWT(0); err != nil {
		t.Fatalf("ttl=0 应退化为默认值而不是报错: %v", err)
	}
}

func TestTLSConfigPinning(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	pin := SPKIPin(srv.Certificate())
	if len(pin) == 0 {
		t.Fatal("SPKI pin 不能为空")
	}

	cc := &config.Control{InsecureSkipVerify: true, PinSHA256: []string{pin}}
	cr := &Credentials{NodeID: "n", NodeSecret: "s"}
	tlsCfg, err := cr.TLSConfig(cc, nil)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("pin 正确时应连接成功: %v", err)
	}
	resp.Body.Close()

	// 错误的 pin 必须握手失败
	cc2 := &config.Control{InsecureSkipVerify: true, PinSHA256: []string{SPKIPin(&x509.Certificate{RawSubjectPublicKeyInfo: []byte("other")})}}
	tlsCfg2, err := cr.TLSConfig(cc2, nil)
	if err != nil {
		t.Fatal(err)
	}
	client2 := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg2}}
	if _, err := client2.Get(srv.URL); err == nil {
		t.Fatal("pin 不匹配必须握手失败")
	}
}

func TestTLSConfigRequiresPinWhenInsecure(t *testing.T) {
	cr := &Credentials{}
	if _, err := cr.TLSConfig(&config.Control{InsecureSkipVerify: true}, nil); err == nil {
		t.Fatal("insecure_skip_verify 且没有 pin 时必须报错（否则等于完全不校验）")
	}
}

func TestTLSConfigUsesProvidedCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	caPEM := pemEncodeCert(t, srv.Certificate())
	cr := &Credentials{CACertPEM: caPEM}
	cc := &config.Control{}
	tlsCfg, err := cr.TLSConfig(cc, nil)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	if tlsCfg.RootCAs == nil {
		t.Fatal("应使用下发的 CA")
	}
	if tlsCfg.MinVersion < tls.VersionTLS12 {
		t.Fatal("TLS 最低版本应 >= 1.2")
	}
	if _, err := x509.ParseCertificate(srv.Certificate().Raw); err != nil {
		t.Fatal(err)
	}
}
