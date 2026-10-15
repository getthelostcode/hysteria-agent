package hyconfig

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// genCert 生成一张自签证书，返回 cert/key 路径。
func genCert(t *testing.T, dir string, notBefore, notAfter time.Time, dns ...string) (string, string) {
	t.Helper()
	// 每次生成都放进独立子目录：否则同一目录里的 cert.pem/key.pem 会互相覆盖，
	// 测"证书与私钥不匹配"时就会拿到同一个文件对。
	dir, err := os.MkdirTemp(dir, "cert-*")
	if err != nil {
		t.Fatal(err)
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "test.example.com"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dns,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certOut, _ := os.Create(certPath)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certOut.Close()
	keyDer, _ := x509.MarshalECPrivateKey(priv)
	keyOut, _ := os.Create(keyPath)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})
	keyOut.Close()
	return certPath, keyPath
}

func baseConfig(cert, key string, port int) string {
	return fmt.Sprintf(`
listen: :%d
tls:
  cert: %s
  key: %s
auth:
  type: password
  password: supersecretpassword
masquerade:
  type: 404
trafficStats:
  listen: 127.0.0.1:18443
  secret: statssecret
`, port, cert, key)
}

func TestValidateAcceptsGoodConfig(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(90*24*time.Hour), "test.example.com")
	raw := []byte(baseConfig(cert, key, 44300))
	res := Validate(context.Background(), raw, "yaml", ValidateOptions{
		CheckCert: true, CertWarnDays: 14, ExpectedTrafficSecret: "statssecret",
	})
	if !res.Valid {
		t.Fatalf("应校验通过，错误: %v", res.Errors)
	}
	if res.Parsed.ListenPort != 44300 || res.Parsed.AuthType != "password" || res.Parsed.TLSMode != "tls" {
		t.Fatalf("解析结果异常: %+v", res.Parsed)
	}
	if res.SHA256 == "" {
		t.Fatal("应计算 sha256")
	}
	if !containsPrefix(res.Checks, "cert:expires_in") {
		t.Fatalf("checks 里应包含证书有效期: %v", res.Checks)
	}
}

func TestValidateRejectsMissingAndWrongFields(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(90*24*time.Hour))

	cases := []struct {
		name    string
		mutate  func(string) string
		wantErr string
	}{
		{"缺少 listen 时用默认 :443（告警不报错）", func(s string) string {
			return strings.Replace(s, "listen: :44300", "", 1)
		}, ""},
		{"auth 类型非法", func(s string) string {
			return strings.Replace(s, "type: password", "type: magic", 1)
		}, "auth.type"},
		{"acme 缺域名", func(s string) string {
			return strings.Replace(s, "trafficStats:", "acme:\n  email: a@b.c\ntrafficStats:", 1)
		}, "acme.domains"},
		{"trafficStats 头为空", func(s string) string {
			return strings.Replace(s, "secret: statssecret", "secret: ''", 1)
		}, "trafficStats.secret"},
		{"trafficStats secret 与 agent 不一致", func(s string) string {
			return s // 用 ExpectedTrafficSecret 不匹配触发
		}, "不一致"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.mutate(baseConfig(cert, key, 44300))
			opt := ValidateOptions{CheckCert: true, CertWarnDays: 14, ExpectedTrafficSecret: "statssecret"}
			if tc.wantErr == "不一致" {
				opt.ExpectedTrafficSecret = "different-secret"
			}
			res := Validate(context.Background(), []byte(body), "yaml", opt)
			if tc.wantErr == "" {
				if !res.Valid {
					t.Fatalf("应当校验通过，实际错误: %v", res.Errors)
				}
				if !strings.Contains(strings.Join(res.Warnings, ";"), "未显式配置 listen") {
					t.Fatalf("应告警 listen 缺省: %v", res.Warnings)
				}
				if res.Parsed.ListenPort != 443 {
					t.Fatalf("缺省 listen 应等价于 :443，得到 %d", res.Parsed.ListenPort)
				}
				return
			}
			if res.Valid {
				t.Fatalf("应当校验失败，实际通过（checks=%v）", res.Checks)
			}
			joined := strings.Join(res.Errors, "; ")
			if !strings.Contains(joined, tc.wantErr) {
				t.Fatalf("错误信息里应提到 %q，实际: %s", tc.wantErr, joined)
			}
		})
	}
}

func TestValidateWarnsOnUnknownKeyAndShortPassword(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(90*24*time.Hour))
	body := strings.Replace(baseConfig(cert, key, 44302), "masqueradee:", "masqueradee:", 1)
	body += "masqueradee:\n  type: proxy\n"
	body = strings.Replace(body, "password: supersecretpassword", "password: short", 1)
	res := Validate(context.Background(), []byte(body), "yaml", ValidateOptions{CheckCert: true, CertWarnDays: 14})
	if !res.Valid {
		t.Fatalf("未知顶层键与短密码只应是告警: %v", res.Errors)
	}
	joined := strings.Join(res.Warnings, "; ")
	if !strings.Contains(joined, "未知的顶层配置键") {
		t.Fatalf("应告警未知顶层键: %v", res.Warnings)
	}
	if !strings.Contains(joined, "auth.password") {
		t.Fatalf("应告警弱密码: %v", res.Warnings)
	}
}

func TestValidateCertExpiredAndMismatchedKey(t *testing.T) {
	dir := t.TempDir()
	expired, expiredKey := genCert(t, dir, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour))
	res := Validate(context.Background(), []byte(baseConfig(expired, expiredKey, 44303)), "yaml",
		ValidateOptions{CheckCert: true, CertWarnDays: 14})
	if res.Valid {
		t.Fatal("过期证书必须拒绝")
	}
	if !strings.Contains(strings.Join(res.Errors, ";"), "过期") {
		t.Fatalf("错误信息应说明过期: %v", res.Errors)
	}

	_, otherKey := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	goodCert, _ := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	res2 := Validate(context.Background(), []byte(baseConfig(goodCert, otherKey, 44304)), "yaml",
		ValidateOptions{CheckCert: true, CertWarnDays: 14})
	if res2.Valid {
		t.Fatal("证书与私钥不匹配必须拒绝")
	}
}

func TestValidateNearExpiryIsWarning(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(3*24*time.Hour))
	res := Validate(context.Background(), []byte(baseConfig(cert, key, 44305)), "yaml",
		ValidateOptions{CheckCert: true, CertWarnDays: 14})
	if !res.Valid {
		t.Fatalf("临期证书是告警不是错误: %v", res.Errors)
	}
	if !strings.Contains(strings.Join(res.Warnings, ";"), "过期") {
		t.Fatalf("应告警临期: %v", res.Warnings)
	}
}

func TestValidatePortOccupiedByOtherProcess(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(90*24*time.Hour))

	// 占用一个 UDP 端口，模拟"别的进程抢了 Hysteria 的端口"。
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("无法监听 UDP: %v", err)
	}
	defer conn.Close()
	port := conn.LocalAddr().(*net.UDPAddr).Port

	body := []byte(baseConfig(cert, key, port))
	res := Validate(context.Background(), body, "yaml", ValidateOptions{
		CheckPort: true, CheckCert: true, CertWarnDays: 14,
	})
	if res.Valid {
		t.Fatalf("端口被别的进程占用时必须拒绝（checks=%v）", res.Checks)
	}

	// 同一个端口，但声明占用者就是"我们的 hysteria 进程"（热更新场景）→ 应当通过。
	res2 := Validate(context.Background(), body, "yaml", ValidateOptions{
		CheckPort: true, CheckCert: true, CertWarnDays: 14, OwnerPID: os.Getpid(),
	})
	if !res2.Valid {
		t.Fatalf("端口被自己持有时应通过（checks=%v, errors=%v）", res2.Checks, res2.Errors)
	}
}

func TestValidateJSONFormatAndMissingTrafficStats(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCert(t, dir, time.Now().Add(-time.Hour), time.Now().Add(90*24*time.Hour))
	jsonBody := fmt.Sprintf(`{"listen":":44306","tls":{"cert":%q,"key":%q},"auth":{"type":"password","password":"supersecretpassword"}}`, cert, key)
	res := Validate(context.Background(), []byte(jsonBody), "json", ValidateOptions{
		CheckCert: true, CertWarnDays: 14, RequireTrafficStats: true,
	})
	if res.Valid {
		t.Fatal("RequireTrafficStats=true 时缺 trafficStats 必须拒绝")
	}
	res2 := Validate(context.Background(), []byte(jsonBody), "json", ValidateOptions{CheckCert: true, CertWarnDays: 14})
	if !res2.Valid {
		t.Fatalf("不强制 trafficStats 时应通过: %v", res2.Errors)
	}
	if res2.Format != "json" {
		t.Fatalf("格式识别错误: %s", res2.Format)
	}
}

func TestValidateGarbageInput(t *testing.T) {
	res := Validate(context.Background(), []byte("listen: :443\n\tbroken: [}"), "yaml", ValidateOptions{})
	if res.Valid {
		t.Fatal("YAML 语法错误必须拒绝")
	}
	res2 := Validate(context.Background(), nil, "yaml", ValidateOptions{})
	if res2.Valid {
		t.Fatal("空内容必须拒绝")
	}
}

func containsPrefix(list []string, prefix string) bool {
	for _, s := range list {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
