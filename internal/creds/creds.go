// Package creds 管理节点身份：注册凭据的持久化（0400）、node_secret 派生的 JWT、
// 以及带证书 pinning 的 TLS 配置。
//
// 对标 cloudflared 的 cert.pem 机制：enroll 一次拿到长期身份（node_id + node_secret），
// 之后每次启停都不再需要 enroll_token。
package creds

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
)

// Credentials 是磁盘上的节点身份文件（/etc/hysteria-agent/credentials.json，权限 0400）。
type Credentials struct {
	NodeID        string    `json:"node_id"`
	NodeSecret    string    `json:"node_secret"`
	ControlURL    string    `json:"control_url,omitempty"`
	NodeName      string    `json:"node_name,omitempty"`
	EnrolledAt    time.Time `json:"enrolled_at"`
	Issuer        string    `json:"jwt_issuer,omitempty"`
	Audience      string    `json:"jwt_audience,omitempty"`
	PinSHA256     []string  `json:"pin_sha256,omitempty"`
	CACertPEM     string    `json:"ca_cert_pem,omitempty"`
	ClientCertPEM string    `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string    `json:"client_key_pem,omitempty"`
	Protocol      string    `json:"protocol,omitempty"` // v1
}

// DefaultIssuer / DefaultAudience 是本地自签 JWT 的 iss/aud（控制面按此校验）。
const (
	DefaultIssuer   = "hysteria-agent"
	DefaultAudience = "hysteria-control"
)

// Load 读取凭据文件并按需收紧权限。
func Load(path string) (*Credentials, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		// 权限过宽不是"可忽略的警告"：node_secret 就是长期身份密钥。
		if err := os.Chmod(path, 0o400); err != nil {
			return nil, fmt.Errorf("凭据文件 %s 权限为 %#o，尝试收紧到 0400 失败: %w", path, perm, err)
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Credentials{}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("解析 %s 失败: %w", path, err)
	}
	if c.NodeID == "" || c.NodeSecret == "" {
		return nil, fmt.Errorf("%s 缺少 node_id / node_secret，请重新 enroll", path)
	}
	if c.Issuer == "" {
		c.Issuer = DefaultIssuer
	}
	if c.Audience == "" {
		c.Audience = DefaultAudience
	}
	return c, nil
}

// Save 原子写入凭据文件：先写 0600 临时文件并 fsync，再 rename，最后 chmod 0400。
func (c *Credentials) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if c.EnrolledAt.IsZero() {
		c.EnrolledAt = time.Now().UTC()
	}
	if c.Protocol == "" {
		c.Protocol = "v1"
	}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return os.Chmod(path, 0o400)
}

// ---------------------------- JWT ----------------------------

// MintJWT 用 node_secret 做 HS256 签发一个短期访问令牌（控制通道握手用）。
// 自签而不是向控制面换 token 的原因：node_secret 是双方共享的长期密钥，
// 控制面本地即可验证；节点断网重启后不需要额外往返就能连上。
func (c *Credentials) MintJWT(ttl time.Duration) (token string, exp time.Time, err error) {
	if ttl <= 0 {
		ttl = time.Hour
	}
	now := time.Now()
	exp = now.Add(ttl)
	jtiRaw := make([]byte, 8)
	if _, err := rand.Read(jtiRaw); err != nil {
		return "", time.Time{}, err
	}
	header := map[string]any{"alg": "HS256", "typ": "JWT", "kid": c.NodeID}
	claims := map[string]any{
		"iss": c.Issuer,
		"sub": c.NodeID,
		"aud": c.Audience,
		"iat": now.Unix(),
		"nbf": now.Add(-30 * time.Second).Unix(), // 容忍少量时钟偏移
		"exp": exp.Unix(),
		"jti": hex.EncodeToString(jtiRaw),
		"ver": "v1",
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", time.Time{}, err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	signing := b64(hb) + "." + b64(cb)
	mac := hmac.New(sha256.New, []byte(c.NodeSecret))
	mac.Write([]byte(signing))
	return signing + "." + b64(mac.Sum(nil)), exp, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// VerifyJWT 校验 HS256 令牌并返回 claims。控制面（Go 实现 / 测试用的 mock）可直接复用。
func VerifyJWT(token, secret string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("JWT 格式错误")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("JWT 签名解码失败: %w", err)
	}
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, fmt.Errorf("JWT 签名不匹配")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	claims := map[string]any{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	if exp, ok := claims["exp"].(float64); ok {
		if time.Now().Unix() > int64(exp) {
			return nil, fmt.Errorf("JWT 已过期")
		}
	}
	return claims, nil
}

// BearerHeader 返回控制通道握手用的 Authorization 头。
func (c *Credentials) BearerHeader(ttl time.Duration) (string, time.Time, error) {
	tok, exp, err := c.MintJWT(ttl)
	if err != nil {
		return "", time.Time{}, err
	}
	return "Bearer " + tok, exp, nil
}

// ---------------------------- TLS / pinning ----------------------------

// SPKIPin 计算证书的 SPKI SHA-256 pin（base64），与 HPKP 习惯一致。
func SPKIPin(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// TLSConfig 构建控制通道的 TLS 配置：
//   - 默认走系统根证书 + 客户端证书（若 enroll 时下发了 mTLS 证书）；
//   - ca_file / ca_cert_pem 提供私有 CA；
//   - pin_sha256 非空时强制校验对端证书链里有任意一张证书的 SPKI 匹配（证书 pinning）；
//   - insecure_skip_verify 只允许显式配置（默认 false），且与 pin 互斥时以 pin 为准并告警。
func (c *Credentials) TLSConfig(cc *config.Control, logger ...func(string)) (*tls.Config, error) {
	warn := func(msg string) {
		for _, f := range logger {
			// 允许调用方传 nil（variadic 里的 nil 会变成一个 nil 元素，直接调用会 panic）。
			if f != nil {
				f(msg)
			}
		}
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// 服务端可能只提供 SNI 路由，保持 http/1.1 ALPN 以便经过 Cloudflare Tunnel。
		NextProtos: []string{"http/1.1"},
	}

	pins := append([]string{}, cc.PinSHA256...)
	pins = append(pins, c.PinSHA256...)

	var roots *x509.CertPool
	if cc.CAFile != "" || c.CACertPEM != "" {
		pool := x509.NewCertPool()
		if cc.CAFile != "" {
			pem, err := os.ReadFile(cc.CAFile)
			if err != nil {
				return nil, fmt.Errorf("读取 control.ca_file 失败: %w", err)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("control.ca_file %s 中没有可用证书", cc.CAFile)
			}
		}
		if c.CACertPEM != "" {
			if !pool.AppendCertsFromPEM([]byte(c.CACertPEM)) {
				return nil, fmt.Errorf("凭据里的 ca_cert_pem 无法解析")
			}
		}
		roots = pool
		cfg.RootCAs = pool
	}

	if c.ClientCertPEM != "" && c.ClientKeyPEM != "" {
		pair, err := tls.X509KeyPair([]byte(c.ClientCertPEM), []byte(c.ClientKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("加载 mTLS 客户端证书失败: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}

	cfg.InsecureSkipVerify = cc.InsecureSkipVerify
	if cc.InsecureSkipVerify {
		if len(pins) == 0 {
			return nil, fmt.Errorf("insecure_skip_verify=true 时必须配置 pin_sha256，否则等于完全不校验对端")
		}
		warn("control.insecure_skip_verify=true：跳过证书链校验，仅依赖 pin_sha256")
	}

	if len(pins) > 0 {
		cfg.VerifyPeerCertificate = pinVerifier(pins, roots)
	}
	return cfg, nil
}

// pinVerifier 在标准校验之外强制 pin 匹配。
func pinVerifier(pins []string, roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("对端没有提供证书")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("解析对端证书失败: %w", err)
			}
			certs = append(certs, cert)
		}
		// 叶子证书 + 链上其余证书都要参与匹配（pin 可能是中间 CA）。
		for _, cert := range certs {
			got := SPKIPin(cert)
			for _, want := range pins {
				if hmac.Equal([]byte(got), []byte(strings.TrimSpace(want))) {
					return nil
				}
			}
		}
		got := SPKIPin(certs[0])
		return fmt.Errorf("证书 pinning 校验失败：对端叶子证书 SPKI pin = %s，本地允许 %v", got, pins)
	}
}

// Apply 把 Authorization 头与 User-Agent 加到请求上（enroll / token 之类的 REST 调用）。
func (c *Credentials) Apply(req *http.Request, cc *config.Control) error {
	if c != nil {
		h, _, err := c.BearerHeader(cc.JWTLifetime())
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", h)
	}
	if cc.UserAgent != "" {
		req.Header.Set("User-Agent", cc.UserAgent)
	}
	return nil
}
