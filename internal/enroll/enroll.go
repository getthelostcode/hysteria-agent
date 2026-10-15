// Package enroll 实现首次注册：用 enroll_token 换取长期身份（node_id + node_secret）。
//
// 参考 cloudflared：注册只做一次，产物落到本机凭据文件，后续启动不再需要 token。
package enroll

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// Request 是注册请求体。
type Request struct {
	EnrollToken     string `json:"enroll_token"`
	NodeName        string `json:"node_name,omitempty"`
	Hostname        string `json:"hostname"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	Kernel          string `json:"kernel,omitempty"`
	MachineID       string `json:"machine_id,omitempty"`
	BootID          string `json:"boot_id,omitempty"`
	AgentVersion    string `json:"agent_version"`
	HysteriaVersion string `json:"hysteria_version,omitempty"`
	Protocol        string `json:"protocol"`
	// Replace 为 true 时允许覆盖已存在的节点注册（需控制面支持）。
	Replace bool `json:"replace,omitempty"`
}

// Response 是注册响应体。
type Response struct {
	NodeID        string   `json:"node_id"`
	NodeSecret    string   `json:"node_secret"`
	NodeName      string   `json:"node_name,omitempty"`
	ControlURL    string   `json:"control_url,omitempty"`
	WSPath        string   `json:"ws_path,omitempty"`
	PinSHA256     []string `json:"pin_sha256,omitempty"`
	CACertPEM     string   `json:"ca_cert_pem,omitempty"`
	ClientCertPEM string   `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string   `json:"client_key_pem,omitempty"`
	JWTIssuer     string   `json:"jwt_issuer,omitempty"`
	JWTAudience   string   `json:"jwt_audience,omitempty"`

	HeartbeatIntervalSeconds int    `json:"heartbeat_interval_seconds,omitempty"`
	TelemetryIntervalSeconds int    `json:"telemetry_interval_seconds,omitempty"`
	ConfigRevision           string `json:"config_revision,omitempty"`
	Message                  string `json:"message,omitempty"`
}

// Options 控制注册行为。
type Options struct {
	Token       string
	NodeName    string
	Replace     bool
	Timeout     time.Duration
	HysteriaBin string
}

// Run 执行注册并落盘凭据；返回写入的凭据。token 不会出现在日志里（只打印前 6 位）。
func Run(ctx context.Context, cfg *config.Config, opt Options, logf func(format string, args ...any)) (*creds.Credentials, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if strings.TrimSpace(opt.Token) == "" {
		return nil, fmt.Errorf("enroll_token 不能为空（用法: hysteria-agent enroll --token=<token>）")
	}
	if cfg.Control.ServerURL == "" {
		return nil, fmt.Errorf("未配置 control.server_url，请先编辑 %s", cfg.Path)
	}

	// 注册阶段的 TLS：pin 只能来自配置文件（本地还没有凭据）。
	// 用一份空凭据 + 配置里的 pin/ca 构建，逻辑与后续控制通道完全一致。
	empty := &creds.Credentials{}
	tlsCfg, err := empty.TLSConfig(&cfg.Control, func(msg string) { logf("%s", msg) })
	if err != nil {
		return nil, err
	}

	timeout := opt.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	hostname, _ := os.Hostname()
	reqBody := Request{
		EnrollToken:  strings.TrimSpace(opt.Token),
		NodeName:     opt.NodeName,
		Hostname:     hostname,
		OS:           "linux",
		Arch:         sysinfo.Arch(),
		Kernel:       sysinfo.Kernel(),
		MachineID:    sysinfo.MachineID(),
		BootID:       sysinfo.BootID(),
		AgentVersion: version.Version,
		Protocol:     "v1",
		Replace:      opt.Replace,
	}
	if opt.HysteriaBin != "" {
		reqBody.HysteriaVersion = sysinfo.BinaryVersion(ctx, opt.HysteriaBin, 5*time.Second)
	}

	raw, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	enrollURL := cfg.ControlHTTPURL(cfg.Control.EnrollPath)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, enrollURL, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", cfg.Control.UserAgent)
	httpReq.Header.Set("X-Agent-Protocol", "v1")
	// 同时给两种常见风格：Bearer + 自定义头，方便对端任选其一。
	httpReq.Header.Set("X-Enroll-Token", reqBody.EnrollToken)

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig:       tlsCfg,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          4,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   cfg.Control.ConnectTimeout(),
			ResponseHeaderTimeout: timeout,
		},
	}
	logf("正在注册节点: POST %s (token=%s…, node_name=%q)", enrollURL, maskToken(reqBody.EnrollToken), opt.NodeName)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("连接控制面失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("注册被拒绝(%d)：enroll_token 无效、已使用或已过期（%s）", resp.StatusCode, snippet(body))
	case resp.StatusCode == http.StatusConflict:
		return nil, fmt.Errorf("节点已存在(%d)：如需重新注册请在控制面删除旧节点，或加 --replace（%s）", resp.StatusCode, snippet(body))
	default:
		return nil, fmt.Errorf("注册失败(%d)：%s", resp.StatusCode, snippet(body))
	}

	var r Response
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("解析注册响应失败: %w (body=%s)", err, snippet(body))
	}
	if r.NodeID == "" || r.NodeSecret == "" {
		return nil, fmt.Errorf("注册响应缺少 node_id/node_secret: %s", snippet(body))
	}
	if len(r.NodeSecret) < 16 {
		return nil, fmt.Errorf("控制面返回的 node_secret 过短（%d 字节），拒绝使用", len(r.NodeSecret))
	}

	c := &creds.Credentials{
		NodeID:        r.NodeID,
		NodeSecret:    r.NodeSecret,
		ControlURL:    r.ControlURL,
		NodeName:      r.NodeName,
		EnrolledAt:    time.Now().UTC(),
		Issuer:        orDefault(r.JWTIssuer, creds.DefaultIssuer),
		Audience:      orDefault(r.JWTAudience, creds.DefaultAudience),
		PinSHA256:     r.PinSHA256,
		CACertPEM:     r.CACertPEM,
		ClientCertPEM: r.ClientCertPEM,
		ClientKeyPEM:  r.ClientKeyPEM,
		Protocol:      "v1",
	}
	if err := c.Save(cfg.CredentialsFile); err != nil {
		return nil, fmt.Errorf("写入凭据文件失败: %w", err)
	}
	logf("注册成功: node_id=%s，凭据已写入 %s (0400)", c.NodeID, cfg.CredentialsFile)
	if len(c.PinSHA256) > 0 {
		logf("控制面下发了 %d 个证书 pin，后续连接将做 SPKI pinning", len(c.PinSHA256))
	}
	if c.ClientCertPEM != "" {
		logf("控制面下发了 mTLS 客户端证书，控制通道将使用双向 TLS")
	}
	return c, nil
}

func maskToken(t string) string {
	if len(t) <= 6 {
		return "***"
	}
	return t[:6] + "***"
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// PingTLS 在 enroll 前做一次 TLS 探测，把 pin/CA 配错的问题提前暴露出来。
func PingTLS(ctx context.Context, cfg *config.Config) error {
	empty := &creds.Credentials{}
	tlsCfg, err := empty.TLSConfig(&cfg.Control, nil)
	if err != nil {
		return err
	}
	if tlsCfg.InsecureSkipVerify && len(cfg.Control.PinSHA256) == 0 {
		tlsCfg.InsecureSkipVerify = false // 探测时不要真的跳过校验
	}
	host := strings.TrimPrefix(strings.TrimPrefix(cfg.Control.ServerURL, "https://"), "http://")
	host = strings.TrimSuffix(host, "/")
	d := &tls.Dialer{Config: tlsCfg, NetDialer: nil}
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return err
	}
	return conn.Close()
}
