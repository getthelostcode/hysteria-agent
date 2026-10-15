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
	"net"
	"net/http"
	"net/url"
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
	// PublicIP：自报的出口公网地址。控制面在 NAT/隧道后只能看到代理地址（真实故障：
	// 面板上所有节点的 IP 都是 127.0.0.1，因为 cloudflared 就在控制面本机）。
	PublicIP string `json:"public_ip,omitempty"`
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
	if err := ValidateTokenShape(opt.Token); err != nil {
		return nil, err
	}
	if cfg.Control.ServerURL == "" {
		return nil, fmt.Errorf("未配置 control.server_url，请先编辑 %s", cfg.Path)
	}

	// 自报公网 IP：尽力而为（探测失败不带该字段继续注册，绝不能让 IP 查询挡住注册）。
	publicIP := detectPublicIP(ctx, cfg, logf)
	if publicIP != "" {
		logf("本机出口公网地址: %s（将随注册上报，便于控制面展示真实 IP）", publicIP)
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
		PublicIP:     publicIP,
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
		return nil, fmt.Errorf("注册被拒绝(%d)：enroll_token 无效、已使用或已过期%s", resp.StatusCode, responseHint(resp, body))
	case resp.StatusCode == http.StatusConflict:
		return nil, fmt.Errorf("节点已存在(%d)：如需重新注册请在控制面删除旧节点，或加 --replace%s", resp.StatusCode, responseHint(resp, body))
	case resp.StatusCode == http.StatusNotFound && looksLikeHTML(body):
		return nil, fmt.Errorf("注册失败(404)：控制面在该路径返回了 HTML 页面而不是 JSON API。\n"+
			"  实际请求: POST %s\n"+
			"  常见原因：① control.enroll_path 与实际路由不一致；② 该控制面还没有实现 Agent 接口\n"+
			"  （Laravel/Rails 等框架的 404 页面长这样；本项目的接口约定见 DESIGN.md §5.1）。\n"+
			"  自检：hysteria-agent enroll --check（会打印探测结果与提示）", enrollURL)
	default:
		return nil, fmt.Errorf("注册失败(%d)%s", resp.StatusCode, responseHint(resp, body))
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

// looksLikeHTML 判断响应体是不是网页（框架默认 404/首页），而不是 JSON API。
func looksLikeHTML(b []byte) bool {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return false
	}
	low := strings.ToLower(s)
	return strings.HasPrefix(low, "<!doctype") || strings.HasPrefix(low, "<html")
}

// responseHint 把"最常见的错配"直接写进错误里，省掉一轮排查。
func responseHint(resp *http.Response, body []byte) string {
	ct := resp.Header.Get("Content-Type")
	switch {
	case looksLikeHTML(body):
		return fmt.Sprintf("：对端返回了 HTML 页面（Content-Type=%s），说明这个路径不是 JSON API——检查 enroll_path，或该控制面还没实现 Agent 接口", ct)
	case len(body) == 0:
		return "：响应体为空"
	default:
		return fmt.Sprintf("：%s", snippet(body))
	}
}

// ProbeResult 是 --check 的探测结论。
type ProbeResult struct {
	URL         string
	TLSOK       bool
	TLSErr      error
	StatusCode  int
	ContentType string
	Body        []byte
	DialErr     error
}

// Hint 给出针对当前探测结果的处置建议（可直接照做）。
func (p *ProbeResult) Hint() string {
	switch {
	case p.StatusCode == 0 && p.DialErr != nil:
		return "连不上控制面：确认域名可解析、端口可达（https 默认 443）、防火墙/Cloudflare 隧道是否放行。"
	case p.StatusCode == 0:
		return "请求已发出但没有拿到响应：检查是否被中间件截断。"
	case p.StatusCode == http.StatusUnauthorized || p.StatusCode == http.StatusForbidden:
		return "路径正确、但认证被拒：enroll_token 无效/已使用/已过期。"
	case p.StatusCode == http.StatusMethodNotAllowed:
		return "该路径不接受 POST（本项目 Agent 用 POST 注册）。"
	case p.StatusCode == http.StatusNotFound && looksLikeHTML(p.Body):
		return "对端在该路径返回网页 404：① control.enroll_path 与实际路由不一致；② 控制面尚未实现 Agent 接口（Laravel/Rails 的默认 404 页正是这种）。需要控制面提供 POST " + p.URL + "。"
	case p.StatusCode == http.StatusNotFound:
		return "404：路径不对。核对控制面 enroll 路由的完整路径，必要时改 control.enroll_path。"
	case p.StatusCode >= 500:
		return "控制面 5xx：对端内部错误，查控制面日志。"
	case p.StatusCode == http.StatusOK || p.StatusCode == http.StatusCreated:
		if looksLikeHTML(p.Body) {
			return "2xx 但返回网页：命中了站点前端路由，不是 Agent API。"
		}
		return "路径可达且返回 2xx：若正式注册仍失败，多为令牌/节点名等业务校验。"
	default:
		return fmt.Sprintf("HTTP %d，请对照控制面日志排查。", p.StatusCode)
	}
}

// ProbeHTTP 对 enroll 端点发一次真实请求（占位 token，不泄露真令牌），
// 让"路径是否存在、是不是 JSON API"变得可判定。
func ProbeHTTP(ctx context.Context, cfg *config.Config) *ProbeResult {
	res := &ProbeResult{URL: cfg.ControlHTTPURL(cfg.Control.EnrollPath)}
	empty := &creds.Credentials{}
	tlsCfg, err := empty.TLSConfig(&cfg.Control, nil)
	if err != nil {
		res.TLSErr = err
		res.DialErr = err
		return res
	}
	if tlsCfg.InsecureSkipVerify && len(cfg.Control.PinSHA256) == 0 {
		tlsCfg.InsecureSkipVerify = false
	}
	res.TLSOK = true
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsCfg, Proxy: http.ProxyFromEnvironment},
	}
	payload, _ := json.Marshal(Request{
		EnrollToken:  "probe-token-not-real",
		NodeName:     "probe",
		Hostname:     "probe",
		AgentVersion: version.Version,
	})
	req, rerr := http.NewRequestWithContext(ctx, http.MethodPost, res.URL, bytes.NewReader(payload))
	if rerr != nil {
		res.DialErr = rerr
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Version", version.Version)
	resp, derr := client.Do(req)
	if derr != nil {
		res.DialErr = derr
		res.TLSOK = false
		return res
	}
	defer resp.Body.Close()
	res.StatusCode = resp.StatusCode
	res.ContentType = resp.Header.Get("Content-Type")
	res.Body, _ = io.ReadAll(io.LimitReader(resp.Body, 4096))
	return res
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
	addr, err := controlAddr(cfg.Control.ServerURL)
	if err != nil {
		return err
	}
	d := &tls.Dialer{Config: tlsCfg}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

// controlAddr 从控制面地址解析出可直接拨号的 "host:port"。
//
// 之前的实现直接拿 "node.ipx.sarl" 去 Dial，会得到
// `dial tcp: address node.ipx.sarl: missing port in address`——
// 没有端口的 URL 必须补默认端口（https/wss→443，http/ws→80），
// 同时要丢掉 path/query 并正确处理 IPv6 字面量。
func controlAddr(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("control.server_url 无法解析: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("control.server_url 里没有主机名: %q", raw)
	}
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http", "ws":
			port = "80"
		default:
			port = "443"
		}
	}
	return net.JoinHostPort(host, port), nil
}
