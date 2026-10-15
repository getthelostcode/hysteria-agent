package cpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Paths 是控制面的路径模板（默认对齐 Laravel 的原生 Agent API）。
type Paths struct {
	Enroll        string
	Heartbeat     string
	ConfigLatest  string
	ConfigAck     string
	Metrics       string
	Logs          string
	CommandsPoll  string
	CommandAckFmt string // %s = command_id
	Diagnostics   string
	UpgradeCheck  string
}

// DefaultPaths 返回 Laravel 控制面的默认路径。
func DefaultPaths() Paths {
	return Paths{
		Enroll:        "/api/v1/agent/enroll",
		Heartbeat:     "/api/agent/v1/heartbeat",
		ConfigLatest:  "/api/agent/v1/config/latest",
		ConfigAck:     "/api/agent/v1/config/ack",
		Metrics:       "/api/agent/v1/metrics",
		Logs:          "/api/agent/v1/logs",
		CommandsPoll:  "/api/agent/v1/commands/poll",
		CommandAckFmt: "/api/agent/v1/commands/%s/ack",
		Diagnostics:   "/api/agent/v1/diagnostics",
		UpgradeCheck:  "/api/agent/v1/upgrade/check",
	}
}

// RetryPolicy 控制重试（指数退避 + 抖动，与 agent 的 outbox 策略同形）。
type RetryPolicy struct {
	Attempts   int
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	Jitter     float64
}

// DefaultRetryPolicy 与 controlplane.yaml 的 laravel.retry 默认值一致。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Attempts: 3, Initial: time.Second, Max: 5 * time.Minute, Multiplier: 2, Jitter: 0.2}
}

// Config 构造客户端所需的最小配置。
type Config struct {
	BaseURL   string
	NodeID    string
	Secret    string
	UserAgent string
	Timeout   time.Duration
	Paths     Paths
	Retry     RetryPolicy
	Gzip      bool
	// HTTPClient 可注入（测试/代理/自定义 TLS pinning）。
	HTTPClient *http.Client
	// Debugf 非空时记录每个请求（用于排障；不要打 secret）。
	Debugf func(format string, args ...any)
}

// Client 是「以节点代理身份」调用控制面的客户端。
type Client struct {
	cfg    Config
	paths  Paths
	signer *Signer
	hc     *http.Client
	rng    *rand.Rand
}

// New 构造客户端。
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("cpapi: BaseURL 不能为空")
	}
	if _, err := url.Parse(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("cpapi: BaseURL 非法: %w", err)
	}
	if cfg.Paths.Enroll == "" {
		cfg.Paths = DefaultPaths()
	}
	if cfg.Retry.Attempts <= 0 {
		cfg.Retry = DefaultRetryPolicy()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "hysteria-controlplane-bridge/1.0"
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}

	return &Client{
		cfg:    cfg,
		paths:  cfg.Paths,
		signer: NewSigner(cfg.Secret),
		hc:     hc,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404 - 抖动无需密码学随机
	}, nil
}

// APIError 表示控制面返回的非 2xx 响应。
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Body       string
}

func (e *APIError) Error() string {
	code := e.Code
	if code == "" {
		code = http.StatusText(e.StatusCode)
	}

	return fmt.Sprintf("控制面返回 %d %s: %s", e.StatusCode, code, e.Message)
}

// IsUnauthorized 判断是否认证失败（签名错/时间戳过期/nonce 重放 409/节点被删 404）。
func IsUnauthorized(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}

	return apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusConflict
}

// IsUnknownNode 判断控制面是否已不认识该节点（节点被删除/禁用）。
func IsUnknownNode(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}

	return apiErr.StatusCode == http.StatusNotFound ||
		apiErr.StatusCode == http.StatusForbidden ||
		(apiErr.Code == "unknown_node") ||
		(apiErr.Code == "node_disabled")
}

// ---- 具体接口 ----

// Heartbeat 上报心跳（同时是存活信号与配置/指令的提示位）。
func (c *Client) Heartbeat(ctx context.Context, req HeartbeatRequest) (*HeartbeatResponse, error) {
	var out HeartbeatResponse
	if err := c.call(ctx, http.MethodPost, c.paths.Heartbeat, nil, req, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// ConfigLatest 拉取最新配置；currentVersion 用于让控制面回答 up_to_date。
func (c *Client) ConfigLatest(ctx context.Context, currentVersion int) (*ConfigLatestResponse, error) {
	query := url.Values{}
	if currentVersion > 0 {
		query.Set("current_version", fmt.Sprintf("%d", currentVersion))
	}

	var out ConfigLatestResponse
	if err := c.call(ctx, http.MethodGet, c.paths.ConfigLatest, query, nil, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// ConfigAck 回执配置应用结果（status: applied|failed）。failed 会触发控制面自动回滚。
func (c *Client) ConfigAck(ctx context.Context, version int, status, errMsg string) (*ConfigAckResponse, error) {
	body := map[string]any{"version": version, "status": status}
	if errMsg != "" {
		body["error"] = errMsg
	}

	var out ConfigAckResponse
	if err := c.call(ctx, http.MethodPost, c.paths.ConfigAck, nil, body, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// Metrics 批量上报指标（默认 gzip；签名针对压缩后的字节）。
func (c *Client) Metrics(ctx context.Context, samples []MetricSample) (*IngestResponse, error) {
	body := map[string]any{"samples": samples}

	var out IngestResponse
	if err := c.call(ctx, http.MethodPost, c.paths.Metrics, nil, body, &out, c.cfg.Gzip); err != nil {
		return nil, err
	}

	return &out, nil
}

// Logs 批量上报日志。
func (c *Client) Logs(ctx context.Context, entries []LogEntry) (*IngestResponse, error) {
	body := map[string]any{"entries": entries}

	var out IngestResponse
	if err := c.call(ctx, http.MethodPost, c.paths.Logs, nil, body, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// CommandsPoll 拉取待执行指令（limit<=0 时用控制面默认值）。
func (c *Client) CommandsPoll(ctx context.Context, limit int) (*CommandsPollResponse, error) {
	body := map[string]any{}
	if limit > 0 {
		body["limit"] = limit
	}

	var out CommandsPollResponse
	if err := c.call(ctx, http.MethodPost, c.paths.CommandsPoll, nil, body, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// CommandAck 回执指令执行结果（status: ok|failed）。
func (c *Client) CommandAck(ctx context.Context, commandID, status string, result map[string]any, errMsg string) (*CommandAckResponse, error) {
	body := map[string]any{"status": status}
	if len(result) > 0 {
		body["result"] = result
	}
	if errMsg != "" {
		body["error"] = errMsg
	}

	var out CommandAckResponse
	path := fmt.Sprintf(c.paths.CommandAckFmt, url.PathEscape(commandID))
	if err := c.call(ctx, http.MethodPost, path, nil, body, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// Diagnostics 上报诊断报告（payload 结构由控制面原样归档）。
func (c *Client) Diagnostics(ctx context.Context, payload map[string]any) error {
	body := map[string]any{"payload": payload}

	return c.call(ctx, http.MethodPost, c.paths.Diagnostics, nil, body, nil, false)
}

// UpgradeCheck 询问是否有新版本（arch/currentVersion/channel）。
func (c *Client) UpgradeCheck(ctx context.Context, arch, currentVersion, channel string) (*UpgradeCheckResponse, error) {
	query := url.Values{"arch": {arch}, "version": {currentVersion}}
	if channel != "" {
		query.Set("channel", channel)
	}

	var out UpgradeCheckResponse
	if err := c.call(ctx, http.MethodGet, c.paths.UpgradeCheck, query, nil, &out, false); err != nil {
		return nil, err
	}

	return &out, nil
}

// Enroll 代为注册（**不签名**，用 enroll_token 换 node_id + node_secret）。
// baseURL 可覆盖客户端自身的 BaseURL（注册地址与数据面可能不同域）。
func Enroll(ctx context.Context, cfg Config, baseURL string, req EnrollRequest) (*EnrollResponse, error) {
	if baseURL != "" {
		cfg.BaseURL = baseURL
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("cpapi: 注册地址（BaseURL）不能为空")
	}

	client, err := New(cfg)
	if err != nil {
		return nil, err
	}

	if req.EnrollToken == "" {
		return nil, errors.New("cpapi: enroll_token 不能为空")
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	path := client.paths.Enroll
	full := strings.TrimRight(client.cfg.BaseURL, "/") + path

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, full, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", client.cfg.UserAgent)
	httpReq.Header.Set(HeaderEnrollTok, req.EnrollToken)

	resp, err := client.hc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("注册请求失败: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, decodeAPIError(resp.StatusCode, payload)
	}

	var out EnrollResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("解析注册响应失败: %w", err)
	}
	if out.NodeID == "" || out.NodeSecret == "" {
		return nil, errors.New("注册响应缺少 node_id / node_secret")
	}

	return &out, nil
}

// ---- 内部实现 ----

// call 执行一次带重试的签名请求。
func (c *Client) call(ctx context.Context, method, path string, query url.Values, body any, out any, gzipBody bool) error {
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}

	full := strings.TrimRight(c.cfg.BaseURL, "/") + path
	if len(query) > 0 {
		full += "?" + query.Encode()
	}

	attempts := c.cfg.Retry.Attempts
	backoff := c.cfg.Retry.Initial

	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		payload, encoding, err := c.encodeBody(raw, gzipBody)
		if err != nil {
			return err
		}

		// path（不含查询串）参与签名；每次尝试都重新生成 timestamp/nonce。
		signedPath := path
		headers := c.signer.Headers(method, signedPath, payload)
		headers[HeaderNodeID] = c.cfg.NodeID

		req, err := http.NewRequestWithContext(ctx, method, full, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", c.cfg.UserAgent)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		if c.cfg.Debugf != nil {
			c.cfg.Debugf("cpapi %s %s attempt=%d bytes=%d", method, signedPath, attempt, len(payload))
		}

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("请求控制面失败: %w", err)
			if attempt == attempts || !sleepCtx(ctx, jitter(backoff, c.cfg.Retry.Jitter, c.rng)) {
				return lastErr
			}
			backoff = nextBackoff(backoff, c.cfg.Retry)

			continue
		}

		respPayload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()

		if readErr != nil {
			lastErr = readErr
			if attempt == attempts {
				return lastErr
			}
			backoff = nextBackoff(backoff, c.cfg.Retry)

			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
			if out == nil || len(respPayload) == 0 {
				return nil
			}

			return json.Unmarshal(respPayload, out)
		}

		apiErr := decodeAPIError(resp.StatusCode, respPayload)

		// 4xx（除 429）是确定性错误：不重试。
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return apiErr
		}

		lastErr = apiErr
		if attempt == attempts || !sleepCtx(ctx, jitter(backoff, c.cfg.Retry.Jitter, c.rng)) {
			return lastErr
		}
		backoff = nextBackoff(backoff, c.cfg.Retry)
	}

	return lastErr
}

func (c *Client) encodeBody(raw []byte, useGzip bool) ([]byte, string, error) {
	if !useGzip || len(raw) == 0 {
		return raw, "", nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}

	return buf.Bytes(), "gzip", nil
}

func decodeAPIError(status int, payload []byte) *APIError {
	apiErr := &APIError{StatusCode: status, Body: string(payload)}

	var body ErrorBody
	if err := json.Unmarshal(payload, &body); err == nil {
		apiErr.Code = body.Error
		apiErr.Message = body.Message
	}
	if apiErr.Message == "" {
		apiErr.Message = strings.TrimSpace(string(payload))
		if len(apiErr.Message) > 200 {
			apiErr.Message = apiErr.Message[:200]
		}
	}

	return apiErr
}

func nextBackoff(current time.Duration, policy RetryPolicy) time.Duration {
	multiplier := policy.Multiplier
	if multiplier <= 1 {
		multiplier = 2
	}
	next := time.Duration(float64(current) * multiplier)
	if policy.Max > 0 && next > policy.Max {
		next = policy.Max
	}

	return next
}

func jitter(d time.Duration, ratio float64, rng *rand.Rand) time.Duration {
	if ratio <= 0 {
		return d
	}
	delta := float64(d) * ratio
	// #nosec G404 - 退避抖动不需要密码学随机
	return time.Duration(float64(d) - delta + rng.Float64()*2*delta)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
