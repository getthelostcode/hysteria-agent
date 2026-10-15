package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// REST 心跳兜底。
//
// 为什么需要它：控制面可能终结不了 /api/v1/agent/control 这个二进制 WS —— Laravel/PHP 就是
// 这样（返回 501）。没有兜底时，节点除了注册之外与控制面再无任何往来：面板「最后心跳」
// 永远停在注册那一刻，状态列也永远不动（真实现场）。DESIGN §5.1 承诺过「WS 不可用时降级
// 到 REST 轮询」，这里把它补齐，先补上最关键的**心跳**（指标/日志仍走控制通道）。
//
// 行为：握手被拒为 404/405/501 时进入 REST 模式，每 heartbeat_interval 发一次心跳；
// 持续 rest_fallback_seconds 后返回，由 Run() 重新尝试控制通道 —— 两端都不会僵死：
// 控制面一旦换成能终结 WS 的实现（例如 Go 控制面），最迟一个窗口后自动恢复。

// restHeartbeatBody 与面板侧 /api/v1/agent/heartbeat 的字段约定一致。
type restHeartbeatBody struct {
	NodeID          string  `json:"node_id,omitempty"`
	Hostname        string  `json:"hostname,omitempty"`
	AgentVersion    string  `json:"agent_version"`
	HysteriaVersion string  `json:"hysteria_version,omitempty"`
	UptimeSeconds   int64   `json:"uptime_seconds,omitempty"`
	Ready           bool    `json:"ready"`
	HysteriaRunning bool    `json:"hysteria_running"`
	ConfigRevision  string  `json:"config_revision,omitempty"`
	PublicIP        string  `json:"public_ip,omitempty"`
	Degraded        bool    `json:"degraded"`
	SentAtMs        int64   `json:"sent_at_ms"`
	Uptime          float64 `json:"uptime,omitempty"`
}

// RESTFallbackActive 报告当前是否处于 REST 兜底模式（CLI status 用）。
func (c *Client) RESTFallbackActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.restFallback
}

// runRESTFallback 进入 REST 心跳模式，到期或被取消后返回。
func (c *Client) runRESTFallback(ctx context.Context, cause error) {
	window := time.Duration(c.cfg.Control.RESTFallbackSeconds * float64(time.Second))
	if window <= 0 {
		return
	}

	c.mu.Lock()
	c.restFallback = true
	c.lastErr = fmt.Sprintf("控制通道不可用（%v），已降级为 REST 心跳", cause)
	c.mu.Unlock()

	c.m.ControlConnected.Set(0)
	c.log.Warn("控制通道端点不可用，降级为 REST 心跳",
		"error", cause.Error(),
		"endpoint", c.cfg.Control.RESTHeartbeatPath,
		"window_seconds", window.Seconds(),
		"interval_seconds", c.heartbeatDuration().Seconds())

	defer func() {
		c.mu.Lock()
		c.restFallback = false
		c.mu.Unlock()
	}()

	deadline := time.Now().Add(window)
	interval := c.heartbeatDuration()
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// 立刻发一次，别让运维等一个心跳周期。
	c.sendRESTHeartbeat(ctx)
	// 配置也立刻拉一次：面板可能刚好发布过新配置
	c.pollRESTConfig(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	configEvery := time.Duration(c.cfg.Control.ConfigPollSeconds * float64(time.Second))
	if configEvery <= 0 {
		configEvery = 5 * time.Second
	}
	configTicker := time.NewTicker(configEvery)
	defer configTicker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sendRESTHeartbeat(ctx)
		case <-configTicker.C:
			c.pollRESTConfig(ctx)
		}
	}
}

func (c *Client) sendRESTHeartbeat(ctx context.Context) {
	path := c.cfg.Control.RESTHeartbeatPath
	if path == "" {
		path = "/api/v1/agent/heartbeat"
	}

	body := restHeartbeatBody{
		Hostname:        c.hostname(),
		AgentVersion:    version.Version,
		HysteriaVersion: c.hysteriaVersionSnapshot(),
		Ready:           c.readySnapshot(),
		HysteriaRunning: c.hysteriaRunningSnapshot(),
		ConfigRevision:  c.configRevisionSnapshot(),
		PublicIP:        c.publicIP(),
		Degraded:        true,
		SentAtMs:        time.Now().UnixMilli(),
	}

	if c.cr != nil {
		body.NodeID = c.cr.NodeID
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Control.ConnectTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.cfg.ControlHTTPURL(path), bytes.NewReader(raw))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if err := c.cr.Apply(req, &c.cfg.Control); err != nil {
		c.log.Warn("REST 心跳签名失败", "error", err.Error())

		return
	}
	if c.cr.NodeID != "" {
		req.Header.Set("X-Node-ID", c.cr.NodeID)
	}

	resp, err := (&http.Client{Timeout: c.cfg.Control.ConnectTimeout()}).Do(req)
	if err != nil {
		c.m.RESTHeartbeatFailures.Inc()
		c.log.Warn("REST 心跳失败", "error", err.Error(), "path", path)

		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		c.m.RESTHeartbeatFailures.Inc()
		c.log.Warn("REST 心跳被拒", "status", resp.StatusCode, "path", path)

		return
	}

	c.m.RESTHeartbeats.Inc()
	c.mu.Lock()
	c.lastSeen = time.Now()
	c.mu.Unlock()
}

func (c *Client) hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}

	return h
}

// shouldFallBack 判断该错误是否说明「对端没有实现控制通道端点」。
func shouldFallBack(err error, cfg *config.Config) bool {
	if cfg.Control.RESTFallbackSeconds <= 0 {
		return false
	}

	var he *HandshakeError

	return errors.As(err, &he) && he.Unimplemented()
}

// ---------------- 配置下发（REST 兜底通道） ----------------

// restConfigLatest 是控制面 GET rest_config_path 的响应。
type restConfigLatest struct {
	OK       bool `json:"ok"`
	UpToDate bool `json:"up_to_date"`
	Config   *struct {
		Version     int    `json:"version"`
		SHA256      string `json:"sha256"`
		Status      string `json:"status"`
		Revision    string `json:"revision"`
		ContentYAML string `json:"content_yaml"`
	} `json:"config"`
}

// pollRESTConfig 拉一次配置；有新版本就交给 agent 的应用管线（校验→原子写→热重载→失败回滚），
// 回执由 Enqueue 走 REST 发回（见 Enqueue 里的 config_ack 分支）。
//
// 为什么要它：本控制面终结不了二进制 WS，配置下发只能走 REST；否则面板「发布配置」写进库
// 就再无下文 —— 真实现场：库里所有配置都是 published、applied_at 为空。
func (c *Client) pollRESTConfig(ctx context.Context) {
	path := c.cfg.Control.RESTConfigPath
	if path == "" {
		path = "/api/v1/agent/config/latest"
	}

	c.mu.Lock()
	current := c.restConfigVersion
	c.mu.Unlock()

	url := fmt.Sprintf("%s?current_version=%d", c.cfg.ControlHTTPURL(path), current)

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Control.ConnectTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "application/json")
	if err := c.cr.Apply(req, &c.cfg.Control); err != nil {
		return
	}
	if c.cr.NodeID != "" {
		req.Header.Set("X-Node-ID", c.cr.NodeID)
	}

	resp, err := (&http.Client{Timeout: c.cfg.Control.ConnectTimeout()}).Do(req)
	if err != nil {
		c.m.RESTConfigFailures.Inc()

		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		c.m.RESTConfigFailures.Inc()
		c.log.Warn("REST 拉取配置被拒", "status", resp.StatusCode, "path", path)

		return
	}

	var payload restConfigLatest
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		c.m.RESTConfigFailures.Inc()

		return
	}

	if payload.UpToDate || payload.Config == nil || strings.TrimSpace(payload.Config.ContentYAML) == "" {
		return
	}

	cfg := payload.Config
	revision := cfg.Revision
	if revision == "" {
		revision = strconv.Itoa(cfg.Version)
	}

	c.m.RESTConfigPolls.Inc()
	c.log.Info("REST 兜底：收到新配置", "version", cfg.Version, "revision", revision, "sha256", cfg.SHA256)

	// 先记账再应用：应用失败时面板会生成更高版本的回滚配置，下一次轮询会取到它。
	c.mu.Lock()
	c.restConfigVersion = cfg.Version
	c.mu.Unlock()

	c.h.HandleConfigPush(ctx, &agentpb.ConfigPush{
		Revision: revision,
		Format:   "yaml",
		Payload:  []byte(cfg.ContentYAML),
		Sha256:   cfg.SHA256,
		Reason:   "rest-fallback",
	})
}

// sendRESTConfigAck 把配置回执通过 REST 发回控制面（兜底模式下没有 WS 可用）。
func (c *Client) sendRESTConfigAck(ctx context.Context, ack *agentpb.ConfigAck) error {
	path := strings.TrimSuffix(c.cfg.Control.RESTConfigPath, "/latest")
	if path == "" || path == c.cfg.Control.RESTConfigPath {
		path = "/api/v1/agent/config"
	}

	version, err := strconv.Atoi(strings.TrimSpace(ack.GetRevision()))
	if err != nil || version <= 0 {
		// 没有数字版本号（例如 revision 是 "20261015-3"）时无法回执，记一条警告即可。
		c.log.Warn("REST 配置回执缺少数字版本号，跳过", "revision", ack.GetRevision())

		return fmt.Errorf("revision %q 不是面板的版本号", ack.GetRevision())
	}

	status := "failed"
	if ack.GetOk() {
		status = "applied"
	}

	body, _ := json.Marshal(map[string]any{
		"version":    version,
		"status":     status,
		"error":      ack.GetError(),
		"applied_at": time.Now().UTC().Format(time.RFC3339),
	})

	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.Control.ConnectTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.cfg.ControlHTTPURL(path+"/ack"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := c.cr.Apply(req, &c.cfg.Control); err != nil {
		return err
	}
	if c.cr.NodeID != "" {
		req.Header.Set("X-Node-ID", c.cr.NodeID)
	}

	resp, err := (&http.Client{Timeout: c.cfg.Control.ConnectTimeout()}).Do(req)
	if err != nil {
		c.m.RESTConfigFailures.Inc()

		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		c.m.RESTConfigFailures.Inc()
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		c.log.Warn("配置回执被拒", "status", resp.StatusCode, "url", req.URL.String(), "body", strings.TrimSpace(string(detail)))

		return fmt.Errorf("配置回执被拒: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}

	c.m.RESTConfigAcks.Inc()
	c.log.Info("REST 兜底：配置回执已送达", "version", version, "status", status, "rolled_back", ack.GetRolledBack())

	return nil
}
