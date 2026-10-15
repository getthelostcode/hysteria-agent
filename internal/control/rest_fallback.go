package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/version"

	"gopkg.in/yaml.v3"
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

	// 节点现状：面板渲染配置时应当照抄这些值，而不是写死。
	// 面板写死一个节点上不存在的证书路径、或与 agent 的 hysteria.api_secret 不一致的
	// trafficStats.secret 时，agent 会**整份拒收**配置（jp8 连发四版全 failed），
	// 而面板上完全看不出原因。
	CertPath           string `json:"cert_path,omitempty"`
	KeyPath            string `json:"key_path,omitempty"`
	TrafficStatsListen string `json:"traffic_stats_listen,omitempty"`
	TrafficStatsSecret string `json:"traffic_stats_secret,omitempty"`
}

// hysteriaShape 是节点当前 hysteria 配置里「面板需要照抄」的那几项。
type hysteriaShape struct {
	CertPath    string
	KeyPath     string
	StatsListen string
	StatsSecret string
}

// readHysteriaShape 读一次节点的 hysteria 配置（只取几个字段，读不到就返回空值）。
func readHysteriaShape(path string) hysteriaShape {
	if path == "" {
		return hysteriaShape{}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return hysteriaShape{}
	}

	var doc struct {
		TLS struct {
			Cert string `yaml:"cert"`
			Key  string `yaml:"key"`
		} `yaml:"tls"`
		TrafficStats struct {
			Listen string `yaml:"listen"`
			Secret string `yaml:"secret"`
		} `yaml:"trafficStats"`
	}

	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return hysteriaShape{}
	}

	return hysteriaShape{
		CertPath:    strings.TrimSpace(doc.TLS.Cert),
		KeyPath:     strings.TrimSpace(doc.TLS.Key),
		StatsListen: strings.TrimSpace(doc.TrafficStats.Listen),
		StatsSecret: strings.TrimSpace(doc.TrafficStats.Secret),
	}
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

	configEvery := time.Duration(c.cfg.Control.ConfigPollSeconds * float64(time.Second))
	if configEvery <= 0 {
		configEvery = 5 * time.Second
	}

	// 心跳与配置轮询**各跑各的 goroutine**。
	//
	// 踩过的坑：两件事共用一个 select 循环时，配置应用（校验→落盘→热重载→等就绪）一旦慢下来
	// （hysteria 就绪检查重试，实测 30s+，节点上还可能一直失败），心跳会被一起饿死 ——
	// 面板上的表现就是「最后心跳不更新」，看着像节点掉线，其实 agent 正忙着应用配置。
	// 存活上报必须独立于业务动作：它是运维判断生死的唯一依据。
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}

	// 窗口一到就整体收工，回到「重试控制通道」的状态。
	loopCtx, cancelLoops := context.WithDeadline(ctx, deadline)
	defer cancelLoops()

	run(func() { c.restHeartbeatLoop(loopCtx, deadline, interval) })
	run(func() { c.restConfigLoop(loopCtx, deadline, configEvery) })

	<-loopCtx.Done()

	// 收尾最多等一小会儿：配置应用可能不认 ctx（等 hysteria 就绪的循环），
	// 不能让它把「回控制通道重试」也一起拖住。
	settled := make(chan struct{})
	go func() { wg.Wait(); close(settled) }()

	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		c.log.Warn("REST 兜底窗口到期，仍有配置动作未收尾；先回到控制通道重试")
	}
}

// restHeartbeatLoop 独立心跳：只要进程活着、窗口没到，就按 interval 一直上报。
func (c *Client) restHeartbeatLoop(ctx context.Context, deadline time.Time, interval time.Duration) {
	c.sendRESTHeartbeat(ctx) // 立刻一次，别让运维等一个周期

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !time.Now().Before(deadline) {
				return
			}
			c.sendRESTHeartbeat(ctx)
		}
	}
}

// restConfigLoop 独立配置轮询：串行取配置（应用慢也只拖慢自己，不影响心跳）。
func (c *Client) restConfigLoop(ctx context.Context, deadline time.Time, every time.Duration) {
	apply := func() {
		actx, cancel := context.WithTimeout(ctx, c.configApplyTimeout())
		defer cancel()
		c.pollRESTConfig(actx)
	}

	apply() // 进兜底先拉一次：面板可能刚好发布过新配置

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !time.Now().Before(deadline) {
				return
			}
			apply()
		}
	}
}

// configApplyTimeout 单次「取配置 + 应用」的总超时（应用可能包含等 hysteria 就绪的重试）。
func (c *Client) configApplyTimeout() time.Duration {
	if secs := c.cfg.Control.ConfigApplyTimeoutSeconds; secs > 0 {
		return time.Duration(secs * float64(time.Second))
	}

	return 180 * time.Second
}

// logDriftOnce 同一类漂移只提示一次（心跳 30s 一次，刷屏会让 journal 没法看）。
func (c *Client) logDriftOnce(msg string) {
	c.mu.Lock()
	if c.driftWarned == nil {
		c.driftWarned = map[string]bool{}
	}
	if c.driftWarned[msg] {
		c.mu.Unlock()

		return
	}
	c.driftWarned[msg] = true
	c.mu.Unlock()

	c.log.Warn("节点现状与 agent 配置不一致", "detail", msg)
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

	// 面板照抄节点现状 —— 注意取哪个值：
	//   * trafficStats.secret 必须是 **agent 自己的 hysteria.api_secret**：校验比的就是它，
	//     用配置文件里的值会让「配置里的 secret ≠ api_secret」这种漂移被原样抄回面板，
	//     于是发布永远失败（jp8 现场）。
	//   * listen 取 agent 的 api_base_url：hysteria 绑在哪、agent 读哪里，写回配置后两者一致。
	//   * 证书路径取配置文件里的（校验就是看这些文件在不在）。
	shape := readHysteriaShape(c.cfg.Hysteria.ConfigPath)
	body.CertPath = shape.CertPath
	body.KeyPath = shape.KeyPath

	body.TrafficStatsSecret = c.cfg.Hysteria.APISecret
	if body.TrafficStatsSecret == "" {
		body.TrafficStatsSecret = shape.StatsSecret
	}

	body.TrafficStatsListen = strings.TrimPrefix(strings.TrimPrefix(c.cfg.Hysteria.APIBaseURL, "http://"), "https://")
	if body.TrafficStatsListen == "" {
		body.TrafficStatsListen = shape.StatsListen
	}

	// 漂移要能被发现：配置里的值与 agent 实际使用的不一致时，流量统计会 401。
	if shape.StatsSecret != "" && c.cfg.Hysteria.APISecret != "" && shape.StatsSecret != c.cfg.Hysteria.APISecret {
		c.logDriftOnce("trafficStats.secret 与 agent 的 hysteria.api_secret 不一致（流量统计会 401）：跑一次 `hysteria-agent config sync` 让控制面重新下发即可对齐")
	}

	_ = struct{}{}

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

// LatestConfig 是控制面 GET rest_config_path 的响应（REST 通道的配置下发）。
type LatestConfig struct {
	UpToDate bool
	Version  int
	Revision string
	SHA256   string
	Content  string
}

const defaultRESTConfigPath = "/api/v1/agent/config/latest"

// FetchLatestConfig 从控制面拉最新配置。
//
// 守护进程的兜底轮询与 CLI 的 `hysteria-agent config sync` 共用这一份实现：
// 鉴权（node_secret 自签 JWT）、TLS/pinning、错误处理都不该有两份，否则迟早分叉。
func FetchLatestConfig(ctx context.Context, cfg *config.Config, cr *creds.Credentials, currentVersion int, currentSHA string) (*LatestConfig, error) {
	path := cfg.Control.RESTConfigPath
	if path == "" {
		path = defaultRESTConfigPath
	}

	url := fmt.Sprintf("%s?current_version=%d", cfg.ControlHTTPURL(path), currentVersion)
	// 带上当前生效配置的 sha256：控制面就地把某个版本的正文改掉时版本号不变，
	// 只比版本号会让「改动」永远下不来。
	if strings.TrimSpace(currentSHA) != "" {
		url += "&current_sha256=" + neturl.QueryEscape(currentSHA)
	}

	reqCtx, cancel := context.WithTimeout(ctx, cfg.Control.ConnectTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if err := cr.Apply(req, &cfg.Control); err != nil {
		return nil, err
	}
	if cr.NodeID != "" {
		req.Header.Set("X-Node-ID", cr.NodeID)
	}

	resp, err := (&http.Client{Timeout: cfg.Control.ConnectTimeout()}).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("拉取配置被拒: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var payload restConfigLatest
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("解析控制面响应失败: %w", err)
	}

	out := &LatestConfig{UpToDate: payload.UpToDate}
	if payload.Config != nil {
		out.Version = payload.Config.Version
		out.Revision = payload.Config.Revision
		out.SHA256 = payload.Config.SHA256
		out.Content = payload.Config.ContentYAML
		if out.Revision == "" && out.Version > 0 {
			out.Revision = strconv.Itoa(out.Version)
		}
	}

	return out, nil
}

// pollRESTConfig 拉一次配置；有新版本就交给 agent 的应用管线（校验→原子写→热重载→失败回滚），
// 回执由 Enqueue 走 REST 发回（见 Enqueue 里的 config_ack 分支）。
//
// 为什么要它：本控制面终结不了二进制 WS，配置下发只能走 REST；否则面板「发布配置」写进库
// 就再无下文 —— 真实现场：库里所有配置都是 published、applied_at 为空。
func (c *Client) pollRESTConfig(ctx context.Context) {
	c.mu.Lock()
	current := c.restConfigVersion
	c.mu.Unlock()

	rc, err := FetchLatestConfig(ctx, c.cfg, c.cr, current, c.configSHA256Snapshot())
	if err != nil {
		c.m.RESTConfigFailures.Inc()
		c.log.Warn("REST 拉取配置失败", "error", err.Error())

		return
	}

	if rc.UpToDate || strings.TrimSpace(rc.Content) == "" {
		return
	}

	c.m.RESTConfigPolls.Inc()
	c.log.Info("REST 兜底：收到新配置", "version", rc.Version, "revision", rc.Revision, "sha256", rc.SHA256)

	// 先记账再应用：应用失败时面板会生成更高版本的回滚配置，下一次轮询会取到它。
	c.mu.Lock()
	c.restConfigVersion = rc.Version
	c.mu.Unlock()

	c.h.HandleConfigPush(ctx, &agentpb.ConfigPush{
		Revision: rc.Revision,
		Format:   "yaml",
		Payload:  []byte(rc.Content),
		Sha256:   rc.SHA256,
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
