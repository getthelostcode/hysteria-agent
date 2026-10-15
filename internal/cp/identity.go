package cp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/cpapi"
)

// 身份权威：谁签发 node_id / node_secret。
//
//   - AuthorityLocal：本控制面自行派生（master_key + node_id），与 Laravel 无关；
//   - AuthorityLaravel：Laravel 控制面是唯一权威，本控制面把 enroll 代理过去，
//     并保存每个节点的 secret（state.json，0600）以便替节点签 REST。
//
// 详见 docs/BRIDGE-TO-CONTROL-PLANE.md。
const (
	AuthorityLocal   = "local"
	AuthorityLaravel = "laravel"
)

// IdentityConfig 选择身份权威。
type IdentityConfig struct {
	Authority string `yaml:"authority" json:"authority"`
}

// RetryConfig 桥接请求的重试策略（秒/倍率）。
type RetryConfig struct {
	InitialSeconds int     `yaml:"initial_seconds" json:"initial_seconds"`
	MaxSeconds     int     `yaml:"max_seconds" json:"max_seconds"`
	Multiplier     float64 `yaml:"multiplier" json:"multiplier"`
	Jitter         float64 `yaml:"jitter" json:"jitter"`
}

// LaravelConfig 指向 Laravel 控制面。
type LaravelConfig struct {
	BaseURL               string      `yaml:"base_url" json:"base_url"`
	AdminToken            string      `yaml:"admin_token" json:"admin_token"`
	EnrollPath            string      `yaml:"enroll_path" json:"enroll_path"`
	RequestTimeoutSeconds int         `yaml:"request_timeout_seconds" json:"request_timeout_seconds"`
	Retry                 RetryConfig `yaml:"retry" json:"retry"`
}

// PollConfig 桥接的轮询节奏；FallbackOnly=true 时只在 Reverb 未启用/断开时才轮询。
type PollConfig struct {
	ConfigSeconds   int  `yaml:"config_seconds" json:"config_seconds"`
	CommandsSeconds int  `yaml:"commands_seconds" json:"commands_seconds"`
	FallbackOnly    bool `yaml:"fallback_only" json:"fallback_only"`
}

// ReverbConfig 订阅 Laravel 的 private-fleet 事件通道（配置发布/指令下发）。
type ReverbConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Key     string `yaml:"key" json:"key"`
	Host    string `yaml:"host" json:"host"`
	Port    int    `yaml:"port" json:"port"`
	Scheme  string `yaml:"scheme" json:"scheme"`
}

// BridgeConfig 桥接器的行为开关。
type BridgeConfig struct {
	GzipMetrics    bool   `yaml:"gzip_metrics" json:"gzip_metrics"`
	MaxMetricBatch int    `yaml:"max_metric_batch" json:"max_metric_batch"`
	OutboxDir      string `yaml:"outbox_dir" json:"outbox_dir"`
}

// DefaultIdentityConfig 默认保持旧的独立模式（不影响既有部署）。
func DefaultIdentityConfig() IdentityConfig {
	return IdentityConfig{Authority: AuthorityLocal}
}

// DefaultLaravelConfig 。
func DefaultLaravelConfig() LaravelConfig {
	return LaravelConfig{
		EnrollPath:            "/api/v1/agent/enroll",
		RequestTimeoutSeconds: 15,
		Retry:                 RetryConfig{InitialSeconds: 1, MaxSeconds: 300, Multiplier: 2, Jitter: 0.2},
	}
}

// DefaultPollConfig 。
func DefaultPollConfig() PollConfig {
	return PollConfig{ConfigSeconds: 2, CommandsSeconds: 10, FallbackOnly: true}
}

// DefaultReverbConfig 。
func DefaultReverbConfig() ReverbConfig {
	return ReverbConfig{Enabled: false, Port: 443, Scheme: "https"}
}

// DefaultBridgeConfig 。
func DefaultBridgeConfig() BridgeConfig {
	return BridgeConfig{GzipMetrics: true, MaxMetricBatch: 500}
}

// UsesLaravelIdentity 判断是否走 Laravel 身份权威（桥接模式）。
func (c *Config) UsesLaravelIdentity() bool {
	return c != nil && strings.EqualFold(strings.TrimSpace(c.Identity.Authority), AuthorityLaravel)
}

// Normalize 补齐新增配置块的默认值（老配置文件里没有这些键也不会崩）。
func (c *Config) Normalize() {
	if strings.TrimSpace(c.Identity.Authority) == "" {
		c.Identity = DefaultIdentityConfig()
	}

	def := DefaultLaravelConfig()
	if c.Laravel.EnrollPath == "" {
		c.Laravel.EnrollPath = def.EnrollPath
	}
	if c.Laravel.RequestTimeoutSeconds <= 0 {
		c.Laravel.RequestTimeoutSeconds = def.RequestTimeoutSeconds
	}
	if c.Laravel.Retry.InitialSeconds <= 0 {
		c.Laravel.Retry.InitialSeconds = def.Retry.InitialSeconds
	}
	if c.Laravel.Retry.MaxSeconds <= 0 {
		c.Laravel.Retry.MaxSeconds = def.Retry.MaxSeconds
	}
	if c.Laravel.Retry.Multiplier <= 0 {
		c.Laravel.Retry.Multiplier = def.Retry.Multiplier
	}

	if c.Poll.ConfigSeconds <= 0 {
		c.Poll.ConfigSeconds = DefaultPollConfig().ConfigSeconds
	}
	if c.Poll.CommandsSeconds <= 0 {
		c.Poll.CommandsSeconds = DefaultPollConfig().CommandsSeconds
	}

	if c.Reverb.Port <= 0 {
		c.Reverb.Port = DefaultReverbConfig().Port
	}
	if c.Reverb.Scheme == "" {
		c.Reverb.Scheme = DefaultReverbConfig().Scheme
	}

	if c.Bridge.MaxMetricBatch <= 0 {
		c.Bridge.MaxMetricBatch = DefaultBridgeConfig().MaxMetricBatch
	}
	if c.Bridge.OutboxDir == "" {
		c.Bridge.OutboxDir = c.StateDir + "/outbox"
	}
}

// Validate 在启动时校验配置自洽性（桥接模式必须有控制面地址）。
func (c *Config) Validate() error {
	c.Normalize()

	if c.UsesLaravelIdentity() {
		if strings.TrimSpace(c.Laravel.BaseURL) == "" {
			return fmt.Errorf("identity.authority=laravel 时必须配置 laravel.base_url")
		}
		if _, err := parseURL(c.Laravel.BaseURL); err != nil {
			return fmt.Errorf("laravel.base_url 非法: %w", err)
		}
	}

	return nil
}

// Timeout 桥接请求超时。
func (c *Config) LaravelTimeout() time.Duration {
	secs := c.Laravel.RequestTimeoutSeconds
	if secs <= 0 {
		secs = DefaultLaravelConfig().RequestTimeoutSeconds
	}

	return time.Duration(secs) * time.Second
}

// LaravelClientConfig 把控制面配置翻译成 cpapi 客户端配置。
func (c *Config) LaravelClientConfig() cpapi.Config {
	retry := c.Laravel.Retry

	return cpapi.Config{
		BaseURL:   strings.TrimRight(c.Laravel.BaseURL, "/"),
		UserAgent: "hysteria-controlplane-bridge/" + versionString(),
		Timeout:   c.LaravelTimeout(),
		Paths: cpapi.Paths{
			Enroll:        c.Laravel.EnrollPath,
			Heartbeat:     "/api/agent/v1/heartbeat",
			ConfigLatest:  "/api/agent/v1/config/latest",
			ConfigAck:     "/api/agent/v1/config/ack",
			Metrics:       "/api/agent/v1/metrics",
			Logs:          "/api/agent/v1/logs",
			CommandsPoll:  "/api/agent/v1/commands/poll",
			CommandAckFmt: "/api/agent/v1/commands/%s/ack",
			Diagnostics:   "/api/agent/v1/diagnostics",
			UpgradeCheck:  "/api/agent/v1/upgrade/check",
		},
		Retry: cpapi.RetryPolicy{
			Attempts:   3,
			Initial:    time.Duration(retry.InitialSeconds) * time.Second,
			Max:        time.Duration(retry.MaxSeconds) * time.Second,
			Multiplier: retry.Multiplier,
			Jitter:     retry.Jitter,
		},
		Gzip: c.Bridge.GzipMetrics,
	}
}

// ProbeLaravel 启动自检：控制面（Laravel）是否可达。
func (c *Config) ProbeLaravel(ctx context.Context) error {
	if !c.UsesLaravelIdentity() {
		return nil
	}

	url := strings.TrimRight(c.Laravel.BaseURL, "/") + "/api/agent/v1/ping"

	ctx, cancel := context.WithTimeout(ctx, c.LaravelTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "hysteria-controlplane-bridge/"+versionString())

	resp, err := (&http.Client{Timeout: c.LaravelTimeout()}).Do(req)
	if err != nil {
		return fmt.Errorf("探活 %s 失败: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("探活 %s 返回 %d", url, resp.StatusCode)
	}

	return nil
}
