// Package config 定义 hysteria-agent 自身的配置（/etc/hysteria-agent/config.yaml）。
//
// 设计原则：
//   - 所有可选项都有默认值，最小配置只需要 control.server_url；
//   - validate() 在启动时跑，错配置直接启动失败，绝不带着坏配置跑；
//   - 每个键都出现在 deploy/config.yaml 模板与 README 配置表里。
package config

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Reconnect 是退避重连策略（控制通道与 hysteria 子进程共用同一套语义）。
type Reconnect struct {
	InitialSeconds float64 `yaml:"initial_seconds"`
	MaxSeconds     float64 `yaml:"max_seconds"`
	Multiplier     float64 `yaml:"multiplier"`
	Jitter         float64 `yaml:"jitter"` // 0~1，随机抖动比例
}

// Control 控制面连接参数。
type Control struct {
	ServerURL  string `yaml:"server_url"`
	WSPath     string `yaml:"ws_path"`
	EnrollPath string `yaml:"enroll_path"`
	TokenPath  string `yaml:"token_path"` // 可选：用 node_secret 换新 JWT 的端点（本地自签 JWT 时不用）
	// RESTHeartbeatPath：控制通道不可用时的心跳兜底端点。控制面必须能终结
	// /api/v1/agent/control 这个二进制 WS —— Laravel/PHP 终结不了（返回 501），
	// 没有兜底的话节点除注册外与控制面再无任何往来，面板「最后心跳」永远是注册时刻。
	RESTHeartbeatPath  string   `yaml:"rest_heartbeat_path"`
	UserAgent          string   `yaml:"user_agent"`
	CAFile             string   `yaml:"ca_file"`
	PinSHA256          []string `yaml:"pin_sha256"` // 证书 SPKI pin（base64）；空则不做 pinning
	InsecureSkipVerify bool     `yaml:"insecure_skip_verify"`
	// AllowInsecureWS 允许 ws:// 明文（只给本地联调用，默认 false）。
	AllowInsecureWS bool `yaml:"allow_insecure_ws"`

	ConnectTimeoutSeconds    float64   `yaml:"connect_timeout_seconds"`
	HandshakeTimeoutSeconds  float64   `yaml:"handshake_timeout_seconds"`
	WriteTimeoutSeconds      float64   `yaml:"write_timeout_seconds"`
	ReadTimeoutSeconds       float64   `yaml:"read_timeout_seconds"`
	HeartbeatIntervalSeconds float64   `yaml:"heartbeat_interval_seconds"`
	HeartbeatTimeoutSeconds  float64   `yaml:"heartbeat_timeout_seconds"`
	TelemetryIntervalSeconds float64   `yaml:"telemetry_interval_seconds"`
	JWTExpirySeconds         float64   `yaml:"jwt_expiry_seconds"`
	MaxMessageBytes          int64     `yaml:"max_message_bytes"`
	Reconnect                Reconnect `yaml:"reconnect"`
	// TokenRefreshLeewaySeconds：JWT 剩余寿命小于该值时重连前重新签发。
	TokenRefreshLeewaySeconds float64 `yaml:"token_refresh_leeway_seconds"`

	// RESTFallbackSeconds：控制通道握手被拒（404/405/501 = 对端没有实现该端点）时，
	// 退化为 REST 心跳的持续时长；0 关闭。到期后重新尝试控制通道（例如控制面换成了
	// 能终结 WS 的 Go 控制面），所以两端都不会僵死。
	RESTFallbackSeconds float64 `yaml:"rest_fallback_seconds"`
}

// Hysteria 被管理的 Hysteria2 服务端。
type Hysteria struct {
	// Mode: process | systemd | external
	//   process  - Agent 直接拉起/守护 hysteria 子进程（默认，最可控）
	//   systemd  - Hysteria 由 systemd 管理，Agent 只通过 systemctl 白名单动作操作它
	//   external - 只观测，不管理（适用于别人起的实例）
	Mode        string   `yaml:"mode"`
	Binary      string   `yaml:"binary"`
	ConfigPath  string   `yaml:"config_path"`
	ServiceName string   `yaml:"service_name"`
	WorkDir     string   `yaml:"work_dir"`
	Env         []string `yaml:"env"`
	ExtraArgs   []string `yaml:"extra_args"`
	APIBaseURL  string   `yaml:"api_base_url"`
	APISecret   string   `yaml:"api_secret"`
	LogPaths    []string `yaml:"log_paths"`    // tail_logs 时可读的日志文件
	JournalUnit string   `yaml:"journal_unit"` // 非空则 tail_logs 允许 journalctl -u <unit>

	StartupTimeoutSeconds  float64 `yaml:"startup_timeout_seconds"`
	ShutdownTimeoutSeconds float64 `yaml:"shutdown_timeout_seconds"`

	Restart              Reconnect `yaml:"restart"`
	RestartMaxInWindow   int       `yaml:"restart_max_in_window"` // 窗口内最多重启次数，超过则放弃并上报
	RestartWindowSeconds float64   `yaml:"restart_window_seconds"`

	HistoryDir  string `yaml:"history_dir"`
	HistoryKeep int    `yaml:"history_keep"`

	ValidatePort bool `yaml:"validate_port"`
	ValidateCert bool `yaml:"validate_cert"`
	CertWarnDays int  `yaml:"cert_expiry_warn_days"`
}

// Telemetry 指标采集。
type Telemetry struct {
	IntervalSeconds   float64  `yaml:"interval_seconds"`
	DiskPaths         []string `yaml:"disk_paths"`
	NetInterfaces     []string `yaml:"net_interfaces"` // 空 = 所有非 lo 接口
	CollectIntervalMs int      `yaml:"collect_interval_ms"`
	HysteriaEnabled   bool     `yaml:"hysteria_enabled"`
}

// LogSample 远程日志采样（防日志风暴打爆控制面）。
type LogSample struct {
	MaxLinesPerSecond int    `yaml:"max_lines_per_second"`
	Burst             int    `yaml:"burst"`
	MinLevel          string `yaml:"min_level"`
	MaxBatch          int    `yaml:"max_batch"`
	FlushIntervalMs   int    `yaml:"flush_interval_ms"`
	MaxLineBytes      int    `yaml:"max_line_bytes"`
}

// Logs 日志。
type Logs struct {
	Level           string    `yaml:"level"`
	Format          string    `yaml:"format"` // json | text
	RemoteEnabled   bool      `yaml:"remote_enabled"`
	RingSize        int       `yaml:"ring_size"`
	IncludeHysteria bool      `yaml:"include_hysteria"`
	Sample          LogSample `yaml:"sample"`
}

// Metrics 本地 Prometheus 端点 + 健康检查。
type Metrics struct {
	Enabled            bool    `yaml:"enabled"`
	Listen             string  `yaml:"listen"`
	Path               string  `yaml:"path"`
	HealthPath         string  `yaml:"health_path"`
	ReadyPath          string  `yaml:"ready_path"`
	ReadHeaderTimeoutS float64 `yaml:"read_header_timeout_seconds"`
}

// Outbox 断线期间的事件缓冲。
type Outbox struct {
	Path            string `yaml:"path"`
	MaxBytes        int64  `yaml:"max_bytes"`
	MaxEvents       int    `yaml:"max_events"`
	Fsync           bool   `yaml:"fsync"`
	CompactMinDrops int    `yaml:"compact_min_drops"`
}

// Upgrade 自升级。
type Upgrade struct {
	Enabled                bool     `yaml:"enabled"`
	Pubkeys                []string `yaml:"pubkeys"` // 追加信任的 ed25519 公钥（base64）
	BinaryPath             string   `yaml:"binary_path"`
	WorkDir                string   `yaml:"work_dir"`
	AllowDowngrade         bool     `yaml:"allow_downgrade"`
	DownloadTimeoutSeconds float64  `yaml:"download_timeout_seconds"`
	SmokeTestSeconds       float64  `yaml:"smoke_test_seconds"`
	RestartMode            string   `yaml:"restart_mode"` // systemd | exec
	ServiceName            string   `yaml:"service_name"`
}

// Diag 诊断项。
type Diag struct {
	DNSHosts          []string `yaml:"dns_hosts"`
	PublicIPURLs      []string `yaml:"public_ip_urls"`
	PortTimeoutMs     int      `yaml:"port_check_timeout_ms"`
	HTTPTimeoutMs     int      `yaml:"http_timeout_ms"`
	CheckControlPlane bool     `yaml:"check_control_plane"`
}

// TailTailLogs 远程 tail_logs 限制。
type TailLogs struct {
	MaxDurationSeconds float64 `yaml:"max_duration_seconds"` // 默认 300（5 分钟，需求硬上限）
	MaxLineBytes       int     `yaml:"max_line_bytes"`
	MaxChunkBytes      int     `yaml:"max_chunk_bytes"`
}

// Config 是 agent 的完整配置。
type Config struct {
	Control         Control   `yaml:"control"`
	CredentialsFile string    `yaml:"credentials_file"`
	StateDir        string    `yaml:"state_dir"`
	RuntimeDir      string    `yaml:"runtime_dir"`
	Hysteria        Hysteria  `yaml:"hysteria"`
	Telemetry       Telemetry `yaml:"telemetry"`
	Logs            Logs      `yaml:"logs"`
	Metrics         Metrics   `yaml:"metrics"`
	Outbox          Outbox    `yaml:"outbox"`
	Upgrade         Upgrade   `yaml:"upgrade"`
	Diag            Diag      `yaml:"diag"`
	Tail            TailLogs  `yaml:"tail_logs"`

	// Path 记录配置文件本身的位置（不参与 YAML）。
	Path string `yaml:"-"`
}

// Paths 是默认路径常量（install.sh 与 README 必须与之保持一致）。
const (
	DefaultConfigPath  = "/etc/hysteria-agent/config.yaml"
	DefaultCredsPath   = "/etc/hysteria-agent/credentials.json"
	DefaultStateDir    = "/var/lib/hysteria-agent"
	DefaultHysteriaCfg = "/etc/hysteria/config.yaml"
	DefaultHysteriaBin = "/usr/local/bin/hysteria"
	DefaultAgentBin    = "/usr/local/bin/hysteria-agent"
	DefaultServiceName = "hysteria-agent"
	DefaultHysteriaSvc = "hysteria-server"
)

// Default 返回带全部默认值的配置。
func Default() *Config {
	return &Config{
		Control: Control{
			WSPath:                    "/api/v1/agent/control",
			EnrollPath:                "/api/v1/agent/enroll",
			TokenPath:                 "/api/v1/agent/token",
			RESTHeartbeatPath:         "/api/v1/agent/heartbeat",
			UserAgent:                 "hysteria-agent",
			ConnectTimeoutSeconds:     15,
			HandshakeTimeoutSeconds:   20,
			WriteTimeoutSeconds:       15,
			ReadTimeoutSeconds:        120,
			HeartbeatIntervalSeconds:  30,
			HeartbeatTimeoutSeconds:   90,
			TelemetryIntervalSeconds:  15,
			JWTExpirySeconds:          3600,
			MaxMessageBytes:           4 << 20,
			TokenRefreshLeewaySeconds: 300,
			RESTFallbackSeconds:       300,
			Reconnect:                 Reconnect{InitialSeconds: 1, MaxSeconds: 300, Multiplier: 2, Jitter: 0.2},
		},
		CredentialsFile: DefaultCredsPath,
		StateDir:        DefaultStateDir,
		RuntimeDir:      "/run/hysteria-agent",
		Hysteria: Hysteria{
			Mode:                   "process",
			Binary:                 DefaultHysteriaBin,
			ConfigPath:             DefaultHysteriaCfg,
			ServiceName:            DefaultHysteriaSvc,
			APIBaseURL:             "http://127.0.0.1:8080",
			StartupTimeoutSeconds:  20,
			ShutdownTimeoutSeconds: 15,
			Restart:                Reconnect{InitialSeconds: 1, MaxSeconds: 60, Multiplier: 2, Jitter: 0.2},
			RestartMaxInWindow:     10,
			RestartWindowSeconds:   300,
			HistoryDir:             "/etc/hysteria-agent/history",
			HistoryKeep:            5,
			ValidatePort:           true,
			ValidateCert:           true,
			CertWarnDays:           14,
			JournalUnit:            DefaultHysteriaSvc,
		},
		Telemetry: Telemetry{
			IntervalSeconds: 15,
			DiskPaths:       []string{"/", DefaultStateDir},
			HysteriaEnabled: true,
		},
		Logs: Logs{
			Level:           "info",
			Format:          "json",
			RemoteEnabled:   true,
			RingSize:        2000,
			IncludeHysteria: true,
			Sample: LogSample{
				MaxLinesPerSecond: 200,
				Burst:             500,
				MinLevel:          "info",
				MaxBatch:          100,
				FlushIntervalMs:   500,
				MaxLineBytes:      8192,
			},
		},
		Metrics: Metrics{
			Enabled:            true,
			Listen:             "127.0.0.1:9100",
			Path:               "/metrics",
			HealthPath:         "/healthz",
			ReadyPath:          "/readyz",
			ReadHeaderTimeoutS: 5,
		},
		Outbox: Outbox{
			Path:            filepath.Join(DefaultStateDir, "outbox.jsonl"),
			MaxBytes:        32 << 20,
			MaxEvents:       20000,
			Fsync:           true,
			CompactMinDrops: 64,
		},
		Upgrade: Upgrade{
			Enabled:                true,
			BinaryPath:             DefaultAgentBin,
			WorkDir:                filepath.Join(DefaultStateDir, "upgrade"),
			DownloadTimeoutSeconds: 120,
			SmokeTestSeconds:       10,
			RestartMode:            "systemd",
			ServiceName:            DefaultServiceName,
		},
		Diag: Diag{
			PublicIPURLs:      []string{"https://api.ipify.org"},
			PortTimeoutMs:     800,
			HTTPTimeoutMs:     5000,
			CheckControlPlane: true,
		},
		Tail: TailLogs{MaxDurationSeconds: 300, MaxLineBytes: 8192, MaxChunkBytes: 16 << 10},
	}
}

// Load 读取配置文件并补齐默认值。文件不存在时返回错误（调用方决定是要求创建还是用默认值）。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := Default()
	c.Path = path
	// KnownFields(true) 让拼错的键直接报错，而不是被静默忽略。
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("解析 %s 失败: %w", path, err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// applyDefaults 只处理"零值等价于未设置"的字段。
func (c *Config) applyDefaults() {
	d := Default()
	if c.Control.WSPath == "" {
		c.Control.WSPath = d.Control.WSPath
	}
	if c.Control.EnrollPath == "" {
		c.Control.EnrollPath = d.Control.EnrollPath
	}
	if c.Control.TokenPath == "" {
		c.Control.TokenPath = d.Control.TokenPath
	}
	if c.Control.UserAgent == "" {
		c.Control.UserAgent = d.Control.UserAgent
	}
	if c.Control.ConnectTimeoutSeconds <= 0 {
		c.Control.ConnectTimeoutSeconds = d.Control.ConnectTimeoutSeconds
	}
	if c.Control.HandshakeTimeoutSeconds <= 0 {
		c.Control.HandshakeTimeoutSeconds = d.Control.HandshakeTimeoutSeconds
	}
	if c.Control.WriteTimeoutSeconds <= 0 {
		c.Control.WriteTimeoutSeconds = d.Control.WriteTimeoutSeconds
	}
	if c.Control.ReadTimeoutSeconds <= 0 {
		c.Control.ReadTimeoutSeconds = d.Control.ReadTimeoutSeconds
	}
	if c.Control.MaxMessageBytes <= 0 {
		c.Control.MaxMessageBytes = d.Control.MaxMessageBytes
	}
	if c.Control.JWTExpirySeconds <= 0 {
		c.Control.JWTExpirySeconds = d.Control.JWTExpirySeconds
	}
	// 心跳/遥测间隔可以由控制面在 HelloAck 里覆盖，这里的值只是本地默认与上限参考。
	if c.Control.HeartbeatIntervalSeconds <= 0 {
		c.Control.HeartbeatIntervalSeconds = d.Control.HeartbeatIntervalSeconds
	}
	if c.Control.HeartbeatTimeoutSeconds <= 0 {
		c.Control.HeartbeatTimeoutSeconds = d.Control.HeartbeatTimeoutSeconds
	}
	if c.Control.TelemetryIntervalSeconds <= 0 {
		c.Control.TelemetryIntervalSeconds = d.Control.TelemetryIntervalSeconds
	}
	if c.Control.TokenRefreshLeewaySeconds <= 0 {
		c.Control.TokenRefreshLeewaySeconds = d.Control.TokenRefreshLeewaySeconds
	}
	if c.Control.Reconnect.InitialSeconds <= 0 {
		c.Control.Reconnect.InitialSeconds = d.Control.Reconnect.InitialSeconds
	}
	if c.Control.Reconnect.MaxSeconds <= 0 {
		c.Control.Reconnect.MaxSeconds = d.Control.Reconnect.MaxSeconds
	}
	if c.Control.Reconnect.Multiplier <= 0 {
		c.Control.Reconnect.Multiplier = d.Control.Reconnect.Multiplier
	}

	if c.CredentialsFile == "" {
		c.CredentialsFile = d.CredentialsFile
	}
	if c.StateDir == "" {
		c.StateDir = d.StateDir
	}
	if c.RuntimeDir == "" {
		c.RuntimeDir = d.RuntimeDir
	}

	if c.Hysteria.Mode == "" {
		c.Hysteria.Mode = d.Hysteria.Mode
	}
	if c.Hysteria.Binary == "" {
		c.Hysteria.Binary = d.Hysteria.Binary
	}
	if c.Hysteria.ConfigPath == "" {
		c.Hysteria.ConfigPath = d.Hysteria.ConfigPath
	}
	if c.Hysteria.ServiceName == "" {
		c.Hysteria.ServiceName = d.Hysteria.ServiceName
	}
	if c.Hysteria.APIBaseURL == "" {
		c.Hysteria.APIBaseURL = d.Hysteria.APIBaseURL
	}
	if c.Hysteria.StartupTimeoutSeconds <= 0 {
		c.Hysteria.StartupTimeoutSeconds = d.Hysteria.StartupTimeoutSeconds
	}
	if c.Hysteria.ShutdownTimeoutSeconds <= 0 {
		c.Hysteria.ShutdownTimeoutSeconds = d.Hysteria.ShutdownTimeoutSeconds
	}
	if c.Hysteria.Restart.InitialSeconds <= 0 {
		c.Hysteria.Restart.InitialSeconds = d.Hysteria.Restart.InitialSeconds
	}
	if c.Hysteria.Restart.MaxSeconds <= 0 {
		c.Hysteria.Restart.MaxSeconds = d.Hysteria.Restart.MaxSeconds
	}
	if c.Hysteria.Restart.Multiplier <= 0 {
		c.Hysteria.Restart.Multiplier = d.Hysteria.Restart.Multiplier
	}
	if c.Hysteria.RestartMaxInWindow <= 0 {
		c.Hysteria.RestartMaxInWindow = d.Hysteria.RestartMaxInWindow
	}
	if c.Hysteria.RestartWindowSeconds <= 0 {
		c.Hysteria.RestartWindowSeconds = d.Hysteria.RestartWindowSeconds
	}
	if c.Hysteria.HistoryDir == "" {
		c.Hysteria.HistoryDir = d.Hysteria.HistoryDir
	}
	if c.Hysteria.HistoryKeep <= 0 {
		c.Hysteria.HistoryKeep = d.Hysteria.HistoryKeep
	}
	if c.Hysteria.CertWarnDays <= 0 {
		c.Hysteria.CertWarnDays = d.Hysteria.CertWarnDays
	}
	if c.Hysteria.JournalUnit == "" && c.Hysteria.Mode == "systemd" {
		c.Hysteria.JournalUnit = c.Hysteria.ServiceName
	}

	if c.Telemetry.IntervalSeconds <= 0 {
		c.Telemetry.IntervalSeconds = d.Telemetry.IntervalSeconds
	}
	if len(c.Telemetry.DiskPaths) == 0 {
		c.Telemetry.DiskPaths = d.Telemetry.DiskPaths
	}

	if c.Logs.Level == "" {
		c.Logs.Level = d.Logs.Level
	}
	if c.Logs.Format == "" {
		c.Logs.Format = d.Logs.Format
	}
	if c.Logs.RingSize <= 0 {
		c.Logs.RingSize = d.Logs.RingSize
	}
	if c.Logs.Sample.MaxLinesPerSecond <= 0 {
		c.Logs.Sample.MaxLinesPerSecond = d.Logs.Sample.MaxLinesPerSecond
	}
	if c.Logs.Sample.Burst <= 0 {
		c.Logs.Sample.Burst = d.Logs.Sample.Burst
	}
	if c.Logs.Sample.MinLevel == "" {
		c.Logs.Sample.MinLevel = d.Logs.Sample.MinLevel
	}
	if c.Logs.Sample.MaxBatch <= 0 {
		c.Logs.Sample.MaxBatch = d.Logs.Sample.MaxBatch
	}
	if c.Logs.Sample.FlushIntervalMs <= 0 {
		c.Logs.Sample.FlushIntervalMs = d.Logs.Sample.FlushIntervalMs
	}
	if c.Logs.Sample.MaxLineBytes <= 0 {
		c.Logs.Sample.MaxLineBytes = d.Logs.Sample.MaxLineBytes
	}

	if c.Metrics.Listen == "" {
		c.Metrics.Listen = d.Metrics.Listen
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = d.Metrics.Path
	}
	if c.Metrics.HealthPath == "" {
		c.Metrics.HealthPath = d.Metrics.HealthPath
	}
	if c.Metrics.ReadyPath == "" {
		c.Metrics.ReadyPath = d.Metrics.ReadyPath
	}
	if c.Metrics.ReadHeaderTimeoutS <= 0 {
		c.Metrics.ReadHeaderTimeoutS = d.Metrics.ReadHeaderTimeoutS
	}

	if c.Outbox.Path == "" {
		c.Outbox.Path = d.Outbox.Path
	}
	if c.Outbox.MaxBytes <= 0 {
		c.Outbox.MaxBytes = d.Outbox.MaxBytes
	}
	if c.Outbox.MaxEvents <= 0 {
		c.Outbox.MaxEvents = d.Outbox.MaxEvents
	}
	if c.Outbox.CompactMinDrops <= 0 {
		c.Outbox.CompactMinDrops = d.Outbox.CompactMinDrops
	}

	if c.Upgrade.BinaryPath == "" {
		c.Upgrade.BinaryPath = d.Upgrade.BinaryPath
	}
	if c.Upgrade.WorkDir == "" {
		c.Upgrade.WorkDir = d.Upgrade.WorkDir
	}
	if c.Upgrade.DownloadTimeoutSeconds <= 0 {
		c.Upgrade.DownloadTimeoutSeconds = d.Upgrade.DownloadTimeoutSeconds
	}
	if c.Upgrade.SmokeTestSeconds <= 0 {
		c.Upgrade.SmokeTestSeconds = d.Upgrade.SmokeTestSeconds
	}
	if c.Upgrade.RestartMode == "" {
		c.Upgrade.RestartMode = d.Upgrade.RestartMode
	}
	if c.Upgrade.ServiceName == "" {
		c.Upgrade.ServiceName = d.Upgrade.ServiceName
	}

	if c.Diag.PortTimeoutMs <= 0 {
		c.Diag.PortTimeoutMs = d.Diag.PortTimeoutMs
	}
	if c.Diag.HTTPTimeoutMs <= 0 {
		c.Diag.HTTPTimeoutMs = d.Diag.HTTPTimeoutMs
	}
	if c.Tail.MaxDurationSeconds <= 0 {
		c.Tail.MaxDurationSeconds = d.Tail.MaxDurationSeconds
	}
	if c.Tail.MaxLineBytes <= 0 {
		c.Tail.MaxLineBytes = d.Tail.MaxLineBytes
	}
	if c.Tail.MaxChunkBytes <= 0 {
		c.Tail.MaxChunkBytes = d.Tail.MaxChunkBytes
	}
}

var validLevels = map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
var validModes = map[string]bool{"process": true, "systemd": true, "external": true}
var validRestartModes = map[string]bool{"systemd": true, "exec": true}

// Validate 校验配置；任何一条不通过都返回错误（启动即失败）。
func (c *Config) Validate() error {
	if c.Control.ServerURL == "" {
		return fmt.Errorf("control.server_url 必填，例如 https://cp.example.com")
	}
	u, err := url.Parse(c.Control.ServerURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("control.server_url 必须是带 scheme 的绝对地址: %q", c.Control.ServerURL)
	}
	switch u.Scheme {
	case "https", "wss":
	case "http", "ws":
		if !c.Control.AllowInsecureWS {
			return fmt.Errorf("control.server_url 使用明文 %s://，如确认要这么做请显式设置 control.allow_insecure_ws: true", u.Scheme)
		}
	default:
		return fmt.Errorf("control.server_url 不支持的 scheme: %s", u.Scheme)
	}
	if !strings.HasPrefix(c.Control.WSPath, "/") {
		return fmt.Errorf("control.ws_path 必须以 / 开头: %q", c.Control.WSPath)
	}
	if !validModes[c.Hysteria.Mode] {
		return fmt.Errorf("hysteria.mode 只能是 process|systemd|external，得到 %q", c.Hysteria.Mode)
	}
	if !validRestartModes[c.Upgrade.RestartMode] {
		return fmt.Errorf("upgrade.restart_mode 只能是 systemd|exec，得到 %q", c.Upgrade.RestartMode)
	}
	if !validLevels[c.Logs.Level] {
		return fmt.Errorf("logs.level 只能是 debug|info|warn|error，得到 %q", c.Logs.Level)
	}
	if c.Logs.Format != "json" && c.Logs.Format != "text" {
		return fmt.Errorf("logs.format 只能是 json|text，得到 %q", c.Logs.Format)
	}
	if !validLevels[c.Logs.Sample.MinLevel] {
		return fmt.Errorf("logs.sample.min_level 只能是 debug|info|warn|error，得到 %q", c.Logs.Sample.MinLevel)
	}
	if c.Control.HeartbeatTimeoutSeconds <= c.Control.HeartbeatIntervalSeconds {
		return fmt.Errorf("control.heartbeat_timeout_seconds(%v) 必须大于 heartbeat_interval_seconds(%v)，否则会误判断线",
			c.Control.HeartbeatTimeoutSeconds, c.Control.HeartbeatIntervalSeconds)
	}
	if c.Upgrade.Enabled && len(c.Upgrade.Pubkeys) == 0 && c.UpgradePublicKeyEmpty() {
		return fmt.Errorf("upgrade.enabled 为 true 但没有可用验签公钥：设置 upgrade.pubkeys 或用 -ldflags 注入 version.UpgradePublicKey")
	}
	for _, p := range c.Control.PinSHA256 {
		if _, err := DecodePin(p); err != nil {
			return fmt.Errorf("control.pin_sha256 里的 %q 不是合法的 base64 SPKI 哈希: %w", p, err)
		}
	}
	return nil
}

// UpgradePublicKeyEmpty 由 version 包注入，避免 config -> version 的循环依赖。
var upgradePubKeyEmptyFn = func() bool { return true }

// DecodePin 校验 base64 SPKI pin 的合法性（只校验格式，实际比对在 creds 包）。
func DecodePin(p string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(p))
	if err != nil {
		return nil, err
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("SPKI SHA-256 pin 应为 32 字节，得到 %d", len(b))
	}
	return b, nil
}

// SetUpgradePublicKeyProbe 允许 main 包把 version.UpgradePublicKey 的存在性告诉 config 包。
func SetUpgradePublicKeyProbe(fn func() bool) { upgradePubKeyEmptyFn = fn }

// UpgradePublicKeyEmpty 返回内置公钥是否为空。
func (c *Config) UpgradePublicKeyEmpty() bool { return upgradePubKeyEmptyFn() }

// ---------------------------- 便捷方法 ----------------------------

func seconds(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }

func (c *Control) HeartbeatInterval() time.Duration {
	return seconds(c.HeartbeatIntervalSeconds)
}
func (c *Control) HeartbeatTimeout() time.Duration   { return seconds(c.HeartbeatTimeoutSeconds) }
func (c *Control) TelemetryInterval() time.Duration  { return seconds(c.TelemetryIntervalSeconds) }
func (c *Control) ConnectTimeout() time.Duration     { return seconds(c.ConnectTimeoutSeconds) }
func (c *Control) HandshakeTimeout() time.Duration   { return seconds(c.HandshakeTimeoutSeconds) }
func (c *Control) WriteTimeout() time.Duration       { return seconds(c.WriteTimeoutSeconds) }
func (c *Control) ReadTimeout() time.Duration        { return seconds(c.ReadTimeoutSeconds) }
func (c *Control) JWTLifetime() time.Duration        { return seconds(c.JWTExpirySeconds) }
func (c *Control) TokenRefreshLeeway() time.Duration { return seconds(c.TokenRefreshLeewaySeconds) }

func (h *Hysteria) StartupTimeout() time.Duration  { return seconds(h.StartupTimeoutSeconds) }
func (h *Hysteria) ShutdownTimeout() time.Duration { return seconds(h.ShutdownTimeoutSeconds) }

func (l *Logs) SampleFlushInterval() time.Duration {
	return time.Duration(l.Sample.FlushIntervalMs) * time.Millisecond
}

func (d *Diag) PortTimeout() time.Duration { return time.Duration(d.PortTimeoutMs) * time.Millisecond }
func (d *Diag) HTTPTimeout() time.Duration { return time.Duration(d.HTTPTimeoutMs) * time.Millisecond }

// ControlWSURL 把 server_url + ws_path 拼成 WebSocket 地址（https→wss）。
func (c *Config) ControlWSURL() string {
	base := strings.TrimRight(c.Control.ServerURL, "/")
	base = strings.Replace(base, "https://", "wss://", 1)
	base = strings.Replace(base, "http://", "ws://", 1)
	return base + c.Control.WSPath
}

// ControlHTTPURL 把 enroll/token 这类 REST 路径拼成 HTTP 地址。
func (c *Config) ControlHTTPURL(path string) string {
	base := strings.TrimRight(c.Control.ServerURL, "/")
	base = strings.Replace(base, "wss://", "https://", 1)
	base = strings.Replace(base, "ws://", "http://", 1)
	return base + path
}

// EnsureDirs 创建 agent 需要的目录，权限按需求收紧。
func (c *Config) EnsureDirs() error {
	dirs := []struct {
		path string
		mode os.FileMode
	}{
		{filepath.Dir(c.CredentialsFile), 0o700},
		{c.StateDir, 0o750},
		{filepath.Dir(c.Outbox.Path), 0o750},
		{c.Hysteria.HistoryDir, 0o700},
		{c.Upgrade.WorkDir, 0o700},
		{c.RuntimeDir, 0o755},
	}
	for _, d := range dirs {
		if d.path == "" || d.path == "." {
			continue
		}
		if err := os.MkdirAll(d.path, d.mode); err != nil {
			return fmt.Errorf("创建目录 %s 失败: %w", d.path, err)
		}
	}
	return nil
}
