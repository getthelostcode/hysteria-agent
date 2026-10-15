package cpapi

// 下面的结构体与 Laravel 控制面的 JSON 字段一一对应
// （契约发布方是 Laravel：docs/API.md / OpenAPI /docs/api.json）。

// ---- 注册（/api/v1/agent/enroll，字段与 agent 的 enroll 契约同形） ----

type EnrollRequest struct {
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
	Protocol        string `json:"protocol,omitempty"`
	Replace         bool   `json:"replace,omitempty"`
	// PublicIP：节点自报的出口公网地址。控制面只能看到「对端代理」的地址
	// （Cloudflare Tunnel/同机 cloudflared → 127.0.0.1），只有节点自己知道公网出口。
	PublicIP string `json:"public_ip,omitempty"`
}

type EnrollResponse struct {
	NodeID                   string   `json:"node_id"`
	NodeSecret               string   `json:"node_secret"`
	NodeName                 string   `json:"node_name,omitempty"`
	ControlURL               string   `json:"control_url,omitempty"`
	WSPath                   string   `json:"ws_path,omitempty"`
	PinSHA256                []string `json:"pin_sha256,omitempty"`
	JWTIssuer                string   `json:"jwt_issuer,omitempty"`
	JWTAudience              string   `json:"jwt_audience,omitempty"`
	HeartbeatIntervalSeconds int      `json:"heartbeat_interval_seconds,omitempty"`
	TelemetryIntervalSeconds int      `json:"telemetry_interval_seconds,omitempty"`
	ConfigRevision           string   `json:"config_revision,omitempty"`
	Message                  string   `json:"message,omitempty"`
}

// ---- 心跳 ----

type HeartbeatRequest struct {
	Version      string            `json:"version,omitempty"`
	AgentVersion string            `json:"agent_version,omitempty"`
	Uptime       int64             `json:"uptime,omitempty"`
	IP           string            `json:"ip,omitempty"`
	PublicIP     string            `json:"public_ip,omitempty"`
	Region       string            `json:"region,omitempty"`
	Tags         []string          `json:"tags,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
}

type ConfigState struct {
	Version int    `json:"version"`
	Sha256  string `json:"sha256"`
	Pending bool   `json:"pending"`
}

type HeartbeatResponse struct {
	OK                bool        `json:"ok"`
	NodeID            string      `json:"node_id"`
	ServerTime        string      `json:"server_time"`
	HeartbeatInterval int         `json:"heartbeat_interval"`
	Config            ConfigState `json:"config"`
	Commands          struct {
		Pending int `json:"pending"`
	} `json:"commands"`
}

// ---- 配置 ----

type ConfigPayload struct {
	Version     int    `json:"version"`
	Sha256      string `json:"sha256"`
	Status      string `json:"status,omitempty"`
	ContentYAML string `json:"content_yaml,omitempty"`
	PublishedAt string `json:"published_at,omitempty"`
}

type ConfigLatestResponse struct {
	OK       bool           `json:"ok"`
	UpToDate bool           `json:"up_to_date"`
	Config   *ConfigPayload `json:"config"`
}

type ConfigAckResponse struct {
	OK            bool   `json:"ok"`
	Version       int    `json:"version"`
	Status        string `json:"status"`
	RolledBack    bool   `json:"rolled_back"`
	ActiveVersion int    `json:"active_version"`
}

// ---- 指标 / 日志 / 诊断 ----

type MetricSample struct {
	CPU        *float64       `json:"cpu,omitempty"`
	Mem        *float64       `json:"mem,omitempty"`
	Disk       *float64       `json:"disk,omitempty"`
	NetRx      *int64         `json:"net_rx,omitempty"`
	NetTx      *int64         `json:"net_tx,omitempty"`
	ConnCount  *int64         `json:"conn_count,omitempty"`
	Users      *int64         `json:"users,omitempty"`
	RecordedAt string         `json:"recorded_at,omitempty"`
	Extra      map[string]any `json:"extra,omitempty"`
}

type LogEntry struct {
	Level      string         `json:"level"`
	Message    string         `json:"message"`
	Context    map[string]any `json:"context,omitempty"`
	RecordedAt string         `json:"recorded_at,omitempty"`
}

type IngestResponse struct {
	OK         bool   `json:"ok"`
	Stored     int    `json:"stored"`
	ReceivedAt string `json:"received_at"`
}

// ---- 指令 ----

type Command struct {
	CommandID string         `json:"command_id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload,omitempty"`
	IssuedAt  string         `json:"issued_at"`
	ExpiresAt string         `json:"expires_at"`
}

type CommandsPollResponse struct {
	OK         bool      `json:"ok"`
	Commands   []Command `json:"commands"`
	ServerTime string    `json:"server_time"`
}

type CommandAckResponse struct {
	OK        bool   `json:"ok"`
	CommandID string `json:"command_id"`
	Status    string `json:"status"`
}

// ---- 升级 ----

type UpgradePackage struct {
	Version    string `json:"version"`
	Arch       string `json:"arch"`
	URL        string `json:"url"`
	Sha256     string `json:"sha256"`
	Signature  string `json:"signature,omitempty"`
	Channel    string `json:"channel,omitempty"`
	ReleasedAt string `json:"released_at,omitempty"`
}

type UpgradeCheckResponse struct {
	OK              bool            `json:"ok"`
	UpdateAvailable bool            `json:"update_available"`
	CurrentVersion  string          `json:"current_version"`
	Package         *UpgradePackage `json:"package,omitempty"`
}

// ---- 错误 ----

type ErrorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
