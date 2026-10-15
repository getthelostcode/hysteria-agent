// Package cp 实现 hysteria-agent 的控制面（Control Plane）。
//
// 面向 Agent 的接口（与 DESIGN.md §5.1 一致）：
//
//	POST /api/v1/agent/enroll    一次性 enroll_token 换取 node_id + node_secret
//	GET  /api/v1/agent/control   WebSocket 控制通道（Authorization: Bearer <本机自签 JWT>）
//
// 面向运维的接口（Bearer <admin_token>）：/admin/nodes、/admin/tokens、配置下发、指令下发……
//
// 状态存盘：state_dir/state.json（0600）+ state_dir/events/<node>.jsonl（0600，追加式事件日志）。
// node_secret 不落盘：用主密钥按 node_id 派生（HMAC-SHA256），因此删掉节点记录即等于吊销。
package cp

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Config 是控制面自身配置（YAML）。
type Config struct {
	Listen     string    `yaml:"listen" json:"listen"`
	StateDir   string    `yaml:"state_dir" json:"state_dir"`
	AdminToken string    `yaml:"admin_token" json:"admin_token"`
	MasterKey  string    `yaml:"master_key" json:"master_key"` // 32 字节 hex；留空则首次启动生成
	PublicURL  string    `yaml:"public_url" json:"public_url"` // 下发给 Agent 的控制面地址（例如 https://node.example.com）
	EnrollPath string    `yaml:"enroll_path" json:"enroll_path"`
	WSPath     string    `yaml:"ws_path" json:"ws_path"`
	TLS        TLSConfig `yaml:"tls" json:"tls"`
	Heartbeat  HBConfig  `yaml:"heartbeat" json:"heartbeat"`
	PinSHA256  []string  `yaml:"pin_sha256" json:"pin_sha256"`   // 可选：要求 Agent 做证书 pin
	CACertPEM  string    `yaml:"ca_cert_pem" json:"ca_cert_pem"` // 可选：下发给 Agent 的私有 CA
	LogLevel   string    `yaml:"log_level" json:"log_level"`
	Retention  Retention `yaml:"retention" json:"retention"`
	ServeDash  *bool     `yaml:"dashboard" json:"dashboard"`

	// 桥接配置（见 identity.go 与 docs/BRIDGE-TO-CONTROL-PLANE.md）：
	// Identity 决定身份权威，Laravel 指向面板侧控制面，Poll/Reverb/Bridge 控制桥接行为。
	Identity IdentityConfig `yaml:"identity" json:"identity"`
	Laravel  LaravelConfig  `yaml:"laravel" json:"laravel"`
	Poll     PollConfig     `yaml:"poll" json:"poll"`
	Reverb   ReverbConfig   `yaml:"reverb" json:"reverb"`
	Bridge   BridgeConfig   `yaml:"bridge" json:"bridge"`
}

// TLSConfig 控制面自身的监听 TLS（放在 Cloudflare Tunnel / nginx 后面时可留空）。
type TLSConfig struct {
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
}

// HBConfig 心跳与在线判定。
type HBConfig struct {
	IntervalSeconds  int `yaml:"interval_seconds" json:"interval_seconds"`
	TimeoutSeconds   int `yaml:"timeout_seconds" json:"timeout_seconds"`
	TelemetrySeconds int `yaml:"telemetry_seconds" json:"telemetry_seconds"`
}

// Retention 限制事件日志增长速度。
type Retention struct {
	MaxEventsPerNode int `yaml:"max_events_per_node" json:"max_events_per_node"`
}

// DefaultConfig 给出一份可直接用的配置。
func DefaultConfig() *Config {
	return &Config{
		Listen:     "127.0.0.1:8443",
		StateDir:   "/var/lib/hysteria-controlplane",
		EnrollPath: "/api/v1/agent/enroll",
		WSPath:     "/api/v1/agent/control",
		Heartbeat:  HBConfig{IntervalSeconds: 30, TimeoutSeconds: 90, TelemetrySeconds: 15},
		Retention:  Retention{MaxEventsPerNode: 20000},
		// 默认保持独立模式：老部署升级后行为不变；要接 Laravel 面板再改 identity.authority。
		Identity: DefaultIdentityConfig(),
		Laravel:  DefaultLaravelConfig(),
		Poll:     DefaultPollConfig(),
		Reverb:   DefaultReverbConfig(),
		Bridge:   DefaultBridgeConfig(),
	}
}

// Node 是一个已注册节点。
type Node struct {
	ID       string `json:"node_id"`
	Name     string `json:"name"`
	Hostname string `json:"hostname,omitempty"`
	OS       string `json:"os,omitempty"`
	Arch     string `json:"arch,omitempty"`
	Kernel   string `json:"kernel,omitempty"`
	BootID   string `json:"boot_id,omitempty"`

	AgentVersion    string   `json:"agent_version,omitempty"`
	HysteriaVersion string   `json:"hysteria_version,omitempty"`
	Caps            []string `json:"capabilities,omitempty"`

	EnrolledAt       int64  `json:"enrolled_at_ms"`
	EnrollTokenName  string `json:"enroll_token_name,omitempty"`
	LastSeenMs       int64  `json:"last_seen_ms,omitempty"`
	LastConnectedMs  int64  `json:"last_connected_ms,omitempty"`
	LastDisconnectMs int64  `json:"last_disconnected_ms,omitempty"`
	Connected        bool   `json:"connected"`
	ConnectedSinceMs int64  `json:"connected_since_ms,omitempty"`
	Connects         int    `json:"connects"`
	Revoked          bool   `json:"revoked,omitempty"`
	Note             string `json:"note,omitempty"`

	// Secret 只在 identity.authority=laravel 时存在：Laravel 下发的长期密钥，
	// 本控制面用它替该节点签 REST（state.json 0600，与 Laravel 侧 secret 同值）。
	Secret string `json:"secret,omitempty"`
	// Authority 记录身份来源：local（master_key 派生）或 laravel（控制面签发）。
	Authority string `json:"identity_authority,omitempty"`

	AckedSeq       uint64 `json:"acked_seq"`
	DuplicateMsgs  uint64 `json:"duplicate_msgs"`
	ReceivedMsgs   uint64 `json:"received_msgs"`
	ResentRequests uint64 `json:"resend_requests"`

	// 最后一次心跳携带的运行时信息
	LastHeartbeat *HeartbeatInfo `json:"last_heartbeat,omitempty"`

	// 配置版本状态
	ConfigRevision string `json:"config_revision,omitempty"`
	ConfigSHA256   string `json:"config_sha256,omitempty"`
	ConfigState    string `json:"config_state,omitempty"` // pending|applied|failed|rolled_back
	ConfigError    string `json:"config_error,omitempty"`

	// 最近一次遥测
	LastMetrics *MetricsInfo `json:"last_metrics,omitempty"`
}

// HeartbeatInfo 是心跳里带来的运行时摘要。
type HeartbeatInfo struct {
	TSMs           int64  `json:"ts_ms"`
	UptimeSeconds  uint64 `json:"uptime_seconds"`
	PendingEvents  uint64 `json:"pending_events"`
	HysteriaRun    bool   `json:"hysteria_running"`
	Ready          bool   `json:"ready"`
	ConfigRevision string `json:"config_revision,omitempty"`
	LastAckedSeq   uint64 `json:"last_acked_seq"`
	SentEvents     uint64 `json:"sent_events"`
}

// MetricsInfo 是最近一次遥测的摘要（保留少量关键字段，原始批量不入库）。
type MetricsInfo struct {
	TSMs        int64             `json:"ts_ms"`
	CPUPct      float64           `json:"cpu_percent,omitempty"`
	MemUsedPct  float64           `json:"mem_used_percent,omitempty"`
	DiskUsedPct float64           `json:"disk_used_percent,omitempty"`
	NetRxBytes  uint64            `json:"net_rx_bytes,omitempty"`
	NetTxBytes  uint64            `json:"net_tx_bytes,omitempty"`
	HyOnline    int64             `json:"hysteria_online,omitempty"`
	HyTx        int64             `json:"hysteria_tx,omitempty"`
	HyRx        int64             `json:"hysteria_rx,omitempty"`
	Extra       map[string]string `json:"extra,omitempty"`
}

// EnrollToken 是一次性（或可复用）注册令牌。
type EnrollToken struct {
	Token     string   `json:"token"`
	Name      string   `json:"name"`
	CreatedMs int64    `json:"created_at_ms"`
	ExpiresMs int64    `json:"expires_at_ms,omitempty"`
	MaxUses   int      `json:"max_uses"`
	Uses      int      `json:"uses"`
	UsedBy    []string `json:"used_by,omitempty"`
	Revoked   bool     `json:"revoked,omitempty"`
}

// Usable 判断令牌当前是否可用。
func (t *EnrollToken) Usable(now int64) error {
	switch {
	case t.Revoked:
		return fmt.Errorf("令牌已吊销")
	case t.ExpiresMs > 0 && now > t.ExpiresMs:
		return fmt.Errorf("令牌已过期")
	case t.MaxUses > 0 && t.Uses >= t.MaxUses:
		return fmt.Errorf("令牌已用尽（%d/%d）", t.Uses, t.MaxUses)
	}
	return nil
}

// PendingPush 记录一次已下发但尚未被 ack 的配置。
type PendingPush struct {
	Revision   string `json:"revision"`
	SHA256     string `json:"sha256"`
	Format     string `json:"format"`
	Payload    []byte `json:"payload"`
	Reason     string `json:"reason,omitempty"`
	DryRun     bool   `json:"dry_run"`
	PushedMs   int64  `json:"pushed_at_ms"`
	Attempts   int    `json:"attempts"`
	LastSentMs int64  `json:"last_sent_ms"`
}

// State 是可持久化的全部状态。
type State struct {
	Version   int                     `json:"version"`
	Nodes     map[string]*Node        `json:"nodes"`
	Tokens    map[string]*EnrollToken `json:"tokens"`
	Pending   map[string]*PendingPush `json:"pending"`
	RevSeq    uint64                  `json:"rev_seq"`
	CreatedMs int64                   `json:"created_at_ms"`
	UpdatedMs int64                   `json:"updated_at_ms"`
}

// Store 是带锁的持久化状态。
type Store struct {
	mu        sync.Mutex
	stateDir  string
	statePath string
	masterKey []byte
	state     *State
	dirty     bool
	maxEvents int
}

// OpenStore 打开（必要时初始化）状态目录。
func OpenStore(stateDir string, masterKey []byte) (*Store, error) {
	if stateDir == "" {
		return nil, fmt.Errorf("state_dir 不能为空")
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "events"), 0o700); err != nil {
		return nil, fmt.Errorf("创建 state_dir 失败: %w", err)
	}
	s := &Store{
		stateDir:  stateDir,
		statePath: filepath.Join(stateDir, "state.json"),
		masterKey: masterKey,
	}
	s.state = &State{Version: 1, Nodes: map[string]*Node{}, Tokens: map[string]*EnrollToken{},
		Pending: map[string]*PendingPush{}, CreatedMs: time.Now().UnixMilli()}
	if b, err := os.ReadFile(s.statePath); err == nil && len(b) > 0 {
		if err := json.Unmarshal(b, s.state); err != nil {
			return nil, fmt.Errorf("解析 %s 失败: %w", s.statePath, err)
		}
		if s.state.Nodes == nil {
			s.state.Nodes = map[string]*Node{}
		}
		if s.state.Tokens == nil {
			s.state.Tokens = map[string]*EnrollToken{}
		}
		if s.state.Pending == nil {
			s.state.Pending = map[string]*PendingPush{}
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("读取 %s 失败: %w", s.statePath, err)
	}
	if err := s.saveLocked(); err != nil { // 立刻收紧权限并落一次盘
		return nil, err
	}
	return s, nil
}

// StateDir 返回状态目录。
func (s *Store) StateDir() string { return s.stateDir }

// DeriveSecret 由主密钥派生节点长期密钥：控制面不需要保存任何共享密钥。
func (s *Store) DeriveSecret(nodeID string) string {
	return HexKey(s.masterKey, "node-secret:"+nodeID)
}

// MasterKey 返回主密钥（hex）。
func (s *Store) MasterKey() string { return hex.EncodeToString(s.masterKey) }

// NodeSecret 返回该节点的长期密钥：
// 桥接模式下用 Laravel 下发的值，其余（含升级前的老节点）回退到主密钥派生。
func (s *Store) NodeSecret(nodeID string) string {
	if n, ok := s.Node(nodeID); ok && n.Secret != "" {
		return n.Secret
	}

	return s.DeriveSecret(nodeID)
}

// MirrorExternalNode 记录/更新一个由外部身份权威（Laravel）签发的节点。
// nodeID 与 secret 都由 Laravel 决定，本控制面不再派生。
func (s *Store) MirrorExternalNode(req EnrollRequest, nodeID, secret, authority, tokenName string) (*Node, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("镜像外部节点需要 node_id 与 secret")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	n, exists := s.state.Nodes[nodeID]
	if !exists {
		n = &Node{ID: nodeID, EnrolledAt: time.Now().UnixMilli(), ConfigState: "unknown"}
		s.state.Nodes[nodeID] = n
	}

	n.Name = firstNonEmpty(req.NodeName, req.Hostname, n.Name, nodeID)
	n.Hostname = firstNonEmpty(req.Hostname, n.Hostname)
	n.OS = firstNonEmpty(req.OS, n.OS)
	n.Arch = firstNonEmpty(req.Arch, n.Arch)
	n.Kernel = firstNonEmpty(req.Kernel, n.Kernel)
	n.BootID = firstNonEmpty(req.BootID, n.BootID)
	n.AgentVersion = firstNonEmpty(req.AgentVersion, n.AgentVersion)
	n.HysteriaVersion = firstNonEmpty(req.HysteriaVersion, n.HysteriaVersion)
	n.EnrollTokenName = firstNonEmpty(tokenName, n.EnrollTokenName, "laravel")
	n.Secret = strings.TrimSpace(secret)
	n.Authority = authority
	if !exists {
		n.Revoked = false
	}

	if err := s.saveLocked(); err != nil {
		return nil, err
	}

	copy := *n

	return &copy, nil
}

// Save 原子落盘。
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	s.state.UpdatedMs = time.Now().UnixMilli()
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.statePath)
}

// ---------------------------- 节点 ----------------------------

// CreateNode 注册一个新节点并返回其长期密钥（唯一次明文出现在 enroll 响应里）。
func (s *Store) CreateNode(req EnrollRequest, tokenName string) (*Node, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := "node_" + NewID(8)
	n := &Node{
		ID:              id,
		Name:            firstNonEmpty(req.NodeName, req.Hostname, id),
		Hostname:        req.Hostname,
		OS:              req.OS,
		Arch:            req.Arch,
		Kernel:          req.Kernel,
		BootID:          req.BootID,
		AgentVersion:    req.AgentVersion,
		HysteriaVersion: req.HysteriaVersion,
		EnrolledAt:      time.Now().UnixMilli(),
		EnrollTokenName: tokenName,
		ConfigState:     "unknown",
	}
	if _, dup := s.state.Nodes[n.ID]; dup {
		return nil, "", fmt.Errorf("node_id 冲突，请重试")
	}
	s.state.Nodes[n.ID] = n
	if err := s.saveLocked(); err != nil {
		return nil, "", err
	}
	return n, s.DeriveSecret(n.ID), nil
}

// Node 读取节点（返回副本，避免调用方长期持锁）。
func (s *Store) Node(id string) (*Node, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.state.Nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// Mutate 在锁内修改节点并落盘；返回 false 表示节点不存在。
func (s *Store) Mutate(id string, fn func(*Node)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.state.Nodes[id]
	if !ok {
		return false
	}
	fn(n)
	_ = s.saveLocked()
	return true
}

// Nodes 返回全部节点（按名字排序）。
func (s *Store) Nodes() []*Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Node, 0, len(s.state.Nodes))
	for _, n := range s.state.Nodes {
		cp := *n
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DeleteNode 吊销并删除节点（等于立即吊销其 node_secret，因为密钥是派生的）。
func (s *Store) DeleteNode(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.state.Nodes[id]; !ok {
		return false
	}
	delete(s.state.Nodes, id)
	delete(s.state.Pending, id)
	_ = s.saveLocked()
	return true
}

// ---------------------------- 令牌 ----------------------------

// NewToken 生成注册令牌；maxUses<=0 表示一次性。
func (s *Store) NewToken(name string, ttl time.Duration, maxUses int) *EnrollToken {
	if maxUses <= 0 {
		maxUses = 1
	}
	t := &EnrollToken{
		Token:     "enr_" + NewID(24),
		Name:      firstNonEmpty(name, "token-"+NewID(4)),
		CreatedMs: time.Now().UnixMilli(),
		MaxUses:   maxUses,
	}
	if ttl != 0 {
		// ttl<0 视为"立即过期"，ttl>0 为有效期，0 表示不过期。
		t.ExpiresMs = time.Now().Add(ttl).UnixMilli()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Tokens[t.Token] = t
	_ = s.saveLocked()
	return t
}

// Tokens 列出全部令牌。
func (s *Store) Tokens() []*EnrollToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*EnrollToken, 0, len(s.state.Tokens))
	for _, t := range s.state.Tokens {
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedMs > out[j].CreatedMs })
	return out
}

// RevokeToken 吊销令牌。
func (s *Store) RevokeToken(tok string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Tokens[tok]
	if !ok {
		return false
	}
	t.Revoked = true
	_ = s.saveLocked()
	return true
}

// UseToken 校验并消耗令牌（一次性令牌用满即不可再用）。
func (s *Store) UseToken(tok, nodeName string) (*EnrollToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Tokens[tok]
	if !ok {
		return nil, fmt.Errorf("未知的 enroll_token")
	}
	if err := t.Usable(time.Now().UnixMilli()); err != nil {
		return nil, err
	}
	t.Uses++
	t.UsedBy = append(t.UsedBy, nodeName)
	if len(t.UsedBy) > 50 {
		t.UsedBy = t.UsedBy[len(t.UsedBy)-50:]
	}
	cp := *t
	_ = s.saveLocked()
	return &cp, nil
}

// ---------------------------- 待确认配置 ----------------------------

// SetPending 记录已下发的配置，等待 Agent 的 ConfigAck。
func (s *Store) SetPending(nodeID string, p *PendingPush) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Pending[nodeID] = p
	_ = s.saveLocked()
}

// Pending 读取待确认配置。
func (s *Store) Pending(nodeID string) *PendingPush {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.state.Pending[nodeID]
	if !ok {
		return nil
	}
	cp := *p
	return &cp
}

// ClearPending 清除待确认配置。
func (s *Store) ClearPending(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Pending, nodeID)
	_ = s.saveLocked()
}

// NextRevision 生成配置版本号：<日期>-<序号>。
func (s *Store) NextRevision() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.RevSeq++
	_ = s.saveLocked()
	return fmt.Sprintf("%s-%d", time.Now().UTC().Format("20060102"), s.state.RevSeq)
}

// ---------------------------- 事件日志 ----------------------------

// Event 是落盘的事件（节点事件 / 指令结果 / 配置回执 / 上下线）。
type Event struct {
	TSMs    int64           `json:"ts_ms"`
	NodeID  string          `json:"node_id"`
	Kind    string          `json:"kind"` // node_event|command_result|config_ack|conn|heartbeat|duplicate
	Seq     uint64          `json:"seq,omitempty"`
	Type    string          `json:"type,omitempty"`
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// AppendEvent 追加事件到 state_dir/events/<node>.jsonl 并裁剪。
func (s *Store) AppendEvent(ev Event) {
	if s == nil {
		return
	}
	ev.TSMs = orNow(ev.TSMs)
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.eventPath(ev.NodeID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return
	}
	f.Close()
	s.trimEventsLocked(ev.NodeID)
}

func (s *Store) eventPath(nodeID string) string {
	safe := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == '.' || r == 0 {
			return '_'
		}
		return r
	}, nodeID)
	return filepath.Join(s.stateDir, "events", safe+".jsonl")
}

// trimEventsLocked 超限时保留最后 N 行（防止磁盘被日志吃满）。
func (s *Store) trimEventsLocked(nodeID string) {
	max := s.maxEvents
	if max <= 0 {
		max = 20000
	}
	path := s.eventPath(nodeID)
	fi, err := os.Stat(path)
	if err != nil || fi.Size() < int64(max)*120 {
		return // 粗估：每条事件约 120B，没到规模就不扫
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) <= max {
		return
	}
	lines = lines[len(lines)-max:]
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err == nil {
		_ = os.Rename(tmp, path)
	}
}

// maxEvents 由 server 设置（Retention.MaxEventsPerNode）。
// SetMaxEvents 设置每个节点事件日志的保留条数。
func (s *Store) SetMaxEvents(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxEvents = n
}

// Events 读取节点最近的事件（倒序，最多 limit 条）。
func (s *Store) Events(nodeID string, limit int) ([]Event, error) {
	if limit <= 0 || limit > 2000 {
		limit = 200
	}
	s.mu.Lock()
	path := s.eventPath(nodeID)
	s.mu.Unlock()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	out := make([]Event, 0, limit)
	for i := len(lines) - 1; i >= 0 && len(out) < limit; i-- {
		if strings.TrimSpace(lines[i]) == "" {
			continue
		}
		var ev Event
		if json.Unmarshal([]byte(lines[i]), &ev) == nil {
			out = append(out, ev)
		}
	}
	return out, nil
}
