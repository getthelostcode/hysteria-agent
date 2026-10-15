// Package metrics 提供内置 Prometheus 端点（默认 127.0.0.1:9100）与 /healthz、/readyz。
//
// 语义区分：
//   - /healthz（liveness）：进程活着就 200，systemd 用它判断要不要重启；
//   - /readyz（readiness）：控制面通道已连 + Hysteria 在跑 + 配置已应用，才 200。
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// Metrics 汇总 agent 的全部指标。
type Metrics struct {
	reg *prometheus.Registry

	ControlConnected  prometheus.Gauge
	ControlReconnects prometheus.Counter
	ControlUptime     prometheus.Gauge
	HeartbeatSent     prometheus.Counter
	HeartbeatAcked    prometheus.Counter
	// REST 心跳兜底（控制通道端点不可用时，面板仍能看到节点在线）
	RESTHeartbeats        prometheus.Counter
	RESTHeartbeatFailures prometheus.Counter
	EventsSent            prometheus.Counter
	EventsResent          prometheus.Counter
	EventsBuffered        prometheus.Gauge
	EventsDropped         prometheus.Counter
	OutboxBytes           prometheus.Gauge
	OutboxCompactions     prometheus.Counter

	ConfigApplyTotal   *prometheus.CounterVec // result=ok|rejected|failed|rolled_back|dry_run
	ConfigApplySeconds *prometheus.HistogramVec
	ConfigRevisionInfo *prometheus.GaugeVec // revision, sha256

	HysteriaRunning       prometheus.Gauge
	HysteriaRestarts      prometheus.Counter
	HysteriaExitCode      prometheus.Gauge
	HysteriaUptime        prometheus.Gauge
	HysteriaActiveUsers   prometheus.Gauge
	HysteriaOnlineDevices prometheus.Gauge
	HysteriaTxBytes       prometheus.Counter
	HysteriaRxBytes       prometheus.Counter

	CPUPercent  prometheus.Gauge
	MemPercent  prometheus.Gauge
	MemUsed     prometheus.Gauge
	DiskPercent prometheus.Gauge
	DiskUsed    prometheus.Gauge
	NetRxBytes  prometheus.Counter
	NetTxBytes  prometheus.Counter
	Load1       prometheus.Gauge

	CommandsTotal   *prometheus.CounterVec // command,status
	CommandsRunning prometheus.Gauge

	LogsSent        prometheus.Counter
	LogsSampledOut  prometheus.Counter
	LogsRingDropped prometheus.Counter
	TailActive      prometheus.Gauge

	UpgradeTotal *prometheus.CounterVec // result=ok|verify_failed|download_failed|...

	AgentInfo     *prometheus.GaugeVec
	AgentUptime   prometheus.Gauge
	LastHeartbeat prometheus.Gauge

	started time.Time
	readyCk ReadyChecker
}

// ReadyChecker 由 agent 注入：返回 (是否就绪, 各子项状态)。
type ReadyChecker func() (bool, map[string]string)

// New 构建指标集合；nodeID 会作为常量标签，便于多节点抓取时区分。
func New(nodeID string) *Metrics {
	constLabels := prometheus.Labels{"node_id": nodeID}
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg:     reg,
		started: time.Now(),
		ControlConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_control_connected", Help: "控制通道是否已连接 (1/0)", ConstLabels: constLabels}),
		ControlReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_control_reconnects_total", Help: "控制通道重连次数", ConstLabels: constLabels}),
		ControlUptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_control_connected_seconds", Help: "当前连接已持续秒数", ConstLabels: constLabels}),
		HeartbeatSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_heartbeat_sent_total", Help: "已发送心跳数", ConstLabels: constLabels}),
		HeartbeatAcked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_heartbeat_acked_total", Help: "已确认心跳数", ConstLabels: constLabels}),
		RESTHeartbeats: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_rest_heartbeats_total", Help: "REST 兜底心跳成功次数（控制通道端点不可用时）", ConstLabels: constLabels}),
		RESTHeartbeatFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_rest_heartbeat_failures_total", Help: "REST 兜底心跳失败次数", ConstLabels: constLabels}),
		EventsSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_events_sent_total", Help: "已投递事件数", ConstLabels: constLabels}),
		EventsResent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_events_resent_total", Help: "重连后补发事件数", ConstLabels: constLabels}),
		EventsBuffered: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_events_buffered", Help: "本地待投递事件数", ConstLabels: constLabels}),
		EventsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_events_dropped_total", Help: "因缓冲上限被丢弃的事件数", ConstLabels: constLabels}),
		OutboxBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_outbox_bytes", Help: "outbox 文件占用字节", ConstLabels: constLabels}),
		OutboxCompactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_outbox_compactions_total", Help: "outbox 压缩次数", ConstLabels: constLabels}),

		ConfigApplyTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hysteria_agent_config_apply_total", Help: "配置应用次数（按结果）", ConstLabels: constLabels}, []string{"result"}),
		ConfigApplySeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "hysteria_agent_config_apply_seconds", Help: "配置应用耗时", ConstLabels: constLabels,
			Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10, 30}}, []string{"result"}),
		ConfigRevisionInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hysteria_agent_config_revision_info", Help: "当前生效配置（恒为 1，标签里带 revision）", ConstLabels: constLabels},
			[]string{"revision", "sha256"}),

		HysteriaRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_hysteria_running", Help: "被管理的 Hysteria 是否在运行 (1/0)", ConstLabels: constLabels}),
		HysteriaRestarts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_hysteria_restarts_total", Help: "Hysteria 崩溃重启次数", ConstLabels: constLabels}),
		HysteriaExitCode: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_hysteria_last_exit_code", Help: "Hysteria 最近一次退出码", ConstLabels: constLabels}),
		HysteriaUptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_hysteria_uptime_seconds", Help: "Hysteria 进程运行秒数", ConstLabels: constLabels}),
		HysteriaActiveUsers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_hysteria_active_users", Help: "本采集周期内有流量的用户数", ConstLabels: constLabels}),
		HysteriaOnlineDevices: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_hysteria_online_devices", Help: "在线设备数（/online 求和）", ConstLabels: constLabels}),
		HysteriaTxBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_hysteria_tx_bytes_total", Help: "Hysteria 上报的上行字节", ConstLabels: constLabels}),
		HysteriaRxBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_hysteria_rx_bytes_total", Help: "Hysteria 上报的下行字节", ConstLabels: constLabels}),

		CPUPercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_cpu_percent", Help: "主机 CPU 使用率", ConstLabels: constLabels}),
		MemPercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_mem_percent", Help: "主机内存使用率", ConstLabels: constLabels}),
		MemUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_mem_used_bytes", Help: "主机已用内存", ConstLabels: constLabels}),
		DiskPercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_disk_percent", Help: "磁盘使用率", ConstLabels: constLabels}),
		DiskUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_disk_used_bytes", Help: "已用磁盘", ConstLabels: constLabels}),
		NetRxBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_net_rx_bytes_total", Help: "主机接收字节", ConstLabels: constLabels}),
		NetTxBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_net_tx_bytes_total", Help: "主机发送字节", ConstLabels: constLabels}),
		Load1: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_load1", Help: "1 分钟负载", ConstLabels: constLabels}),

		CommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hysteria_agent_commands_total", Help: "远程指令执行次数", ConstLabels: constLabels}, []string{"command", "status"}),
		CommandsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_commands_running", Help: "正在执行的指令数", ConstLabels: constLabels}),

		LogsSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_logs_sent_total", Help: "远程投递的日志行数", ConstLabels: constLabels}),
		LogsSampledOut: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_logs_sampled_out_total", Help: "被采样丢弃的日志行数", ConstLabels: constLabels}),
		LogsRingDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hysteria_agent_logs_tail_dropped_total", Help: "tail_logs 订阅者跟不上的丢弃行数", ConstLabels: constLabels}),
		TailActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_tail_active", Help: "进行中的 tail_logs 数量", ConstLabels: constLabels}),

		UpgradeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hysteria_agent_upgrade_total", Help: "自升级尝试次数（按结果）", ConstLabels: constLabels}, []string{"result"}),

		AgentInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hysteria_agent_info", Help: "Agent 版本信息（恒为 1）", ConstLabels: constLabels}, []string{"version"}),
		AgentUptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_uptime_seconds", Help: "Agent 运行秒数", ConstLabels: constLabels}),
		LastHeartbeat: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hysteria_agent_last_heartbeat_timestamp", Help: "最近一次成功心跳的 Unix 时间戳", ConstLabels: constLabels}),
	}
	for _, c := range []prometheus.Collector{
		m.ControlConnected, m.ControlReconnects, m.ControlUptime, m.HeartbeatSent, m.HeartbeatAcked,
		m.EventsSent, m.EventsResent, m.EventsBuffered, m.EventsDropped, m.OutboxBytes, m.OutboxCompactions,
		m.ConfigApplyTotal, m.ConfigApplySeconds, m.ConfigRevisionInfo,
		m.HysteriaRunning, m.HysteriaRestarts, m.HysteriaExitCode, m.HysteriaUptime,
		m.HysteriaActiveUsers, m.HysteriaOnlineDevices, m.HysteriaTxBytes, m.HysteriaRxBytes,
		m.CPUPercent, m.MemPercent, m.MemUsed, m.DiskPercent, m.DiskUsed, m.NetRxBytes, m.NetTxBytes, m.Load1,
		m.CommandsTotal, m.CommandsRunning, m.LogsSent, m.LogsSampledOut, m.LogsRingDropped, m.TailActive,
		m.UpgradeTotal, m.AgentInfo, m.AgentUptime, m.LastHeartbeat,
	} {
		reg.MustRegister(c)
	}
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.AgentInfo.WithLabelValues(version.Version).Set(1)
	return m
}

// Registry 返回底层注册表（测试里直接 gather 用）。
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// SetReadyChecker 注入就绪检查函数。
func (m *Metrics) SetReadyChecker(fn ReadyChecker) { m.readyCk = fn }

// Snapshot 刷新派生指标（uptime / 连接时长）。
func (m *Metrics) Snapshot(controlConnectedSince time.Time) {
	m.AgentUptime.Set(time.Since(m.started).Seconds())
	if !controlConnectedSince.IsZero() {
		m.ControlUptime.Set(time.Since(controlConnectedSince).Seconds())
	}
}

// ---------------------------- HTTP 端点 ----------------------------

// Server 是本地指标/健康端点。
type Server struct {
	m    *Metrics
	cfg  MetricsConfig
	srv  *http.Server
	addr atomic.Value // 实际监听地址（:0 测试时有用）
}

// MetricsConfig 是 metrics 包需要的配置子集，避免直接依赖 config 包。
type MetricsConfig struct {
	Listen             string
	Path               string
	HealthPath         string
	ReadyPath          string
	ReadHeaderTimeoutS float64
	Version            string
	NodeID             string
}

// NewServer 构建 HTTP 端点。
func NewServer(m *Metrics, cfg MetricsConfig) *Server {
	if cfg.Path == "" {
		cfg.Path = "/metrics"
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/healthz"
	}
	if cfg.ReadyPath == "" {
		cfg.ReadyPath = "/readyz"
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:9100"
	}
	s := &Server{m: m, cfg: cfg}

	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc(cfg.HealthPath, s.handleHealth)
	mux.HandleFunc(cfg.ReadyPath, s.handleReady)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "hysteria-agent: try %s %s %s\n", cfg.Path, cfg.HealthPath, cfg.ReadyPath)
	})

	timeout := time.Duration(cfg.ReadHeaderTimeoutS * float64(time.Second))
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: timeout,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	return s
}

// Start 开始监听；返回实际地址（支持 ":0" 随机端口，便于测试）。
func (s *Server) Start() (string, error) {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return "", fmt.Errorf("监听 %s 失败: %w", s.cfg.Listen, err)
	}
	s.addr.Store(ln.Addr().String())
	go func() {
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			// 端点挂掉不该影响节点服务，交给调用方记录。
			s.addr.Store(ln.Addr().String() + " (serve error: " + err.Error() + ")")
		}
	}()
	return ln.Addr().String(), nil
}

// Addr 返回实际监听地址。
func (s *Server) Addr() string {
	if v, ok := s.addr.Load().(string); ok {
		return v
	}
	return s.cfg.Listen
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"version":        s.cfg.Version,
		"node_id":        s.cfg.NodeID,
		"uptime_seconds": int(time.Since(s.m.started).Seconds()),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.m.readyCk == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "unknown"})
		return
	}
	ok, detail := s.m.readyCk()
	code := http.StatusOK
	status := "ready"
	if !ok {
		code = http.StatusServiceUnavailable
		status = "not_ready"
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": detail})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
