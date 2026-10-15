package cp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
	"github.com/gorilla/websocket"
)

// EnrollRequest 与 Agent 的 enroll.Request 字段一致（只声明用得到的）。
type EnrollRequest struct {
	EnrollToken     string `json:"enroll_token"`
	NodeName        string `json:"node_name,omitempty"`
	Hostname        string `json:"hostname"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	Kernel          string `json:"kernel,omitempty"`
	BootID          string `json:"boot_id,omitempty"`
	AgentVersion    string `json:"agent_version"`
	HysteriaVersion string `json:"hysteria_version,omitempty"`
	Protocol        string `json:"protocol"`
	Replace         bool   `json:"replace,omitempty"`
}

// EnrollResponse 与 Agent 的 enroll.Response 字段一致。
type EnrollResponse struct {
	NodeID                   string   `json:"node_id"`
	NodeSecret               string   `json:"node_secret"`
	NodeName                 string   `json:"node_name,omitempty"`
	ControlURL               string   `json:"control_url,omitempty"`
	WSPath                   string   `json:"ws_path,omitempty"`
	PinSHA256                []string `json:"pin_sha256,omitempty"`
	CACertPEM                string   `json:"ca_cert_pem,omitempty"`
	JWTIssuer                string   `json:"jwt_issuer,omitempty"`
	JWTAudience              string   `json:"jwt_audience,omitempty"`
	HeartbeatIntervalSeconds int      `json:"heartbeat_interval_seconds,omitempty"`
	TelemetryIntervalSeconds int      `json:"telemetry_interval_seconds,omitempty"`
	ServerVersion            string   `json:"server_version,omitempty"`
	Message                  string   `json:"message,omitempty"`
}

// Server 是控制面 HTTP 服务。
type Server struct {
	cfg      *Config
	store    *Store
	log      *slog.Logger
	upgrader websocket.Upgrader

	mu      sync.Mutex
	conns   map[string]*session   // node_id -> 活跃会话
	waiters map[string][]chan any // node_id -> 等待回执的通道
	metrics *Metrics
}

// session 是一条控制通道连接。
type session struct {
	node    string
	conn    *websocket.Conn
	writeMu sync.Mutex
	closed  bool

	ackMu sync.Mutex
	acks  map[uint64]struct{} // 本会话内已处理过的 seq（进程内幂等）
}

// Metrics 是控制面的进程内计数器。
type Metrics struct {
	mu             sync.Mutex
	EnrollTotal    int64
	EnrollFailed   int64
	WSConnected    int64
	WSRejected     int64
	Heartbeats     int64
	EventsTotal    int64
	Duplicates     int64
	CommandsSent   int64
	CommandsDone   int64
	PushesSent     int64
	PushApplied    int64
	PushFailed     int64
	CfgStateCounts map[string]int64
}

func newMetrics() *Metrics { return &Metrics{CfgStateCounts: map[string]int64{}} }

// New 构建控制面服务。
func New(cfg *Config, store *Store, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Heartbeat.IntervalSeconds <= 0 {
		cfg.Heartbeat.IntervalSeconds = 30
	}
	if cfg.Heartbeat.TimeoutSeconds <= 0 {
		cfg.Heartbeat.TimeoutSeconds = 90
	}
	if cfg.Heartbeat.TelemetrySeconds <= 0 {
		cfg.Heartbeat.TelemetrySeconds = 15
	}
	if cfg.EnrollPath == "" {
		cfg.EnrollPath = "/api/v1/agent/enroll"
	}
	if cfg.WSPath == "" {
		cfg.WSPath = "/api/v1/agent/control"
	}
	store.SetMaxEvents(cfg.Retention.MaxEventsPerNode)
	return &Server{
		cfg:   cfg,
		store: store,
		log:   log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1 << 16,
			WriteBufferSize: 1 << 16,
			// 控制面既可能直接暴露，也可能在 Cloudflare Tunnel / nginx 后面；
			// 鉴权由 JWT 保证，因此不限制 Origin（Agent 不是浏览器）。
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		conns:   map[string]*session{},
		waiters: map[string][]chan any{},
		metrics: newMetrics(),
	}
}

// Handler 返回完整的 HTTP 路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.EnrollPath, s.handleEnroll)
	mux.HandleFunc(s.cfg.WSPath, s.handleControl)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": version.Version, "time_ms": time.Now().UnixMilli()})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.Handle("/admin/", s.adminAuth(http.HandlerFunc(s.handleAdmin)))
	if s.cfg.ServeDash == nil || *s.cfg.ServeDash {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(dashboardHTML))
		})
	}
	return mux
}

// Serve 启动 HTTP 服务并阻塞。
func (s *Server) Serve(ctx context.Context) error {
	if s.cfg.AdminToken == "" {
		return fmt.Errorf("admin_token 不能为空：请写进配置文件（0600），否则 /admin 无人看守")
	}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", s.cfg.Listen, err)
	}
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 20 * time.Second}
	go s.offlineSweeper(ctx)
	go s.pendingResender(ctx)
	s.log.Info("控制面已启动", "listen", s.cfg.Listen,
		"enroll", s.cfg.EnrollPath, "control", s.cfg.WSPath,
		"public_url", s.cfg.PublicURL, "state_dir", s.store.StateDir())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

// ---------------------------- enroll ----------------------------

func (s *Server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeErr(w, http.StatusMethodNotAllowed, "只接受 POST")
		return
	}
	var req EnrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体不是合法 JSON："+err.Error())
		return
	}
	// 令牌可放在 body 或 X-Enroll-Token 头。
	if strings.TrimSpace(req.EnrollToken) == "" {
		req.EnrollToken = strings.TrimSpace(r.Header.Get("X-Enroll-Token"))
	}
	if req.EnrollToken == "" {
		s.metrics.bump("enroll_failed")
		writeErr(w, http.StatusUnauthorized, "缺少 enroll_token（body.enroll_token 或 X-Enroll-Token）")
		return
	}
	tok, err := s.store.UseToken(req.EnrollToken, firstNonEmpty(req.NodeName, req.Hostname))
	if err != nil {
		s.metrics.bump("enroll_failed")
		s.log.Warn("enroll 被拒绝", "token", mask(req.EnrollToken), "err", err)
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	node, secret, err := s.store.CreateNode(req, tok.Name)
	if err != nil {
		s.metrics.bump("enroll_failed")
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.store.AppendEvent(Event{NodeID: node.ID, Kind: "conn", Type: "enrolled",
		Message: fmt.Sprintf("节点注册：%s (%s/%s) agent=%s token=%s", node.Name, node.OS, node.Arch, req.AgentVersion, tok.Name)})
	s.metrics.bump("enroll")
	s.log.Info("节点已注册", "node_id", node.ID, "name", node.Name,
		"hostname", req.Hostname, "agent", req.AgentVersion, "token", tok.Name, "uses", fmt.Sprintf("%d/%d", tok.Uses, tok.MaxUses))

	writeJSON(w, http.StatusOK, EnrollResponse{
		NodeID:                   node.ID,
		NodeSecret:               secret,
		NodeName:                 node.Name,
		ControlURL:               s.cfg.PublicURL,
		WSPath:                   s.cfg.WSPath,
		PinSHA256:                s.cfg.PinSHA256,
		CACertPEM:                s.cfg.CACertPEM,
		JWTIssuer:                creds.DefaultIssuer,
		JWTAudience:              creds.DefaultAudience,
		HeartbeatIntervalSeconds: s.cfg.Heartbeat.IntervalSeconds,
		TelemetryIntervalSeconds: s.cfg.Heartbeat.TelemetrySeconds,
		ServerVersion:            version.Version,
		Message:                  "注册成功；后续启动请使用 credentials.json 里的 node_secret 自签 JWT 连接控制通道",
	})
}

// ---------------------------- 控制通道 ----------------------------

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	nodeID := strings.TrimSpace(r.Header.Get("X-Node-ID"))
	if tok == "" || nodeID == "" {
		s.metrics.bump("ws_rejected")
		writeErr(w, http.StatusUnauthorized, "缺少 Authorization Bearer 令牌或 X-Node-ID")
		return
	}
	node, ok := s.store.Node(nodeID)
	if !ok || node.Revoked {
		s.metrics.bump("ws_rejected")
		s.log.Warn("控制通道拒绝：未知或已吊销节点", "node_id", nodeID)
		writeErr(w, http.StatusUnauthorized, "未知节点或节点已吊销")
		return
	}
	claims, err := creds.VerifyJWT(tok, s.store.DeriveSecret(nodeID))
	if err != nil {
		s.metrics.bump("ws_rejected")
		s.log.Warn("控制通道拒绝：JWT 校验失败", "node_id", nodeID, "err", err)
		writeErr(w, http.StatusUnauthorized, "JWT 校验失败: "+err.Error())
		return
	}
	if aud, _ := claims["aud"].(string); aud != creds.DefaultAudience {
		s.metrics.bump("ws_rejected")
		writeErr(w, http.StatusUnauthorized, "JWT audience 不匹配")
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("WebSocket 升级失败", "node_id", nodeID, "err", err)
		return
	}
	s.mu.Lock()
	if old := s.conns[nodeID]; old != nil {
		// 同节点重复连接（常见于重启竞态）：踢掉旧的，保留新的。
		old.close()
		s.log.Info("同一节点的旧连接被新连接替换", "node_id", nodeID)
	}
	sess := &session{node: nodeID, conn: conn, acks: map[uint64]struct{}{}}
	s.conns[nodeID] = sess
	s.mu.Unlock()

	s.metrics.bump("ws_connect")
	now := time.Now().UnixMilli()
	s.store.Mutate(nodeID, func(n *Node) {
		n.Connected = true
		n.ConnectedSinceMs = now
		n.LastConnectedMs = now
		n.LastSeenMs = now
		n.Connects++
	})
	s.store.AppendEvent(Event{NodeID: nodeID, Kind: "conn", Type: "connected",
		Message: fmt.Sprintf("控制通道已建立（jti=%v，第 %d 次连接）", claims["jti"], node.Connects+1)})
	s.log.Info("控制通道已建立", "node_id", nodeID, "name", node.Name, "agent", node.AgentVersion)

	defer func() {
		s.mu.Lock()
		if s.conns[nodeID] == sess {
			delete(s.conns, nodeID)
		}
		s.mu.Unlock()
		conn.Close()
		s.store.Mutate(nodeID, func(n *Node) {
			n.Connected = false
			n.LastDisconnectMs = time.Now().UnixMilli()
		})
		s.store.AppendEvent(Event{NodeID: nodeID, Kind: "conn", Type: "disconnected", Message: "控制通道断开"})
		s.log.Info("控制通道断开", "node_id", nodeID)
	}()

	// 若存在未确认的配置下发，连接建立后重发一次（Agent 侧以 revision 幂等）。
	if p := s.store.Pending(nodeID); p != nil {
		go func() {
			time.Sleep(300 * time.Millisecond) // 等 Hello/HelloAck 完成
			_ = s.sendConfig(nodeID, p)
		}()
	}

	for {
		conn.SetReadDeadline(time.Now().Add(time.Duration(s.cfg.Heartbeat.TimeoutSeconds*3) * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		env, err := wire.Unmarshal(raw)
		if err != nil {
			s.log.Warn("解析消息失败", "node_id", nodeID, "err", err)
			continue
		}
		s.onMessage(sess, env)
	}
}

func (s *Server) onMessage(sess *session, env *agentpb.Envelope) {
	nodeID := sess.node
	typ := wire.TypeName(env.GetType())
	seq := env.GetSeq()

	dup := false
	if seq > 0 {
		sess.ackMu.Lock()
		_, seen := sess.acks[seq]
		if seen {
			dup = true
		} else {
			sess.acks[seq] = struct{}{}
		}
		sess.ackMu.Unlock()
	}
	if node, ok := s.store.Node(nodeID); ok && seq > 0 && seq <= node.AckedSeq {
		dup = true // 跨会话的重发（控制面重启/换连接）
	}
	if dup {
		s.metrics.bump("duplicate")
		s.store.Mutate(nodeID, func(n *Node) { n.DuplicateMsgs++ })
		if seq > 0 { // 仍然确认，让 Agent 的 outbox 前进
			s.ack(sess, seq)
		}
		return
	}

	s.store.Mutate(nodeID, func(n *Node) { n.ReceivedMsgs++; n.LastSeenMs = time.Now().UnixMilli() })

	var (
		persist any
		kind    = strings.TrimPrefix(typ, "MSG_TYPE_")
		etype   string
		msg     string
	)
	switch b := env.GetBody().(type) {
	case *agentpb.Envelope_Hello:
		h := b.Hello
		s.store.Mutate(nodeID, func(n *Node) {
			n.AgentVersion = h.GetAgentVersion()
			n.HysteriaVersion = h.GetHysteriaVersion()
			n.Hostname = firstNonEmpty(h.GetHostname(), n.Hostname)
			n.OS, n.Arch, n.Kernel = h.GetOs(), h.GetArch(), h.GetKernel()
			if h.GetBootId() != "" {
				n.BootID = h.GetBootId()
			}
			n.Caps = h.GetCapabilities()
			if h.GetConfigRevision() != "" {
				n.ConfigRevision = h.GetConfigRevision()
			}
			if h.GetConfigSha256() != "" {
				n.ConfigSHA256 = h.GetConfigSha256()
			}
		})
		node, _ := s.store.Node(nodeID)
		acked := uint64(0)
		if node != nil {
			acked = node.AckedSeq
		}
		rev := ""
		if node != nil {
			rev = node.ConfigRevision
		}
		s.log.Info("Hello", "node_id", nodeID, "agent", h.GetAgentVersion(), "hysteria", h.GetHysteriaVersion(),
			"resume_from_seq", h.GetResumeFromSeq(), "last_acked", h.GetLastAckedSeq(),
			"pending_events", h.GetLastAckedSeq(), "caps", len(h.GetCapabilities()))
		s.send(sess, wire.New(agentpb.MsgType_MSG_TYPE_HELLO_ACK, &agentpb.HelloAck{
			ServerTimeMs:             time.Now().UnixMilli(),
			HeartbeatIntervalSeconds: uint32(s.cfg.Heartbeat.IntervalSeconds),
			HeartbeatTimeoutSeconds:  uint32(s.cfg.Heartbeat.TimeoutSeconds),
			TelemetryIntervalSeconds: uint32(s.cfg.Heartbeat.TelemetrySeconds),
			ResendFromSeq:            acked + 1,
			ConfigRevision:           rev,
			AcceptedCommands:         wire.CommandWhitelist(),
			ServerVersion:            version.Version,
		}))
		return

	case *agentpb.Envelope_Heartbeat:
		hb := b.Heartbeat
		s.metrics.bump("heartbeat")
		s.store.Mutate(nodeID, func(n *Node) {
			n.LastSeenMs = time.Now().UnixMilli()
			n.LastHeartbeat = &HeartbeatInfo{
				TSMs: hb.GetTsMs(), UptimeSeconds: hb.GetUptimeSeconds(), PendingEvents: hb.GetPendingEvents(),
				HysteriaRun: hb.GetHysteriaRunning(), Ready: hb.GetReady(),
				ConfigRevision: hb.GetConfigRevision(), LastAckedSeq: hb.GetLastAckedSeq(), SentEvents: hb.GetSentEvents(),
			}
		})
		acked := uint64(0)
		if n, ok := s.store.Node(nodeID); ok {
			acked = n.AckedSeq
			// 控制面期望的版本与 Agent 上报不一致 → 提示（可能是下发未生效或人为改了文件）
			if p := s.store.Pending(nodeID); p != nil && hb.GetConfigRevision() != "" && hb.GetConfigRevision() != p.Revision {
				s.log.Info("Agent 上报的配置版本与待确认下发不一致",
					"node_id", nodeID, "agent_rev", hb.GetConfigRevision(), "pending_rev", p.Revision)
			}
		}
		s.send(sess, wire.New(agentpb.MsgType_MSG_TYPE_HEARTBEAT_ACK, &agentpb.HeartbeatAck{
			ServerTimeMs: time.Now().UnixMilli(), AckedSeq: acked,
		}))
		if seq > 0 {
			s.ack(sess, seq)
		}
		return

	case *agentpb.Envelope_NodeEvent:
		e := b.NodeEvent
		kind, etype, msg = "node_event", e.GetType(), e.GetReason()
		persist = map[string]any{
			"type": e.GetType(), "exit_code": e.GetExitCode(), "pid": e.GetPid(),
			"reason": e.GetReason(), "revision": e.GetRevision(),
			"data": json.RawMessage(orJSON(e.GetDataJson())), "log_tail": e.GetLogTail(),
		}
		s.log.Info("节点事件", "node_id", nodeID, "type", e.GetType(), "reason", e.GetReason(), "revision", e.GetRevision())
		if e.GetType() == "config_applied" || e.GetType() == "config_failed" || e.GetType() == "config_rolled_back" {
			st := map[string]string{"config_applied": "applied", "config_failed": "failed", "config_rolled_back": "rolled_back"}[e.GetType()]
			s.store.Mutate(nodeID, func(n *Node) {
				n.ConfigState = st
				if e.GetRevision() != "" {
					n.ConfigRevision = e.GetRevision()
				}
				if e.GetType() != "config_applied" {
					n.ConfigError = e.GetReason()
				} else {
					n.ConfigError = ""
				}
			})
			if st == "applied" {
				s.metrics.bump("push_applied")
			} else {
				s.metrics.bump("push_failed")
			}
			s.store.ClearPending(nodeID)
		}
		if e.GetType() == "outbox_dropped" {
			s.log.Warn("Agent outbox 丢弃了事件（超过上限）", "node_id", nodeID, "reason", e.GetReason())
		}

	case *agentpb.Envelope_ConfigAck:
		a := b.ConfigAck
		kind, etype = "config_ack", "ConfigAck"
		msg = fmt.Sprintf("revision=%s ok=%v rolled_back=%v dry_run=%v", a.GetRevision(), a.GetOk(), a.GetRolledBack(), a.GetDryRun())
		if a.GetError() != "" {
			msg += " err=" + a.GetError()
		}
		persist = map[string]any{
			"revision": a.GetRevision(), "ok": a.GetOk(), "error": a.GetError(),
			"previous_revision": a.GetPreviousRevision(), "sha256": a.GetSha256(),
			"warnings": a.GetWarnings(), "duration_ms": a.GetDurationMs(),
			"rolled_back": a.GetRolledBack(), "dry_run": a.GetDryRun(), "checks": a.GetChecks(),
		}
		state := "applied"
		switch {
		case a.GetError() != "":
			state = "failed"
		case a.GetRolledBack():
			state = "rolled_back"
		case !a.GetOk():
			state = "failed"
		case a.GetDryRun():
			state = "dry_run_ok"
		}
		s.store.Mutate(nodeID, func(n *Node) {
			n.ConfigState = state
			n.ConfigError = a.GetError()
			if a.GetRevision() != "" && !a.GetDryRun() {
				n.ConfigRevision = a.GetRevision()
			}
			if a.GetSha256() != "" {
				n.ConfigSHA256 = a.GetSha256()
			}
		})
		switch {
		case a.GetOk() && !a.GetDryRun():
			s.metrics.bump("push_applied")
			s.store.ClearPending(nodeID)
		case a.GetDryRun():
			// dry_run 不改变实际配置，保留 pending（若有）
		default:
			s.metrics.bump("push_failed")
			s.store.ClearPending(nodeID)
		}
		s.log.Info("配置回执", "node_id", nodeID, "revision", a.GetRevision(), "ok", a.GetOk(),
			"rolled_back", a.GetRolledBack(), "dry_run", a.GetDryRun(), "err", a.GetError())
		s.deliver(nodeID, "config_ack", a)

	case *agentpb.Envelope_CommandResult:
		r := b.CommandResult
		kind = "command_result"
		etype = wire.CommandTypeName(r.GetType())
		msg = fmt.Sprintf("status=%s err=%s", wire.CommandStatusName(r.GetStatus()), r.GetError())
		persist = map[string]any{
			"command_id": r.GetCommandId(), "type": wire.CommandTypeName(r.GetType()),
			"status": wire.CommandStatusName(r.GetStatus()), "error": r.GetError(),
			"result":     json.RawMessage(orJSON(r.GetResultJson())),
			"started_ms": r.GetStartedMs(), "finished_ms": r.GetFinishedMs(),
		}
		s.metrics.bump("command_done")
		s.log.Info("指令结果", "node_id", nodeID, "command_id", r.GetCommandId(),
			"type", wire.CommandTypeName(r.GetType()), "status", wire.CommandStatusName(r.GetStatus()), "err", r.GetError())
		s.deliver(nodeID, "command_result", r)

	case *agentpb.Envelope_Metrics:
		m := b.Metrics
		info := summarizeMetrics(m)
		s.store.Mutate(nodeID, func(n *Node) { n.LastMetrics = info })
		persist = map[string]any{"sample": m.GetSample(), "hysteria": m.GetHysteria(),
			"agent_version": m.GetAgentVersion(), "config_revision": m.GetConfigRevision()}
		kind, etype = "metrics", "MetricBatch"

	case *agentpb.Envelope_Logs:
		l := b.Logs
		kind, etype = "logs", "LogBatch"
		msg = fmt.Sprintf("%d 条来自 %s", len(l.GetEntries()), l.GetSource())
		persist = map[string]any{"source": l.GetSource(), "entries": l.GetEntries()}

	case *agentpb.Envelope_EventAck:
		return // Agent 对控制面事件的确认：本控制面不主动发事件，忽略即可

	default:
		s.log.Debug("收到未处理的消息类型", "node_id", nodeID, "type", typ)
		return
	}

	if seq > 0 {
		s.ack(sess, seq)
	}
	if persist != nil {
		if kind != "metrics" { // 遥测每 15s 一条，不入事件日志（只留最新快照）
			data, _ := json.Marshal(persist)
			s.store.AppendEvent(Event{NodeID: nodeID, Kind: kind, Seq: seq, Type: etype, Message: msg, Data: data})
		}
	}
}

// ack 累积确认：无论消息体是什么，只要落库了就确认它的 seq。
func (s *Server) ack(sess *session, seq uint64) {
	s.store.Mutate(sess.node, func(n *Node) {
		if seq > n.AckedSeq {
			n.AckedSeq = seq
		}
	})
	s.send(sess, wire.New(agentpb.MsgType_MSG_TYPE_EVENT_ACK, &agentpb.EventAck{AckedSeq: seq}))
}

// ---------------------------- 下发 ----------------------------

// send 向节点写一帧（加写锁，允许并发调用）。
func (s *Server) send(sess *session, env *agentpb.Envelope) error {
	raw, err := wire.Marshal(env)
	if err != nil {
		return err
	}
	sess.writeMu.Lock()
	defer sess.writeMu.Unlock()
	if sess.closed {
		return fmt.Errorf("连接已关闭")
	}
	sess.conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
	return sess.conn.WriteMessage(websocket.BinaryMessage, raw)
}

// Session 返回节点的活跃会话。
func (s *Server) Session(nodeID string) (*session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.conns[nodeID]
	return sess, ok
}

func (sess *session) close() {
	sess.writeMu.Lock()
	sess.closed = true
	sess.writeMu.Unlock()
	_ = sess.conn.Close()
}

// sendConfig 下发一份配置（内部/管理接口共用）。
func (s *Server) sendConfig(nodeID string, p *PendingPush) error {
	sess, ok := s.Session(nodeID)
	if !ok {
		return fmt.Errorf("节点 %s 当前不在线（无控制通道）", nodeID)
	}
	format := p.Format
	if format == "" {
		format = "yaml"
	}
	s.metrics.bump("push_sent")
	s.store.Mutate(nodeID, func(n *Node) { n.ConfigState = "pending" })
	return s.send(sess, wire.New(agentpb.MsgType_MSG_TYPE_CONFIG_PUSH, &agentpb.ConfigPush{
		Revision: p.Revision,
		Format:   format,
		Payload:  p.Payload,
		Sha256:   p.SHA256,
		Reason:   p.Reason,
		DryRun:   p.DryRun,
		Force:    true,
	}))
}

// Command 下发指令并可选等待结果。
func (s *Server) Command(nodeID, cmdID string, ctype agentpb.CommandType, params []byte, wait time.Duration) (map[string]any, error) {
	sess, ok := s.Session(nodeID)
	if !ok {
		return nil, fmt.Errorf("节点 %s 当前不在线", nodeID)
	}
	if cmdID == "" {
		cmdID = "cmd_" + NewID(10)
	}
	ch := s.addWaiter(nodeID, "command_result")
	defer s.removeWaiter(nodeID, ch)
	s.metrics.bump("command_sent")
	if err := s.send(sess, wire.New(agentpb.MsgType_MSG_TYPE_COMMAND, &agentpb.CommandRequest{
		CommandId:  cmdID,
		Type:       ctype,
		ParamsJson: params,
		TraceId:    "trace_" + NewID(8),
	})); err != nil {
		return nil, err
	}
	if wait <= 0 {
		return map[string]any{"command_id": cmdID, "dispatched": true, "waited": false}, nil
	}
	select {
	case v := <-ch:
		if r, ok := v.(*agentpb.CommandResult); ok {
			out := map[string]any{
				"command_id": r.GetCommandId(), "dispatched": true, "waited": true,
				"status": wire.CommandStatusName(r.GetStatus()), "type": wire.CommandTypeName(r.GetType()),
				"error": r.GetError(), "started_ms": r.GetStartedMs(), "finished_ms": r.GetFinishedMs(),
			}
			if len(r.GetResultJson()) > 0 {
				out["result"] = json.RawMessage(r.GetResultJson())
			}
			return out, nil
		}
		return map[string]any{"command_id": cmdID, "dispatched": true, "waited": true}, nil
	case <-time.After(wait):
		return map[string]any{"command_id": cmdID, "dispatched": true, "waited": false, "note": "等待结果超时，指令可能仍在执行；用 /admin/nodes/{id}/events 查看结果"}, nil
	}
}

// PushConfig 下发配置并可选等待回执。
func (s *Server) PushConfig(nodeID string, payload []byte, format, revision, reason string, dryRun bool, wait time.Duration) (map[string]any, error) {
	if format == "" {
		if len(payload) > 0 && payload[0] == '{' {
			format = "json"
		} else {
			format = "yaml"
		}
	}
	if revision == "" {
		revision = s.store.NextRevision()
	}
	p := &PendingPush{
		Revision: revision, SHA256: SHA256Hex(payload), Format: format, Payload: payload,
		Reason: reason, DryRun: dryRun, PushedMs: time.Now().UnixMilli(),
	}
	s.store.SetPending(nodeID, p)
	ch := s.addWaiter(nodeID, "config_ack")
	defer s.removeWaiter(nodeID, ch)
	if err := s.sendConfig(nodeID, p); err != nil {
		return nil, err
	}
	s.store.AppendEvent(Event{NodeID: nodeID, Kind: "config_ack", Type: "config_pushed",
		Message: fmt.Sprintf("控制面下发配置 revision=%s sha256=%s dry_run=%v reason=%s", revision, p.SHA256[:12], dryRun, reason)})
	if wait <= 0 {
		return map[string]any{"revision": revision, "sha256": p.SHA256, "dispatched": true, "waited": false}, nil
	}
	select {
	case v := <-ch:
		if a, ok := v.(*agentpb.ConfigAck); ok {
			return map[string]any{
				"revision": a.GetRevision(), "sha256": a.GetSha256(), "dispatched": true, "waited": true,
				"ok": a.GetOk(), "error": a.GetError(), "rolled_back": a.GetRolledBack(),
				"dry_run": a.GetDryRun(), "checks": a.GetChecks(), "warnings": a.GetWarnings(),
				"duration_ms": a.GetDurationMs(),
			}, nil
		}
	case <-time.After(wait):
		return map[string]any{"revision": revision, "sha256": p.SHA256, "dispatched": true, "waited": false,
			"note": "等待回执超时；Agent 可能正在重启 Hysteria，稍后用 /admin/nodes/{id} 看 config_state"}, nil
	}
	return map[string]any{"revision": revision, "dispatched": true}, nil
}

// ---------------------------- 等待通道 ----------------------------

func (s *Server) addWaiter(nodeID, kind string) chan any {
	ch := make(chan any, 4)
	s.mu.Lock()
	s.waiters[nodeID+"|"+kind] = append(s.waiters[nodeID+"|"+kind], ch)
	s.mu.Unlock()
	return ch
}

func (s *Server) removeWaiter(nodeID string, ch chan any) {
	for _, kind := range []string{"config_ack", "command_result"} {
		key := nodeID + "|" + kind
		s.mu.Lock()
		list := s.waiters[key]
		for i, c := range list {
			if c == ch {
				s.waiters[key] = append(list[:i], list[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) deliver(nodeID, kind string, v any) {
	key := nodeID + "|" + kind
	s.mu.Lock()
	list := s.waiters[key]
	chans := append([]chan any(nil), list...)
	s.mu.Unlock()
	for _, c := range chans {
		select {
		case c <- v:
		default:
		}
	}
}

// ---------------------------- 后台任务 ----------------------------

// offlineSweeper 把超时未心跳的节点标记为离线。
func (s *Server) offlineSweeper(ctx context.Context) {
	interval := time.Duration(s.cfg.Heartbeat.TimeoutSeconds) * time.Second / 3
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			now := time.Now().UnixMilli()
			limit := now - int64(s.cfg.Heartbeat.TimeoutSeconds)*1000
			for _, n := range s.store.Nodes() {
				if !n.Connected {
					continue
				}
				if n.LastSeenMs > 0 && n.LastSeenMs < limit {
					s.store.Mutate(n.ID, func(x *Node) { x.Connected = false })
					s.store.AppendEvent(Event{NodeID: n.ID, Kind: "conn", Type: "timeout",
						Message: fmt.Sprintf("超过 %ds 没有心跳，判定为离线", s.cfg.Heartbeat.TimeoutSeconds)})
					s.log.Warn("节点心跳超时", "node_id", n.ID, "last_seen_ms", n.LastSeenMs)
				}
			}
		}
	}
}

// pendingResender 定期重发未确认的配置（节点重连后自行拉取，这里兜住"下发时刚好断线"的情况）。
func (s *Server) pendingResender(ctx context.Context) {
	tk := time.NewTicker(30 * time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			for _, n := range s.store.Nodes() {
				p := s.store.Pending(n.ID)
				if p == nil || !n.Connected {
					continue
				}
				if time.Now().UnixMilli()-p.LastSentMs < 30_000 {
					continue
				}
				p.LastSentMs = time.Now().UnixMilli()
				p.Attempts++
				if p.Attempts > 20 {
					s.store.ClearPending(n.ID)
					s.log.Warn("配置下发重试次数过多，放弃", "node_id", n.ID, "revision", p.Revision)
					continue
				}
				s.store.SetPending(n.ID, p)
				if err := s.sendConfig(n.ID, p); err != nil {
					s.log.Warn("重发配置失败", "node_id", n.ID, "err", err)
					continue
				}
				s.log.Info("重发未确认的配置", "node_id", n.ID, "revision", p.Revision, "attempt", p.Attempts)
			}
		}
	}
}

// ---------------------------- 工具 ----------------------------

func (m *Metrics) bump(what string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch what {
	case "enroll":
		m.EnrollTotal++
	case "enroll_failed":
		m.EnrollFailed++
	case "ws_connect":
		m.WSConnected++
	case "ws_rejected":
		m.WSRejected++
	case "heartbeat":
		m.Heartbeats++
	case "duplicate":
		m.Duplicates++
	case "command_sent":
		m.CommandsSent++
	case "command_done":
		m.CommandsDone++
	case "push_sent":
		m.PushesSent++
	case "push_applied":
		m.PushApplied++
	case "push_failed":
		m.PushFailed++
	}
}

func mask(t string) string {
	if len(t) <= 8 {
		return "***"
	}
	return t[:8] + "***"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg, "status": status})
}

func orJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("null")
	}
	return b
}
