// Package control 实现与控制面的长连接控制通道。
//
// 传输：WSS（TLS，带 SPKI pinning）+ protobuf 信封，一条 TCP 上多路复用所有逻辑流
// （心跳、配置下发、指令、日志、指标），用 Envelope.seq + EventAck 做可靠投递。
//
// 可靠性设计：
//   - 需要可靠投递的消息先落 outbox（本地 JSONL），再发送；控制面累积确认后删除；
//   - 断线期间产生的事件留在 outbox，重连后从 resend_from_seq 补发；
//   - 重连用指数退避 + 抖动，上限 5 分钟（control.reconnect.max_seconds）；
//   - 心跳 30s 一次，90s 收不到任何服务端消息即判定断线并重连。
package control

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/outbox"
	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

// Handler 是控制面下发消息的处理器（由 agent 包实现）。
type Handler interface {
	// HandleConfigPush 处理配置下发；实现里负责把 ConfigAck 经 Enqueue 送回。
	HandleConfigPush(ctx context.Context, cp *agentpb.ConfigPush)
	// HandleCommand 处理远程指令；实现里自行 Enqueue CommandResult/CommandChunk。
	HandleCommand(ctx context.Context, req *agentpb.CommandRequest)
	// HandleHelloAck 处理握手完成事件（刷新间隔、记录 server revision）。
	HandleHelloAck(ctx context.Context, ack *agentpb.HelloAck)
	// HandleUpgradeOffer 处理控制面主动下发的升级提议。
	HandleUpgradeOffer(ctx context.Context, offer *agentpb.UpgradeOffer)
}

// Status 是控制通道状态快照（status 命令 / /readyz / 诊断用）。
type Status struct {
	Connected      bool      `json:"connected"`
	ConnectedSince time.Time `json:"connected_since,omitempty"`
	Reconnects     uint64    `json:"reconnects"`
	LastAckSeq     uint64    `json:"last_acked_seq"`
	LastServerAck  time.Time `json:"last_ack_at,omitempty"`
	PendingEvents  int       `json:"pending_events"`
	PendingBytes   int64     `json:"pending_bytes"`
	DroppedEvents  uint64    `json:"dropped_events"`
	ServerRevision string    `json:"server_config_revision,omitempty"`
	ServerSkewMs   int64     `json:"server_skew_ms"`
	HeartbeatEvery float64   `json:"heartbeat_interval_seconds"`
	TelemetryEvery float64   `json:"telemetry_interval_seconds"`
	LastError      string    `json:"last_error,omitempty"`
	Endpoint       string    `json:"endpoint"`
}

// Client 是控制通道客户端。
type Client struct {
	cfg *config.Config
	cr  *creds.Credentials
	log *slog.Logger
	m   *metrics.Metrics
	ob  *outbox.Outbox
	hub *logging.Hub
	h   Handler

	mu              sync.Mutex
	sess            *session
	connectedSince  time.Time
	reconnects      uint64
	lastErr         string
	serverRevision  string
	skewMs          int64
	heartbeatEvery  time.Duration
	telemetryEvery  time.Duration
	resendFromSeq   uint64
	lastHeartbeatAt time.Time
	lastHelloAt     time.Time
	helloAcked      bool

	// 由 agent 注入的握手/状态元数据（Hello 与心跳要用）
	startedAt       time.Time
	hysteriaVersion string
	configRevision  string
	configSHA256    string
	hysteriaRunning bool
	ready           bool
	publicIPAddr    string
	lastAckAt       time.Time
	firstResendMark uint64
	lastDropped     uint64

	// REST 兜底：控制通道端点不可用（404/405/501，例如 Laravel 面板）时的降级状态
	restFallback bool
	lastSeen     time.Time
	// restConfigVersion：兜底下最近一次取到的配置版本（面板按 int 版本号比较）
	restConfigVersion int
	// driftWarned：已经提示过的漂移（避免心跳周期内刷屏）
	driftWarned map[string]bool
}

// New 构建客户端。
func New(cfg *config.Config, cr *creds.Credentials, log *slog.Logger, m *metrics.Metrics, ob *outbox.Outbox, hub *logging.Hub, h Handler) *Client {
	return &Client{
		cfg: cfg, cr: cr, log: log, m: m, ob: ob, hub: hub, h: h,
		heartbeatEvery: cfg.Control.HeartbeatInterval(),
		telemetryEvery: cfg.Control.TelemetryInterval(),
		startedAt:      time.Now(),
	}
}

// TelemetryInterval 返回控制面协商后的遥测间隔。
// heartbeatDuration 返回当前生效的心跳间隔（带下限保护，避免被对端设成 0 导致忙循环）。
func (c *Client) heartbeatDuration() time.Duration {
	c.mu.Lock()
	d := c.heartbeatEvery
	c.mu.Unlock()
	if d < time.Second {
		d = 30 * time.Second
	}
	return d
}

func (c *Client) TelemetryInterval() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.telemetryEvery
}

// Status 返回状态快照。
func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, bytes, dropped, acked, _ := c.ob.Stat()
	return Status{
		Connected:      c.sess != nil,
		ConnectedSince: c.connectedSince,
		Reconnects:     c.reconnects,
		LastAckSeq:     acked,
		PendingEvents:  pending,
		PendingBytes:   bytes,
		DroppedEvents:  dropped,
		ServerRevision: c.serverRevision,
		ServerSkewMs:   c.skewMs,
		HeartbeatEvery: c.heartbeatEvery.Seconds(),
		TelemetryEvery: c.telemetryEvery.Seconds(),
		LastError:      c.lastErr,
		Endpoint:       c.cfg.ControlWSURL(),
	}
}

// Connected 报告当前是否已连接。
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess != nil
}

// SkewMs 返回与控制面的时钟偏移。
func (c *Client) SkewMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.skewMs
}

// ServerRevision 返回控制面声明的最新配置版本。
func (c *Client) ServerRevision() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serverRevision
}

// Enqueue 把一个需要可靠投递的信封写进 outbox 并触发发送。
// 断线时它只落盘，重连后自动补发——调用方不需要关心连接状态。
func (c *Client) Enqueue(kind string, env *agentpb.Envelope) (uint64, error) {
	env.NodeId = c.cr.NodeID
	if env.TsMs == 0 {
		env.TsMs = time.Now().UnixMilli()
	}

	// REST 兜底模式：配置回执没有 WS 可发，落 outbox 只会永远躺着。直接走 REST 回执。
	if ack := env.GetConfigAck(); ack != nil && c.RESTFallbackActive() {
		return 0, c.sendRESTConfigAck(context.Background(), ack)
	}
	seq, err := c.ob.Append(env, kind)
	if err != nil {
		return 0, err
	}
	pending, bytes, dropped, _, _ := c.ob.Stat()
	c.m.EventsBuffered.Set(float64(pending))
	c.m.OutboxBytes.Set(float64(bytes))
	c.mu.Lock()
	if dropped > c.lastDropped {
		c.m.EventsDropped.Add(float64(dropped - c.lastDropped))
		c.lastDropped = dropped
	}
	c.mu.Unlock()

	c.mu.Lock()
	s := c.sess
	c.mu.Unlock()
	if s != nil {
		s.nudge()
	}
	return seq, nil
}

// sendLive 发送不需要 ack 的实时消息（心跳）。
func (c *Client) sendLive(env *agentpb.Envelope) error {
	c.mu.Lock()
	s := c.sess
	c.mu.Unlock()
	if s == nil {
		return fmt.Errorf("控制通道未连接")
	}
	env.NodeId = c.cr.NodeID
	env.TsMs = time.Now().UnixMilli()
	return s.write(env)
}

// Run 持续维持连接直到 ctx 结束。每次会话失败后按指数退避重连。
func (c *Client) Run(ctx context.Context) {
	backoff := c.cfg.Control.Reconnect.InitialSeconds
	maxBackoff := c.cfg.Control.Reconnect.MaxSeconds
	mult := c.cfg.Control.Reconnect.Multiplier
	if mult <= 1 {
		mult = 2
	}
	jitter := c.cfg.Control.Reconnect.Jitter

	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.log.Warn("控制通道断开", slog.String("error", err.Error()),
				slog.Float64("session_seconds", time.Since(started).Seconds()))
			c.mu.Lock()
			c.lastErr = err.Error()
			c.mu.Unlock()

			// 对端根本没实现控制通道（404/405/501）：退化为 REST 心跳，
			// 否则节点除注册外与控制面再无往来，面板「最后心跳」永远不动。
			if shouldFallBack(err, c.cfg) {
				c.runRESTFallback(ctx, err)
				if ctx.Err() != nil {
					return
				}
			}
		}
		c.mu.Lock()
		c.sess = nil
		c.helloAcked = false
		c.connectedSince = time.Time{}
		c.mu.Unlock()
		c.m.ControlConnected.Set(0)

		// 连接活过 30s 就从最小退避重新开始（说明对端是短暂抖动而不是持续故障）。
		if time.Since(started) > 30*time.Second {
			backoff = c.cfg.Control.Reconnect.InitialSeconds
		}
		delay := time.Duration(backoff * float64(time.Second))
		if jitter > 0 {
			f := 1 + (rand.Float64()*2-1)*jitter
			delay = time.Duration(float64(delay) * f)
		}
		if delay < time.Second {
			delay = time.Second
		}
		c.log.Info("准备重连", slog.Float64("delay_seconds", delay.Seconds()))
		c.m.ControlReconnects.Inc()
		c.mu.Lock()
		c.reconnects++
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		backoff *= mult
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// ---------------------------- 一次会话 ----------------------------

type session struct {
	c             *Client
	conn          *websocket.Conn
	writeM        sync.Mutex
	done          chan struct{}
	once          sync.Once
	notify        chan struct{}
	hbChanged     chan struct{} // HelloAck 改了心跳间隔时用来立刻重排定时器
	lastServerMsg time.Time
}

func (s *session) close() { s.once.Do(func() { close(s.done) }) }

func (s *session) nudge() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// nudgeHeartbeat 让写循环用新的心跳间隔立刻重排（HelloAck 协商后会调用）。
func (s *session) nudgeHeartbeat() {
	select {
	case s.hbChanged <- struct{}{}:
	default:
	}
}

func (s *session) write(env *agentpb.Envelope) error {
	b, err := wire.Marshal(env)
	if err != nil {
		return err
	}
	s.writeM.Lock()
	defer s.writeM.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.c.cfg.Control.WriteTimeout()))
	return s.conn.WriteMessage(websocket.BinaryMessage, b)
}

func (c *Client) session(ctx context.Context) error {
	tlsCfg, err := c.cr.TLSConfig(&c.cfg.Control, func(m string) { c.log.Warn(m) })
	if err != nil {
		return err
	}
	dialer := &websocket.Dialer{
		HandshakeTimeout: c.cfg.Control.HandshakeTimeout(),
		TLSClientConfig:  tlsCfg,
		ReadBufferSize:   1 << 16,
		WriteBufferSize:  1 << 16,
	}
	token, exp, err := c.cr.BearerHeader(c.cfg.Control.JWTLifetime())
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("Authorization", token)
	header.Set("X-Node-ID", c.cr.NodeID)
	header.Set("User-Agent", c.cfg.Control.UserAgent)
	header.Set("X-Agent-Version", version.Version)
	header.Set("X-Agent-Protocol", "v1")

	endpoint := c.cfg.ControlWSURL()
	c.log.Info("连接控制面", slog.String("endpoint", endpoint),
		slog.String("jwt_expires", exp.Format(time.RFC3339)))

	dctx, cancel := context.WithTimeout(ctx, c.cfg.Control.ConnectTimeout())
	defer cancel()
	conn, resp, err := dialer.DialContext(dctx, endpoint, header)
	if err != nil {
		if resp != nil {
			// 带上状态码：404/405/501 表示对端**没有实现**这个端点（例如 Laravel 面板
			// 终结不了二进制 WS），此时应退化为 REST 心跳而不是无限重试握手。
			return &HandshakeError{Status: resp.StatusCode, Err: err}
		}
		return fmt.Errorf("拨号失败: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(c.cfg.Control.MaxMessageBytes)

	s := &session{c: c, conn: conn, done: make(chan struct{}), notify: make(chan struct{}, 1),
		hbChanged: make(chan struct{}, 1), lastServerMsg: time.Now()}

	// ctx 结束或会话被关闭时立刻断开底层连接。
	// 少了这一步：读循环会卡在 ReadMessage 里直到读超时（默认 90s），
	// Agent 的优雅停止要等它，systemd 会以为进程卡死（E2E 里真实踩到：
	// 收到 SIGTERM 后 Hysteria 都停了，Agent 还要拖很久才退出）。
	go func() {
		select {
		case <-ctx.Done():
		case <-s.done:
		}
		_ = conn.Close()
	}()

	conn.SetPongHandler(func(string) error {
		s.lastServerMsg = time.Now()
		return conn.SetReadDeadline(time.Now().Add(c.cfg.Control.HeartbeatTimeout()))
	})
	conn.SetPingHandler(func(appData string) error {
		s.lastServerMsg = time.Now()
		_ = conn.SetReadDeadline(time.Now().Add(c.cfg.Control.HeartbeatTimeout()))
		s.writeM.Lock()
		defer s.writeM.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})
	if err := conn.SetReadDeadline(time.Now().Add(c.cfg.Control.HeartbeatTimeout())); err != nil {
		return err
	}

	c.mu.Lock()
	c.sess = s
	c.connectedSince = time.Now()
	c.mu.Unlock()
	c.m.ControlConnected.Set(1)
	c.m.Snapshot(c.connectedSince)

	// 1) Hello（带上 outbox 里最小未确认 seq，控制面据此决定补发起点）
	_, _, _, acked, _ := c.ob.Stat()
	resumeFrom := acked + 1
	if entries := c.ob.Snapshot(); len(entries) > 0 && entries[0].Seq < resumeFrom {
		resumeFrom = entries[0].Seq
	}
	c.mu.Lock()
	c.firstResendMark = acked + 1
	c.mu.Unlock()
	if err := s.write(wire.New(agentpb.MsgType_MSG_TYPE_HELLO, c.buildHello(resumeFrom, acked))); err != nil {
		return err
	}
	c.mu.Lock()
	c.lastHelloAt = time.Now()
	c.mu.Unlock()

	// 2) 写循环：心跳 + 补发 + 实时消息
	go func() {
		// 用 Timer 而不是 Ticker：HelloAck 可以把心跳间隔改小（例如 30s→5s），
		// 用 Ticker 的话新间隔要等到下一次重连才生效。
		heartbeat := time.NewTimer(c.heartbeatDuration())
		defer heartbeat.Stop()
		flush := time.NewTicker(time.Second)
		defer flush.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ctx.Done():
				s.close()
				return
			case <-heartbeat.C:
				if err := s.sendHeartbeat(ctx); err != nil {
					c.log.Warn("心跳发送失败", slog.String("error", err.Error()))
					s.close()
					return
				}
				heartbeat.Reset(c.heartbeatDuration())
			case <-s.hbChanged:
				if !heartbeat.Stop() {
					select {
					case <-heartbeat.C:
					default:
					}
				}
				heartbeat.Reset(c.heartbeatDuration())
			case <-flush.C:
				if err := s.flushPending(); err != nil {
					c.log.Warn("补发事件失败", slog.String("error", err.Error()))
					s.close()
					return
				}
			case <-s.notify:
				if err := s.flushPending(); err != nil {
					c.log.Warn("发送事件失败", slog.String("error", err.Error()))
					s.close()
					return
				}
			}
		}
	}()

	// 3) 读循环
	var readErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return readErr
		default:
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			readErr = fmt.Errorf("读取消息失败: %w", err)
			s.close()
			return readErr
		}
		s.lastServerMsg = time.Now()
		_ = conn.SetReadDeadline(time.Now().Add(c.cfg.Control.HeartbeatTimeout()))
		env, err := wire.Unmarshal(raw)
		if err != nil {
			c.log.Warn("丢弃无法解析的消息", slog.String("error", err.Error()))
			continue
		}
		c.dispatch(ctx, s, env)
	}
}

func (c *Client) buildHello(resumeFrom, acked uint64) *agentpb.Hello {
	return &agentpb.Hello{
		NodeId:          c.cr.NodeID,
		AgentVersion:    version.Version,
		HysteriaVersion: c.hysteriaVersion,
		Hostname:        sysinfo.Hostname(),
		Os:              sysinfo.GOOS(),
		Arch:            sysinfo.Arch(),
		Kernel:          sysinfo.Kernel(),
		BootId:          sysinfo.BootID(),
		ResumeFromSeq:   resumeFrom,
		LastAckedSeq:    acked,
		Capabilities:    wire.CommandWhitelist(),
		StartTimeMs:     c.startedAt.UnixMilli(),
		ConfigSha256:    c.configSHA256,
		ConfigRevision:  c.configRevision,
		HysteriaRunning: c.hysteriaRunning,
	}
}

// 由 agent 在 Run 前注入的握手元数据。
var _ = 0

func (s *session) sendHeartbeat(ctx context.Context) error {
	c := s.c
	pending, _, _, acked, last := c.ob.Stat()
	hb := &agentpb.Heartbeat{
		TsMs:            time.Now().UnixMilli(),
		UptimeSeconds:   uint64(time.Since(c.startedAt).Seconds()),
		PendingEvents:   uint64(pending),
		HysteriaRunning: c.hysteriaRunning,
		Ready:           c.ready,
		ConfigRevision:  c.configRevision,
		LastAckedSeq:    acked,
		SentEvents:      last,
	}
	c.m.HeartbeatSent.Inc()
	c.mu.Lock()
	c.lastHeartbeatAt = time.Now()
	c.mu.Unlock()
	return s.write(wire.New(agentpb.MsgType_MSG_TYPE_HEARTBEAT, hb))
}

// flushPending 把 outbox 里未确认的事件按序发出（断线补发也走这里）。
//
// 关键约束：**只要本地还持有某个事件，就一定要发出去**。
// 早些的实现在这里用 `acked_seq+1` 作为起点，一旦水位线与实际持有的条目
// 出现不一致（例如多个进程共用同一个 state_dir），积压的事件就会永远发不出去。
// 重复投递是协议允许的（at-least-once + 控制面按 seq 幂等），漏投则不可接受。
func (s *session) flushPending() error {
	c := s.c
	c.mu.Lock()
	from := c.resendFromSeq
	c.mu.Unlock()

	entries := c.ob.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	if from == 0 {
		_, _, _, acked, _ := c.ob.Stat()
		from = acked + 1
	}
	// 本地持有的最旧事件比水位线更旧时，以它为准。
	if entries[0].Seq < from {
		from = entries[0].Seq
	}
	pending := make([]*outbox.Entry, 0, len(entries))
	for _, e := range entries {
		if e.Seq >= from {
			pending = append(pending, e)
		}
	}

	// 每轮最多发 512 条，避免一次性冲爆对端（下一轮 tick 会继续）。
	const maxBatch = 512
	sent := 0
	for _, e := range pending {
		if sent >= maxBatch {
			s.nudge()
			break
		}
		env := e.Envelope()
		if env == nil {
			continue
		}
		if err := s.write(env); err != nil {
			return err
		}
		sent++
	}
	if sent > 0 {
		c.m.EventsSent.Add(float64(sent))
		if from <= c.firstResendMark {
			c.m.EventsResent.Add(float64(sent))
		}
	}
	c.mu.Lock()
	c.resendFromSeq = 0
	c.mu.Unlock()
	return nil
}

// dispatch 处理一条来自控制面的消息。
func (c *Client) dispatch(ctx context.Context, s *session, env *agentpb.Envelope) {
	tid := env.GetTraceId()
	if tid != "" {
		ctx = logging.WithTrace(ctx, tid)
	}
	switch b := env.GetBody().(type) {
	case *agentpb.Envelope_HelloAck:
		ack := b.HelloAck
		c.applyHelloAck(ack, s)
		c.log.Info("握手完成",
			slog.String("server_version", ack.GetServerVersion()),
			slog.String("server_config_revision", ack.GetConfigRevision()),
			slog.Uint64("resend_from_seq", ack.GetResendFromSeq()))
		c.mu.Lock()
		c.resendFromSeq = ack.GetResendFromSeq()
		if c.resendFromSeq == 0 {
			_, _, _, acked, _ := c.ob.Stat()
			c.resendFromSeq = acked + 1
		}
		c.helloAcked = true
		c.mu.Unlock()
		// hello 成功即重置退避（连接可用）。
		s.nudge()
		if c.h != nil {
			c.h.HandleHelloAck(ctx, ack)
		}

	case *agentpb.Envelope_HeartbeatAck:
		c.m.HeartbeatAcked.Inc()
		c.m.LastHeartbeat.Set(float64(time.Now().Unix()))
		if rev := b.HeartbeatAck.GetConfigRevision(); rev != "" && rev != c.ServerRevision() {
			c.mu.Lock()
			c.serverRevision = rev
			c.mu.Unlock()
			c.log.Info("控制面声明了新的配置版本（等待下发）", slog.String("revision", rev))
		}

	case *agentpb.Envelope_EventAck:
		seq := b.EventAck.GetAckedSeq()
		if removed := c.ob.Ack(seq); removed > 0 {
			pending, bytes, _, _, _ := c.ob.Stat()
			c.m.EventsBuffered.Set(float64(pending))
			c.m.OutboxBytes.Set(float64(bytes))
			c.mu.Lock()
			c.lastAckAt = time.Now()
			c.mu.Unlock()
		}

	case *agentpb.Envelope_ConfigPush:
		go c.safeHandle("config_push", ctx, func(ctx context.Context) {
			c.h.HandleConfigPush(ctx, b.ConfigPush)
		})

	case *agentpb.Envelope_Command:
		go c.safeHandle("command", ctx, func(ctx context.Context) {
			c.h.HandleCommand(ctx, b.Command)
		})

	case *agentpb.Envelope_UpgradeOffer:
		go c.safeHandle("upgrade_offer", ctx, func(ctx context.Context) {
			c.h.HandleUpgradeOffer(ctx, b.UpgradeOffer)
		})

	case *agentpb.Envelope_Error:
		c.log.Warn("控制面返回错误",
			slog.String("code", b.Error.GetCode()),
			slog.String("message", b.Error.GetMessage()),
			slog.Bool("fatal", b.Error.GetFatal()))

	default:
		c.log.Debug("忽略未处理的消息类型", slog.String("type", wire.TypeName(env.GetType())))
	}
}

// safeHandle 保证处理器 panic 不影响连接读循环。
func (c *Client) safeHandle(name string, ctx context.Context, fn func(context.Context)) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("处理器 panic", slog.String("handler", name), slog.Any("panic", r))
		}
	}()
	fn(ctx)
}

func (c *Client) applyHelloAck(ack *agentpb.HelloAck, s *session) {
	c.mu.Lock()
	changed := false
	if v := ack.GetHeartbeatIntervalSeconds(); v >= 5 {
		if d := time.Duration(v) * time.Second; d != c.heartbeatEvery {
			c.heartbeatEvery = d
			changed = true
		}
	}
	if v := ack.GetTelemetryIntervalSeconds(); v >= 1 {
		c.telemetryEvery = time.Duration(v) * time.Second
	}
	if v := ack.GetServerTimeMs(); v > 0 {
		c.skewMs = time.Now().UnixMilli() - v
	}
	if r := ack.GetConfigRevision(); r != "" {
		c.serverRevision = r
	}
	c.mu.Unlock()
	if changed && s != nil {
		// 立刻按新间隔重排心跳定时器（否则要等到下一次重连才生效）
		s.nudgeHeartbeat()
	}
}

// HandshakeError 控制通道握手被对端拒绝。
type HandshakeError struct {
	Status int
	Err    error
}

func (e *HandshakeError) Error() string {
	return fmt.Sprintf("握手失败 HTTP %d: %v", e.Status, e.Err)
}

func (e *HandshakeError) Unwrap() error {
	return e.Err
}

// Unimplemented 表示对端没有实现控制通道端点（404/405/501），可以走 REST 兜底。
func (e *HandshakeError) Unimplemented() bool {
	switch e.Status {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

// ---------------------------- 由 agent 注入的握手/状态钩子 ----------------------------

// SetHandshakeMeta 在 Run 之前调用，把 Hello 需要的运行态元数据交给客户端。
func (c *Client) SetHandshakeMeta(hysteriaVersion string, configRevision, configSHA256 string, hysteriaRunning bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hysteriaVersion = hysteriaVersion
	c.configRevision = configRevision
	c.configSHA256 = configSHA256
	c.hysteriaRunning = hysteriaRunning
}

// SetState 供 agent 在状态变化时刷新（心跳会带上）。
func (c *Client) SetState(ready, hysteriaRunning bool, configRevision, configSHA256 string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = ready
	c.hysteriaRunning = hysteriaRunning
	c.configRevision = configRevision
	c.configSHA256 = configSHA256
}

// RequireAuthRoundTrip 用一个最小的 API 探测确认 JWT 是否被接受（enroll 后自检）。
func (c *Client) RequireAuthRoundTrip(ctx context.Context) error {
	u := c.cfg.ControlHTTPURL(c.cfg.Control.TokenPath)
	client := &http.Client{Timeout: c.cfg.Control.ConnectTimeout()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if err := c.cr.Apply(req, &c.cfg.Control); err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("JWT 自检失败: HTTP %d", resp.StatusCode)
	}
	return nil
}

// SetPublicIP 记录自报的出口公网地址（REST 心跳会带上；控制面在隧道后只能看到代理地址）。
func (c *Client) SetPublicIP(ip string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.publicIPAddr = strings.TrimSpace(ip)
}

func (c *Client) publicIP() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.publicIPAddr
}

func (c *Client) hysteriaVersionSnapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.hysteriaVersion
}

func (c *Client) configRevisionSnapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.configRevision
}

func (c *Client) readySnapshot() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ready
}

func (c *Client) hysteriaRunningSnapshot() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.hysteriaRunning
}

// MarshalStatus 便于 CLI 输出。
func (s Status) Marshal() []byte {
	b, _ := json.MarshalIndent(s, "", "  ")
	return b
}
