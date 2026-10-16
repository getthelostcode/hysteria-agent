// mockcontrol 是一个"最小可运行的控制面参考实现"，用于本地联调与端到端测试。
//
// 它同时是协议的第二实现：Agent 的协议改动能立刻在这里跑通，避免"只有客户端"
// 的自说自话。生产控制面（Go / Python / PHP）可以照这个逻辑实现：
//
//	POST /api/v1/agent/enroll     注册，签发 node_id + node_secret
//	GET  /api/v1/agent/control    WebSocket 控制通道（Authorization: Bearer <JWT>）
//
// 测试辅助接口（-test-api 打开，仅用于 E2E，不要暴露到公网）：
//
//	GET  /_test/state             状态与统计
//	POST /_test/push_config       下发配置（body=配置正文，query: revision/format/dry_run）
//	POST /_test/command           下发指令（query: type，body=params JSON）
//	POST /_test/ack_all           累积确认已收到的所有事件
//	POST /_test/upgrade_offer     下发升级提议（body=manifest JSON）
//	GET  /_test/dump              返回收到的所有消息（JSONL）
//	GET  /_test/files/<name>      暴露 -files-dir 下的文件（自升级下载用）
//	POST /_test/set_default_config 改「配置版本文件库的默认版本」（body=新正文；用于验证节点自动收敛）
//
// REST 兜底面（与真实面板 /api/v1/agent/* 同构；-rest-only 时 /control 返回 501，模拟 Laravel 面板）：
//
//	GET  /api/v1/agent/config/default   配置版本文件库的默认版本（current_version/current_sha256 参与判断）
//	GET  /api/v1/agent/config/latest    本节点发布的版本（这里恒为“没有”）
//	POST /api/v1/agent/config/ack       配置回执（记录 status/applied）
//	POST /api/v1/agent/heartbeat        REST 兜底心跳
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

type nodeState struct {
	NodeID     string
	Secret     string
	Name       string
	Conn       *websocket.Conn
	WriteM     sync.Mutex
	AckedSeq   uint64
	Connected  bool
	LastHello  *agentpb.Hello
	HelloAcked bool
	Hellos     int

	Counts   map[string]int
	Seqs     []uint64
	Received int
	BadSeqs  []uint64
}

type server struct {
	mu     sync.Mutex
	nodes  map[string]*nodeState
	byName map[string]*nodeState

	enrollToken string
	dumpPath    string
	filesDir    string
	autoAck     bool
	hbInterval  int
	telInterval int
	revision    string
	pushes      int
	commands    []string
	heartbeats  int
	resends     int

	start time.Time

	// restOnly：/api/v1/agent/control 返回 501，强制 Agent 走 REST 兜底（对齐真实 Laravel 面板）
	restOnly bool
	// defaultConfig：配置版本文件库的默认版本正文（节点应当自动收敛到它）
	defaultConfig string
	defaultSHA    string
	defaultVer    int
	acks          []map[string]any
}

// setDefaultConfig 设置配置版本文件库的默认版本（版本号自增，sha256 由正文算出）。
func (s *server) setDefaultConfig(content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultConfig = content
	s.defaultVer++
	sum := sha256.Sum256([]byte(content))
	s.defaultSHA = hex.EncodeToString(sum[:])
}

func (s *server) defaultConfigSnapshot() (string, string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.defaultConfig, s.defaultSHA, s.defaultVer
}

func (s *server) handleSetDefaultConfig(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), 400)

		return
	}
	s.setDefaultConfig(string(body))
	_, sum, ver := s.defaultConfigSnapshot()
	fmt.Fprintf(w, "version=%d sha256=%s\n", ver, sum)
}

func (s *server) handleRESTHeartbeat(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.heartbeats++
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "node_id": "mock", "server_time": time.Now().UTC().Format(time.RFC3339),
		"heartbeat_interval_seconds": s.hbInterval, "telemetry_interval_seconds": s.telInterval,
		"config": map[string]any{"version": nil, "up_to_date": true},
	})
}

// handleRESTConfigLatest：本节点发布的版本 —— mock 里恒为「没有」，
// 这样节点只能靠 /config/default（版本文件库的默认版本）收敛，正好覆盖用户要的那条路。
func (s *server) handleRESTConfigLatest(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "up_to_date": true, "config": nil})
}

func (s *server) handleRESTConfigDefault(w http.ResponseWriter, r *http.Request) {
	content, sum, ver := s.defaultConfigSnapshot()
	if content == "" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "up_to_date": true, "config": nil,
			"note": "配置版本文件库里还没有默认版本。"})

		return
	}

	curVer := atoiSafe(r.URL.Query().Get("current_version"))
	curSha := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("current_sha256")))
	if curVer == ver && (curSha == "" || curSha == sum) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "up_to_date": true,
			"config": map[string]any{"version": ver, "sha256": sum}})

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "up_to_date": false, "config": map[string]any{
		"version": ver, "sha256": sum, "revision": fmt.Sprint(ver),
		"content_yaml": content, "source": "config-file",
	}})
}

func (s *server) handleRESTConfigAck(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	s.mu.Lock()
	s.acks = append(s.acks, body)
	s.mu.Unlock()
	fmt.Fprintf(os.Stderr, "[mockcontrol] config ack: %v\n", body)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}

	return n
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 16,
	WriteBufferSize: 1 << 16,
	// 测试环境用 ws://，不校验 Origin。
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "监听地址")
	token := flag.String("enroll-token", "e2e-enroll-token", "注册令牌")
	dump := flag.String("dump", "/tmp/mockcontrol-dump.jsonl", "收到消息的落盘文件（JSONL）")
	filesDir := flag.String("files-dir", "", "静态文件目录（自升级二进制下载）")
	autoAck := flag.Bool("auto-ack", true, "是否自动累积确认事件")
	hbInterval := flag.Int("heartbeat-interval", 5, "下发给 Agent 的心跳间隔（秒）")
	telInterval := flag.Int("telemetry-interval", 2, "下发给 Agent 的遥测间隔（秒）")
	restOnly := flag.Bool("rest-only", false, "让 /api/v1/agent/control 返回 501，强制 Agent 走 REST 兜底（对齐真实面板）")
	defaultConfigFile := flag.String("default-config", "", "配置版本文件库的默认版本正文文件（节点会自动收敛到它）")
	flag.Parse()

	s := &server{
		nodes: map[string]*nodeState{}, byName: map[string]*nodeState{},
		enrollToken: *token, dumpPath: *dump, filesDir: *filesDir,
		autoAck: *autoAck, hbInterval: *hbInterval, telInterval: *telInterval,
		restOnly: *restOnly,
		start:    time.Now(),
	}
	if *defaultConfigFile != "" {
		b, err := os.ReadFile(*defaultConfigFile)
		if err != nil {
			log.Fatalf("读取 default-config 失败: %v", err)
		}
		s.setDefaultConfig(string(b))
	}
	if err := os.WriteFile(*dump, nil, 0o644); err != nil {
		log.Fatalf("初始化 dump 文件失败: %v", err)
	}
	s.loadNodes()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/agent/enroll", s.handleEnroll)
	mux.HandleFunc("/api/v1/agent/control", s.handleControl)
	mux.HandleFunc("/_test/state", s.handleState)
	mux.HandleFunc("/_test/push_config", s.handlePushConfig)
	mux.HandleFunc("/_test/command", s.handleCommand)
	mux.HandleFunc("/_test/ack_all", s.handleAckAll)
	mux.HandleFunc("/_test/set_default_config", s.handleSetDefaultConfig)
	// REST 兜底面（与真实面板同构）
	mux.HandleFunc("/api/v1/agent/heartbeat", s.handleRESTHeartbeat)
	mux.HandleFunc("/api/v1/agent/config/latest", s.handleRESTConfigLatest)
	mux.HandleFunc("/api/v1/agent/config/default", s.handleRESTConfigDefault)
	mux.HandleFunc("/api/v1/agent/config/ack", s.handleRESTConfigAck)
	mux.HandleFunc("/_test/upgrade_offer", s.handleUpgradeOffer)
	mux.HandleFunc("/_test/dump", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, s.dumpPath)
	})
	mux.HandleFunc("/_test/files/", func(w http.ResponseWriter, r *http.Request) {
		if s.filesDir == "" {
			http.Error(w, "no files dir", 404)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/_test/files/")
		http.ServeFile(w, r, filepath.Join(s.filesDir, filepath.Base(name)))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("mockcontrol 监听 %s（enroll_token=%s, auto_ack=%v, heartbeat=%ds, telemetry=%ds）",
		*listen, *token, *autoAck, *hbInterval, *telInterval)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatalf("监听失败: %v", err)
	}
}

// ---------------------------- 注册 ----------------------------

type enrollReq struct {
	EnrollToken     string `json:"enroll_token"`
	NodeName        string `json:"node_name"`
	Hostname        string `json:"hostname"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	Kernel          string `json:"kernel"`
	MachineID       string `json:"machine_id"`
	BootID          string `json:"boot_id"`
	AgentVersion    string `json:"agent_version"`
	HysteriaVersion string `json:"hysteria_version"`
	Protocol        string `json:"protocol"`
	Replace         bool   `json:"replace"`
}

func (s *server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 令牌可以放在 body 或 X-Enroll-Token 头里，两种都支持。
	tok := r.Header.Get("X-Enroll-Token")
	var req enrollReq
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if tok == "" {
		tok = req.EnrollToken
	}
	if tok != s.enrollToken {
		log.Printf("注册被拒: 令牌不匹配")
		http.Error(w, `{"error":"invalid enroll token"}`, http.StatusUnauthorized)
		return
	}
	name := req.NodeName
	if name == "" {
		name = req.Hostname
	}

	s.mu.Lock()
	n := s.byName[name]
	if n == nil || req.Replace {
		n = &nodeState{
			NodeID: fmt.Sprintf("node_%06d", rand.Intn(999999)),
			Secret: randomSecret(),
			Name:   name,
			Counts: map[string]int{},
		}
		s.nodes[n.NodeID] = n
		s.byName[name] = n
	}
	s.mu.Unlock()
	s.saveNodes()

	log.Printf("注册成功: node_id=%s name=%s hostname=%s agent=%s hysteria=%s",
		n.NodeID, name, req.Hostname, req.AgentVersion, req.HysteriaVersion)

	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":                    n.NodeID,
		"node_secret":                n.Secret,
		"node_name":                  n.Name,
		"protocol":                   "v1",
		"heartbeat_interval_seconds": s.hbInterval,
		"telemetry_interval_seconds": s.telInterval,
		"message":                    "mock control plane",
	})
}

func randomSecret() string {
	const chars = "0123456789abcdef"
	b := make([]byte, 48)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// ---------------------------- 控制通道 ----------------------------

func (s *server) handleControl(w http.ResponseWriter, r *http.Request) {
	if s.restOnly {
		// 真实 Laravel 面板终结不了二进制 WS（返回 501），Agent 据此降级到 REST。
		http.Error(w, "binary WS not supported", http.StatusNotImplemented)

		return
	}
	auth := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	nodeID := r.Header.Get("X-Node-ID")
	if auth == "" {
		http.Error(w, "missing bearer", http.StatusUnauthorized)
		return
	}

	s.mu.Lock()
	n := s.nodes[nodeID]
	s.mu.Unlock()
	if n == nil {
		http.Error(w, "unknown node", http.StatusUnauthorized)
		return
	}
	claims, err := creds.VerifyJWT(auth, n.Secret)
	if err != nil {
		log.Printf("JWT 校验失败 node=%s: %v", nodeID, err)
		http.Error(w, "invalid jwt: "+err.Error(), http.StatusUnauthorized)
		return
	}
	if aud, _ := claims["aud"].(string); aud != creds.DefaultAudience {
		http.Error(w, "bad audience", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("升级 WebSocket 失败: %v", err)
		return
	}
	s.mu.Lock()
	n.Conn = conn
	n.Connected = true
	s.mu.Unlock()
	log.Printf("控制通道已建立: node=%s jti=%v", nodeID, claims["jti"])

	defer func() {
		s.mu.Lock()
		n.Connected = false
		n.Conn = nil
		s.mu.Unlock()
		conn.Close()
		log.Printf("控制通道断开: node=%s", nodeID)
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		env, err := wire.Unmarshal(raw)
		if err != nil {
			log.Printf("解析失败: %v", err)
			continue
		}
		s.onMessage(n, env)
	}
}

func (s *server) onMessage(n *nodeState, env *agentpb.Envelope) {
	typ := wire.TypeName(env.GetType())
	s.mu.Lock()
	n.Counts[typ]++
	n.Received++
	if seq := env.GetSeq(); seq > 0 {
		n.Seqs = append(n.Seqs, seq)
	}
	if len(n.Seqs) > 1 && env.GetSeq() > 0 {
		// 记录"缺口"：seq 必须单调递增（除重发之外）
		prev := n.Seqs[len(n.Seqs)-2]
		if env.GetSeq() > prev+1 && prev != 0 {
			n.BadSeqs = append(n.BadSeqs, env.GetSeq())
		}
	}
	s.mu.Unlock()
	s.dump(env)

	switch b := env.GetBody().(type) {
	case *agentpb.Envelope_Hello:
		s.mu.Lock()
		n.LastHello = b.Hello
		n.HelloAcked = true
		resendFrom := n.AckedSeq + 1
		n.Hellos++
		rev := s.revision
		s.mu.Unlock()
		ack := &agentpb.HelloAck{
			ServerTimeMs:             time.Now().UnixMilli(),
			HeartbeatIntervalSeconds: uint32(s.hbInterval),
			HeartbeatTimeoutSeconds:  uint32(s.hbInterval * 3),
			TelemetryIntervalSeconds: uint32(s.telInterval),
			ResendFromSeq:            resendFrom,
			ConfigRevision:           rev,
			AcceptedCommands:         wire.CommandWhitelist(),
			ServerVersion:            "mockcontrol-1.0",
		}
		s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_HELLO_ACK, ack))
		log.Printf("Hello: node=%s agent=%s hysteria=%s resume_from_seq=%d last_acked=%d caps=%d",
			b.Hello.GetNodeId(), b.Hello.GetAgentVersion(), b.Hello.GetHysteriaVersion(),
			b.Hello.GetResumeFromSeq(), b.Hello.GetLastAckedSeq(), len(b.Hello.GetCapabilities()))

	case *agentpb.Envelope_Heartbeat:
		s.mu.Lock()
		s.heartbeats++
		acked := n.AckedSeq
		rev := s.revision
		s.mu.Unlock()
		s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_HEARTBEAT_ACK, &agentpb.HeartbeatAck{
			ServerTimeMs: time.Now().UnixMilli(), AckedSeq: acked, ConfigRevision: rev,
		}))
		if s.autoAck && env.GetSeq() > 0 {
			s.ackAll(n)
		}

	case *agentpb.Envelope_ConfigAck:
		log.Printf("配置回执: revision=%s ok=%v error=%q rolled_back=%v checks=%v",
			b.ConfigAck.GetRevision(), b.ConfigAck.GetOk(), b.ConfigAck.GetError(),
			b.ConfigAck.GetRolledBack(), b.ConfigAck.GetChecks())

	case *agentpb.Envelope_CommandResult:
		log.Printf("指令结果: id=%s type=%s status=%s error=%q",
			b.CommandResult.GetCommandId(), wire.CommandTypeName(b.CommandResult.GetType()),
			wire.CommandStatusName(b.CommandResult.GetStatus()), b.CommandResult.GetError())

	case *agentpb.Envelope_NodeEvent:
		log.Printf("节点事件: type=%s exit_code=%d reason=%q",
			b.NodeEvent.GetType(), b.NodeEvent.GetExitCode(), b.NodeEvent.GetReason())
	}

	// 非心跳类事件在自动确认模式下立即确认，模拟"控制面已落库"。
	if s.autoAck && env.GetSeq() > 0 && env.GetType() != agentpb.MsgType_MSG_TYPE_HEARTBEAT {
		s.ackAll(n)
	}
}

func (s *server) ackAll(n *nodeState) {
	s.mu.Lock()
	if len(n.Seqs) == 0 {
		s.mu.Unlock()
		return
	}
	max := uint64(0)
	for _, q := range n.Seqs {
		if q > max {
			max = q
		}
	}
	if max <= n.AckedSeq {
		s.mu.Unlock()
		return
	}
	n.AckedSeq = max
	s.mu.Unlock()
	s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_EVENT_ACK, &agentpb.EventAck{AckedSeq: max}))
}

func (s *server) send(n *nodeState, env *agentpb.Envelope) {
	s.mu.Lock()
	conn := n.Conn
	s.mu.Unlock()
	if conn == nil {
		return
	}
	raw, err := wire.Marshal(env)
	if err != nil {
		return
	}
	n.WriteM.Lock()
	defer n.WriteM.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.BinaryMessage, raw); err != nil {
		log.Printf("下发失败: %v", err)
	}
}

// dump 把收到的消息追加到 JSONL，便于测试断言"控制面到底收到了什么"。
func (s *server) dump(env *agentpb.Envelope) {
	// EmitUnpopulated：把 false/0/空值也写出来，否则测试无法断言 `ok=false`
	// 这类"零值即语义"的字段（proto3 的 JSON 默认会省略它们）。
	body, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(env)
	if err != nil {
		body = []byte(fmt.Sprintf("%q", "protojson: "+err.Error()))
	}
	rec := map[string]any{
		"recv_ms": time.Now().UnixMilli(),
		"type":    wire.TypeName(env.GetType()),
		"seq":     env.GetSeq(),
		"ts_ms":   env.GetTsMs(),
		"body":    json.RawMessage(body),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		// 不要再静默失败：dump 是 E2E 的断言依据，写不进去必须看到。
		log.Printf("dump 序列化失败: %v", err)
		return
	}
	f, err := os.OpenFile(s.dumpPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		log.Printf("dump 打开失败 %s: %v", s.dumpPath, err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("dump 写入失败: %v", err)
	}
}

// ---------------------------- 节点注册表持久化 ----------------------------
//
// 为什么需要：控制面重启在真实世界里很常见（发布、崩溃恢复）。如果重启后
// 忘了 node_id/node_secret 的映射，所有节点会拿到 401 并被永久拒之门外。
// 真实控制面当然要把节点存数据库；这个 mock 存一个 JSON 文件，语义一致。
func (s *server) nodesFile() string { return s.dumpPath + ".nodes.json" }

func (s *server) loadNodes() {
	b, err := os.ReadFile(s.nodesFile())
	if err != nil {
		return
	}
	var saved []struct{ NodeID, Secret, Name string }
	if err := json.Unmarshal(b, &saved); err != nil {
		log.Printf("节点注册表损坏（忽略）: %v", err)
		return
	}
	for _, n := range saved {
		st := &nodeState{NodeID: n.NodeID, Secret: n.Secret, Name: n.Name, Counts: map[string]int{}}
		s.nodes[n.NodeID] = st
		s.byName[n.Name] = st
	}
	log.Printf("已恢复 %d 个已注册节点（%s）", len(saved), s.nodesFile())
}

func (s *server) saveNodes() {
	s.mu.Lock()
	saved := make([]map[string]string, 0, len(s.nodes))
	for _, n := range s.nodes {
		saved = append(saved, map[string]string{"NodeID": n.NodeID, "Secret": n.Secret, "Name": n.Name})
	}
	s.mu.Unlock()
	b, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(s.nodesFile(), b, 0o600)
}

// ---------------------------- 测试辅助 ----------------------------

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodes := map[string]any{}
	for id, n := range s.nodes {
		nodes[id] = map[string]any{
			"node_id": id, "name": n.Name, "connected": n.Connected,
			"acked_seq": n.AckedSeq, "received": n.Received,
			"counts": n.Counts, "seqs": n.Seqs, "gaps": n.BadSeqs,
			"hello_count":      n.Hellos,
			"hysteria_running": boolPtr(n.LastHello),
		}
	}
	writeJSON(w, 200, map[string]any{
		"uptime_seconds": int(time.Since(s.start).Seconds()),
		"heartbeats":     s.heartbeats,
		"pushes":         s.pushes,
		"commands_sent":  s.commands,
		"nodes":          nodes,
		"revision":       s.revision,
		// REST 兜底面：配置版本文件库的默认版本 + 收到的配置回执（验证节点自动收敛用）
		"default_config_version": s.defaultVer,
		"default_config_sha256":  s.defaultSHA,
		"config_acks":            s.acks,
	})
}

func boolPtr(h *agentpb.Hello) bool {
	if h == nil {
		return false
	}
	return h.GetHysteriaRunning()
}

func (s *server) handlePushConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", 405)
		return
	}
	body := make([]byte, 0, 8192)
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	q := r.URL.Query()
	rev := q.Get("revision")
	if rev == "" {
		rev = fmt.Sprintf("e2e-%d", time.Now().Unix())
	}
	format := q.Get("format")
	if format == "" {
		format = "yaml"
	}
	cp := &agentpb.ConfigPush{
		Revision: rev, Format: format, Payload: body,
		Reason: q.Get("reason"), Force: q.Get("force") == "1", DryRun: q.Get("dry_run") == "1",
	}
	s.mu.Lock()
	s.revision = rev
	s.pushes++
	n := firstNode(s)
	s.mu.Unlock()
	if n == nil {
		http.Error(w, "no node connected", 409)
		return
	}
	s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_CONFIG_PUSH, cp))
	log.Printf("已下发配置: revision=%s bytes=%d dry_run=%v", rev, len(body), cp.DryRun)
	writeJSON(w, 200, map[string]any{"revision": rev, "bytes": len(body)})
}

func (s *server) handleCommand(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	typ, err := wire.ParseCommandType(q.Get("type"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	body := make([]byte, 0, 1024)
	buf := make([]byte, 1024)
	for {
		n, err := r.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	id := q.Get("id")
	if id == "" {
		id = fmt.Sprintf("cmd-%d", time.Now().UnixNano())
	}
	req := &agentpb.CommandRequest{CommandId: id, Type: typ, ParamsJson: body, TraceId: q.Get("trace_id")}
	s.mu.Lock()
	s.commands = append(s.commands, wire.CommandTypeName(typ)+":"+id)
	n := firstNode(s)
	s.mu.Unlock()
	if n == nil {
		http.Error(w, "no node connected", 409)
		return
	}
	s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_COMMAND, req))
	log.Printf("已下发指令: %s id=%s params=%s", wire.CommandTypeName(typ), id, string(body))
	writeJSON(w, 200, map[string]any{"command_id": id, "type": wire.CommandTypeName(typ)})
}

func (s *server) handleAckAll(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	n := firstNode(s)
	s.mu.Unlock()
	if n == nil {
		http.Error(w, "no node", 409)
		return
	}
	s.ackAll(n)
	s.mu.Lock()
	acked := n.AckedSeq
	s.mu.Unlock()
	writeJSON(w, 200, map[string]any{"acked_seq": acked})
}

func (s *server) handleUpgradeOffer(w http.ResponseWriter, r *http.Request) {
	var mf struct {
		Version   string `json:"version"`
		URL       string `json:"url"`
		SHA256    string `json:"sha256"`
		Signature string `json:"signature"`
		Size      int64  `json:"size"`
		Force     bool   `json:"force"`
		Notes     string `json:"notes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&mf); err != nil {
		http.Error(w, "bad json: "+err.Error(), 400)
		return
	}
	sig, err := decodeSig(mf.Signature)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	offer := &agentpb.UpgradeOffer{
		Version: mf.Version, Url: mf.URL, Sha256: mf.SHA256, Signature: sig,
		Size: mf.Size, Force: mf.Force, Notes: mf.Notes,
	}
	s.mu.Lock()
	n := firstNode(s)
	s.mu.Unlock()
	if n == nil {
		http.Error(w, "no node connected", 409)
		return
	}
	s.send(n, wire.New(agentpb.MsgType_MSG_TYPE_UPGRADE_OFFER, offer))
	writeJSON(w, 200, map[string]any{"sent": true, "version": mf.Version})
}

func firstNode(s *server) *nodeState {
	for _, n := range s.nodes {
		return n
	}
	return nil
}

func decodeSig(s string) ([]byte, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	if b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s)); err == nil {
		return b, nil
	}
	return hex.DecodeString(strings.TrimSpace(s))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
