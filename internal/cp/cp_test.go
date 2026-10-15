package cp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
	"github.com/gorilla/websocket"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	key, _ := NewMasterKey()
	st, err := OpenStore(filepath.Join(dir, "state"), key)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return st
}

func TestDeriveSecretStableAndScoped(t *testing.T) {
	key1, _ := NewMasterKey()
	key2, _ := NewMasterKey()
	s1, _ := OpenStore(t.TempDir(), key1)
	s2, _ := OpenStore(t.TempDir(), key2)

	a1 := s1.DeriveSecret("node_a")
	if a1 != s1.DeriveSecret("node_a") {
		t.Fatal("同一节点两次派生结果不一致")
	}
	if a1 == s1.DeriveSecret("node_b") {
		t.Fatal("不同节点派生出相同密钥")
	}
	if a1 == s2.DeriveSecret("node_a") {
		t.Fatal("不同主密钥派生出相同密钥（主密钥没生效）")
	}
	if len(a1) != 64 {
		t.Fatalf("派生密钥应为 64 位 hex，实际 %d", len(a1))
	}
}

func TestTokenLifecycle(t *testing.T) {
	st := newTestStore(t)

	one := st.NewToken("one-shot", time.Hour, 1)
	if _, err := st.UseToken(one.Token, "n1"); err != nil {
		t.Fatalf("首次使用应成功: %v", err)
	}
	if _, err := st.UseToken(one.Token, "n2"); err == nil {
		t.Fatal("一次性令牌第二次使用应失败")
	}

	multi := st.NewToken("multi", time.Hour, 0) // 0 => 一次性
	if multi.MaxUses != 1 {
		t.Fatalf("maxUses<=0 应当作一次性，实际 %d", multi.MaxUses)
	}

	expired := st.NewToken("expired", -time.Second, 5)
	if _, err := st.UseToken(expired.Token, "n3"); err == nil {
		t.Fatal("过期令牌应失败")
	}

	revoked := st.NewToken("revoked", time.Hour, 5)
	if !st.RevokeToken(revoked.Token) {
		t.Fatal("吊销应成功")
	}
	if _, err := st.UseToken(revoked.Token, "n4"); err == nil {
		t.Fatal("已吊销令牌应失败")
	}

	if _, err := st.UseToken("enr_不存在", "n5"); err == nil {
		t.Fatal("未知令牌应失败")
	}
}

func TestStatePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	key, _ := NewMasterKey()
	st, err := OpenStore(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	node, secret, err := st.CreateNode(EnrollRequest{NodeName: "hk-1", Hostname: "hk1", OS: "linux", Arch: "amd64"}, "tok")
	if err != nil {
		t.Fatal(err)
	}
	tok := st.NewToken("keep", time.Hour, 3)
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := st2.Node(node.ID)
	if !ok || got.Name != "hk-1" {
		t.Fatalf("重开后节点丢失: %+v", got)
	}
	if st2.DeriveSecret(node.ID) != secret {
		t.Fatal("重开后派生密钥变化（节点将无法再连接）")
	}
	if len(st2.Tokens()) != 1 || st2.Tokens()[0].Token != tok.Token {
		t.Fatal("重开后令牌丢失")
	}
	if fi, err := os.Stat(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	} else if fi.Mode().Perm() != 0o600 {
		t.Fatalf("state.json 权限应为 0600，实际 %o", fi.Mode().Perm())
	}
}

func TestDeleteNodeRevokesSecret(t *testing.T) {
	st := newTestStore(t)
	node, _, err := st.CreateNode(EnrollRequest{NodeName: "n"}, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if !st.DeleteNode(node.ID) {
		t.Fatal("删除应成功")
	}
	if _, ok := st.Node(node.ID); ok {
		t.Fatal("删除后仍能查到节点")
	}
	if st.DeleteNode(node.ID) {
		t.Fatal("重复删除应返回 false")
	}
}

func TestEventsAppendAndRead(t *testing.T) {
	st := newTestStore(t)
	node, _, _ := st.CreateNode(EnrollRequest{NodeName: "n"}, "tok")
	for i := 1; i <= 5; i++ {
		st.AppendEvent(Event{NodeID: node.ID, Kind: "node_event", Seq: uint64(i), Type: "started",
			Message: "m", Data: json.RawMessage(`{"exit_code":0}`)})
	}
	evs, err := st.Events(node.ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 {
		t.Fatalf("应返回 3 条，实际 %d", len(evs))
	}
	if evs[0].Seq != 5 {
		t.Fatalf("应按时间倒序返回，首条 seq 应为 5，实际 %d", evs[0].Seq)
	}
	if evs, _ := st.Events("node_不存在", 10); len(evs) != 0 {
		t.Fatal("不存在的节点应返回空")
	}
}

// TestControlChannelHandshake 覆盖控制通道核心契约：
// Hello→HelloAck(含 resend_from_seq)、业务消息→累积 EventAck、重复 seq 不重复入库、
// 配置下发→ConfigPush→ConfigAck 回执被等待方收到。
func TestControlChannelHandshake(t *testing.T) {
	// 控制通道是 hijack 出来的连接，httptest.Server.Close() 不会等它收尾；
	// 这里留一点时间让 handler goroutine 落完事件，否则 t.TempDir 清理会失败
	// （报 "directory not empty"）。defer 先注册的先入栈、最后执行。
	defer func() { time.Sleep(400 * time.Millisecond) }()

	key, _ := NewMasterKey()
	store, err := OpenStore(filepath.Join(t.TempDir(), "state"), key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.AdminToken = "adm_test"
	cfg.Heartbeat = HBConfig{IntervalSeconds: 5, TimeoutSeconds: 20, TelemetrySeconds: 5}
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1) enroll
	tok := store.NewToken("test", time.Hour, 1)
	body, _ := json.Marshal(EnrollRequest{EnrollToken: tok.Token, NodeName: "node-x", Hostname: "h", OS: "linux", Arch: "amd64", AgentVersion: "0.1.0"})
	resp, err := http.Post(ts.URL+cfg.EnrollPath, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("enroll 失败: %d %s", resp.StatusCode, raw)
	}
	var er EnrollResponse
	if err := json.Unmarshal(raw, &er); err != nil {
		t.Fatal(err)
	}
	if er.NodeID == "" || er.NodeSecret == "" {
		t.Fatal("enroll 响应缺少 node_id/node_secret")
	}
	if er.NodeSecret != store.DeriveSecret(er.NodeID) {
		t.Fatal("下发的 node_secret 与派生结果不一致")
	}

	// 2) 用 node_secret 自签 JWT 连 WS
	c := &creds.Credentials{NodeID: er.NodeID, NodeSecret: er.NodeSecret, Issuer: creds.DefaultIssuer, Audience: creds.DefaultAudience}
	jwtTok, _, err := c.MintJWT(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + cfg.WSPath
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+jwtTok)
	hdr.Set("X-Node-ID", er.NodeID)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		t.Fatalf("WS 连接失败: %v", err)
	}
	defer conn.Close()

	send := func(env *agentpb.Envelope) {
		b, _ := wire.Marshal(env)
		if err := conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
			t.Fatalf("写失败: %v", err)
		}
	}
	readEnv := func() *agentpb.Envelope {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, b, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("读失败: %v", err)
		}
		env, err := wire.Unmarshal(b)
		if err != nil {
			t.Fatalf("解析失败: %v", err)
		}
		return env
	}

	// 3) Hello → HelloAck
	send(wire.New(agentpb.MsgType_MSG_TYPE_HELLO, &agentpb.Hello{
		NodeId: er.NodeID, AgentVersion: "0.1.0", HysteriaVersion: "v2-mock",
		Hostname: "h", Os: "linux", Arch: "amd64", ResumeFromSeq: 1,
		Capabilities: []string{"reload_config", "run_diagnostic"},
	}))
	ack := readEnv()
	if ack.GetType() != agentpb.MsgType_MSG_TYPE_HELLO_ACK {
		t.Fatalf("首个响应应为 HelloAck，实际 %s", wire.TypeName(ack.GetType()))
	}
	ha := ack.GetHelloAck()
	if ha.GetResendFromSeq() != 1 {
		t.Fatalf("新节点 resend_from_seq 应为 1，实际 %d", ha.GetResendFromSeq())
	}
	if ha.GetHeartbeatIntervalSeconds() != 5 {
		t.Fatalf("心跳间隔应来自控制面配置(5)，实际 %d", ha.GetHeartbeatIntervalSeconds())
	}
	if len(ha.GetAcceptedCommands()) == 0 {
		t.Fatal("HelloAck 应带上白名单指令")
	}
	if n, _ := store.Node(er.NodeID); n.AgentVersion != "0.1.0" || n.HysteriaVersion != "v2-mock" {
		t.Fatalf("Hello 信息没有写进节点记录: %+v", n)
	}

	// 4) seq=1 的节点事件 → EventAck(1)；重复发送不再重复入库
	ev := wire.New(agentpb.MsgType_MSG_TYPE_NODE_EVENT, &agentpb.NodeEvent{Type: "started", Reason: "boot"})
	ev.Seq = 1
	send(ev)
	if a := readEnv().GetEventAck(); a == nil || a.GetAckedSeq() != 1 {
		t.Fatalf("应确认 seq=1，实际 %v", a)
	}
	before, _ := store.Node(er.NodeID)
	send(ev) // 重发同一 seq
	if a := readEnv().GetEventAck(); a == nil || a.GetAckedSeq() != 1 {
		t.Fatalf("重发也应确认 seq=1，实际 %v", a)
	}
	after, _ := store.Node(er.NodeID)
	if after.ReceivedMsgs != before.ReceivedMsgs {
		t.Fatalf("重复 seq 不应增加 received_msgs（%d → %d）", before.ReceivedMsgs, after.ReceivedMsgs)
	}
	if after.DuplicateMsgs != before.DuplicateMsgs+1 {
		t.Fatalf("重复 seq 应计数，%d → %d", before.DuplicateMsgs, after.DuplicateMsgs)
	}
	evs, _ := store.Events(er.NodeID, 50)
	kept := 0
	for _, e := range evs {
		if e.Kind == "node_event" && e.Seq == 1 {
			kept++
		}
	}
	if kept != 1 {
		t.Fatalf("重复事件被重复入库：seq=1 的 node_event 有 %d 条", kept)
	}

	// 5) 心跳 → HeartbeatAck（带累积 ack）
	send(wire.New(agentpb.MsgType_MSG_TYPE_HEARTBEAT, &agentpb.Heartbeat{TsMs: time.Now().UnixMilli(), UptimeSeconds: 42, Ready: true}))
	ha2 := readEnv()
	if ha2.GetType() != agentpb.MsgType_MSG_TYPE_HEARTBEAT_ACK {
		t.Fatalf("应收到 HeartbeatAck，实际 %s", wire.TypeName(ha2.GetType()))
	}
	if ha2.GetHeartbeatAck().GetAckedSeq() != 1 {
		t.Fatalf("HeartbeatAck 应带累积 ack=1，实际 %d", ha2.GetHeartbeatAck().GetAckedSeq())
	}

	// 6) 控制面下发配置 → Agent 回 ConfigAck → 等待方拿到回执
	go func() {
		env := readEnv()
		if env.GetType() != agentpb.MsgType_MSG_TYPE_CONFIG_PUSH {
			t.Errorf("应收到 ConfigPush，实际 %s", wire.TypeName(env.GetType()))
			return
		}
		cpMsg := env.GetConfigPush()
		if cpMsg.GetRevision() != "test-rev-1" || cpMsg.GetSha256() != SHA256Hex([]byte("listen: 8443\n")) {
			t.Errorf("ConfigPush 内容不对: %s %s", cpMsg.GetRevision(), cpMsg.GetSha256())
		}
		ev := wire.New(agentpb.MsgType_MSG_TYPE_CONFIG_ACK, &agentpb.ConfigAck{
			Revision: "test-rev-1", Ok: true, Sha256: cpMsg.GetSha256(), DurationMs: 12,
			Checks: []string{"schema:ok"},
		})
		ev.Seq = 2
		send(ev)
		// 这里会再收到针对 seq=2 的 EventAck，留给后面读
	}()

	payload := []byte("listen: 8443\n")
	out, err := srv.PushConfig(er.NodeID, payload, "", "test-rev-1", "unit-test", false, 10*time.Second)
	if err != nil {
		t.Fatalf("PushConfig 失败: %v", err)
	}
	if ok, _ := out["ok"].(bool); !ok {
		t.Fatalf("回执应 ok=true，实际 %+v", out)
	}
	if !strings.Contains(strings.Join(out["checks"].([]string), ","), "schema:ok") {
		t.Fatalf("回执应带校验明细：%+v", out)
	}
	time.Sleep(200 * time.Millisecond)
	n2, _ := store.Node(er.NodeID)
	if n2.ConfigState != "applied" || n2.ConfigRevision != "test-rev-1" {
		t.Fatalf("配置状态未更新: %+v", n2)
	}
	if p := store.Pending(er.NodeID); p != nil {
		t.Fatalf("回执后不应再有待确认下发: %+v", p)
	}
}

func TestOfflineWhenHeartbeatTimesOut(t *testing.T) {
	key, _ := NewMasterKey()
	store, err := OpenStore(filepath.Join(t.TempDir(), "state"), key)
	if err != nil {
		t.Fatal(err)
	}
	node, _, _ := store.CreateNode(EnrollRequest{NodeName: "n"}, "tok")
	store.Mutate(node.ID, func(n *Node) { n.Connected = true; n.LastSeenMs = time.Now().Add(-time.Minute).UnixMilli() })

	cfg := DefaultConfig()
	cfg.Heartbeat = HBConfig{IntervalSeconds: 1, TimeoutSeconds: 1, TelemetrySeconds: 1}
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.offlineSweeper(ctx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := store.Node(node.ID); !n.Connected {
			return // 已被判定离线
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("心跳超时的节点没有被标记为离线")
}

func TestAdminAuthAndRoutes(t *testing.T) {
	key, _ := NewMasterKey()
	store, _ := OpenStore(filepath.Join(t.TempDir(), "state"), key)
	cfg := DefaultConfig()
	cfg.AdminToken = "adm_secret"
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	get := func(path, token string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := get("/admin/nodes", ""); code != 401 {
		t.Fatalf("无令牌应 401，实际 %d", code)
	}
	if code, _ := get("/admin/nodes", "wrong"); code != 401 {
		t.Fatalf("错误令牌应 401，实际 %d", code)
	}
	if code, body := get("/admin/nodes", "adm_secret"); code != 200 || !strings.Contains(body, "\"count\"") {
		t.Fatalf("正确令牌应 200 且返回 count，实际 %d %s", code, body)
	}
	if code, _ := get("/admin/不存在", "adm_secret"); code != 404 {
		t.Fatalf("未知路径应 404，实际 %d", code)
	}
}

func TestMetricsExposeNodes(t *testing.T) {
	key, _ := NewMasterKey()
	store, _ := OpenStore(filepath.Join(t.TempDir(), "state"), key)
	node, _, _ := store.CreateNode(EnrollRequest{NodeName: "n"}, "tok")
	store.Mutate(node.ID, func(n *Node) { n.Connected = true })
	srv := New(DefaultConfig(), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	body := string(b)
	for _, want := range []string{`cp_nodes_total{status="online"} 1`, "cp_enroll_total", "cp_info{version="} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics 缺少 %q\n%s", want, body)
		}
	}
}
