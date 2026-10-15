package wire

import (
	"testing"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
)

func TestRoundTripAllBodies(t *testing.T) {
	envs := []*agentpb.Envelope{
		New(agentpb.MsgType_MSG_TYPE_HELLO, &agentpb.Hello{NodeId: "n1", AgentVersion: "0.1.0", Capabilities: []string{"reload_config"}}),
		New(agentpb.MsgType_MSG_TYPE_HELLO_ACK, &agentpb.HelloAck{ResendFromSeq: 7, HeartbeatIntervalSeconds: 30}),
		New(agentpb.MsgType_MSG_TYPE_HEARTBEAT, &agentpb.Heartbeat{PendingEvents: 3, Ready: true}),
		New(agentpb.MsgType_MSG_TYPE_HEARTBEAT_ACK, &agentpb.HeartbeatAck{AckedSeq: 9}),
		New(agentpb.MsgType_MSG_TYPE_CONFIG_PUSH, &agentpb.ConfigPush{Revision: "rev-1", Format: "yaml", Payload: []byte("listen: :443\n"), Sha256: "abc"}),
		New(agentpb.MsgType_MSG_TYPE_CONFIG_ACK, &agentpb.ConfigAck{Revision: "rev-1", Ok: true, Checks: []string{"schema:ok"}}),
		New(agentpb.MsgType_MSG_TYPE_COMMAND, &agentpb.CommandRequest{CommandId: "c1", Type: agentpb.CommandType_COMMAND_TYPE_RELOAD_CONFIG, ParamsJson: []byte("{}")}),
		New(agentpb.MsgType_MSG_TYPE_COMMAND_RESULT, &agentpb.CommandResult{CommandId: "c1", Status: agentpb.CommandStatus_COMMAND_STATUS_OK}),
		New(agentpb.MsgType_MSG_TYPE_COMMAND_CHUNK, &agentpb.CommandChunk{CommandId: "c1", Index: 2, Data: []byte("line\n"), Eof: true}),
		New(agentpb.MsgType_MSG_TYPE_LOG_BATCH, &agentpb.LogBatch{Source: "agent", Entries: []*agentpb.LogEntry{{Msg: "hello", Level: "info"}}}),
		New(agentpb.MsgType_MSG_TYPE_METRIC_BATCH, &agentpb.MetricBatch{Sample: &agentpb.MetricSample{CpuPercent: 12.5}, Hysteria: &agentpb.HysteriaStats{Running: true}}),
		New(agentpb.MsgType_MSG_TYPE_EVENT_ACK, &agentpb.EventAck{AckedSeq: 42}),
		New(agentpb.MsgType_MSG_TYPE_ERROR, &agentpb.ErrorMsg{Code: "E1", Message: "boom", Fatal: true}),
		New(agentpb.MsgType_MSG_TYPE_UPGRADE_OFFER, &agentpb.UpgradeOffer{Version: "1.2.3", Url: "https://x/y"}),
		New(agentpb.MsgType_MSG_TYPE_NODE_EVENT, &agentpb.NodeEvent{Type: "exited", ExitCode: 137, LogTail: []string{"boom"}}),
	}
	for _, env := range envs {
		raw, err := Marshal(env)
		if err != nil {
			t.Fatalf("Marshal(%s): %v", TypeName(env.GetType()), err)
		}
		got, err := Unmarshal(raw)
		if err != nil {
			t.Fatalf("Unmarshal(%s): %v", TypeName(env.GetType()), err)
		}
		if got.GetType() != env.GetType() {
			t.Fatalf("type 不一致: 期望 %s 得到 %s", TypeName(env.GetType()), TypeName(got.GetType()))
		}
		if TypeOf(got) != env.GetType() {
			t.Fatalf("从 oneof 反推的类型不一致: %s", TypeName(TypeOf(got)))
		}
		if got.GetProtocolVersion() != uint32(ProtocolVersion) {
			t.Fatalf("协议版本未写入: %d", got.GetProtocolVersion())
		}
	}
}

func TestUnmarshalRejectsWrongProtocolVersion(t *testing.T) {
	env := New(agentpb.MsgType_MSG_TYPE_HELLO, &agentpb.Hello{NodeId: "n"})
	env.ProtocolVersion = 99
	raw, _ := Marshal(env)
	if _, err := Unmarshal(raw); err == nil {
		t.Fatal("不兼容的协议版本必须报错，而不是静默解析")
	}
}

func TestCommandWhitelistParsing(t *testing.T) {
	names := []string{"reload_config", "restart_hysteria", "upgrade_agent", "run_diagnostic", "tail_logs", "kick_users", "get_state"}
	for _, n := range names {
		ct, err := ParseCommandType(n)
		if err != nil {
			t.Fatalf("ParseCommandType(%s): %v", n, err)
		}
		if CommandTypeName(ct) != n {
			t.Fatalf("名字往返不一致: %s -> %s", n, CommandTypeName(ct))
		}
	}
	if _, err := ParseCommandType("rm -rf /"); err == nil {
		t.Fatal("白名单外的指令必须解析失败")
	}
	if len(CommandWhitelist()) < len(names) {
		t.Fatalf("白名单长度异常: %d", len(CommandWhitelist()))
	}
}
