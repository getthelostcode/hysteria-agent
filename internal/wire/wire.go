// Package wire 封装 Envelope 的构造、编解码与枚举命名，避免各包散落 proto 细节。
package wire

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
)

// ProtocolVersion 是当前实现支持的协议版本。
const ProtocolVersion = agentpb.ProtocolVersion_PROTOCOL_VERSION_V1

// New 用 body 构造信封。body 必须是 agentpb 里的 *Hello / *HelloAck / ... 之一。
func New(t agentpb.MsgType, body any) *agentpb.Envelope {
	env := &agentpb.Envelope{ProtocolVersion: uint32(ProtocolVersion), Type: t}
	switch b := body.(type) {
	case *agentpb.Hello:
		env.Body = &agentpb.Envelope_Hello{Hello: b}
	case *agentpb.HelloAck:
		env.Body = &agentpb.Envelope_HelloAck{HelloAck: b}
	case *agentpb.Heartbeat:
		env.Body = &agentpb.Envelope_Heartbeat{Heartbeat: b}
	case *agentpb.HeartbeatAck:
		env.Body = &agentpb.Envelope_HeartbeatAck{HeartbeatAck: b}
	case *agentpb.ConfigPush:
		env.Body = &agentpb.Envelope_ConfigPush{ConfigPush: b}
	case *agentpb.ConfigAck:
		env.Body = &agentpb.Envelope_ConfigAck{ConfigAck: b}
	case *agentpb.CommandRequest:
		env.Body = &agentpb.Envelope_Command{Command: b}
	case *agentpb.CommandResult:
		env.Body = &agentpb.Envelope_CommandResult{CommandResult: b}
	case *agentpb.CommandChunk:
		env.Body = &agentpb.Envelope_CommandChunk{CommandChunk: b}
	case *agentpb.LogBatch:
		env.Body = &agentpb.Envelope_Logs{Logs: b}
	case *agentpb.MetricBatch:
		env.Body = &agentpb.Envelope_Metrics{Metrics: b}
	case *agentpb.EventAck:
		env.Body = &agentpb.Envelope_EventAck{EventAck: b}
	case *agentpb.ErrorMsg:
		env.Body = &agentpb.Envelope_Error{Error: b}
	case *agentpb.UpgradeOffer:
		env.Body = &agentpb.Envelope_UpgradeOffer{UpgradeOffer: b}
	case *agentpb.NodeEvent:
		env.Body = &agentpb.Envelope_NodeEvent{NodeEvent: b}
	default:
		panic(fmt.Sprintf("wire.New: 未支持的消息体类型 %T", body))
	}
	return env
}

// Marshal 序列化信封。
func Marshal(env *agentpb.Envelope) ([]byte, error) { return proto.Marshal(env) }

// Unmarshal 反序列化信封并做基本校验（协议版本 + type 一致性）。
func Unmarshal(b []byte) (*agentpb.Envelope, error) {
	env := &agentpb.Envelope{}
	if err := proto.Unmarshal(b, env); err != nil {
		return nil, err
	}
	if v := env.GetProtocolVersion(); v != 0 && v != uint32(ProtocolVersion) {
		return nil, fmt.Errorf("协议版本不兼容: 对端 %d, 本端 %d", v, ProtocolVersion)
	}
	if env.GetType() == agentpb.MsgType_MSG_TYPE_UNSPECIFIED {
		// 容忍只填了 oneof 没填 type 的实现：从 oneof 反推。
		env.Type = TypeOf(env)
	}
	return env, nil
}

// TypeOf 从 oneof 反推消息类型。
func TypeOf(env *agentpb.Envelope) agentpb.MsgType {
	switch env.GetBody().(type) {
	case *agentpb.Envelope_Hello:
		return agentpb.MsgType_MSG_TYPE_HELLO
	case *agentpb.Envelope_HelloAck:
		return agentpb.MsgType_MSG_TYPE_HELLO_ACK
	case *agentpb.Envelope_Heartbeat:
		return agentpb.MsgType_MSG_TYPE_HEARTBEAT
	case *agentpb.Envelope_HeartbeatAck:
		return agentpb.MsgType_MSG_TYPE_HEARTBEAT_ACK
	case *agentpb.Envelope_ConfigPush:
		return agentpb.MsgType_MSG_TYPE_CONFIG_PUSH
	case *agentpb.Envelope_ConfigAck:
		return agentpb.MsgType_MSG_TYPE_CONFIG_ACK
	case *agentpb.Envelope_Command:
		return agentpb.MsgType_MSG_TYPE_COMMAND
	case *agentpb.Envelope_CommandResult:
		return agentpb.MsgType_MSG_TYPE_COMMAND_RESULT
	case *agentpb.Envelope_CommandChunk:
		return agentpb.MsgType_MSG_TYPE_COMMAND_CHUNK
	case *agentpb.Envelope_Logs:
		return agentpb.MsgType_MSG_TYPE_LOG_BATCH
	case *agentpb.Envelope_Metrics:
		return agentpb.MsgType_MSG_TYPE_METRIC_BATCH
	case *agentpb.Envelope_EventAck:
		return agentpb.MsgType_MSG_TYPE_EVENT_ACK
	case *agentpb.Envelope_Error:
		return agentpb.MsgType_MSG_TYPE_ERROR
	case *agentpb.Envelope_UpgradeOffer:
		return agentpb.MsgType_MSG_TYPE_UPGRADE_OFFER
	case *agentpb.Envelope_NodeEvent:
		return agentpb.MsgType_MSG_TYPE_NODE_EVENT
	default:
		return agentpb.MsgType_MSG_TYPE_UNSPECIFIED
	}
}

// TypeName 返回控制面友好的类型名（用于日志与错误信息）。
func TypeName(t agentpb.MsgType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "MSG_TYPE_"))
}

// CommandTypeName 返回指令的 snake_case 名字。
func CommandTypeName(t agentpb.CommandType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "COMMAND_TYPE_"))
}

// CommandStatusName 返回指令状态的 snake_case 名字。
func CommandStatusName(s agentpb.CommandStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "COMMAND_STATUS_"))
}

var commandTypes = map[string]agentpb.CommandType{
	"reload_config":    agentpb.CommandType_COMMAND_TYPE_RELOAD_CONFIG,
	"restart_hysteria": agentpb.CommandType_COMMAND_TYPE_RESTART_HYSTERIA,
	"upgrade_agent":    agentpb.CommandType_COMMAND_TYPE_UPGRADE_AGENT,
	"run_diagnostic":   agentpb.CommandType_COMMAND_TYPE_RUN_DIAGNOSTIC,
	"tail_logs":        agentpb.CommandType_COMMAND_TYPE_TAIL_LOGS,
	"kick_users":       agentpb.CommandType_COMMAND_TYPE_KICK_USERS,
	"get_state":        agentpb.CommandType_COMMAND_TYPE_GET_STATE,
	"cancel_tail":      agentpb.CommandType_COMMAND_TYPE_CANCEL_TAIL,
	"rollback_config":  agentpb.CommandType_COMMAND_TYPE_ROLLBACK_CONFIG,
	"self_restart":     agentpb.CommandType_COMMAND_TYPE_SELF_RESTART,
	"clear_outbox":     agentpb.CommandType_COMMAND_TYPE_CLEAR_OUTBOX,
}

// ParseCommandType 解析指令名（命令行与 Python 控制面都可用）。
func ParseCommandType(s string) (agentpb.CommandType, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("指令名不能为空")
	}
	if v, ok := commandTypes[strings.ToLower(s)]; ok {
		return v, nil
	}
	// 也接受 "COMMAND_TYPE_XXX" 形式
	if v, ok := agentpb.CommandType_value[strings.ToUpper(s)]; ok {
		return agentpb.CommandType(v), nil
	}
	return 0, fmt.Errorf("未知指令 %q（白名单内的指令见 DESIGN.md「远程指令」）", s)
}

// CommandWhitelist 返回全部可用指令名（/metrics 暴露、Hello.capabilities 上报）。
func CommandWhitelist() []string {
	out := make([]string, 0, len(commandTypes))
	for k := range commandTypes {
		out = append(out, k)
	}
	return out
}
