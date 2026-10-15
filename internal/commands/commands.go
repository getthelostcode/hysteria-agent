// Package commands 实现远程指令的白名单分发与执行。
//
// 安全底线：Agent 只认识下面这些指令类型，任何情况下都不执行任意 shell 命令。
// 需要操作外部服务时，一律通过固定二进制 + 固定参数的 exec（例如 systemctl / journalctl），
// 参数绝不拼接用户输入。
package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/diag"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/hyconfig"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/supervisor"
	"github.com/getthelostcode/hysteria-agent/internal/upgrade"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

// Sender 负责把结果/分片回传（由 control.Client 实现，走 outbox 保证不丢）。
type Sender interface {
	Enqueue(kind string, env *agentpb.Envelope) (uint64, error)
}

// ConfigApplier 由 agent 实现：配置校验/落盘/热更新/回滚的统一入口。
type ConfigApplier interface {
	// ApplyPayload 校验并应用一份配置正文。
	ApplyPayload(ctx context.Context, payload []byte, format, revision, source, reason string, dryRun bool) *agentpb.ConfigAck
	// ReloadCurrent 用当前磁盘上的配置重新加载（不换内容）。
	ReloadCurrent(ctx context.Context, reason string) *agentpb.ConfigAck
	// Rollback 回滚到上一个（或指定 revision）版本。
	Rollback(ctx context.Context, key string) *agentpb.ConfigAck
}

// Deps 是执行指令所需的依赖。
type Deps struct {
	Cfg      *config.Config
	Log      *slog.Logger
	M        *metrics.Metrics
	Hub      *logging.Hub
	Super    *supervisor.Supervisor
	API      *hyapi.Client
	Store    *hyconfig.Store
	Upgrader *upgrade.Upgrader
	Applier  ConfigApplier
	Sender   Sender
	// State 返回 agent 的运行状态快照（GET_STATE 用）。
	State func() map[string]any
	// DiagDeps 供 RUN_DIAGNOSTIC 使用。
	DiagDeps func() diag.Deps
	// ClearOutbox 清空本地积压事件（CLEAR_OUTBOX 指令）。
	ClearOutbox func() (int, error)
}

// Dispatcher 指令分发器。
type Dispatcher struct {
	d Deps

	sem    chan struct{}
	mu     sync.Mutex
	seen   map[string]time.Time          // command_id 幂等表
	cancel map[string]context.CancelFunc // 正在执行的可取消指令（tail_logs）
}

// New 构建分发器。并发上限固定为 4（防止控制面一次性下发大量指令把节点压垮）。
func New(d Deps) *Dispatcher {
	return &Dispatcher{
		d:      d,
		sem:    make(chan struct{}, 4),
		seen:   map[string]time.Time{},
		cancel: map[string]context.CancelFunc{},
	}
}

// Handle 处理一条指令请求。立即返回，实际执行在后台 goroutine 里。
func (d *Dispatcher) Handle(ctx context.Context, req *agentpb.CommandRequest) {
	id := req.GetCommandId()
	if id == "" {
		id = fmt.Sprintf("anon-%d", time.Now().UnixNano())
	}
	// 幂等：同一个 command_id 只执行一次（控制面可能因断线重发）。
	d.mu.Lock()
	if _, dup := d.seen[id]; dup {
		d.mu.Unlock()
		d.d.Log.Info("忽略重复指令", slog.String("command_id", id))
		return
	}
	d.seen[id] = time.Now()
	// 幂等表裁剪，避免长期运行无限增长。
	if len(d.seen) > 512 {
		cutoff := time.Now().Add(-30 * time.Minute)
		for k, t := range d.seen {
			if t.Before(cutoff) {
				delete(d.seen, k)
			}
		}
	}
	d.mu.Unlock()

	cmdType, err := parseType(req.GetType())
	if err != nil {
		d.finish(ctx, req, agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, err.Error(), time.Now())
		return
	}

	go func() {
		ctx := logging.WithTrace(ctx, req.GetTraceId())
		select {
		case d.sem <- struct{}{}:
			defer func() { <-d.sem }()
		case <-ctx.Done():
			return
		}
		d.d.M.CommandsRunning.Inc()
		defer d.d.M.CommandsRunning.Dec()

		started := time.Now()
		d.d.Log.Info("开始执行远程指令",
			slog.String("command", wire.CommandTypeName(cmdType)),
			slog.String("command_id", id))
		result, detail, execErr := d.execute(ctx, id, cmdType, req.GetParamsJson())
		d.finish(ctx, req, result, detail, errString(execErr), started)
	}()
}

func parseType(t agentpb.CommandType) (agentpb.CommandType, error) {
	switch t {
	case agentpb.CommandType_COMMAND_TYPE_RELOAD_CONFIG,
		agentpb.CommandType_COMMAND_TYPE_RESTART_HYSTERIA,
		agentpb.CommandType_COMMAND_TYPE_UPGRADE_AGENT,
		agentpb.CommandType_COMMAND_TYPE_RUN_DIAGNOSTIC,
		agentpb.CommandType_COMMAND_TYPE_TAIL_LOGS,
		agentpb.CommandType_COMMAND_TYPE_KICK_USERS,
		agentpb.CommandType_COMMAND_TYPE_GET_STATE,
		agentpb.CommandType_COMMAND_TYPE_CANCEL_TAIL,
		agentpb.CommandType_COMMAND_TYPE_ROLLBACK_CONFIG,
		agentpb.CommandType_COMMAND_TYPE_SELF_RESTART,
		agentpb.CommandType_COMMAND_TYPE_CLEAR_OUTBOX:
		return t, nil
	default:
		return t, fmt.Errorf("指令 %q 不在白名单内，拒绝执行", wire.CommandTypeName(t))
	}
}

// execute 执行具体指令，返回 (状态, 结果 JSON, 错误)。
func (d *Dispatcher) execute(ctx context.Context, id string, t agentpb.CommandType, params []byte) (agentpb.CommandStatus, []byte, error) {
	switch t {
	case agentpb.CommandType_COMMAND_TYPE_RELOAD_CONFIG:
		var p struct {
			Revision   string `json:"revision"`
			Format     string `json:"format"`
			Payload    string `json:"payload"`     // base64 可选：带内容时直接应用
			PayloadRaw []byte `json:"payload_raw"` // 也接受 []byte（protobuf JSON 兼容）
			Reason     string `json:"reason"`
			Force      bool   `json:"force"`
			DryRun     bool   `json:"dry_run"`
		}
		_ = json.Unmarshal(params, &p)
		if p.Payload != "" {
			raw, err := decodeMaybeBase64(p.Payload)
			if err != nil {
				return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
			}
			ack := d.d.Applier.ApplyPayload(ctx, raw, p.Format, p.Revision, "control", p.Reason, p.DryRun)
			return statusFromAck(ack), ackJSON(ack), nil
		}
		ack := d.d.Applier.ReloadCurrent(ctx, p.Reason)
		return statusFromAck(ack), ackJSON(ack), nil

	case agentpb.CommandType_COMMAND_TYPE_ROLLBACK_CONFIG:
		var p struct {
			Revision string `json:"revision"`
			Reason   string `json:"reason"`
		}
		_ = json.Unmarshal(params, &p)
		ack := d.d.Applier.Rollback(ctx, p.Revision)
		return statusFromAck(ack), ackJSON(ack), nil

	case agentpb.CommandType_COMMAND_TYPE_RESTART_HYSTERIA:
		if err := d.d.Super.Restart(ctx, "远程指令 restart_hysteria"); err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
		}
		st := d.d.Super.State()
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{
			"pid": st.PID, "running": st.Running, "restarts": st.Restarts,
		}), nil

	case agentpb.CommandType_COMMAND_TYPE_UPGRADE_AGENT:
		if d.d.Upgrader == nil {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("升级未启用或没有验签公钥")
		}
		mf, err := upgrade.ParseManifest(params)
		if err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, err
		}
		res, err := d.d.Upgrader.Run(ctx, mf)
		if err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_FAILED, mustJSON(res), err
		}
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(res), nil

	case agentpb.CommandType_COMMAND_TYPE_RUN_DIAGNOSTIC:
		dep := d.d.DiagDeps()
		rep := diag.Run(ctx, dep)
		_ = rep.WriteFile(d.d.Cfg.StateDir)
		st := agentpb.CommandStatus_COMMAND_STATUS_OK
		if rep.Status == "error" {
			st = agentpb.CommandStatus_COMMAND_STATUS_FAILED
		}
		return st, rep.Marshal(), nil

	case agentpb.CommandType_COMMAND_TYPE_GET_STATE:
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(d.d.State()), nil

	case agentpb.CommandType_COMMAND_TYPE_KICK_USERS:
		var p struct {
			UserIDs []string `json:"user_ids"`
			Users   []string `json:"users"`
			Reason  string   `json:"reason"`
		}
		_ = json.Unmarshal(params, &p)
		ids := p.UserIDs
		if len(ids) == 0 {
			ids = p.Users
		}
		if len(ids) == 0 {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("缺少 user_ids")
		}
		if err := d.d.API.Kick(ctx, ids); err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
		}
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{
			"kicked": ids, "note": "踢出后客户端会自动重连；要真正阻断请在控制面把用户置为禁用",
		}), nil

	case agentpb.CommandType_COMMAND_TYPE_TAIL_LOGS:
		return d.tailLogs(ctx, id, params)

	case agentpb.CommandType_COMMAND_TYPE_CANCEL_TAIL:
		var p struct {
			CommandID string `json:"command_id"`
		}
		_ = json.Unmarshal(params, &p)
		d.mu.Lock()
		cancel := d.cancel[p.CommandID]
		d.mu.Unlock()
		if cancel == nil {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("没有正在进行的 tail_logs: %s", p.CommandID)
		}
		cancel()
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{"cancelled": p.CommandID}), nil

	case agentpb.CommandType_COMMAND_TYPE_SELF_RESTART:
		// 用 systemd 的 --no-block 让 systemd 接管重启，避免自己杀掉自己的重启动作。
		if _, err := exec.LookPath("systemctl"); err == nil {
			cmd := exec.Command("systemctl", "--no-block", "restart", d.d.Cfg.Upgrade.ServiceName)
			if err := cmd.Start(); err != nil {
				return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
			}
			go func() { _ = cmd.Wait() }()
			return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{"restarting": d.d.Cfg.Upgrade.ServiceName}), nil
		}
		return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("本机没有 systemctl，无法重启 Agent（请在前台手动重启）")

	case agentpb.CommandType_COMMAND_TYPE_CLEAR_OUTBOX:
		var p struct {
			Confirm string `json:"confirm"`
		}
		_ = json.Unmarshal(params, &p)
		if strings.ToLower(strings.TrimSpace(p.Confirm)) != "yes" {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil,
				fmt.Errorf("丢弃本地积压事件需要显式确认：params 里必须带 {\"confirm\":\"yes\"}")
		}
		if d.d.ClearOutbox == nil {
			return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("当前不支持清空 outbox")
		}
		n, err := d.d.ClearOutbox()
		if err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_FAILED, mustJSON(map[string]any{"cleared": n}), err
		}
		return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{"cleared": n}), nil
	}
	return agentpb.CommandStatus_COMMAND_STATUS_REJECTED, nil, fmt.Errorf("未实现的指令 %s", t)
}

// tailLogs 流式推送日志：先回放最近 N 行，再实时推送到时限（最长 5 分钟）。
func (d *Dispatcher) tailLogs(ctx context.Context, id string, params []byte) (agentpb.CommandStatus, []byte, error) {
	var p struct {
		DurationSeconds float64 `json:"duration_seconds"`
		BacklogLines    int     `json:"backlog_lines"`
		Source          string  `json:"source"` // agent | hysteria | all
		Level           string  `json:"level"`  // 最小级别
	}
	_ = json.Unmarshal(params, &p)
	dur := time.Duration(p.DurationSeconds * float64(time.Second))
	maxDur := time.Duration(d.d.Cfg.Tail.MaxDurationSeconds) * time.Second
	if maxDur <= 0 {
		maxDur = 5 * time.Minute
	}
	if dur <= 0 || dur > maxDur {
		dur = maxDur // 需求硬上限：5 分钟
	}
	if p.BacklogLines <= 0 {
		p.BacklogLines = 200
	}
	source := strings.ToLower(strings.TrimSpace(p.Source))
	if source == "" {
		source = "all"
	}
	minRank := levelRank(p.Level)

	cctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()
	d.mu.Lock()
	d.cancel[id] = cancel
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.cancel, id)
		d.mu.Unlock()
	}()

	d.d.M.TailActive.Inc()
	defer d.d.M.TailActive.Dec()

	var (
		idx    uint32
		sent   int
		target = "hysteria.agent." + id
	)

	// 1) 回放历史
	send := func(entries []*agentpb.LogEntry, eof bool) error {
		data, err := encodeChunk(entries)
		if err != nil {
			return err
		}
		chunk := &agentpb.CommandChunk{CommandId: id, Index: idx, Data: data, Eof: eof}
		idx++
		_, err = d.d.Sender.Enqueue("command_chunk", wire.New(agentpb.MsgType_MSG_TYPE_COMMAND_CHUNK, chunk))
		if err != nil {
			return err
		}
		sent += len(entries)
		return nil
	}

	history := d.d.Hub.Tail(p.BacklogLines)
	var backlog []*agentpb.LogEntry
	for _, e := range history {
		if !matchSource(e, source) || levelRank(e.GetLevel()) < minRank {
			continue
		}
		backlog = append(backlog, e)
	}
	if len(backlog) > 0 {
		if err := send(backlog, false); err != nil {
			return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
		}
	}

	// 2) 实时订阅（Hub 里同时有 agent 自己的日志与被守护 Hysteria 的输出）
	sub := d.d.Hub.Subscribe(512)
	defer sub.Close()

	// 3) systemd 模式下额外挂 journalctl，拿到 Hysteria 写到 journal 的日志
	journalCh := make(chan string, 256)
	var journalCmd *exec.Cmd
	if source == "hysteria" || source == "all" {
		if unit := d.d.Cfg.Hysteria.JournalUnit; unit != "" && d.d.Cfg.Hysteria.Mode != "process" {
			if _, err := exec.LookPath("journalctl"); err == nil {
				journalCmd = exec.CommandContext(cctx, "journalctl", "-u", unit, "-f", "-n", "0", "--output=cat")
				if out, err := journalCmd.StdoutPipe(); err == nil {
					if err := journalCmd.Start(); err == nil {
						go func() {
							buf := make([]byte, 0, 4096)
							tmp := make([]byte, 4096)
							for {
								n, err := out.Read(tmp)
								if n > 0 {
									buf = append(buf, tmp[:n]...)
									for {
										i := indexByte(buf, '\n')
										if i < 0 {
											break
										}
										line := string(buf[:i])
										buf = buf[i+1:]
										select {
										case journalCh <- line:
										default:
										}
									}
								}
								if err != nil {
									close(journalCh)
									return
								}
							}
						}()
						d.d.Log.Info("tail_logs 已挂载 journalctl", slog.String("unit", unit))
					}
				}
			}
		}
	}
	defer func() {
		if journalCmd != nil && journalCmd.Process != nil {
			_ = journalCmd.Process.Kill()
		}
	}()

	flushInterval := 200 * time.Millisecond
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	buf := make([]*agentpb.LogEntry, 0, 64)

	for {
		select {
		case <-cctx.Done():
			if len(buf) > 0 {
				_ = send(buf, false)
			}
			_ = send(nil, true) // eof
			return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{
				"lines_sent": sent, "target": target, "duration_seconds": dur.Seconds(), "stopped": "deadline_or_cancel",
			}), nil

		case e, ok := <-sub.Ch:
			if !ok {
				_ = send(buf, true)
				return agentpb.CommandStatus_COMMAND_STATUS_OK, mustJSON(map[string]any{"lines_sent": sent}), nil
			}
			if !matchSource(e, source) || levelRank(e.GetLevel()) < minRank {
				continue
			}
			buf = append(buf, e)
			if len(buf) >= 100 {
				if err := send(buf, false); err != nil {
					return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
				}
				buf = buf[:0]
			}

		case line, ok := <-journalCh:
			if !ok {
				journalCh = nil
				continue
			}
			entry := &agentpb.LogEntry{TsMs: time.Now().UnixMilli(), Level: "info", Component: "hysteria", Msg: line}
			buf = append(buf, entry)
			if len(buf) >= 100 {
				if err := send(buf, false); err != nil {
					return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
				}
				buf = buf[:0]
			}

		case <-t.C:
			if len(buf) > 0 {
				if err := send(buf, false); err != nil {
					return agentpb.CommandStatus_COMMAND_STATUS_FAILED, nil, err
				}
				buf = buf[:0]
			}
		}
	}
}

// finish 回传指令结果。
func (d *Dispatcher) finish(ctx context.Context, req *agentpb.CommandRequest, status agentpb.CommandStatus, detail []byte, errMsg string, started time.Time) {
	finished := time.Now()
	res := &agentpb.CommandResult{
		CommandId:  req.GetCommandId(),
		Status:     status,
		Type:       req.GetType(),
		Error:      errMsg,
		ResultJson: detail,
		StartedMs:  started.UnixMilli(),
		FinishedMs: finished.UnixMilli(),
	}
	if _, err := d.d.Sender.Enqueue("command_result", wire.New(agentpb.MsgType_MSG_TYPE_COMMAND_RESULT, res)); err != nil {
		d.d.Log.Warn("回传指令结果失败", slog.String("error", err.Error()))
	}
	d.d.M.CommandsTotal.WithLabelValues(wire.CommandTypeName(req.GetType()), wire.CommandStatusName(status)).Inc()
	attrs := []any{
		slog.String("command", wire.CommandTypeName(req.GetType())),
		slog.String("command_id", req.GetCommandId()),
		slog.String("status", wire.CommandStatusName(status)),
		slog.Float64("duration_seconds", finished.Sub(started).Seconds()),
	}
	if errMsg != "" {
		attrs = append(attrs, slog.String("error", errMsg))
	}
	if status == agentpb.CommandStatus_COMMAND_STATUS_OK {
		d.d.Log.Info("远程指令完成", attrs...)
	} else {
		d.d.Log.Warn("远程指令失败", attrs...)
	}
}

// ---------------------------- 工具 ----------------------------

func statusFromAck(ack *agentpb.ConfigAck) agentpb.CommandStatus {
	if ack == nil {
		return agentpb.CommandStatus_COMMAND_STATUS_FAILED
	}
	if ack.GetOk() {
		return agentpb.CommandStatus_COMMAND_STATUS_OK
	}
	if ack.GetError() != "" && strings.Contains(ack.GetError(), "拒绝") {
		return agentpb.CommandStatus_COMMAND_STATUS_REJECTED
	}
	return agentpb.CommandStatus_COMMAND_STATUS_FAILED
}

func ackJSON(ack *agentpb.ConfigAck) []byte {
	if ack == nil {
		return nil
	}
	b, _ := json.MarshalIndent(ack, "", "  ")
	return b
}

func mustJSON(v any) []byte {
	b, _ := json.MarshalIndent(v, "", "  ")
	return b
}

func matchSource(e *agentpb.LogEntry, source string) bool {
	switch source {
	case "", "all":
		return true
	case "hysteria":
		return e.GetComponent() == "hysteria"
	case "agent":
		return e.GetComponent() != "hysteria"
	default:
		return e.GetComponent() == source
	}
}

func levelRank(level string) int {
	switch strings.ToLower(level) {
	case "debug":
		return 0
	case "info", "":
		return 1
	case "warn", "warning":
		return 2
	case "error", "fatal":
		return 3
	default:
		return 1
	}
}

// encodeChunk 把日志条目编码成 JSON Lines 分片（控制面按行解析即可）。
func encodeChunk(entries []*agentpb.LogEntry) ([]byte, error) {
	var sb strings.Builder
	for _, e := range entries {
		line := map[string]any{
			"ts":     e.GetTsMs(),
			"level":  e.GetLevel(),
			"source": e.GetComponent(),
			"msg":    e.GetMsg(),
			"id":     e.GetId(),
		}
		if e.GetTraceId() != "" {
			line["trace_id"] = e.GetTraceId()
		}
		if len(e.GetFieldsJson()) > 0 {
			line["fields"] = json.RawMessage(e.GetFieldsJson())
		}
		b, err := json.Marshal(line)
		if err != nil {
			continue
		}
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return []byte(sb.String()), nil
}

func decodeMaybeBase64(s string) ([]byte, error) {
	// 支持三种：base64（标准）、base64（URL 安全）、纯文本
	if b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s)); err == nil {
		return b, nil
	}
	if b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s)); err == nil {
		return b, nil
	}
	return []byte(s), nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// Version 供诊断/自检判断指令模块版本。
func Version() string { return version.Version }
