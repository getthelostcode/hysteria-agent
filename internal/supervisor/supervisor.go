// Package supervisor 负责 Hysteria2 进程的生命周期：拉起、监控、崩溃重启、优雅停止。
//
// 三种模式：
//
//	process  —— Agent 直接 fork hysteria 子进程（默认）。子进程独立进程组，crash 自动重启；
//	systemd  —— Hysteria 由 systemd 管理，Agent 只通过白名单 systemctl 动词操作它；
//	external —— 只观测不管理（别人起的实例，或容器里）。
//
// 关于"平滑重载"：Hysteria 2 只处理 SIGINT/SIGTERM（源码里没有 SIGHUP 处理），
// 所以配置热更新 = SIGTERM 让进程优雅收尾（等待连接排空）→ 以新配置立即拉起。
// 断连窗口是进程重启时间（通常 <1s），QUIC 客户端会自动重连。
// 这个行为在 DESIGN.md 里显式说明，不做"假装支持 SIGHUP"的封装。
package supervisor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
)

// 模式常量。
const (
	ModeProcess  = "process"
	ModeSystemd  = "systemd"
	ModeExternal = "external"
)

// State 是运行态快照（遥测/status/诊断都用它）。
type State struct {
	Mode           string    `json:"mode"`
	Running        bool      `json:"running"`
	PID            int       `json:"pid"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	UptimeSeconds  uint64    `json:"uptime_seconds"`
	ExitCode       int       `json:"last_exit_code"`
	Restarts       uint64    `json:"restart_count"`
	Version        string    `json:"version,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	ConfigRevision string    `json:"config_revision,omitempty"`
	GiveUp         bool      `json:"give_up"`
	BackoffSeconds float64   `json:"backoff_seconds,omitempty"`
}

// EventType 描述被守护进程发生的事件（会上报给控制面）。
type EventType string

const (
	EventStarted     EventType = "started"
	EventExited      EventType = "exited"
	EventRestarting  EventType = "restart_scheduled"
	EventGiveUp      EventType = "restart_gave_up"
	EventStopped     EventType = "stopped"
	EventStartFailed EventType = "start_failed"
)

// Event 是一条上报事件。
type Event struct {
	Type     EventType `json:"type"`
	At       time.Time `json:"at"`
	PID      int       `json:"pid,omitempty"`
	ExitCode int       `json:"exit_code,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	LogTail  []string  `json:"log_tail,omitempty"`
}

// Supervisor 守护 Hysteria。
type Supervisor struct {
	cfg *config.Config
	log *slog.Logger
	m   *metrics.Metrics
	hub *logging.Hub
	api *hyapi.Client

	mu           sync.Mutex
	cmd          *exec.Cmd
	pid          int
	startedAt    time.Time
	exitCode     int
	restarts     uint64
	lastErr      string
	giveUp       bool
	stopping     bool
	backoff      time.Duration
	restartTimes []time.Time
	logRing      []string
	version      string
	revision     string
	wanted       bool // 期望运行（false 表示被显式停止，不自动重启）

	onEvent func(Event)

	waitDone chan struct{}
	runDone  chan struct{}
}

const logRingSize = 300

// New 构建 Supervisor。api 可以为 nil（不采集流量时）。
func New(cfg *config.Config, log *slog.Logger, m *metrics.Metrics, hub *logging.Hub, api *hyapi.Client) *Supervisor {
	return &Supervisor{
		cfg: cfg, log: log, m: m, hub: hub, api: api,
		backoff:  time.Duration(cfg.Hysteria.Restart.InitialSeconds * float64(time.Second)),
		waitDone: make(chan struct{}),
		runDone:  make(chan struct{}),
	}
}

// OnEvent 注册事件回调（Agent 用它上报控制面）。
func (s *Supervisor) OnEvent(fn func(Event)) {
	s.mu.Lock()
	s.onEvent = fn
	s.mu.Unlock()
}

// SetRevision 记录当前生效的配置版本（遥测上报用）。
func (s *Supervisor) SetRevision(rev string) {
	s.mu.Lock()
	s.revision = rev
	s.mu.Unlock()
}

func (s *Supervisor) emit(ev Event) {
	s.mu.Lock()
	if ev.LogTail == nil {
		ev.LogTail = append([]string{}, tailOf(s.logRing, 20)...)
	}
	fn := s.onEvent
	s.mu.Unlock()
	if fn != nil {
		fn(ev)
	}
}

// ---------------------------- 生命周期 ----------------------------

// Start 按模式启动被守护的 Hysteria。
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = false
	s.wanted = true
	s.giveUp = false
	mode := s.cfg.Hysteria.Mode
	s.mu.Unlock()

	if s.version == "" {
		s.version = sysinfo.BinaryVersion(ctx, s.cfg.Hysteria.Binary, 5*time.Second)
	}

	switch mode {
	case ModeProcess:
		return s.startProcess(ctx)
	case ModeSystemd:
		return s.startSystemd(ctx)
	case ModeExternal:
		return s.observeExternal(ctx)
	default:
		return fmt.Errorf("未知的 hysteria.mode: %s", mode)
	}
}

// Stop 优雅停止（SIGTERM → 等待 → SIGKILL），并置为"不再自动重启"。
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopping = true
	s.wanted = false
	mode := s.cfg.Hysteria.Mode
	s.mu.Unlock()

	switch mode {
	case ModeProcess:
		return s.stopProcess(ctx)
	case ModeSystemd:
		return s.systemctl(ctx, "stop")
	default:
		return nil
	}
}

// Restart 平滑重启：先优雅停止再启动。reason 会写进事件里，便于对账。
func (s *Supervisor) Restart(ctx context.Context, reason string) error {
	s.log.Info("重启 Hysteria", slog.String("reason", reason))
	if err := s.Stop(ctx); err != nil {
		s.log.Warn("停止 Hysteria 时出错，继续尝试启动", slog.String("error", err.Error()))
	}
	// Stop 会把 wanted 置 false，这里重新打开。
	if err := s.Start(ctx); err != nil {
		return err
	}
	return s.WaitHealthy(ctx, s.cfg.Hysteria.StartupTimeout())
}

// Reload 应用新配置。
//
// process 模式下等价于 Restart（见包注释：Hysteria 无 SIGHUP）；
// systemd 模式下先试 `systemctl reload`（若 unit 定义了 ExecReload），失败再重启。
func (s *Supervisor) Reload(ctx context.Context, reason string) error {
	if s.cfg.Hysteria.Mode == ModeSystemd {
		if err := s.systemctl(ctx, "reload"); err == nil {
			return s.WaitHealthy(ctx, s.cfg.Hysteria.StartupTimeout())
		}
		s.log.Info("systemctl reload 不可用，回退为 restart", slog.String("reason", reason))
	}
	return s.Restart(ctx, reason)
}

// WaitHealthy 等待 Hysteria 真正可用：进程存活 + 本地 API 可读（或端口已监听）。
func (s *Supervisor) WaitHealthy(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		if !s.IsRunning() {
			return fmt.Errorf("Hysteria 进程已退出（exit=%d: %s）", s.ExitCode(), s.LastError())
		}
		if s.api != nil {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := s.api.Online(cctx)
			cancel()
			if err == nil {
				return nil
			}
			if time.Now().After(deadline) {
				// 本地 API 不可读但进程活着：给最后一次机会看端口
				return fmt.Errorf("Hysteria 进程在跑但本地 API 不可用: %w", err)
			}
		} else if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// State 返回运行态快照。
func (s *Supervisor) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := State{
		Mode:           s.cfg.Hysteria.Mode,
		Running:        s.isAliveLocked(),
		PID:            s.pid,
		ExitCode:       s.exitCode,
		Restarts:       s.restarts,
		Version:        s.version,
		LastError:      s.lastErr,
		GiveUp:         s.giveUp,
		BackoffSeconds: s.backoff.Seconds(),
		ConfigRevision: s.revision,
	}
	if !s.startedAt.IsZero() && st.Running {
		st.StartedAt = s.startedAt
		st.UptimeSeconds = uint64(time.Since(s.startedAt).Seconds())
	}
	return st
}

// IsRunning 报告被守护进程是否存活。
func (s *Supervisor) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isAliveLocked()
}

func (s *Supervisor) isAliveLocked() bool {
	switch s.cfg.Hysteria.Mode {
	case ModeProcess:
		if s.pid <= 0 {
			return false
		}
		return sysinfo.ProcessAlive(s.pid, s.cfg.Hysteria.Binary)
	case ModeExternal:
		if s.pid <= 0 {
			pid, err := sysinfo.FindProcessByExecutable(baseName(s.cfg.Hysteria.Binary))
			if err == nil {
				s.pid = pid
			}
			return err == nil
		}
		return sysinfo.ProcessAlive(s.pid, "")
	case ModeSystemd:
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "systemctl", "is-active", s.cfg.Hysteria.ServiceName).Output()
		return err == nil && strings.TrimSpace(string(out)) == "active"
	}
	return false
}

// Pid 返回被守护进程的 pid（拿不到返回 0）。
func (s *Supervisor) Pid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pid
}

// ExitCode 返回最近一次退出码。
func (s *Supervisor) ExitCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitCode
}

// LastError 返回最近的错误描述。
func (s *Supervisor) LastError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// RecentLogs 返回最近 n 行 Hysteria 日志。
func (s *Supervisor) RecentLogs(n int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return tailOf(s.logRing, n)
}

// ---------------------------- process 模式 ----------------------------

func (s *Supervisor) startProcess(ctx context.Context) error {
	s.mu.Lock()
	if s.isAliveLocked() {
		s.mu.Unlock()
		return nil
	}
	_, err := os.Stat(s.cfg.Hysteria.Binary)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("找不到 Hysteria 可执行文件 %s: %w", s.cfg.Hysteria.Binary, err)
	}
	s.mu.Unlock()

	args := []string{"server", "-c", s.cfg.Hysteria.ConfigPath}
	args = append(args, s.cfg.Hysteria.ExtraArgs...)
	cmd := exec.Command(s.cfg.Hysteria.Binary, args...)
	if s.cfg.Hysteria.WorkDir != "" {
		cmd.Dir = s.cfg.Hysteria.WorkDir
	} else {
		cmd.Dir = "/"
	}
	cmd.Env = append(os.Environ(), s.cfg.Hysteria.Env...)
	// 独立进程组：停止时整组收信号，避免 Hysteria 的子进程变孤儿。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		s.mu.Lock()
		s.lastErr = err.Error()
		s.mu.Unlock()
		s.emit(Event{Type: EventStartFailed, At: time.Now(), Reason: err.Error()})
		return fmt.Errorf("启动 Hysteria 失败: %w", err)
	}

	waitDone := make(chan struct{})
	s.mu.Lock()
	s.cmd = cmd
	s.pid = cmd.Process.Pid
	s.startedAt = time.Now()
	s.waitDone = waitDone
	s.exitCode = 0
	s.mu.Unlock()

	s.log.Info("Hysteria 已启动",
		slog.Int("pid", cmd.Process.Pid),
		slog.String("binary", s.cfg.Hysteria.Binary),
		slog.String("config", s.cfg.Hysteria.ConfigPath))
	s.m.HysteriaRunning.Set(1)
	s.emit(Event{Type: EventStarted, At: time.Now(), PID: cmd.Process.Pid})

	go s.pump(stdout, "info")
	go s.pump(stderr, "warn")
	go s.reap(cmd, waitDone)
	return nil
}

// pump 把子进程输出逐行送进日志 Hub 与本地环（tail_logs / 诊断都要看它）。
func (s *Supervisor) pump(r io.Reader, level string) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			continue
		}
		lvl := level
		low := strings.ToLower(line)
		if strings.Contains(low, "level=error") || strings.Contains(low, "error") && strings.Contains(low, "fatal") {
			lvl = "error"
		}
		s.mu.Lock()
		s.logRing = append(s.logRing, line)
		if len(s.logRing) > logRingSize {
			s.logRing = s.logRing[len(s.logRing)-logRingSize:]
		}
		s.mu.Unlock()
		if s.hub != nil {
			s.hub.Ingest(lvl, "hysteria", line, "", nil)
		}
	}
}

// reap 等待子进程退出，按退避策略决定是否重启。
func (s *Supervisor) reap(cmd *exec.Cmd, waitDone chan struct{}) {
	err := cmd.Wait()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
			if code < 0 {
				code = 128 + int(ee.Sys().(syscall.WaitStatus).Signal())
			}
		} else {
			code = -1
		}
	}

	s.mu.Lock()
	if s.cmd != cmd {
		// 这个 goroutine 等待的进程已经被更新的进程取代（例如热更新期间的
		// Stop→Start 竞态、或旧进程的延迟重启）：绝不能拿它的退出码去覆盖当前状态。
		s.mu.Unlock()
		close(waitDone)
		return
	}
	s.exitCode = code
	s.pid = 0
	s.cmd = nil
	if err != nil {
		s.lastErr = err.Error()
	}
	stopping := s.stopping
	wanted := s.wanted
	s.mu.Unlock()
	close(waitDone)

	s.m.HysteriaRunning.Set(0)
	s.m.HysteriaExitCode.Set(float64(code))
	if stopping || !wanted {
		s.log.Info("Hysteria 已停止", slog.Int("exit_code", code))
		s.emit(Event{Type: EventStopped, At: time.Now(), ExitCode: code})
		return
	}

	s.log.Warn("Hysteria 异常退出", slog.Int("exit_code", code), slog.String("error", errString(err)))
	s.emit(Event{Type: EventExited, At: time.Now(), ExitCode: code, Reason: errString(err)})
	s.scheduleRestart(code)
}

// scheduleRestart 按指数退避 + 窗口限流决定是否重启。
func (s *Supervisor) scheduleRestart(exitCode int) {
	s.mu.Lock()
	now := time.Now()
	window := time.Duration(s.cfg.Hysteria.RestartWindowSeconds * float64(time.Second))
	if window <= 0 {
		window = 5 * time.Minute
	}
	keep := s.restartTimes[:0]
	for _, t := range s.restartTimes {
		if now.Sub(t) < window {
			keep = append(keep, t)
		}
	}
	s.restartTimes = append(keep, now)
	inWindow := len(s.restartTimes)
	maxInWindow := s.cfg.Hysteria.RestartMaxInWindow
	if maxInWindow > 0 && inWindow > maxInWindow {
		s.giveUp = true
		s.lastErr = fmt.Sprintf("窗口内已重启 %d 次（上限 %d），放弃自动重启；最后退出码 %d", inWindow, maxInWindow, exitCode)
		s.mu.Unlock()
		s.log.Error("放弃自动重启 Hysteria", slog.Int("restarts_in_window", inWindow))
		s.emit(Event{Type: EventGiveUp, At: now, ExitCode: exitCode, Reason: s.LastError()})
		return
	}
	delay := s.backoff
	s.backoff = time.Duration(float64(s.backoff) * s.cfg.Hysteria.Restart.Multiplier)
	maxBackoff := time.Duration(s.cfg.Hysteria.Restart.MaxSeconds * float64(time.Second))
	if maxBackoff <= 0 {
		maxBackoff = time.Minute
	}
	if s.backoff > maxBackoff {
		s.backoff = maxBackoff
	}
	s.mu.Unlock()

	s.m.HysteriaRestarts.Inc()
	s.log.Warn("计划重启 Hysteria", slog.Float64("delay_seconds", delay.Seconds()))
	s.emit(Event{Type: EventRestarting, At: now, ExitCode: exitCode, Reason: fmt.Sprintf("backoff=%s", delay)})

	go func() {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-s.runDone:
			return
		case <-t.C:
		}
		s.mu.Lock()
		if s.stopping || !s.wanted || s.giveUp {
			// 已经放弃重启（或已被显式停止）：不能因为"之前排队的定时器"再拉起来。
			// 少了 giveUp 这个判断，崩溃循环会变成"放弃后还被旧定时器反复拉起"。
			s.mu.Unlock()
			return
		}
		// 重启成功才累加计数并复位退避
		s.restarts++
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Hysteria.StartupTimeout())
		defer cancel()
		if err := s.startProcess(ctx); err != nil {
			s.log.Error("重启 Hysteria 失败", slog.String("error", err.Error()))
			s.scheduleRestart(-1)
			return
		}
		s.mu.Lock()
		s.backoff = time.Duration(s.cfg.Hysteria.Restart.InitialSeconds * float64(time.Second))
		s.mu.Unlock()
	}()
}

func (s *Supervisor) stopProcess(ctx context.Context) error {
	s.mu.Lock()
	cmd := s.cmd
	pid := s.pid
	waitDone := s.waitDone
	s.mu.Unlock()
	if cmd == nil || pid <= 0 {
		return nil
	}
	// 负 pid：向整个进程组发 SIGTERM。
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	timeout := s.cfg.Hysteria.ShutdownTimeout()
	select {
	case <-waitDone:
		return nil
	case <-time.After(timeout):
		s.log.Warn("Hysteria 未在优雅期内退出，发送 SIGKILL", slog.Int("pid", pid))
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-waitDone:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("Hysteria(pid=%d) 在 SIGKILL 后仍未退出", pid)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ---------------------------- systemd / external 模式 ----------------------------

// allowedSystemctlVerbs 是允许的动词白名单：Agent 永远不会执行任意命令。
var allowedSystemctlVerbs = map[string]bool{
	"start": true, "stop": true, "restart": true, "reload": true,
	"is-active": true, "status": true, "show": true,
}

func (s *Supervisor) systemctl(ctx context.Context, verb string) error {
	if !allowedSystemctlVerbs[verb] {
		return fmt.Errorf("systemctl 动词 %q 不在白名单内", verb)
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("本机没有 systemctl，无法管理模式为 systemd 的 Hysteria: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, "systemctl", verb, s.cfg.Hysteria.ServiceName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %s %s 失败: %v (%s)", verb, s.cfg.Hysteria.ServiceName, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (s *Supervisor) startSystemd(ctx context.Context) error {
	if err := s.systemctl(ctx, "start"); err != nil {
		return err
	}
	pid := s.systemdMainPID(ctx)
	s.mu.Lock()
	s.pid = pid
	s.startedAt = time.Now()
	s.mu.Unlock()
	s.m.HysteriaRunning.Set(1)
	s.emit(Event{Type: EventStarted, At: time.Now(), PID: pid})
	return nil
}

func (s *Supervisor) systemdMainPID(ctx context.Context) int {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, "systemctl", "show", "-p", "MainPID", "--value", s.cfg.Hysteria.ServiceName).Output()
	if err != nil {
		return 0
	}
	var pid int
	fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &pid)
	return pid
}

// observeExternal 只记录外部实例的 pid，不做任何管理动作。
func (s *Supervisor) observeExternal(ctx context.Context) error {
	pid, err := sysinfo.FindProcessByExecutable(baseName(s.cfg.Hysteria.Binary))
	if err != nil {
		s.mu.Lock()
		s.lastErr = err.Error()
		s.mu.Unlock()
		return fmt.Errorf("external 模式：未找到运行中的 %s", baseName(s.cfg.Hysteria.Binary))
	}
	s.mu.Lock()
	s.pid = pid
	s.startedAt = time.Now()
	s.mu.Unlock()
	s.m.HysteriaRunning.Set(1)
	s.log.Info("已接管观测外部 Hysteria 实例", slog.Int("pid", pid))
	s.emit(Event{Type: EventStarted, At: time.Now(), PID: pid, Reason: "external"})
	return nil
}

// DetectExternal 在不接管进程的前提下探测"正在运行的 Hysteria"，把 pid/启动时间填进状态。
//
// 用途：`hysteria-agent diag` / `status` 这类一次性 CLI 场景——它们没有 supervisor
// 运行时，若不探测就会把"本机明明跑着 Hysteria"误报成 `hysteria_process=error`，
// 端口检查也会因为拿不到 OwnerPID 而误报"端口被别的进程占用"。
func (s *Supervisor) DetectExternal() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var pid int
	switch s.cfg.Hysteria.Mode {
	case ModeSystemd:
		out, err := exec.CommandContext(ctx, "systemctl", "is-active", s.cfg.Hysteria.ServiceName).Output()
		if err != nil || strings.TrimSpace(string(out)) != "active" {
			return false
		}
		pid = s.systemdMainPID(ctx)
	default:
		p, err := sysinfo.FindProcessByExecutable(baseName(s.cfg.Hysteria.Binary))
		if err != nil {
			return false
		}
		pid = p
	}
	if pid <= 0 {
		return false
	}
	started := time.Now()
	if t, err := sysinfo.ProcessStartTime(pid); err == nil {
		started = t
	}
	s.mu.Lock()
	s.pid = pid
	s.startedAt = started
	if s.version == "" {
		s.version = sysinfo.BinaryVersion(ctx, s.cfg.Hysteria.Binary, 3*time.Second)
	}
	s.mu.Unlock()
	return true
}

// ---------------------------- 工具 ----------------------------

func baseName(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func tailOf(ring []string, n int) []string {
	if n <= 0 || n > len(ring) {
		n = len(ring)
	}
	out := make([]string, 0, n)
	out = append(out, ring[len(ring)-n:]...)
	return out
}

// Close 释放后台状态（停止后调用）。
func (s *Supervisor) Close() {
	select {
	case <-s.runDone:
	default:
		close(s.runDone)
	}
}
