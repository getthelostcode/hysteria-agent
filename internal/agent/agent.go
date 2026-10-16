// Package agent 是整个守护进程的装配层：把凭据、控制通道、配置管理、进程守护、
// 遥测、日志、远程指令装配成一个可运行的 Agent。
//
// 目录与权限约定（install.sh 与 README 必须一致）：
//
//	/etc/hysteria-agent/config.yaml     0600   agent 自身配置
//	/etc/hysteria-agent/credentials.json 0400  节点身份（node_id/node_secret）
//	/etc/hysteria-agent/history/         0700  配置历史（保留最近 5 个版本）
//	/etc/hysteria/config.yaml            0600  Hysteria 实际读取的配置
//	/var/lib/hysteria-agent/             0750  outbox / state.json / 升级工作目录
//	/run/hysteria-agent/                 0755  pid 等运行时文件
package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/commands"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/control"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/diag"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/hyconfig"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/logs"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/outbox"
	"github.com/getthelostcode/hysteria-agent/internal/supervisor"
	"github.com/getthelostcode/hysteria-agent/internal/telemetry"
	"github.com/getthelostcode/hysteria-agent/internal/upgrade"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

// Agent 是装配好的守护进程。
type Agent struct {
	cfg *config.Config
	cr  *creds.Credentials
	log *slog.Logger
	hub *logging.Hub
	m   *metrics.Metrics

	ob    *outbox.Outbox
	store *hyconfig.Store
	api   *hyapi.Client
	sup   *supervisor.Supervisor
	cli   *control.Client
	disp  *commands.Dispatcher
	tele  *telemetry.Collector
	fwd   *logs.Forwarder
	upg   *upgrade.Upgrader
	srv   *metrics.Server

	mu              sync.Mutex
	currentRevision string
	currentSHA256   string
	lastApplyAt     time.Time
	lastApplyError  string

	ready atomic.Bool
}

// New 构建 Agent（不会建立网络连接，便于测试）。
func New(cfg *config.Config, cr *creds.Credentials, log *slog.Logger, hub *logging.Hub, m *metrics.Metrics) (*Agent, error) {
	a := &Agent{cfg: cfg, cr: cr, log: log, hub: hub, m: m}
	log = logging.Component(log, "agent")

	if err := cfg.EnsureDirs(); err != nil {
		return nil, err
	}

	ob, err := outbox.Open(cfg.Outbox.Path, cfg.Outbox.MaxBytes, cfg.Outbox.MaxEvents, cfg.Outbox.Fsync, cfg.Outbox.CompactMinDrops)
	if err != nil {
		return nil, fmt.Errorf("打开事件缓冲失败: %w", err)
	}
	a.ob = ob
	pending, _, dropped, acked, last := ob.Stat()
	if pending > 0 {
		log.Info("从 outbox 恢复未投递事件",
			slog.Int("pending", pending), slog.Uint64("last_acked_seq", acked), slog.Uint64("max_seq", last))
	}
	if dropped > 0 {
		log.Warn("outbox 曾因超限丢弃事件", slog.Uint64("dropped", dropped))
	}

	a.store = hyconfig.NewStore(cfg.Hysteria.HistoryDir, cfg.Hysteria.ConfigPath, cfg.Hysteria.HistoryKeep)
	if cfg.Hysteria.APIBaseURL != "" {
		a.api = hyapi.New(cfg.Hysteria.APIBaseURL, cfg.Hysteria.APISecret, 5*time.Second)
	}
	a.sup = supervisor.New(cfg, logging.Component(log, "supervisor"), m, hub, a.api)

	// 控制通道：handler 就是本 Agent。
	a.cli = control.New(cfg, cr, logging.Component(log, "control"), m, ob, hub, a)
	a.tele = telemetry.New(cfg, m, logging.Component(log, "telemetry"), a.api, func() telemetry.HysteriaState {
		st := a.sup.State()
		return telemetry.HysteriaState{
			Running: st.Running, PID: st.PID, UptimeSeconds: st.UptimeSeconds,
			Version: st.Version, Restarts: st.Restarts, LastExitCode: st.ExitCode,
			ConfigRevision: a.Revision(),
		}
	})
	a.fwd = logs.New(cfg.Logs, logging.Component(log, "logs"), hub, m, a.cli.Enqueue)

	if cfg.Upgrade.Enabled {
		client, err := a.httpsClient()
		if err != nil {
			log.Warn("构建升级下载客户端失败，自升级将不可用", slog.String("error", err.Error()))
		} else if upg, err := upgrade.New(cfg, logging.Component(log, "upgrade"), m, client); err != nil {
			log.Warn("自升级不可用（缺少验签公钥）", slog.String("error", err.Error()))
		} else {
			// 重启前先把 Hysteria 停干净，避免 exec 后留下占着端口的孤儿进程。
			upg.PreRestart = func(ctx context.Context) error {
				log.Info("升级重启前停止被守护的 Hysteria")
				return a.sup.Stop(ctx)
			}
			a.upg = upg
		}
	}

	a.disp = commands.New(commands.Deps{
		Cfg: cfg, Log: logging.Component(log, "commands"), M: m, Hub: hub,
		Super: a.sup, API: a.api, Store: a.store, Upgrader: a.upg, Applier: a, Sender: a.cli,
		State:       a.StateSnapshot,
		DiagDeps:    a.diagDeps,
		ClearOutbox: func() (int, error) { return a.ob.Clear() },
	})

	a.sup.OnEvent(func(ev supervisor.Event) { a.reportSupervisorEvent(ev) })
	if cfg.Metrics.Enabled {
		a.srv = metrics.NewServer(m, metrics.MetricsConfig{
			Listen: cfg.Metrics.Listen, Path: cfg.Metrics.Path,
			HealthPath: cfg.Metrics.HealthPath, ReadyPath: cfg.Metrics.ReadyPath,
			ReadHeaderTimeoutS: cfg.Metrics.ReadHeaderTimeoutS, Version: version.Version, NodeID: cr.NodeID,
		})
	}
	m.SetReadyChecker(a.readiness)
	return a, nil
}

// httpsClient 构建带 pinning 的 HTTPS 客户端（自升级下载用）。
func (a *Agent) httpsClient() (*http.Client, error) {
	tlsCfg, err := a.cr.TLSConfig(&a.cfg.Control, func(msg string) { a.log.Warn(msg) })
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: time.Duration(a.cfg.Upgrade.DownloadTimeoutSeconds * float64(time.Second)),
		Transport: &http.Transport{
			TLSClientConfig:     tlsCfg,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        4,
			TLSHandshakeTimeout: a.cfg.Control.ConnectTimeout(),
		},
	}, nil
}

// Run 启动全部子系统并阻塞到 ctx 结束。
func (a *Agent) Run(ctx context.Context) error {
	log := a.log
	log.Info("hysteria-agent 启动",
		slog.String("version", version.Version),
		slog.String("node_id", a.cr.NodeID),
		slog.String("endpoint", a.cfg.Control.ServerURL),
		slog.String("hysteria_mode", a.cfg.Hysteria.Mode),
		slog.String("config", a.cfg.Hysteria.ConfigPath))

	if a.srv != nil {
		addr, err := a.srv.Start()
		if err != nil {
			// 指标端点起不来不影响节点服务，但要明确告警。
			log.Error("本地指标端点启动失败", slog.String("error", err.Error()))
		} else {
			log.Info("指标端点已启动", slog.String("addr", addr),
				slog.String("metrics", "http://"+addr+a.cfg.Metrics.Path),
				slog.String("healthz", "http://"+addr+a.cfg.Metrics.HealthPath),
				slog.String("readyz", "http://"+addr+a.cfg.Metrics.ReadyPath))
		}
	}

	// 读取当前生效配置
	a.refreshCurrentConfig()

	// 启动 Hysteria（有配置才启动；没有就先等控制面下发）
	if _, err := os.Stat(a.cfg.Hysteria.ConfigPath); err == nil {
		sctx, cancel := context.WithTimeout(ctx, a.cfg.Hysteria.StartupTimeout())
		if err := a.sup.Start(sctx); err != nil {
			log.Error("Hysteria 启动失败", slog.String("error", err.Error()))
			a.reportEvent("start_failed", 0, err.Error(), nil)
		}
		cancel()
	} else {
		log.Warn("目标配置不存在，等待控制面下发配置后再启动 Hysteria",
			slog.String("path", a.cfg.Hysteria.ConfigPath))
	}

	var wg sync.WaitGroup
	start := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("子系统 panic", slog.String("component", name), slog.Any("panic", r))
				}
			}()
			fn()
		}()
	}

	start("control", func() { a.cli.Run(ctx) })
	start("telemetry", func() { a.telemetryLoop(ctx) })
	start("logs", func() { a.fwd.Run(ctx) })
	start("state", func() { a.stateLoop(ctx) })
	start("events", func() { a.eventLoop(ctx) })

	<-ctx.Done()
	log.Info("收到退出信号，开始优雅停止")

	// 停 Hysteria（SIGTERM → 等待 → SIGKILL）
	sctx, cancel := context.WithTimeout(context.Background(), a.cfg.Hysteria.ShutdownTimeout()+5*time.Second)
	if err := a.sup.Stop(sctx); err != nil {
		log.Warn("停止 Hysteria 时出错", slog.String("error", err.Error()))
	}
	cancel()
	a.sup.Close()

	if a.srv != nil {
		cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = a.srv.Shutdown(cctx)
		ccancel()
	}
	if err := a.ob.Close(); err != nil {
		log.Warn("关闭 outbox 失败", slog.String("error", err.Error()))
	}
	wg.Wait()
	log.Info("hysteria-agent 已退出")
	return nil
}

// telemetryLoop 每个周期重新读取控制面协商的间隔（HelloAck 可能改过）。
func (a *Agent) telemetryLoop(ctx context.Context) {
	emit := func(b *agentpb.MetricBatch) {
		if _, err := a.cli.Enqueue("metric", wire.New(agentpb.MsgType_MSG_TYPE_METRIC_BATCH, b)); err != nil {
			a.log.Warn("指标入队失败", slog.String("error", err.Error()))
		}
	}
	for {
		interval := a.cli.TelemetryInterval()
		if interval <= 0 {
			interval = a.cfg.Control.TelemetryInterval()
		}
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		b := a.tele.Collect(ctx)
		func() {
			defer func() {
				if r := recover(); r != nil {
					a.log.Error("指标采集 panic", slog.Any("panic", r))
				}
			}()
			emit(b)
		}()
	}
}

// stateLoop 周期刷新握手元数据、就绪状态与状态文件。
func (a *Agent) stateLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	write := func() {
		st := a.sup.State()
		a.cli.SetState(a.readinessOK(), st.Running, a.Revision(), a.SHA256())
		a.cli.SetConfigOnDisk(a.diskConfigIdentity())
		a.m.Snapshot(a.cli.Status().ConnectedSince)
		if err := a.writeStateFile(); err != nil {
			a.log.Debug("写状态文件失败", slog.String("error", err.Error()))
		}
	}
	write()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						a.log.Error("状态刷新 panic", slog.Any("panic", r))
					}
				}()
				write()
			}()
		}
	}
}

// eventLoop 上报"启动完成/心跳失联"这类需要控制面知道的节点事件。
func (a *Agent) eventLoop(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	var lastConnected bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			connected := a.cli.Connected()
			if connected != lastConnected {
				lastConnected = connected
				// 由断到通是正常恢复；由通到断只记事件，不刷屏。
				if !connected {
					a.reportEvent("control_disconnected", 0, "控制通道断开", nil)
				}
			}
		}
	}
}

// ---------------------------- control.Handler ----------------------------

// HandleHelloAck 握手完成：如果本地配置版本与控制面不一致，等控制面下发 ConfigPush。
func (a *Agent) HandleHelloAck(ctx context.Context, ack *agentpb.HelloAck) {
	want := ack.GetConfigRevision()
	have := a.Revision()
	if want != "" && want != have {
		a.log.Info("本地配置版本与控制面不一致，等待控制面下发",
			slog.String("local", have), slog.String("remote", want))
	}
	a.cli.SetState(a.readinessOK(), a.sup.IsRunning(), have, a.SHA256())
	a.cli.SetConfigOnDisk(a.diskConfigIdentity())
}

// HandleConfigPush 处理配置下发：校验 → 快照 → 原子落盘 → 热更新 → 失败回滚。
func (a *Agent) HandleConfigPush(ctx context.Context, cp *agentpb.ConfigPush) {
	log := logging.Component(a.log, "hyconfig")
	started := time.Now()
	ack := a.ApplyPayload(ctx, cp.GetPayload(), cp.GetFormat(), cp.GetRevision(), "control", cp.GetReason(), cp.GetDryRun())
	if cp.GetSignature() != nil {
		// 配置签名（可选）：控制面如果签了配置，就在这里校验（ed25519 与升级共用公钥集）。
		log.Debug("收到带签名的配置下发（本版本仅记录，不阻断）")
	}
	if _, err := a.cli.Enqueue("config_ack", wire.New(agentpb.MsgType_MSG_TYPE_CONFIG_ACK, ack)); err != nil {
		log.Warn("回传配置回执失败", slog.String("error", err.Error()))
	}
	log.Info("配置下发处理完成",
		slog.String("revision", cp.GetRevision()),
		slog.Bool("ok", ack.GetOk()),
		slog.Bool("dry_run", ack.GetDryRun()),
		slog.Bool("rolled_back", ack.GetRolledBack()),
		slog.Float64("duration_seconds", time.Since(started).Seconds()))
}

// HandleCommand 把指令交给白名单分发器。
func (a *Agent) HandleCommand(ctx context.Context, req *agentpb.CommandRequest) {
	a.disp.Handle(ctx, req)
}

// HandleUpgradeOffer 处理控制面主动下发的升级提议。
func (a *Agent) HandleUpgradeOffer(ctx context.Context, offer *agentpb.UpgradeOffer) {
	if a.upg == nil {
		a.log.Warn("收到升级提议但自升级不可用（无验签公钥）")
		return
	}
	mf := upgrade.Manifest{
		Version:   offer.GetVersion(),
		URL:       offer.GetUrl(),
		SHA256:    offer.GetSha256(),
		Signature: encodeSig(offer.GetSignature()),
		Size:      offer.GetSize(),
		Force:     offer.GetForce(),
		Notes:     offer.GetNotes(),
	}
	a.log.Info("收到升级提议", slog.String("version", mf.Version), slog.Bool("force", mf.Force))
	go func() {
		defer func() {
			if r := recover(); r != nil {
				a.log.Error("升级过程 panic", slog.Any("panic", r))
			}
		}()
		res, err := a.upg.Run(ctx, mf)
		payload, _ := json.Marshal(map[string]any{"manifest": mf, "result": res, "error": errString(err)})
		a.reportEvent("upgrade", 0, errString(err), payload)
	}()
}

// ---------------------------- commands.ConfigApplier ----------------------------

// ApplyPayload 是配置下发的唯一入口：校验 → 快照 → 原子写 → 热更新 →（失败）回滚。
func (a *Agent) ApplyPayload(ctx context.Context, payload []byte, format, revision, source, reason string, dryRun bool) *agentpb.ConfigAck {
	started := time.Now()
	log := logging.Component(a.log, "hyconfig")
	ack := &agentpb.ConfigAck{Revision: revision, PreviousRevision: a.Revision(), DryRun: dryRun}

	sum := hyconfig.Sum(payload)
	ack.Sha256 = sum

	// 相同内容（且非强制）直接短路，避免无谓重启。
	if !dryRun && sum == a.SHA256() {
		ack.Ok = true
		ack.Checks = []string{"content:unchanged"}
		a.m.ConfigApplyTotal.WithLabelValues("ok").Inc()
		log.Info("配置内容与当前一致，跳过应用", slog.String("revision", revision))
		return ack
	}

	// 1) 校验
	opt := hyconfig.ValidateOptions{
		CheckPort:             a.cfg.Hysteria.ValidatePort,
		CheckCert:             a.cfg.Hysteria.ValidateCert,
		CertWarnDays:          a.cfg.Hysteria.CertWarnDays,
		OwnerPID:              a.sup.Pid(),
		RequireTrafficStats:   a.cfg.Telemetry.HysteriaEnabled,
		ExpectedTrafficListen: a.cfg.Hysteria.APIBaseURL,
		ExpectedTrafficSecret: a.cfg.Hysteria.APISecret,
		HysteriaBinary:        a.cfg.Hysteria.Binary,
	}
	res := hyconfig.Validate(ctx, payload, format, opt)
	ack.Warnings = res.Warnings
	ack.Checks = res.Checks

	// 唯一的硬底线：**连配置都解析不出来**（写下去只会让 Hysteria 起不来）。
	if res.Parsed == nil {
		ack.Ok = false
		ack.Error = "配置无法解析（YAML/结构错误）: " + joinErr(res.Errors)
		ack.DurationMs = time.Since(started).Milliseconds()
		a.m.ConfigApplyTotal.WithLabelValues("rejected").Inc()
		a.m.ConfigApplySeconds.WithLabelValues("rejected").Observe(time.Since(started).Seconds())
		a.recordVersion(payload, res, revision, format, source, reason, "rejected", ack.Error)
		a.reportEvent("config_failed", 0, ack.Error, mustJSON(map[string]any{"checks": res.Checks, "errors": res.Errors}))
		log.Error("拒绝下发配置（无法解析）", slog.String("revision", revision), slog.String("error", ack.Error))

		return ack
	}

	// 其余校验问题（证书路径不存在、secret 与 agent 不一致、端口被占…）**只作警告，仍写入** ——
	// 与控制面的「以面板为准」原则、以及 CLI 的 `config sync` 保持一致。
	// 这里曾是硬门禁，后果是节点永远不更新：控制面下发的正文与节点现状稍有不符就被拒收，
	// 节点磁盘一直停在旧内容（真实现场：japan2 每 4 秒回执一次
	// 「证书/私钥无法加载: open /etc/hysteria/cert.pem: no such file or directory」，配置永不更新）。
	// 写下去的风险由后面的就绪检查 + 自动回滚兜底：起不来就恢复旧配置并上报原因。
	if !res.Valid {
		ack.Warnings = append(ack.Warnings,
			fmt.Sprintf("校验有 %d 处问题，按「面板为准」仍写入: %s", len(res.Errors), joinErr(res.Errors)))
		log.Warn("下发的配置有校验问题，按「面板为准」仍写入",
			slog.String("revision", revision), slog.String("errors", joinErr(res.Errors)))
	}
	if dryRun {
		ack.Ok = true
		ack.DurationMs = time.Since(started).Milliseconds()
		a.m.ConfigApplyTotal.WithLabelValues("dry_run").Inc()
		log.Info("配置 dry-run 校验通过", slog.String("revision", revision))
		return ack
	}

	// 2) 备份当前配置到历史（回滚依据）
	if cur, err := os.ReadFile(a.cfg.Hysteria.ConfigPath); err == nil && len(cur) > 0 {
		if v, err := a.store.Snapshot(cur, a.Revision(), "yaml", "local", "应用新配置前自动备份"); err == nil {
			v.Result = "ok"
			v.AppliedAt = time.Now().UTC()
			_ = a.store.Record(*v)
		}
	}

	// 3) 快照新配置 + 原子落盘（0600）
	snap, err := a.store.Snapshot(payload, revision, res.Format, source, reason)
	if err != nil {
		ack.Ok = false
		ack.Error = "写入配置历史失败: " + err.Error()
		a.m.ConfigApplyTotal.WithLabelValues("failed").Inc()
		return ack
	}
	snap.Result = "applying"
	snap.Checks = append(snap.Checks, res.Checks...)
	snap.Warnings = res.Warnings
	snap.AppliedAt = time.Now().UTC()
	if err := a.store.Record(*snap); err != nil {
		log.Warn("记录配置历史失败", slog.String("error", err.Error()))
	}
	if _, err := a.store.Apply(payload); err != nil {
		ack.Ok = false
		ack.Error = "写入目标配置失败: " + err.Error()
		a.m.ConfigApplyTotal.WithLabelValues("failed").Inc()
		return ack
	}
	log.Info("配置已原子写入", slog.String("path", a.cfg.Hysteria.ConfigPath), slog.String("sha256", short(sum)))

	// 4) 热更新（Hysteria 无 SIGHUP，用 SIGTERM 优雅停止 + 新配置拉起，见 supervisor 包注释）
	rctx, cancel := context.WithTimeout(ctx, a.cfg.Hysteria.StartupTimeout()+a.cfg.Hysteria.ShutdownTimeout())
	defer cancel()
	if err := a.sup.Reload(rctx, fmt.Sprintf("配置更新 rev=%s", revision)); err != nil {
		ack.Ok = false
		ack.Error = "配置已写入但 Hysteria 未能以新配置就绪: " + err.Error() + hysteriaLogHint(a.sup)
		a.m.ConfigApplyTotal.WithLabelValues("failed").Inc()
		// 5) 自动回滚
		if rb, rerr := a.rollbackLocked(rctx, "配置应用失败自动回滚"); rerr == nil {
			ack.RolledBack = true
			ack.Error += fmt.Sprintf("；已自动回滚到 %s", rb)
			a.m.ConfigApplyTotal.WithLabelValues("rolled_back").Inc()
			a.reportEvent("config_rolled_back", 0, ack.Error, mustJSON(map[string]any{"revision": revision, "rolled_back_to": rb}))
		} else {
			ack.Error += "；自动回滚也失败: " + rerr.Error()
			log.Error("自动回滚失败", slog.String("error", rerr.Error()))
		}
		a.recordVersion(payload, res, revision, res.Format, source, reason, "failed", ack.Error)
		ack.DurationMs = time.Since(started).Milliseconds()
		a.m.ConfigApplySeconds.WithLabelValues("failed").Observe(time.Since(started).Seconds())
		log.Error("配置应用失败", slog.String("revision", revision), slog.String("error", ack.Error))
		return ack
	}

	// 6) 成功
	snap.Result = "ok"
	snap.AppliedAt = time.Now().UTC()
	_ = a.store.Record(*snap)
	a.setCurrent(revision, sum)
	a.sup.SetRevision(revision)
	a.m.ConfigApplyTotal.WithLabelValues("ok").Inc()
	a.m.ConfigApplySeconds.WithLabelValues("ok").Observe(time.Since(started).Seconds())
	a.m.ConfigRevisionInfo.Reset()
	a.m.ConfigRevisionInfo.WithLabelValues(revision, short(sum)).Set(1)
	ack.Ok = true
	ack.DurationMs = time.Since(started).Milliseconds()
	a.mu.Lock()
	a.lastApplyAt = time.Now()
	a.lastApplyError = ""
	a.mu.Unlock()
	a.reportEvent("config_applied", 0, "配置已生效", mustJSON(map[string]any{
		"revision": revision, "sha256": sum, "checks": res.Checks, "warnings": res.Warnings,
	}))
	log.Info("配置已生效", slog.String("revision", revision), slog.Int64("duration_ms", ack.DurationMs))
	return ack
}

// ReloadCurrent 用磁盘上的配置重新加载（不换内容）。
func (a *Agent) ReloadCurrent(ctx context.Context, reason string) *agentpb.ConfigAck {
	payload, err := os.ReadFile(a.cfg.Hysteria.ConfigPath)
	if err != nil {
		return &agentpb.ConfigAck{Ok: false, Error: "读取当前配置失败: " + err.Error()}
	}
	ack := a.ApplyPayload(ctx, payload, "yaml", a.Revision(), "local", "reload: "+reason, false)
	return ack
}

// Rollback 回滚到上一个（或指定）版本。
func (a *Agent) Rollback(ctx context.Context, key string) *agentpb.ConfigAck {
	started := time.Now()
	target, err := a.rollbackLocked(ctx, "远程回滚指令 rev="+key)
	ack := &agentpb.ConfigAck{PreviousRevision: a.Revision()}
	if err != nil {
		ack.Ok = false
		ack.Error = err.Error()
		a.m.ConfigApplyTotal.WithLabelValues("failed").Inc()
		return ack
	}
	ack.Ok = true
	ack.Revision = target
	ack.PreviousRevision = a.Revision()
	ack.DurationMs = time.Since(started).Milliseconds()
	a.m.ConfigApplyTotal.WithLabelValues("rolled_back").Inc()
	a.reportEvent("config_rolled_back", 0, "已回滚配置", mustJSON(map[string]any{"to": target}))
	return ack
}

// rollbackLocked 恢复历史版本并重启 Hysteria，返回恢复到的 revision。
func (a *Agent) rollbackLocked(ctx context.Context, reason string) (string, error) {
	v, _, err := a.store.Rollback()
	if err != nil {
		return "", err
	}
	// 记录"回滚"这件事本身（写进历史索引，便于审计）
	rec := *v
	rec.Result = "rolled_back"
	rec.Source = "rollback"
	rec.Reason = reason
	rec.AppliedAt = time.Now().UTC()
	_ = a.store.Record(rec)

	if err := a.sup.Reload(ctx, reason); err != nil {
		return v.Revision, fmt.Errorf("已恢复配置 %s 但 Hysteria 启动失败: %w", v.Revision, err)
	}
	a.setCurrent(v.Revision, v.SHA256)
	a.sup.SetRevision(v.Revision)
	a.m.ConfigRevisionInfo.Reset()
	a.m.ConfigRevisionInfo.WithLabelValues(v.Revision, short(v.SHA256)).Set(1)
	a.log.Info("已回滚配置", slog.String("revision", v.Revision), slog.String("reason", reason))
	return v.Revision, nil
}

// recordVersion 把一次（失败的）应用记进历史，便于事后对账。
func (a *Agent) recordVersion(payload []byte, res *hyconfig.Result, revision, format, source, reason, result, errMsg string) {
	snap, err := a.store.Snapshot(payload, revision, format, source, reason)
	if err != nil {
		return
	}
	snap.Result = result
	snap.Error = errMsg
	snap.Checks = res.Checks
	snap.Warnings = res.Warnings
	snap.AppliedAt = time.Now().UTC()
	_ = a.store.Record(*snap)
}

// ---------------------------- 状态与事件 ----------------------------

func (a *Agent) Revision() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.currentRevision
}

func (a *Agent) SHA256() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.currentSHA256
}

// diskConfigIdentity 回报本机 config.yaml 的**实际**身份，供面板「配置版本」显示。
// 逻辑在 hyconfig.Store.DiskIdentity（可单测）：认不出对应版本时给 0 + 实际 sha256。
func (a *Agent) diskConfigIdentity() (int, string) {
	n, sum := a.store.DiskIdentity()
	if n == 0 {
		a.log.Warn("本机配置与控制面已知版本都对不上（可能被手工改过），只上报 sha256", slog.String("sha256", short(sum)))
	}

	return n, sum
}

func (a *Agent) setCurrent(revision, sha string) {
	a.mu.Lock()
	a.currentRevision = revision
	a.currentSHA256 = sha
	a.mu.Unlock()
}

// refreshCurrentConfig 启动时读取当前生效配置的 hash/revision。
func (a *Agent) refreshCurrentConfig() {
	sum, err := a.store.ConfigHash()
	if err != nil {
		a.log.Warn("读取当前配置失败", slog.String("error", err.Error()))
	}
	rev := ""
	if sum != "" {
		rev = a.store.CurrentRevision()
	}
	a.setCurrent(rev, sum)
	a.m.ConfigRevisionInfo.Reset()
	if rev != "" || sum != "" {
		a.m.ConfigRevisionInfo.WithLabelValues(rev, short(sum)).Set(1)
	}
	if sum != "" {
		a.log.Info("检测到已有配置", slog.String("revision", rev), slog.String("sha256", short(sum)))
	}
}

// hysteriaLogHint 把 Hysteria 最后几行输出附在错误后面。
//
// 价值：真实现场（japan2 重启风暴）面板上只能看到「exit=1」这种总结，看不出 Hysteria 到底
// 在抱怨什么，必须登机器翻 journal。子进程的 stderr 就在 supervisor 手里，顺手带上即可。
func hysteriaLogHint(sup interface{ RecentLogs(int) []string }) string {
	if sup == nil {
		return ""
	}
	lines := sup.RecentLogs(6)
	if len(lines) == 0 {
		return ""
	}
	// 只留有用的部分并限制长度，避免把回执撑爆。
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if len(l) > 200 {
			l = l[:200] + "…"
		}
		out = append(out, l)
	}
	if len(out) == 0 {
		return ""
	}

	return "；Hysteria 最后输出: " + strings.Join(out, " | ")
}

// reportSupervisorEvent 把子进程事件转成 NodeEvent 上报。
func (a *Agent) reportSupervisorEvent(ev supervisor.Event) {
	payload, _ := json.Marshal(map[string]any{
		"at": ev.At.Format(time.RFC3339), "event": ev.Type,
	})
	a.reportEvent(string(ev.Type), ev.ExitCode, ev.Reason, payload, ev.LogTail...)
	if ev.Type == supervisor.EventGiveUp {
		a.log.Error("Hysteria 已放弃自动重启，需要人工介入", slog.String("reason", ev.Reason))
	}
}

// reportEvent 上报一条节点事件（走 outbox，断线也不丢）。
func (a *Agent) reportEvent(kind string, exitCode int, reason string, data []byte, logTail ...string) {
	ev := &agentpb.NodeEvent{
		Type: kind, TsMs: time.Now().UnixMilli(), ExitCode: int32(exitCode),
		Reason: reason, DataJson: data, Revision: a.Revision(),
	}
	if pid := a.sup.Pid(); pid > 0 {
		ev.Pid = fmt.Sprintf("%d", pid)
	}
	if len(logTail) > 20 {
		logTail = logTail[len(logTail)-20:]
	}
	ev.LogTail = logTail
	if _, err := a.cli.Enqueue("event", wire.New(agentpb.MsgType_MSG_TYPE_NODE_EVENT, ev)); err != nil {
		a.log.Warn("上报节点事件失败", slog.String("error", err.Error()))
	}
}

// readiness 实现 metrics.ReadyChecker。
func (a *Agent) readiness() (bool, map[string]string) {
	st := a.sup.State()
	ctrl := a.cli.Status()
	checks := map[string]string{
		"hysteria":        boolStr(st.Running, "running", "stopped"),
		"control_channel": boolStr(ctrl.Connected, "connected", "disconnected"),
		"config":          ternaryStr(a.SHA256() != "", "applied:"+short(a.SHA256()), "missing"),
		"hysteria_mode":   st.Mode,
	}
	return a.readinessOK(), checks
}

func (a *Agent) readinessOK() bool {
	return a.sup.IsRunning() && a.SHA256() != ""
}

// StateSnapshot 返回完整状态（GET_STATE 指令与 state.json 都用它）。
func (a *Agent) StateSnapshot() map[string]any {
	a.mu.Lock()
	rev, sha, lastApply, lastErr := a.currentRevision, a.currentSHA256, a.lastApplyAt, a.lastApplyError
	a.mu.Unlock()

	st := a.sup.State()
	ctrl := a.cli.Status()
	sent, skipped := a.fwd.Stats()
	history, _ := a.store.List()
	revs := make([]string, 0, len(history))
	for _, h := range history {
		revs = append(revs, fmt.Sprintf("%s(%s)", h.Revision, h.Result))
	}
	return map[string]any{
		"node_id":        a.cr.NodeID,
		"agent_version":  version.Version,
		"uptime_seconds": int(time.Since(agentStart).Seconds()),
		"ready":          a.readinessOK(),
		"config": map[string]any{
			"path":             a.cfg.Hysteria.ConfigPath,
			"revision":         rev,
			"sha256":           sha,
			"last_apply_at":    timeOrEmpty(lastApply),
			"last_apply_error": lastErr,
			"history":          revs,
		},
		"control":  ctrl,
		"hysteria": st,
		"logs": map[string]any{
			"remote_enabled": a.cfg.Logs.RemoteEnabled,
			"sent":           sent,
			"sampled_out":    skipped,
		},
	}
}

// writeStateFile 供 `hysteria-agent status` 在没有 metrics 端点时也能读到状态。
func (a *Agent) writeStateFile() error {
	snap := a.StateSnapshot()
	snap["written_at"] = time.Now().UTC().Format(time.RFC3339)
	snap["pid"] = os.Getpid()
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(a.cfg.StateDir, "state.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// diagDeps 组装诊断依赖。
func (a *Agent) diagDeps() diag.Deps {
	st := a.sup.State()
	ctrl := a.cli.Status()
	return diag.Deps{
		Cfg: a.cfg, NodeID: a.cr.NodeID, API: a.api, Super: a.sup,
		HyVersion: st.Version, ControlConnected: ctrl.Connected, ControlSkewMs: ctrl.ServerSkewMs,
		ConfigRevision: a.Revision(), ConfigSHA256: a.SHA256(),
		OutboxPending: ctrl.PendingEvents, OutboxBytes: ctrl.PendingBytes, OutboxDropped: ctrl.DroppedEvents,
	}
}

// ---------------------------- 小工具 ----------------------------

var agentStart = time.Now()

func short(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

func boolStr(b bool, t, f string) string {
	if b {
		return t
	}
	return f
}

func ternaryStr(cond bool, t, f string) string {
	if cond {
		return t
	}
	return f
}

func timeOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func joinErr(errs []string) string {
	out := ""
	for i, e := range errs {
		if i > 0 {
			out += "; "
		}
		out += e
	}
	return out
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func encodeSig(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}
