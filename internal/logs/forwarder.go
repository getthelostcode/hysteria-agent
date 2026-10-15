// Package logs 实现"本地日志 → 控制面"的远程采集：批量、采样、限速。
//
// 数据来源是 logging.Hub（agent 自己的 slog 输出 + 被守护 Hysteria 子进程的 stdout/stderr 都汇进这里），
// 所以控制面看到的是统一的日志流，不需要在两处分别采集。
package logs

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
)

// EnqueueFunc 由 control.Client.Enqueue 实现：把信封交给可靠投递通道。
type EnqueueFunc func(kind string, env *agentpb.Envelope) (uint64, error)

// Forwarder 定期把 Hub 里的日志批量投递给控制面。
type Forwarder struct {
	cfg     config.Logs
	log     *slog.Logger
	hub     *logging.Hub
	m       *metrics.Metrics
	enqueue EnqueueFunc
	sampler *logging.Sampler

	minLevel int
	sent     uint64
	skipped  uint64
}

// New 构建转发器。
func New(cfg config.Logs, log *slog.Logger, hub *logging.Hub, m *metrics.Metrics, enqueue EnqueueFunc) *Forwarder {
	return &Forwarder{
		cfg:      cfg,
		log:      log,
		hub:      hub,
		m:        m,
		enqueue:  enqueue,
		sampler:  logging.NewSampler(cfg.Sample.MaxLinesPerSecond, cfg.Sample.Burst),
		minLevel: levelRank(cfg.Sample.MinLevel),
	}
}

// Run 订阅 Hub 并按 flush_interval 成批投递，直到 ctx 结束。
func (f *Forwarder) Run(ctx context.Context) {
	if !f.cfg.RemoteEnabled {
		f.log.Info("远程日志采集已关闭（logs.remote_enabled=false）")
		return
	}
	sub := f.hub.Subscribe(1024)
	defer sub.Close()

	interval := f.cfg.SampleFlushInterval()
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	buf := make([]*agentpb.LogEntry, 0, f.cfg.Sample.MaxBatch*2)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		payload := make([]*agentpb.LogEntry, 0, len(buf))
		for _, e := range buf {
			if levelRank(e.GetLevel()) < f.minLevel {
				atomic.AddUint64(&f.skipped, 1)
				f.m.LogsSampledOut.Inc()
				continue
			}
			if !f.sampler.Allow(e.GetLevel()) {
				atomic.AddUint64(&f.skipped, 1)
				f.m.LogsSampledOut.Inc()
				continue
			}
			if max := f.cfg.Sample.MaxLineBytes; max > 0 && len(e.GetMsg()) > max {
				e.Msg = e.GetMsg()[:max] + "…(截断)"
			}
			payload = append(payload, e)
		}
		buf = buf[:0]
		if len(payload) == 0 {
			return
		}
		env := logging.NewLogEnvelope(payload, "agent")
		if _, err := f.enqueue("log", env); err != nil {
			f.log.Warn("日志入队失败", slog.String("error", err.Error()))
			return
		}
		atomic.AddUint64(&f.sent, uint64(len(payload)))
		f.m.LogsSent.Add(float64(len(payload)))
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case e, ok := <-sub.Ch:
			if !ok {
				flush()
				return
			}
			buf = append(buf, e)
			if len(buf) >= f.cfg.Sample.MaxBatch {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// Stats 返回 (已投递, 被采样丢弃)。
func (f *Forwarder) Stats() (sent, skipped uint64) {
	return atomic.LoadUint64(&f.sent), atomic.LoadUint64(&f.skipped)
}

func levelRank(level string) int {
	switch level {
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
