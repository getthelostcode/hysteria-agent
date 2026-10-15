// Package telemetry 负责周期采集主机指标 + Hysteria 运行指标，并交付给控制通道。
package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// HysteriaState 是 supervisor 提供给遥测的"运行态快照"（避免包循环依赖）。
type HysteriaState struct {
	Running        bool
	PID            int
	UptimeSeconds  uint64
	Version        string
	Restarts       uint64
	LastExitCode   int
	ConfigRevision string
}

// Collector 采集器。
type Collector struct {
	cfg     *config.Config
	m       *metrics.Metrics
	log     *slog.Logger
	meter   *sysinfo.Meter
	api     *hyapi.Client
	stateFn func() HysteriaState

	mu            sync.Mutex
	cumTx, cumRx  uint64
	lastTrafficEr string
}

// New 构建采集器。stateFn 可以为 nil（不管理 Hysteria 时）。
func New(cfg *config.Config, m *metrics.Metrics, log *slog.Logger, api *hyapi.Client, stateFn func() HysteriaState) *Collector {
	return &Collector{
		cfg: cfg, m: m, log: log,
		meter:   sysinfo.NewMeter(),
		api:     api,
		stateFn: stateFn,
	}
}

// Collect 采集一次并返回可直接投递的 MetricBatch。
func (c *Collector) Collect(ctx context.Context) *agentpb.MetricBatch {
	batch := &agentpb.MetricBatch{
		AgentVersion:   versionOf(),
		ConfigRevision: c.configRevision(),
		Labels:         map[string]string{"hostname": sysinfo.Hostname()},
	}

	sample, err := c.meter.Sample(c.cfg.Telemetry.DiskPaths, c.cfg.Telemetry.NetInterfaces)
	if err != nil {
		c.log.Warn("采集主机指标失败", slog.String("error", err.Error()))
		sample = &agentpb.MetricSample{TsMs: time.Now().UnixMilli()}
	}
	batch.Sample = sample
	c.m.CPUPercent.Set(sample.GetCpuPercent())
	c.m.MemPercent.Set(sample.GetMemPercent())
	c.m.MemUsed.Set(float64(sample.GetMemUsedBytes()))
	c.m.DiskPercent.Set(sample.GetDiskPercent())
	c.m.DiskUsed.Set(float64(sample.GetDiskUsedBytes()))
	c.m.Load1.Set(sample.GetLoad1())
	c.m.NetRxBytes.Add(float64(sample.GetNetRxBytes()))
	c.m.NetTxBytes.Add(float64(sample.GetNetTxBytes()))

	hs := &agentpb.HysteriaStats{}
	if c.stateFn != nil {
		st := c.stateFn()
		hs.Running = st.Running
		hs.Pid = int32(st.PID)
		hs.UptimeSeconds = st.UptimeSeconds
		hs.Version = st.Version
		hs.RestartCount = st.Restarts
		hs.LastExitCode = int32(st.LastExitCode)
	}
	// Hysteria 自带计数器：用 clear=1 拿"自上次读取以来的增量"，不需要本地维护基线，
	// 服务端重启后清零也不会产生负数增量。
	if c.cfg.Telemetry.HysteriaEnabled && c.api != nil && hs.Running {
		if traffic, err := c.api.Traffic(ctx, true); err != nil {
			if err.Error() != c.lastTrafficEr {
				c.log.Warn("读取 Hysteria 流量失败", slog.String("error", err.Error()))
				c.lastTrafficEr = err.Error()
			}
		} else {
			c.lastTrafficEr = ""
			var tx, rx uint64
			for _, t := range traffic {
				tx += t.Tx
				rx += t.Rx
			}
			hs.TxBytes = tx
			hs.RxBytes = rx
			hs.ActiveUsers = uint64(len(traffic))
		}
		if online, err := c.api.Online(ctx); err == nil {
			var devs uint64
			for _, n := range online {
				devs += uint64(n)
			}
			hs.OnlineDevices = devs
			hs.Conns = devs
		}
	}
	batch.Hysteria = hs

	c.m.HysteriaRunning.Set(boolToFloat(hs.GetRunning()))
	c.m.HysteriaUptime.Set(float64(hs.GetUptimeSeconds()))
	c.m.HysteriaActiveUsers.Set(float64(hs.GetActiveUsers()))
	c.m.HysteriaOnlineDevices.Set(float64(hs.GetOnlineDevices()))
	c.m.HysteriaTxBytes.Add(float64(hs.GetTxBytes()))
	c.m.HysteriaRxBytes.Add(float64(hs.GetRxBytes()))
	c.m.HysteriaExitCode.Set(float64(hs.GetLastExitCode()))

	c.mu.Lock()
	c.cumTx += hs.GetTxBytes()
	c.cumRx += hs.GetRxBytes()
	c.mu.Unlock()
	return batch
}

// Run 按 interval 周期采集并回调 emit，直到 ctx 结束。
func (c *Collector) Run(ctx context.Context, interval time.Duration, emit func(*agentpb.MetricBatch)) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// 立即采一次，让控制面在节点刚上线时就能看到数据。
	emit(c.Collect(ctx))
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b := c.Collect(ctx)
			// 每个周期用 recover 包住，单次异常不能把常驻循环带崩。
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.log.Error("遥测周期 panic", slog.Any("panic", r))
					}
				}()
				emit(b)
			}()
		}
	}
}

// Cumulative 返回自启动以来的累计流量（status 命令展示用）。
func (c *Collector) Cumulative() (tx, rx uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cumTx, c.cumRx
}

func (c *Collector) configRevision() string {
	if c.stateFn == nil {
		return ""
	}
	return c.stateFn().ConfigRevision
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func versionOf() string { return version.Version }
