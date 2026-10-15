// Package diag 执行节点侧健康检查（run_diagnostic 指令的实现）。
//
// 检查项：端口 / 证书 / DNS / 出口 IP / 控制面连通性 / Hysteria 本地 API /
// 磁盘空间 / 配置与历史 / outbox 积压 / 时钟偏移 / systemd 单元。
package diag

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/hyconfig"
	"github.com/getthelostcode/hysteria-agent/internal/supervisor"
	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// Check 单项检查结果。
type Check struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"` // ok | warn | error | skipped
	Detail     string         `json:"detail"`
	Data       map[string]any `json:"data,omitempty"`
	DurationMs int64          `json:"duration_ms"`
}

// Report 是完整诊断报告。
type Report struct {
	NodeID          string    `json:"node_id"`
	Hostname        string    `json:"hostname"`
	AgentVersion    string    `json:"agent_version"`
	HysteriaVersion string    `json:"hysteria_version,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	DurationMs      int64     `json:"duration_ms"`
	Status          string    `json:"status"` // ok | warn | error
	Checks          []Check   `json:"checks"`
}

// Deps 是诊断需要的外部依赖（由 agent 装配注入，避免反向依赖）。
type Deps struct {
	Cfg              *config.Config
	NodeID           string
	API              *hyapi.Client
	Super            *supervisor.Supervisor
	HyVersion        string
	ControlConnected bool
	ControlSkewMs    int64
	ConfigRevision   string
	ConfigSHA256     string
	OutboxPending    int
	OutboxBytes      int64
	OutboxDropped    uint64
	Configured       bool
}

// Run 执行全部检查并返回报告。
func Run(ctx context.Context, d Deps) *Report {
	start := time.Now()
	rep := &Report{
		NodeID:          d.NodeID,
		Hostname:        sysinfo.Hostname(),
		AgentVersion:    version.Version,
		HysteriaVersion: d.HyVersion,
		StartedAt:       start.UTC(),
	}
	add := func(c Check) { rep.Checks = append(rep.Checks, c) }
	timeit := func(name string, fn func() Check) {
		t0 := time.Now()
		c := fn()
		c.Name = name
		c.DurationMs = time.Since(t0).Milliseconds()
		add(c)
	}

	if d.Super != nil {
		timeit("hysteria_process", func() Check { return checkProcess(d) })
	}
	timeit("config", func() Check { return checkConfig(d) })
	timeit("ports", func() Check { return checkPorts(d) })
	timeit("certificate", func() Check { return checkCertificate(d) })
	timeit("dns", func() Check { return checkDNS(ctx, d) })
	timeit("egress_ip", func() Check { return checkEgressIP(ctx, d) })
	timeit("control_plane", func() Check { return checkControl(ctx, d) })
	timeit("hysteria_api", func() Check { return checkHysteriaAPI(ctx, d) })
	timeit("disk", func() Check { return checkDisk(d) })
	timeit("outbox", func() Check { return checkOutbox(d) })
	timeit("clock", func() Check { return checkClock(d) })
	timeit("systemd", func() Check { return checkSystemd(ctx, d) })

	rep.FinishedAt = time.Now().UTC()
	rep.DurationMs = time.Since(start).Milliseconds()
	rep.Status = "ok"
	for _, c := range rep.Checks {
		if c.Status == "error" {
			rep.Status = "error"
			break
		}
		if c.Status == "warn" && rep.Status != "error" {
			rep.Status = "warn"
		}
	}
	return rep
}

func checkProcess(d Deps) Check {
	st := d.Super.State()
	data := map[string]any{
		"mode": st.Mode, "running": st.Running, "pid": st.PID,
		"restarts": st.Restarts, "last_exit_code": st.ExitCode,
		"uptime_seconds": st.UptimeSeconds, "backoff_seconds": st.BackoffSeconds,
	}
	switch {
	case st.GiveUp:
		return Check{Status: "error", Detail: "Hysteria 反复崩溃，Agent 已放弃自动重启: " + st.LastError, Data: data}
	case !st.Running:
		return Check{Status: "error", Detail: "Hysteria 未在运行" + strIf(st.LastError, ": "+st.LastError), Data: data}
	case st.Restarts > 3:
		return Check{Status: "warn", Detail: fmt.Sprintf("Hysteria 已重启 %d 次（最近退出码 %d），建议查看日志", st.Restarts, st.ExitCode), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("Hysteria 运行中 (pid=%d, mode=%s, 已运行 %ds)", st.PID, st.Mode, st.UptimeSeconds), Data: data}
}

func checkConfig(d Deps) Check {
	data := map[string]any{
		"path": d.Cfg.Hysteria.ConfigPath, "revision": d.ConfigRevision, "sha256": d.ConfigSHA256,
	}
	if d.ConfigSHA256 == "" {
		return Check{Status: "error", Detail: "目标配置不存在或不可读: " + d.Cfg.Hysteria.ConfigPath, Data: data}
	}
	payload, err := os.ReadFile(d.Cfg.Hysteria.ConfigPath)
	if err != nil {
		return Check{Status: "error", Detail: "读取配置失败: " + err.Error(), Data: data}
	}
	res := hyconfig.Validate(context.Background(), payload, "yaml", hyconfig.ValidateOptions{
		CheckPort: false, CheckCert: true, CertWarnDays: d.Cfg.Hysteria.CertWarnDays,
		RequireTrafficStats: false,
	})
	data["valid"] = res.Valid
	data["checks"] = res.Checks
	if !res.Valid {
		return Check{Status: "error", Detail: "本机配置未通过校验: " + strings.Join(res.Errors, "; "), Data: data}
	}
	if len(res.Warnings) > 0 {
		return Check{Status: "warn", Detail: "配置有告警: " + strings.Join(res.Warnings, "; "), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("配置合法 (rev=%s, sha256=%s)", d.ConfigRevision, short(d.ConfigSHA256)), Data: data}
}

func checkPorts(d Deps) Check {
	payload, err := os.ReadFile(d.Cfg.Hysteria.ConfigPath)
	if err != nil {
		return Check{Status: "skipped", Detail: "读不到配置，跳过端口检查: " + err.Error()}
	}
	res := hyconfig.Validate(context.Background(), payload, "yaml", hyconfig.ValidateOptions{CheckCert: false})
	hy := res.Parsed
	if hy == nil || hy.ListenPort == 0 {
		return Check{Status: "error", Detail: "解析不出监听端口"}
	}
	owner := 0
	if d.Super != nil {
		owner = d.Super.Pid()
	}
	data := map[string]any{"port": hy.ListenPort, "listen": hy.Listen, "owner_pid": owner}
	var details []string
	status := "ok"
	for _, proto := range []string{"udp", "tcp"} {
		pu, err := sysinfo.LookupPort(hy.ListenPort, proto)
		if err != nil {
			details = append(details, fmt.Sprintf("%s 检查失败: %v", proto, err))
			status = "warn"
			continue
		}
		switch {
		case !pu.InUse:
			details = append(details, fmt.Sprintf("%s/%d 空闲", proto, hy.ListenPort))
		case owner > 0 && containsInt(pu.PIDs, owner):
			details = append(details, fmt.Sprintf("%s/%d 由本机 Hysteria(pid=%d) 持有", proto, hy.ListenPort, owner))
		case len(pu.PIDs) == 0:
			details = append(details, fmt.Sprintf("%s/%d 被占用（无法归因持有者）", proto, hy.ListenPort))
			if status == "ok" {
				status = "warn"
			}
		default:
			details = append(details, fmt.Sprintf("%s/%d 被进程 %v 占用", proto, hy.ListenPort, pu.PIDs))
			// Hysteria 主要用 UDP：UDP 被抢才是致命错误。
			if proto == "udp" {
				status = "error"
			} else if status != "error" {
				status = "warn"
			}
		}
	}
	data["details"] = details
	return Check{Status: status, Detail: strings.Join(details, "; "), Data: data}
}

func checkCertificate(d Deps) Check {
	payload, err := os.ReadFile(d.Cfg.Hysteria.ConfigPath)
	if err != nil {
		return Check{Status: "skipped", Detail: "读不到配置"}
	}
	res := hyconfig.Validate(context.Background(), payload, "yaml", hyconfig.ValidateOptions{CheckCert: true, CertWarnDays: d.Cfg.Hysteria.CertWarnDays})
	hy := res.Parsed
	if hy == nil {
		return Check{Status: "error", Detail: "配置无法解析"}
	}
	if hy.TLSMode != "tls" {
		return Check{Status: "ok", Detail: "TLS 模式为 " + hy.TLSMode + "（由 acme 自动管理证书）",
			Data: map[string]any{"tls_mode": hy.TLSMode, "domains": hy.ACMEDomains}}
	}
	data := map[string]any{"cert": hy.CertFile, "expires_at": hy.CertExpiresAt, "expires_in_days": hy.CertExpiresInDays}
	cert, err := loadLeaf(hy.CertFile)
	if err != nil {
		return Check{Status: "error", Detail: "证书不可用: " + err.Error(), Data: data}
	}
	data["subject"] = cert.Subject.CommonName
	data["dns_names"] = cert.DNSNames
	data["issuer"] = cert.Issuer.CommonName
	switch {
	case time.Now().After(cert.NotAfter):
		return Check{Status: "error", Detail: "证书已过期: " + cert.NotAfter.Format(time.RFC3339), Data: data}
	case hy.CertExpiresInDays <= d.Cfg.Hysteria.CertWarnDays:
		return Check{Status: "warn", Detail: fmt.Sprintf("证书 %d 天后过期（%s）", hy.CertExpiresInDays, cert.NotAfter.Format("2006-01-02")), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("证书有效，%d 天后过期（%s）", hy.CertExpiresInDays, cert.NotAfter.Format("2006-01-02")), Data: data}
}

func checkDNS(ctx context.Context, d Deps) Check {
	hosts := append([]string{}, d.Cfg.Diag.DNSHosts...)
	// 控制面地址要剥掉端口再解析：LookupHost("host:443") 会直接报 no such host。
	ctrlHost := controlHostname(d.Cfg)
	if ctrlHost != "" {
		hosts = append([]string{ctrlHost}, hosts...)
	}
	if len(hosts) == 0 {
		return Check{Status: "skipped", Detail: "没有配置 dns_hosts"}
	}
	r := &net.Resolver{}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data := map[string]any{}
	status := "ok"
	var details []string
	for _, h := range hosts {
		ips, err := r.LookupHost(cctx, h)
		if err != nil {
			details = append(details, fmt.Sprintf("%s: 解析失败(%v)", h, err))
			data[h] = "error: " + err.Error()
			if h == ctrlHost {
				status = "error"
			} else if status == "ok" {
				status = "warn"
			}
			continue
		}
		sort.Strings(ips)
		data[h] = ips
		details = append(details, fmt.Sprintf("%s -> %s", h, strings.Join(ips, ",")))
	}
	return Check{Status: status, Detail: strings.Join(details, "; "), Data: data}
}

func checkEgressIP(ctx context.Context, d Deps) Check {
	if len(d.Cfg.Diag.PublicIPURLs) == 0 {
		return Check{Status: "skipped", Detail: "未配置 public_ip_urls"}
	}
	client := &http.Client{Timeout: d.Cfg.Diag.HTTPTimeout()}
	data := map[string]any{"local_ips": sysinfo.LocalIPs()}
	var details []string
	status := "ok"
	for _, u := range d.Cfg.Diag.PublicIPURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			details = append(details, fmt.Sprintf("%s: %v", u, err))
			status = "warn"
			continue
		}
		buf := make([]byte, 128)
		n, _ := resp.Body.Read(buf)
		resp.Body.Close()
		ip := strings.TrimSpace(string(buf[:n]))
		data["public_ip"] = ip
		details = append(details, "出口 IP: "+ip)
	}
	return Check{Status: status, Detail: strings.Join(details, "; "), Data: data}
}

func checkControl(ctx context.Context, d Deps) Check {
	data := map[string]any{
		"connected": d.ControlConnected, "skew_ms": d.ControlSkewMs,
		"server_url": d.Cfg.Control.ServerURL, "ws_path": d.Cfg.Control.WSPath,
	}
	if !d.Cfg.Diag.CheckControlPlane {
		return Check{Status: "skipped", Detail: "已关闭控制面探测", Data: data}
	}
	host := controlHost(d.Cfg)
	if host == "" {
		return Check{Status: "error", Detail: "control.server_url 无主机名", Data: data}
	}
	// 只做 TLS 探测（含 pin 校验），不调业务接口，避免产生额外副作用。
	dialer := &net.Dialer{Timeout: d.Cfg.Control.ConnectTimeout()}
	cctx, cancel := context.WithTimeout(ctx, d.Cfg.Control.ConnectTimeout())
	defer cancel()
	t0 := time.Now()
	conn, err := dialer.DialContext(cctx, "tcp", host)
	if err != nil {
		return Check{Status: "error", Detail: "TCP 连接控制面失败: " + err.Error(), Data: data}
	}
	defer conn.Close()
	data["tcp_connect_ms"] = time.Since(t0).Milliseconds()
	if host, _, err := net.SplitHostPort(host); err == nil {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
		tlsConn.SetDeadline(time.Now().Add(d.Cfg.Control.ConnectTimeout()))
		if err := tlsConn.HandshakeContext(cctx); err != nil {
			return Check{Status: "warn", Detail: "TCP 可达但 TLS 握手失败: " + err.Error() + "（若控制面在隧道后面可能属正常）", Data: data}
		}
		state := tlsConn.ConnectionState()
		data["tls_version"] = tlsVersionName(state.Version)
		data["cert_expires"] = state.PeerCertificates[0].NotAfter.UTC().Format(time.RFC3339)
		data["spki_pin"] = spkiPin(state.PeerCertificates[0])
	}
	if d.ControlConnected {
		return Check{Status: "ok", Detail: "控制通道已连接", Data: data}
	}
	return Check{Status: "warn", Detail: "控制通道当前未连接（Agent 正在按退避策略重连）", Data: data}
}

func checkHysteriaAPI(ctx context.Context, d Deps) Check {
	if d.API == nil {
		return Check{Status: "skipped", Detail: "未配置 hysteria.api_base_url"}
	}
	data := map[string]any{"base_url": d.API.BaseURL()}
	t0 := time.Now()
	traffic, err := d.API.Traffic(ctx, false)
	if err != nil {
		data["error"] = err.Error()
		return Check{Status: "error", Detail: "读取 /traffic 失败: " + err.Error(), Data: data}
	}
	data["traffic_latency_ms"] = time.Since(t0).Milliseconds()
	data["users_with_traffic"] = len(traffic)
	online, err := d.API.Online(ctx)
	if err != nil {
		data["online_error"] = err.Error()
		return Check{Status: "warn", Detail: "读取 /online 失败: " + err.Error(), Data: data}
	}
	devices := 0
	for _, n := range online {
		devices += n
	}
	data["online_users"] = len(online)
	data["online_devices"] = devices
	return Check{Status: "ok", Detail: fmt.Sprintf("Hysteria API 可用（%d 用户有流量，%d 台设备在线）", len(traffic), devices), Data: data}
}

func checkDisk(d Deps) Check {
	var st systemStatfs
	path := d.Cfg.StateDir
	if err := statfs(path, &st); err != nil {
		path = "/"
		if err := statfs(path, &st); err != nil {
			return Check{Status: "warn", Detail: "无法读取磁盘信息: " + err.Error()}
		}
	}
	total := st.Blocks * uint64(st.Bsize)
	free := st.Bavail * uint64(st.Bsize)
	usedPct := 0.0
	if total > 0 {
		usedPct = float64(total-free) / float64(total) * 100
	}
	data := map[string]any{"path": path, "total_bytes": total, "free_bytes": free, "used_percent": round2(usedPct)}
	switch {
	case free < 200<<20:
		return Check{Status: "error", Detail: fmt.Sprintf("%s 剩余空间仅 %.1f MB", path, float64(free)/1024/1024), Data: data}
	case usedPct > 90:
		return Check{Status: "warn", Detail: fmt.Sprintf("%s 使用率 %.1f%%", path, usedPct), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("%s 剩余 %.1f GB (%.1f%%)", path, float64(free)/1024/1024/1024, usedPct), Data: data}
}

func checkOutbox(d Deps) Check {
	data := map[string]any{"pending": d.OutboxPending, "bytes": d.OutboxBytes, "dropped_total": d.OutboxDropped,
		"path": d.Cfg.Outbox.Path}
	if d.OutboxPending > 0 && !d.ControlConnected {
		return Check{Status: "warn", Detail: fmt.Sprintf("控制面断线中，本地积压 %d 条事件（%d 字节），重连后会自动补发", d.OutboxPending, d.OutboxBytes), Data: data}
	}
	if d.OutboxDropped > 0 {
		return Check{Status: "warn", Detail: fmt.Sprintf("历史上因缓冲上限丢弃了 %d 条事件（可调大 outbox.max_bytes / max_events）", d.OutboxDropped), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("事件缓冲正常（积压 %d 条）", d.OutboxPending), Data: data}
}

func checkClock(d Deps) Check {
	data := map[string]any{"skew_ms": d.ControlSkewMs, "local_time": time.Now().UTC().Format(time.RFC3339)}
	skew := time.Duration(d.ControlSkewMs) * time.Millisecond
	if skew < 0 {
		skew = -skew
	}
	switch {
	case d.ControlSkewMs == 0:
		return Check{Status: "skipped", Detail: "尚未与控制面对过时", Data: data}
	case skew > 5*time.Minute:
		return Check{Status: "error", Detail: fmt.Sprintf("与本机控制面时间偏差 %s，JWT/cert 时间校验会失败，请同步 NTP", skew), Data: data}
	case skew > 60*time.Second:
		return Check{Status: "warn", Detail: fmt.Sprintf("时间偏差 %s（建议开 NTP）", skew), Data: data}
	}
	return Check{Status: "ok", Detail: fmt.Sprintf("时钟偏差 %s（正常）", skew), Data: data}
}

func checkSystemd(ctx context.Context, d Deps) Check {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return Check{Status: "skipped", Detail: "本机没有 systemctl"}
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data := map[string]any{}
	status := "ok"
	var details []string
	units := []string{d.Cfg.Upgrade.ServiceName}
	if d.Cfg.Hysteria.Mode == "systemd" {
		units = append(units, d.Cfg.Hysteria.ServiceName)
	}
	for _, u := range units {
		out, err := exec.CommandContext(cctx, "systemctl", "is-active", u).Output()
		state := strings.TrimSpace(string(out))
		if err != nil && state == "" {
			state = "unknown"
		}
		data[u] = state
		details = append(details, u+": "+state)
		if state != "active" {
			if u == d.Cfg.Upgrade.ServiceName {
				status = "warn" // Agent 自己不在 systemd 里跑也常见（容器/前台调试）
			} else if status == "ok" {
				status = "warn"
			}
		}
	}
	return Check{Status: status, Detail: strings.Join(details, "; "), Data: data}
}

// ---------------------------- 工具 ----------------------------

func loadLeaf(path string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pemDecode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s 不是 PEM 证书", path)
	}
	return x509.ParseCertificate(block)
}

func controlHost(cfg *config.Config) string {
	s := cfg.Control.ServerURL
	s = strings.TrimPrefix(strings.TrimPrefix(s, "https://"), "http://")
	s = strings.TrimPrefix(s, "wss://")
	s = strings.TrimSuffix(s, "/")
	if !strings.Contains(s, ":") {
		s += ":443"
	}
	return s
}

// controlHostname 返回控制面的主机名（不含端口），供 DNS 解析使用。
func controlHostname(cfg *config.Config) string {
	h := controlHost(cfg)
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}

func strIf(s, suffix string) string {
	if s == "" {
		return ""
	}
	return suffix
}

func short(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

// tlsVersionName 把 TLS 版本号转成可读字符串。
func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS1.3"
	case tls.VersionTLS12:
		return "TLS1.2"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS10:
		return "TLS1.0"
	default:
		return fmt.Sprintf("unknown(%d)", v)
	}
}

// spkiPin 复用 creds 包的 SPKI pin 计算（与控制通道 pinning 同一算法）。
func spkiPin(c *x509.Certificate) string { return creds.SPKIPin(c) }

// systemStatfs 是 syscall.Statfs_t 的别名，避免 diag 里出现平台长类型名。
type systemStatfs = syscall.Statfs_t

func statfs(path string, st *systemStatfs) error { return syscall.Statfs(path, st) }

// pemDecode 返回 PEM 块内容（无块时返回 nil）。
func pemDecode(raw []byte) ([]byte, []byte) {
	blk, rest := pem.Decode(raw)
	if blk == nil {
		return nil, rest
	}
	return blk.Bytes, rest
}

func containsInt(l []int, v int) bool {
	for _, i := range l {
		if i == v {
			return true
		}
	}
	return false
}

// Marshal 是给命令结果用的便捷方法。
func (r *Report) Marshal() []byte {
	b, _ := json.Marshal(r)
	return b
}

// WriteFile 把报告落到 state 目录（人工排障时可直接看）。
func (r *Report) WriteFile(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "last-diagnostic.json"), r.Marshal(), 0o640)
}
