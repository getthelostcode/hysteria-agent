// Package sysinfo 用纯 /proc + syscall 读取系统信息，不引第三方采集库。
//
// 提供两类能力：
//  1. Meter —— 周期采样 CPU/内存/磁盘/网络（带上次采样的基线，算增量与百分比）；
//  2. 进程/端口查询 —— 端口占用与"占用者是哪个 PID"，供配置校验与诊断使用。
package sysinfo

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
)

// ---------------------------- 静态信息 ----------------------------

func Arch() string { return runtime.GOARCH }
func GOOS() string { return runtime.GOOS }
func Hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// Kernel 返回内核版本（报告里用于区分环境）。
func Kernel() string {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(b))
}

// BootID 每次系统启动都会变，用来识别"机器重启过"。
func BootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// MachineID 是稳定的主机标识（用于控制面识别同一台机器重复 enroll）。
func MachineID() string {
	for _, p := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if b, err := os.ReadFile(p); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return s
			}
		}
	}
	return ""
}

// BinaryVersion 执行 `<bin> version` 拿版本号（Hysteria 2 的 version 子命令）。
func BinaryVersion(ctx context.Context, bin string, timeout time.Duration) string {
	if bin == "" {
		return ""
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, "version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(out))
	for i, f := range fields {
		// 常见输出： "Version:\n  v2.6.5" 或 "v2.6.5"
		if strings.HasPrefix(f, "v2.") || strings.HasPrefix(f, "v1.") {
			return strings.Trim(f, "\"")
		}
		if f == "Version:" && i+1 < len(fields) {
			return strings.Trim(fields[i+1], "\"")
		}
	}
	if len(fields) > 0 {
		return strings.Trim(fields[0], "\"")
	}
	return ""
}

// ---------------------------- 采样器 ----------------------------

type cpuTimes struct {
	total uint64
	idle  uint64
}

type Meter struct {
	mu       sync.Mutex
	prevCPU  cpuTimes
	prevNet  map[string][2]uint64 // iface -> {rx, tx}
	prevTime time.Time
}

// NewMeter 构建采样器。
func NewMeter() *Meter {
	m := &Meter{}
	m.prevCPU, _ = readCPU()
	m.prevNet, _ = readNet()
	m.prevTime = time.Now()
	return m
}

// Sample 采集一次指标。diskPaths 为要统计的文件系统路径；ifaces 为空表示全部非 lo 接口。
func (m *Meter) Sample(diskPaths, ifaces []string) (*agentpb.MetricSample, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	s := &agentpb.MetricSample{TsMs: now.UnixMilli()}

	// CPU：两次采样之间的 busy/total 比例；首次采样用瞬时值（自启动以来的平均）。
	cur, err := readCPU()
	if err == nil {
		dTotal := cur.total - m.prevCPU.total
		dIdle := cur.idle - m.prevCPU.idle
		if dTotal > 0 {
			s.CpuPercent = round2(float64(dTotal-dIdle) / float64(dTotal) * 100)
		} else if cur.total > 0 {
			s.CpuPercent = round2(float64(cur.total-cur.idle) / float64(cur.total) * 100)
		}
		m.prevCPU = cur
	}

	// 内存：用 MemAvailable 而不是 MemFree，后者把 cache 也算成"已用"。
	if total, avail, err := readMem(); err == nil {
		s.MemTotalBytes = total
		s.MemUsedBytes = total - avail
		if total > 0 {
			s.MemPercent = round2(float64(total-avail) / float64(total) * 100)
		}
	}

	// 磁盘：累加配置的路径（同一台机器通常只有一个根文件系统）。
	var dTotal, dUsed uint64
	for _, p := range diskPaths {
		var st syscall.Statfs_t
		if err := syscall.Statfs(p, &st); err != nil {
			continue
		}
		total := st.Blocks * uint64(st.Bsize)
		free := st.Bavail * uint64(st.Bsize)
		dTotal += total
		if total > free {
			dUsed += total - free
		}
	}
	s.DiskTotalBytes = dTotal
	s.DiskUsedBytes = dUsed
	if dTotal > 0 {
		s.DiskPercent = round2(float64(dUsed) / float64(dTotal) * 100)
	}

	// 网络：自上次采样以来的增量（控制面要算速率，所以发增量而不是累计值）。
	if net, err := readNet(); err == nil {
		var rxDelta, txDelta uint64
		selected := 0
		for name, v := range net {
			if !ifaceSelected(name, ifaces) {
				continue
			}
			selected++
			if prev, ok := m.prevNet[name]; ok {
				if v[0] >= prev[0] {
					rxDelta += v[0] - prev[0]
				}
				if v[1] >= prev[1] {
					txDelta += v[1] - prev[1]
				}
			}
		}
		s.NetRxBytes = rxDelta
		s.NetTxBytes = txDelta
		_ = selected
		m.prevNet = net
	}

	if l1, l5, l15, err := readLoad(); err == nil {
		s.Load1, s.Load5, s.Load15 = l1, l5, l15
	}
	s.UptimeSeconds = readUptime()
	s.ProcCount = uint32(countProcs())
	m.prevTime = now
	return s, nil
}

func ifaceSelected(name string, ifaces []string) bool {
	if len(ifaces) == 0 {
		return name != "lo"
	}
	for _, want := range ifaces {
		if want == name {
			return true
		}
	}
	return false
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

func readCPU() (cpuTimes, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return cpuTimes{}, fmt.Errorf("/proc/stat 为空")
	}
	fields := strings.Fields(sc.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuTimes{}, fmt.Errorf("/proc/stat 格式异常")
	}
	var total, idle uint64
	for i, v := range fields[1:] {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			continue
		}
		total += n
		// idle + iowait：iowait 算空闲更贴近用户感受
		if i == 3 || i == 4 {
			idle += n
		}
	}
	return cpuTimes{total: total, idle: idle}, nil
}

func readMem() (total, available uint64, err error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = v * 1024
		case "MemAvailable:":
			available = v * 1024
		}
	}
	if total == 0 {
		return 0, 0, fmt.Errorf("/proc/meminfo 缺少 MemTotal")
	}
	return total, available, nil
}

func readNet() (map[string][2]uint64, error) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string][2]uint64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		i := strings.Index(line, ":")
		if i < 0 {
			continue
		}
		name := strings.TrimSpace(line[:i])
		fields := strings.Fields(line[i+1:])
		if len(fields) < 9 {
			continue
		}
		rx, _ := strconv.ParseUint(fields[0], 10, 64)
		tx, _ := strconv.ParseUint(fields[8], 10, 64)
		out[name] = [2]uint64{rx, tx}
	}
	return out, sc.Err()
}

func readLoad() (float64, float64, float64, error) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0, err
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return 0, 0, 0, fmt.Errorf("/proc/loadavg 格式异常")
	}
	l1, _ := strconv.ParseFloat(fields[0], 64)
	l5, _ := strconv.ParseFloat(fields[1], 64)
	l15, _ := strconv.ParseFloat(fields[2], 64)
	return l1, l5, l15, nil
}

func readUptime() uint64 {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0
	}
	f, _ := strconv.ParseFloat(fields[0], 64)
	return uint64(f)
}

func countProcs() int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && isNumeric(e.Name()) {
			n++
		}
	}
	return n
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ---------------------------- 进程 / 端口 ----------------------------

// ProcessAlive 判断 pid 是否活着，且（若给了 wantExe）可执行文件是否匹配，
// 避免 pid 复用导致误判。
func ProcessAlive(pid int, wantExe string) bool {
	if pid <= 0 {
		return false
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
		return false
	}
	if wantExe == "" {
		return true
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		// 权限不足时退化为 cmdline 匹配
		if cl, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
			return strings.Contains(string(cl), filepath.Base(wantExe))
		}
		return true
	}
	return filepath.Base(exe) == filepath.Base(wantExe)
}

// ProcessStartTime 返回 pid 的启动时刻。
//
// 用 /proc/stat 的 btime（开机时间）+ /proc/<pid>/stat 的 starttime（自开机的时钟滴答数）
// 计算，而不是用 "now - /proc/uptime"：在容器里 uptime 与滴答数可能来自不同时钟域，
// 算出来的启动时间会跑到未来，导致"运行时长"溢出成天文数字（真实踩过）。
func ProcessStartTime(pid int) (time.Time, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return time.Time{}, err
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return time.Time{}, fmt.Errorf("/proc/%d/stat 格式异常", pid)
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 20 {
		return time.Time{}, fmt.Errorf("/proc/%d/stat 字段不足", pid)
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64) // 第 22 项（starttime）
	if err != nil {
		return time.Time{}, err
	}
	const hz = 100 // Linux USER_HZ，x86/arm 上恒为 100
	started := bootTime().Add(time.Duration(startTicks/hz) * time.Second)
	now := time.Now()
	if started.After(now) {
		// 时钟域不一致时不要输出未来时间，退化为"刚刚启动"。
		started = now
	}
	return started, nil
}

// bootTime 读 /proc/stat 的 btime（epoch 秒）；读不到时退化为 now - uptime。
func bootTime() time.Time {
	if b, err := os.ReadFile("/proc/stat"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "btime ") {
				if secs, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "btime ")), 10, 64); err == nil && secs > 0 {
					return time.Unix(secs, 0)
				}
			}
		}
	}
	return time.Now().Add(-time.Duration(readUptime()) * time.Second)
}

// PortUsage 描述一个端口占用情况。
type PortUsage struct {
	Proto string
	Port  int
	InUse bool
	PIDs  []int
}

// LookupPort 查询 TCP 或 UDP 端口占用（proto 取 "tcp" 或 "udp"）。
func LookupPort(port int, proto string) (PortUsage, error) {
	pu := PortUsage{Proto: proto, Port: port}
	inodes := map[string]bool{}
	for _, file := range protoFiles(proto) {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		first := true
		for sc.Scan() {
			line := sc.Text()
			if first {
				first = false
				continue // 表头
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				continue
			}
			_, p, err := parseHexAddr(fields[1])
			if err != nil || p != port {
				continue
			}
			inodes[fields[9]] = true
			pu.InUse = true
		}
		f.Close()
	}
	if len(inodes) > 0 {
		pu.PIDs = pidsForSocketInodes(inodes)
	}
	return pu, nil
}

func protoFiles(proto string) []string {
	switch proto {
	case "tcp":
		return []string{"/proc/net/tcp", "/proc/net/tcp6"}
	case "udp":
		return []string{"/proc/net/udp", "/proc/net/udp6"}
	}
	return nil
}

func parseHexAddr(s string) (string, int, error) {
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return "", 0, fmt.Errorf("地址格式异常: %s", s)
	}
	p, err := strconv.ParseInt(s[i+1:], 16, 32)
	if err != nil {
		return "", 0, err
	}
	return s[:i], int(p), nil
}

// pidsForSocketInodes 扫描 /proc/*/fd 找出持有这些 socket inode 的进程。
func pidsForSocketInodes(inodes map[string]bool) []int {
	var pids []int
	procs, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	for _, p := range procs {
		if !p.IsDir() || !isNumeric(p.Name()) {
			continue
		}
		pid, _ := strconv.Atoi(p.Name())
		fdDir := fmt.Sprintf("/proc/%s/fd", p.Name())
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue // 权限不足（别人的进程）就跳过
		}
		for _, fd := range fds {
			link, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err != nil {
				continue
			}
			if !strings.HasPrefix(link, "socket:[") {
				continue
			}
			ino := strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")
			if inodes[ino] {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// PortOwnedBy 判断端口是否由指定 pid 持有。pid<=0 时只要"没有被别人占用"就返回 true。
func PortOwnedBy(port int, proto string, pid int) (bool, PortUsage, error) {
	pu, err := LookupPort(port, proto)
	if err != nil {
		return false, pu, err
	}
	if !pu.InUse {
		return true, pu, nil
	}
	if pid <= 0 {
		return false, pu, nil
	}
	for _, p := range pu.PIDs {
		if p == pid {
			return true, pu, nil
		}
	}
	// 拿不到 fd（权限/内核限制）时无法归因，保守返回"不是我们的"。
	return false, pu, nil
}

// FindProcessByExecutable 按可执行文件名找进程（external 模式识别已有实例）。
func FindProcessByExecutable(name string) (int, error) {
	procs, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	for _, p := range procs {
		if !p.IsDir() || !isNumeric(p.Name()) {
			continue
		}
		exe, err := os.Readlink(filepath.Join("/proc", p.Name(), "exe"))
		if err != nil {
			continue
		}
		if filepath.Base(exe) == name {
			pid, _ := strconv.Atoi(p.Name())
			return pid, nil
		}
	}
	return 0, fmt.Errorf("未找到进程 %s", name)
}

// LocalIPs 返回本机所有非 lo 的 IPv4 地址（诊断报告用）。
func LocalIPs() []string {
	ifaces, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return nil
	}
	var out []string
	for _, iface := range ifaces {
		up, _ := os.ReadFile(filepath.Join("/sys/class/net", iface.Name(), "operstate"))
		if strings.TrimSpace(string(up)) == "down" {
			continue
		}
		cmd := exec.Command("ip", "-4", "-o", "addr", "show", "dev", iface.Name())
		b, err := cmd.Output()
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			for i, f := range fields {
				if f == "inet" && i+1 < len(fields) {
					out = append(out, strings.Split(fields[i+1], "/")[0])
				}
			}
		}
	}
	return out
}
