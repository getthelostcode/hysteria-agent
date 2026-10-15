// hysteria-agent 是 Hysteria2 节点的管理守护进程。
//
// 子命令：
//
//	enroll   首次注册，用 enroll_token 换取 node_id/node_secret
//	run      以守护进程方式运行（由 systemd 拉起）
//	status   查看运行状态（读 state.json + 本地指标/健康端点）
//	restart  重启被守护的 Hysteria 服务/进程
//	upgrade  手动升级自身二进制（同样要求 sha256 + ed25519 验签）
//	diag     在本地跑一遍健康检查并打印报告
//	config   配置子命令：validate / history / rollback
//	version  输出版本
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/getthelostcode/hysteria-agent/internal/agent"
	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
	"github.com/getthelostcode/hysteria-agent/internal/diag"
	"github.com/getthelostcode/hysteria-agent/internal/enroll"
	"github.com/getthelostcode/hysteria-agent/internal/hyapi"
	"github.com/getthelostcode/hysteria-agent/internal/hyconfig"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/supervisor"
	"github.com/getthelostcode/hysteria-agent/internal/upgrade"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

func main() {
	// 让 config 包能判断"编译期是否内置了升级公钥"。
	config.SetUpgradePublicKeyProbe(version.HasBuiltinUpgradeKey)

	// 允许全局 -config 写在子命令之前（`hysteria-agent -config x run` 与
	// `hysteria-agent run -config x` 都能用），避免用户抄 systemd 单元时踩坑。
	args := os.Args[1:]
	var pre []string
	for len(args) > 0 && strings.HasPrefix(args[0], "-") && args[0] != "-" {
		if args[0] == "-version" || args[0] == "--version" || args[0] == "-h" || args[0] == "--help" {
			break
		}
		if strings.HasPrefix(args[0], "-config") || args[0] == "-c" {
			pre = append(pre, args[0])
			args = args[1:]
			if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
				pre = append(pre, args[0])
				args = args[1:]
			}
			continue
		}
		break
	}
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	sub := args[0]
	rest := append(append([]string{}, pre...), args[1:]...)

	switch sub {
	case "enroll":
		os.Exit(cmdEnroll(rest))
	case "run":
		os.Exit(cmdRun(rest))
	case "status":
		os.Exit(cmdStatus(rest))
	case "restart":
		os.Exit(cmdRestart(rest))
	case "upgrade":
		os.Exit(cmdUpgrade(rest))
	case "diag", "diagnose":
		os.Exit(cmdDiag(rest))
	case "config":
		os.Exit(cmdConfig(rest))
	case "version", "-version", "--version", "-v":
		fmt.Printf("hysteria-agent %s\n", version.String())
		os.Exit(0)
	case "help", "-h", "--help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "未知子命令 %q\n\n", sub)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `hysteria-agent %s — Hysteria2 节点管理守护进程

用法:
  hysteria-agent enroll --token=<enroll_token> [--server=https://cp.example.com] [--name=<节点名>]
  hysteria-agent run [-config %s]
  hysteria-agent status [-config ...] [--json]
  hysteria-agent restart [-config ...]
  hysteria-agent upgrade --url=<url> --sha256=<hex> --signature=<base64> [--version=v] [--force]
  hysteria-agent diag [--json] [-config ...]
  hysteria-agent config validate -f <file> | config history | config rollback --rev=<revision>
  hysteria-agent version

配置与凭据默认路径:
  %s
  %s (0400)
`, version.String(), config.DefaultConfigPath, config.DefaultConfigPath, config.DefaultCredsPath)
}

// ---------------------------- run ----------------------------

func cmdRun(argv []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	_ = fs.Parse(argv)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	cr, err := creds.Load(cfg.CredentialsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载凭据失败: %v\n先执行: hysteria-agent enroll --token=<token>\n", err)
		return 1
	}
	// 日志 Hub 必须与 agent 共享同一个实例：slog 输出、远程采集、tail_logs 都走它。
	log, hub := logging.New(logging.Options{
		Level: cfg.Logs.Level, Format: cfg.Logs.Format, NodeID: cr.NodeID, RingSize: cfg.Logs.RingSize,
	})
	m := metrics.New(cr.NodeID)

	ag, err := agent.New(cfg, cr, log, hub, m)
	if err != nil {
		log.Error("初始化 Agent 失败", slog.String("error", err.Error()))
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 信号处理：只注册一个带缓冲的通道。
	// 注意：不能用 signal.NotifyContext + 另一个 signal.Notify 的 goroutine 组合——
	// 同一个 SIGTERM 会被两处同时消费，第二个处理器立刻触发"立即退出"，
	// 于是优雅停止（停 Hysteria）根本没执行（E2E 里真实踩到：子进程变成孤儿占着 443）。
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigCh
		log.Info("收到信号，开始优雅停止", slog.String("signal", s.String()))
		cancel()
		s = <-sigCh
		log.Warn("再次收到信号，立即退出（不再等待优雅停止）", slog.String("signal", s.String()))
		os.Exit(1)
	}()

	if err := ag.Run(ctx); err != nil {
		log.Error("运行失败", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

// ---------------------------- enroll ----------------------------

func cmdEnroll(argv []string) int {
	fs := flag.NewFlagSet("enroll", flag.ExitOnError)
	token := fs.String("token", "", "控制面签发的注册令牌（必填）")
	server := fs.String("server", "", "控制面地址，例如 https://cp.example.com（会写入配置文件）")
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	name := fs.String("name", "", "节点名（默认取主机名）")
	replace := fs.Bool("replace", false, "允许覆盖控制面上已存在的节点")
	pin := fs.String("pin", "", "证书 SPKI pin（base64），多值用逗号分隔")
	insecure := fs.Bool("insecure-skip-verify", false, "跳过证书链校验（必须同时提供 --pin）")
	checkOnly := fs.Bool("check", false, "只做预检（TLS/配置），不真正注册")
	_ = fs.Parse(argv)

	if *token == "" && !*checkOnly {
		fmt.Fprintln(os.Stderr, "必须提供 --token（用法: hysteria-agent enroll --token=<token>）")
		return 2
	}

	cfg, err := loadOrInitConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	if *server != "" {
		cfg.Control.ServerURL = *server
	}
	if *pin != "" {
		cfg.Control.PinSHA256 = splitCSV(*pin)
	}
	if *insecure {
		cfg.Control.InsecureSkipVerify = true
	}
	if cfg.Control.ServerURL == "" {
		fmt.Fprintln(os.Stderr, "缺少控制面地址：用 --server=https://... 或在配置文件里设置 control.server_url")
		return 2
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "配置校验失败: %v\n", err)
		return 1
	}
	if *server != "" || *pin != "" || *insecure {
		if err := writeConfigBack(cfg, *cfgPath); err != nil {
			fmt.Fprintf(os.Stderr, "写回配置失败: %v\n", err)
			return 1
		}
	}
	if err := cfg.EnsureDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "创建目录失败: %v\n", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	logf := func(format string, a ...any) { fmt.Printf(format+"\n", a...) }

	// 预检：TLS/pin 配错的问题在这里暴露，而不是等注册失败。
	logf("控制面地址: %s", cfg.Control.ServerURL)
	logf("注册端点  : %s", cfg.ControlHTTPURL(cfg.Control.EnrollPath))
	if err := enroll.PingTLS(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] TLS 预检失败（可能是控制面在隧道后面或证书 pin 不匹配）: %v\n", err)
	} else {
		logf("TLS 预检通过（含证书链与 pin 校验）")
	}
	if *checkOnly {
		// 再发一次真实请求（占位 token）：判定"路径到底存不存在、是不是 JSON API"，
		// 这类错配（尤其是框架默认的 HTML 404）光看 TLS 预检是看不出来的。
		probe := enroll.ProbeHTTP(ctx, cfg)
		logf("")
		logf("HTTP 探测结果：")
		logf("  URL          : %s", probe.URL)
		if probe.TLSErr != nil {
			logf("  TLS          : 失败 (%v)", probe.TLSErr)
		} else if probe.TLSOK {
			logf("  TLS          : 通过")
		} else {
			logf("  TLS          : 失败 (%v)", probe.DialErr)
		}
		if probe.DialErr != nil && probe.StatusCode == 0 {
			logf("  传输         : 失败 (%v)", probe.DialErr)
		} else {
			logf("  HTTP 状态    : %d", probe.StatusCode)
			logf("  Content-Type : %s", probe.ContentType)
			if len(probe.Body) > 0 {
				logf("  响应片段     : %s", firstLine(probe.Body))
			}
		}
		logf("  结论         : %s", probe.Hint())
		logf("")
		logf("预检完成（--check 模式，未注册）")
		return 0
	}

	cr, err := enroll.Run(ctx, cfg, enroll.Options{
		Token: *token, NodeName: *name, Replace: *replace, HysteriaBin: cfg.Hysteria.Binary,
	}, logf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "注册失败: %v\n", err)
		return 1
	}
	logf("")
	logf("下一步：")
	logf("  1) systemctl enable --now hysteria-agent")
	logf("  2) hysteria-agent status")
	logf("  3) 在控制面确认节点 %s 已上线", cr.NodeID)
	return 0
}

// ---------------------------- status ----------------------------

func cmdStatus(argv []string) int {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	asJSON := fs.Bool("json", false, "输出 JSON")
	_ = fs.Parse(argv)

	cfg, err := loadOrInitConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	out := map[string]any{
		"agent_version": version.Version,
		"config_path":   cfg.Path,
	}

	// 1) 本机状态文件（由 run 周期写入）
	statePath := filepath.Join(cfg.StateDir, "state.json")
	if b, err := os.ReadFile(statePath); err == nil {
		var snap map[string]any
		if json.Unmarshal(b, &snap) == nil {
			out["state_file"] = statePath
			out["state"] = snap
			if pid, ok := snap["pid"].(float64); ok && !processAlive(int(pid)) {
				out["state_stale"] = true
			}
		}
	} else {
		out["state_error"] = err.Error()
	}

	// 2) 本地运维端点
	endpoint := func(path string) map[string]any {
		u := "http://" + cfg.Metrics.Listen + path
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(u)
		if err != nil {
			return map[string]any{"url": u, "error": err.Error()}
		}
		defer resp.Body.Close()
		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)
		return map[string]any{"url": u, "status_code": resp.StatusCode, "body": strings.TrimSpace(string(body[:n]))}
	}
	if cfg.Metrics.Enabled {
		out["healthz"] = endpoint(cfg.Metrics.HealthPath)
		out["readyz"] = endpoint(cfg.Metrics.ReadyPath)
	}

	// 3) systemd 单元状态
	if _, err := exec.LookPath("systemctl"); err == nil {
		cmd := exec.Command("systemctl", "is-active", cfg.Upgrade.ServiceName)
		if b, err := cmd.Output(); err == nil {
			out["systemd"] = strings.TrimSpace(string(b))
		} else {
			out["systemd"] = strings.TrimSpace(string(b)) + " " + err.Error()
		}
	}

	if *asJSON {
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return 0
	}
	return printStatusHuman(out)
}

func printStatusHuman(out map[string]any) int {
	snap, _ := out["state"].(map[string]any)
	if snap == nil {
		fmt.Printf("hysteria-agent %s\n", out["agent_version"])
		fmt.Println("状态: 未运行（读不到 state.json，可能 Agent 没有启动）")
		if e, ok := out["state_error"]; ok {
			fmt.Printf("  原因: %v\n", e)
		}
		return 3
	}
	fmt.Printf("hysteria-agent %s  node_id=%v  pid=%v\n", out["agent_version"], snap["node_id"], snap["pid"])
	if b, ok := snap["ready"].(bool); ok {
		fmt.Printf("就绪: %v\n", b)
	}
	if c, ok := snap["config"].(map[string]any); ok {
		fmt.Printf("配置: revision=%v sha256=%v\n", c["revision"], shortStr(fmt.Sprint(c["sha256"])))
		if e := fmt.Sprint(c["last_apply_error"]); e != "" && e != "<nil>" {
			fmt.Printf("  最近一次应用错误: %s\n", e)
		}
		if h, ok := c["history"].([]any); ok && len(h) > 0 {
			fmt.Printf("  历史版本(%d): %v\n", len(h), h)
		}
	}
	if c, ok := snap["control"].(map[string]any); ok {
		fmt.Printf("控制通道: connected=%v reconnects=%v pending=%v acked_seq=%v\n",
			c["connected"], c["reconnects"], c["pending_events"], c["last_acked_seq"])
	}
	if h, ok := snap["hysteria"].(map[string]any); ok {
		fmt.Printf("Hysteria: mode=%v running=%v pid=%v restarts=%v last_exit=%v version=%v\n",
			h["mode"], h["running"], h["pid"], h["restart_count"], h["last_exit_code"], h["version"])
	}
	if st := out["systemd"]; st != nil {
		fmt.Printf("systemd(%s): %v\n", "hysteria-agent", st)
	}
	for _, k := range []string{"healthz", "readyz"} {
		if m, ok := out[k].(map[string]any); ok {
			fmt.Printf("%s: %v %v\n", k, m["status_code"], m["body"])
		}
	}
	if out["state_stale"] == true {
		fmt.Println("注意: state.json 里的 pid 已不存在，Agent 可能已崩溃")
		return 3
	}
	return 0
}

// ---------------------------- restart ----------------------------

func cmdRestart(argv []string) int {
	fs := flag.NewFlagSet("restart", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	hysteriaOnly := fs.Bool("hysteria-only", false, "只重启 Hysteria，不重启 Agent")
	_ = fs.Parse(argv)

	cfg, err := loadOrInitConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		fmt.Fprintln(os.Stderr, "本机没有 systemctl；请直接给 Agent 进程发 SIGTERM 或在前台重启")
		return 1
	}
	unit := cfg.Upgrade.ServiceName
	if *hysteriaOnly {
		if cfg.Hysteria.Mode == "systemd" {
			unit = cfg.Hysteria.ServiceName
		} else {
			fmt.Fprintln(os.Stderr, "hysteria.mode 不是 systemd，请用 run_diagnostic / 控制面 restart_hysteria 指令")
			return 2
		}
	}
	cmd := exec.Command("systemctl", "restart", unit)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "systemctl restart %s 失败: %v (%s)\n", unit, err, strings.TrimSpace(string(out)))
		return 1
	}
	fmt.Printf("已重启 %s\n", unit)
	return 0
}

// ---------------------------- upgrade ----------------------------

func cmdUpgrade(argv []string) int {
	fs := flag.NewFlagSet("upgrade", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	url := fs.String("url", "", "新二进制下载地址")
	sha := fs.String("sha256", "", "新二进制 sha256（hex）")
	sig := fs.String("signature", "", "ed25519 签名（base64，对象为 sha256 的 hex 字符串）")
	ver := fs.String("version", "", "目标版本号（用于日志与防降级判断）")
	force := fs.Bool("force", false, "允许同版本覆盖")
	restartMode := fs.String("restart-mode", "", "systemd 或 exec（默认读配置）")
	_ = fs.Parse(argv)

	if *url == "" || *sha == "" || *sig == "" {
		fmt.Fprintln(os.Stderr, "必须提供 --url --sha256 --signature")
		return 2
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	cr, _ := creds.Load(cfg.CredentialsFile)
	client, err := buildHTTPClient(cfg, cr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "构建下载客户端失败: %v\n", err)
		return 1
	}
	log := simpleLogger()
	m := metrics.New("cli-upgrade")
	upg, err := upgrade.New(cfg, log, m, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化升级器失败: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	res, err := upg.Run(ctx, upgrade.Manifest{
		Version: *ver, URL: *url, SHA256: *sha, Signature: *sig,
		Force: *force, RestartMode: *restartMode,
	})
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if err != nil {
		fmt.Fprintf(os.Stderr, "升级失败: %v\n", err)
		return 1
	}
	return 0
}

// ---------------------------- diag ----------------------------

func cmdDiag(argv []string) int {
	fs := flag.NewFlagSet("diag", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultConfigPath, "配置文件路径")
	asJSON := fs.Bool("json", false, "输出 JSON")
	_ = fs.Parse(argv)

	cfg, err := loadOrInitConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	cr, _ := creds.Load(cfg.CredentialsFile)
	nodeID := "unknown"
	if cr != nil {
		nodeID = cr.NodeID
	}
	m := metrics.New(nodeID)
	log := simpleLogger()
	var api *hyapi.Client
	if cfg.Hysteria.APIBaseURL != "" {
		api = hyapi.New(cfg.Hysteria.APIBaseURL, cfg.Hysteria.APISecret, 5*time.Second)
	}
	sup := supervisor.New(cfg, log, m, nil, api)
	// 一次性 CLI 场景：先探测本机是否已有 Hysteria 在跑，否则诊断会把"正在运行"
	// 误报成 hysteria_process=error、端口也会误判为被别人占用。
	if sup.DetectExternal() {
		log.Info("检测到本机已有 Hysteria 在运行", slog.Int("pid", sup.Pid()), slog.String("mode", cfg.Hysteria.Mode))
	}
	store := hyconfig.NewStore(cfg.Hysteria.HistoryDir, cfg.Hysteria.ConfigPath, cfg.Hysteria.HistoryKeep)
	sha, _ := store.ConfigHash()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rep := diag.Run(ctx, diag.Deps{
		Cfg: cfg, NodeID: nodeID, API: api, Super: sup,
		HyVersion:        storeVersion(ctx, cfg.Hysteria.Binary),
		ControlConnected: false,
		ConfigRevision:   store.CurrentRevision(), ConfigSHA256: sha,
	})
	if *asJSON {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Printf("诊断报告 %s  状态=%s  耗时=%dms\n", rep.Hostname, strings.ToUpper(rep.Status), rep.DurationMs)
		for _, c := range rep.Checks {
			fmt.Printf("  [%-7s] %-16s %s\n", strings.ToUpper(c.Status), c.Name, c.Detail)
		}
	}
	if rep.Status == "error" {
		return 1
	}
	return 0
}

func storeVersion(ctx context.Context, bin string) string {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, bin, "version").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// ---------------------------- config ----------------------------

func cmdConfig(argv []string) int {
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "用法: hysteria-agent config <validate|history|rollback>")
		return 2
	}
	switch argv[0] {
	case "validate":
		fs := flag.NewFlagSet("config validate", flag.ExitOnError)
		file := fs.String("f", "", "待校验的 Hysteria 配置文件")
		cfgPath := fs.String("config", config.DefaultConfigPath, "agent 配置文件路径")
		_ = fs.Parse(argv[1:])
		if *file == "" {
			fmt.Fprintln(os.Stderr, "用法: hysteria-agent config validate -f <file>")
			return 2
		}
		cfg, err := loadOrInitConfig(*cfgPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
			return 1
		}
		payload, err := os.ReadFile(*file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取 %s 失败: %v\n", *file, err)
			return 1
		}
		res := hyconfig.Validate(context.Background(), payload, "yaml", hyconfig.ValidateOptions{
			CheckPort: true, CheckCert: true, CertWarnDays: cfg.Hysteria.CertWarnDays,
			RequireTrafficStats: false, ExpectedTrafficSecret: cfg.Hysteria.APISecret,
			HysteriaBinary: cfg.Hysteria.Binary,
		})
		b, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(b))
		if !res.Valid {
			return 1
		}
		return 0

	case "history":
		fs := flag.NewFlagSet("config history", flag.ExitOnError)
		cfgPath := fs.String("config", config.DefaultConfigPath, "agent 配置文件路径")
		_ = fs.Parse(argv[1:])
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
			return 1
		}
		store := hyconfig.NewStore(cfg.Hysteria.HistoryDir, cfg.Hysteria.ConfigPath, cfg.Hysteria.HistoryKeep)
		vs, err := store.List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取历史失败: %v\n", err)
			return 1
		}
		for i, v := range vs {
			fmt.Printf("%2d) revision=%-20s result=%-12s sha256=%s applied=%s reason=%s\n",
				i+1, v.Revision, v.Result, shortStr(v.SHA256), v.AppliedAt.Local().Format("2006-01-02 15:04:05"), v.Reason)
		}
		return 0

	case "rollback":
		fs := flag.NewFlagSet("config rollback", flag.ExitOnError)
		cfgPath := fs.String("config", config.DefaultConfigPath, "agent 配置文件路径")
		rev := fs.String("rev", "", "目标 revision（留空=上一个版本）")
		_ = fs.Parse(argv[1:])
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
			return 1
		}
		store := hyconfig.NewStore(cfg.Hysteria.HistoryDir, cfg.Hysteria.ConfigPath, cfg.Hysteria.HistoryKeep)
		var target *hyconfig.Version
		if *rev == "" {
			v, err := store.Previous()
			if err != nil {
				fmt.Fprintf(os.Stderr, "找不到可回滚版本: %v\n", err)
				return 1
			}
			target = v
		} else {
			v, err := store.Find(*rev)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return 1
			}
			target = v
		}
		payload, err := store.Read(target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取版本 %s 失败: %v\n", target.Revision, err)
			return 1
		}
		if _, err := store.Apply(payload); err != nil {
			fmt.Fprintf(os.Stderr, "写入配置失败: %v\n", err)
			return 1
		}
		fmt.Printf("已把 %s 回滚到 revision=%s (sha256=%s)\n", cfg.Hysteria.ConfigPath, target.Revision, shortStr(target.SHA256))
		fmt.Println("请重启 Hysteria 使其生效: systemctl restart " + cfg.Hysteria.ServiceName)
		return 0
	}
	fmt.Fprintf(os.Stderr, "未知的 config 子命令 %q\n", argv[0])
	return 2
}

// ---------------------------- 公共辅助 ----------------------------

func loadOrInitConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err == nil {
		return cfg, nil
	}
	if os.IsNotExist(err) {
		cfg = config.Default()
		cfg.Path = path
		return cfg, nil
	}
	return nil, err
}

func writeConfigBack(cfg *config.Config, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	type outControl struct {
		ServerURL          string   `yaml:"server_url"`
		WSPath             string   `yaml:"ws_path"`
		EnrollPath         string   `yaml:"enroll_path"`
		PinSHA256          []string `yaml:"pin_sha256,omitempty"`
		InsecureSkipVerify bool     `yaml:"insecure_skip_verify,omitempty"`
	}
	payload := map[string]any{
		"control": outControl{
			ServerURL: cfg.Control.ServerURL, WSPath: cfg.Control.WSPath,
			EnrollPath: cfg.Control.EnrollPath, PinSHA256: cfg.Control.PinSHA256,
			InsecureSkipVerify: cfg.Control.InsecureSkipVerify,
		},
		"credentials_file": cfg.CredentialsFile,
		"state_dir":        cfg.StateDir,
	}
	b, err := yaml.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		_ = os.Rename(path, path+".bak")
	}
	return os.WriteFile(path, b, 0o600)
}

func buildHTTPClient(cfg *config.Config, cr *creds.Credentials) (*http.Client, error) {
	if cr == nil {
		cr = &creds.Credentials{}
	}
	tlsCfg, err := cr.TLSConfig(&cfg.Control, nil)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: time.Duration(cfg.Upgrade.DownloadTimeoutSeconds * float64(time.Second)),
		Transport: &http.Transport{
			TLSClientConfig:     tlsCfg,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: cfg.Control.ConnectTimeout(),
		},
	}, nil
}

// firstLine 取响应体的第一行（探测输出用，避免把整页 HTML 打印出来）。
func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

func simpleLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func shortStr(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}
