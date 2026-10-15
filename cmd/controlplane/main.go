// Command controlplane 是 hysteria-agent 的控制面：节点注册、控制通道、配置下发、指令下发、事件归档。
//
// 用法：
//
//	controlplane init    [-config /etc/hysteria-controlplane/config.yaml]   生成配置（随机 admin_token 与主密钥，0600）
//	controlplane serve   [-config ...]                                      启动服务（Agent 的 enroll/控制通道在 /api/v1/agent/*）
//	controlplane token new --name ci [--ttl 24h] [--max-uses 1]             生成注册令牌
//	controlplane token list
//	controlplane nodes
//	controlplane node <node_id>
//	controlplane push-config <node_id> <file.yaml> [--reason 说明] [--dry-run] [--wait 25s]
//	controlplane command <node_id> <command_type> [--params '{"k":"v"}'] [--wait 30s]
//	controlplane events <node_id> [--limit 50]
//	controlplane delete-node <node_id>
//	controlplane version
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/cp"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
	"gopkg.in/yaml.v3"
)

const defaultConfigPath = "/etc/hysteria-controlplane/config.yaml"

func main() {
	// 支持 `controlplane -config x serve` 与 `controlplane serve -config x` 两种写法。
	args, cfgPath := splitGlobalConfig(os.Args[1:])
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "init":
		os.Exit(cmdInit(rest, cfgPath))
	case "serve", "run":
		os.Exit(cmdServe(rest, cfgPath))
	case "token":
		os.Exit(cmdToken(rest, cfgPath))
	case "nodes":
		os.Exit(cmdNodes(rest, cfgPath, false))
	case "node":
		os.Exit(cmdNodes(rest, cfgPath, true))
	case "push-config", "push":
		os.Exit(cmdPush(rest, cfgPath))
	case "command", "cmd":
		os.Exit(cmdCommand(rest, cfgPath))
	case "events":
		os.Exit(cmdEvents(rest, cfgPath))
	case "telemetry":
		os.Exit(cmdTelemetry(rest, cfgPath))
	case "delete-node":
		os.Exit(cmdDeleteNode(rest, cfgPath))
	case "version", "-v", "--version":
		fmt.Printf("controlplane %s (commit %s, built %s)\n", version.Version, version.Commit, version.BuildTime)
		os.Exit(0)
	case "help", "-h", "--help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`hysteria-agent 控制面

  controlplane init                         生成配置（随机 admin_token + 主密钥，权限 0600）
  controlplane serve                        启动服务
  controlplane token new --name ci          生成一次性注册令牌
  controlplane token list                   列出令牌（掩码）
  controlplane nodes                        节点列表
  controlplane node <node_id>               节点详情
  controlplane push-config <node_id> <file> 下发 Hysteria 配置（YAML/JSON，body 即正文）
  controlplane command <node_id> <type>     下发指令（白名单）
  controlplane events <node_id>             最近事件
  controlplane telemetry <node_id>          最近遥测
  controlplane delete-node <node_id>        吊销并删除节点
  controlplane version

全局参数：-config <路径>（默认 ` + defaultConfigPath + `）
客户端参数：--server <http://127.0.0.1:8443>、--token <admin_token>（默认读配置文件）

可用指令：` + strings.Join(wire.CommandWhitelist(), ", ") + `
`)
}

// splitGlobalConfig 取出全局 -config（可出现在任意位置）。
func splitGlobalConfig(argv []string) ([]string, string) {
	out := make([]string, 0, len(argv))
	path := defaultConfigPath
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		switch {
		case a == "-config" || a == "--config":
			if i+1 < len(argv) {
				path = argv[i+1]
				i++
			}
		case strings.HasPrefix(a, "-config="):
			path = strings.TrimPrefix(a, "-config=")
		case strings.HasPrefix(a, "--config="):
			path = strings.TrimPrefix(a, "--config=")
		default:
			out = append(out, a)
		}
	}
	return out, path
}

// ---------------------------- init ----------------------------

func cmdInit(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	path := fs.String("config", cfgPath, "配置输出路径")
	force := fs.Bool("force", false, "覆盖已存在的配置")
	_ = fs.Parse(argv)

	if _, err := os.Stat(*path); err == nil && !*force {
		fmt.Fprintf(os.Stderr, "配置已存在: %s（要覆盖加 --force）\n", *path)
		return 1
	}
	cfg := cp.DefaultConfig()
	cfg.AdminToken = "adm_" + cp.NewID(24)
	_, masterKey := cp.NewMasterKey()
	cfg.MasterKey = masterKey

	if err := os.MkdirAll(dirOf(*path), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "创建目录失败: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "创建 state_dir 失败: %v\n", err)
		return 1
	}
	b, _ := yaml.Marshal(cfg)
	if err := os.WriteFile(*path, b, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "写入配置失败: %v\n", err)
		return 1
	}
	_ = os.Chmod(*path, 0o600)
	fmt.Printf(`已生成配置: %s (0600)
  listen     : %s
  state_dir  : %s
  admin_token: %s

下一步：
  1) controlplane serve -config %s
  2) controlplane token new --name node-1
  3) 在节点上安装 Agent：bash install.sh <控制面地址> --token=<上面的令牌>
     （控制面在 Cloudflare Tunnel / nginx 后面时，<控制面地址> 填对外域名）
`, *path, cfg.Listen, cfg.StateDir, cfg.AdminToken, *path)
	return 0
}

func dirOf(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i <= 0 {
		return "."
	}
	return p[:i]
}

// ---------------------------- serve ----------------------------

func cmdServe(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	path := fs.String("config", cfgPath, "配置文件路径")
	listen := fs.String("listen", "", "覆盖监听地址")
	_ = fs.Parse(argv)

	cfg, err := LoadConfig(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	key, err := ResolveMasterKey(cfg, *path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "主密钥处理失败: %v\n", err)
		return 1
	}
	store, err := cp.OpenStore(cfg.StateDir, key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开状态存储失败: %v\n", err)
		return 1
	}
	srv := cp.New(cfg, store, logger)

	// 桥接模式启动自检：Laravel 不可达时仍然启动（注册会等到它恢复），但要显式告警。
	if cfg.UsesLaravelIdentity() {
		if err := cfg.ProbeLaravel(context.Background()); err != nil {
			logger.Warn("Laravel 控制面探活失败：服务继续启动，注册/上报会在恢复后自动可用", "err", err, "base_url", cfg.Laravel.BaseURL)
		} else {
			logger.Info("身份权威：laravel（Laravel 控制面可达）", "base_url", cfg.Laravel.BaseURL)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "服务退出: %v\n", err)
		return 1
	}
	logger.Info("已优雅停止")
	return 0
}

// LoadConfig 读取 YAML 配置并套用默认值。
func LoadConfig(path string) (*cp.Config, error) {
	cfg := cp.DefaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("解析 %s 失败: %w", path, err)
	}
	// 补齐新增配置块（老配置文件没有这些键也不会崩），并校验自洽性。
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ResolveMasterKey 取主密钥：配置里写了就用配置；否则读/写 state_dir/master.key（0600）。
func ResolveMasterKey(cfg *cp.Config, cfgPath string) ([]byte, error) {
	if strings.TrimSpace(cfg.MasterKey) != "" {
		k, err := hex.DecodeString(strings.TrimSpace(cfg.MasterKey))
		if err != nil || len(k) < 16 {
			return nil, fmt.Errorf("master_key 必须是 >=16 字节的 hex 字符串")
		}
		return k, nil
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(cfg.StateDir, "master.key")
	b, err := os.ReadFile(keyPath)
	if err == nil {
		k, derr := hex.DecodeString(strings.TrimSpace(string(b)))
		if derr == nil && len(k) >= 16 {
			return k, nil
		}
	}
	k, hexKey := cp.NewMasterKey()
	if werr := os.WriteFile(keyPath, []byte(hexKey+"\n"), 0o600); werr != nil {
		return nil, fmt.Errorf("写入 %s 失败: %w", keyPath, werr)
	}
	_ = os.Chmod(keyPath, 0o600)
	slog.Warn("已生成新的主密钥（node_secret 的派生种子）", "path", keyPath,
		"note", "备份它：丢失后所有节点必须重新注册；也可以把它写进配置的 master_key 字段")
	return k, nil
}

func lastElem(p string) string {
	p = strings.TrimRight(p, "/")
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return p
	}
	return p[i+1:]
}

var _ = lastElem // 保留：日志/文档里会用到

// ---------------------------- 客户端 ----------------------------

type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient(cfgPath, serverOverride, tokenOverride string) (*client, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("加载配置失败(%s): %w", cfgPath, err)
	}
	base := "http://" + cfg.Listen
	if strings.HasPrefix(cfg.Listen, "/") {
		return nil, fmt.Errorf("listen=%s 是 unix socket，请用 --server 指定地址", cfg.Listen)
	}
	if serverOverride != "" {
		base = strings.TrimRight(serverOverride, "/")
	}
	tok := cfg.AdminToken
	if tokenOverride != "" {
		tok = tokenOverride
	}
	return &client{base: base, token: tok, http: &http.Client{Timeout: 3 * time.Minute}}, nil
}

func (c *client) do(method, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("请求 %s 失败: %w（控制面是否在运行？）", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func (c *client) getJSON(path string, out any) error {
	b, code, err := c.do(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", code, strings.TrimSpace(string(b)))
	}
	return json.Unmarshal(b, out)
}

func clientFlags(fs *flag.FlagSet, cfgPath string) (*string, *string) {
	return fs.String("server", "", "控制面地址，例如 http://127.0.0.1:8443"),
		fs.String("token", "", "admin 令牌（默认取配置文件）")
}

func mustClient(cfgPath, server, token string) *client {
	c, err := newClient(cfgPath, server, token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	return c
}

// ---------------------------- token ----------------------------

func cmdToken(argv []string, cfgPath string) int {
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "用法: controlplane token new|list")
		return 2
	}
	sub, rest := argv[0], argv[1:]
	switch sub {
	case "new", "create":
		fs := flag.NewFlagSet("token new", flag.ExitOnError)
		name := fs.String("name", "", "令牌名（用于审计）")
		ttl := fs.Duration("ttl", 24*time.Hour, "有效期，0 表示不过期")
		maxUses := fs.Int("max-uses", 1, "最大使用次数（1=一次性）")
		server, token := clientFlags(fs, cfgPath)
		scfg := fs.String("config", cfgPath, "配置文件路径")
		_ = fs.Parse(rest)
		c := mustClient(*scfg, *server, *token)
		payload, _ := json.Marshal(map[string]any{"name": *name, "ttl_seconds": int(ttl.Seconds()), "max_uses": *maxUses})
		b, code, err := c.do(http.MethodPost, "/admin/tokens", payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
		if code/100 != 2 {
			fmt.Fprintf(os.Stderr, "生成失败 HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
			return 1
		}
		var out struct {
			Token     string `json:"token"`
			Name      string `json:"name"`
			ExpiresMs int64  `json:"expires_ms"`
			MaxUses   int    `json:"max_uses"`
		}
		_ = json.Unmarshal(b, &out)
		fmt.Printf("enroll_token: %s\n  name=%s max_uses=%d", out.Token, out.Name, out.MaxUses)
		if out.ExpiresMs > 0 {
			fmt.Printf(" 有效至 %s", cp.HumanTime(out.ExpiresMs))
		}
		fmt.Println()
		fmt.Println("节点上执行： bash install.sh <控制面地址> --token=" + out.Token)
		return 0
	case "list", "ls":
		fs := flag.NewFlagSet("token list", flag.ExitOnError)
		server, token := clientFlags(fs, cfgPath)
		scfg := fs.String("config", cfgPath, "配置文件路径")
		_ = fs.Parse(rest)
		c := mustClient(*scfg, *server, *token)
		var out struct {
			Count  int `json:"count"`
			Tokens []struct {
				Token, Name string
				Usable      bool  `json:"usable"`
				MaxUses     int   `json:"max_uses"`
				Uses        int   `json:"uses"`
				ExpiresMs   int64 `json:"expires_ms"`
			} `json:"tokens"`
		}
		if err := c.getJSON("/admin/tokens", &out); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
		fmt.Printf("%-24s %-16s %-8s %-10s %s\n", "TOKEN", "NAME", "USES", "USABLE", "EXPIRES")
		for _, t := range out.Tokens {
			exp := "-"
			if t.ExpiresMs > 0 {
				exp = cp.HumanTime(t.ExpiresMs)
			}
			fmt.Printf("%-24s %-16s %-8s %-10v %s\n", t.Token, t.Name, fmt.Sprintf("%d/%d", t.Uses, t.MaxUses), t.Usable, exp)
		}
		return 0
	default:
		fmt.Fprintf(os.Stderr, "未知: token %s\n", sub)
		return 2
	}
}

// ---------------------------- nodes ----------------------------

type nodeListResp struct {
	Count int              `json:"count"`
	Nodes []map[string]any `json:"nodes"`
}

func cmdNodes(argv []string, cfgPath string, detail bool) int {
	fs := flag.NewFlagSet("nodes", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "输出原始 JSON")
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	c := mustClient(*scfg, *server, *token)

	if detail {
		args := fs.Args()
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "用法: controlplane node <node_id>")
			return 2
		}
		b, code, err := c.do(http.MethodGet, "/admin/nodes/"+args[0], nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
		if code/100 != 2 {
			fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
			return 1
		}
		if *jsonOut {
			fmt.Println(string(b))
			return 0
		}
		var n map[string]any
		_ = json.Unmarshal(b, &n)
		printNodeDetail(n)
		return 0
	}

	var out nodeListResp
	if err := c.getJSON("/admin/nodes", &out); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if *jsonOut {
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return 0
	}
	fmt.Printf("共 %d 个节点\n\n", out.Count)
	fmt.Printf("%-20s %-10s %-14s %-10s %-22s %s\n", "NAME", "STATUS", "LAST SEEN", "HYSTERIA", "CONFIG", "NODE_ID")
	for _, n := range out.Nodes {
		status := "offline"
		if v, _ := n["revoked"].(bool); v {
			status = "revoked"
		} else if v, _ := n["connected"].(bool); v {
			status = "online"
		}
		hy := "-"
		if v, ok := n["hysteria_running"].(bool); ok {
			hy = map[bool]string{true: "running", false: "stopped"}[v]
		}
		cfgCol := str(n["config_state"])
		if r := str(n["config_revision"]); r != "" {
			cfgCol += " " + r
		}
		fmt.Printf("%-20s %-10s %-14s %-10s %-22s %s\n",
			str(n["name"]), status, str(n["last_seen_ago"]), hy, cfgCol, str(n["node_id"]))
	}
	return 0
}

func printNodeDetail(n map[string]any) {
	fmt.Printf("节点      : %s (%s)\n", str(n["name"]), str(n["node_id"]))
	fmt.Printf("主机      : %s  %s/%s  kernel=%s\n", str(n["hostname"]), str(n["os"]), str(n["arch"]), str(n["kernel"]))
	fmt.Printf("版本      : agent=%s  hysteria=%s\n", str(n["agent_version"]), str(n["hysteria_version"]))
	fmt.Printf("状态      : connected=%v  revoked=%v  连接次数=%s\n", n["connected"], n["revoked"], str(n["connects"]))
	fmt.Printf("最近心跳  : %s (%s)  uptime=%ss  ready=%v\n", str(n["last_seen_human"]), str(n["last_seen_ago"]), str(n["uptime_seconds"]), n["ready"])
	fmt.Printf("配置      : state=%s revision=%s sha256=%s\n", str(n["config_state"]), str(n["config_revision"]), firstN(str(n["config_sha256"]), 16))
	if e := str(n["config_error"]); e != "" {
		fmt.Printf("配置错误  : %s\n", e)
	}
	if hb, ok := n["last_heartbeat"].(map[string]any); ok {
		fmt.Printf("心跳详情  : pending_events=%s last_acked_seq=%s sent_events=%s hysteria=%v\n",
			str(hb["pending_events"]), str(hb["last_acked_seq"]), str(hb["sent_events"]), hb["hysteria_running"])
	}
	if m, ok := n["last_metrics"].(map[string]any); ok {
		fmt.Printf("遥测      : cpu=%.1f%% mem=%.0f%% disk=%.0f%% hysteria_online=%s\n",
			fnum(m["cpu_percent"]), fnum(m["mem_used_percent"]), fnum(m["disk_used_percent"]), str(m["hysteria_online"]))
	}
	fmt.Printf("事件序号  : acked_seq=%s received=%s duplicates=%s\n", str(n["acked_seq"]), str(n["received_msgs"]), str(n["duplicate_msgs"]))
	if caps, ok := n["capabilities"].([]any); ok && len(caps) > 0 {
		cs := make([]string, 0, len(caps))
		for _, c := range caps {
			cs = append(cs, str(c))
		}
		sort.Strings(cs)
		fmt.Printf("能力      : %s\n", strings.Join(cs, " "))
	}
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func str(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case string:
		if x == "" {
			return "-"
		}
		return x
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	case bool:
		return fmt.Sprintf("%v", x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func fnum(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

// ---------------------------- push / command / events ----------------------------

func cmdPush(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("push-config", flag.ExitOnError)
	reason := fs.String("reason", "", "变更原因（写进审计事件）")
	revision := fs.String("revision", "", "版本号（默认控制面生成 <日期>-<序号>）")
	format := fs.String("format", "", "yaml|json（默认按内容猜）")
	dryRun := fs.Bool("dry-run", false, "只校验不落盘")
	wait := fs.Duration("wait", 25*time.Second, "等待回执的时间")
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	args := fs.Args()
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: controlplane push-config <node_id> <配置文件>")
		return 2
	}
	payload, err := os.ReadFile(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取 %s 失败: %v\n", args[1], err)
		return 1
	}
	c := mustClient(*scfg, *server, *token)
	q := fmt.Sprintf("/admin/nodes/%s/config?reason=%s&wait=%s", args[0], urlEnc(*reason), wait.String())
	if *revision != "" {
		q += "&revision=" + urlEnc(*revision)
	}
	if *format != "" {
		q += "&format=" + *format
	}
	if *dryRun {
		q += "&dry_run=1"
	}
	b, code, err := c.do(http.MethodPost, q, payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if code/100 != 2 {
		fmt.Fprintf(os.Stderr, "下发失败 HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
		return 1
	}
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	fmt.Printf("revision=%s sha256=%s dispatched=%v waited=%v\n", str(out["revision"]), firstN(str(out["sha256"]), 16), out["dispatched"], out["waited"])
	if v, ok := out["ok"]; ok {
		okv, _ := v.(bool)
		if okv {
			fmt.Printf("回执: ok=true rolled_back=%v dry_run=%v duration_ms=%s\n", out["rolled_back"], out["dry_run"], str(out["duration_ms"]))
		} else {
			fmt.Printf("回执: ok=false error=%s rolled_back=%v\n", str(out["error"]), out["rolled_back"])
		}
		if cs, ok := out["checks"].([]any); ok && len(cs) > 0 {
			items := make([]string, 0, len(cs))
			for _, x := range cs {
				items = append(items, str(x))
			}
			fmt.Printf("校验: %s\n", strings.Join(items, " | "))
		}
		return map[bool]int{true: 0, false: 1}[okv]
	}
	if n := str(out["note"]); n != "-" {
		fmt.Printf("提示: %s\n", n)
	}
	return 0
}

func cmdCommand(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("command", flag.ExitOnError)
	params := fs.String("params", "{}", "参数 JSON，或 @文件")
	wait := fs.Duration("wait", 30*time.Second, "等待结果的时间")
	id := fs.String("id", "", "指令幂等 ID（默认控制面生成）")
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	args := fs.Args()
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "用法: controlplane command <node_id> <type>\n可用: %s\n", strings.Join(wire.CommandWhitelist(), ", "))
		return 2
	}
	body := []byte(*params)
	if strings.HasPrefix(*params, "@") {
		b, err := os.ReadFile(strings.TrimPrefix(*params, "@"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取参数文件失败: %v\n", err)
			return 1
		}
		body = b
	}
	if !json.Valid(body) {
		fmt.Fprintln(os.Stderr, "--params 必须是合法 JSON")
		return 2
	}
	c := mustClient(*scfg, *server, *token)
	q := fmt.Sprintf("/admin/nodes/%s/commands?type=%s&wait=%s", args[0], urlEnc(args[1]), wait.String())
	if *id != "" {
		q += "&id=" + urlEnc(*id)
	}
	b, code, err := c.do(http.MethodPost, q, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if code/100 != 2 {
		fmt.Fprintf(os.Stderr, "下发失败 HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
		return 1
	}
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	fmt.Printf("command_id=%s dispatched=%v waited=%v status=%s\n", str(out["command_id"]), out["dispatched"], out["waited"], str(out["status"]))
	if e := str(out["error"]); e != "-" {
		fmt.Printf("error: %s\n", e)
	}
	if r, ok := out["result"]; ok && r != nil {
		pretty, _ := json.MarshalIndent(r, "", "  ")
		fmt.Printf("result:\n%s\n", pretty)
	}
	if n := str(out["note"]); n != "-" {
		fmt.Printf("提示: %s\n", n)
	}
	if s := str(out["status"]); s == "ok" || s == "-" {
		return 0
	}
	return 1
}

func cmdEvents(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	limit := fs.Int("limit", 50, "条数")
	kind := fs.String("kind", "", "只看某类: node_event|command_result|config_ack|conn")
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	args := fs.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: controlplane events <node_id>")
		return 2
	}
	c := mustClient(*scfg, *server, *token)
	var out struct {
		Count  int        `json:"count"`
		Events []cp.Event `json:"events"`
	}
	if err := c.getJSON(fmt.Sprintf("/admin/nodes/%s/events?limit=%d", args[0], *limit), &out); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	n := 0
	for _, e := range out.Events {
		if *kind != "" && e.Kind != *kind {
			continue
		}
		n++
		fmt.Printf("%s  %-16s seq=%-6d %-22s %s\n", cp.HumanTime(e.TSMs), e.Kind, e.Seq, e.Type, e.Message)
		if len(e.Data) > 2 {
			var m map[string]any
			if json.Unmarshal(e.Data, &m) == nil {
				if b, err := json.Marshal(m); err == nil && len(b) > 2 {
					fmt.Printf("%s\n", "    "+trim(string(b), 300))
				}
			}
		}
	}
	if n == 0 {
		fmt.Println("没有匹配的事件")
	}
	return 0
}

func cmdTelemetry(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("telemetry", flag.ExitOnError)
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	args := fs.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: controlplane telemetry <node_id>")
		return 2
	}
	c := mustClient(*scfg, *server, *token)
	b, code, err := c.do(http.MethodGet, "/admin/nodes/"+args[0]+"/telemetry", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if code/100 != 2 {
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
		return 1
	}
	var pretty bytes.Buffer
	_ = json.Indent(&pretty, b, "", "  ")
	fmt.Println(pretty.String())
	return 0
}

func cmdDeleteNode(argv []string, cfgPath string) int {
	fs := flag.NewFlagSet("delete-node", flag.ExitOnError)
	server, token := clientFlags(fs, cfgPath)
	scfg := fs.String("config", cfgPath, "配置文件路径")
	_ = fs.Parse(argv)
	args := fs.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: controlplane delete-node <node_id>")
		return 2
	}
	c := mustClient(*scfg, *server, *token)
	b, code, err := c.do(http.MethodDelete, "/admin/nodes/"+args[0], nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if code/100 != 2 {
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", code, strings.TrimSpace(string(b)))
		return 1
	}
	fmt.Printf("已吊销并删除 %s\n", args[0])
	return 0
}

func urlEnc(s string) string {
	r := strings.NewReplacer(" ", "%20", "&", "%26", "#", "%23", "?", "%3F", "+", "%2B", "=", "%3D", "/", "%2F")
	return r.Replace(s)
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
