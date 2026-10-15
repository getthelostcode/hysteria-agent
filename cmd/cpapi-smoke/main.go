// Command cpapi-smoke 用真实控制面验证桥接客户端（internal/cpapi）。
//
// 流程：管理登录 → 申请一次性 enroll 令牌 → Enroll（拿 node_id/node_secret）
//
//	→ 以该节点身份做 心跳 / gzip 指标 / 指令轮询 / 配置拉取 / 诊断上报 / 升级检查。
//
// 用法：
//
//	go run ./cmd/cpapi-smoke -cp https://cp.example.com -email admin@example.com -password '***'
//	go run ./cmd/cpapi-smoke -cp http://127.0.0.1:8001 -token enr_xxx   # 已有令牌
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/cpapi"
)

type result struct {
	name string
	err  error
	note string
}

func main() {
	cp := flag.String("cp", "", "控制面地址（Laravel），如 http://127.0.0.1:8001")
	email := flag.String("email", "", "管理后台邮箱（用于申请 enroll 令牌）")
	password := flag.String("password", "", "管理后台密码")
	enrollToken := flag.String("token", "", "已持有的 enroll 令牌（给了就不登录）")
	nodeName := flag.String("node-name", "cpapi-smoke", "节点名")
	flag.Parse()

	if *cp == "" {
		fmt.Fprintln(os.Stderr, "-cp 必填")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var results []result

	if *enrollToken == "" {
		if *email == "" || *password == "" {
			fmt.Fprintln(os.Stderr, "需要 -token，或 -email/-password 用于申请令牌")
			os.Exit(2)
		}

		tok, err := issueEnrollToken(ctx, *cp, *email, *password)
		results = append(results, result{name: "申请 enroll 令牌", err: err, note: mask(tok)})
		if err != nil {
			report(results)
		}
		*enrollToken = tok
	}

	enrolled, err := cpapi.Enroll(ctx, cpapi.Config{UserAgent: "hysteria-controlplane-bridge-smoke/1.0"}, *cp, cpapi.EnrollRequest{
		EnrollToken:  *enrollToken,
		NodeName:     *nodeName,
		Hostname:     hostname(),
		OS:           "linux",
		Arch:         "amd64",
		Kernel:       "smoke",
		MachineID:    "cpapi-smoke-" + fmt.Sprint(time.Now().UnixNano()),
		AgentVersion: "0.1.0",
		Protocol:     "bridge/v1",
	})
	results = append(results, result{name: "Enroll（代理注册）", err: err})
	if err != nil {
		report(results)
	}

	results[len(results)-1].note = fmt.Sprintf("node_id=%s ws_path=%s revision=%s hb=%ds",
		enrolled.NodeID, enrolled.WSPath, enrolled.ConfigRevision, enrolled.HeartbeatIntervalSeconds)
	if enrolled.WSPath != "/api/v1/agent/control" {
		results = append(results, result{name: "注册响应契约", err: fmt.Errorf("ws_path 异常: %s", enrolled.WSPath)})
	}

	client, err := cpapi.New(cpapi.Config{
		BaseURL:   *cp,
		NodeID:    enrolled.NodeID,
		Secret:    enrolled.NodeSecret,
		UserAgent: "hysteria-controlplane-bridge-smoke/1.0",
		Gzip:      true,
		Retry:     cpapi.RetryPolicy{Attempts: 3, Initial: time.Second, Max: 10 * time.Second, Multiplier: 2, Jitter: 0.2},
	})
	if err != nil {
		results = append(results, result{name: "构造客户端", err: err})
		report(results)
	}

	hb, err := client.Heartbeat(ctx, cpapi.HeartbeatRequest{Version: "2.6.0", AgentVersion: "0.1.0", Uptime: 12})
	results = append(results, result{name: "心跳（HMAC）", err: err, note: fmt.Sprintf("interval=%d config_v=%d pending_cmds=%d", hbInt(hb), cfgV(hb), cmdPending(hb))})

	cpu, mem := 12.5, 37.5
	netRx, netTx, conns, users := int64(1024), int64(2048), int64(7), int64(3)
	ing, err := client.Metrics(ctx, []cpapi.MetricSample{{
		CPU: &cpu, Mem: &mem, NetRx: &netRx, NetTx: &netTx, ConnCount: &conns, Users: &users,
		RecordedAt: time.Now().UTC().Format(time.RFC3339),
	}})
	results = append(results, result{name: "指标上报（gzip 签名）", err: err, note: fmt.Sprintf("stored=%d", ingInt(ing))})

	polled, err := client.CommandsPoll(ctx, 10)
	results = append(results, result{name: "指令轮询", err: err, note: fmt.Sprintf("pending=%d", len(polledCommands(polled)))})

	latest, err := client.ConfigLatest(ctx, 0)
	results = append(results, result{name: "配置拉取", err: err, note: configNote(latest)})

	_, err = client.Logs(ctx, []cpapi.LogEntry{{Level: "info", Message: "cpapi smoke", Context: map[string]any{"step": "logs"}}})
	results = append(results, result{name: "日志上报", err: err})

	err = client.Diagnostics(ctx, map[string]any{"summary": "cpapi smoke", "dns": "ok"})
	results = append(results, result{name: "诊断上报", err: err})

	up, err := client.UpgradeCheck(ctx, "amd64", "0.0.1", "stable")
	results = append(results, result{name: "升级检查", err: err, note: upgradeNote(up)})

	// 反例：错密钥必须 401，且不能被当成成功。
	bad, _ := cpapi.New(cpapi.Config{BaseURL: *cp, NodeID: enrolled.NodeID, Secret: "nds_wrong", Retry: cpapi.RetryPolicy{Attempts: 1, Initial: time.Millisecond}})
	_, err = bad.Heartbeat(ctx, cpapi.HeartbeatRequest{})
	results = append(results, result{name: "错密钥被拒（期望 401）", err: isRejected(err)})

	report(results)
}

func issueEnrollToken(ctx context.Context, base, email, password string) (string, error) {
	body, _ := json.Marshal(map[string]string{"email": email, "password": password, "device_name": "cpapi-smoke"})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/admin/v1/auth/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "hysteria-controlplane-bridge-smoke/1.0")

	var login struct {
		Token string `json:"token"`
	}
	if err := do(req, &login); err != nil {
		return "", fmt.Errorf("登录失败: %w", err)
	}

	body, _ = json.Marshal(map[string]any{"note": "cpapi smoke", "max_uses": 1, "expires_in_days": 1})
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/admin/v1/enroll-tokens", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+login.Token)
	req.Header.Set("User-Agent", "hysteria-controlplane-bridge-smoke/1.0")

	var issued struct {
		Token string `json:"token"`
	}
	if err := do(req, &issued); err != nil {
		return "", fmt.Errorf("申请令牌失败: %w", err)
	}
	if issued.Token == "" {
		return "", fmt.Errorf("响应没有 token 字段")
	}

	return issued.Token, nil
}

func do(req *http.Request, out any) error {
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(raw)))
	}

	return json.Unmarshal(raw, out)
}

func report(results []result) {
	failed := 0

	for _, r := range results {
		if r.err != nil {
			failed++
			fmt.Printf("[FAIL] %-24s %v\n", r.name, r.err)

			continue
		}
		if r.note != "" {
			fmt.Printf("[PASS] %-24s %s\n", r.name, r.note)
		} else {
			fmt.Printf("[PASS] %-24s\n", r.name)
		}
	}

	fmt.Printf("\n合计 %d 项，失败 %d 项\n", len(results), failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func isRejected(err error) error {
	if err == nil {
		return fmt.Errorf("错密钥竟然通过了")
	}
	if cpapi.IsUnauthorized(err) || cpapi.IsUnknownNode(err) {
		return nil
	}

	return fmt.Errorf("期望认证失败，实际: %w", err)
}

func mask(s string) string {
	if len(s) <= 10 {
		return "***"
	}

	return s[:10] + "..."
}

func truncate(s string) string {
	if len(s) > 160 {
		return s[:160]
	}

	return s
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "smoke-host"
	}

	return h
}

func hbInt(hb *cpapi.HeartbeatResponse) int {
	if hb == nil {
		return 0
	}

	return hb.HeartbeatInterval
}

func cfgV(hb *cpapi.HeartbeatResponse) int {
	if hb == nil {
		return 0
	}

	return hb.Config.Version
}

func cmdPending(hb *cpapi.HeartbeatResponse) int {
	if hb == nil {
		return 0
	}

	return hb.Commands.Pending
}

func ingInt(r *cpapi.IngestResponse) int {
	if r == nil {
		return 0
	}

	return r.Stored
}

func polledCommands(r *cpapi.CommandsPollResponse) []cpapi.Command {
	if r == nil {
		return nil
	}

	return r.Commands
}

func configNote(r *cpapi.ConfigLatestResponse) string {
	if r == nil {
		return ""
	}
	if r.UpToDate {
		return "up_to_date=true"
	}
	if r.Config == nil {
		return "no config"
	}

	return fmt.Sprintf("v%d sha=%s bytes=%d", r.Config.Version, truncate(r.Config.Sha256), len(r.Config.ContentYAML))
}

func upgradeNote(r *cpapi.UpgradeCheckResponse) string {
	if r == nil {
		return ""
	}
	if r.UpdateAvailable && r.Package != nil {
		return "有新版 " + r.Package.Version
	}

	return "已是最新"
}
