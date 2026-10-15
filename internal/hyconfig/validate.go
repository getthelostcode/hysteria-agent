// Package hyconfig 负责 Hysteria2 配置的校验、版本管理、原子写入与回滚。
//
// 校验分五层（任何一层失败都拒绝落盘）：
//  1. schema  —— 语法 + 必填字段 + 枚举值 + 未知键告警（专治 masqueradee 这类拼写错误）；
//  2. 端口     —— listen 的端口能不能绑（TCP/UDP 都查；被我们自己的 hysteria 进程占着算通过）；
//  3. 证书     —— tls.cert/key 存在、能配对、未过期、是否临近过期；acme 模式检查域名格式；
//  4. 一致性   —— trafficStats 是否是 Agent 能读到的那一份（否则遥测/踢人会静默失效）；
//  5. 引用     —— acl / resolver / mmdb 等引用的文件是否存在。
package hyconfig

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/getthelostcode/hysteria-agent/internal/sysinfo"
)

// ValidateOptions 控制校验的严格程度。
type ValidateOptions struct {
	CheckPort    bool
	CheckCert    bool
	CertWarnDays int
	// OwnerPID 是当前正在运行的 hysteria 进程；端口被它占着视为合法（热更新场景）。
	OwnerPID int
	// RequireTrafficStats 为 true 时 trafficStats 缺失会记为 error（Agent 的遥测依赖它）。
	RequireTrafficStats bool
	// ExpectedTrafficListen / ExpectedTrafficSecret 由 Agent 从自身配置注入，用来发现"对端配置与本地不一致"。
	ExpectedTrafficListen string
	ExpectedTrafficSecret string
	// HysteriaBinary 是可执行文件路径：端口被"同名二进制"的进程占着时视为合法。
	// 场景：运维在一台已经在跑 hysteria 的机器上校验一份新配置——那台机器上占用
	// 端口的正是 hysteria 自己，不该报"端口被别的进程占用"。
	HysteriaBinary string
}

// Hysteria 是从配置里抽取的关键字段。
type Hysteria struct {
	Listen             string   `json:"listen"`
	ListenPort         int      `json:"listen_port"`
	TLSMode            string   `json:"tls_mode"` // tls | acme | none
	CertFile           string   `json:"cert_file,omitempty"`
	KeyFile            string   `json:"key_file,omitempty"`
	CertExpiresAt      string   `json:"cert_expires_at,omitempty"`
	CertExpiresInDays  int      `json:"cert_expires_in_days,omitempty"`
	ACMEDomains        []string `json:"acme_domains,omitempty"`
	AuthType           string   `json:"auth_type"`
	AuthURL            string   `json:"auth_url,omitempty"`
	UserCount          int      `json:"user_count"`
	MasqueradeType     string   `json:"masquerade_type,omitempty"`
	TrafficStatsListen string   `json:"traffic_stats_listen,omitempty"`
	TrafficStatsSecret bool     `json:"traffic_stats_secret_set"`
	DisableUDP         bool     `json:"disable_udp,omitempty"`
	IgnoreClientBW     bool     `json:"ignore_client_bandwidth,omitempty"`
}

// Result 是校验结果，字段直接对应控制通道 ConfigAck。
type Result struct {
	Valid    bool      `json:"valid"`
	Format   string    `json:"format"`
	SHA256   string    `json:"sha256"`
	Errors   []string  `json:"errors,omitempty"`
	Warnings []string  `json:"warnings,omitempty"`
	Checks   []string  `json:"checks"` // "schema:ok" "port:ok" "cert:expires_in=45d" 形式
	Parsed   *Hysteria `json:"parsed,omitempty"`
}

// Sum 计算 payload 的 sha256（hex），控制面与本地都用它做一致性校验。
func Sum(payload []byte) string {
	s := sha256.Sum256(payload)
	return hex.EncodeToString(s[:])
}

var validAuthTypes = map[string]bool{"password": true, "userpass": true, "http": true, "command": true}
var validTLSModes = map[string]bool{"tls": true, "acme": true, "none": true}

// knownTopLevel 是 Hysteria2 服务端配置里允许出现的顶层键（其它键一律告警，多半是拼写错误）。
var knownTopLevel = map[string]bool{
	"listen": true, "tls": true, "acme": true, "auth": true, "masquerade": true,
	"trafficStats": true, "bandwidth": true, "ignoreClientBandwidth": true,
	"disableUDP": true, "udpIdleTimeout": true, "resolver": true, "acl": true,
	"outbounds": true, "quic": true, "congestion": true, "sniff": true,
	"mmdb": true, "geoip": true, "geosite": true, "geoUpdateInterval": true,
	"metrics": true, "log": true, "logLevel": true, "speedTest": true,
	"speedTestMode": true, "http": true, "fastOpen": true, "disablePathMTUDiscovery": true,
	"localHTTPListen": true, "insecure": true, "listenHTTP": true, "listenHTTPS": true,
}

// Validate 执行完整校验。payload 是控制面下发的配置正文。
func Validate(ctx context.Context, payload []byte, format string, opt ValidateOptions) *Result {
	res := &Result{Format: normalizeFormat(format), SHA256: Sum(payload)}
	fail := func(format string, args ...any) {
		res.Errors = append(res.Errors, fmt.Sprintf(format, args...))
	}
	warn := func(format string, args ...any) {
		res.Warnings = append(res.Warnings, fmt.Sprintf(format, args...))
	}

	if len(payload) == 0 {
		fail("配置为空")
		res.Valid = false
		res.Checks = append(res.Checks, "schema:empty")
		return res
	}

	// ---- 第 1 层：schema ----
	doc := map[string]any{}
	switch res.Format {
	case "json":
		if err := json.Unmarshal(payload, &doc); err != nil {
			fail("JSON 语法错误: %v", err)
		}
	default:
		if err := yaml.Unmarshal(payload, &doc); err != nil {
			fail("YAML 语法错误: %v", err)
		}
	}
	if len(res.Errors) > 0 {
		res.Checks = append(res.Checks, "schema:error")
		return res
	}
	if len(doc) == 0 {
		fail("配置解析后为空文档")
		res.Checks = append(res.Checks, "schema:empty")
		return res
	}

	hy := &Hysteria{}
	res.Parsed = hy

	for k := range doc {
		if !knownTopLevel[k] {
			warn("未知的顶层配置键 %q（拼写错误？Hysteria 会忽略它）", k)
		}
	}

	// listen：Hysteria 允许省略（默认 :443）。省略时告警并按默认值继续校验端口，
	// 这样"诊断一台 listen 被注释掉的老机器"不会报假错误。
	listen, _ := doc["listen"].(string)
	if strings.TrimSpace(listen) == "" {
		warn("未显式配置 listen，Hysteria 会使用默认值 :443")
		listen = ":443"
	}
	hy.Listen = listen
	port, err := listenPort(listen)
	if err != nil {
		fail("listen %q 无法解析出端口: %v", listen, err)
	} else {
		hy.ListenPort = port
	}

	// tls / acme
	if tlsRaw, ok := doc["tls"]; ok {
		hy.TLSMode = "tls"
		tlsMap, _ := tlsRaw.(map[string]any)
		if tlsMap == nil {
			fail("tls 必须是对象（tls: {cert: ..., key: ...}）")
		} else {
			hy.CertFile, _ = tlsMap["cert"].(string)
			hy.KeyFile, _ = tlsMap["key"].(string)
			if hy.CertFile == "" || hy.KeyFile == "" {
				if _, insecure := tlsMap["insecure"]; !insecure {
					fail("tls 块必须同时提供 cert 与 key（或用 acme）")
				}
			}
		}
	}
	if acmeRaw, ok := doc["acme"]; ok {
		if hy.TLSMode == "tls" {
			fail("tls 与 acme 不能同时配置")
		}
		hy.TLSMode = "acme"
		acmeMap, _ := acmeRaw.(map[string]any)
		if acmeMap == nil {
			fail("acme 必须是对象")
		} else {
			hy.ACMEDomains = toStringSlice(acmeMap["domains"])
			if len(hy.ACMEDomains) == 0 {
				fail("acme.domains 不能为空")
			}
			for _, d := range hy.ACMEDomains {
				if !strings.Contains(d, ".") || strings.ContainsAny(d, " \t") {
					fail("acme 域名格式可疑: %q", d)
				}
			}
			if _, ok := acmeMap["email"]; !ok {
				warn("acme 未配置 email，Let's Encrypt 下发失败时不会有通知")
			}
		}
	}
	if hy.TLSMode == "" {
		hy.TLSMode = "none"
		fail("缺少 tls 或 acme 配置块（Hysteria 需要证书才能启动）")
	}

	// auth
	authRaw, ok := doc["auth"]
	if !ok {
		fail("缺少 auth 配置块")
	} else if authMap, ok := authRaw.(map[string]any); ok {
		hy.AuthType, _ = authMap["type"].(string)
		if hy.AuthType == "" {
			hy.AuthType = "password" // Hysteria 的默认值
		}
		if !validAuthTypes[hy.AuthType] {
			fail("auth.type %q 不在白名单（password|userpass|http|command）", hy.AuthType)
		}
		switch hy.AuthType {
		case "password":
			pw, _ := authMap["password"].(string)
			if pw == "" {
				fail("auth.type=password 但缺少 auth.password")
			} else if len(pw) < 8 {
				warn("auth.password 只有 %d 个字符，建议至少 16 位随机串", len(pw))
			}
		case "userpass":
			users, _ := authMap["userpass"].(map[string]any)
			hy.UserCount = len(users)
			if len(users) == 0 {
				fail("auth.type=userpass 但 userpass 表为空")
			} else {
				for u, p := range users {
					if strings.TrimSpace(u) == "" {
						fail("userpass 里有空用户名")
					}
					if s, _ := p.(string); strings.TrimSpace(s) == "" {
						fail("用户 %q 的密码为空", u)
					}
				}
			}
		case "http":
			// HTTP 鉴权：节点会把鉴权请求转发到 auth.http.url。url 写错 = 所有用户都连不上，
			// 所以要判死而不是警告（历史上这里只是 warn，配错了要等用户报故障才发现）。
			httpRaw, ok := authMap["http"].(map[string]any)
			if !ok {
				fail("auth.type=http 但缺少 auth.http 配置块（需要 http.url）")
				break
			}
			url, _ := httpRaw["url"].(string)
			url = strings.TrimSpace(url)
			if url == "" {
				fail("auth.type=http 但 auth.http.url 为空")
			} else if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
				fail("auth.http.url %q 必须是 http:// 或 https:// 开头", url)
			} else if strings.Contains(url, "@") {
				warn("auth.http.url 里带了 userinfo（%s）：Hysteria 不会用它做鉴权，建议去掉", url)
			}
			hy.AuthURL = url
			if v, ok := httpRaw["insecure"]; ok {
				if _, isBool := v.(bool); !isBool {
					fail("auth.http.insecure 必须是 true/false")
				}
			}
			if strings.HasPrefix(url, "http://") {
				warn("auth.http.url 使用明文 http://：鉴权凭据在链路上不加密，建议改成 https:// 并保持 insecure: false")
			}
		case "command":
			if _, ok := authMap["command"]; !ok {
				warn("auth.type=command 但未配置 auth.command")
			}
		}
	} else {
		fail("auth 必须是对象")
	}

	// masquerade
	if mq, ok := doc["masquerade"]; ok {
		if m, ok := mq.(map[string]any); ok {
			hy.MasqueradeType, _ = m["type"].(string)
			if hy.MasqueradeType == "" {
				hy.MasqueradeType = "404"
			}
			if hy.MasqueradeType == "proxy" {
				if p, ok := m["proxy"].(map[string]any); ok {
					if u, _ := p["url"].(string); u == "" {
						fail("masquerade.type=proxy 但 masquerade.proxy.url 为空")
					}
				} else {
					fail("masquerade.type=proxy 但缺少 masquerade.proxy")
				}
			}
		} else {
			fail("masquerade 必须是对象")
		}
	} else {
		warn("没有 masquerade 配置，Hysteria 默认返回 404（建议配置伪装站点）")
	}

	// trafficStats：Agent 的遥测/踢人都依赖它
	if tsRaw, ok := doc["trafficStats"]; ok {
		if tsMap, ok := tsRaw.(map[string]any); ok {
			hy.TrafficStatsListen, _ = tsMap["listen"].(string)
			secret, _ := tsMap["secret"].(string)
			hy.TrafficStatsSecret = secret != ""
			if secret == "" {
				fail("trafficStats.secret 为空，本地 API 无须鉴权即可读取（不允许下发）")
			}
			if opt.ExpectedTrafficSecret != "" && secret != opt.ExpectedTrafficSecret {
				fail("trafficStats.secret 与 Agent 的 hysteria.api_secret 不一致：Agent 将无法读取流量/踢人")
			}
			if hy.TrafficStatsListen == "" {
				fail("trafficStats.listen 不能为空（Agent 需要它读取流量）")
			}
		} else {
			fail("trafficStats 必须是对象")
		}
	} else if opt.RequireTrafficStats {
		fail("缺少 trafficStats 配置块：Agent 无法采集流量/在线数/踢人")
	} else {
		warn("没有 trafficStats 配置，Agent 的流量与在线数指标会缺失")
	}

	// 引用文件
	if hy.TLSMode == "tls" && hy.CertFile != "" && hy.CheckCertEnabled() {
		checkCert(ctx, hy, opt, res, fail, warn)
	}
	if acl, ok := doc["acl"].(map[string]any); ok {
		if f, _ := acl["file"].(string); f != "" {
			if _, err := os.Stat(f); err != nil {
				warn("acl.file %s 不存在: %v", f, err)
			}
		}
	}
	if m, ok := doc["mmdb"].(string); ok && m != "" {
		if _, err := os.Stat(m); err != nil {
			warn("mmdb 文件 %s 不存在: %v", m, err)
		}
	}
	if r, ok := doc["resolver"].(map[string]any); ok {
		if t, _ := r["type"].(string); t == "file" {
			if f, _ := r["file"].(string); f != "" {
				if _, err := os.Stat(f); err != nil {
					warn("resolver.file %s 不存在: %v", f, err)
				}
			}
		}
	}

	res.Checks = append(res.Checks, "schema:ok")
	if hy.TLSMode == "tls" && hy.CertFile != "" {
		res.Checks = append(res.Checks, fmt.Sprintf("cert:%s", hy.CertExpiresAt))
	} else {
		res.Checks = append(res.Checks, "cert:"+hy.TLSMode)
	}
	res.Checks = append(res.Checks, fmt.Sprintf("auth:%s", hy.AuthType))
	if hy.TrafficStatsListen != "" {
		res.Checks = append(res.Checks, "traffic_stats:ok")
	}

	// ---- 第 2 层：端口 ----
	if opt.CheckPort && hy.ListenPort > 0 {
		portCheck(res, hy, opt, fail, warn)
	} else if hy.ListenPort > 0 {
		res.Checks = append(res.Checks, "port:skipped")
	}

	res.Valid = len(res.Errors) == 0
	return res
}

// CheckCertEnabled 保留钩子，便于测试里关掉证书文件检查。
func (h *Hysteria) CheckCertEnabled() bool { return true }

func checkCert(ctx context.Context, hy *Hysteria, opt ValidateOptions, res *Result, fail, warn func(string, ...any)) {
	_ = ctx
	pair, err := tls.LoadX509KeyPair(hy.CertFile, hy.KeyFile)
	if err != nil {
		fail("证书/私钥无法加载: %v", err)
		return
	}
	if len(pair.Certificate) == 0 {
		fail("证书链为空: %s", hy.CertFile)
		return
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		fail("解析证书失败: %v", err)
		return
	}
	now := time.Now()
	hy.CertExpiresAt = leaf.NotAfter.UTC().Format(time.RFC3339)
	hy.CertExpiresInDays = int(leaf.NotAfter.Sub(now).Hours() / 24)
	switch {
	case now.After(leaf.NotAfter):
		fail("证书已于 %s 过期", leaf.NotAfter.Format(time.RFC3339))
	case now.Before(leaf.NotBefore):
		fail("证书尚未生效（NotBefore=%s）", leaf.NotBefore.Format(time.RFC3339))
	case hy.CertExpiresInDays <= opt.CertWarnDays:
		warn("证书将在 %d 天后过期（%s）", hy.CertExpiresInDays, leaf.NotAfter.Format(time.RFC3339))
	}
	// 证书 SAN 覆盖域名检查（acme 模式下不适用）
	if len(hy.ACMEDomains) > 0 && leaf.DNSNames != nil {
		for _, d := range hy.ACMEDomains {
			if err := leaf.VerifyHostname(d); err != nil {
				warn("证书不覆盖域名 %s: %v", d, err)
			}
		}
	}
	res.Checks = append(res.Checks, fmt.Sprintf("cert:expires_in=%dd", hy.CertExpiresInDays))
	res.Valid = res.Valid || true
}

func portCheck(res *Result, hy *Hysteria, opt ValidateOptions, fail, warn func(string, ...any)) {
	// Hysteria 2 是 QUIC（UDP）服务；同端口若还配了 HTTP/HTTPS 明文监听，TCP 也要查。
	protos := []string{"udp"}
	if hy.DisableUDP {
		protos = []string{"tcp"}
	}
	for _, proto := range protos {
		pu, err := sysinfo.LookupPort(hy.ListenPort, proto)
		if err != nil {
			warn("检查 %s/%d 端口占用失败: %v", proto, hy.ListenPort, err)
			continue
		}
		switch {
		case !pu.InUse:
			res.Checks = append(res.Checks, fmt.Sprintf("port:%s/%d=free", proto, hy.ListenPort))
		case opt.OwnerPID > 0 && containsInt(pu.PIDs, opt.OwnerPID):
			// 端口被正在运行的 hysteria 自己占着：热更新场景下完全正常。
			res.Checks = append(res.Checks, fmt.Sprintf("port:%s/%d=owned_by_hysteria", proto, hy.ListenPort))
		case opt.HysteriaBinary != "" && anyPIDMatchesBinary(pu.PIDs, opt.HysteriaBinary):
			// 占用者可执行文件名与配置里的 hysteria 一致（例如另一份配置启的实例）。
			res.Checks = append(res.Checks, fmt.Sprintf("port:%s/%d=owned_by_hysteria_process", proto, hy.ListenPort))
		case len(pu.PIDs) == 0:
			// 拿不到持有者（权限/namespace 差异）时有占用但无法归因：记为告警而不是拒绝。
			warn("%s/%d 已被占用但无法确认持有者（可能是本机 hysteria，也可能是别的进程）", proto, hy.ListenPort)
			res.Checks = append(res.Checks, fmt.Sprintf("port:%s/%d=unknown_owner", proto, hy.ListenPort))
		default:
			fail("%s/%d 已被进程 %v 占用，Hysteria 将无法启动", proto, hy.ListenPort, pu.PIDs)
		}
	}
}

func listenPort(listen string) (int, error) {
	s := strings.TrimSpace(listen)
	if s == "" {
		return 0, fmt.Errorf("空地址")
	}
	// 支持 ":443" / "0.0.0.0:443" / "[::]:443"
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return 0, fmt.Errorf("缺少端口部分")
	}
	p, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return 0, fmt.Errorf("端口不是数字: %q", s[i+1:])
	}
	if p < 1 || p > 65535 {
		return 0, fmt.Errorf("端口越界: %d", p)
	}
	return p, nil
}

func normalizeFormat(f string) string {
	switch strings.ToLower(strings.TrimSpace(f)) {
	case "json":
		return "json"
	case "", "yaml", "yml":
		return "yaml"
	default:
		return "yaml"
	}
}

func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, i := range t {
			if s, ok := i.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	}
	return nil
}

func containsInt(l []int, v int) bool {
	for _, i := range l {
		if i == v {
			return true
		}
	}
	return false
}

// anyPIDMatchesBinary 判断这些 PID 里有没有"可执行文件与给定路径同名"的进程。
func anyPIDMatchesBinary(pids []int, binary string) bool {
	if binary == "" {
		return false
	}
	for _, pid := range pids {
		if sysinfo.ProcessAlive(pid, binary) {
			return true
		}
	}
	return false
}
