package enroll

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
)

// publicIPProbeTimeout 单次探测上限。注册是交互式操作（运维在等），
// 2 秒拿不到就直接放弃：IP 只是展示信息，不值得让注册卡住。
const publicIPProbeTimeout = 2 * time.Second

// ipv4OnlyClient 强制 IPv4 出口。
//
// 为什么必须强制：双栈机器上 Go 的拨号按 RFC 6724 排序，**优先 IPv6** —— 请求
// https://api.ipify.org 会走 v6 出去，返回的自然也是 v6 地址，于是面板「节点 IP」列
// 显示成一串 2001:... 的地址。运维要的是可读、可 ping、能填进防火墙的 **IPv4**。
// 用 tcp4 拨号后，同样的 URL 返回的就是本机 IPv4 出口地址。
var ipv4OnlyClient = &http.Client{
	Timeout: publicIPProbeTimeout,
	Transport: &http.Transport{
		// network 固定 "tcp4"：域名只解析 A 记录，连接也只走 IPv4。
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: publicIPProbeTimeout}

			return dialer.DialContext(ctx, "tcp4", addr)
		},
	},
}

// detectPublicIP 探测本机出口公网地址，**优先 IPv4**。
//
// 依次请求 diag.public_ip_urls（默认 https://api.ipify.org）：先用强制 v4 的客户端，
// 拿不到（例如纯 v6 主机）再用默认客户端兜底取 v6。任何失败都返回空字符串，
// 调用方照常注册 —— IP 只影响展示。
func detectPublicIP(ctx context.Context, cfg *config.Config, logf func(format string, args ...any)) string {
	if cfg == nil || len(cfg.Diag.PublicIPURLs) == 0 {
		return ""
	}

	urls := make([]string, 0, len(cfg.Diag.PublicIPURLs))

	for _, url := range cfg.Diag.PublicIPURLs {
		if url = strings.TrimSpace(url); url != "" {
			urls = append(urls, url)
		}
	}

	var fallbackV6 string

	for _, url := range urls {
		ip := probePublicIP(ctx, url, ipv4OnlyClient)
		if ip == "" {
			continue
		}

		if isIPv4(ip) {
			return ip
		}

		// v4 拿不到时先记住 v6：宁可展示 v6，也不要不展示。
		if fallbackV6 == "" {
			fallbackV6 = ip
		}
	}

	if fallbackV6 != "" {
		if logf != nil {
			logf("本机没有可用的 IPv4 出口，回退使用 IPv6 地址 %s", fallbackV6)
		}

		return fallbackV6
	}

	// 强制 v4 全部失败（可能是纯 v6 主机）：用默认客户端再试一遍。
	for _, url := range urls {
		if ip := probePublicIP(ctx, url, http.DefaultClient); ip != "" {
			if logf != nil {
				logf("IPv4 探测失败，回退到默认网络栈取得的地址 %s", ip)
			}

			return ip
		}
	}

	if logf != nil {
		logf("提示：公网地址探测失败（不影响注册）；面板会退回显示控制面观测到的地址")
	}

	return ""
}

func probePublicIP(ctx context.Context, url string, client *http.Client) string {
	reqCtx, cancel := context.WithTimeout(ctx, publicIPProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "hysteria-agent/1.0.0")

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return ""
	}

	ip := strings.TrimSpace(string(body))
	parsed := net.ParseIP(ip)

	if parsed == nil || parsed.IsLoopback() || parsed.IsPrivate() || parsed.IsUnspecified() {
		return ""
	}

	return parsed.String()
}

func isIPv4(ip string) bool {
	parsed := net.ParseIP(ip)

	return parsed != nil && parsed.To4() != nil
}
