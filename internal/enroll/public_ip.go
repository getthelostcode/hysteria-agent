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

// detectPublicIP 依次请求 diag.public_ip_urls（默认 https://api.ipify.org），
// 返回第一个合法且为公网的单行响应。任何失败都返回空字符串（调用方继续注册）。
func detectPublicIP(ctx context.Context, cfg *config.Config, logf func(format string, args ...any)) string {
	if cfg == nil || len(cfg.Diag.PublicIPURLs) == 0 {
		return ""
	}

	for _, url := range cfg.Diag.PublicIPURLs {
		url = strings.TrimSpace(url)
		if url == "" {
			continue
		}

		ip := probePublicIP(ctx, url)
		if ip != "" {
			return ip
		}
	}

	if logf != nil {
		logf("提示：公网地址探测失败（不影响注册）；面板会退回显示控制面观测到的地址")
	}

	return ""
}

func probePublicIP(ctx context.Context, url string) string {
	reqCtx, cancel := context.WithTimeout(ctx, publicIPProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "hysteria-agent/1.0.0")

	resp, err := http.DefaultClient.Do(req)
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
