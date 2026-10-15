// Package hyapi 是 Hysteria 2 服务端「本地 Traffic Stats API」的客户端。
//
// 只用官方文档里的三个端点（其它端点各构建版本差异大，不要碰）：
//
//	GET  /traffic[?clear=1]  -> {"用户ID": {"tx": 字节, "rx": 字节}}
//	POST /kick               -> body 是裸 JSON 数组 ["uid1","uid2"]
//	GET  /online             -> {"用户ID": 在线设备数}
//
// 鉴权：Authorization: <secret> 头；POST /kick 额外把 secret 放进 query(?secret=)，
// 两个通道都发，兼容只在其中一处校验的构建版本。
package hyapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client 是 Hysteria 本地 API 客户端。
type Client struct {
	baseURL string
	secret  string
	http    *http.Client
}

// New 构建客户端；baseURL 形如 http://127.0.0.1:8080。
func New(baseURL, secret string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		secret:  secret,
		http:    &http.Client{Timeout: timeout},
	}
}

// BaseURL 返回配置的基地址（诊断报告里会用到）。
func (c *Client) BaseURL() string { return c.baseURL }

// Traffic 返回每个用户的流量计数器。clear=true 时服务端返回"自上次读取以来的增量"并清零，
// 这样客户端不需要维护累计基线，服务端重启清零也不会产生负数增量。
type Traffic struct {
	Tx uint64 `json:"tx"`
	Rx uint64 `json:"rx"`
}

func (c *Client) Traffic(ctx context.Context, clear bool) (map[string]Traffic, error) {
	u := c.baseURL + "/traffic"
	if clear {
		u += "?clear=1"
	}
	body, err := c.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	// 双路容错：先试扁平 map[string]{tx,rx}，失败再试 {"userids": {...}} 包装格式。
	flat := map[string]Traffic{}
	if err := json.Unmarshal(body, &flat); err == nil {
		return filterZero(flat), nil
	}
	var wrapped struct {
		UserIDs map[string]Traffic `json:"userids"`
		Users   map[string]Traffic `json:"users"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil {
		if len(wrapped.UserIDs) > 0 {
			return filterZero(wrapped.UserIDs), nil
		}
		return filterZero(wrapped.Users), nil
	}
	return nil, fmt.Errorf("解析 /traffic 响应失败: %s", truncate(body, 200))
}

func filterZero(in map[string]Traffic) map[string]Traffic {
	out := make(map[string]Traffic, len(in))
	for k, v := range in {
		if v.Tx == 0 && v.Rx == 0 {
			continue
		}
		out[k] = v
	}
	return out
}

// Online 返回每个用户的在线设备数。
func (c *Client) Online(ctx context.Context) (map[string]int, error) {
	body, err := c.do(ctx, http.MethodGet, c.baseURL+"/online", nil)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("解析 /online 响应失败: %s", truncate(body, 200))
	}
	return out, nil
}

// Kick 踢掉指定用户（Hysteria 客户端会自动重连，所以踢 ≠ 封禁）。
func (c *Client) Kick(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	// 去重，避免同一个用户被写两次。
	seen := map[string]bool{}
	ids := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	payload, err := json.Marshal(ids) // 裸数组，不是 {"user_ids":[...]}
	if err != nil {
		return err
	}
	u := c.baseURL + "/kick"
	if c.secret != "" {
		u += "?secret=" + url.QueryEscape(c.secret)
	}
	_, err = c.do(ctx, http.MethodPost, u, payload)
	return err
}

func (c *Client) do(ctx context.Context, method, u string, payload []byte) ([]byte, error) {
	var rdr io.Reader
	if payload != nil {
		rdr = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, err
	}
	// secret 在头里原样发送（对应 hysteria 配置里的 trafficStats.secret）。
	req.Header.Set("Authorization", c.secret)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("Hysteria API 鉴权失败(%d)：检查 hysteria.config.trafficStats.secret 与 hysteria.api_secret 是否一致", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("Hysteria API 404：config.yaml 里没有启用 trafficStats")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Hysteria API %s %s 返回 %d: %s", method, u, resp.StatusCode, truncate(body, 200))
	}
	return body, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
