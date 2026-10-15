package cp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/getthelostcode/hysteria-agent/internal/cpapi"
	"github.com/getthelostcode/hysteria-agent/internal/creds"
)

// upstreamError 保留 Laravel 的原始状态码与响应体，便于对 Agent 原样透传
// （agent 的 enroll 客户端就是按 401/409 文案做判断的，不能改写）。
type upstreamError struct {
	Status int
	Body   string
	Code   string
}

func (e *upstreamError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("Laravel 拒绝注册：%d %s", e.Status, e.Code)
	}

	return fmt.Sprintf("Laravel 拒绝注册：%d", e.Status)
}

// handleLaravelEnroll 是 identity.authority=laravel 时的注册入口：
// 把 Agent 的注册请求代理给 Laravel，拿到权威身份后建立本地镜像记录。
func (s *Server) handleLaravelEnroll(w http.ResponseWriter, r *http.Request, req EnrollRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.LaravelTimeout())
	defer cancel()

	upstream, node, err := s.enrollViaLaravel(ctx, req)
	if err != nil {
		s.metrics.bump("enroll_failed")

		var up *upstreamError
		if errors.As(err, &up) {
			s.log.Warn("Laravel 拒绝注册", "status", up.Status, "code", up.Code, "hostname", req.Hostname)
			writeUpstreamError(w, up)

			return
		}

		s.log.Error("代理注册失败", "err", err, "hostname", req.Hostname)
		writeErr(w, http.StatusBadGateway, "控制面（Laravel）不可达："+err.Error())

		return
	}

	s.metrics.bump("enroll")
	s.store.AppendEvent(Event{NodeID: node.ID, Kind: "conn", Type: "enrolled",
		Message: fmt.Sprintf("节点注册（身份来自 Laravel）：%s (%s/%s) agent=%s", node.Name, node.OS, node.Arch, node.AgentVersion)})
	s.log.Info("节点已注册（Laravel 权威）", "node_id", node.ID, "name", node.Name,
		"hostname", req.Hostname, "agent", req.AgentVersion, "agent_protocol", req.Protocol,
		"config_revision", upstream.ConfigRevision)

	hbInterval := s.cfg.Heartbeat.IntervalSeconds
	if upstream.HeartbeatIntervalSeconds > 0 {
		hbInterval = upstream.HeartbeatIntervalSeconds
	}
	telemetry := s.cfg.Heartbeat.TelemetrySeconds
	if upstream.TelemetryIntervalSeconds > 0 {
		telemetry = upstream.TelemetryIntervalSeconds
	}

	writeJSON(w, http.StatusOK, EnrollResponse{
		NodeID:     node.ID,
		NodeSecret: upstream.NodeSecret,
		NodeName:   node.Name,
		ControlURL: s.cfg.PublicURL,
		WSPath:     s.cfg.WSPath,
		PinSHA256:  s.cfg.PinSHA256,
		CACertPEM:  s.cfg.CACertPEM,
		// 通道 JWT 的 issuer/audience 由**校验方**（本控制面）决定：agent 会用这两个值
		// 自签，控制通道再按 creds.Default* 校验。透传上游值会导致 audience 不匹配 → 401。
		JWTIssuer:                creds.DefaultIssuer,
		JWTAudience:              creds.DefaultAudience,
		HeartbeatIntervalSeconds: hbInterval,
		TelemetryIntervalSeconds: telemetry,
		ConfigRevision:           upstream.ConfigRevision,
		ServerVersion:            versionString(),
		Message:                  "注册成功（身份由 Laravel 控制面签发）；后续用 credentials.json 里的 node_secret 自签 JWT 连接控制通道",
	})
}

// enrollViaLaravel 调 Laravel 的 enroll 兼容面，并把结果落到本地 state。
func (s *Server) enrollViaLaravel(ctx context.Context, req EnrollRequest) (*cpapi.EnrollResponse, *Node, error) {
	clientCfg := s.cfg.LaravelClientConfig()

	upstream, err := cpapi.Enroll(ctx, clientCfg, s.cfg.Laravel.BaseURL, cpapi.EnrollRequest{
		EnrollToken:     req.EnrollToken,
		NodeName:        req.NodeName,
		Hostname:        req.Hostname,
		OS:              req.OS,
		Arch:            req.Arch,
		Kernel:          req.Kernel,
		MachineID:       req.MachineID,
		BootID:          req.BootID,
		AgentVersion:    req.AgentVersion,
		HysteriaVersion: req.HysteriaVersion,
		// 通道标记：Laravel 侧 nodes.meta.protocol=bridge/v1，面板可据此区分
		// 「Go 控制面桥接」与「直连节点」；agent 自己的 protocol 版本号记在 CP 日志里。
		Protocol: "bridge/v1",
		Replace:  req.Replace,
		// 节点自报的公网出口：Laravel 只会看到本控制面的地址（127.0.0.1），
		// 所以面板要显示真实公网 IP 就必须靠这里透传。
		PublicIP: req.PublicIP,
	})
	if err != nil {
		var apiErr *cpapi.APIError
		if errors.As(err, &apiErr) {
			return nil, nil, &upstreamError{Status: apiErr.StatusCode, Body: apiErr.Body, Code: apiErr.Code}
		}

		return nil, nil, err
	}

	if strings.TrimSpace(upstream.NodeSecret) == "" {
		return nil, nil, errors.New("Laravel 响应缺少 node_secret")
	}

	node, err := s.store.MirrorExternalNode(req, upstream.NodeID, upstream.NodeSecret, AuthorityLaravel, "laravel")
	if err != nil {
		return nil, nil, err
	}

	return upstream, node, nil
}

// writeUpstreamError 原样透传上游状态码与响应体。
func writeUpstreamError(w http.ResponseWriter, up *upstreamError) {
	body := strings.TrimSpace(up.Body)
	if body == "" {
		body = fmt.Sprintf(`{"error":"upstream_error","message":%q}`, up.Error())
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(up.Status)
	_, _ = w.Write([]byte(body))
}
