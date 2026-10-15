package cp

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/getthelostcode/hysteria-agent/gen/agentpb"
	"github.com/getthelostcode/hysteria-agent/internal/version"
	"github.com/getthelostcode/hysteria-agent/internal/wire"
)

// SHA256Hex 返回 payload 的 sha256（hex），与 Agent 侧的校验口径一致。
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// adminAuth 校验 /admin/* 的 Bearer 令牌（常数时间比较）。
func (s *Server) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if got == "" {
			got = strings.TrimSpace(r.Header.Get("X-Admin-Token"))
		}
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.AdminToken)) != 1 {
			writeErr(w, http.StatusUnauthorized, "缺少或错误的 admin 令牌（Authorization: Bearer <admin_token>）")
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// handleAdmin 分发 /admin/* 路由。
//
//	GET    /admin/nodes                        节点列表
//	GET    /admin/nodes/{id}                   节点详情
//	DELETE /admin/nodes/{id}                   吊销并删除节点
//	POST   /admin/nodes/{id}/config            下发配置（body=YAML/JSON）
//	GET    /admin/nodes/{id}/events?limit=200  最近事件（倒序）
//	GET    /admin/nodes/{id}/telemetry         最近遥测快照
//	POST   /admin/nodes/{id}/commands?type=x   下发白名单指令
//	GET    /admin/tokens                       令牌列表
//	POST   /admin/tokens                       新建令牌 {name,ttl_seconds,max_uses}
//	DELETE /admin/tokens/{token}               吊销令牌
func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	switch {
	case len(parts) == 1 && parts[0] == "nodes":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		nodes := s.store.Nodes()
		out := make([]map[string]any, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, nodeView(n))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"count": len(out), "nodes": out, "server_time_ms": time.Now().UnixMilli(),
		})

	case len(parts) >= 2 && parts[0] == "nodes":
		id := parts[1]
		switch {
		case len(parts) == 2:
			switch r.Method {
			case http.MethodGet:
				n, ok := s.store.Node(id)
				if !ok {
					writeErr(w, http.StatusNotFound, "节点不存在")
					return
				}
				writeJSON(w, http.StatusOK, nodeView(n))
			case http.MethodDelete:
				if !s.store.DeleteNode(id) {
					writeErr(w, http.StatusNotFound, "节点不存在")
					return
				}
				s.log.Warn("节点已吊销", "node_id", id)
				writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "node_id": id,
					"note": "node_secret 由主密钥派生，删除记录即等于立即吊销"})
			default:
				methodNotAllowed(w, "GET, DELETE")
			}
		case len(parts) == 3 && parts[2] == "config" && r.Method == http.MethodPost:
			s.adminPushConfig(w, r, id)
		case len(parts) == 3 && parts[2] == "commands" && r.Method == http.MethodPost:
			s.adminCommand(w, r, id)
		case len(parts) == 3 && parts[2] == "events" && r.Method == http.MethodGet:
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			evs, err := s.store.Events(id, limit)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"count": len(evs), "events": evs})
		case len(parts) == 3 && parts[2] == "telemetry" && r.Method == http.MethodGet:
			n, ok := s.store.Node(id)
			if !ok {
				writeErr(w, http.StatusNotFound, "节点不存在")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"node_id": id, "metrics": n.LastMetrics, "heartbeat": n.LastHeartbeat,
				"connected": n.Connected, "last_seen_ms": n.LastSeenMs,
			})
		default:
			writeErr(w, http.StatusNotFound, "未知的 /admin 路径: "+r.URL.Path)
		}

	case len(parts) == 1 && parts[0] == "tokens":
		switch r.Method {
		case http.MethodGet:
			toks := s.store.Tokens()
			out := make([]map[string]any, 0, len(toks))
			for _, t := range toks {
				out = append(out, map[string]any{
					"token": mask(t.Token), "name": t.Name, "created_ms": t.CreatedMs,
					"expires_ms": t.ExpiresMs, "max_uses": t.MaxUses, "uses": t.Uses,
					"used_by": t.UsedBy, "revoked": t.Revoked, "usable": t.Usable(time.Now().UnixMilli()) == nil,
				})
			}
			writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "tokens": out})
		case http.MethodPost:
			var body struct {
				Name       string `json:"name"`
				TTLSeconds int    `json:"ttl_seconds"`
				MaxUses    int    `json:"max_uses"`
			}
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body)
			t := s.store.NewToken(body.Name, time.Duration(body.TTLSeconds)*time.Second, body.MaxUses)
			s.log.Info("生成注册令牌", "name", t.Name, "token", mask(t.Token), "max_uses", t.MaxUses)
			writeJSON(w, http.StatusOK, map[string]any{
				"token": t.Token, "name": t.Name, "created_ms": t.CreatedMs,
				"expires_ms": t.ExpiresMs, "max_uses": t.MaxUses,
				"note": "请立刻保存：列表接口只返回掩码",
			})
		default:
			methodNotAllowed(w, "GET, POST")
		}

	case len(parts) == 2 && parts[0] == "tokens" && r.Method == http.MethodDelete:
		if !s.store.RevokeToken(parts[1]) {
			writeErr(w, http.StatusNotFound, "令牌不存在（注意：需要用完整 token，不能用掩码）")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"revoked": true})

	default:
		writeErr(w, http.StatusNotFound, "未知的 /admin 路径: "+r.URL.Path)
	}
}

func (s *Server) adminPushConfig(w http.ResponseWriter, r *http.Request, id string) {
	if _, ok := s.store.Node(id); !ok {
		writeErr(w, http.StatusNotFound, "节点不存在")
		return
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil || len(payload) == 0 {
		writeErr(w, http.StatusBadRequest, "请求体为空（应直接放 Hysteria 配置正文）")
		return
	}
	q := r.URL.Query()
	wait := parseDuration(q.Get("wait"), 25*time.Second)
	out, err := s.PushConfig(id, payload, q.Get("format"), q.Get("revision"), q.Get("reason"),
		q.Get("dry_run") == "1" || q.Get("dry_run") == "true", wait)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) adminCommand(w http.ResponseWriter, r *http.Request, id string) {
	if _, ok := s.store.Node(id); !ok {
		writeErr(w, http.StatusNotFound, "节点不存在")
		return
	}
	q := r.URL.Query()
	name := firstNonEmpty(q.Get("type"), q.Get("command"))
	if name == "" {
		writeErr(w, http.StatusBadRequest, "缺少 type 参数。可用指令: "+strings.Join(wire.CommandWhitelist(), ", "))
		return
	}
	ctype, err := wire.ParseCommandType(name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error()+". 可用指令: "+strings.Join(wire.CommandWhitelist(), ", "))
		return
	}
	params, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if len(params) == 0 {
		params = []byte("{}")
	}
	if !json.Valid(params) {
		writeErr(w, http.StatusBadRequest, "params 必须是合法 JSON")
		return
	}
	wait := parseDuration(q.Get("wait"), 30*time.Second)
	out, err := s.Command(id, q.Get("id"), ctype, params, wait)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleMetrics 输出 Prometheus 文本指标。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := s.metrics
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes := s.store.Nodes()
	online, offline, revoked := 0, 0, 0
	for _, n := range nodes {
		switch {
		case n.Revoked:
			revoked++
		case n.Connected:
			online++
		default:
			offline++
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP cp_nodes_total 已注册节点数（按状态）\n# TYPE cp_nodes_total gauge\n")
	fmt.Fprintf(w, "cp_nodes_total{status=\"online\"} %d\n", online)
	fmt.Fprintf(w, "cp_nodes_total{status=\"offline\"} %d\n", offline)
	fmt.Fprintf(w, "cp_nodes_total{status=\"revoked\"} %d\n", revoked)
	fmt.Fprintf(w, "# HELP cp_enroll_total 注册尝试\n# TYPE cp_enroll_total counter\n")
	fmt.Fprintf(w, "cp_enroll_total{result=\"ok\"} %d\n", m.EnrollTotal)
	fmt.Fprintf(w, "cp_enroll_total{result=\"failed\"} %d\n", m.EnrollFailed)
	fmt.Fprintf(w, "# HELP cp_control_ws_total 控制通道连接/拒绝次数\n# TYPE cp_control_ws_total counter\n")
	fmt.Fprintf(w, "cp_control_ws_total{result=\"accepted\"} %d\n", m.WSConnected)
	fmt.Fprintf(w, "cp_control_ws_total{result=\"rejected\"} %d\n", m.WSRejected)
	fmt.Fprintf(w, "# HELP cp_messages_total 收到的消息\n# TYPE cp_messages_total counter\n")
	fmt.Fprintf(w, "cp_messages_total{kind=\"heartbeat\"} %d\n", m.Heartbeats)
	fmt.Fprintf(w, "cp_messages_total{kind=\"duplicate\"} %d\n", m.Duplicates)
	fmt.Fprintf(w, "# HELP cp_commands_total 指令下发/回执\n# TYPE cp_commands_total counter\n")
	fmt.Fprintf(w, "cp_commands_total{stage=\"sent\"} %d\n", m.CommandsSent)
	fmt.Fprintf(w, "cp_commands_total{stage=\"result\"} %d\n", m.CommandsDone)
	fmt.Fprintf(w, "# HELP cp_config_push_total 配置下发\n# TYPE cp_config_push_total counter\n")
	fmt.Fprintf(w, "cp_config_push_total{result=\"sent\"} %d\n", m.PushesSent)
	fmt.Fprintf(w, "cp_config_push_total{result=\"applied\"} %d\n", m.PushApplied)
	fmt.Fprintf(w, "cp_config_push_total{result=\"failed\"} %d\n", m.PushFailed)
	fmt.Fprintf(w, "# HELP cp_bridge_total 桥接到 Laravel 控制面的转发次数\n# TYPE cp_bridge_total counter\n")
	fmt.Fprintf(w, "cp_bridge_total{kind=\"heartbeat\"} %d\n", m.BridgeHeartbeats)
	fmt.Fprintf(w, "cp_bridge_total{kind=\"metrics\"} %d\n", m.BridgeMetrics)
	fmt.Fprintf(w, "cp_bridge_total{kind=\"logs\"} %d\n", m.BridgeLogs)
	fmt.Fprintf(w, "cp_bridge_total{kind=\"diagnostics\"} %d\n", m.BridgeDiags)
	fmt.Fprintf(w, "# HELP cp_bridge_failures_total 桥接转发失败/丢弃\n# TYPE cp_bridge_failures_total counter\n")
	fmt.Fprintf(w, "cp_bridge_failures_total{result=\"failed\"} %d\n", m.BridgeFailed)
	fmt.Fprintf(w, "cp_bridge_failures_total{result=\"dropped\"} %d\n", m.BridgeDropped)
	fmt.Fprintf(w, "# HELP cp_info 控制面信息\n# TYPE cp_info gauge\n")
	fmt.Fprintf(w, "cp_info{version=%q} 1\n", version.Version)
	for _, n := range nodes {
		last := int64(0)
		if n.LastSeenMs > 0 {
			last = (time.Now().UnixMilli() - n.LastSeenMs) / 1000
		}
		fmt.Fprintf(w, "cp_node_last_seen_seconds{node=%q,name=%q} %d\n", n.ID, n.Name, last)
		if n.LastMetrics != nil {
			fmt.Fprintf(w, "cp_node_cpu_percent{node=%q} %g\n", n.ID, n.LastMetrics.CPUPct)
			fmt.Fprintf(w, "cp_node_mem_percent{node=%q} %g\n", n.ID, n.LastMetrics.MemUsedPct)
			fmt.Fprintf(w, "cp_node_disk_percent{node=%q} %g\n", n.ID, n.LastMetrics.DiskUsedPct)
			fmt.Fprintf(w, "cp_node_hysteria_online{node=%q} %d\n", n.ID, n.LastMetrics.HyOnline)
		}
	}
}

// nodeView 是给 API/仪表盘用的节点视图（把"在线"这种派生字段也算好）。
func nodeView(n *Node) map[string]any {
	v := map[string]any{
		"node_id": n.ID, "name": n.Name, "hostname": n.Hostname,
		"os": n.OS, "arch": n.Arch, "kernel": n.Kernel,
		"agent_version": n.AgentVersion, "hysteria_version": n.HysteriaVersion,
		"capabilities": n.Caps, "connected": n.Connected, "revoked": n.Revoked,
		"enrolled_at_ms": n.EnrolledAt, "last_seen_ms": n.LastSeenMs,
		"last_seen_human": HumanTime(n.LastSeenMs), "last_seen_ago": since(n.LastSeenMs),
		"connected_since_ms": n.ConnectedSinceMs, "connects": n.Connects,
		"acked_seq": n.AckedSeq, "received_msgs": n.ReceivedMsgs, "duplicate_msgs": n.DuplicateMsgs,
		"config_revision": n.ConfigRevision, "config_sha256": n.ConfigSHA256,
		"config_state": n.ConfigState, "config_error": n.ConfigError,
		"last_heartbeat": n.LastHeartbeat, "last_metrics": n.LastMetrics,
		"uptime_seconds": 0,
	}
	if n.LastHeartbeat != nil {
		v["uptime_seconds"] = n.LastHeartbeat.UptimeSeconds
		v["ready"] = n.LastHeartbeat.Ready
		v["hysteria_running"] = n.LastHeartbeat.HysteriaRun
		v["pending_events"] = n.LastHeartbeat.PendingEvents
	}
	return v
}

func summarizeMetrics(m *agentpb.MetricBatch) *MetricsInfo {
	s := m.GetSample()
	h := m.GetHysteria()
	info := &MetricsInfo{TSMs: s.GetTsMs()}
	if info.TSMs == 0 {
		info.TSMs = time.Now().UnixMilli()
	}
	info.CPUPct = s.GetCpuPercent()
	info.MemUsedPct = s.GetMemPercent()
	info.DiskUsedPct = s.GetDiskPercent()
	info.NetRxBytes = s.GetNetRxBytes()
	info.NetTxBytes = s.GetNetTxBytes()
	info.HyOnline = int64(h.GetActiveUsers())
	info.HyTx = int64(h.GetTxBytes())
	info.HyRx = int64(h.GetRxBytes())
	if m.GetAgentVersion() != "" || m.GetConfigRevision() != "" {
		info.Extra = map[string]string{"agent_version": m.GetAgentVersion(), "config_revision": m.GetConfigRevision()}
	}
	return info
}

func parseDuration(v string, def time.Duration) time.Duration {
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	return def
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeErr(w, http.StatusMethodNotAllowed, "方法不允许，允许: "+allow)
}

// dashboardHTML 是一个零依赖的只读仪表盘（令牌存 localStorage，不落 cookie）。
const dashboardHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hysteria 控制面</title>
<style>
 :root{color-scheme:dark light}
 body{font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;margin:0;background:#0f1115;color:#e6e6e6}
 header{display:flex;gap:12px;align-items:center;padding:14px 18px;background:#161a21;border-bottom:1px solid #262c36;position:sticky;top:0}
 h1{font-size:15px;margin:0;font-weight:600;letter-spacing:.4px}
 input{background:#0d1014;border:1px solid #2b323d;color:#e6e6e6;border-radius:6px;padding:6px 9px;font:inherit;min-width:280px}
 button{background:#1f6feb;border:0;color:#fff;border-radius:6px;padding:6px 12px;font:inherit;cursor:pointer}
 button.ghost{background:#21262d}
 main{padding:16px 18px}
 table{width:100%;border-collapse:collapse;font-size:13px}
 th,td{text-align:left;padding:7px 10px;border-bottom:1px solid #21262d;vertical-align:top}
 th{color:#8b949e;font-weight:500;position:sticky;top:53px;background:#0f1115}
 .ok{color:#3fb950}.bad{color:#f85149}.warn{color:#d29922}.dim{color:#8b949e}
 code{background:#161b22;padding:1px 5px;border-radius:4px}
 .cards{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:14px}
 .card{background:#161a21;border:1px solid #262c36;border-radius:8px;padding:10px 14px;min-width:120px}
 .card b{display:block;font-size:20px}
 #msg{padding:4px 0;color:#8b949e;font-size:12px}
</style>
</head>
<body>
<header>
  <h1>Hysteria 控制面</h1>
  <input id="tok" type="password" placeholder="admin_token（只存在浏览器本地）">
  <button onclick="save()">保存并刷新</button>
  <button class="ghost" onclick="load()">刷新</button>
  <span id="msg"></span>
</header>
<main>
  <div class="cards" id="cards"></div>
  <div id="nodes"></div>
</main>
<script>
const $ = s => document.querySelector(s);
function save(){ localStorage.setItem('cp_admin_token', $('#tok').value.trim()); load(); }
async function api(p, opt){
  const t = localStorage.getItem('cp_admin_token') || '';
  const r = await fetch(p, Object.assign({headers:{Authorization:'Bearer '+t,'Content-Type':'application/json'}}, opt||{}));
  if(!r.ok){ throw new Error(r.status + ' ' + (await r.text()).slice(0,200)); }
  return r.json();
}
function ago(ms){ if(!ms) return '-'; const d=(Date.now()-ms)/1000;
  if(d<60) return '刚刚'; if(d<3600) return Math.floor(d/60)+' 分钟前';
  if(d<86400) return Math.floor(d/3600)+' 小时前'; return Math.floor(d/86400)+' 天前'; }
async function load(){
  $('#tok').value = localStorage.getItem('cp_admin_token') || '';
  try{
    const d = await api('/admin/nodes');
    const ns = d.nodes || [];
    const on = ns.filter(n=>n.connected).length;
    $('#cards').innerHTML = ['节点 '+ns.length, '在线 '+on, '离线 '+(ns.length-on)]
      .map((t,i)=>'<div class="card"><span class="dim">'+t.split(' ')[0]+'</span><b'+(i===1?' class="ok"':'')+'>'+t.split(' ')[1]+'</b></div>').join('');
    $('#nodes').innerHTML = '<table><thead><tr><th>节点</th><th>状态</th><th>最近心跳</th><th>版本</th><th>配置</th><th>遥测</th><th>seq</th></tr></thead><tbody>' +
      ns.map(n=>{
        const st = n.revoked?'<span class="bad">已吊销</span>':(n.connected?'<span class="ok">在线</span>':'<span class="warn">离线</span>');
        const hy = n.hysteria_running===undefined?'<span class="dim">-</span>':(n.hysteria_running?'<span class="ok">running</span>':'<span class="bad">stopped</span>');
        const cfg = n.config_state ? '<code>'+n.config_state+'</code> '+(n.config_revision?n.config_revision:'') +
          (n.config_error?'<br><span class="bad">'+n.config_error.slice(0,80)+'</span>':'') : '<span class="dim">-</span>';
        const m = n.last_metrics ? ('cpu '+ (n.last_metrics.cpu_percent||0).toFixed(1) + '% · mem ' + (n.last_metrics.mem_used_percent||0).toFixed(0) + '% · disk ' + (n.last_metrics.disk_used_percent||0).toFixed(0) + '%') : '<span class="dim">-</span>';
        return '<tr><td><b>'+n.name+'</b><br><span class="dim">'+n.node_id+'<br>'+n.hostname+' · '+n.os+'/'+n.arch+'</span></td>'+
          '<td>'+st+'<br>'+hy+'</td><td>'+ago(n.last_seen_ms)+'<br><span class="dim">uptime '+(n.uptime_seconds||0)+'s</span></td>'+
          '<td>agent '+(n.agent_version||'-')+'<br><span class="dim">hysteria '+(n.hysteria_version||'-')+'</span></td>'+
          '<td>'+cfg+'</td><td>'+m+'</td><td>'+n.acked_seq+'</td></tr>';
      }).join('') + '</tbody></table>';
    $('#msg').textContent = '更新于 ' + new Date().toLocaleTimeString();
  }catch(e){ $('#msg').innerHTML = '<span class="bad">'+e.message+'</span>'; }
}
load(); setInterval(load, 5000);
</script>
</body>
</html>
`
