// mockhysteria 是 Hysteria2 服务端的"假人"，用于端到端测试 Agent 的进程管理、
// 配置热更新、回滚、流量采集与踢人。
//
// 行为对齐真实 Hysteria 2 的关键面：
//
//	命令行   : mockhysteria server -c <config.yaml>（与 hysteria 一致）
//	version  : mockhysteria version 输出 v2.mock
//	监听     : 按配置里的 listen 绑一个 UDP + TCP 端口（QUIC 是 UDP，TCP 用来验证端口占用逻辑）
//	本地 API : GET /traffic[?clear=1]、GET /online、POST /kick（Authorization: <secret>，kick 还要 ?secret=）
//	信号     : SIGTERM/SIGINT 优雅退出（exit 0）；SIGKILL 模拟崩溃（非 0 退出码）
//	毒配置   : 配置里出现 poison_start: true 时立刻退出 1，用来测 Agent 的自动回滚
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type fakeState struct {
	mu       sync.Mutex
	secret   string
	users    map[string]*userCounters
	kicked   []string
	kickFile string
	started  time.Time
	requests map[string]int
}

type userCounters struct {
	Tx uint64 `json:"tx"`
	Rx uint64 `json:"rx"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: mockhysteria server -c <config.yaml> | version")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version":
		fmt.Println("Version:\n  v2.mock-1.0")
		os.Exit(0)
	case "server":
		runServer(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

func runServer(args []string) {
	var cfgPath, apiListen, secret, kickFile string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-c", "-config", "--config":
			if i+1 < len(args) {
				cfgPath = args[i+1]
				i++
			}
		case "-api-listen":
			if i+1 < len(args) {
				apiListen = args[i+1]
				i++
			}
		case "-secret":
			if i+1 < len(args) {
				secret = args[i+1]
				i++
			}
		case "-kick-file":
			if i+1 < len(args) {
				kickFile = args[i+1]
				i++
			}
		}
	}
	if apiListen == "" {
		apiListen = os.Getenv("MOCK_HYSTERIA_API")
		if apiListen == "" {
			apiListen = "127.0.0.1:18081"
		}
	}
	if kickFile == "" {
		kickFile = os.Getenv("MOCK_HYSTERIA_KICK_FILE")
	}

	cfg := map[string]any{}
	if cfgPath != "" {
		raw, err := os.ReadFile(cfgPath)
		if err != nil {
			log.Fatalf("读取配置失败: %v", err)
		}
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			log.Fatalf("配置 YAML 解析失败: %v", err)
		}
	}
	raw2, _ := os.ReadFile(cfgPath)

	// 毒配置：模拟"配置校验通过但服务起不来"，用于验证 Agent 的自动回滚。
	if strings.Contains(string(raw2), "poison_start: true") {
		fmt.Fprintln(os.Stderr, "FATAL failed to listen: invalid QUIC config (poison_start)")
		os.Exit(1)
	}

	listen, _ := cfg["listen"].(string)
	if listen == "" {
		listen = ":443"
	}
	if secret == "" {
		if ts, ok := cfg["trafficStats"].(map[string]any); ok {
			secret, _ = ts["secret"].(string)
		}
	}
	if ts, ok := cfg["trafficStats"].(map[string]any); ok {
		if l, _ := ts["listen"].(string); l != "" && os.Getenv("MOCK_HYSTERIA_API") == "" {
			// 优先用业务配置里声明的 API 监听地址（真实 Hysteria 也是这么读的）
			apiListen = l
		}
	}

	st := &fakeState{
		secret: secret, kickFile: kickFile, started: time.Now(),
		requests: map[string]int{},
		users: map[string]*userCounters{
			"alice": {}, "bob": {}, "carol": {},
		},
	}

	// 绑定业务端口（UDP + TCP），与真实 Hysteria 一样：占不到端口就退出非 0。
	udpAddr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatalf("FATAL 解析 listen 失败: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("FATAL failed to listen udp %s: %v", listen, err)
	}
	defer udpConn.Close()
	tcpLn, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("FATAL failed to listen tcp %s: %v", listen, err)
	}
	defer tcpLn.Close()

	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := udpConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = udpConn.WriteToUDP([]byte("mock-quic-ack"), addr)
			_ = n
		}
	}()
	go func() {
		for {
			c, err := tcpLn.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	// 假的流量计数器：每 500ms 给每个用户加一点，模拟真实客户端流量。
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			st.mu.Lock()
			for _, u := range st.users {
				u.Tx += 1024
				u.Rx += 2048
			}
			st.mu.Unlock()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/traffic", func(w http.ResponseWriter, r *http.Request) {
		if !st.auth(w, r) {
			return
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		st.requests["traffic"]++
		clear := r.URL.Query().Get("clear") != ""
		out := map[string]userCounters{}
		for k, v := range st.users {
			out[k] = *v
			if clear {
				v.Tx, v.Rx = 0, 0
			}
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("/online", func(w http.ResponseWriter, r *http.Request) {
		if !st.auth(w, r) {
			return
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		st.requests["online"]++
		out := map[string]int{}
		for k := range st.users {
			out[k] = 1
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("/kick", func(w http.ResponseWriter, r *http.Request) {
		if !st.auth(w, r) {
			return
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		st.requests["kick"]++
		var ids []string
		if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
			http.Error(w, "bad body", 400)
			return
		}
		// 记录被踢用户，测试脚本据此断言"裸 JSON 数组"格式正确。
		st.kicked = append(st.kicked, ids...)
		if st.kickFile != "" {
			b, _ := json.MarshalIndent(map[string]any{"kicked": st.kicked, "at": time.Now()}, "", "  ")
			_ = os.WriteFile(st.kickFile, b, 0o644)
		}
		log.Printf("[hysteria] kicked users: %v", ids)
		writeJSON(w, map[string]any{"ok": true})
	})
	mux.HandleFunc("/_requests", func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		writeJSON(w, st.requests)
	})

	apiSrv := &http.Server{Addr: apiListen, Handler: mux}
	go func() {
		if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("FATAL 本地 API 监听失败 %s: %v", apiListen, err)
		}
	}()

	log.Printf("[hysteria] server started: listen=%s api=%s secret_set=%v config=%s",
		listen, apiListen, secret != "", cfgPath)
	fmt.Printf("%s INFO hysteria server started (mock) listen=%s\n", time.Now().Format(time.RFC3339), listen)

	// 周期性打日志，让 Agent 的日志采集/远程日志有内容可推。
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for range t.C {
			fmt.Printf("%s INFO mock hysteria heartbeat, uptime=%ds\n",
				time.Now().Format(time.RFC3339), int(time.Since(st.started).Seconds()))
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	s := <-sig
	fmt.Printf("%s INFO received %v, shutting down gracefully\n", time.Now().Format(time.RFC3339), s)
	_ = apiSrv.Close()
	os.Exit(0)
}

// auth 校验 Authorization 头；POST /kick 还要求 query 里带同样的 secret
// （真实 Hysteria 的两种校验方式我们两个都实现，测试才可能发现客户端漏发）。
func (st *fakeState) auth(w http.ResponseWriter, r *http.Request) bool {
	st.mu.Lock()
	secret := st.secret
	st.mu.Unlock()
	if secret == "" {
		return true
	}
	if r.Header.Get("Authorization") != secret {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return false
	}
	if r.Method == http.MethodPost && r.URL.Query().Get("secret") != secret {
		http.Error(w, `{"error":"unauthorized (query secret)"}`, http.StatusUnauthorized)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
