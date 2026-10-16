package supervisor

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/logging"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
)

// systemctlShim 造一个假的 systemctl：记录调用，按脚本里声明的行为回答。
func systemctlShim(t *testing.T, reloadOK bool) (string, string) {
	t.Helper()
	dir := t.TempDir()
	callLog := filepath.Join(dir, "systemctl.log")
	reload := "exit 4" // 没有 ExecReload 的单元：systemctl reload 会失败
	if reloadOK {
		reload = "exit 0"
	}
	script := "#!/bin/bash\n" +
		"printf '%s\\n' \"$*\" >> \"" + callLog + "\"\n" +
		"case \"$1\" in\n" +
		"  is-active) echo active ;;\n" +
		"  show) echo 4242 ;;\n" +
		"  reload) " + reload + " ;;\n" +
		"esac\n" +
		"exit 0\n"
	shim := filepath.Join(dir, "systemctl")
	if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
		t.Fatalf("写 systemctl shim: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	return shim, callLog
}

func newSystemdSupervisor(t *testing.T, service string) *Supervisor {
	t.Helper()
	cfg := config.Default()
	cfg.Hysteria.Mode = "systemd"
	cfg.Hysteria.ServiceName = service
	cfg.Hysteria.StartupTimeoutSeconds = 1
	cfg.Hysteria.Restart.InitialSeconds = 0.01

	return New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("nd_supervisor_test"),
		logging.NewHub(4), nil)
}

// 需求：节点把新配置写下去之后，要**重新运行 hysteria-server**。
// systemd 模式下就是 `systemctl restart hysteria-server`（先试 reload，单元没定义就回退 restart）。
func TestReloadInSystemdModeRestartsTheManagedService(t *testing.T) {
	_, callLog := systemctlShim(t, false) // reload 不可用 → 必须回退 restart
	sup := newSystemdSupervisor(t, "hysteria-server")

	if err := sup.Reload(context.Background(), "配置更新 rev=3"); err != nil {
		t.Fatalf("Reload 失败: %v", err)
	}

	calls := readCalls(t, callLog)
	if !containsCall(calls, "reload hysteria-server") {
		t.Fatalf("应先尝试 systemctl reload hysteria-server，实际调用：%v", calls)
	}
	if !containsCall(calls, "restart hysteria-server") {
		t.Fatalf("reload 不可用时必须 restart hysteria-server（用户要求「重新运行 hysteria-server」），实际调用：%v", calls)
	}
}

func TestReloadInSystemdModeKeepsReloadWhenTheUnitSupportsIt(t *testing.T) {
	_, callLog := systemctlShim(t, true) // reload 成功 → 不该再 restart
	sup := newSystemdSupervisor(t, "hysteria-server")

	if err := sup.Reload(context.Background(), "配置更新 rev=4"); err != nil {
		t.Fatalf("Reload 失败: %v", err)
	}

	calls := readCalls(t, callLog)
	if !containsCall(calls, "reload hysteria-server") {
		t.Fatalf("应调用 systemctl reload hysteria-server，实际：%v", calls)
	}
	if containsCall(calls, "restart hysteria-server") {
		t.Fatalf("reload 成功时不该重启，实际：%v", calls)
	}
}

// 白名单：agent 永远不执行任意 systemctl 动词。
func TestSystemctlRefusesVerbsOutsideTheAllowList(t *testing.T) {
	systemctlShim(t, true)
	sup := newSystemdSupervisor(t, "hysteria-server")

	if err := sup.systemctl(context.Background(), "daemon-reload"); err == nil {
		t.Fatal("daemon-reload 不在白名单里，必须被拒绝")
	}
}

func readCalls(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读 systemctl 调用记录失败: %v", err)
	}

	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if strings.TrimSpace(c) == want {
			return true
		}
	}

	return false
}
