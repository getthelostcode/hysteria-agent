// Package upgrade 实现 Agent 自升级：下载 → 校验 sha256 → 验签(ed25519) → 冒烟测试 → 原子替换 → 重启。
//
// 安全约定（缺一不可，任何一步失败都必须放弃升级并保留原二进制）：
//  1. 只信任内置/配置里声明的 ed25519 公钥；
//  2. 签名的对象是 sha256(payload) 的 hex 字符串（见 DESIGN.md「自升级签名」）；
//  3. 新二进制落盘前先 chmod 0755 并 shell 级冒烟测试（`hysteria-agent version`），
//     跑不起来就绝不替换——把"升级成功但服务起不来"这种最坏情况挡在门外；
//  4. 替换用同目录 rename（POSIX 原子），旧二进制留备份便于人工回滚。
package upgrade

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
	"github.com/getthelostcode/hysteria-agent/internal/version"
)

// Manifest 是一次升级的描述（来自 upgrade_agent 指令的 params，或控制面主动下发的 UpgradeOffer）。
type Manifest struct {
	Version     string `json:"version"`
	URL         string `json:"url"`
	SHA256      string `json:"sha256"`
	Signature   string `json:"signature"` // base64 或 hex 的 ed25519 签名
	Size        int64  `json:"size,omitempty"`
	Force       bool   `json:"force,omitempty"`
	Notes       string `json:"notes,omitempty"`
	MinVersion  string `json:"min_version,omitempty"`
	RestartMode string `json:"restart_mode,omitempty"`
}

// Upgrader 执行升级。
type Upgrader struct {
	cfg     *config.Config
	log     *slog.Logger
	m       *metrics.Metrics
	client  *http.Client
	pubkeys []ed25519.PublicKey

	// RestartHook 仅用于测试：替换真实的重启动作（否则测试会把自己的进程 exec 掉）。
	RestartHook func(mode string) error

	// PreRestart 在重启前调用（Agent 注入：先把被守护的 Hysteria 优雅停掉）。
	//
	// 为什么必须有：exec 模式下 syscall.Exec 只替换当前进程映像，**不会**带走已经
	// fork 出去的 Hysteria 子进程；不停掉它就会变成孤儿继续占着 443，
	// 新进程再启动 Hysteria 直接端口冲突并进入崩溃重启循环（E2E 里真实踩到）。
	PreRestart func(ctx context.Context) error
}

// New 构建 Upgrader。client 必须已经带好 TLS pinning（由 agent 装配时注入）。
func New(cfg *config.Config, log *slog.Logger, m *metrics.Metrics, client *http.Client) (*Upgrader, error) {
	keys, err := LoadPublicKeys(cfg.Upgrade.Pubkeys, version.UpgradePublicKey)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("没有可用的升级验签公钥（upgrade.pubkeys 为空且编译期未注入）")
	}
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &Upgrader{cfg: cfg, log: log, m: m, client: client, pubkeys: keys}, nil
}

// LoadPublicKeys 解析 base64 公钥列表（可为空字符串，跳过）。
func LoadPublicKeys(extra []string, builtin string) ([]ed25519.PublicKey, error) {
	var out []ed25519.PublicKey
	all := append([]string{}, extra...)
	if strings.TrimSpace(builtin) != "" {
		all = append(all, builtin)
	}
	for _, s := range all {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			// 也接受 hex 形式，方便 openssl 生成
			raw2, err2 := hex.DecodeString(s)
			if err2 != nil {
				return nil, fmt.Errorf("公钥 %q 既不是 base64 也不是 hex: %v", s, err)
			}
			raw = raw2
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("公钥长度应为 %d 字节，得到 %d", ed25519.PublicKeySize, len(raw))
		}
		out = append(out, ed25519.PublicKey(raw))
	}
	return out, nil
}

// VerifyPayload 校验 payload 的 sha256 与 ed25519 签名。
func (u *Upgrader) VerifyPayload(payload []byte, wantSHA256, signature string) error {
	sum := sha256.Sum256(payload)
	got := hex.EncodeToString(sum[:])
	if wantSHA256 != "" && !strings.EqualFold(strings.TrimSpace(wantSHA256), got) {
		return fmt.Errorf("sha256 不匹配：期望 %s，实际 %s", wantSHA256, got)
	}
	if strings.TrimSpace(signature) == "" {
		return fmt.Errorf("缺少签名，拒绝升级")
	}
	sig, err := decodeSignature(signature)
	if err != nil {
		return err
	}
	// 签名对象是 sha256 的 hex 字符串（控制面用同一规则生成）。
	msg := []byte(got)
	for _, pk := range u.pubkeys {
		if ed25519.Verify(pk, msg, sig) {
			return nil
		}
		// 兼容直接对文件内容签名的实现
		if ed25519.Verify(pk, payload, sig) {
			return nil
		}
	}
	return fmt.Errorf("ed25519 验签失败（%d 个公钥都不匹配），拒绝升级", len(u.pubkeys))
}

func decodeSignature(s string) ([]byte, error) {
	trimmed := strings.TrimSpace(s)
	if b, err := base64.StdEncoding.DecodeString(trimmed); err == nil && len(b) == ed25519.SignatureSize {
		return b, nil
	}
	if b, err := hex.DecodeString(trimmed); err == nil && len(b) == ed25519.SignatureSize {
		return b, nil
	}
	return nil, fmt.Errorf("签名格式非法（需 %d 字节的 base64 或 hex）", ed25519.SignatureSize)
}

// Result 是升级执行结果（回传给控制面）。
type Result struct {
	Version    string   `json:"version"`
	OldVersion string   `json:"old_version"`
	SHA256     string   `json:"sha256"`
	Backup     string   `json:"backup,omitempty"`
	Steps      []string `json:"steps"`
	Restarted  bool     `json:"restarted"`
	BinaryPath string   `json:"binary_path"`
}

// Run 执行完整升级流程。ctx 结束时立即放弃。
func (u *Upgrader) Run(ctx context.Context, mf Manifest) (*Result, error) {
	res := &Result{Version: mf.Version, OldVersion: version.Version, BinaryPath: u.cfg.Upgrade.BinaryPath}
	step := func(format string, args ...any) {
		res.Steps = append(res.Steps, fmt.Sprintf(format, args...))
		u.log.Info("升级步骤", slog.String("step", fmt.Sprintf(format, args...)))
	}
	fail := func(err error) (*Result, error) {
		u.m.UpgradeTotal.WithLabelValues(classify(err)).Inc()
		return res, err
	}

	if !u.cfg.Upgrade.Enabled {
		return fail(fmt.Errorf("升级未启用（upgrade.enabled=false）"))
	}
	if mf.URL == "" || mf.SHA256 == "" || mf.Signature == "" {
		return fail(fmt.Errorf("升级清单不完整：url/sha256/signature 都必须提供"))
	}
	if mf.Version != "" && mf.Version == version.Version && !mf.Force {
		return fail(fmt.Errorf("当前版本已经是 %s（如需强制覆盖请带 force=true）", mf.Version))
	}
	if err := os.MkdirAll(u.cfg.Upgrade.WorkDir, 0o700); err != nil {
		return fail(fmt.Errorf("创建升级工作目录失败: %w", err))
	}

	// 1) 下载
	payload, err := u.download(ctx, mf.URL)
	if err != nil {
		return fail(fmt.Errorf("下载失败: %w", err))
	}
	step("下载完成 %d 字节", len(payload))
	if mf.Size > 0 && int64(len(payload)) != mf.Size {
		return fail(fmt.Errorf("文件大小不符：期望 %d，实际 %d", mf.Size, len(payload)))
	}

	// 2) 校验 + 验签
	if err := u.VerifyPayload(payload, mf.SHA256, mf.Signature); err != nil {
		return fail(err)
	}
	sum := sha256.Sum256(payload)
	res.SHA256 = hex.EncodeToString(sum[:])
	step("sha256 与 ed25519 验签通过")

	// 3) 落盘 + 冒烟测试
	newPath := filepath.Join(u.cfg.Upgrade.WorkDir, fmt.Sprintf("hysteria-agent.new.%s", sanitizeVer(mf.Version)))
	if err := os.WriteFile(newPath, payload, 0o755); err != nil {
		return fail(fmt.Errorf("写入新二进制失败: %w", err))
	}
	if err := os.Chmod(newPath, 0o755); err != nil {
		return fail(err)
	}
	if err := u.smokeTest(ctx, newPath); err != nil {
		return fail(err)
	}
	step("冒烟测试通过（新二进制可执行）")

	// 4) 备份 + 原子替换（必须同目录 rename，跨设备 rename 会失败）
	backup := filepath.Join(u.cfg.Upgrade.WorkDir,
		fmt.Sprintf("hysteria-agent.backup.%s.%s", sanitizeVer(version.Version), time.Now().UTC().Format("20060102T150405")))
	if _, err := os.Stat(u.cfg.Upgrade.BinaryPath); err == nil {
		if err := copyFile(u.cfg.Upgrade.BinaryPath, backup, 0o755); err != nil {
			return fail(fmt.Errorf("备份当前二进制失败: %w", err))
		}
		res.Backup = backup
		step("已备份原二进制到 %s", backup)
	} else {
		step("没有旧二进制可备份（首次安装？）")
	}
	target := u.cfg.Upgrade.BinaryPath
	tmpInTargetDir := target + ".new.tmp"
	if err := copyFile(newPath, tmpInTargetDir, 0o755); err != nil {
		return fail(fmt.Errorf("准备替换文件失败: %w", err))
	}
	if err := os.Rename(tmpInTargetDir, target); err != nil {
		os.Remove(tmpInTargetDir)
		return fail(fmt.Errorf("原子替换二进制失败（目标与升级目录是否在同一文件系统？）: %w", err))
	}
	step("已原子替换 %s", target)

	// 5) 重启
	if u.PreRestart != nil {
		if err := u.PreRestart(ctx); err != nil {
			u.log.Warn("重启前停止被守护进程失败（继续尝试重启）", slog.String("error", err.Error()))
		}
	}
	if err := u.restart(ctx, mf.RestartMode); err != nil {
		// 替换已成功，但重启失败：明确告知，避免"看起来升级成功了其实没生效"。
		return fail(fmt.Errorf("二进制已替换，但重启失败: %w（可用 %s 手工恢复）", err, res.Backup))
	}
	res.Restarted = true
	step("已触发重启")
	u.m.UpgradeTotal.WithLabelValues("ok").Inc()
	return res, nil
}

func (u *Upgrader) download(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, secs(u.cfg.Upgrade.DownloadTimeoutSeconds))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", u.cfg.Control.UserAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	limit := int64(64 << 20)
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}

// smokeTest 跑 `newbinary version`，确认新二进制在本机能跑起来。
func (u *Upgrader) smokeTest(ctx context.Context, path string) error {
	timeout := secs(u.cfg.Upgrade.SmokeTestSeconds)
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := exec.CommandContext(cctx, path, "version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("新二进制冒烟测试失败: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	if !strings.Contains(string(out), "hysteria-agent") {
		return fmt.Errorf("新二进制输出异常，拒绝替换: %q", strings.TrimSpace(string(out)))
	}
	return nil
}

// restart 触发自身重启。systemd 模式用 --no-block 让 systemd 接管（避免自己杀掉自己的重启任务）。
func (u *Upgrader) restart(ctx context.Context, override string) error {
	mode := override
	if mode == "" {
		mode = u.cfg.Upgrade.RestartMode
	}
	if u.RestartHook != nil {
		return u.RestartHook(mode)
	}
	switch mode {
	case "systemd":
		if _, err := exec.LookPath("systemctl"); err != nil {
			u.log.Warn("没有 systemctl，退回 exec 方式重启")
			return u.execSelf()
		}
		cmd := exec.Command("systemctl", "--no-block", "restart", u.cfg.Upgrade.ServiceName)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("systemctl restart %s 启动失败: %w", u.cfg.Upgrade.ServiceName, err)
		}
		// 不 Wait：systemd 会把当前进程杀掉，等待没有意义。
		go func() { _ = cmd.Wait() }()
		return nil
	case "exec":
		return u.execSelf()
	default:
		return fmt.Errorf("未知的 upgrade.restart_mode: %s", mode)
	}
}

func (u *Upgrader) execSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	u.log.Info("exec 自身以加载新二进制", slog.String("path", exe))
	return syscall.Exec(exe, os.Args, os.Environ())
}

// ---------------------------- 工具 ----------------------------

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, perm)
}

// secs 把配置里的秒数（float64）转成 time.Duration。
func secs(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }

func sanitizeVer(v string) string {
	if v == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func classify(err error) string {
	if err == nil {
		return "ok"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "验签"):
		return "verify_failed"
	case strings.Contains(s, "sha256"):
		return "hash_mismatch"
	case strings.Contains(s, "下载"):
		return "download_failed"
	case strings.Contains(s, "冒烟"):
		return "smoke_test_failed"
	case strings.Contains(s, "重启"):
		return "restart_failed"
	default:
		return "failed"
	}
}

// ParseManifest 从指令参数（JSON）解析升级清单，也支持 UpgradeOffer。
func ParseManifest(params []byte) (Manifest, error) {
	var mf Manifest
	if len(params) == 0 {
		return mf, fmt.Errorf("缺少升级参数")
	}
	if err := json.Unmarshal(params, &mf); err != nil {
		return mf, fmt.Errorf("解析升级参数失败: %w", err)
	}
	return mf, nil
}

// SignForTest 用私钥按同一规则签名（单元测试/控制面示例用）。
func SignForTest(priv ed25519.PrivateKey, payload []byte) string {
	sum := sha256.Sum256(payload)
	return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(hex.EncodeToString(sum[:]))))
}
