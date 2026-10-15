package upgrade

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getthelostcode/hysteria-agent/internal/config"
	"github.com/getthelostcode/hysteria-agent/internal/metrics"
)

// fakeBinary 是一个能通过冒烟测试（`version` 输出含 "hysteria-agent"）的最小可执行文件。
const fakeBinary = `#!/bin/sh
if [ "$1" = "version" ]; then echo "hysteria-agent 9.9.9 (fake)"; exit 0; fi
echo "fake agent"; exit 0
`

func newTestUpgrader(t *testing.T, pub ed25519.PublicKey) (*Upgrader, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Upgrade.BinaryPath = filepath.Join(dir, "hysteria-agent")
	cfg.Upgrade.WorkDir = filepath.Join(dir, "work")
	cfg.Upgrade.RestartMode = "exec"
	cfg.Upgrade.Pubkeys = []string{base64.StdEncoding.EncodeToString(pub)}
	// 先放一个"旧版本"二进制
	if err := os.WriteFile(cfg.Upgrade.BinaryPath, []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	u, err := New(cfg, logger, metrics.New("test-node"), &http.Client{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	u.RestartHook = func(mode string) error { return nil } // 测试里不真的重启
	return u, dir
}

func TestVerifyPayloadAcceptsValidSignature(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	payload := []byte(fakeBinary)
	sig := SignForTest(priv, payload)
	sum := sha256.Sum256(payload)
	if err := u.VerifyPayload(payload, hex.EncodeToString(sum[:]), sig); err != nil {
		t.Fatalf("合法签名应通过: %v", err)
	}
}

func TestVerifyPayloadRejectsTampered(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	payload := []byte(fakeBinary)
	sig := SignForTest(priv, payload)
	sum := sha256.Sum256(payload)

	// 内容被改：sha256 不匹配
	tampered := append([]byte{}, payload...)
	tampered = append(tampered, "echo hacked\n"...)
	if err := u.VerifyPayload(tampered, hex.EncodeToString(sum[:]), sig); err == nil {
		t.Fatal("内容被篡改必须报错")
	}
	// 签名被换：验签失败
	_, priv2, _ := ed25519.GenerateKey(rand.Reader)
	badSig := SignForTest(priv2, payload)
	if err := u.VerifyPayload(payload, hex.EncodeToString(sum[:]), badSig); err == nil {
		t.Fatal("错误密钥的签名必须报错")
	}
	// 缺签名
	if err := u.VerifyPayload(payload, hex.EncodeToString(sum[:]), ""); err == nil {
		t.Fatal("缺签名必须报错（绝不能退化成不验签）")
	}
}

func TestRunReplacesBinaryAtomically(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	payload := []byte(fakeBinary)
	sum := sha256.Sum256(payload)
	sig := SignForTest(priv, payload)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer srv.Close()

	res, err := u.Run(context.Background(), Manifest{
		Version: "9.9.9", URL: srv.URL, SHA256: hex.EncodeToString(sum[:]), Signature: sig, Size: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("升级应成功: %v", err)
	}
	if !res.Restarted || res.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("结果异常: %+v", res)
	}
	got, err := os.ReadFile(u.cfg.Upgrade.BinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("目标二进制内容不是新版本")
	}
	if res.Backup == "" {
		t.Fatal("应保留旧二进制备份")
	}
	if b, err := os.ReadFile(res.Backup); err != nil || !strings.Contains(string(b), "echo old") {
		t.Fatalf("备份内容不对: %v", err)
	}
	fi, _ := os.Stat(u.cfg.Upgrade.BinaryPath)
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("替换后的权限应为 0755，得到 %v", fi.Mode().Perm())
	}
}

func TestRunRejectsBadSignatureAndKeepsOldBinary(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	payload := []byte(fakeBinary)
	sum := sha256.Sum256(payload)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer srv.Close()

	// 用一个不相干的私钥签名
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	_, err := u.Run(context.Background(), Manifest{
		Version: "9.9.9", URL: srv.URL, SHA256: hex.EncodeToString(sum[:]), Signature: SignForTest(other, payload),
	})
	if err == nil {
		t.Fatal("验签失败必须拒绝升级")
	}
	got, _ := os.ReadFile(u.cfg.Upgrade.BinaryPath)
	if !strings.Contains(string(got), "echo old") {
		t.Fatal("升级被拒绝时不能改动原二进制")
	}
}

func TestRunRejectsMismatchedHash(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	payload := []byte(fakeBinary)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer srv.Close()

	_, err := u.Run(context.Background(), Manifest{
		Version: "9.9.9", URL: srv.URL,
		SHA256:    strings.Repeat("ab", 32), // 错误的 sha256
		Signature: SignForTest(priv, payload),
	})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("sha256 不匹配必须拒绝，得到: %v", err)
	}
}

func TestRunRejectsSmokeTestFailure(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	u, _ := newTestUpgrader(t, pub)
	// 一个"能下载、验签通过，但根本跑不起来"的二进制
	payload := []byte("#!/bin/sh\nexit 3\n")
	sum := sha256.Sum256(payload)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer srv.Close()

	_, err := u.Run(context.Background(), Manifest{
		Version: "9.9.9", URL: srv.URL, SHA256: hex.EncodeToString(sum[:]), Signature: SignForTest(priv, payload),
	})
	if err == nil || !strings.Contains(err.Error(), "冒烟") {
		t.Fatalf("冒烟测试失败必须拒绝替换，得到: %v", err)
	}
	got, _ := os.ReadFile(u.cfg.Upgrade.BinaryPath)
	if !strings.Contains(string(got), "echo old") {
		t.Fatal("冒烟测试失败时不能改动原二进制")
	}
}
