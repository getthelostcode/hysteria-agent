// agent-sign 是控制面侧的自升级签名工具（参考实现）。
//
// 用法：
//
//	agent-sign keygen -out /etc/hysteria/upgrade.key
//	    生成 ed25519 密钥对；私钥写 <out>（0600），公钥以 base64 打印到 stdout。
//	    公钥放到 Agent 的 upgrade.pubkeys，或用 -ldflags 注入 version.UpgradePublicKey。
//
//	agent-sign sign -key /etc/hysteria/upgrade.key -file ./hysteria-agent
//	    输出 {"version":..,"sha256":..,"signature":..,"size":..} 的 JSON，
//	    可直接作为 upgrade_agent 指令的 params 使用。
//
// 签名对象是 **sha256 十六进制字符串**（不是原始文件的二进制摘要），
// 与 Agent 的 upgrade.VerifyPayload 完全一致：这样控制面在只有哈希的场合（例如
// 数据库里存了哈希）也能完成签名校验。
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "keygen":
		keygen(os.Args[2:])
	case "sign":
		sign(os.Args[2:])
	case "pubkey":
		pubkey(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `agent-sign — hysteria-agent 自升级签名工具

  agent-sign keygen -out <private-key-file>
  agent-sign sign   -key <private-key-file> -file <binary> [-version <v>] [-url <url>]
  agent-sign pubkey -key <private-key-file>     # 从私钥导出 base64 公钥`)
}

func keygen(args []string) {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "upgrade.key", "私钥输出路径")
	_ = fs.Parse(args)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*out, []byte(base64.StdEncoding.EncodeToString(priv)), 0o600); err != nil {
		fatal(err)
	}
	fmt.Printf("私钥已写入 %s (0600)\n", *out)
	fmt.Printf("公钥(base64): %s\n", base64.StdEncoding.EncodeToString(pub))
	fmt.Println("把它填到 Agent 的 upgrade.pubkeys，或编译时 -ldflags 注入 version.UpgradePublicKey")
}

func loadKey(path string) ed25519.PrivateKey {
	raw, err := os.ReadFile(path)
	if err != nil {
		fatal(err)
	}
	b, err := base64.StdEncoding.DecodeString(string(trim(string(raw))))
	if err != nil {
		fatal(fmt.Errorf("私钥不是 base64 编码: %w", err))
	}
	if len(b) != ed25519.PrivateKeySize {
		fatal(fmt.Errorf("私钥长度应为 %d 字节，得到 %d", ed25519.PrivateKeySize, len(b)))
	}
	return ed25519.PrivateKey(b)
}

func pubkey(args []string) {
	fs := flag.NewFlagSet("pubkey", flag.ExitOnError)
	key := fs.String("key", "", "私钥路径")
	_ = fs.Parse(args)
	priv := loadKey(*key)
	pub := priv.Public().(ed25519.PublicKey)
	fmt.Println(base64.StdEncoding.EncodeToString(pub))
}

func sign(args []string) {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	key := fs.String("key", "", "私钥路径")
	file := fs.String("file", "", "待签名的二进制")
	version := fs.String("version", "", "版本号（写进清单，便于 Agent 做防降级判断）")
	url := fs.String("url", "", "下载地址（可选，写进清单）")
	notes := fs.String("notes", "", "发布说明（可选）")
	_ = fs.Parse(args)
	if *key == "" || *file == "" {
		usage()
		os.Exit(2)
	}
	priv := loadKey(*key)
	payload, err := os.ReadFile(*file)
	if err != nil {
		fatal(err)
	}
	sum := sha256.Sum256(payload)
	hexSum := hex.EncodeToString(sum[:])
	sig := ed25519.Sign(priv, []byte(hexSum))

	manifest := map[string]any{
		"version":   *version,
		"url":       *url,
		"sha256":    hexSum,
		"signature": base64.StdEncoding.EncodeToString(sig),
		"size":      len(payload),
		"notes":     *notes,
	}
	b, _ := json.MarshalIndent(manifest, "", "  ")
	fmt.Println(string(b))
}

func trim(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "错误: %v\n", err)
	os.Exit(1)
}
