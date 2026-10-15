// Package version 保存构建期注入的版本信息。
package version

import "fmt"

// 这些变量通过 -ldflags "-X ..." 在编译时注入（见 Makefile）。
var (
	Version   = "0.1.0"
	Commit    = "dev"
	BuildTime = "unknown"

	// UpgradePublicKey 是自升级验签用的 ed25519 公钥（base64，标准编码）。
	// 内置一份编译期公钥作为根信任；配置里的 upgrade.pubkeys 可以追加信任，
	// 但空值表示"只信任内置密钥"，绝不会退化成"不验签"。
	UpgradePublicKey = ""
)

// String 返回 "版本 (commit, 构建时间)" 形式，用于 status 和 /metrics。
func String() string {
	return fmt.Sprintf("%s (%s, %s)", Version, Commit, BuildTime)
}

// HasBuiltinUpgradeKey 报告编译期是否内置了升级公钥。
func HasBuiltinUpgradeKey() bool { return UpgradePublicKey != "" }
