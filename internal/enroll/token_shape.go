package enroll

import (
	"errors"
	"fmt"
	"strings"
)

// 控制平面（hysteria-control-plane / Laravel 面板）签发的注册令牌形如
// `enr_` + 40 位随机字符 = 44 字符；面板列表里只显示前 12 位掩码（`enr_GmCVlx2W…`），
// 因为服务端只保存 SHA-256 哈希、无法反查明文。
const (
	TokenPrefix     = "enr_"
	TokenBodyLength = 40
	TokenLength     = len(TokenPrefix) + TokenBodyLength // 44
)

// ValidateTokenShape 在**发起网络请求之前**做一次形状校验。
//
// 为什么值得单独做：运维在面板列表上看到的是 12 位掩码，抄去注册会一路走到 TLS 预检 + 注册
// 请求，最后才拿到一句 401；在节点机上这趟往返的成本远高于本地判死。实测（japan2 真实安装）：
// `--token=enr_GmCVlx2W`（12 字符）。控制面侧现在能报出「前 12 位与记录 #17 一致」，
// 但节点机上更该直接告诉运维「你给的是掩码，完整值 44 字符，请用复制按钮」。
//
// 只对 `enr_` 前缀的令牌判死（那是本控制面的格式）：别的控制面可能自定格式，不做无谓拦截。
func ValidateTokenShape(token string) error {
	trimmed := strings.TrimSpace(token)

	if trimmed == "" {
		return errors.New("enroll_token 不能为空（用法: hysteria-agent enroll --token=<token>）")
	}

	if !strings.HasPrefix(trimmed, TokenPrefix) {
		// 非本控制面格式：只提示，不阻断（自建控制面可以有自己的令牌格式）。
		return nil
	}

	// 剪掉常见的手工粘贴残留：省略号、结尾标点。
	body := strings.TrimRight(strings.TrimPrefix(trimmed, TokenPrefix), "….。 \t\r\n")

	if len(body) == TokenBodyLength {
		return nil
	}

	if len(body) < TokenBodyLength {
		return fmt.Errorf(
			"注册令牌被截断了：当前共 %d 字符（enr_ 之后 %d 位），本控制面签发的完整令牌是 %d 字符（enr_ + %d 位随机字符）。\n"+
				"  面板「注册令牌」列表里显示的 %s… 是**掩码前缀**，不是完整令牌 —— 控制面只保存哈希，无法反查明文。\n"+
				"  请到面板 → 注册令牌 → 该行点「重新签发」→「查看刚签发的令牌」→「复制完整令牌」，再重跑本命令。",
			len(trimmed), len(body), TokenLength, TokenBodyLength, trimmed,
		)
	}

	return fmt.Errorf(
		"注册令牌长度不对：共 %d 字符，本控制面签发的是 %d 字符（enr_ + %d 位随机字符）。请确认复制时没有多带内容。",
		len(trimmed), TokenLength, TokenBodyLength,
	)
}
