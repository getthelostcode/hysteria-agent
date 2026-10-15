package cp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

const idChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// NewID 生成 n 个字符的随机 ID（小写字母+数字，URL 安全）。
func NewID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败是极其罕见的系统级问题：退化成时间戳，避免 panic。
		s := hex.EncodeToString([]byte(time.Now().UTC().String()))
		for i := range b {
			b[i] = s[i%len(s)]
		}
		return string(b)
	}
	out := make([]byte, n)
	for i, v := range b {
		out[i] = idChars[int(v)%len(idChars)]
	}
	return string(out)
}

// HexKey 用主密钥做 HMAC-SHA256 派生（HKDF 的简化形式，够用且标准库即可验证）。
func HexKey(master []byte, info string) string {
	mac := hmac.New(sha256.New, master)
	mac.Write([]byte("hysteria-agent/v1/" + info))
	return hex.EncodeToString(mac.Sum(nil))
}

// NewMasterKey 生成 32 字节主密钥。
func NewMasterKey() ([]byte, string) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("无法获取随机数: " + err.Error())
	}
	return b, hex.EncodeToString(b)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func orNow(ms int64) int64 {
	if ms == 0 {
		return time.Now().UnixMilli()
	}
	return ms
}

// HumanTime 是给 CLI/仪表盘用的可读时间。
func HumanTime(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05")
}

// since 返回"多久以前"的简短描述。
func since(ms int64) string {
	if ms == 0 {
		return "-"
	}
	d := time.Since(time.UnixMilli(ms))
	switch {
	case d < 0:
		return "刚刚"
	case d < time.Minute:
		return "刚刚"
	case d < time.Hour:
		return itoa(int(d.Minutes())) + " 分钟前"
	case d < 24*time.Hour:
		return itoa(int(d.Hours())) + " 小时前"
	default:
		return itoa(int(d.Hours()/24)) + " 天前"
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
