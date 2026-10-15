// Package cpapi 实现「以节点代理身份」调用 Laravel 控制面的签名 REST 客户端。
//
// 签名规范必须与 Laravel 的 App\Support\SignatureVerifier 逐字节一致
// （见 docs/BRIDGE-TO-CONTROL-PLANE.md §3）：
//
//	canonical = METHOD + "\n" + PATH + "\n" + sha256_hex(rawBody) + "\n" + X-Timestamp + "\n" + X-Nonce
//	signature = hex(hmac_sha256(secret, canonical))
//
// 三个易错点：
//  1. PATH 只含路径，**不含查询串**；
//  2. rawBody 是传输中的字节 —— 用 gzip 时签的是压缩后的字节；
//  3. nonce 每个请求都要重新生成（重放会被控制面以 409 拒绝）。
package cpapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// 控制面约定的请求头名（与 Laravel SignatureVerifier 的常量一致）。
const (
	HeaderNodeID    = "X-Node-Id"
	HeaderTimestamp = "X-Timestamp"
	HeaderNonce     = "X-Nonce"
	HeaderSignature = "X-Node-Signature"
	HeaderEnrollTok = "X-Enroll-Token"
)

// CanonicalString 拼出签名原文。
func CanonicalString(method, path string, body []byte, timestamp, nonce string) string {
	sum := sha256.Sum256(body)

	return strings.Join([]string{
		strings.ToUpper(method),
		path,
		hex.EncodeToString(sum[:]),
		timestamp,
		nonce,
	}, "\n")
}

// Sign 计算十六进制 HMAC-SHA256 签名。
func Sign(secret, method, path string, body []byte, timestamp, nonce string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(CanonicalString(method, path, body, timestamp, nonce)))

	return hex.EncodeToString(mac.Sum(nil))
}

// Signer 负责为每个请求生成头集合；timestamp/nonce 可注入以便测试。
type Signer struct {
	Secret string
	Now    func() time.Time
	Nonce  func() string
}

// NewSigner 返回使用真实时钟与随机 nonce 的签名器。
func NewSigner(secret string) *Signer {
	return &Signer{
		Secret: secret,
		Now:    time.Now,
		Nonce:  newNonce,
	}
}

// Headers 生成一次请求所需的签名头（每次调用都是新的 timestamp/nonce）。
func (s *Signer) Headers(method, path string, body []byte) map[string]string {
	now := s.Now
	if now == nil {
		now = time.Now
	}
	nonceFn := s.Nonce
	if nonceFn == nil {
		nonceFn = newNonce
	}

	ts := strconv.FormatInt(now().Unix(), 10)
	nonce := nonceFn()

	return map[string]string{
		HeaderTimestamp: ts,
		HeaderNonce:     nonce,
		HeaderSignature: Sign(s.Secret, method, path, body, ts, nonce),
	}
}

// newNonce 生成 UUIDv4 形态的随机串（crypto/rand，避免引入依赖）。
func newNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// 极端情况退化为时间戳，仍保证唯一性语义由控制面侧兜底
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return strings.Join([]string{
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	}, "-")
}
