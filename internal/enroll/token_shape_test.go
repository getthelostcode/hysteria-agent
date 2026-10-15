package enroll

import (
	"strings"
	"testing"
)

func TestValidateTokenShapeAcceptsAWellFormedToken(t *testing.T) {
	full := "enr_" + strings.Repeat("aB3x", 10) // 44 字符

	if err := ValidateTokenShape(full); err != nil {
		t.Fatalf("完整令牌不应被拒: %v", err)
	}
	if err := ValidateTokenShape("  " + full + "\n"); err != nil {
		t.Fatalf("两端空白应被容忍（面板复制常带换行）: %v", err)
	}
	if err := ValidateTokenShape(full + "…"); err != nil {
		t.Fatalf("尾部省略号应被剪掉后判为合法: %v", err)
	}
}

// 真实案例：运维在面板列表上看到 enr_GmCVlx2W… 就抄了前 12 位（japan2 节点实测）。
func TestValidateTokenShapeRejectsTheMaskedPrefixFromThePanel(t *testing.T) {
	masked := "enr_GmCVlx2W" // 面板列表里的掩码前缀，12 字符

	err := ValidateTokenShape(masked)
	if err == nil {
		t.Fatal("掩码前缀必须在本地就被拒（否则要白跑一趟注册请求才拿到 401）")
	}

	msg := err.Error()
	for _, want := range []string{"12 字符", "44 字符", "掩码", "重新签发"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误信息应包含 %q，实际: %s", want, msg)
		}
	}

	// 带省略号、只有 6 位、以及空令牌都要被拒
	for _, bad := range []string{"enr_GmCVlx2W…", "enr_Gm", "enr_", ""} {
		if err := ValidateTokenShape(bad); err == nil {
			t.Fatalf("%q 应被拒绝", bad)
		}
	}
}

func TestValidateTokenShapeRejectsOverlongTokensButSparesForeignFormats(t *testing.T) {
	if err := ValidateTokenShape("enr_" + strings.Repeat("a", 60)); err == nil {
		t.Fatal("超长令牌应被拒（多为多复制了一段）")
	}

	// 别的控制面可能有自己的格式：不拦，交给服务端判定
	for _, foreign := range []string{"tok_abcdef", "eyJhbGciOi.payload.sig"} {
		if err := ValidateTokenShape(foreign); err != nil {
			t.Fatalf("非 enr_ 前缀不应在本地判死（%s -> %v）", foreign, err)
		}
	}
}
