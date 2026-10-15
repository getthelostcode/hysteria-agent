package hyconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newStore(t *testing.T) (*Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	history := filepath.Join(dir, "history")
	target := filepath.Join(dir, "etc", "config.yaml")
	return NewStore(history, target, 3), history, target
}

func TestStoreApplyAtomicAndPerms(t *testing.T) {
	st, _, target := newStore(t)
	payload := []byte("listen: :443\n")
	sum, err := st.Apply(payload)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if sum != Sum(payload) {
		t.Fatal("返回的 sha256 与内容不符")
	}
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	// 配置里有认证密钥，权限必须是 0600。
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("目标配置权限应为 0600，得到 %v", fi.Mode().Perm())
	}
	got, _ := st.ConfigHash()
	if got != sum {
		t.Fatalf("ConfigHash 应为 %s，得到 %s", sum, got)
	}
	// 目录里不应残留临时文件
	entries, _ := os.ReadDir(filepath.Dir(target))
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".tmp" {
			t.Fatalf("残留临时文件: %s", e.Name())
		}
	}
}

func TestStoreHistoryAndRollback(t *testing.T) {
	st, _, target := newStore(t)

	v1 := []byte("listen: :443\nauth: {type: password, password: one}\n")
	v2 := []byte("listen: :443\nauth: {type: password, password: two}\n")

	snap1, err := st.Snapshot(v1, "rev-1", "yaml", "control", "初版")
	if err != nil {
		t.Fatal(err)
	}
	snap1.Result = "ok"
	snap1.AppliedAt = time.Now().Add(-2 * time.Minute)
	if err := st.Record(*snap1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(v1); err != nil {
		t.Fatal(err)
	}

	snap2, err := st.Snapshot(v2, "rev-2", "yaml", "control", "改密码")
	if err != nil {
		t.Fatal(err)
	}
	snap2.Result = "ok"
	snap2.AppliedAt = time.Now()
	if err := st.Record(*snap2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(v2); err != nil {
		t.Fatal(err)
	}

	cur, err := st.Current()
	if err != nil || cur == nil || cur.Revision != "rev-2" {
		t.Fatalf("Current 应为 rev-2，得到 %+v err=%v", cur, err)
	}
	// 回滚：应恢复到 rev-1 的内容
	prev, err := st.Previous()
	if err != nil || prev.Revision != "rev-1" {
		t.Fatalf("Previous 应为 rev-1，得到 %+v err=%v", prev, err)
	}
	v, payload, err := st.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if v.Revision != "rev-1" || string(payload) != string(v1) {
		t.Fatalf("回滚内容不对: %s", string(payload))
	}
	onDisk, _ := os.ReadFile(target)
	if string(onDisk) != string(v1) {
		t.Fatal("回滚后目标配置内容不对")
	}
}

// 保留最近 5 个版本是需求的硬指标，这里按 keep=3 验证裁剪逻辑。
func TestStorePruneKeepsRecent(t *testing.T) {
	st, history, _ := newStore(t)
	for i := 0; i < 6; i++ {
		p := []byte("listen: :443\n# version " + string(rune('a'+i)) + "\n")
		snap, err := st.Snapshot(p, "rev", "yaml", "control", "")
		if err != nil {
			t.Fatal(err)
		}
		snap.Result = "ok"
		snap.AppliedAt = time.Now().Add(time.Duration(i) * time.Second)
		if err := st.Record(*snap); err != nil {
			t.Fatal(err)
		}
	}
	vs, err := st.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 3 {
		t.Fatalf("应只保留 3 个版本，实际 %d", len(vs))
	}
	files, _ := os.ReadDir(history)
	cfgFiles := 0
	for _, f := range files {
		if len(f.Name()) > 4 && f.Name()[:4] == "rev-" {
			cfgFiles++
		}
	}
	if cfgFiles != 3 {
		t.Fatalf("历史目录应只剩 3 个快照文件，实际 %d", cfgFiles)
	}
}

func TestStoreFindAndCorruptIndex(t *testing.T) {
	st, history, _ := newStore(t)
	snap, _ := st.Snapshot([]byte("listen: :443\n"), "rev-9", "yaml", "control", "")
	snap.Result = "ok"
	snap.AppliedAt = time.Now()
	_ = st.Record(*snap)

	if _, err := st.Find("rev-9"); err != nil {
		t.Fatalf("按 revision 查找失败: %v", err)
	}
	if _, err := st.Find(snap.SHA256[:8]); err != nil {
		t.Fatalf("按 sha256 前缀查找失败: %v", err)
	}

	// 索引损坏时不能影响 agent 启动：备份后当空索引继续。
	_ = os.WriteFile(filepath.Join(history, "index.json"), []byte("{broken"), 0o600)
	vs, err := st.List()
	if err != nil {
		t.Fatalf("索引损坏不应报错: %v", err)
	}
	if len(vs) != 0 {
		t.Fatalf("损坏索引应视为空，得到 %d 条", len(vs))
	}
	if _, err := os.Stat(filepath.Join(history, "index.json.corrupt")); err != nil {
		t.Fatal("应保留 .corrupt 备份")
	}
}

func TestStoreRejectedVersionNotUsedForRollback(t *testing.T) {
	st, _, _ := newStore(t)
	good, _ := st.Snapshot([]byte("listen: :443\n# good\n"), "rev-good", "yaml", "control", "")
	good.Result = "ok"
	good.AppliedAt = time.Now()
	_ = st.Record(*good)
	bad, _ := st.Snapshot([]byte("listen: :443\n# bad\n"), "rev-bad", "yaml", "control", "")
	bad.Result = "rejected"
	bad.AppliedAt = time.Now().Add(time.Second)
	_ = st.Record(*bad)

	// 目标文件不存在（hash 为空），Previous 应跳过被拒绝的版本。
	prev, err := st.Previous()
	if err != nil {
		t.Fatalf("Previous 出错: %v", err)
	}
	if prev.Revision != "rev-good" {
		t.Fatalf("回滚目标应是 rev-good，得到 %s", prev.Revision)
	}
}
