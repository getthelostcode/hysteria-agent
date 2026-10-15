package hyconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Version 是一次配置落盘的记录（保留最近 N 个用于回滚）。
type Version struct {
	Revision  string    `json:"revision"`
	SHA256    string    `json:"sha256"`
	File      string    `json:"file"` // history 目录下的文件名
	Size      int       `json:"size"`
	Format    string    `json:"format"` // yaml|json
	Source    string    `json:"source"` // control|local|rollback|install
	Reason    string    `json:"reason,omitempty"`
	Result    string    `json:"result"` // ok|rejected|failed|dry_run|rolled_back
	Error     string    `json:"error,omitempty"`
	Checks    []string  `json:"checks,omitempty"`
	Warnings  []string  `json:"warnings,omitempty"`
	AppliedAt time.Time `json:"applied_at"`
	Actor     string    `json:"actor,omitempty"` // control plane 操作者（若有）
}

// Store 管理配置历史与目标配置文件的原子写入。
//
// 目录布局：
//
//	/etc/hysteria-agent/history/
//	  index.json                 ← 版本索引（倒序，最新在前）
//	  rev-000001-20261015-3.yaml ← 每个版本一份只读快照
//	/etc/hysteria/config.yaml    ← Hysteria 实际读取的文件（0600，原子替换）
type Store struct {
	historyDir string
	configPath string
	keep       int
}

// NewStore 构建 Store。keep<=0 时默认 5（需求里"保留最近 5 个版本"）。
func NewStore(historyDir, configPath string, keep int) *Store {
	if keep <= 0 {
		keep = 5
	}
	return &Store{historyDir: historyDir, configPath: configPath, keep: keep}
}

func (s *Store) indexFile() string { return filepath.Join(s.historyDir, "index.json") }

// List 返回历史版本（最新在前）。
func (s *Store) List() ([]Version, error) {
	raw, err := os.ReadFile(s.indexFile())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var vs []Version
	if err := json.Unmarshal(raw, &vs); err != nil {
		// 索引损坏不该让 agent 起不来：备份后当空索引继续。
		_ = os.Rename(s.indexFile(), s.indexFile()+".corrupt")
		return nil, nil
	}
	sort.SliceStable(vs, func(i, j int) bool { return vs[i].AppliedAt.After(vs[j].AppliedAt) })
	return vs, nil
}

func (s *Store) saveIndex(vs []Version) error {
	if err := os.MkdirAll(s.historyDir, 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(vs, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.indexFile() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	return os.Rename(tmp, s.indexFile())
}

// Record 追加一条版本记录并裁剪到 keep 个。
func (s *Store) Record(v Version) error {
	vs, err := s.List()
	if err != nil {
		return err
	}
	// 同一个 revision 重复记录时覆盖，避免历史里出现重复项。
	filtered := vs[:0]
	for _, x := range vs {
		if x.Revision == v.Revision && x.SHA256 == v.SHA256 {
			continue
		}
		filtered = append(filtered, x)
	}
	vs = append([]Version{v}, filtered...)
	if err := s.saveIndex(vs); err != nil {
		return err
	}
	_, err = s.Prune()
	return err
}

// Snapshot 把配置正文写进历史目录（只读快照），返回记录（未写入索引）。
func (s *Store) Snapshot(payload []byte, revision, format, source, reason string) (*Version, error) {
	if err := os.MkdirAll(s.historyDir, 0o700); err != nil {
		return nil, err
	}
	seq, err := s.nextSeq()
	if err != nil {
		return nil, err
	}
	ext := ".yaml"
	if format == "json" {
		ext = ".json"
	}
	safeRev := sanitize(revision)
	if safeRev == "" {
		safeRev = time.Now().UTC().Format("20060102-150405")
		revision = safeRev
	}
	name := fmt.Sprintf("rev-%06d-%s%s", seq, safeRev, ext)
	path := filepath.Join(s.historyDir, name)
	if err := atomicWrite(path, payload, 0o600); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	return &Version{
		Revision: revision,
		SHA256:   hex.EncodeToString(sum[:]),
		File:     name,
		Size:     len(payload),
		Format:   format,
		Source:   source,
		Reason:   reason,
	}, nil
}

// nextSeq 扫描历史目录取最大序号 +1。
//
// 不能用"索引里已有多少条"来算：Prune 会裁剪索引，序号会回退，
// 于是新快照会覆盖掉同名的旧文件（历史上真踩过这个 bug）。
func (s *Store) nextSeq() (int, error) {
	entries, err := os.ReadDir(s.historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	max := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "rev-") {
			continue
		}
		parts := strings.SplitN(name, "-", 3)
		if len(parts) < 2 {
			continue
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max + 1, nil
}

// Apply 原子写入 Hysteria 的目标配置文件（0600），返回写入内容的 sha256。
//
// 顺序很重要：先 fsync 临时文件 → rename（同一目录内，POSIX 保证原子）→ chmod 0600。
// Hysteria 侧永远看到"旧文件或新文件"，不会读到半截内容。
func (s *Store) Apply(payload []byte) (string, error) {
	if err := os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
		return "", err
	}
	if err := atomicWrite(s.configPath, payload, 0o600); err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// Read 读取某个历史版本的正文。
func (s *Store) Read(v *Version) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("版本为空")
	}
	return os.ReadFile(filepath.Join(s.historyDir, v.File))
}

// Current 返回当前生效的版本记录（按目标配置文件的 hash 匹配）。
func (s *Store) Current() (*Version, error) {
	cur, err := s.ConfigHash()
	if err != nil {
		return nil, err
	}
	vs, err := s.List()
	if err != nil {
		return nil, err
	}
	for i := range vs {
		if vs[i].SHA256 == cur && vs[i].Result != "rejected" && vs[i].Result != "failed" {
			return &vs[i], nil
		}
	}
	if len(vs) > 0 {
		return &vs[0], nil
	}
	return nil, nil
}

// ConfigHash 返回当前目标配置文件的 sha256。
func (s *Store) ConfigHash() (string, error) {
	b, err := os.ReadFile(s.configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// CurrentExact 只在目标配置文件与某条历史记录**逐字节一致**时返回它，否则返回 nil。
//
// 与 Current() 的区别：Current() 匹配不到就退回「最新一条记录」，于是被校验拒绝、
// 从未真正落盘的版本也会被当成「本机当前版本」—— sync 会误报「已是最新」，
// 节点再也取不到修复后的配置（jp8 上真踩过）。
func (s *Store) CurrentExact() (*Version, error) {
	cur, err := s.ConfigHash()
	if err != nil {
		return nil, err
	}
	if cur == "" {
		return nil, nil
	}
	vs, err := s.List()
	if err != nil {
		return nil, err
	}
	for i := range vs {
		if vs[i].SHA256 == cur && vs[i].Result != "rejected" && vs[i].Result != "failed" {
			return &vs[i], nil
		}
	}

	return nil, nil
}

// CurrentRevision 返回当前 revisions 字符串（找 hash 匹配的最新记录）。
func (s *Store) CurrentRevision() string {
	v, err := s.Current()
	if err != nil || v == nil {
		return ""
	}
	return v.Revision
}

// Find 按 revision 或 sha256 前缀查找历史版本。
func (s *Store) Find(key string) (*Version, error) {
	vs, err := s.List()
	if err != nil {
		return nil, err
	}
	for i := range vs {
		if vs[i].Revision == key || strings.HasPrefix(vs[i].SHA256, key) {
			return &vs[i], nil
		}
	}
	return nil, fmt.Errorf("未找到版本 %q（可用版本: %s）", key, s.revisionList(vs))
}

// Previous 返回"上一个可回滚的版本"：与当前配置文件 hash 不同的最新一条成功记录。
func (s *Store) Previous() (*Version, error) {
	cur, err := s.ConfigHash()
	if err != nil {
		return nil, err
	}
	vs, err := s.List()
	if err != nil {
		return nil, err
	}
	for i := range vs {
		if vs[i].SHA256 == cur {
			continue
		}
		if vs[i].Result == "rejected" || vs[i].Result == "failed" {
			continue
		}
		if _, err := os.Stat(filepath.Join(s.historyDir, vs[i].File)); err != nil {
			continue
		}
		return &vs[i], nil
	}
	return nil, fmt.Errorf("没有可回滚的历史版本（历史目录 %s）", s.historyDir)
}

// Prune 只保留最近 keep 个版本，删除多余快照文件。
func (s *Store) Prune() (int, error) {
	vs, err := s.List()
	if err != nil {
		return 0, err
	}
	if len(vs) <= s.keep {
		return 0, nil
	}
	keep := vs[:s.keep]
	drop := vs[s.keep:]
	for _, v := range drop {
		_ = os.Remove(filepath.Join(s.historyDir, v.File))
	}
	if err := s.saveIndex(keep); err != nil {
		return 0, err
	}
	return len(drop), nil
}

// Rollback 把目标配置文件回滚到上一个版本，返回被恢复的版本与正文。
// 调用方负责随后重启 Hysteria 并记录结果。
func (s *Store) Rollback() (*Version, []byte, error) {
	v, err := s.Previous()
	if err != nil {
		return nil, nil, err
	}
	payload, err := s.Read(v)
	if err != nil {
		return nil, nil, err
	}
	if _, err := s.Apply(payload); err != nil {
		return nil, nil, err
	}
	return v, payload, nil
}

func (s *Store) revisionList(vs []Version) string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Revision)
	}
	return strings.Join(out, ", ")
}

// ---------------------------- 工具 ----------------------------

// atomicWrite 同目录内先写 tmp 再 rename，保证读者看到的是完整文件。
func atomicWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-.")
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}
