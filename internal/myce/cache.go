package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// ModelCache serializes copies and eviction so concurrent loads cannot overspend.
type ModelCache struct {
	mu            sync.Mutex
	Root, NASRoot string
	Limit         int64
	Clock         Clock
	Free          func(string) (int64, error)
	Protected     func(string) bool
}

func diskFree(path string) (int64, error) {
	var st syscall.Statfs_t
	e := syscall.Statfs(path, &st)
	return int64(st.Bavail) * int64(st.Bsize), e
}
func (c *ModelCache) init() error {
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.Free == nil {
		c.Free = diskFree
	}
	if c.Limit <= 0 {
		return errors.New("model cache size limit must be positive")
	}
	if err := os.MkdirAll(c.Root, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".partial") {
			if err = c.discard(filepath.Join(c.Root, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Move evictions to NAS Trash when a NAS root is configured. This preserves
// recoverability while releasing local blocks on a separate filesystem.
func (c *ModelCache) discard(path string) error {
	home, e := os.UserHomeDir()
	if e != nil {
		return e
	}
	trash := filepath.Join(home, ".Trash", "myce-model-cache")
	if c.NASRoot != "" {
		trash = filepath.Join(c.NASRoot, ".Trash", "myce-model-cache")
	}
	if e = os.MkdirAll(trash, 0700); e != nil {
		return e
	}
	dest := filepath.Join(trash, ID()+"-"+filepath.Base(path))
	if e = os.Rename(path, dest); e == nil {
		return nil
	} else if !errors.Is(e, syscall.EXDEV) {
		return e
	}
	// Save and verify a recovery copy before asking mv to finish the Trash move.
	in, e := os.Open(path)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	hash := sha256.New()
	n, e := io.Copy(io.MultiWriter(out, hash), in)
	if e == nil {
		e = out.Sync()
	}
	e = errors.Join(e, out.Close())
	if e != nil {
		return e
	}
	if e = verifyModel(dest, Model{Name: "evicted cache copy", SizeBytes: n, SHA256: hex.EncodeToString(hash.Sum(nil))}); e != nil {
		return e
	}
	// mv performs a recoverable cross-filesystem move; keep the verified backup.
	return exec.Command("mv", path, dest+"-original").Run()
}
func cacheKey(m Model) string {
	h := sha256.Sum256([]byte(m.Name))
	return fmt.Sprintf("%x-%s.model", h[:8], m.SHA256)
}
func (c *ModelCache) source(m Model) (string, error) {
	if filepath.IsAbs(m.Path) {
		return m.Path, nil
	}
	if c.NASRoot == "" {
		return "", errors.New("relative model path requires --model-root")
	}
	p := filepath.Clean(m.Path)
	if p == ".." || strings.HasPrefix(p, ".."+string(os.PathSeparator)) {
		return "", errors.New("model path escapes NAS root")
	}
	return filepath.Join(c.NASRoot, p), nil
}
func verifyModel(path string, m Model) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	if e != nil {
		return e
	}
	if n != m.SizeBytes || hex.EncodeToString(h.Sum(nil)) != strings.ToLower(m.SHA256) {
		return fmt.Errorf("model %s size or SHA-256 mismatch", m.Name)
	}
	return nil
}
func (c *ModelCache) Prepare(ctx context.Context, m Model) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	source, e := c.source(m)
	if e != nil {
		return "", e
	}
	if m.DirectNAS {
		return source, verifyModel(source, m)
	}
	if m.SizeBytes > c.Limit {
		return "", fmt.Errorf("model %s exceeds cache size limit: need %d bytes, limit %d", m.Name, m.SizeBytes, c.Limit)
	}
	dest := filepath.Join(c.Root, cacheKey(m))
	if verifyModel(dest, m) == nil {
		now := c.Clock.Now()
		e = os.Chtimes(dest, now, now)
		return dest, e
	}
	if _, e = os.Stat(dest); e == nil {
		if e = c.discard(dest); e != nil {
			return "", e
		}
	}
	type entry struct {
		path string
		size int64
		used int64
	}
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return "", e
	}
	var used int64
	var victims []entry
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".model") {
			continue
		}
		st, err := entry.Info()
		if err != nil {
			return "", err
		}
		used += st.Size()
		if c.Protected == nil || !c.Protected(entry.Name()) {
			victims = append(victims, struct {
				path string
				size int64
				used int64
			}{filepath.Join(c.Root, entry.Name()), st.Size(), st.ModTime().UnixNano()})
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].used == victims[j].used {
			return victims[i].path < victims[j].path
		}
		return victims[i].used < victims[j].used
	})
	free, e := c.Free(c.Root)
	if e != nil {
		return "", e
	}
	for _, v := range victims {
		if used+m.SizeBytes <= c.Limit && free >= m.SizeBytes {
			break
		}
		if e = c.discard(v.path); e != nil {
			return "", e
		}
		used -= v.size
		free, e = c.Free(c.Root)
		if e != nil {
			return "", e
		}
	}
	if used+m.SizeBytes > c.Limit || free < m.SizeBytes {
		return "", fmt.Errorf("insufficient local model cache space for %s: need %d bytes, disk free %d, cache free %d", m.Name, m.SizeBytes, free, max(0, c.Limit-used))
	}
	in, e := os.Open(source)
	if e != nil {
		return "", e
	}
	defer in.Close()
	temp := dest + ".partial"
	out, e := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return "", e
	}
	h := sha256.New()
	buf := make([]byte, 1024*1024)
	var n int64
	for {
		if e = ctx.Err(); e != nil {
			break
		}
		var count int
		count, e = in.Read(buf)
		if count > 0 {
			var written int
			written, e = io.MultiWriter(out, h).Write(buf[:count])
			n += int64(written)
			if n > m.SizeBytes {
				e = errors.New("NAS model exceeds declared size")
			}
		}
		if e != nil {
			break
		}
	}
	if e == io.EOF {
		e = nil
	}
	if e == nil && (n != m.SizeBytes || hex.EncodeToString(h.Sum(nil)) != strings.ToLower(m.SHA256)) {
		e = errors.New("NAS model size or SHA-256 mismatch")
	}
	if e == nil {
		e = out.Sync()
	}
	e = errors.Join(e, out.Close())
	if e != nil {
		cleanup := c.discard(temp)
		return "", errors.Join(e, cleanup)
	}
	if e = os.Rename(temp, dest); e != nil {
		return "", e
	}
	now := c.Clock.Now()
	if e = os.Chtimes(dest, now, now); e != nil {
		return "", e
	}
	return dest, nil
}

// Inventory never waits behind a large copy: heartbeats must stay live during
// loading. Atomic publication and ENOENT tolerance provide a safe snapshot.
func (c *ModelCache) Report(models []Model) ([]string, int64, error) {
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return nil, 0, e
	}
	var used int64
	var names []string
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".model") {
			continue
		}
		st, err := entry.Info()
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		used += st.Size()
		for _, m := range models {
			if entry.Name() == cacheKey(m) && st.Size() == m.SizeBytes {
				names = append(names, m.Name)
			}
		}
	}
	free, e := c.Free(c.Root)
	sort.Strings(names)
	return names, min(free, max(0, c.Limit-used)), e
}

// FetchModel is the only network download path. Loading never invokes it.
func FetchModel(ctx context.Context, raw, dest string, size int64, sum string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host != "huggingface.co" {
		return errors.New("fetch requires an HTTPS huggingface.co file URL")
	}
	if size <= 0 || len(sum) != 64 {
		return errors.New("fetch requires size and SHA-256")
	}
	if _, e = os.Stat(dest); !os.IsNotExist(e) {
		return errors.New("destination already exists or cannot be inspected")
	}
	if e = os.MkdirAll(filepath.Dir(dest), 0700); e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if e != nil {
		return e
	}
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("fetch: HTTP %d", res.StatusCode)
	}
	temp := dest + ".partial"
	f, e := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = io.Copy(f, io.LimitReader(res.Body, size+1))
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if e = verifyModel(temp, Model{Name: dest, SizeBytes: size, SHA256: sum}); e != nil {
		return e
	}
	// Link rather than overwrite a master published by another fetch.
	if e = os.Link(temp, dest); e != nil {
		return e
	}
	c := ModelCache{NASRoot: filepath.Dir(dest)}
	return c.discard(temp)
}
