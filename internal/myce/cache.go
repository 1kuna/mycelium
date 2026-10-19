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
	"time"
)

// ModelCache reserves complete copy sizes under one admission lock.
type ModelCache struct {
	ApprovalRoot  string
	storedModels  sync.Map
	FileClient    *Client
	mu            sync.Mutex
	Root, NASRoot string
	Limit         int64
	Clock         Clock
	Free          func(string) (int64, error)
	Protected     func(string) bool
	Usage         func(string) int
	Parallel      int
	active        map[string]chan struct{}
	reservations  sync.Map
	verified      sync.Map
	progress      sync.Map
	speed         float64
	speedMu       sync.Mutex
	beforeChunk   func(context.Context) // deterministic interruption hook for tests

	localCheckMu        sync.Mutex
	localApprovalRound  uint64
	localResults        sync.Map // Completed metadata checks, consumed once by their heartbeat.
	localRecordProbe    checkpointIO[localApprovalRecords]
	localProbe          checkpointIO[localApprovalSnapshot]
	localProbeInit      sync.Once
	localWarningMu      sync.Mutex
	nextApprovalWarning time.Time
	localMetadataProbe  func(context.Context, string, string) ([]localModelMetadata, error) // Filesystem fault seam.
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
	if err := PrepareStateRoots(c.Root); err != nil {
		return err
	}
	// Partial files carry the immutable model hash in their names and survive restart.
	if c.active == nil {
		c.active = map[string]chan struct{}{}
	}
	return nil
}

// Move evictions to NAS Trash when a NAS root is configured. This preserves
// recoverability while releasing local blocks on a separate filesystem.
func (c *ModelCache) discard(path string) error {
	return c.discardContext(context.Background(), path)
}
func (c *ModelCache) discardContext(ctx context.Context, path string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if model, ok := c.storedModels.Load(strings.TrimSuffix(filepath.Base(path), ".partial")); ok && c.FileClient != nil {
		m := model.(Model)
		if len(m.Files) == 0 {
			if e := c.FileClient.HasFile(ctx, FileRef{m.SHA256, m.SizeBytes}); e != nil {
				return e
			}
		} else {
			for _, f := range m.Files {
				if e := c.FileClient.HasFile(ctx, FileRef{f.SHA256, f.SizeBytes}); e != nil {
					return e
				}
			}
		}
		return os.RemoveAll(path) // Disposable, hash-verified authoritative bytes remain in the store.
	}
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
	if st, err := os.Lstat(path); err != nil {
		return err
	} else if st.IsDir() {
		if err = c.recoverModelDirectory(ctx, path, dest); err != nil {
			return err
		}
		return exec.CommandContext(ctx, "mv", path, dest+"-original").Run()
	}
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
	n, e := io.Copy(io.MultiWriter(out, hash), contextReader{ctx, in})
	if e == nil {
		e = out.Sync()
	}
	e = errors.Join(e, out.Close())
	if e != nil {
		return e
	}
	if e = verifyModelContext(ctx, dest, Model{Name: "evicted cache copy", SizeBytes: n, SHA256: hex.EncodeToString(hash.Sum(nil))}); e != nil {
		return e
	}
	// mv performs a recoverable cross-filesystem move; keep the verified backup.
	return exec.CommandContext(ctx, "mv", path, dest+"-original").Run()
}
func cacheKey(m Model) string {
	h := sha256.Sum256([]byte(m.Name))
	return fmt.Sprintf("%x-%s.model", h[:8], m.SHA256)
}
func (c *ModelCache) source(m Model) (string, error) {
	if m.FileStore {
		if c.FileClient == nil {
			return "", errors.New("model file store unavailable")
		}
		return "", nil
	}
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
	return verifyModelContext(context.Background(), path, m)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.reader.Read(p)
}
func verifyModelContext(ctx context.Context, path string, m Model) error {
	if len(m.Files) > 0 {
		return verifyModelFiles(ctx, path, m)
	}
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, contextReader{ctx, f})
	if e != nil {
		return e
	}
	if n != m.SizeBytes || hex.EncodeToString(h.Sum(nil)) != strings.ToLower(m.SHA256) {
		return fmt.Errorf("model %s size or SHA-256 mismatch", m.Name)
	}
	return nil
}

// Prepare is foreground work; Precopy adds popularity-aware eviction.
func (c *ModelCache) Prepare(ctx context.Context, m Model) (string, error) {
	return c.prepare(ctx, m, false, 0)
}
func (c *ModelCache) Precopy(ctx context.Context, m Model, score int) (string, error) {
	return c.prepare(ctx, m, true, score)
}
func (c *ModelCache) prepare(ctx context.Context, m Model, background bool, score int) (string, error) {
	if m.LocalSource != "" {
		return c.prepareLocal(ctx, m)
	}

	if len(m.Files) > 0 {
		if e := validateModelFiles(m); e != nil {
			return "", e
		}
		if e := noModelSymlinks(c.Root, false); e != nil {
			return "", e
		}
	}
	key := cacheKey(m)
	if m.FileStore {
		c.storedModels.Store(key, m)
	}
	// Admission, reservations, and eviction share one short lock. File I/O does
	// not hold it, so independent copies can run with an explicit parallel limit.
	for {
		c.mu.Lock()
		if c.active == nil {
			c.active = map[string]chan struct{}{}
		}
		parallel := max(1, c.Parallel)
		wait := c.active[key]
		if wait == nil && len(c.active) >= parallel {
			for _, ch := range c.active {
				wait = ch
				break
			}
		}
		if wait == nil {
			break
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-wait:
		}
	}
	if e := ctx.Err(); e != nil {
		c.mu.Unlock()
		return "", e
	}
	done := make(chan struct{})
	c.active[key] = done
	if !m.DirectNAS {
		c.reservations.Store(key, m.SizeBytes)
	}
	defer func() { c.mu.Lock(); delete(c.active, key); c.reservations.Delete(key); close(done); c.mu.Unlock() }()
	source, e := c.source(m)
	if e != nil {
		c.mu.Unlock()
		return "", e
	}
	if m.DirectNAS {
		c.mu.Unlock()
		return source, verifyModelContext(ctx, source, m)
	}
	if m.SizeBytes <= 0 || m.SizeBytes > c.Limit {
		c.mu.Unlock()
		return "", fmt.Errorf("model %s exceeds cache size limit or has no verified size", m.Name)
	}
	dest := filepath.Join(c.Root, key)
	// Verify outside the admission lock. Only this key's owner can publish it.
	c.mu.Unlock()
	cached := verifyModelContext(ctx, dest, m) == nil
	if e := ctx.Err(); e != nil {
		return "", e
	}
	c.mu.Lock()
	if _, e = os.Lstat(dest); e == nil && !cached {
		c.verified.Delete(key)
		if c.Protected != nil && c.Protected(key) {
			c.mu.Unlock()
			return "", errors.New("corrupt protected cache copy")
		}
		if e = c.discardContext(ctx, dest); e != nil {
			c.mu.Unlock()
			return "", e
		}
	}
	temp := dest + ".partial"
	offset := int64(0)
	if _, err := os.Lstat(temp); err == nil {
		offset, e = cacheEntrySize(temp)
		if e != nil {
			c.mu.Unlock()
			return "", e
		}
	}
	if offset > m.SizeBytes {
		if e = c.discardContext(ctx, temp); e != nil {
			c.mu.Unlock()
			return "", e
		}
		offset = 0
	}
	type entry struct {
		path, key  string
		size, used int64
	}
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		c.mu.Unlock()
		return "", e
	}
	var used, reserved int64
	var victims []entry
	for _, ent := range entries {
		if !strings.HasSuffix(ent.Name(), ".model") && !strings.HasSuffix(ent.Name(), ".partial") {
			continue
		}
		st, err := ent.Info()
		if err != nil {
			c.mu.Unlock()
			return "", err
		}
		k := strings.TrimSuffix(ent.Name(), ".partial")
		if k != key && c.active[k] != nil {
			continue
		}
		entrySize, err := cacheEntrySize(filepath.Join(c.Root, ent.Name()))
		if err != nil {
			c.mu.Unlock()
			return "", err
		}
		used += entrySize
		if k == key || c.active[k] != nil || c.Protected != nil && c.Protected(k) {
			continue
		}
		if background && c.Usage != nil && c.Usage(k) > score {
			continue
		}
		victims = append(victims, entry{filepath.Join(c.Root, ent.Name()), k, entrySize, st.ModTime().UnixNano()})
	}
	// Reserve other active models in full. Disk admission is deliberately
	// conservative so in-flight writes cannot race the filesystem free snapshot.
	c.reservations.Range(func(k, size any) bool {
		if k.(string) != key {
			reserved += size.(int64)
		}
		return true
	})
	free, e := c.Free(c.Root)
	if e != nil {
		c.mu.Unlock()
		return "", e
	}
	need := m.SizeBytes - offset
	if cached {
		need = 0
	}
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].used == victims[j].used {
			return victims[i].path < victims[j].path
		}
		return victims[i].used < victims[j].used
	})
	reclaim := int64(0)
	for _, v := range victims {
		reclaim += v.size
	}
	if used+reserved+need-reclaim > c.Limit {
		c.mu.Unlock()
		return "", errors.New("insufficient local model cache space (protected or more-used copies)")
	}
	for _, v := range victims {
		if used+reserved+need <= c.Limit && free-reserved >= need {
			break
		}
		if e = ctx.Err(); e != nil {
			c.mu.Unlock()
			return "", e
		}
		// Protection may have changed since the candidate scan.
		if c.Protected != nil && c.Protected(v.key) {
			continue
		}
		if background && c.Usage != nil && c.Usage(v.key) > score {
			continue
		}
		if e = c.discardContext(ctx, v.path); e != nil {
			c.mu.Unlock()
			return "", e
		}
		c.verified.Delete(v.key)
		c.progress.Delete(v.key)
		used -= v.size
		free, e = c.Free(c.Root)
		if e != nil {
			c.mu.Unlock()
			return "", e
		}
	}
	if used+reserved+need > c.Limit || free-reserved < need {
		c.mu.Unlock()
		return "", errors.New("insufficient local model cache space")
	}
	if cached {
		c.mu.Unlock()
		c.verified.Store(key, true)
		now := c.Clock.Now()
		return dest, os.Chtimes(dest, now, now)
	}
	r := CopyReport{Model: m.Name, State: "copying", Bytes: offset, Total: m.SizeBytes}
	c.progress.Store(key, r)
	c.mu.Unlock()
	if err := checkLaunchGate(ctx); err != nil {
		r.State, r.Error = "paused", err.Error()
		c.progress.Store(key, r)
		return "", err
	}
	started := c.Clock.Now()
	copied := int64(0)
	defer func() {
		elapsed := c.Clock.Now().Sub(started).Seconds()
		c.speedMu.Lock()
		if copied > 0 && elapsed > 0 {
			c.speed = float64(copied) / elapsed
			r.BytesPerSecond = c.speed
			c.progress.Store(key, r)
		}
		c.speedMu.Unlock()
	}()
	if m.FileStore {
		e = c.copyStoredModel(ctx, temp, m, &r, &copied)
		if e != nil {
			r.State = "paused"
			r.Error = e.Error()
			c.progress.Store(key, r)
			return "", e
		}
		if e = os.Rename(temp, dest); e != nil {
			return "", e
		}
		if e = syncWorkerDir(c.Root); e != nil {
			return "", e
		}
		c.verified.Store(key, true)
		r.State = "ready"
		r.Error = ""
		r.Bytes = m.SizeBytes
		c.progress.Store(key, r)
		now := c.Clock.Now()
		return dest, os.Chtimes(dest, now, now)
	}
	if len(m.Files) > 0 {
		e = c.copyModelFiles(ctx, source, temp, m, &r, &copied)
		if e != nil {
			r.State = "paused"
			r.Error = e.Error()
			if errors.Is(e, errInvalidBundleCopy) {
				r.State = "failed"
				discardErr := c.discard(temp)
				e = errors.Join(e, discardErr)
				if discardErr == nil {
					r.Bytes = 0
				}
			}
			c.progress.Store(key, r)
			return "", e
		}
		if e = os.Rename(temp, dest); e != nil {
			return "", e
		}
		dir, err := os.Open(c.Root)
		if err != nil {
			return "", err
		}
		if e = errors.Join(dir.Sync(), dir.Close()); e != nil {
			return "", e
		}
		c.verified.Store(key, true)
		r.State = "ready"
		r.Error = ""
		c.progress.Store(key, r)
		now := c.Clock.Now()
		return dest, os.Chtimes(dest, now, now)
	}
	in, e := os.Open(source)
	if e != nil {
		r.State = "paused"
		r.Error = e.Error()
		c.progress.Store(key, r)
		return "", e
	}
	defer in.Close()
	out, e := os.OpenFile(temp, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		r.State = "failed"
		r.Error = e.Error()
		c.progress.Store(key, r)
		return "", e
	}
	h := sha256.New()
	// Rehash durable prefix after restart; only a full final hash permits publication.
	_, e = io.CopyN(h, contextReader{ctx, out}, offset)
	if e == nil {
		_, e = in.Seek(offset, io.SeekStart)
	}
	buf := make([]byte, 1024*1024)
	n := offset
	oversize := false
	for e == nil {
		if c.beforeChunk != nil {
			c.beforeChunk(ctx)
		}
		if e = ctx.Err(); e != nil {
			break
		}
		count, readErr := in.Read(buf[:min(int64(len(buf)), m.SizeBytes-n+1)])
		if count > 0 {
			if n+int64(count) > m.SizeBytes {
				oversize = true
				e = errors.New("NAS model exceeds declared size")
				break
			}
			written, writeErr := io.MultiWriter(out, h).Write(buf[:count])
			n += int64(written)
			copied += int64(written)
			r.Bytes = n
			c.progress.Store(key, r)
			if writeErr != nil {
				e = writeErr
				break
			}
		}
		if readErr != nil {
			e = readErr
			break
		}
	}
	if e == io.EOF {
		e = nil
	}
	mismatch := e == nil && (n != m.SizeBytes || hex.EncodeToString(h.Sum(nil)) != strings.ToLower(m.SHA256))
	if mismatch {
		e = errors.New("NAS model size or SHA-256 mismatch")
	}
	e = errors.Join(e, out.Sync(), out.Close())
	if e != nil {
		r.State = "paused"
		r.Error = e.Error()
		if mismatch || oversize {
			r.State = "failed"
			e = errors.Join(e, c.discard(temp))
			r.Bytes = 0
		}
		c.progress.Store(key, r)
		return "", e
	}
	if e = os.Rename(temp, dest); e != nil {
		return "", e
	}
	// Sync the directory so atomic publication survives a power failure.
	dir, e := os.Open(c.Root)
	if e == nil {
		e = errors.Join(dir.Sync(), dir.Close())
	}
	if e != nil {
		return "", e
	}
	c.verified.Store(key, true)
	r.State = "ready"
	r.Error = ""
	c.progress.Store(key, r)
	now := c.Clock.Now()
	return dest, os.Chtimes(dest, now, now)
}

// Inventory only trusts copies verified in this worker session. Startup validation
// happens before joining; it cannot block heartbeats behind a large NAS copy.
func (c *ModelCache) ValidateInventory(models []Model) {
	for _, m := range models {
		if m.LocalSource != "" {
			continue
		}
		if m.FileStore {
			c.storedModels.Store(cacheKey(m), m)
		}
		c.verified.Delete(cacheKey(m))
		if size, e := cacheEntrySize(filepath.Join(c.Root, cacheKey(m)+".partial")); e == nil {
			c.progress.Store(cacheKey(m), CopyReport{Model: m.Name, State: "paused", Bytes: size, Total: m.SizeBytes})
		}
		if verifyModel(filepath.Join(c.Root, cacheKey(m)), m) == nil {
			c.verified.Store(cacheKey(m), true)
			c.progress.Store(cacheKey(m), CopyReport{Model: m.Name, State: "ready", Bytes: m.SizeBytes, Total: m.SizeBytes})
		}
	}
}
func (c *ModelCache) Report(models []Model) ([]string, int64, error) {
	return c.report(models, c.ApprovedModels(models))
}

// The heartbeat shares its one approval snapshot with cache inventory.
func (c *ModelCache) report(models []Model, approved map[string]string) ([]string, int64, error) {
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return nil, 0, e
	}
	var used int64
	bytesByKey := map[string]int64{}
	var names []string
	for _, ent := range entries {
		if !strings.HasSuffix(ent.Name(), ".model") && !strings.HasSuffix(ent.Name(), ".partial") {
			continue
		}
		size, err := cacheEntrySize(filepath.Join(c.Root, ent.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		used += size
		bytesByKey[strings.TrimSuffix(ent.Name(), ".partial")] += size
		if _, ok := c.verified.Load(ent.Name()); !ok {
			continue
		}
		for _, m := range models {
			if ent.Name() == cacheKey(m) && size == m.SizeBytes {
				names = append(names, m.Name)
			}
		}
	}
	for _, m := range models {
		if m.LocalSource != "" {
			if approved[m.LocalSource] == m.SHA256 {
				names = append(names, m.Name)
			}
		}
	}
	free, e := c.Free(c.Root)
	// Do not take the admission lock: eviction may be waiting on the NAS.
	// Reserve the unwritten suffix as well as bytes already on disk.
	var pending int64
	c.reservations.Range(func(key, size any) bool {
		pending += max(0, size.(int64)-bytesByKey[key.(string)])
		return true
	})
	sort.Strings(names)
	return names, max(0, min(free-pending, c.Limit-used-pending)), e
}
func (c *ModelCache) CopyReports() ([]CopyReport, float64) {
	out := []CopyReport{}
	c.progress.Range(func(_, v any) bool { out = append(out, v.(CopyReport)); return true })
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	c.speedMu.Lock()
	speed := c.speed
	c.speedMu.Unlock()
	return out, speed
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

func (c *ModelCache) noteCopyError(m Model, err error) {
	key := cacheKey(m)
	r := CopyReport{Model: m.Name, State: "failed", Total: m.SizeBytes}
	if v, ok := c.progress.Load(key); ok {
		r = v.(CopyReport)
	}
	r.Error = err.Error()
	c.progress.Store(key, r)
}

// Initialize prepares a standalone cache, as used by local measurement.
func (c *ModelCache) Initialize() error { return c.init() }
