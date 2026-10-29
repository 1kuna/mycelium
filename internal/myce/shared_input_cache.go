package myce

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Reuse the environment cache's OS locking and state-root initialization. Data
// entries have their own integrity records and no environment/catalog lifecycle.
type SharedInputCache struct {
	cache         EnvironmentCache
	Client        Client
	mu            sync.Mutex
	verified      map[string]string
	staging       map[*sharedInputStaging]struct{}
	beforeStage   func(context.Context)
	resolveNAS    func(root, relative string) (string, error)
	beforePublish func()
	hits          uint64
	misses        uint64
	nasBytes      uint64
}
type sharedEntry struct {
	Files FileManifest `json:"files"`
}

func (c *SharedInputCache) nasPathContext(ctx context.Context, root, relative string) (string, error) {
	if err := sharedInputProgress(ctx); err != nil {
		return "", err
	}
	path, err := c.nasPath(root, relative)
	if err == nil {
		err = sharedInputProgress(ctx)
	}
	return path, err
}

func (c *SharedInputCache) nasPath(root, relative string) (string, error) {
	if c.resolveNAS != nil {
		return c.resolveNAS(root, relative)
	}
	return confinedPath(root, relative)
}

// readyKeys is an advisory snapshot of entries verified by this worker. It
// performs no filesystem or NAS I/O on the heartbeat or queue path.
func (c *SharedInputCache) readyKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.verified))
	for key := range c.verified {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (c *SharedInputCache) evidence() SharedInputEvidence {
	c.mu.Lock()
	defer c.mu.Unlock()
	return SharedInputEvidence{Hits: c.hits, Misses: c.misses, NASDeclaredBytes: c.nasBytes}
}

func removeSharedEntry(path string) error {
	err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.Chmod(p, 0700)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
func (c *SharedInputCache) Acquire(ctx context.Context, s SharedInput, nasRoot string) (string, func(), error) {
	if err := s.validate(); err != nil {
		return "", nil, err
	}
	if err := c.cache.init(); err != nil {
		return "", nil, err
	}
	key := s.key()
	use, err := c.cache.lock(ctx, key+".use", syscall.LOCK_SH)
	if err != nil {
		return "", nil, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			at := c.cache.Clock.Now()
			_ = os.Chtimes(filepath.Join(c.cache.Root, key, "ready.json"), at, at)
			unlockEnvironment(use)
		})
	}
	path, err := c.ensure(ctx, s, nasRoot)
	if err != nil {
		release()
		return "", nil, err
	}
	return path, release, nil
}
func (c *SharedInputCache) ensure(ctx context.Context, s SharedInput, nasRoot string) (string, error) {
	key := s.key()
	root := filepath.Join(c.cache.Root, key)
	payload := filepath.Join(root, "data")
	build, err := c.cache.lock(ctx, key+".build", syscall.LOCK_EX)
	if err != nil {
		return "", err
	}
	defer unlockEnvironment(build)
	// A failed local verification must not continue advertising this identity.
	valid := false
	defer func() {
		if !valid {
			c.mu.Lock()
			delete(c.verified, key)
			c.mu.Unlock()
		}
	}()
	if _, err = os.Lstat(root); err == nil {
		var record sharedEntry
		b, e := os.ReadFile(filepath.Join(root, "ready.json"))
		if e != nil {
			return "", e
		}
		if e = json.Unmarshal(b, &record); e != nil {
			return "", e
		}
		signature, e := sharedSignature(payload)
		if e != nil {
			return "", fmt.Errorf("shared input cache changed: %w", e)
		}
		c.mu.Lock()
		previous := c.verified[key]
		c.mu.Unlock()
		// Another process may legitimately evict and republish this key. Changed
		// metadata requires full verification, just like first use after restart.
		if previous != signature {
			if e = verifySharedEntry(ctx, payload, record.Files); e != nil {
				return "", e
			}
			c.mu.Lock()
			if c.verified == nil {
				c.verified = map[string]string{}
			}
			c.verified[key] = signature
			c.mu.Unlock()
		}
		valid = true
		c.mu.Lock()
		c.hits++
		c.mu.Unlock()
		return payload, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	need := s.bytes()
	if s.Archive != "" {
		if s.SizeBytes > int64(^uint64(0)>>1)-need {
			return "", errors.New("shared input size overflow")
		}
		need += s.SizeBytes
	}
	err = c.stageInput(ctx, func(ctx context.Context, stage string) error {
		// The writer owns this random stage's build lock before reserving space.
		// Matching reservation/partial names reuse existing remaining-byte accounting.
		gate, e := c.cache.lock(ctx, "admission", syscall.LOCK_EX)
		if e != nil {
			return e
		}
		if e = removeSharedEntry(root + ".partial"); e == nil {
			e = c.admit(ctx, key, need)
		}
		reservation := strings.TrimSuffix(stage, ".partial") + ".reservation"
		if e == nil {
			e = os.WriteFile(reservation, []byte(strconv.FormatInt(need, 10)), 0600)
		}
		unlockEnvironment(gate)
		if e != nil {
			return e
		}
		// Keep the disk reservation until the writer joins, including any syscall
		// tail after cancellation. The attempt's execution reservation is independent.
		defer func() {
			gate, e := c.cache.lock(context.Background(), "admission", syscall.LOCK_EX)
			if e == nil {
				_ = os.Remove(reservation)
				unlockEnvironment(gate)
			}
		}()
		if e = c.populateInput(ctx, s, nasRoot, stage); e != nil {
			return e
		}
		if c.beforePublish != nil {
			c.beforePublish()
		}
		return ctx.Err()
	}, func(stage string) error {
		gate, e := c.cache.lock(ctx, "admission", syscall.LOCK_EX)
		if e != nil {
			return e
		}
		defer unlockEnvironment(gate)
		if e = c.admit(ctx, key, 0); e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		return os.Rename(stage, root)
	})
	if err != nil {
		return "", err
	}
	if err = syncWorkerDir(c.cache.Root); err != nil {
		return "", err
	}
	signature, err := sharedSignature(payload)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	if c.verified == nil {
		c.verified = map[string]string{}
	}
	c.verified[key] = signature
	c.misses++
	if s.NAS != "" {
		var bytes uint64
		if len(s.Manifest) > 0 {
			bytes = uint64(s.bytes())
			if s.ManifestFile != "" {
				bytes = addDiskBytes(bytes, uint64(s.SizeBytes))
			}
		} else {
			bytes = uint64(s.SizeBytes)
		}
		c.nasBytes = addDiskBytes(c.nasBytes, bytes)
	}
	c.mu.Unlock()
	valid = true
	return payload, nil
}

// Only the calling attempt may publish. This detached work writes exclusively
// into its unique unpublished directory and cannot touch another build's data.
func (c *SharedInputCache) populateInput(ctx context.Context, s SharedInput, nasRoot, stage string) error {
	if c.beforeStage != nil {
		c.beforeStage(ctx)
	}
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	var err error
	if s.ManifestFile != "" {
		manifestPath, e := c.nasPathContext(ctx, nasRoot, s.ManifestFile)
		if e != nil {
			return e
		}
		if e = verifyFile(ctx, manifestPath, s.ref()); e != nil {
			return e
		}
		f, e := os.Open(manifestPath)
		if e != nil {
			return e
		}
		b, e := io.ReadAll(io.LimitReader(contextReader{ctx, f}, s.SizeBytes+1))
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
		// Hash the exact bytes decoded, guarding mutation between verify and read.
		if int64(len(b)) != s.SizeBytes || digestBytes(b) != s.SHA256 {
			return errors.New("shared manifest changed")
		}
		if e = json.Unmarshal(b, &s.Manifest); e != nil {
			return e
		}
		if len(s.Manifest) == 0 {
			return errors.New("shared tree manifest is empty")
		}
		if e = s.Manifest.validate(); e != nil {
			return e
		}
		if s.bytes() > s.UnpackedBytes {
			return errors.New("shared manifest exceeds unpacked_bytes")
		}
	}
	dst := filepath.Join(stage, "data")
	files := FileManifest{}
	if len(s.Manifest) > 0 {
		if err = os.Mkdir(dst, 0700); err != nil {
			return err
		}
		for name, ref := range s.Manifest {
			if err = sharedInputProgress(ctx); err != nil {
				return err
			}
			source, e := c.nasPathContext(ctx, nasRoot, filepath.ToSlash(filepath.Join(s.NAS, name)))
			if e != nil {
				return e
			}
			target := filepath.Join(dst, name)
			if e = copySharedFile(ctx, source, target, ref); e != nil {
				return e
			}
			files[name] = ref
		}
	} else {
		target := dst
		if s.Archive != "" {
			target = filepath.Join(stage, "archive")
		}
		if s.NAS != "" {
			source, e := c.nasPathContext(ctx, nasRoot, s.NAS)
			if e != nil {
				return e
			}
			if err = copySharedFile(ctx, source, target, s.ref()); err != nil {
				return err
			}
		} else {
			err = c.Client.DownloadFile(ctx, s.ref(), target)
			if err != nil {
				return err
			}
		}
		if s.Archive != "" {
			if err = unpackSharedArchive(ctx, target, dst, s.Archive, s.UnpackedBytes); err != nil {
				return err
			}
			if err = os.Remove(target); err != nil {
				return err
			}
			err = filepath.WalkDir(dst, func(p string, d os.DirEntry, e error) error {
				if err := sharedInputProgress(ctx); err != nil {
					return err
				}
				if e != nil {
					return e
				}
				if d.IsDir() {
					return nil
				}
				name, e := filepath.Rel(dst, p)
				if e != nil {
					return e
				}
				ref, e := identifyFileContext(ctx, p)
				if e == nil {
					files[filepath.ToSlash(name)] = ref
				}
				return e
			})
			if err != nil {
				return err
			}
		} else {
			files["."] = s.ref()
		}
	}
	if err = makeSharedReadOnlyContext(ctx, dst); err != nil {
		return err
	}
	b, err := json.Marshal(sharedEntry{files})
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(stage, "ready.json"), b, 0400); err != nil {
		return err
	}
	return syncModelDirectoryContext(ctx, stage)
}

func copySharedFile(ctx context.Context, source, target string, ref FileRef) error {
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	if err := noModelSymlinks(source, false); err != nil {
		return err
	}
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != ref.SizeBytes {
		return errors.New("shared NAS file size/type differs from manifest")
	}
	if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|info.Mode().Perm()&0111)
	if err != nil {
		return err
	}
	// A changing NAS original cannot consume more than its admitted bytes.
	_, err = io.CopyN(struct{ io.Writer }{out}, contextReader{ctx, in}, ref.SizeBytes)
	err = errors.Join(err, out.Sync(), out.Close())
	if err != nil {
		return err
	}
	if err = os.Chmod(target, 0400|info.Mode().Perm()&0111); err != nil {
		return err
	}
	return verifyFile(ctx, target, ref)
}

func makeSharedReadOnly(root string) error {
	return makeSharedReadOnlyContext(context.Background(), root)
}
func makeSharedReadOnlyContext(ctx context.Context, root string) error {
	return filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if err := sharedInputProgress(ctx); err != nil {
			return err
		}
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if err := sharedInputProgress(ctx); err != nil {
			return err
		}
		mode := os.FileMode(0400) | info.Mode().Perm()&0111
		if d.IsDir() {
			mode = 0500
		}
		if err := os.Chmod(p, mode); err != nil {
			return err
		}
		return sharedInputProgress(ctx)
	})
}
func verifySharedEntry(ctx context.Context, root string, files FileManifest) error {
	seen := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if info.Mode().Perm()&0222 != 0 {
			return errors.New("shared input cache is writable")
		}
		if d.IsDir() {
			return nil
		}
		name, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		ref, ok := files[filepath.ToSlash(name)]
		if !ok {
			return errors.New("unexpected shared cache file")
		}
		seen++
		return verifyFile(ctx, p, ref)
	})
	if err == nil && seen != len(files) {
		return errors.New("shared cache manifest incomplete")
	}
	return err
}

// Compare inode/ctime as well as size/mtime: restoring a tampered file's mtime
// cannot evade detection. Access time is excluded because jobs read these files.
func sharedSignature(root string) (string, error) {
	type entry struct {
		Name   string
		Size   int64
		Mode   os.FileMode
		MTime  int64
		Inode  uint64
		Change any
	}
	entries := []entry{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("shared cache contains a link")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("shared cache metadata unavailable")
		}
		v := reflect.ValueOf(stat).Elem()
		ct := v.FieldByName("Ctim")
		if !ct.IsValid() {
			ct = v.FieldByName("Ctimespec")
		}
		if !ct.IsValid() {
			return errors.New("shared cache change time unavailable")
		}
		name, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		entries = append(entries, entry{name, info.Size(), info.Mode(), info.ModTime().UnixNano(), stat.Ino, ct.Interface()})
		return nil
	})
	return identityHash(entries), err
}
func (c *SharedInputCache) admit(ctx context.Context, key string, need int64) error {
	type victim struct {
		name string
		used int64
	}
	entries, err := os.ReadDir(c.cache.Root)
	if err != nil {
		return err
	}
	victims := []victim{}
	var reserved int64
	for _, d := range entries {
		if strings.HasSuffix(d.Name(), ".reservation") {
			k := strings.TrimSuffix(d.Name(), ".reservation")
			if (FileRef{SHA256: k}).validate() != nil || k == key {
				continue
			}
			lock, e := c.cache.tryLock(k + ".build")
			if e != nil {
				return e
			}
			if lock != nil {
				e = os.Remove(filepath.Join(c.cache.Root, d.Name()))
				unlockEnvironment(lock)
				if e != nil && !os.IsNotExist(e) {
					return e
				}
				continue
			}
			data, e := os.ReadFile(filepath.Join(c.cache.Root, d.Name()))
			if e != nil {
				return e
			}
			n, e := strconv.ParseInt(string(data), 10, 64)
			if e != nil || n < 0 || n > int64(^uint64(0)>>1)-reserved {
				return errors.New("invalid shared input reservation")
			}
			// Staged files grow monotonically while admission is unlocked. Read
			// their logical size before measuring allocated usage/free space, so
			// writes during the scan only make the estimate more conservative.
			var written int64
			for _, part := range []string{"data", "archive"} {
				size, e := environmentSize(filepath.Join(c.cache.Root, k+".partial", part))
				if e != nil && !os.IsNotExist(e) {
					return e
				}
				written += size
			}
			reserved += max(0, n-written)
			continue
		}
		if strings.HasSuffix(d.Name(), ".partial") {
			k := strings.TrimSuffix(d.Name(), ".partial")
			if (FileRef{SHA256: k}).validate() != nil {
				continue
			}
			lock, e := c.cache.tryLock(k + ".build")
			if e != nil {
				return e
			}
			if lock != nil {
				e = removeSharedEntry(filepath.Join(c.cache.Root, d.Name()))
				unlockEnvironment(lock)
				if e != nil {
					return e
				}
			}
			continue
		}
		if !d.IsDir() || (FileRef{SHA256: d.Name()}).validate() != nil || d.Name() == key {
			continue
		}
		info, e := os.Stat(filepath.Join(c.cache.Root, d.Name(), "ready.json"))
		if e != nil {
			return e
		}
		victims = append(victims, victim{d.Name(), info.ModTime().UnixNano()})
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i].used < victims[j].used })
	enough := func() (bool, error) {
		used, e := c.cache.DiskUsage()
		if e != nil {
			return false, e
		}
		disk, e := workerDiskProbe(c.cache.Root)
		if e != nil {
			return false, e
		}
		floor := diskFreeFloor(disk.TotalBytes)
		if reserved > int64(^uint64(0)>>1)-need {
			return false, nil
		}
		projected := need + reserved
		return projected <= c.cache.Limit-used && disk.FreeBytes >= floor && uint64(projected) <= disk.FreeBytes-floor, nil
	}
	for _, v := range victims {
		ok, e := enough()
		if e != nil {
			return e
		}
		if ok {
			return nil
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		use, e := c.cache.tryLock(v.name + ".use")
		if e != nil {
			return e
		}
		if use == nil {
			continue
		}
		e = removeSharedEntry(filepath.Join(c.cache.Root, v.name))
		unlockEnvironment(use)
		if e != nil {
			return e
		}
		c.mu.Lock()
		delete(c.verified, v.name)
		c.mu.Unlock()
	}
	ok, err := enough()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("disk_headroom: shared input cache full; active entries protected")
	}
	return nil
}
func unpackSharedArchive(ctx context.Context, source, dest, format string, limit int64) error {
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	defer f.Close()
	var reader io.Reader = contextReader{ctx, f}
	if format == "tar.gz" {
		z, e := gzip.NewReader(reader)
		if e != nil {
			return e
		}
		defer z.Close()
		reader = z
	}
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	tr := tar.NewReader(contextReader{ctx, reader})
	var used int64
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		h, e := tr.Next()
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		name := strings.TrimSuffix(strings.TrimPrefix(h.Name, "./"), "/")
		if name == "" && h.Typeflag == tar.TypeDir {
			continue
		}
		if !safeRelative(name) {
			return errors.New("unsafe shared archive path")
		}
		target := filepath.Join(dest, name)
		switch h.Typeflag {
		case tar.TypeDir:
			if e = os.MkdirAll(target, 0700); e != nil {
				return e
			}
		case tar.TypeReg, tar.TypeRegA:
			if h.Size < 0 || h.Size > limit-used {
				return errors.New("shared archive exceeds unpacked_bytes")
			}
			used += h.Size
			if e = os.MkdirAll(filepath.Dir(target), 0700); e != nil {
				return e
			}
			out, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|os.FileMode(h.Mode)&0111)
			if e != nil {
				return e
			}
			_, e = io.CopyN(out, tr, h.Size)
			e = errors.Join(e, out.Sync(), out.Close())
			if e != nil {
				return e
			}
		default:
			return errors.New("shared archives require regular files and directories")
		}
	}
}

func digestBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
