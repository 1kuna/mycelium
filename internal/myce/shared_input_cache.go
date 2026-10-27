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
	"strings"
	"sync"
	"syscall"
)

// Reuse the environment cache's OS locking and state-root initialization. Data
// entries have their own integrity records and no environment/catalog lifecycle.
type SharedInputCache struct {
	cache    EnvironmentCache
	Client   Client
	mu       sync.Mutex
	verified map[string]string
}
type sharedEntry struct {
	Files FileManifest `json:"files"`
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
		return payload, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	// Serialize reservations and publication, so concurrent builders cannot each
	// spend the same headroom. Active readers hold only their per-key use lock.
	gate, err := c.cache.lock(ctx, "admission", syscall.LOCK_EX)
	if err != nil {
		return "", err
	}
	defer unlockEnvironment(gate)
	stage := root + ".partial"
	if err = removeSharedEntry(stage); err != nil {
		return "", err
	}
	if s.ManifestFile != "" {
		manifestPath, e := confinedPath(nasRoot, s.ManifestFile)
		if e != nil {
			return "", e
		}
		if e = verifyFile(ctx, manifestPath, s.ref()); e != nil {
			return "", e
		}
		f, e := os.Open(manifestPath)
		if e != nil {
			return "", e
		}
		b, e := io.ReadAll(io.LimitReader(f, s.SizeBytes+1))
		e = errors.Join(e, f.Close())
		if e != nil {
			return "", e
		}
		// Hash the exact bytes decoded, guarding mutation between verify and read.
		if int64(len(b)) != s.SizeBytes || digestBytes(b) != s.SHA256 {
			return "", errors.New("shared manifest changed")
		}
		if e = json.Unmarshal(b, &s.Manifest); e != nil {
			return "", e
		}
		if len(s.Manifest) == 0 {
			return "", errors.New("shared tree manifest is empty")
		}
		if e = s.Manifest.validate(); e != nil {
			return "", e
		}
		if s.bytes() > s.UnpackedBytes {
			return "", errors.New("shared manifest exceeds unpacked_bytes")
		}
	}
	need := s.bytes()
	if s.Archive != "" {
		if s.SizeBytes > int64(^uint64(0)>>1)-need {
			return "", errors.New("shared input size overflow")
		}
		need += s.SizeBytes
	}
	if err = c.admit(ctx, key, need); err != nil {
		return "", err
	}
	if err = os.Mkdir(stage, 0700); err != nil {
		return "", err
	}
	defer removeSharedEntry(stage)
	dst := filepath.Join(stage, "data")
	files := FileManifest{}
	if len(s.Manifest) > 0 {
		if err = os.Mkdir(dst, 0700); err != nil {
			return "", err
		}
		for name, ref := range s.Manifest {
			source, e := confinedPath(nasRoot, filepath.ToSlash(filepath.Join(s.NAS, name)))
			if e != nil {
				return "", e
			}
			target := filepath.Join(dst, name)
			if e = copySharedFile(ctx, source, target, ref); e != nil {
				return "", e
			}
			files[name] = ref
		}
	} else {
		target := dst
		if s.Archive != "" {
			target = filepath.Join(stage, "archive")
		}
		if s.NAS != "" {
			source, e := confinedPath(nasRoot, s.NAS)
			if e != nil {
				return "", e
			}
			if err = copySharedFile(ctx, source, target, s.ref()); err != nil {
				return "", err
			}
		} else {
			err = c.Client.DownloadFile(ctx, s.ref(), target)
			if err != nil {
				return "", err
			}
		}
		if s.Archive != "" {
			if err = unpackSharedArchive(ctx, target, dst, s.Archive, s.UnpackedBytes); err != nil {
				return "", err
			}
			if err = os.Remove(target); err != nil {
				return "", err
			}
			err = filepath.WalkDir(dst, func(p string, d os.DirEntry, e error) error {
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
				ref, e := IdentifyFile(p)
				if e == nil {
					files[filepath.ToSlash(name)] = ref
				}
				return e
			})
			if err != nil {
				return "", err
			}
		} else {
			files["."] = s.ref()
		}
	}
	if err = makeSharedReadOnly(dst); err != nil {
		return "", err
	}
	b, err := json.Marshal(sharedEntry{files})
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(stage, "ready.json"), b, 0400); err != nil {
		return "", err
	}
	if err = c.admit(ctx, key, 0); err != nil {
		return "", err
	}
	if err = syncModelDirectory(stage); err != nil {
		return "", err
	}
	if err = os.Rename(stage, root); err != nil {
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
	c.mu.Unlock()
	return payload, nil
}
func copySharedFile(ctx context.Context, source, target string, ref FileRef) error {
	if err := noModelSymlinks(source, false); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
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
	_, err = io.CopyN(out, in, ref.SizeBytes)
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
	return filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		mode := os.FileMode(0400) | info.Mode().Perm()&0111
		if d.IsDir() {
			mode = 0500
		}
		return os.Chmod(p, mode)
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
	for _, d := range entries {
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
		return need <= c.cache.Limit-used && disk.FreeBytes >= floor && uint64(need) <= disk.FreeBytes-floor, nil
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
	var reader io.Reader = f
	if format == "tar.gz" {
		z, e := gzip.NewReader(f)
		if e != nil {
			return e
		}
		defer z.Close()
		reader = z
	}
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	tr := tar.NewReader(reader)
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
