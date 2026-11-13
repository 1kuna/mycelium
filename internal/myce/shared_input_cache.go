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
	budget                 *workerDiskBudget
	cache                  EnvironmentCache
	automatic              bool
	readNAS                func(*os.File, []byte) (int, error)
	diskProbe              func(string) (DiskReading, error)
	Client                 Client
	mu                     sync.Mutex
	verified               map[string]string
	staging                map[*sharedInputStaging]struct{}
	beforeStage            func(context.Context)
	resolveNAS             func(root, relative string) (string, error)
	beforePublish          func()
	beforeVerify           func() // Test hook: a full re-hash of a cached entry starts.
	beforeAdmissionMeasure func() // Test hook: reservations counted, before disk measurement.
	afterArchiveReset      func() // Test hook: interrupted extraction removed.
	afterStageCancel       func() // Test hook: withdrawal cancellation has reached its writer.
	hits                   uint64
	misses                 uint64
	nasBytes               uint64
	capacity               *SharedInputCapacity
}

// sharedVerifiedRecord holds the metadata signature (size, mode, mtime, inode
// and change time of every file) taken after the entry's bytes last verified.
const sharedVerifiedRecord = "verified.json"

// A unique temporary name lets workers sharing one --file-cache re-verify the
// same entry at once without one rename failing the other's attempt.
func recordSharedVerification(root, signature string) error {
	f, err := os.CreateTemp(root, sharedVerifiedRecord+".*.tmp")
	if err != nil {
		return err
	}
	_, err = f.WriteString(signature)
	if err = errors.Join(err, f.Close()); err == nil {
		err = os.Rename(f.Name(), filepath.Join(root, sharedVerifiedRecord))
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

type sharedEntry struct {
	Files    FileManifest          `json:"files"`
	Recovery *sharedRecoverySource `json:"recovery,omitempty"`
}

func (c *SharedInputCache) nasPathContext(ctx context.Context, root, relative string) (string, error) {
	if err := sharedInputProgress(ctx); err != nil {
		return "", err
	}
	traceSharedInput(ctx, "resolve", "source")
	path, err := c.nasPath(root, relative)
	if err != nil {
		// Keep the source boundary, never the expanded worker mount path.
		return "", PathReadError("shared_inputs", "worker", "input", "worker-checkpoint-root", "", err)
	}
	return path, sharedInputProgress(ctx)
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
	defer c.refreshCapacityIfTracked()
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
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
	var unpin func()
	release := func() {
		once.Do(func() {
			if unpin != nil {
				unpin()
			}
			at := c.cache.Clock.Now()
			_ = os.Chtimes(filepath.Join(c.cache.Root, key, "ready.json"), at, at)
			unlockEnvironment(use)
			c.refreshCapacityIfTracked()
		})
	}
	path, err := c.ensure(ctx, s, nasRoot)
	if err == nil && sharedInputOwner(ctx) != nil {
		unpin, err = sharedInputOwner(ctx).pinSharedInput(c.cache.Root, key)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		release()
		return "", nil, err
	}
	return path, release, nil
}
func (c *SharedInputCache) ensure(ctx context.Context, s SharedInput, nasRoot string) (_ string, stageErr error) {
	ctx = context.WithValue(ctx, sharedInputTraceKey{}, &sharedInputTrace{detail: StagingCause{Operation: "shared_input", Stage: "prepare", PathClass: "cache"}})
	traceSharedFile(ctx, s.ref())
	defer func() { stageErr = tracedSharedError(ctx, stageErr) }()
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
		traceSharedInput(ctx, "metadata", "cache")
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
		// metadata requires full verification. A restart trusts the signature
		// recorded after the last verification, so unchanged files are not re-hashed.
		if previous == "" {
			if b, e := os.ReadFile(filepath.Join(root, sharedVerifiedRecord)); e == nil {
				previous = string(b)
			}
		}
		if previous != signature {
			pinned, err := sharedInputPinned(c.cache.Root, key)
			if err != nil {
				return "", err
			}
			if pinned {
				return "", errors.New("shared input cache changed while an attempt retains custody")
			}
			if c.beforeVerify != nil {
				c.beforeVerify()
			}
			traceSharedInput(ctx, "verify", "cache")
			if e = verifySharedEntry(ctx, payload, record.Files); e != nil {
				return "", e
			}
			if e = recordSharedVerification(root, signature); e != nil {
				return "", e
			}
		}
		c.mu.Lock()
		if c.verified == nil {
			c.verified = map[string]string{}
		}
		c.verified[key] = signature
		c.mu.Unlock()
		valid = true
		if c.budget != nil {
			if err = saveWorkerJSON(root, "recovery.json", sharedRecoverySource{Input: s, Root: nasRoot, Sources: s.sources, Model: s.model, Hub: s.hub}); err != nil {
				return "", err
			}
		}
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
	err = c.stageInputNamed(ctx, key, func(ctx context.Context, stage string) error {
		if c.budget != nil {
			if err := saveWorkerJSON(stage, "recovery.json", sharedRecoverySource{Input: s, Root: nasRoot, Sources: s.sources, Model: s.model, Hub: s.hub}); err != nil {
				return err
			}
		}
		floor, _ := ctx.Value(diskBudgetFloorKey{}).(uint64)
		// Name disk-budget waits and reclaim as admission, not generic prepare.
		traceSharedInput(ctx, "admission", "cache")
		release, err := c.budget.reserve(ctx, c.cache.Root, uint64(max(0, need)), floor)
		if err != nil {
			return err
		}
		defer release()
		// The stage build lock protects this local verification, without holding
		// admission while hashing. Completed archives no longer need transfer space.
		complete, e := completedSharedStage(ctx, stage)
		if e != nil {
			return e
		}
		stageNeed := need
		if complete {
			stageNeed = 0
		}
		traceSharedInput(ctx, "admission", "cache")
		// The writer owns this stage's build lock before reserving space.
		// Matching reservation/partial names reuse existing remaining-byte accounting.
		gate, e := c.cache.lock(ctx, "admission", syscall.LOCK_EX)
		if e != nil {
			return e
		}
		reservation := strings.TrimSuffix(stage, ".partial") + ".reservation"
		// We own this persistent stage's build lock. A reservation left by its
		// crashed writer belongs to us, never to another in-flight transfer.
		if e = os.Remove(reservation); os.IsNotExist(e) {
			e = nil
		}
		if e == nil {
			e = removeSharedEntry(root + ".partial")
		}
		if e == nil && !complete && s.Archive != "" {
			// Rebuilding an extracted tree shrinks staged bytes. Serialize that
			// reset with all accounting, then reserve its full expansion again.
			// Once admission is unlocked, this stage must only grow.
			e = removeSharedEntry(filepath.Join(stage, "data"))
			if e == nil && c.afterArchiveReset != nil {
				c.afterArchiveReset()
			}
		}
		if e == nil {
			// Retained local prefixes already occupy cache/free bytes. Reserve
			// only the remainder for this stage, without double charging it.
			var written int64
			for _, part := range []string{"data", "archive"} {
				n, err := environmentSize(filepath.Join(stage, part))
				if err != nil && !os.IsNotExist(err) {
					e = err
					break
				}
				written += n
			}
			if e == nil {
				e = c.admit(ctx, key, max(0, stageNeed-written))
			}
		}
		if e == nil {
			e = os.WriteFile(reservation, []byte(strconv.FormatInt(stageNeed, 10)), 0600)
		}
		c.mu.Lock()
		tracked := c.capacity != nil
		c.mu.Unlock()
		if tracked {
			c.refreshCapacityLocked()
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
		if !complete {
			if e = c.populateInput(ctx, s, nasRoot, stage); e != nil {
				return e
			}
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
	traceSharedInput(ctx, "sync", "cache")
	if err = syncWorkerDir(c.cache.Root); err != nil {
		return "", err
	}
	traceSharedInput(ctx, "metadata", "cache")
	signature, err := sharedSignature(payload)
	if err == nil {
		err = recordSharedVerification(root, signature)
	}
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

// Called under the stage build lock, before reserving any further transfer.
// Invalid unpublished metadata from an older interrupted writer is replaceable;
// payload verification failures still refuse publication.
func completedSharedStage(ctx context.Context, stage string) (bool, error) {
	if b, err := os.ReadFile(filepath.Join(stage, "ready.json")); err == nil {
		traceSharedInput(ctx, "verify", "cache")
		var record sharedEntry
		if err := json.Unmarshal(b, &record); err != nil {
			return false, os.Remove(filepath.Join(stage, "ready.json"))
		}
		if err := verifySharedEntry(ctx, filepath.Join(stage, "data"), record.Files); err != nil {
			return false, err
		}
		// An interruption after publishing ready.json may retain the archive.
		// Only a verified durable completion record makes it redundant.
		if err := os.Remove(filepath.Join(stage, "archive")); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return true, ctx.Err()
	} else if !os.IsNotExist(err) {
		return false, err
	}
	return false, nil
}

// Only the calling attempt may publish. This detached work writes exclusively
// into its unique unpublished directory and cannot touch another build's data.
func (c *SharedInputCache) populateInput(ctx context.Context, s SharedInput, nasRoot, stage string) (err error) {
	defer func() { observeStorageContext(ctx, err) }()
	if c.beforeStage != nil {
		c.beforeStage(ctx)
	}
	traceSharedFile(ctx, s.ref())
	if err := sharedInputProgress(ctx); err != nil {
		return err
	}
	if s.ManifestFile != "" {
		manifestPath, e := c.nasPathContext(ctx, nasRoot, s.ManifestFile)
		if e != nil {
			return e
		}
		traceSharedInput(ctx, "verify", "source")
		if e = verifyFile(ctx, manifestPath, s.ref()); e != nil {
			return e
		}
		traceSharedInput(ctx, "open", "source")
		f, e := os.Open(manifestPath)
		if e != nil {
			return e
		}
		traceSharedInput(ctx, "read", "source")
		b, e := io.ReadAll(io.LimitReader(contextReader{ctx, f}, s.SizeBytes+1))
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
		// Hash the exact bytes decoded, guarding mutation between verify and read.
		if int64(len(b)) != s.SizeBytes || digestBytes(b) != s.SHA256 {
			return errors.New("shared manifest changed")
		}
		traceSharedInput(ctx, "manifest", "source")
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
		if err = os.MkdirAll(dst, 0700); err != nil {
			return err
		}
		for name, ref := range s.Manifest {
			if err = sharedInputProgress(ctx); err != nil {
				return err
			}
			source := s.sources[name]
			target := filepath.Join(dst, name)
			if source == "" && s.fetch != nil {
				if e := s.fetch(ctx, name, target, ref); e != nil {
					return e
				}
				files[name] = ref
				continue
			}
			if source == "" {
				var e error
				source, e = c.nasPathContext(ctx, nasRoot, filepath.ToSlash(filepath.Join(s.NAS, name)))
				if e != nil {
					return e
				}
			}
			if e := copyNASInput(ctx, c.Client, source, target, &ref, c.readNAS); e != nil {
				return e
			}
			files[name] = ref
		}
	} else {
		target := dst
		archiveReady := false
		if s.Archive != "" {
			target = filepath.Join(stage, "archive")
			if info, e := os.Lstat(target); e == nil && info.Mode().IsRegular() && info.Size() == s.SizeBytes {
				traceSharedInput(ctx, "verify", "cache")
				if e = verifyFile(ctx, target, s.ref()); e != nil {
					return e
				}
				archiveReady = true
			}
		}
		if archiveReady {
			// A verified retained archive needs no source resolution or transfer.
		} else if s.NAS != "" {
			source, e := c.nasPathContext(ctx, nasRoot, s.NAS)
			if e != nil {
				return e
			}
			if err = copyNASInput(ctx, c.Client, source, target, inputRef(s), c.readNAS); err != nil {
				return err
			}
		} else {
			err = c.Client.recoverInputTransfer(ctx, s.ref(), "shared_input", func(one Client) error { return one.DownloadFile(ctx, s.ref(), target) })
			if err != nil {
				return err
			}
		}
		if s.Archive != "" {
			traceSharedInput(ctx, "extract", "cache")
			// Admission already reset any interrupted extraction under its lock.
			// Reuse the verified local archive and only grow the reserved tree.
			if err = unpackSharedArchive(ctx, target, dst, s.Archive, s.UnpackedBytes); err != nil {
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
	traceSharedInput(ctx, "permissions", "cache")
	if err = makeSharedReadOnlyContext(ctx, dst); err != nil {
		return err
	}
	b, err := json.Marshal(sharedEntry{Files: files, Recovery: &sharedRecoverySource{Input: s, Root: nasRoot, Sources: s.sources, Model: s.model, Hub: s.hub}})
	if err != nil {
		return err
	}
	// Persist the payload before atomically declaring it complete. Until then
	// retain the verified archive, so an interrupted extraction can rebuild locally.
	traceSharedInput(ctx, "sync", "cache")
	if err = syncModelDirectoryContext(ctx, stage); err != nil {
		return err
	}
	if err = writeSharedReady(ctx, stage, b); err != nil {
		return errors.Join(errFileTransferLocal, err)
	}
	if s.Archive != "" {
		traceSharedInput(ctx, "metadata", "cache")
		if err = os.Remove(filepath.Join(stage, "archive")); err != nil {
			return err
		}
	}
	traceSharedInput(ctx, "sync", "cache")
	return syncWorkerDir(stage)
}

func writeSharedReady(ctx context.Context, stage string, b []byte) error {
	traceSharedInput(ctx, "manifest", "cache")
	f, err := os.CreateTemp(stage, "ready.*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	if err == nil {
		err = f.Chmod(0400)
	}
	if err == nil {
		traceSharedInput(ctx, "sync", "cache")
		err = f.Sync()
	}
	// Snapshot a sync failure before tracing the subsequent close boundary.
	err = tracedSharedError(ctx, err)
	traceSharedInput(ctx, "close", "cache")
	err = errors.Join(err, tracedSharedError(ctx, f.Close()))
	if err == nil {
		traceSharedInput(ctx, "manifest", "cache")
		err = os.Rename(f.Name(), filepath.Join(stage, "ready.json"))
	}
	if err == nil {
		traceSharedInput(ctx, "sync", "cache")
		err = syncWorkerDir(stage)
	}
	return err
}

func inputRef(s SharedInput) *FileRef { ref := s.ref(); return &ref }

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
				partial := filepath.Join(c.cache.Root, d.Name())
				if info, resumeErr := os.Lstat(filepath.Join(partial, "resume")); resumeErr == nil && info.Mode().IsRegular() {
					victims = append(victims, victim{d.Name(), info.ModTime().UnixNano()})
				} else {
					e = removeSharedEntry(partial)
				}
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
	var cacheAvailable, diskAvailable, limit int64
	enough := func() (bool, error) {
		if c.beforeAdmissionMeasure != nil {
			c.beforeAdmissionMeasure()
		}
		used, e := c.cache.DiskUsage()
		if e != nil {
			return false, e
		}
		explicit := c.cache.Limit
		if c.automatic {
			explicit = 0
		}
		var free int64
		limit, free, e = inputCacheBudget(c.cache.Root, used, explicit, c.diskProbe, c.budget.foreignHeadroom(c.cache.Root))
		if e != nil {
			return false, e
		}
		if reserved > int64(^uint64(0)>>1)-need {
			return false, nil
		}
		projected := need + reserved
		cacheAvailable = max(0, limit-used-reserved)
		diskAvailable = free
		return projected <= limit-used && projected <= free, nil
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
		// A retained partial is protected by its writer's build lock, not a
		// published-entry use lock. Never unlink a detached writer's files.
		lockKey := v.name + ".use"
		if strings.HasSuffix(v.name, ".partial") {
			lockKey = strings.TrimSuffix(v.name, ".partial") + ".build"
		}
		use, e := c.cache.tryLock(lockKey)
		if e != nil {
			return e
		}
		if use == nil {
			continue
		}
		pinned, e := sharedInputPinned(c.cache.Root, v.name)
		if e != nil || pinned {
			unlockEnvironment(use)
			if e != nil {
				return e
			}
			continue
		}
		if c.budget != nil {
			var recovery sharedRecoverySource
			data, err := os.ReadFile(filepath.Join(c.cache.Root, v.name, "recovery.json"))
			if err != nil || json.Unmarshal(data, &recovery) != nil || c.recoverable(ctx, &recovery, filepath.Join(c.cache.Root, v.name)) != nil {
				unlockEnvironment(use)
				continue
			}
			key := v.name
			if strings.HasSuffix(v.name, ".partial") {
				data, err := os.ReadFile(filepath.Join(c.cache.Root, v.name, "resume"))
				if err != nil {
					unlockEnvironment(use)
					continue
				}
				key = string(data)
			}
			referenced, err := c.queueReferenced(ctx, key, &recovery)
			if err != nil || referenced {
				unlockEnvironment(use)
				continue
			}
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
		if need > cacheAvailable {
			return &stagingCapacityError{required: need, available: min(cacheAvailable, max(0, diskAvailable-reserved)), limit: limit, capacity: "shared_input_cache"}
		}
		return &stagingCapacityError{required: need, available: max(0, diskAvailable-reserved), capacity: "disk_free_floor"}
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
