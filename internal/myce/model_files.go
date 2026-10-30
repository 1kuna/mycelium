package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// ModelFilesIdentity hashes a JSON array of [path,size,lowercase SHA-256]
// tuples sorted by path. The input is neither reordered nor modified.
func ModelFilesIdentity(files []ModelFile) (int64, string, error) {
	if len(files) == 0 {
		return 0, "", errors.New("model bundle requires files")
	}
	sorted := append([]ModelFile(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	tuples := make([][3]any, 0, len(sorted))
	seen := map[string]bool{}
	var total int64
	for _, f := range sorted {
		if f.Path == "" || f.Path == "." || f.Path == ".." || path.IsAbs(f.Path) || path.Clean(f.Path) != f.Path || strings.ContainsAny(f.Path, "\\\x00") || strings.HasPrefix(f.Path, "../") {
			return 0, "", fmt.Errorf("invalid bundle file path %q", f.Path)
		}
		// Case-folded duplicates are unsafe on laptop filesystems.
		key := strings.ToLower(f.Path)
		if seen[key] {
			return 0, "", fmt.Errorf("duplicate bundle file path %q", f.Path)
		}
		for p := path.Dir(key); p != "."; p = path.Dir(p) {
			if seen[p] {
				return 0, "", errors.New("bundle file/directory collision")
			}
		}
		for p := range seen {
			if strings.HasPrefix(p, key+"/") {
				return 0, "", errors.New("bundle file/directory collision")
			}
		}
		seen[key] = true
		if f.SizeBytes < 0 || f.SizeBytes > math.MaxInt64-total {
			return 0, "", errors.New("invalid bundle file size")
		}
		hash, e := hex.DecodeString(f.SHA256)
		if e != nil || len(hash) != sha256.Size {
			return 0, "", errors.New("invalid bundle file SHA-256")
		}
		total += f.SizeBytes
		tuples = append(tuples, [3]any{f.Path, f.SizeBytes, strings.ToLower(f.SHA256)})
	}
	data, _ := json.Marshal(tuples)
	sum := sha256.Sum256(data)
	return total, hex.EncodeToString(sum[:]), nil
}

func validateModelFiles(m Model) error {
	total, sum, e := ModelFilesIdentity(m.Files)
	if e != nil {
		return e
	}
	if total != m.SizeBytes || sum != strings.ToLower(m.SHA256) {
		return errors.New("model bundle manifest identity mismatch")
	}
	return nil
}

// Reject symlinks in every component, including the configured roots. Missing
// suffixes are allowed only for paths about to be created in the local cache.
func noModelSymlinks(p string, missing bool) error {
	p, e := filepath.Abs(p)
	if e != nil {
		return e
	}
	cur := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(p, cur), string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if missing && os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("model path contains symlink: %s", cur)
		}
	}
	return nil
}

// cacheEntrySize counts file bytes, not directory inode sizes, and refuses
// symlinks/special files rather than following them out of the cache.
func cacheEntrySize(p string) (int64, error) {
	var total int64
	e := filepath.WalkDir(p, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in model cache")
		}
		if d.IsDir() {
			return nil
		}
		st, e := d.Info()
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() {
			return errors.New("nonregular model cache file")
		}
		if st.Size() > math.MaxInt64-total {
			return errors.New("model cache size overflow")
		}
		total += st.Size()
		return nil
	})
	return total, e
}

func verifyModelFiles(ctx context.Context, root string, m Model) error {
	if e := validateModelFiles(m); e != nil {
		return e
	}
	if e := noModelSymlinks(root, false); e != nil {
		return e
	}
	st, e := os.Stat(root)
	if e != nil {
		return e
	}
	if !st.IsDir() {
		return errors.New("model bundle is not a directory")
	}
	expected := map[string]ModelFile{}
	for _, f := range m.Files {
		expected[f.Path] = f
	}
	e = filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in model bundle")
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		f, ok := expected[filepath.ToSlash(rel)]
		if !ok {
			return errors.New("unlisted file in model bundle")
		}
		st, e := d.Info()
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() {
			return errors.New("nonregular model bundle file")
		}
		if e = verifyModelContext(ctx, p, Model{Name: m.Name + "/" + f.Path, SizeBytes: f.SizeBytes, SHA256: f.SHA256}); e != nil {
			return e
		}
		delete(expected, f.Path)
		return nil
	})
	if e != nil {
		return e
	}
	if len(expected) != 0 {
		return errors.New("missing model bundle files")
	}
	return nil
}

var errInvalidBundleCopy = errors.New("invalid model bundle copy")

func (c *ModelCache) copyModelFiles(ctx context.Context, source, temp string, m Model, r *CopyReport, copied *int64) error {
	if e := noModelSymlinks(source, false); e != nil {
		return e
	}
	if e := noModelSymlinks(temp, true); e != nil {
		return e
	}
	if e := os.MkdirAll(temp, 0700); e != nil {
		return e
	}
	files := append([]ModelFile(nil), m.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	for _, f := range files {
		src, dst := filepath.Join(source, filepath.FromSlash(f.Path)), filepath.Join(temp, filepath.FromSlash(f.Path))
		if e := noModelSymlinks(src, false); e != nil {
			return e
		}
		if e := noModelSymlinks(dst, true); e != nil {
			return e
		}
		st, e := os.Stat(src)
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() || st.Size() != f.SizeBytes {
			return fmt.Errorf("%w: NAS bundle file size or type mismatch", errInvalidBundleCopy)
		}
		if e = os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
			return e
		}
		if e = c.copyBundleFile(ctx, src, dst, f, r, copied, cacheKey(m)); e != nil {
			return e
		}
	}
	if e := verifyModelFiles(ctx, temp, m); e != nil {
		if ctx.Err() != nil {
			return e
		}
		return errors.Join(errInvalidBundleCopy, e)
	}
	// Sync nested directories before publishing the complete bundle.
	return syncModelDirectory(temp)
}

func syncModelDirectory(root string) error {
	return syncModelDirectoryContext(context.Background(), root)
}
func syncModelDirectoryContext(ctx context.Context, root string) error {
	return filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if err := sharedInputProgress(ctx); err != nil {
			return err
		}
		if e != nil {
			return e
		}
		if !d.IsDir() {
			return nil
		}
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		err := errors.Join(f.Sync(), f.Close())
		if err != nil {
			return err
		}
		return sharedInputProgress(ctx)
	})
}

func (c *ModelCache) copyBundleFile(ctx context.Context, src, dst string, f ModelFile, r *CopyReport, copied *int64, key string) error {
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer out.Close()
	st, e := out.Stat()
	if e != nil {
		return e
	}
	offset := st.Size()
	if offset > f.SizeBytes {
		return fmt.Errorf("%w: bundle partial exceeds declared size", errInvalidBundleCopy)
	}
	h := sha256.New()
	if _, e = io.CopyN(h, contextReader{ctx, out}, offset); e != nil {
		return e
	}
	if _, e = in.Seek(offset, io.SeekStart); e != nil {
		return e
	}
	buf := make([]byte, 1024*1024)
	n := offset
	for {
		if c.beforeChunk != nil {
			c.beforeChunk(ctx)
		}
		if e = ctx.Err(); e != nil {
			break
		}
		count, readErr := in.Read(buf[:min(int64(len(buf)), f.SizeBytes-n+1)])
		if n+int64(count) > f.SizeBytes {
			e = fmt.Errorf("%w: NAS bundle file exceeds declared size", errInvalidBundleCopy)
			break
		}
		if count > 0 {
			written, err := io.MultiWriter(out, h).Write(buf[:count])
			n += int64(written)
			*copied += int64(written)
			r.Bytes += int64(written)
			c.progress.Store(key, *r)
			if err != nil {
				e = err
				break
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			e = readErr
			break
		}
	}
	if e == nil && (n != f.SizeBytes || hex.EncodeToString(h.Sum(nil)) != strings.ToLower(f.SHA256)) {
		e = fmt.Errorf("%w: NAS bundle file SHA-256 mismatch", errInvalidBundleCopy)
	}
	return errors.Join(e, out.Sync(), out.Close())
}

// Cross-filesystem eviction retains a verified recovery directory before mv
// removes any source bytes. Failed recovery leaves the original intact.
func (c *ModelCache) recoverModelDirectory(ctx context.Context, src, dst string) error {
	if e := noModelSymlinks(src, false); e != nil {
		return e
	}
	if e := os.Mkdir(dst, 0700); e != nil {
		return e
	}
	e := filepath.WalkDir(src, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in evicted bundle")
		}
		rel, e := filepath.Rel(src, p)
		if e != nil {
			return e
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			if rel == "." {
				return nil
			}
			return os.Mkdir(target, 0700)
		}
		st, e := d.Info()
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() {
			return errors.New("nonregular evicted bundle file")
		}
		in, e := os.Open(p)
		if e != nil {
			return e
		}
		out, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			_ = in.Close()
			return e
		}
		hash := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(out, hash), contextReader{ctx, in})
		e = errors.Join(copyErr, in.Close(), out.Sync(), out.Close())
		if e != nil {
			return e
		}
		if n != st.Size() {
			return errors.New("evicted bundle changed during recovery")
		}
		return verifyModelContext(ctx, target, Model{Name: "evicted bundle", SizeBytes: n, SHA256: hex.EncodeToString(hash.Sum(nil))})
	})
	if e != nil {
		return e
	}
	if e = syncModelDirectory(dst); e != nil {
		return e
	}
	parent, e := os.Open(filepath.Dir(dst))
	if e != nil {
		return e
	}
	return errors.Join(parent.Sync(), parent.Close())
}
