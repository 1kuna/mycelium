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
	"path/filepath"
	"strings"
	"syscall"
)

// FetchModel explicitly downloads one master file. Relative names start with
// the bundle directory (or the filename for a single-file model) under root.
// Staging is a sibling of that first component, never part of the bundle.
func FetchModel(ctx context.Context, raw, root, relative string, size int64, sum string) error {
	return fetchModel(ctx, raw, root, relative, size, sum, syncWorkerDir, os.Stderr)
}

func fetchModel(ctx context.Context, raw, root, relative string, size int64, sum string, syncDir func(string) error, report io.Writer) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return errors.New("fetch requires an HTTPS file URL")
	}
	hash, err := hex.DecodeString(sum)
	if size < 0 || err != nil || len(hash) != sha256.Size {
		return errors.New("fetch requires size and SHA-256")
	}
	if root == "" || relative == "." || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("fetch needs a model root and a relative destination")
	}
	// Resolve the operator's root (including platform aliases such as /var).
	// Below it, reject symlinks instead of redirecting staging or publication.
	if err = fetchMkdir(root, root, syncDir); err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	first := strings.Split(relative, string(filepath.Separator))[0]
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.ToLower(first))))
	stage := filepath.Join(root, ".myce-fetch-"+key)
	if err = noModelSymlinks(stage, true); err != nil {
		return err
	}
	if err = fetchMkdir(stage, root, syncDir); err != nil {
		return err
	}
	// A permanent regular file coordinates NAS clients too: directory flock
	// can fall back to host-local locking on network filesystems. Never unlink
	// this file, so every fetch contends on the same inode across publication.
	lock, err := os.OpenFile(filepath.Join(stage, "fetch.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("model bundle fetch is in use; retry: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = errors.Join(lock.Sync(), syncDir(stage)); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	quarantine := filepath.Join(stage, "quarantine")
	if err = noModelSymlinks(quarantine, true); err != nil {
		return err
	}
	// Also sync existing directories on entry: a previous invocation may have
	// created/moved entries and then failed to sync them. Never reuse their names
	// until both quarantine and staging are durable.
	if err = fetchMkdir(quarantine, root, syncDir); err != nil {
		return err
	}
	dest := filepath.Join(root, relative)
	if err = noModelSymlinks(dest, true); err != nil {
		return err
	}
	if err = fetchMkdir(filepath.Dir(dest), root, syncDir); err != nil {
		return err
	}
	a, err := os.Stat(stage)
	if err != nil {
		return err
	}
	b, err := os.Stat(filepath.Dir(dest))
	if err != nil {
		return err
	}
	if a.Sys().(*syscall.Stat_t).Dev != b.Sys().(*syscall.Stat_t).Dev {
		return errors.New("fetch staging and bundle must be on the same filesystem")
	}
	m := Model{Name: relative, SizeBytes: size, SHA256: strings.ToLower(sum)}
	if _, err = os.Lstat(dest); err == nil {
		// A process may have exited after rename but before syncing directories.
		// Reverify and finish durability; never replace an existing master.
		if err = verifyModel(dest, m); err != nil {
			return fmt.Errorf("destination already exists: %w", err)
		}
		return errors.Join(syncDir(filepath.Dir(dest)), syncDir(stage))
	} else if !os.IsNotExist(err) {
		return err
	}
	key = fmt.Sprintf("%x", sha256.Sum256([]byte(relative)))
	partial := filepath.Join(stage, key+".partial")
	// Retry a corrupt resumed prefix once from zero. A corrupt fresh response
	// is quarantined and reported, then fails rather than downloading forever.
	for fresh := false; ; fresh = true {
		st, err := os.Lstat(partial)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if st != nil && !st.Mode().IsRegular() {
			return errors.New("fetch partial is not a regular file")
		}
		offset := int64(0)
		if st != nil {
			offset = st.Size()
		}
		resumed := offset > 0 && !fresh
		if offset < size || st == nil {
			if err = fetchModelBytes(ctx, raw, partial, offset, size, func() error {
				return quarantineModelPartial(partial, quarantine, "Range ignored", syncDir, report)
			}); err != nil {
				return errors.Join(err, syncDir(stage))
			}
		}
		if err = verifyModel(partial, m); err != nil {
			if qerr := quarantineModelPartial(partial, quarantine, "invalid partial", syncDir, report); qerr != nil {
				return errors.Join(err, qerr)
			}
			if resumed || (st != nil && !fresh) {
				continue
			}
			return fmt.Errorf("fetch failed verification (bytes quarantined): %w", err)
		}
		if err = syncModelFetchFile(partial); err != nil {
			return err
		}
		if err = syncDir(stage); err != nil {
			return err
		}
		if err = os.Rename(partial, dest); err != nil {
			return err
		}
		if err = errors.Join(syncDir(filepath.Dir(dest)), syncDir(stage)); err != nil {
			// Roll publication back on directory-sync failure. The verified file
			// returns to staging for an explicit retry, without a second download.
			if rollback := os.Rename(dest, partial); rollback != nil {
				return errors.Join(err, fmt.Errorf("fetch publication rollback: %w", rollback))
			}
			return errors.Join(err, syncDir(stage), syncDir(filepath.Dir(dest)))
		}
		return nil
	}
}

func fetchModelBytes(ctx context.Context, raw, partial string, offset, size int64, restart func() error) error {
	req, err := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if err != nil {
		return err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 && res.StatusCode == http.StatusOK {
		// Range was ignored: this body starts at zero. Preserve the unverified
		// prefix before reusing its name, since it could contain corrupt bytes.
		if err = restart(); err != nil {
			return err
		}
		offset = 0
	} else if offset > 0 {
		if res.StatusCode != http.StatusPartialContent || res.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", offset, size-1, size) {
			return errors.New("fetch: invalid range response; partial retained")
		}
	} else if res.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch: HTTP %d", res.StatusCode)
	}
	f, err := os.OpenFile(partial, flags, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Seek(offset, io.SeekStart); err == nil {
		// One excess byte detects oversized bodies without an overflowing size+1.
		_, err = io.CopyN(f, contextReader{ctx, res.Body}, size-offset)
		if err == nil {
			_, err = io.Copy(f, io.LimitReader(contextReader{ctx, res.Body}, 1))
		}
	}
	// Preserve interrupted prefixes, including a durable file flush on error.
	return errors.Join(err, f.Sync(), f.Close())
}

func syncModelFetchFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func quarantineModelPartial(partial, quarantine, reason string, syncDir func(string) error, report io.Writer) error {
	if err := syncModelFetchFile(partial); err != nil {
		return err
	}
	f, err := os.CreateTemp(quarantine, filepath.Base(partial)+"-*")
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(partial, f.Name()); err != nil {
		return err
	}
	fmt.Fprintf(report, "fetch: %s; partial preserved in %s\n", reason, f.Name())
	return errors.Join(syncDir(quarantine), syncDir(filepath.Dir(partial)))
}

// fetchMkdir creates dir and makes its entry durable. At and below base (the
// model root), every directory is synced on each call, existing or not, so a
// retry repairs an earlier failed sync. Above base, only directories this call
// creates are synced, each with its parent, and one whose sync fails is removed
// so a retry recreates it. Existing ancestors are never opened: a job's sandbox
// may deny reading them, and their entries were durable before this fetch (#1471).
func fetchMkdir(dir, base string, syncDir func(string) error) error {
	dir, base = filepath.Clean(dir), filepath.Clean(base)
	parent := filepath.Dir(dir)
	rel, err := filepath.Rel(base, dir)
	below := err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	if _, err := os.Stat(dir); err == nil {
		switch {
		case below:
			if err := fetchMkdir(parent, base, syncDir); err != nil {
				return err
			}
			return errors.Join(syncDir(dir), syncDir(parent))
		case dir == base:
			return syncDir(dir)
		default:
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if parent != dir {
		if err := fetchMkdir(parent, base, syncDir); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		if _, serr := os.Stat(dir); errors.Is(err, os.ErrExist) && serr == nil {
			return fetchMkdir(dir, base, syncDir) // created concurrently
		}
		return err
	}
	if err := errors.Join(syncDir(dir), syncDir(parent)); err != nil {
		if !below {
			_ = os.Remove(dir)
		}
		return err
	}
	return nil
}
