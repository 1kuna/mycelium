package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

// attemptTempDir returns the TMPDIR an attempt sees. The real folder stays at
// work/.tmp inside the managed attempt tree, so custody, retention and sandbox
// grants are unchanged.
//
// A Unix-domain socket path must fit sun_path (108 bytes on Linux, 104 on
// Darwin). The attempt tree (spool/job-files/JOB/ATTEMPT/.tmp) alone can use
// most of that, leaving no room for the socket names that servers create in
// TMPDIR: vLLM binds ZMQ IPC sockets named by a UUID there and fails to start.
// The attempt therefore sees a short alias, a link in the worker spool that
// resolves to the real folder. Path resolution, and so every sandbox check,
// lands on the real folder. Without links, the real path is used as before.
func attemptTempDir(spool, work string) (string, error) {
	real := filepath.Join(work, ".tmp")
	if err := os.MkdirAll(real, 0700); err != nil {
		return "", err
	}
	if spool == "" {
		return real, nil
	}
	sum := sha256.Sum256([]byte(real))
	root := filepath.Join(spool, "t")
	alias := filepath.Join(root, hex.EncodeToString(sum[:6]))
	if len(alias) >= len(real) {
		return real, nil
	}
	if err := linkAttemptTemp(root, alias, real); err != nil {
		log.Printf("attempt temp alias unavailable; using %s: %v", real, err)
		return real, nil
	}
	return alias, nil
}

func linkAttemptTemp(root, alias, real string) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	// Aliases of cleaned attempts dangle; drop them so the folder stays small.
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			path := filepath.Join(root, e.Name())
			if e.Type()&fs.ModeSymlink != 0 && path != alias {
				if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
					_ = os.Remove(path)
				}
			}
		}
	}
	if target, err := os.Readlink(alias); err == nil {
		if target == real {
			return nil
		}
		if err := os.Remove(alias); err != nil {
			return err
		}
	}
	if err := os.Symlink(real, alias); err != nil {
		if target, e := os.Readlink(alias); e == nil && target == real {
			return nil
		}
		return err
	}
	return nil
}
