package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func retireSharedProfileSocket(root, path string, identity os.FileInfo) error {
	// Idle exit has already released lifetime ownership. Every ordinary startup
	// and cooperative handoff holds startup.lock through replacement readiness,
	// so keep this complete check/unlink transaction under that same lock.
	// A busy lock means another startup owns cleanup; leave its endpoint alone.
	startup, err := profileLock(filepath.Join(root, "startup.lock"))
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return nil
	}
	if err != nil {
		return err
	}
	defer startup.Close()
	if current, err := os.Stat(path); err == nil && os.SameFile(identity, current) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		_ = os.Remove(filepath.Dir(path))
	}
	return nil
}
