package myce

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func processPS(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", args...)
	cmd.WaitDelay = 100 * time.Millisecond
	return cmd.Output()
}

func commandLock(dir, name string, wait bool) (func(), error) {
	if err := PrepareStateRoots(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	mode := syscall.LOCK_EX
	if !wait {
		mode |= syscall.LOCK_NB
	}
	if err = syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		return nil, fmt.Errorf("command already in progress: %w", err)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
