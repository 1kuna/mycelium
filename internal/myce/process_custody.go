package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func processPS(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", args...)
	// A 100ms drain allowance can expire solely because the reader goroutine
	// is starved under load, even after ps exits zero. Bound the OS probe while
	// allowing a full second to drain its small inventory; callers recheck errors.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	return out, errors.Join(err, ctx.Err())
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
