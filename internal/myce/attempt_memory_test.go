package myce

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAttemptMemoryDeviceQueryInheritedPipe(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	pidFile := filepath.Join(dir, "child")
	t.Setenv("MYCE_TEST_DEVICE_CHILD", pidFile)
	// The query exits, but a descendant retains stdout/stderr. CommandContext
	// alone cannot interrupt Output's pipe-copy goroutines after that exit.
	script := "#!/bin/sh\nsleep 5 &\necho $! > \"$MYCE_TEST_DEVICE_CHILD\"\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b, err := os.ReadFile(pidFile)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err == nil && pid > 0 {
				p, err := os.FindProcess(pid)
				if err == nil {
					_ = p.Kill()
				}
			}
		}
	})
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-one"}}
	start := time.Now()
	_, _, err := w.attemptDeviceMemory(context.Background(), map[int]bool{42: true}, nil)
	if elapsed := time.Since(start); elapsed >= 4*time.Second {
		t.Fatalf("inherited pipe stalled device query for %s", elapsed)
	}
	if err != errAttemptMemoryPending {
		t.Fatalf("incomplete query must remain unknown: %v", err)
	}
}

type nonreclaimableSamplingFS struct {
	memoryFS
	reads   atomic.Int32
	sampled chan struct{}
	missing bool
}

func (f *nonreclaimableSamplingFS) ReadFile(path string) ([]byte, error) {
	b, err := f.memoryFS.ReadFile(path)
	if filepath.Base(path) == "memory.stat" && f.reads.Add(1) == 2 {
		close(f.sampled)
		if f.missing {
			return []byte("anon 4021977088\nkernel 17985536\nshmem 4096\n"), nil
		}
	}
	return b, err
}
