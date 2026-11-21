package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWorkerFileStorePressureBoundsCompilerTrim(t *testing.T) {
	_, clock := fixture(t)
	cache := &workerCompilerCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock}}
	if err := cache.cache.initStorage(); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(cache.cache.Root, identityHash("go-build"), "aa")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 3)
	var units uint64
	for i := range paths {
		path := filepath.Join(root, fmt.Sprintf("aa%062x-d", i))
		if err := os.WriteFile(path, []byte(strings.Repeat("test", 1024)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, clock.Now(), clock.Now()); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			units += uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		}
		clock.Advance(time.Second)
		paths[i] = path
	}
	const volume = "shared"
	const free = uint64(10 << 30)
	b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		// Lost space is consumed by another writer during the trim. The
		// measured request still cannot remove more than its initial deficit.
		return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: free}, nil
	})
	pressure := workerFileStorePressure{TargetFreeBytes: free + 2*units, ShortfallBytes: units}
	owner := &workerStore{dir: t.TempDir()}
	unpin, err := owner.pinSharedInput(cache.cache.Root, identityHash("go-build"))
	if err != nil {
		t.Fatal(err)
	}
	b.trimForFileStore(context.Background(), cache, volume, pressure)
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatalf("queue pressure bypassed compiler custody: %v", err)
	}
	unpin()
	b.trimForFileStore(context.Background(), cache, volume, pressure)
	for _, path := range paths[:2] {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old compiler object was not trimmed: %v", err)
		}
	}
	if _, err := os.Stat(paths[2]); err != nil {
		t.Fatalf("trim exceeded the measured deficit: %v", err)
	}
}
