package myce

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCacheMaintenancePreservesLoadedAndUnknownModels(t *testing.T) {
	c, model := cacheFixture(t)
	a, b := model("active", "aaaa"), model("cold", "bbbb")
	for _, m := range []Model{a, b} {
		if _, err := c.Prepare(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	unknown := filepath.Join(c.Root, "unknown.model")
	if err := os.WriteFile(unknown, []byte("unique"), 0600); err != nil {
		t.Fatal(err)
	}
	c.Protected = func(key string) bool { return key == cacheKey(a) }
	c.Limit = 1
	c.collectAdaptive(context.Background())
	for _, path := range []string{filepath.Join(c.Root, cacheKey(a)), unknown} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("protected model lost", err)
		}
	}
	if _, err := os.Stat(filepath.Join(c.Root, cacheKey(b))); !os.IsNotExist(err) {
		t.Fatal("cold verified model retained", err)
	}
}
func TestCacheMaintenanceSharedInputsKeepsPins(t *testing.T) {
	_, clock := fixture(t)
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock, Limit: 1 << 20}}
	if err := c.cache.init(); err != nil {
		t.Fatal(err)
	}
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "source", "rebuildable")
	input := SharedInput{NAS: "source", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	_, release, err := c.Acquire(context.Background(), input, nas)
	if err != nil {
		t.Fatal(err)
	}
	release()
	key := input.key()
	unknown := pruneFixture(t, &c.cache, 1, 0)
	lock, err := c.cache.lock(context.Background(), key+".use", syscall.LOCK_SH)
	if err != nil {
		t.Fatal(err)
	}
	c.cache.Limit = 1
	c.collectAdaptive(context.Background())
	if _, err := os.Stat(filepath.Join(c.cache.Root, key)); err != nil {
		t.Fatal("reader removed", err)
	}
	unlockEnvironment(lock)
	c.collectAdaptive(context.Background())
	if _, err := os.Stat(filepath.Join(c.cache.Root, unknown)); err != nil {
		t.Fatal("unknown metadata deleted", err)
	}
	if _, err := os.Stat(filepath.Join(c.cache.Root, key)); !os.IsNotExist(err) {
		t.Fatal("inactive cache not collected", err)
	}
}
