package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnvironmentRecentUseSurvivesPressureAndRetirement(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)}
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	r := binaryRecipe(t, "fake")
	path, release, err := c.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	// Qualification and execution both hold an Acquire reference. Release must
	// refresh recent use even when a command ran longer than the protection window.
	clock.Advance(25 * time.Hour)
	release()
	c.Limit = 1
	if err = c.admit("", 0); err == nil || !c.ready(r.Key) {
		t.Fatal("recently used/qualified environment evicted instead of refusing capacity", err)
	}
	if err = c.trash(path); err != nil {
		t.Fatal(err)
	}
	before, err := environmentSize(filepath.Join(c.Root, ".Trash"))
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := c.purgeRetired()
	if err != nil || remaining != before || before == 0 {
		t.Fatal("recent retired environment purged", before, remaining, err)
	}
	clock.Advance(25 * time.Hour)
	if remaining, err = c.purgeRetired(); err != nil || remaining != 0 {
		t.Fatal("expired unreferenced environment retained", remaining, err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
