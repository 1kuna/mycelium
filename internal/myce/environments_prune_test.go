package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func pruneFixture(t *testing.T, c *EnvironmentCache, n int, age time.Duration) string {
	t.Helper()
	key := fmt.Sprintf("%064x", n)
	path := filepath.Join(c.Root, key)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(EnvironmentReport{Key: key, State: "ready"})
	if err := os.WriteFile(filepath.Join(path, "ready.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "payload"), []byte(strings.Repeat("x", 8192)), 0600); err != nil {
		t.Fatal(err)
	}
	at := c.Clock.Now().Add(-age)
	if err := os.Chtimes(filepath.Join(path, "ready.json"), at, at); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestEnvironmentPrune(t *testing.T) {
	q, clock := fixture(t)
	c := &EnvironmentCache{Root: t.TempDir(), Clock: clock, sizeLimit: func(string) (int64, error) { return environmentCacheSize(23 << 30) }}
	if err := c.init(); err == nil || !strings.Contains(err.Error(), "24 GiB free") {
		t.Fatal("preparation did not enforce its free-space floor", err)
	}
	old := 48 * time.Hour
	eligible := pruneFixture(t, c, 1, old)
	referenced := pruneFixture(t, c, 2, old)
	recent := pruneFixture(t, c, 3, time.Hour)
	useLocked := pruneFixture(t, c, 4, old)
	buildLocked := pruneFixture(t, c, 5, old)
	for _, key := range []string{useLocked + ".use", buildLocked + ".build"} {
		lock, err := c.lock(context.Background(), key, syscall.LOCK_SH)
		if err != nil {
			t.Fatal(err)
		}
		defer unlockEnvironment(lock)
	}
	r := binaryRecipe(t, "fake")
	r.Key = referenced
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"needed": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "needed"
	submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	refs := func(ctx context.Context, key string) (bool, error) {
		var state struct {
			Referenced bool `json:"referenced"`
		}
		err := client.Call(ctx, "GET", "/api/environments/"+key+"/reference", nil, &state)
		return state.Referenced, err
	}
	report, err := c.Prune(context.Background(), false, refs)
	if err != nil || len(report.Entries) != 5 || report.ReclaimedBytes != 0 {
		t.Fatal(report, err)
	}
	for _, v := range report.Entries {
		if v.Bytes <= 0 || v.Removed {
			t.Fatal(v)
		}
		if v.Key == referenced && !v.Referenced || v.Key == recent && (!v.Recent || v.Age != time.Hour) || (v.Key == useLocked || v.Key == buildLocked) && !v.Locked {
			t.Fatal(v)
		}
		if _, err := os.Stat(filepath.Join(c.Root, v.Key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(c.Root, "prune-audit.jsonl")); !os.IsNotExist(err) {
		t.Fatal("dry-run wrote audit", err)
	}
	for _, unavailable := range []bool{false, true} {
		calls := 0
		_, err := c.Prune(context.Background(), true, func(ctx context.Context, key string) (bool, error) {
			if key != eligible {
				return refs(ctx, key)
			}
			calls++
			if calls == 2 && unavailable {
				return false, errors.New("queue unavailable")
			}
			return calls == 2, nil
		})
		if (err != nil) != unavailable || calls != 2 {
			t.Fatal(err, calls)
		}
		if _, err := os.Stat(filepath.Join(c.Root, eligible)); err != nil {
			t.Fatal("reference recheck failed to preserve environment", err)
		}
	}
	report, err = c.Prune(context.Background(), true, refs)
	if err != nil || report.ReclaimedBytes <= 0 {
		t.Fatal(report, err)
	}
	for _, key := range []string{referenced, recent, useLocked, buildLocked} {
		if _, err := os.Stat(filepath.Join(c.Root, key)); err != nil {
			t.Fatal("protected environment removed", key, err)
		}
	}
	if _, err := os.Stat(filepath.Join(c.Root, eligible)); !os.IsNotExist(err) {
		t.Fatal("eligible environment retained", err)
	}
	b, err := os.ReadFile(filepath.Join(c.Root, "prune-audit.jsonl"))
	if err != nil || !strings.Contains(string(b), eligible) || !strings.Contains(string(b), `"removed":true`) || !strings.Contains(string(b), fmt.Sprintf(`"reclaimed_bytes":%d`, report.ReclaimedBytes)) {
		t.Fatal(string(b), err)
	}

	modelKey := strings.Repeat("b", 64)
	if err := q.update(func() error {
		q.s.ModelSlots = map[string]*ModelSlot{"loading": {EnvironmentRecipe: &EnvironmentRecipe{Key: modelKey}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if referenced, err := refs(context.Background(), modelKey); err != nil || !referenced {
		t.Fatal("loading model environment unprotected", err)
	}
	if err := q.update(func() error {
		q.s.ModelSlots = nil
		q.models.Models = []Model{{Name: "queued", Environment: "runtime"}}
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"runtime": {"fake": {Key: modelKey}}}
		q.calls = map[string]*ModelCall{"call": {Model: "queued"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if referenced, err := refs(context.Background(), modelKey); err != nil || !referenced {
		t.Fatal("queued call environment unprotected", err)
	}

	t.Run("release-refreshes-recent-use", func(t *testing.T) {

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

	})
}
