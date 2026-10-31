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
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if err := c.init(); err != nil {
		t.Fatal(err)
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
}

func TestEnvironmentPruneReferenceRecheck(t *testing.T) {
	_, clock := fixture(t)
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	key := pruneFixture(t, c, 1, 48*time.Hour)
	for _, unavailable := range []bool{false, true} {
		calls := 0
		_, err := c.Prune(context.Background(), true, func(context.Context, string) (bool, error) {
			calls++
			if calls == 2 && unavailable {
				return false, errors.New("queue unavailable")
			}
			return calls == 2, nil
		})
		if (err != nil) != unavailable || calls != 2 {
			t.Fatal(err, calls)
		}
		if _, err := os.Stat(filepath.Join(c.Root, key)); err != nil {
			t.Fatal("reference recheck failed to preserve environment", err)
		}
	}
}

func TestEnvironmentPruneRetainedReferences(t *testing.T) {
	q, _ := fixture(t)
	ctx := context.Background()
	key := strings.Repeat("a", 64)
	r := EnvironmentRecipe{Key: key}
	for _, source := range []string{"sidecar", "extension", "setup", "unreleased", "preparation"} {
		t.Run(source, func(t *testing.T) {
			key := key
			e := Extension{Digest: strings.Repeat("b", 64)}
			m := machine("setup-worker", "fake")
			if source == "setup" {
				key = extensionPreparationKey(e, &m)
			}
			j := submit(t, q, spec("experiment"))
			if err := q.update(func() error {
				stored := q.s.Jobs[j.ID]
				switch source {
				case "sidecar":
					stored.SidecarEnvironmentRecipes = map[string]map[string]EnvironmentRecipe{"helper": {"fake": r}}
				case "extension":
					stored.Extensions = []Extension{{Variants: []ExtensionVariant{{Environments: map[string]EnvironmentRecipe{"main": r}}}}}
				case "setup":
					q.s.Machines[m.ID] = &m
					stored.Extensions = []Extension{e}
				case "unreleased":
					stored.State = "succeeded"
					stored.Attempt = 1
					stored.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": r}
				case "preparation":
					q.s.PreparationPlans = map[string]HostLaunch{"build": {Kind: "environment", Key: key}}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !q.environmentReferenced(ctx, key) {
				t.Fatal("lost reference", source)
			}
			if err := q.update(func() error {
				stored := q.s.Jobs[j.ID]
				stored.State = "succeeded"
				stored.ReleaseConfirmed = true
				q.s.PreparationPlans = nil
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if q.environmentReferenced(ctx, key) {
				t.Fatal("released terminal job still referenced", source)
			}
		})
	}
}

func TestEnvironmentPruneModelReferences(t *testing.T) {
	q, _ := fixture(t)
	key := strings.Repeat("b", 64)
	if err := q.update(func() error {
		q.s.ModelSlots = map[string]*ModelSlot{"loading": {EnvironmentRecipe: &EnvironmentRecipe{Key: key}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !q.environmentReferenced(context.Background(), key) {
		t.Fatal("loading model environment unprotected")
	}
	if err := q.update(func() error {
		q.s.ModelSlots = nil
		q.models.Models = []Model{{Name: "queued", Environment: "runtime"}}
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"runtime": {"fake": {Key: key}}}
		q.calls = map[string]*ModelCall{"call": {Model: "queued"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !q.environmentReferenced(context.Background(), key) {
		t.Fatal("queued call environment unprotected")
	}
}
