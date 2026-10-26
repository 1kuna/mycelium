package myce

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #216: cleared attempts retain diagnosis for the log window; held and replay
// pending custody, the machine index, and Docker cleanup retain their evidence.
func TestWorkerRetentionPrunesClearedAttemptStateAfterLogWindow(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
	ids := map[string]string{}
	for _, kind := range []string{"cleared", "live", "replay", "indexed", "docker", "recent"} {
		id := ID()
		ids[kind] = id
		state := workerState{Identity: id}
		if kind == "live" || kind == "replay" {
			state.Job = &Job{ID: kind, Attempt: 1}
		}
		if kind == "replay" {
			state.Result = &Result{Success: true}
		}
		if err := saveWorkerJSON(dir, "state-attempt-"+kind+".json", state); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{id + ".result.json", id + ".outcome.json", id + ".cid", id + "-helper.cid", id + ".docker-evidence.json", id + ".docker-events.json", "attempt-" + id + ".json", "native-" + id + ".json"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := saveWorkerJSON(dir, "state.json", workerState{Attempts: []string{"state-attempt-indexed.json"}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ids["docker"]+".docker.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), clock.Now(), clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	pass := func() {
		t.Helper()
		if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
			t.Fatal(err)
		}
	}
	pass()
	clock.Advance(DefaultLogRetention - time.Second)
	pass()
	assertPresent := func(name string, want bool) {
		t.Helper()
		_, err := os.Lstat(filepath.Join(dir, name))
		if want && err != nil || !want && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s present=%t: %v", name, want, err)
		}
	}
	assertPresent("state-attempt-cleared.json", true)
	clock.Advance(2 * time.Second)
	// Freshly cleared custody gets a full window even when its command was old.
	if err := os.Chtimes(filepath.Join(dir, "state-attempt-recent.json"), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	pass()
	for kind, id := range ids {
		keep := kind != "cleared"
		assertPresent("state-attempt-"+kind+".json", keep)
		for _, name := range []string{id + ".result.json", id + ".outcome.json", id + ".cid", id + "-helper.cid", id + ".docker-evidence.json", id + ".docker-events.json", "attempt-" + id + ".json", "native-" + id + ".json"} {
			assertPresent(name, keep)
		}
	}
	// Once the existing Docker collector removes custody, old private evidence goes.
	if err := os.Remove(filepath.Join(dir, ids["docker"]+".docker.json")); err != nil {
		t.Fatal(err)
	}
	pass()
	assertPresent(ids["docker"]+".docker-evidence.json", false)
	assertPresent("state-attempt-docker.json", false)
}
