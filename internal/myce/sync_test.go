package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func syncFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	q, c := fixture(t)
	models := []Model{}
	for _, name := range []string{"a", "b"} {
		models = append(models, Model{Name: name, Path: name, Engine: "fake", SizeBytes: 40_000_000_000, SHA256: strings.Repeat("a", 64), MemoryGB: 8, Platforms: []string{"fake", "cuda"}})
	}
	if e := q.ConfigureModels(ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: models}); e != nil {
		t.Fatal(e)
	}
	m := machine("one", "fake")
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ModelURL = "http://127.0.0.1:1234"
	check(t, q, m, nil)
	return q, c, m
}

func TestSyncUsesRealAPIAndWorker(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		dir := t.TempDir()
		nas := filepath.Join(dir, "nas")
		os.Mkdir(nas, 0700)
		data := []byte("verified local weights")
		os.WriteFile(filepath.Join(nas, "m"), data, 0600)
		model := Model{Name: "m", Path: "m", Engine: "fake", MemoryGB: 1, Platforms: []string{"fake"}, SizeBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
		cfg := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{model}}
		if e := q.ConfigureModels(cfg); e != nil {
			t.Fatal(e)
		}
		server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		defer server.Close()
		client := Client{URL: server.URL, Token: "test"}
		if e := client.Call(context.Background(), "POST", "/api/models/sync", SyncRequest{Model: "m", To: "all"}, nil); e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		w := Worker{EnvironmentLimit: 1 << 30, Client: client, Machine: machine("sync-worker", "fake"), Clock: RealClock{}, Interval: 20 * time.Millisecond, StateDir: filepath.Join(dir, "worker"), ModelRoot: nas, CacheLimit: 1024, ModelConfig: cfg}
		go func() { done <- w.Run(ctx) }()
		deadline := time.After(testWaitTimeout)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case e := <-done:
				t.Fatal("worker exited before copying", e)
			case <-deadline:
				t.Fatal("sync did not reach verified cache")
			case <-tick.C:
				snap, e := q.Snapshot()
				if e != nil {
					t.Fatal(e)
				}
				if len(snap.Machines) == 1 && contains(snap.Machines[0].CachedModels, "m") {
					// Verified inventory is published before Precopy returns and the
					// attempt reports ready. Wait for that terminal acknowledgement;
					// the same deadline and final progress/speed assertions still apply.
					if len(snap.Machines[0].CopyReports) == 1 && snap.Machines[0].CopyReports[0].State == "copying" {
						continue
					}
					if len(snap.Machines[0].CopyReports) != 1 || snap.Machines[0].CopyReports[0].State != "ready" || snap.Machines[0].CopyBytesPerSecond <= 0 {
						t.Fatal("no progress/speed", snap.Machines[0])
					}
					cancel()
					if e := <-done; e != nil {
						t.Fatal(e)
					}
					if e := verifyModel(filepath.Join(dir, "worker", "model-cache", cacheKey(model)), model); e != nil {
						t.Fatal(e)
					}
					return
				}
			}
		}

	})

	t.Run("SyncTargetsCancelAndPersistence", func(t *testing.T) {
		for _, target := range []string{"all"} {
			t.Run(target, func(t *testing.T) {
				q, _, m := syncFixture(t)
				if e := q.SyncModel(SyncRequest{Model: "a", To: target}); e != nil {
					t.Fatal(e)
				}
				reply := check(t, q, m, nil)
				if len(reply.CopyPlan) != 1 || reply.CopyPlan[0].Model != "a" {
					t.Fatal(reply)
				}
				db := filepath.Join(t.TempDir(), "persist.db")
				other, e := Open(db, q.root, q.clock)
				if e != nil {
					t.Fatal(e)
				}
				if e = other.ConfigureModels(q.models); e != nil {
					t.Fatal(e)
				}
				check(t, other, m, nil)
				if e = other.SyncModel(SyncRequest{Model: "a", To: target}); e != nil {
					t.Fatal(e)
				}
				other.Close()
				other, e = Open(db, q.root, q.clock)
				if e != nil {
					t.Fatal(e)
				}
				defer other.Close()
				if e = other.ConfigureModels(q.models); e != nil {
					t.Fatal(e)
				}
				if len(check(t, other, m, nil).CopyPlan) != 1 {
					t.Fatal("restart lost sync")
				}
				if e = q.SyncModel(SyncRequest{Model: "a", To: target, Cancel: true}); e != nil {
					t.Fatal(e)
				}
				if len(check(t, q, m, nil).CopyPlan) != 0 {
					t.Fatal("cancel ignored")
				}
			})
		}

	})
}
