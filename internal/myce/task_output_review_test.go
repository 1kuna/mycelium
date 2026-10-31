package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskOutputsCollectionRechecksDestinationAfterVerification(t *testing.T) {
	for _, scenario := range []string{"missing-payload", "corrupt-after-restart", "offline-intent-replay"} {
		t.Run(scenario, func(t *testing.T) {
			f := newTaskOutputFixture(t, "nas", "")
			f.pending(t)
			f.c.Advance(filePublicationGrace)
			if err := f.q.Tick(); err != nil {
				t.Fatal(err)
			}
			verifiedReceipt := taskOutputReceipt(*job(t, f.q, f.j.ID))
			if scenario == "corrupt-after-restart" {
				if err := f.q.Close(); err != nil {
					t.Fatal(err)
				}
				q, err := Open(f.db, f.nas, f.c)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = q.Close() })
				f.q, f.w.Client = q, deliveryClient(t, q)
			}
			if scenario == "offline-intent-replay" {
				if err := saveWorkerJSON(f.store.dir, "task-output-collection.json", taskOutputReceipt(*job(t, f.q, f.j.ID))); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(f.nas, f.nas+"-offline"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Rename(f.nas+"-offline", f.nas) })
			} else {
				path := filepath.Join(f.nas, f.j.Spec.Outputs["out.txt"], f.j.ID, fmt.Sprint(f.j.Attempt), "out.txt")
				var err error
				if scenario == "missing-payload" {
					err = os.Remove(path)
				} else {
					err = os.WriteFile(path, []byte("corrupt"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			_ = f.step(t)
			taskOutputAssertSpool(t, f, true)
			j := job(t, f.q, f.j.ID)
			if j.LocalOutputs.CollectedAt != nil || f.boundary.collected != 0 || len(f.q.s.StorageProtection.Allocations) != 1 || len(f.q.s.StorageProtection.Custody) != 1 {
				t.Fatal("lost preservation released charged custody")
			}
			if j.Delivery.State != "failed" || j.Delivery.VerifiedAt != nil {
				t.Fatal("lost destination not marked for redelivery", j.Delivery)
			}
			if scenario == "corrupt-after-restart" {
				if err := f.q.Close(); err != nil {
					t.Fatal(err)
				}
				q, err := Open(f.db, f.nas, f.c)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = q.Close() })
				f.q, f.w.Client = q, deliveryClient(t, q)
			}
			if err := f.q.ConfirmTaskOutputs(context.Background(), verifiedReceipt); !errors.Is(err, ErrStale) {
				t.Fatal("historical success restored lost preservation", err)
			}
			if len(alertsOf(f.q, "output_destination_preservation_lost")) != 1 {
				t.Fatal("lost destination did not alert")
			}
			if scenario == "offline-intent-replay" {
				if err := os.Rename(f.nas+"-offline", f.nas); err != nil {
					t.Fatal(err)
				}
			} else {
				data, err := os.ReadFile(filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt"))
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(f.nas, f.j.Spec.Outputs["out.txt"], f.j.ID, fmt.Sprint(f.j.Attempt), "out.txt")
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.c.Advance(deliveryRetryDelay)
			f.pending(t) // Retire the old intent only after verifying the full spool.
			f.pending(t) // Fresh destination publication/verification.
			if err := f.step(t); err != nil {
				t.Fatal("recovered preservation did not collect", err)
			}
			taskOutputAssertSpool(t, f, false)
		})
	}
}

func TestTaskOutputsCollectionRechecksAttemptAfterDestinationIO(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	f.pending(t)
	f.c.Advance(filePublicationGrace)
	if err := f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	f.w.outputDeliveryProgress = func() {
		once.Do(func() {
			// Taking the queue lock during hashing also proves the I/O is outside it.
			if err := f.q.update(func() error { f.q.touchJob(f.j.ID).Attempt++; return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	if err := f.step(t); err == nil {
		t.Fatal("stale collection verification accepted")
	}
	taskOutputAssertSpool(t, f, true)
	if f.boundary.collected != 0 || len(f.q.s.StorageProtection.Allocations) != 1 {
		t.Fatal("stale verification released custody")
	}
}

func TestTaskOutputsRestartRetainsDeliveryWithoutBlockingProduction(t *testing.T) {
	for _, kind := range []string{"submitter", "nas"} {
		t.Run(kind, func(t *testing.T) {
			f := newTaskOutputFixture(t, kind, "retained")
			f.pending(t)
			clock := &workerFakeClock{now: f.c.Now()}
			f.q.clock, f.w.Clock = clock, clock
			root := &workerStore{dir: f.store.indexRoot, state: workerState{Session: f.j.Session, Attempts: []string{f.store.stateName}}}
			if err := root.save(); err != nil {
				t.Fatal(err)
			}
			production := submit(t, f.q, protectionSpec("production"))
			var shutdown atomic.Bool
			var heldRecovering atomic.Int32
			offered := make(chan Checkin, 1)
			api := API{Queue: f.q, Token: "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/workers/checkin" {
					api.ServeHTTP(w, r)
					return
				}
				if shutdown.Load() {
					http.Error(w, "fixture stopped", 400)
					return
				}
				var checkin Checkin
				if err := json.NewDecoder(r.Body).Decode(&checkin); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				if checkin.Recovering {
					heldRecovering.Add(1)
				}
				reply, err := f.q.Checkin(checkin)
				if err != nil {
					t.Logf("checkin rejected: %v", err)
					http.Error(w, err.Error(), 400)
					return
				}
				for _, directive := range reply.Attempts {
					if directive.Job != nil && directive.Job.ID == production.ID {
						select {
						case offered <- checkin:
						default:
						}
						shutdown.Store(true)
						// End this disposable worker before it launches the offered command.
						http.Error(w, "fixture stopped", 400)
						return
					}
				}
				_ = json.NewEncoder(w).Encode(reply)
			}))
			t.Cleanup(server.Close)
			f.w.Client.URL, f.w.StateDir, f.w.Interval = server.URL, root.dir, 10*time.Millisecond
			f.w.Machine.HostID = f.w.Machine.ID
			f.w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			done := make(chan error, 1)
			go func() { done <- f.w.Run(context.Background()) }()
			t.Cleanup(func() {
				shutdown.Store(true)
				tick := time.NewTicker(5 * time.Millisecond)
				defer tick.Stop()
				deadline := time.After(5 * time.Second)
				for {
					select {
					case <-done:
						return
					case <-tick.C:
						clock.Advance(time.Second)
					case <-deadline:
						t.Error("worker did not stop")
						return
					}
				}
			})
			var observed Checkin
			await(t, func() bool {
				clock.Advance(10 * time.Millisecond)
				if heldRecovering.Load() >= 20 {
					t.Fatal("retained delivery blocked 20 production admission heartbeats")
				}
				select {
				case observed = <-offered:
					return true
				case err := <-done:
					done <- err
					t.Fatalf("worker exited before assignment: %v", err)
					return false
				default:
					return false
				}
			})
			if observed.Recovering || observed.Machine.Session != f.j.Session || len(observed.Attempts) != 1 || observed.Attempts[0].ID != f.j.ID {
				t.Fatal("delivery recovery changed compute/session custody", observed)
			}
			taskOutputAssertSpool(t, f, true)
			if j := job(t, f.q, f.j.ID); !j.ReleaseConfirmed || j.LocalOutputs.CollectedAt != nil {
				t.Fatal("production consumed retained delivery custody")
			}
		})
	}
}
