package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func pendingControl(t *testing.T, dir string, workers []HostWorker) {
	t.Helper()
	for i := range workers {
		workers[i].Generation = 1
	}
	if err := saveWorkerJSON(dir, "enable-pending.json", hostReceipt{Generation: 1, EnableGeneration: 2, Workers: workers}); err != nil {
		t.Fatal(err)
	}
}

func TestControlAckRetiresOnlyAcceptedTargets(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	dir := t.TempDir()
	live := HostWorker{Host: "b70", Queue: server.URL, State: t.TempDir()}
	dead := HostWorker{Host: "old-drill", Queue: "http://127.0.0.1:1", State: t.TempDir()}
	pendingControl(t, dir, []HostWorker{live, dead})
	h := HostControl{Dir: dir, Notify: NotifyHost(Client{Token: "test"})}
	if err := h.ReconcileEnable(context.Background()); err == nil {
		t.Fatal("offline queue should remain pending")
	}
	q.mu.Lock()
	accepted := q.s.HostControls["b70"]
	q.mu.Unlock()
	if !accepted.Enabled || accepted.Generation != 2 {
		t.Fatalf("fleet enable not accepted: %+v", accepted)
	}
	b, err := os.ReadFile(filepath.Join(dir, "enable-pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt hostReceipt
	if err := json.Unmarshal(b, &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Workers) != 1 || receipt.Workers[0].Host != dead.Host || receipt.Workers[0].Generation != 1 {
		t.Fatalf("accepted target retained or offline receipt changed: %+v", receipt)
	}
}

type controlAckClock struct{ nanos atomic.Int64 }

func (c *controlAckClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *controlAckClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }

func TestWorkerControlAckHoldRetryAndUnrelatedQueue(t *testing.T) {
	for _, offlineDrill := range []bool{false, true} {
		t.Run(map[bool]string{false: "current-queue-unavailable", true: "unrelated-queue-unavailable"}[offlineDrill], func(t *testing.T) {
			q, _ := fixture(t)
			api := API{Queue: q, Token: "test"}.Handler()
			var allow atomic.Bool
			var controls atomic.Int64
			reports := make(chan bool, 100)
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/hosts/b70/enable" {
					controls.Add(1)
					if !offlineDrill && !allow.Load() {
						rw.WriteHeader(http.StatusServiceUnavailable)
						return
					}
				}
				if r.URL.Path == "/api/workers/checkin" {
					var b map[string]any
					// Inspect a copy without changing the API's request body.
					data, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(data))
					_ = json.Unmarshal(data, &b)
					m := b["machine"].(map[string]any)
					pending, _ := m["enable_pending"].(bool)
					select {
					case reports <- pending:
					default:
					}
				}
				api.ServeHTTP(rw, r)
			}))
			t.Cleanup(server.Close)
			dir := t.TempDir()
			workers := []HostWorker{{Host: "b70", Queue: server.URL, State: t.TempDir()}}
			if offlineDrill {
				workers = append(workers, HostWorker{Host: "drill", Queue: "http://127.0.0.1:1", State: t.TempDir()})
			}
			pendingControl(t, dir, workers)
			unitDir, _ := WorkerUnitDirectory(dir, "myce-worker@gpu1.service")
			clock := &controlAckClock{}
			clock.Advance(time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			m := machine("b70-gpu1", "fake")
			m.HostID = "b70"
			w := Worker{HostStateDir: unitDir, Unit: "myce-worker@gpu1.service", StateDir: t.TempDir(), Client: Client{URL: server.URL, Token: "test"}, Machine: m, Clock: clock, Interval: 5 * time.Millisecond}
			go func() { done <- w.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				clock.Advance(time.Minute)
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case err := <-done:
						if err != nil {
							t.Error(err)
						}
						return
					case <-ticker.C:
						clock.Advance(time.Second)
					case <-deadline.C:
						t.Error("worker did not stop")
						return
					}
				}
			})
			select {
			case err := <-done:
				done <- err
				t.Fatalf("worker crash-loop trigger: %v", err)
			case pending := <-reports:
				if pending == offlineDrill {
					t.Fatalf("wrong control hold: pending=%v unrelated=%v", pending, offlineDrill)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no held/joined heartbeat")
			}
			if !offlineDrill {
				// Heartbeats continue, but fake time alone determines reconciliation retries.
				heartbeats := 0
				await(t, func() bool {
					clock.Advance(5 * time.Millisecond)
					select {
					case <-reports:
						heartbeats++
					default:
					}
					return heartbeats >= 3
				})
				if controls.Load() != 1 {
					t.Fatalf("reconcile did not back off: %d", controls.Load())
				}
				snap, _ := q.Snapshot()
				if len(snap.Machines) != 1 || snap.Machines[0].Gone {
					t.Fatal("held worker missing from status")
				}
				j := submit(t, q, spec("experiment"))
				requireState(t, q, j.ID, "queued")
				allow.Store(true)
				clock.Advance(time.Second)
				await(t, func() bool { return controls.Load() >= 2 })
				await(t, func() bool {
					snap, _ := q.Snapshot()
					clock.Advance(5 * time.Millisecond)
					return len(snap.Machines) == 1 && !snap.Machines[0].EnablePending
				})
			}
		})
	}
}

func TestEnablePendingFencesBatchModelsAndPreparation(t *testing.T) {
	q, _, m := syncFixture(t)
	m.EnablePending = true
	m.EnvironmentBuilder = true
	if err := q.SyncModel(SyncRequest{Model: "a", To: m.ID}); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Model: "a", Machine: m.ID, Project: "test"}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	reply := check(t, q, m, nil)
	if !reply.AdmissionBlocked || reply.Leave || reply.Job != nil || len(reply.ModelPlan) != 0 || len(reply.CopyPlan) != 0 || len(reply.EnvironmentPlan) != 0 {
		t.Fatalf("held admission: %+v", reply)
	}
	requireState(t, q, j.ID, "queued")
	if q.placementAllowed(job(t, q, j.ID), &m) || modelFitsMachine(q.models.Models[0], &m) {
		t.Fatal("held worker eligible for placement")
	}
	m.EnablePending = false
	reply = check(t, q, m, nil)
	if reply.AdmissionBlocked {
		t.Fatal("hold became sticky")
	}
}
