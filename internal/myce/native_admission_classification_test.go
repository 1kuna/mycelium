package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// A real ready helper has custody before either final main launch gate. A local
// capacity refusal remains typed even if durable Started authorization won.
func TestNativeMemberAdmissionClassification(t *testing.T) {
	for _, phase := range []string{"helper-final-gate", "before-started", "after-started", "compact-after-started"} {
		t.Run(phase, func(t *testing.T) {
			compact := phase == "compact-after-started"
			measure := compact || phase == "after-started"
			q, clock := fixture(t)
			s := nativeSpec(t)
			s.Priority = "production"
			// Host-RAM admission applies to CPU work too. This fixture holds
			// time at the refused gate, so GPU stop grace could never elapse.
			s.Needs.MemoryGB, s.Needs.CPUCores = 0, 1
			s.Run.Env["NATIVE_TEST_BLOCK"] = "0"
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			submit(t, q, s)
			offer := check(t, q, m, nil).Job
			if offer == nil {
				t.Fatal("missing native offer")
			}
			var controlTime atomic.Int64
			const requestDelay = 3 * time.Second
			api := (API{Queue: q, Token: "test"}).Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if measure && (r.URL.Path == "/api/workers/launch" || r.URL.Path == "/api/workers/started" || r.URL.Path == "/api/workers/native-custody") {
					clock.Advance(requestDelay)
					controlTime.Add(int64(requestDelay))
				}
				api.ServeHTTP(w, r)
			}))
			defer server.Close()
			w := recoveryWorker(t)
			w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
			w.compactNativeLaunch = compact
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.nativeListenerProbe = func(string, int) error { return nil }
			var store *workerStore
			w.batchAdmission = func(context.Context, HostLaunch) error {
				refuse := false
				if phase == "helper-final-gate" {
					data, _ := os.ReadFile(filepath.Join(store.dir, "native-"+store.state.Identity+".json"))
					var state struct{ Members []nativeMember }
					_ = json.Unmarshal(data, &state)
					refuse = len(state.Members) > 0 && state.Members[0].Phase == "gated"
				} else if store.startExecutionEvidence(*offer).Sidecars == executionMayHaveRun {
					refuse = phase == "before-started" || job(t, q, offer.ID).StartedAt != nil
				}
				if refuse {
					return refuseAdmission("live_host_growth", "GiB", 4, 3, false)
				}
				return nil
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = m.Session
			p, err := startAfterFixtureInputs(t, w, store, *offer)
			if err != nil || p == nil {
				t.Fatal("native start", err)
			}
			select {
			case f := <-p.done:
				if f.err != nil {
					t.Fatal(f.err)
				}
				r := f.result
				c := r.ExecutionCause
				if r.Success || !r.Interrupted || r.ReleasePending || c == nil || c.Operation != "launch" || c.Class != "resource_limit" || c.Predicate != "memory_capacity" || c.PrelaunchRefused {
					t.Fatalf("member refusal lost type or custody: result=%+v cause=%+v", r, c)
				}
				d := c.Admission
				if d == nil || d.Subcheck != "live_host_growth" || d.Unit != "GiB" || d.Required != 4 || d.Available == nil || *d.Available != 3 || d.Unavailable {
					t.Fatalf("member comparison lost: %+v", d)
				}
				if started := job(t, q, offer.ID).StartedAt != nil; started != measure {
					t.Fatal("Started race not exercised", started, phase)
				}
				if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "main.json")); !os.IsNotExist(err) {
					t.Fatal("main ran after refusal", err)
				}
				_, helperErr := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "side.json"))
				if phase == "helper-final-gate" {
					if !os.IsNotExist(helperErr) {
						t.Fatal("helper payload ran through closed gate", helperErr)
					}
				} else if helperErr != nil {
					t.Fatal("helper custody was not exercised", helperErr)
				}
				nativeNoSurvivors(t, &nativeTestRun{p: p, dir: p.job.Spec.Run.Workdir})
				if measure {
					// This refusal is immediately after Started, before the
					// legacy path's additional main exec RPC.
					trips := 7
					if compact {
						trips = 3
					}
					if got := time.Duration(controlTime.Load()); got != time.Duration(trips)*requestDelay {
						t.Fatalf("control time through Started = %s for %d requests", got, trips)
					}
					t.Logf("control through Started: %d requests, %s at %s per request", trips, time.Duration(controlTime.Load()), requestDelay)
				}
			case <-time.After(testWaitTimeout):
				p.stop(clock.Now(), 0, true)
				t.Fatal("native refusal did not finish exact cleanup")
			}
		})
	}
}

// Reaping an unopened shell wrapper must not turn its exit 125 into a payload
// failure, regardless of whether the waiter wins the refusal-classification race.
func TestNativeAdmissionIndependentFailureRequiresExec(t *testing.T) {
	for _, released := range []bool{false, true} {
		for _, reaped := range []bool{false, true} {
			done := make(chan struct{})
			if reaped {
				close(done)
			}
			g := &nativeGroup{p: &process{}, members: []*nativeMember{{Name: "helper", done: done, execReleased: released, memberOutcome: memberOutcome{Exit: &memberExit{Status: 125}}}}}
			if got := g.independentMemberFailure(); got != released {
				t.Fatalf("independent failure=%v, exec gate opened=%v, waiter done=%v", got, released, reaped)
			}
		}
	}
}

// Wait has recorded the exit while its publication is blocked behind p.mu.
// That observation must participate even when m.Exit and done still lag it.
func TestNativeAdmissionPendingExitObservation(t *testing.T) {
	for _, released := range []bool{false, true} {
		exit := &memberExit{Status: 125}
		m := &nativeMember{Name: "helper", execReleased: released, done: make(chan struct{}), exitObservation: &nativeExitObservation{exit: exit}}
		g := &nativeGroup{p: &process{}, members: []*nativeMember{m}}
		g.p.mu.Lock()
		independent := g.independentMemberFailureLocked()
		facts := g.outcomeLocked(errWorkerMemory, true)
		g.p.mu.Unlock()
		if independent != released || m.Exit != exit || facts.Exit != exit {
			t.Fatalf("pending exit not in consistent snapshot: independent=%v released=%v exit=%+v facts=%+v", independent, released, m.Exit, facts)
		}
	}
}
