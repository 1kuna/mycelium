package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A real ready helper has custody before either final main launch gate. A local
// capacity refusal remains typed even if durable Started authorization won.
func TestNativeMemberAdmissionClassification(t *testing.T) {
	for _, refuseAt := range []int{3, 5} {
		t.Run(map[int]string{3: "helper-final-gate", 4: "before-started", 5: "after-started"}[refuseAt], func(t *testing.T) {
			q, clock := fixture(t)
			s := nativeSpec(t)
			s.Priority = "production"
			s.Run.Env["NATIVE_TEST_BLOCK"] = "0"
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			submit(t, q, s)
			offer := check(t, q, m, nil).Job
			if offer == nil {
				t.Fatal("missing native offer")
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			w := recoveryWorker(t)
			w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.nativeListenerProbe = func(string, int) error { return nil }
			calls := 0
			w.batchAdmission = func(context.Context, HostLaunch) error {
				calls++
				if calls == refuseAt {
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
				if started := job(t, q, offer.ID).StartedAt != nil; started != (refuseAt == 5) {
					t.Fatal("Started race not exercised", started, refuseAt)
				}
				if _, err := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "main.json")); !os.IsNotExist(err) {
					t.Fatal("main ran after refusal", err)
				}
				_, helperErr := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "side.json"))
				if refuseAt == 3 {
					if !os.IsNotExist(helperErr) {
						t.Fatal("helper payload ran through closed gate", helperErr)
					}
				} else if helperErr != nil {
					t.Fatal("helper custody was not exercised", helperErr)
				}
				nativeNoSurvivors(t, &nativeTestRun{p: p, dir: p.job.Spec.Run.Workdir})
			case <-time.After(testWaitTimeout):
				p.stop(clock.Now(), 0, true)
				t.Fatal("native refusal did not finish exact cleanup")
			}
		})
	}
}
