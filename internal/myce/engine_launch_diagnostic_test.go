package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// #868: the final shared-host check runs after queue Started authorization.
// Its refusal must retain the comparison, even though custody already exists.
func TestEngineLaunchAdmissionDiagnostic(t *testing.T) {
	for _, check := range []string{"live_host_growth", "aggregate_cpu_allowance"} {
		t.Run(check, func(t *testing.T) {
			rt, _, _ := probeMemoryRuntimeFixture(t)
			rt.models = map[string]*workerModel{}
			next := Job{ID: "next", Attempt: 14, Session: "session", Machine: rt.machine, State: "running", Spec: cpuSpec(2, 2)}
			if err := normalizeResources(&next.Spec); err != nil {
				t.Fatal(err)
			}
			rt.batchJobs = []Job{next}
			rt.batchSupervisors = map[attemptKey]*attemptSupervisor{jobKey(next): {job: next, admissionMu: &rt.admissionMu}}
			rt.hostMemorySource = func(context.Context) (float64, error) { return 1, nil }
			required, available, unit := localJobHostNeed(Worker{Machine: rt.memoryMachine}, next), 1.0, "GiB"
			if check == "aggregate_cpu_allowance" {
				rt.memoryMachine.CapabilityProfile.LogicalCPUs = 1
				required, available, unit = 2, 1, "cores"
			} else if check == "aggregate_host_allowance" {
				rt.memoryMachine.HostMemoryBudgetGB = 1
			}
			var authorized atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/workers/started" {
					t.Errorf("unexpected authorization %s", r.URL.Path)
				}
				authorized.Store(true)
				fmt.Fprintf(w, `{"ok":true,"start_gate":%q}`, startGateProtocol)
			}))
			defer server.Close()
			w := recoveryWorker(t)
			w.Clock, w.Machine, w.Client = rt.clock, rt.memoryMachine, Client{URL: server.URL}
			w.batchAdmission = rt.checkBatchLaunchMemory
			err := w.authorizeStart(next, next.Session)
			if !authorized.Load() || !errors.Is(err, errWorkerMemory) {
				t.Fatalf("expected post-authorization memory refusal: authorized=%t error=%v", authorized.Load(), err)
			}
			store, e := w.openStore()
			if e != nil {
				t.Fatal(e)
			}
			defer store.close()
			store.state.CustodyID = "gated-process"
			refusal := store.startFailureAt(next, launchRefused, err, rt.clock.Now())
			r := startFailureOutcome(Result{}, refusal)
			if r.ExecutionCause == nil || r.ExecutionCause.Admission == nil {
				t.Fatalf("shared-host comparison lost: %+v", r.ExecutionCause)
			}
			d := r.ExecutionCause.Admission
			if d.Subcheck != check || d.Unit != unit || d.Required != required || d.Available == nil || *d.Available != available || d.Unavailable || r.ExecutionCause.PrelaunchRefused {
				t.Fatalf("wrong post-custody comparison: %+v cause=%+v", d, r.ExecutionCause)
			}
		})
	}
}
