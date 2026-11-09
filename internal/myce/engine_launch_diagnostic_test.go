package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// #868: the final shared-host check runs after queue Started authorization.
// Its refusal must retain the comparison, even though custody already exists.
func TestEngineLaunchAdmissionDiagnostic(t *testing.T) {
	for _, check := range []string{"live_host_growth", "aggregate_cpu_allowance", "aggregate_host_allowance"} {
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

func TestEngineLaunchGrowthContributors(t *testing.T) {
	_, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 96)
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	next := Job{ID: ID(), Attempt: 1, Session: m.Session, State: "running", Spec: cpuSpec(2, 2)}
	other := next
	other.ID, other.Spec = ID(), cpuSpec(2, 8)
	for _, j := range []*Job{&next, &other} {
		if err := normalizeResources(&j.Spec); err != nil {
			t.Fatal(err)
		}
	}
	now := clock.Now()
	rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{next, other},
		models: map[string]*workerModel{"resident": {state: "ready", model: Model{HostMemoryGB: 4}}}}
	// The launching attempt has fresh resident credit; its sibling's missing
	// credit is explained by age, not misattributed to the launching envelope.
	samples := map[attemptKey]hostMemorySample{
		jobKey(next):  {gb: 1, at: now},
		jobKey(other): {gb: 6, at: now.Add(-time.Minute)},
	}
	err := rt.checkHostAdmissionLocked(10, true, samples, jobKey(next))
	d := admissionDetail(err, next)
	if d == nil || d.Subcheck != "live_host_growth" || d.Required != 13 || len(d.Contributors) != 3 {
		t.Fatalf("missing aggregate explanation: %+v error=%v", d, err)
	}
	own, sibling, model := d.Contributors[0], d.Contributors[1], d.Contributors[2]
	if own.ID != next.ID || !own.Launching || own.PendingGB != 1 || !own.SampleFresh || own.SampleAgeSeconds == nil || *own.SampleAgeSeconds != 0 || sibling.ID != other.ID || sibling.PendingGB != 8 || sibling.SampleFresh || sibling.SampleAgeSeconds == nil || *sibling.SampleAgeSeconds != 60 || model.Kind != "model" || model.PendingGB != 4 || model.SampleAgeSeconds != nil {
		t.Fatalf("wrong growth terms: %+v", d.Contributors)
	}
	if own.PendingGB+sibling.PendingGB+model.PendingGB != d.Required {
		t.Fatal("contributions do not reproduce comparison")
	}
	// Safe retained details own their numeric storage rather than borrowing
	// a worker-provided pointer into a later report.
	copy := safeAdmissionDetail(d)
	*own.SampleAgeSeconds = 42
	if *copy.Contributors[0].SampleAgeSeconds != 0 {
		t.Fatal("retained contributor changed after publication")
	}
}
