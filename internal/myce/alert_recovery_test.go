package myce

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

// Assignment context is synthetic; #868 did not retain paired snapshots. These
// fixtures test retained findings, never an inferred historical repair.
func alertRecoveryFixture(t *testing.T, needs float64) (*Queue, *fakeClock, Machine, *Job, *Job) {
	t.Helper()
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 128)
	m.Devices = []MachineDevice{}
	submit(t, q, cpuSpec(1, 13))
	co := assignedJob(machineCheck(t, q, m))
	submit(t, q, cpuSpec(1, needs))
	j := assignedJob(machineCheck(t, q, m, co))
	if co == nil || j == nil {
		t.Fatal("missing assignments")
	}
	return q, c, m, co, j
}

func refusedRAM(j *Job, available float64) Result {
	r := result(j, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true,
		Admission: &AdmissionDetail{Subcheck: "live_host_ram", Unit: "GiB", Required: j.Spec.Needs.HostMemoryGB, Available: &available, EnvelopeSource: "declared", EnvelopeHostGB: j.Spec.Needs.HostMemoryGB}}
	return r
}

func twoRAMRefusals(t *testing.T, q *Queue, m Machine, co, j *Job) *Job {
	t.Helper()
	if err := q.Complete(refusedRAM(j, 3)); err != nil {
		t.Fatal(err)
	}
	next := assignedJob(machineCheck(t, q, m, co))
	if next == nil {
		t.Fatal("missing second attempt")
	}
	if err := q.Complete(refusedRAM(next, 3)); err != nil {
		t.Fatal(err)
	}
	return next
}

func recoveryReceipt(t *testing.T, q *Queue) *AlertRecovery {
	t.Helper()
	e := alertsOf(q, "infrastructure_loss_streak")
	if len(e) != 1 || e[0].Recovery == nil {
		t.Fatalf("missing or repeated decision receipt: %+v", e)
	}
	return e[0].Recovery
}

func TestAlertRecoveryRecordedRefusalsNeedOwner(t *testing.T) {
	for _, tc := range []struct {
		name        string
		needs       float64
		comparisons [2][2]float64
	}{
		{"original-868-attempts-5-6", 4, [2][2]float64{{17.106637954711914, 17.005722045898438}, {16.86988639831543, 16.814495086669922}}},
		{"unbounded-forecast-868-909", 4.125, [2][2]float64{{89.83, 89.68}, {89.83, 89.68}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _, m, co, j := alertRecoveryFixture(t, tc.needs)
			for i, comparison := range tc.comparisons {
				r := refusedRAM(j, comparison[1])
				r.ExecutionCause.Admission.Subcheck = "live_host_growth"
				r.ExecutionCause.Admission.Required = comparison[0]
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					j = assignedJob(machineCheck(t, q, m, co))
				}
			}
			r := recoveryReceipt(t, q)
			if r.State != "owner_required" || r.Action != "none" || r.Cause != "live_host_growth" || r.Admission == nil || r.Admission.Required != tc.comparisons[1][0] || r.Admission.Available == nil || *r.Admission.Available != tc.comparisons[1][1] || r.Admission.EnvelopeHostGB != tc.needs || !strings.Contains(r.NextStep, "forecast bounds") {
				t.Fatal("recorded refusal lacks exact evidence and immediate owner finding", r)
			}
			if q.s.Machines[m.ID].HostAvailableMemoryGB != 128 || r.JobID != j.ID || r.Attempt != j.Attempt || r.Session != j.Session {
				t.Fatal("finding changed capacity or lost execution identity", r)
			}
			stored := q.s.Jobs[j.ID]
			if stored.Spec.Needs.HostMemoryGB != tc.needs || stored.Losses != 0 || stored.InfrastructureRetries != 0 || !stored.ReleaseConfirmed {
				t.Fatal("finding changed attempt contract", stored)
			}
			payload, err := json.Marshal(alertsOf(q, "infrastructure_loss_streak")[0])
			if err != nil || !strings.Contains(string(payload), "forecast bounds") || !strings.Contains(string(payload), "live_host_growth") {
				t.Fatal("existing notifier payload lacks evidence/next step", err)
			}
			public, err := q.jobSnapshot(j.ID)
			if err != nil || !strings.Contains(JobReason(public), "forecast bounds") {
				t.Fatal("job finding omitted the next step", public, err)
			}
		})
	}
}

func TestAlertRecoveryHealthyAndUnsupportedHistory(t *testing.T) {
	q, _, m, co, j := alertRecoveryFixture(t, 4)
	if err := q.Complete(refusedRAM(j, 3)); err != nil {
		t.Fatal(err)
	}
	healthy := assignedJob(machineCheck(t, q, m, co))
	finish(t, q, healthy, true)
	if len(alertsOf(q, "infrastructure_loss_streak")) != 0 || q.s.Machines[m.ID].HostAvailableMemoryGB != 128 {
		t.Fatal("healthy history triggered a finding or changed capacity")
	}
	for _, unsupported := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct-ram", true: "command-start"}[unsupported], func(t *testing.T) {
			q, _, m, co, j := alertRecoveryFixture(t, 4)
			if err := q.Complete(refusedRAM(j, 3)); err != nil {
				t.Fatal(err)
			}
			next := assignedJob(machineCheck(t, q, m, co))
			r := refusedRAM(next, 3)
			if unsupported {
				r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "io_error", Predicate: "command_start"}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			receipt := recoveryReceipt(t, q)
			if receipt.State != "owner_required" || receipt.Action != "none" || receipt.NextStep == "" || q.s.Machines[m.ID].HostAvailableMemoryGB != 128 {
				t.Fatal("unsupported evidence acted or lacks owner finding", receipt)
			}
			for range 4 {
				if err := q.update(func() error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if len(alertsOf(q, "infrastructure_loss_streak")) != 1 {
				t.Fatal("unsupported finding repeated")
			}
		})
	}
}

func TestAlertRecoveryFindingConcurrentDeliveryAndRestart(t *testing.T) {
	q, c, m, co, j := alertRecoveryFixture(t, 4)
	refused := twoRAMRefusals(t, q, m, co, j)
	r := recoveryReceipt(t, q)
	before, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 32
	next := assignedJob(machineCheck(t, q, m, co))
	if next == nil {
		t.Fatal("missing newer execution")
	}
	for _, delivered := range []bool{true, false, true} {
		if err := q.update(func() error {
			for i := range q.s.Health.Events {
				if q.s.Health.Events[i].DeliveryID == r.Episode {
					q.s.Health.Events[i].Delivered = delivered
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Complete(refusedRAM(refused, 1))
			if err != nil && !errors.Is(err, ErrStale) {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(failures) != 0 || q.s.Machines[m.ID].HostAvailableMemoryGB != 32 || q.s.Jobs[j.ID].Attempt != next.Attempt || recoveryReceipt(t, q).Episode != r.Episode {
		t.Fatal("duplicate delivery touched newer evidence/execution", failures)
	}
	var sequence int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fakeCheckpointAvailability(t, reopened, true)
	machineCheck(t, reopened, m, co, next)
	if err := reopened.Started(result(next, true)); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(recoveryReceipt(t, reopened))
	if err != nil || !bytes.Equal(before, after) || reopened.s.Machines[m.ID].HostAvailableMemoryGB != 32 {
		t.Fatal("restart/progress rewrote the original finding or changed capacity", string(after), err)
	}
}

func TestAlertRecoveryFindingPreservesNewerObservation(t *testing.T) {
	q, _, m, co, j := alertRecoveryFixture(t, 4)
	if err := q.Complete(refusedRAM(j, 3)); err != nil {
		t.Fatal(err)
	}
	next := assignedJob(machineCheck(t, q, m, co))
	// A fresher same-clock-timestamp check-in remains authoritative. The old
	// refusal can retain its comparison but cannot correct the newer snapshot.
	m.HostAvailableMemoryGB = 32
	machineCheck(t, q, m, co, next)
	if err := q.Complete(refusedRAM(next, 3)); err != nil {
		t.Fatal(err)
	}
	r := recoveryReceipt(t, q)
	if r.Action != "none" || r.State != "owner_required" || q.s.Machines[m.ID].HostAvailableMemoryGB != 32 {
		t.Fatal("old refusal changed newer availability", r)
	}
}
