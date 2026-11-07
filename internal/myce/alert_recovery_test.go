package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// #868: a 4 GiB job refused live_host_growth at 17.106637954711914 /
// 17.005722045898438 GiB. This replay supplies explicit co-tenant growth and
// assignment availability, which the historical receipt did NOT retain. It
// proves the supported recovery shape, not the incident's unknown RAM cause.
func alertRecoveryFixture(t *testing.T) (*Queue, *fakeClock, Machine, *Job, *Job) {
	t.Helper()
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.Devices = []MachineDevice{} // the keyed concurrent worker protocol
	co := submit(t, q, cpuSpec(1, 13.106637954711914))
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil {
		t.Fatal("missing co-tenant")
	}
	co = assigned
	j := submit(t, q, cpuSpec(1, 4))
	j = assignedJob(machineCheck(t, q, m, co))
	if j == nil {
		t.Fatal("missing target")
	}
	return q, c, m, co, j
}

func refusedRAM(j *Job, available float64) Result {
	r := result(j, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true,
		Admission: &AdmissionDetail{Subcheck: "live_host_growth", Unit: "GiB", Required: 17.106637954711914, Available: &available, EnvelopeSource: "declared", EnvelopeHostGB: 4}}
	return r
}

func twoRAMRefusals(t *testing.T, q *Queue, m Machine, co, j *Job) *Job {
	t.Helper()
	if err := q.Complete(refusedRAM(j, 17.005722045898438)); err != nil {
		t.Fatal(err)
	}
	next := assignedJob(machineCheck(t, q, m, co))
	if next == nil {
		t.Fatal("missing second attempt")
	}
	if err := q.Complete(refusedRAM(next, 17.005722045898438)); err != nil {
		t.Fatal(err)
	}
	return next
}

func recoveryReceipt(t *testing.T, q *Queue) *AlertRecovery {
	t.Helper()
	e := alertsOf(q, "infrastructure_loss_streak")
	if len(e) != 1 || e[0].Recovery == nil {
		t.Fatalf("missing or repeated recovery: %+v", e)
	}
	return e[0].Recovery
}

func TestAlertRecoveryRAMIncidentAndHealthyHistory(t *testing.T) {
	q, c, m, co, j := alertRecoveryFixture(t)
	if len(alertsOf(q, "infrastructure_loss_streak")) != 0 {
		t.Fatal("healthy assignments triggered recovery")
	}
	refused := twoRAMRefusals(t, q, m, co, j)
	r := recoveryReceipt(t, q)
	if r.State != "verifying" || r.Action != "refresh_live_host_memory" || r.Cause != "live_host_growth" || r.PreviousGB != 64 || r.AvailableGB != 17.005722045898438 {
		t.Fatalf("missing exact action/cause/result: %+v", r)
	}
	if q.s.Machines[m.ID].HostAvailableMemoryGB != r.AvailableGB || q.s.Machines[m.ID].FreeHostMemoryGB >= 4 {
		t.Fatal("queue did not apply worker's lower live availability")
	}
	// The action changes a live snapshot only. Explicit sizes, release custody,
	// logical identity, policy and infrastructure accounting are unchanged.
	stored := q.s.Jobs[j.ID]
	if stored.Spec.Needs.HostMemoryGB != 4 || stored.Losses != 0 || stored.InfrastructureRetries != 0 || stored.Attempt != refused.Attempt || !stored.ReleaseConfirmed {
		t.Fatal("recovery changed attempt contract", stored)
	}
	public, err := q.jobSnapshot(j.ID)
	if err != nil || public.HealthRecovery == nil || !strings.Contains(JobReason(public), "corrected an older free-RAM snapshot") {
		t.Fatal("job finding omitted recovery", public, err)
	}
	// Fresh low observations continue the normal capacity wait, without a
	// remediation retry. No timer or repeated observation changes the receipt.
	m.HostAvailableMemoryGB = r.AvailableGB
	for range 3 {
		c.Advance(time.Second)
		if next := assignedJob(machineCheck(t, q, m, co)); next != nil {
			t.Fatal("retried while measured capacity is insufficient", next)
		}
	}
	if recoveryReceipt(t, q) != r {
		t.Fatal("repeated observations repeated action")
	}
	// Recovery requires a real later start, not merely a healthier heartbeat.
	m.HostAvailableMemoryGB = 32
	c.Advance(time.Second)
	next := assignedJob(machineCheck(t, q, m, co))
	if next == nil || recoveryReceipt(t, q).State != "verifying" {
		t.Fatal("heartbeat claimed verified recovery")
	}
	if err := q.Started(result(next, true)); err != nil {
		t.Fatal(err)
	}
	verified := recoveryReceipt(t, q)
	if verified.State != "verified" || verified.VerifiedAt == nil || verified.VerifiedAttempt != next.Attempt {
		t.Fatal("later start not verified", verified)
	}
	if r.State != "verifying" {
		t.Fatal("verification mutated a detached delivery receipt")
	}
	finish(t, q, next, true)
	if len(alertsOf(q, "alert_recovery_action_required")) != 0 {
		t.Fatal("healthy recovery asked owner to act")
	}
	// A separate healthy history includes successful work between losses.
	q2, _, m2, co2, j2 := alertRecoveryFixture(t)
	if err := q2.Complete(refusedRAM(j2, 17.005722045898438)); err != nil {
		t.Fatal(err)
	}
	healthy := assignedJob(machineCheck(t, q2, m2, co2))
	finish(t, q2, healthy, true)
	if len(alertsOf(q2, "infrastructure_loss_streak")) != 0 || q2.s.Machines[m2.ID].HostAvailableMemoryGB != 64 {
		t.Fatal("healthy history triggered capacity recovery")
	}
}

func TestAlertRecoveryFreshnessConcurrentDeliveryAndRestart(t *testing.T) {
	q, c, m, co, j := alertRecoveryFixture(t)
	refused := twoRAMRefusals(t, q, m, co, j)
	r := recoveryReceipt(t, q)
	// Concurrent duplicate result delivery cannot repeat the action or overwrite
	// a fresher same-clock-timestamp check-in. A later execution stays exact.
	next := assignedJob(machineCheck(t, q, m, co))
	if next == nil {
		t.Fatal("missing newer execution")
	}
	// Receiver acknowledgements, including repeated delivery after an ambiguous
	// acknowledgement, are not recovery commands and keep the delivery ID.
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
	if len(failures) != 0 || q.s.Machines[m.ID].HostAvailableMemoryGB != 64 || q.s.Jobs[j.ID].Attempt != next.Attempt || recoveryReceipt(t, q).Episode != r.Episode {
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
	if recoveryReceipt(t, reopened).Episode != r.Episode || reopened.s.Machines[m.ID].HostAvailableMemoryGB != 64 {
		t.Fatal("restart repeated recovery or lost receipt")
	}
	machineCheck(t, reopened, m, co, next)
	if err := reopened.Started(result(next, true)); err != nil {
		t.Fatal(err)
	}
	if recoveryReceipt(t, reopened).State != "verified" {
		t.Fatal("restart lost verification custody")
	}
}

func TestAlertRecoveryRejectsNewerObservationAndUnsupportedCause(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer-observation", true: "unsupported"}[unsupported], func(t *testing.T) {
			q, _, m, co, j := alertRecoveryFixture(t)
			if err := q.Complete(refusedRAM(j, 17.005722045898438)); err != nil {
				t.Fatal(err)
			}
			next := assignedJob(machineCheck(t, q, m, co))
			r := refusedRAM(next, 17.005722045898438)
			if unsupported {
				r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "io_error", Predicate: "command_start"}
			} else {
				// A new observation at the same clock time must win over an older
				// assignment. Timestamp comparisons alone would wrongly apply it.
				machineCheck(t, q, m, co, next)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			receipt := recoveryReceipt(t, q)
			if receipt.State != "owner_required" || receipt.Action != "none" || receipt.NextStep == "" || q.s.Machines[m.ID].HostAvailableMemoryGB != 64 {
				t.Fatal("unsafe/unsupported recovery acted or lacks owner finding", receipt)
			}
			for range 4 {
				if err := q.update(func() error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if len(alertsOf(q, "infrastructure_loss_streak")) != 1 {
				t.Fatal("unsupported finding retried")
			}
		})
	}
}

func TestAlertRecoveryFailedVerificationOneOwnerFinding(t *testing.T) {
	q, _, m, co, j := alertRecoveryFixture(t)
	twoRAMRefusals(t, q, m, co, j)
	r := recoveryReceipt(t, q)
	next := assignedJob(machineCheck(t, q, m, co))
	if err := q.Complete(refusedRAM(next, 16.814495086669922)); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	findings := alertsOf(q, "alert_recovery_action_required")
	if len(findings) != 1 || findings[0].Recovery.State != "owner_required" || findings[0].Recovery.Episode != r.Episode || findings[0].Recovery.NextStep == "" {
		t.Fatal("failed recovery lacks one actionable delivery", findings)
	}
	if q.s.Machines[m.ID].HostAvailableMemoryGB != 64 {
		t.Fatal("failed recovery repeated its action")
	}
	payload, err := json.Marshal(findings[0])
	if err != nil || !strings.Contains(string(payload), "Ask the worker owner") {
		t.Fatal("existing notifier payload lacks plain-language finding", err)
	}
	public, err := q.jobSnapshot(j.ID)
	if err != nil || !strings.Contains(JobReason(public), "did not resolve") {
		t.Fatal("job finding lost failed recovery", public, err)
	}
}
