package myce

import (
	"testing"
	"time"
)

func TestRemainingHealthLossSuccessLoss(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	submit(t, q, cpuSpec(1, 1))
	complete := func(infra bool) {
		t.Helper()
		a := check(t, q, m, nil).Job
		if a == nil {
			t.Fatal("missing assignment")
		}
		c.Advance(time.Second)
		r := result(a, !infra)
		r.Interrupted = infra
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	complete(true)
	complete(false)
	submit(t, q, cpuSpec(1, 1))
	complete(true)
	requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 0, false)
}

func TestRemainingHealthOOMFinalClassification(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"first", "second"} {
		c.Advance(time.Second)
		// Replay retryOOM's nested requeue/final-ledger sequence without coupling
		// the diagnostic check to memory admission or worker release custody.
		if err := q.update(func() error {
			j := &Job{ID: id, Attempt: 1, Machine: "worker", Session: "session", State: "running", Spec: cpuSpec(1, 1)}
			final := attemptOutcome(j, Result{Interrupted: true, OutOfMemory: true})
			q.requeue(j, "out of memory; waiting for room above the previous envelope", false)
			q.recordAttemptOutcome(j, final)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 0, false)
	}
}

func TestRemainingHealthStagingNeedsPositiveRecovery(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	submit(t, q, cpuSpec(1, 1))
	for range 2 {
		a := check(t, q, m, nil).Job
		if a == nil {
			t.Fatal("missing staging retry")
		}
		c.Advance(time.Second)
		r := result(a, false)
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "timeout"}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, true)
	}
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("missing recovered assignment")
	}
	c.Advance(time.Second)
	if err := q.Complete(result(a, true)); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, false)
	// A new verified refusal after successful staging is a second episode.
	submit(t, q, cpuSpec(1, 1))
	a = check(t, q, m, nil).Job
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "timeout"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 2, true)
}

func TestRemainingHealthMissingToolsAreUnknown(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	s.Needs.Tools = []string{"python3"}
	j := submit(t, q, s)
	requireIncident(t, q, "worker_reports_missing", "reports/worker/tool_inventory_report_missing", 1, true)
	requireIncident(t, q, "job_target_unsatisfied", "target/"+j.ID, 0, false)
	// A present inventory with no matching tool proves incompatibility, while
	// omission did not. A later matching report clears it and allows assignment.
	m.ToolInventory = map[string][]string{"": {}}
	check(t, q, m, nil)
	requireIncident(t, q, "job_target_unsatisfied", "target/"+j.ID, 1, true)
	m.ToolInventory[""] = []string{"python3"}
	check(t, q, m, nil)
	requireIncident(t, q, "job_target_unsatisfied", "target/"+j.ID, 1, false)
}

func TestRemainingHealthResolvedToolsAreUnknown(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	m.ExtensionFingerprint = "cpu-v1"
	// The frozen extension recipe owns these needs; the authored top-level
	// job has no needs.tools. Resolve must still distinguish missing evidence.
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto", Tools: []string{"python3"}}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	r := newExtensionReport(e, &m, "extension:sample")
	r.Setup = true
	r.Qualified = true
	r.CheckID = "tools-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	s := Spec{Name: "tools-work", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	j := submit(t, q, s)
	requireIncident(t, q, "worker_reports_missing", "reports/"+m.ID+"/tool_inventory_report_missing", 1, true)
	requireIncident(t, q, "job_target_unsatisfied", "target/"+j.ID, 0, false)
}
