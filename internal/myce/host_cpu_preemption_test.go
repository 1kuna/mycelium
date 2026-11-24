package myce

import (
	"errors"
	"testing"
	"time"
)

func TestCPUBusyHostQueuesAndRejectsLostCompletion(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("cpu", "physical", "cpu", 2, 8)
	check(t, q, m, nil)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"cpu-project": {Weight: 1, AllowedMachines: []string{m.ID}}}); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, cpuSpec(2, 1))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != first.ID {
		t.Fatal("CPU assignment missing", run)
	}
	pending, err := q.Submit(cpuSpec(2, 1))
	if err != nil || pending.State != "queued" {
		t.Fatal("busy CPU host rejected job", pending, err)
	}
	clock.Advance(LossAfter + time.Second)
	check(t, q, observer(), nil)
	if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
		t.Fatal("lost CPU attempt completion accepted", err)
	}
	if current := job(t, q, first.ID); current.State != "queued" {
		t.Fatal("stale CPU result published", current.State)
	}
}
