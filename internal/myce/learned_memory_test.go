package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const boundedQuarter = "exploratory first run (bounded: 25% of host allowance)"

func setLearnedMemory(t *testing.T, q *Queue, on bool) {
	t.Helper()
	record := q.PriorityPolicy()
	p := record.Policy
	p.LearnedMemory = &on
	if _, err := q.SetPriorityPolicy(record.Revision, p); err != nil {
		t.Fatal(err)
	}
}

// One 32 GiB CPU host; the cold bound is a quarter of its allowance.
func learnedMemoryFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker-host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1" // Workers report their backend probe.
	check(t, q, m, nil)
	return q, clock, m
}

func runMemory(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, peak float64, mutate func(*Result)) *Job {
	t.Helper()
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID {
		t.Fatalf("not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if mutate != nil {
		mutate(&r)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	return j
}

func reopenQueue(t *testing.T, q *Queue, clock *fakeClock) *Queue {
	t.Helper()
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	fakeCheckpointAvailability(t, reopened, true)
	return reopened
}

func TestLearnedMemoryOmittedFigureLearnsAcrossReopen(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0) // No memory figure at all.
	if err := s.Validate(); err != nil {
		t.Fatal("omitted memory must pass static validation:", err)
	}
	first := runMemory(t, q, clock, m, s, 2, nil)
	if e := first.MemoryEnvelope; e.Source != boundedQuarter || e.Basis != "exploratory" || first.Spec.Needs.HostMemoryGB != 8 || first.Spec.Needs.Memory != "auto" {
		t.Fatalf("cold run not bounded: %+v needs=%+v", e, first.Spec.Needs)
	}
	second := runMemory(t, q, clock, m, s, 2, nil)
	if e := second.MemoryEnvelope; e.Source != "accepted peak × 1.25" || e.Basis != "learned" || e.Evidence != 1 || second.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("second run did not learn: %+v", e)
	}

	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	busy := cpuSpec(1, 30) // Explicit figure: kept exactly, holds the host.
	busy.Name = "busy"
	held := submit(t, q, busy)
	if j := check(t, q, m, nil).Job; j == nil || j.ID != held.ID {
		t.Fatal("declared job not admitted")
	} else if e := j.MemoryEnvelope; e.Basis != "declared" || e.Evidence != 0 || j.Spec.Needs.HostMemoryGB != 30 {
		t.Fatalf("explicit figure changed: %+v %+v", e, j.Spec.Needs)
	} else if !strings.Contains(JobTiming(j), "memory 30.") || !strings.Contains(JobTiming(j), "declared") {
		t.Fatalf("status hides the declared figure: %s", JobTiming(j))
	}
	waiting := submit(t, q, s)
	check(t, q, m, nil)
	reason := job(t, q, waiting.ID).Reason
	if !strings.Contains(reason, "learned from 2 comparable accepted attempts: accepted peak × 1.25") {
		t.Fatalf("queued reason lacks source and evidence: %s", reason)
	}
}

func TestLearnedMemoryBoundsGrowAndNeverShrink(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0)
	s.Name = "suite"
	constrained := func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		r.MemoryPeak.Events = &MemoryEventCounts{Max: 1627}
	}
	// #559 replay: a failed ceiling-bound attempt is a lower bound only.
	runMemory(t, q, clock, m, s, 6, constrained)
	next := runMemory(t, q, clock, m, s, 3, nil)
	if e := next.MemoryEnvelope; next.Spec.Needs.HostMemoryGB != 12 || !strings.HasSuffix(e.Source, "(bounded: constrained peak × 2)") {
		t.Fatalf("constrained peak did not raise the bound: %+v", e)
	}
	// A small constrained peak never lowers the quarter-allowance bound.
	other := s
	other.Run.Command = []string{"echo", "other"}
	runMemory(t, q, clock, m, other, 1, constrained)
	if j := runMemory(t, q, clock, m, other, 1, nil); j.Spec.Needs.HostMemoryGB != 3*similarMemoryFactor {
		t.Fatalf("similar accepted peak did not bound the cold run: %+v", j.MemoryEnvelope)
	} else if e := j.MemoryEnvelope; e.Evidence != 1 || !strings.Contains(e.Origin(), "bounded by 1 similar accepted attempts") {
		t.Fatalf("similar evidence not reported: %s", e.Origin())
	}

	// OOM on a bounded first run doubles without spending the one retry; at
	// half the allowance, host-wide exploration and its retry take over.
	fresh := cpuSpec(1, 0)
	fresh.Name = "fresh"
	queued := submit(t, q, fresh)
	for _, want := range []string{boundedQuarter, "exploratory first run; OOM retry floor"} {
		j := check(t, q, m, nil).Job
		if j == nil || j.ID != queued.ID || j.MemoryEnvelope.Source != want || j.OOMRetries != 0 {
			t.Fatalf("want %q: %+v %s", want, j.MemoryEnvelope, job(t, q, queued.ID).Reason)
		}
		if want != boundedQuarter && j.Spec.Needs.HostMemoryGB < 16 {
			t.Fatalf("retry shrank below the doubled floor: %+v", j.Spec.Needs)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(j, false)
		r.OutOfMemory = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	if got := job(t, q, queued.ID); got.State != "failed" || got.OOMRetries != 1 {
		t.Fatalf("host-wide OOM lost its one-retry limit: %s %s retries=%d", got.State, got.Reason, got.OOMRetries)
	}
}

func TestLearnedMemoryPolicyOffRestoresDeclaredFigures(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	data, _ := json.Marshal(q.PriorityPolicy())
	if strings.Contains(string(data), "learned_memory") {
		t.Fatalf("default switch must stay off the wire for older CLIs: %s", data)
	}
	setLearnedMemory(t, q, false)
	// An older CLI's round trip omits the field and must not flip it back.
	record := q.PriorityPolicy()
	record.Policy.LearnedMemory = nil
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	_, err := q.Submit(cpuSpec(1, 0))
	var refused *HTTPError
	if !errors.As(err, &refused) || refused.Code != "job_memory_invalid" {
		t.Fatalf("omitted memory accepted with learning off: %v", err)
	}
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	j := runMemory(t, q, clock, m, s, 2, nil)
	if j.MemoryEnvelope.Source != "exploratory first run" || j.Spec.Needs.HostMemoryGB <= 16 {
		t.Fatalf("switch off did not restore host-wide exploration: %+v", j.MemoryEnvelope)
	}
}
