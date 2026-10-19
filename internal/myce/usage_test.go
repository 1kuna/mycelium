package myce

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageRecordingBusyIdleAndDecay(t *testing.T) {
	q, c := fixture(t)
	start := c.Now()
	m := machine("one", "fake")
	j := submit(t, q, projectSpec("atlas"))
	run := check(t, q, m, nil).Job
	if e := q.Started(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: run.Attempt}}); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		c.Advance(30 * time.Second)
		check(t, q, m, run)
	}
	r, e := q.Usage(start)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Projects) != 1 || math.Abs(r.Projects[0].GPUHours-2.0/60) > 1e-8 || r.Machines[0].BusyPercent != 100 {
		t.Fatalf("bad usage %+v", r)
	}
	if e = q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: run.Attempt}, Success: true}); e != nil {
		t.Fatal(e)
	}
	check(t, q, m, nil)
	c.Advance(30 * time.Second)
	check(t, q, m, nil)
	r, e = q.Usage(start)
	if e != nil {
		t.Fatal(e)
	}
	if math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-8 {
		t.Fatal(r)
	}
	u := q.s.FairUsage[fairUsageKey("experiment", "gpu", "atlas")].Seconds
	if u <= 119 || u >= 120 {
		t.Fatal("decaying integral", u)
	}
	var count int
	if e = q.db.QueryRow("SELECT COUNT(*) FROM usage_samples WHERE span=60").Scan(&count); e != nil || count != 3 {
		t.Fatal(count, e)
	}
}
func TestUsageUnknownOutageIsNotIdle(t *testing.T) {
	q, c := fixture(t)
	start := c.Now()
	m := machine("one", "fake")
	check(t, q, m, nil)
	c.Advance(2 * time.Hour)
	r, e := q.Usage(start)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Machines) != 1 || math.Abs(r.Machines[0].IdleHours-1.0/60) > 1e-8 {
		t.Fatalf("unknown outage counted %+v", r)
	}
}
func TestUsageDownsamplingAndRetention(t *testing.T) {
	q, c := fixture(t)
	start := c.Now()
	m := machine("one", "fake")
	check(t, q, m, nil)
	for i := 0; i < 4; i++ {
		c.Advance(30 * time.Second)
		check(t, q, m, nil)
	}
	c.Advance(8 * 24 * time.Hour)
	if e := q.Tick(); e != nil {
		t.Fatal(e)
	}
	var minutes, hours int
	q.db.QueryRow("SELECT COUNT(*) FROM usage_samples WHERE span=60").Scan(&minutes)
	q.db.QueryRow("SELECT COUNT(*) FROM usage_samples WHERE span=3600").Scan(&hours)
	if minutes != 0 || hours != 1 {
		t.Fatal(minutes, hours)
	}
	r, e := q.Usage(start)
	if e != nil || math.Abs(r.Machines[0].IdleHours-3.0/60) > 1e-8 {
		t.Fatal(r, e)
	}
	c.Advance(90 * 24 * time.Hour)
	if e = q.Tick(); e != nil {
		t.Fatal(e)
	}
	var count int
	q.db.QueryRow("SELECT COUNT(*) FROM usage_samples").Scan(&count)
	if count != 0 {
		t.Fatal("old usage retained", count)
	}
}
func TestUsageSimultaneousProjectsNotDoubleCounted(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	check(t, q, m, nil)
	a := submit(t, q, projectSpec("atlas"))
	b := submit(t, q, projectSpec("verso"))
	now := c.Now()
	for _, j := range []*Job{q.s.Jobs[a.ID], q.s.Jobs[b.ID]} {
		j.State = "running"
		j.Machine = m.ID
		j.StartedAt = &now
	}
	start := c.Now()
	c.Advance(time.Minute)
	r, e := q.Usage(start)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Projects) != 2 || math.Abs(r.Machines[0].GPUHours-1.0/60) > 1e-8 || r.Machines[0].BusyPercent != 100 {
		t.Fatal(r)
	}
}
func TestUsageRollbackIsAtomic(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	check(t, q, m, nil)
	start := c.Now()
	c.Advance(30 * time.Second)
	if _, e := q.Checkin(Checkin{}); e == nil {
		t.Fatal("expected rejection")
	}
	r, e := q.Usage(start)
	if e != nil || math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-9 {
		t.Fatal(r, e)
	}
}

func TestUsagePinDoesNotMakeIdleDeviceBusy(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	check(t, q, m, nil)
	if e := q.ConfigureModels(ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "pinned", Engine: "fake", Path: "fake", MemoryGB: 1, Platforms: []string{"fake"}}}}); e != nil {
		t.Fatal(e)
	}
	q.s.Pins = []ModelPin{{Model: "pinned", Project: "atlas", Assigned: m.ID, Machine: m.ID}}
	q.s.ModelSlots[slotKey(m.ID, "pinned")] = &ModelSlot{Model: "pinned", Machine: m.ID, State: "ready"}
	start := c.Now()
	c.Advance(30 * time.Second)
	r, e := q.Usage(start)
	if e != nil {
		t.Fatal(e)
	}
	if r.Machines[0].GPUHours != 0 || r.Machines[0].BusyPercent != 0 || r.Projects[0].PinnedHours <= 0 {
		t.Fatal(r)
	}
}
func TestUsageProductionPreemptionAndStartHistory(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	experiment := submit(t, q, projectSpec("atlas"))
	run := check(t, q, m, nil).Job
	_ = experiment
	start := c.Now()
	production := submit(t, q, spec("production"))
	if len(q.s.Preemptions) != 1 {
		t.Fatal("missing preemption", q.s.Preemptions)
	}
	c.Advance(2 * time.Second)
	if e := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Interrupted: true}); e != nil {
		t.Fatal(e)
	}
	p := check(t, q, m, nil).Job
	if p == nil || p.ID != production.ID {
		t.Fatal("production not assigned", p)
	}
	if e := q.Started(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: p.ID, Attempt: p.Attempt}}); e != nil {
		t.Fatal(e)
	}
	r, e := q.Usage(start)
	if e != nil || len(r.Preemptions) != 1 || len(r.ProductionStarts) != 1 || r.ProductionStarts[0].Seconds != 2 {
		t.Fatal(r, e)
	}
}

func TestUsageAndFairnessPersistAcrossRestart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(t.TempDir(), "queue.db")
	c := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	q, e := Open(path, root, c)
	if e != nil {
		t.Fatal(e)
	}
	if e = q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 2}}); e != nil {
		t.Fatal(e)
	}
	m := machine("one", "fake")
	run := check(t, q, m, nil)
	_ = run
	start := c.Now()
	c.Advance(30 * time.Second)
	if e = q.Tick(); e != nil {
		t.Fatal(e)
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	q, e = Open(path, root, c)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	r, e := q.Usage(start)
	if e != nil || len(r.Machines) != 1 || math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-9 || q.weight("atlas") != 2 {
		t.Fatal(r, e, q.s.Policies)
	}
	c.Advance(30 * time.Second)
	r, e = q.Usage(start)
	if e != nil || math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-9 {
		t.Fatal("restart unknown time counted", r, e)
	}
}
