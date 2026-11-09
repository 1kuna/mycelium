package myce

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageRecordingBusyIdleAndDecay(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
	})

	t.Run("UsageUnknownOutageIsNotIdle", func(t *testing.T) {
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

	})

	t.Run("UsageAndFairnessPersistAcrossRestart", func(t *testing.T) {
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
		for range 30 {
			c.Advance(time.Second)
			check(t, q, m, nil)
		}
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
		if e != nil || len(r.Machines) != 1 || math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-9 {
			t.Fatal(r, e, q.s.Policies)
		}
		c.Advance(30 * time.Second)
		r, e = q.Usage(start)
		if e != nil || math.Abs(r.Machines[0].IdleHours-30.0/3600) > 1e-9 {
			t.Fatal("restart unknown time counted", r, e)
		}

	})
}
