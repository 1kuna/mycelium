package myce

import (
	"math"
	"testing"
	"time"
)

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

func TestUsageLegacyGroupsAndModelCalls(t *testing.T) {
	q, c := fixture(t)
	start := c.Now().Truncate(time.Minute)
	c.Advance(time.Minute)
	// Historical batch rows already charged physical cards; the group batch
	// row duplicates card 0. The group model then serves both cards for 15s.
	for _, row := range []usageSample{
		{Device: "0", Job: "batch", Project: "atlas", Busy: 30, Observed: 30},
		{Device: "0", Idle: 30, Observed: 30},
		{Device: "1", Idle: 60, Observed: 60},
		{Device: "0,1", Job: "batch", Project: "atlas", Busy: 30, Observed: 30},
		{Device: "0,1", Model: "model", Project: "atlas", Busy: 15, Observed: 15},
		{Device: "0,1", Idle: 15, Observed: 15},
	} {
		_, err := q.db.Exec("INSERT INTO usage_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", start.Unix(), 60, "dual", row.Device, row.Project, row.Job, row.Model, row.Busy, row.Idle, row.Pinned, 0, row.Observed)
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err := q.readUsage(q.usageReader, start)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Cards) != 2 || math.Abs(r.Machines[0].GPUHours-1.0/60) > 1e-9 || math.Abs(r.Machines[0].IdleHours-1.0/60) > 1e-9 || math.Abs(r.Projects[0].GPUHours-1.0/60) > 1e-9 {
		t.Fatalf("legacy group double count: %+v", r)
	}
}
