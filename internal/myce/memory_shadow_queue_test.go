package myce

import (
	"testing"
	"time"
)

func TestMemoryShadowWeeklyCounters(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var monitor shadowQueueMonitor
	order := uint64(0)
	observe := func(attempt int, free, owners, forecast float64, overshoot, neighbor bool) {
		t.Helper()
		order++
		now := clock.Now()
		p := MemoryShadowPool{Name: "ram", CapacityGB: 96, FreeGB: shadowGB(free), OwnersGB: owners, OwnersForecastGB: forecast}
		candidates := map[Execution]shadowCandidate{}
		if attempt != 0 {
			a := MemoryShadowAttempt{ID: "job", Attempt: attempt, ExpectedGB: 70, HighWaterGB: 70, FootprintGB: shadowGB(70)}
			if overshoot {
				a.FootprintGB, a.HighWaterGB = shadowGB(80), 80
			}
			p.Attempts = append(p.Attempts, a)
			candidates[Execution{ID: a.ID, Attempt: a.Attempt}] = shadowCandidate{priority: 2, started: now.Add(-time.Hour)}
		}
		if neighbor {
			p.Attempts = append(p.Attempts, MemoryShadowAttempt{ID: "neighbor", Attempt: 1, ExpectedGB: 2, HighWaterGB: 10, FootprintGB: shadowGB(10), Growth: 2})
			candidates[Execution{ID: "neighbor", Attempt: 1}] = shadowCandidate{priority: 0, started: now.Add(-time.Hour)}
		}
		p.sum()
		if forecast > owners || neighbor || overshoot {
			p.ForecastFreeGB = shadowGB(-1)
		}
		monitor.observe(shadowObservation{worker: "worker", host: "host", session: "session", order: order, at: now,
			report: &MemoryShadowReport{ReactionSeconds: 11, Pools: []MemoryShadowPool{p}}, candidates: candidates})
	}
	observe(1, 16, 10, 27, false, false)
	clock.Advance(time.Second)
	observe(1, 16, 10, 27, false, false) // same physical attempt is counted once
	clock.Advance(time.Second)
	observe(0, 86, 10, 10, false, false)
	for elapsed := time.Duration(0); elapsed <= 24*time.Hour; elapsed += 10 * time.Second {
		clock.Advance(10 * time.Second)
		observe(0, 86, 10, 10, false, false)
	}
	clock.Advance(time.Second)
	observe(2, 16, 10, 27, false, false)
	clock.Advance(time.Second)
	observe(0, 86, 10, 10, false, false)
	clock.Advance(time.Second)
	observe(3, 6, 10, 10, false, true)
	clock.Advance(time.Second)
	observe(0, 86, 10, 10, false, false)
	clock.Advance(time.Second)
	observe(4, 12, 4, 4, true, false)
	clock.Advance(time.Second)
	observe(5, 20, 6, 6, false, false)
	clock.Advance(10 * time.Second)
	observe(5, 20, 6, 6, false, false)

	week := monitor.week(clock.Now())
	if week.Victim != 3 || week.OverE != 1 || week.AttemptHours != 4 || week.Repeat["lapsed_owners_hold"] != 1 ||
		week.Repeat["co_tenant_over_e"] != 1 || week.Repeat["own_overshoot"] != 1 || week.OwnersHolds != 2 ||
		week.Raises != 1 || week.RaiseGiBHours <= 0 || week.OwnersGiBHours < 24*17 {
		t.Fatalf("weekly shadow counts %+v", week)
	}
	clock.Advance(shadowWeek + time.Hour)
	if later := monitor.week(clock.Now()); later.Victim != 0 || later.OverE != 0 || later.OwnersGiBHours != 0 || len(later.Repeat) != 0 {
		t.Fatalf("old data survived the observation window: %+v", later)
	}
}
