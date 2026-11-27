package myce

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryStopAttemptForecast(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		expected, used, free float64
		growth               float64
		wantStop             bool
	}{
		{"2026-11-26T18:24Z", 88, 63, 50.6, 8.06, false},
		{"2026-11-26T18:24Z with stop keep", 88, 63, 50.6 - 15, 8.06, false},
		{"2091 FP8 overshoot", 60, 79.6, 16.4, 4.34, true},
		{"2091 combined counter overshoot", 87, 95, 1, 1.1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m := memoryAdmissionFixture(t)
			server := httptest.NewServer(API{Queue: q, Token: "memory-stop-fixture"}.Handler())
			t.Cleanup(server.Close)
			w := Worker{Clock: clock, Interval: time.Second, Client: Client{URL: server.URL, Token: "memory-stop-fixture",
				Clock: clock, memoryShadowWire: new(atomic.Bool), memoryStopWire: new(atomic.Bool)}}

			var running []*Job
			c := Checkin{Machine: m}
			for _, name := range []string{"load", "newer-peer"} {
				s := cpuSpec(1, 2)
				s.Name = name
				want := submit(t, q, s)
				clock.Advance(time.Second)
				reply, err := w.checkin(t.Context(), c)
				if err != nil {
					t.Fatal(err)
				}
				j := assignedJob(reply)
				if j == nil || j.ID != want.ID {
					t.Fatal("fixture did not assign", name)
				}
				if err := q.Started(result(j, true)); err != nil {
					t.Fatal(err)
				}
				running = append(running, j)
				c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: m.Session, Started: true})
			}
			p := MemoryShadowPool{Name: "ram", CapacityGB: 96, FreeGB: shadowGB(tc.free), Attempts: []MemoryShadowAttempt{
				{ID: running[0].ID, Attempt: 1, ExpectedGB: tc.expected, FootprintGB: shadowGB(tc.used), HighWaterGB: tc.used, Growth: tc.growth},
				{ID: running[1].ID, Attempt: 1, ExpectedGB: 2, FootprintGB: shadowGB(2), HighWaterGB: 2},
			}}
			p.sum()
			prior := p
			p.forecast(&prior, 1, 11)
			clock.Advance(time.Second)
			c.Machine.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{p}, ReactionSeconds: 11, sampledAt: clock.Now()}
			reply, err := w.checkin(t.Context(), c)
			if err != nil {
				t.Fatal(err)
			}
			if !w.supportsMemoryStops() {
				t.Fatal("worker and queue did not negotiate R4")
			}
			for i, j := range running {
				got := job(t, q, j.ID)
				if i == 0 && tc.wantStop {
					if stop := priorityStop(got); got.State != "stopping" || stop == nil || stop.MemoryPool == nil ||
						stop.MemoryPool.Trigger != "attempt_forecast" {
						t.Fatalf("expected the over-E load to get the R4 stop: %s %+v", got.State, stop)
					}
				} else if got.State != "running" {
					t.Fatalf("unexpected stop for %s: %s %s", j.Spec.Name, got.State, got.Reason)
				}
			}
			var stopped int
			for _, d := range reply.Attempts {
				if d.Stop {
					stopped++
				}
			}
			if (stopped == 1) != tc.wantStop || stopped > 1 {
				t.Fatalf("unexpected stop directives: %d", stopped)
			}
		})
	}
}

func TestMemoryStopCardUsesAttemptForecast(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	s := spec("experiment")
	s.Needs.MemoryGB = 4
	want := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != want.ID {
		t.Fatal("fixture did not assign card attempt")
	}
	if err := q.Started(result(j, true)); err != nil {
		t.Fatal(err)
	}
	card := m.Devices[0]
	e := card.BudgetGB
	a := MemoryShadowAttempt{ID: j.ID, Attempt: j.Attempt, ExpectedGB: e, FootprintGB: shadowGB(e - 1), HighWaterGB: e - 1, Growth: 2}
	pool := MemoryShadowPool{Name: "card/" + card.ID, CapacityGB: e, FreeGB: shadowGB(2), Attempts: []MemoryShadowAttempt{a}}
	pool.sum()
	prior := pool
	pool.forecast(&prior, 1, 11)
	clock.Advance(time.Second)
	m.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{pool}, ReactionSeconds: 11, stopForecast: true}
	used := e + 1 // The legacy budget guard would stop on this counter.
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
		Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{DeviceGB: &used}}}}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "running" {
		t.Fatalf("card stopped a load that fits inside E: %s", got.Reason)
	}
	a.FootprintGB, a.HighWaterGB = shadowGB(e+1), e+1
	pressure := MemoryShadowPool{Name: pool.Name, CapacityGB: e, FreeGB: shadowGB(0), Attempts: []MemoryShadowAttempt{a}}
	pressure.sum()
	pressure.forecast(&pool, 1, 11)
	clock.Advance(time.Second)
	c.Machine.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{pressure}, ReactionSeconds: 11, stopForecast: true}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if stop := priorityStop(got); got.State != "stopping" || stop == nil || stop.DevicePool == nil ||
		stop.DevicePool.Trigger != "attempt_forecast" || stop.DevicePool.OverageGB != 1 {
		t.Fatalf("card stop lost R4 evidence: %s %+v", got.State, stop)
	}
}

func TestMemoryStopImmediateKillsProductionThroughLimitResult(t *testing.T) {
	w := recoveryWorker(t)
	clock := &workerFakeClock{now: time.Unix(100, 0)}
	w.Clock = clock
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sleep", "60"})
	j.Spec.Priority, j.Spec.Class, j.Spec.Resumable = "production", "production", true
	startBy := clock.Now().Add(time.Hour)
	j.Spec.StartBy = &startBy
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stopDirective(clock.Now(), 0, true, true) })
	p.mu.Lock()
	p.stopGroup, p.stopGrace = true, time.Hour
	p.mu.Unlock()

	probe := memoryStopProbe{fastestGBPerSecond: 8, detection: 500 * time.Millisecond, killToFree: 250 * time.Millisecond}
	keep := 4 + probe.keepGB()
	free := keep
	stop := immediateMemoryStop{pool: "ram", keep: keep, expected: 8, free: func(context.Context) (float64, error) { return free, nil }}
	// Without a qualified stop-time record there is no stop term.
	if keep != 10 || (memoryStopProbe{fastestGBPerSecond: 8, detection: time.Second}).keepGB() != 0 ||
		w.immediateRAMStop(j).free != nil {
		t.Fatalf("invalid keep or probe gate: %v", keep)
	}
	reading := shadowMemoryReading{At: clock.Now(), GB: 9}
	if stop.sample(t.Context(), clock, p, reading) {
		t.Fatal("kill at keep")
	}
	free, reading.GB = keep-1, 8
	if stop.sample(t.Context(), clock, p, reading) {
		t.Fatal("kill within E")
	}
	reading.GB = 9
	if !stop.sample(t.Context(), clock, p, reading) {
		t.Fatal("single fresh below-keep sample did not kill production")
	}
	r := d5Finish(t, p)
	cause := safeRuntimeCause(r.ExecutionCause)
	if r.Success || !r.Interrupted || r.OutOfMemory || r.ReleasePending || cause == nil || cause.Class != memoryPressureCause ||
		cause.RSSLimit.pool() != "ram" || cause.RSSLimit.LimitBytes != 8<<30 || cause.RSSLimit.ObservedBytes != 9<<30 {
		t.Fatalf("unexpected durable limit result: %+v, cause %+v", r, cause)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.outcome.Signals) != 1 || p.outcome.Signals[0].Signal != "KILL" {
		t.Fatalf("immediate stop used grace or the wrong signal: %+v", p.outcome.Signals)
	}
}
