package myce

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"slices"
	"testing"
	"time"
)

func TestIdleHistoryJoinsPhysicalOccupancyAndAdmission(t *testing.T) {
	q, clock := fixture(t)
	q.evidenceMaintenance.capacity = idleTestCapacity
	setStatusTestCapacity(q)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	idle := cpuMachine("idle", "physical-idle", "cpu", 8, 32)
	worker := machine("worker", "fake", "physical-working")
	worker.Devices = fakeMachineDevices()
	for i := range worker.Devices {
		worker.Devices[i].Platform = "fake"
		worker.Devices[i].MemoryMeasured = true
	}
	check(t, q, idle, nil)
	check(t, q, worker, nil)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"fixture": {Weight: 1, MaxConcurrentExperiments: 1}}); err != nil {
		t.Fatal(err)
	}
	gpu := projectSpec("fixture")
	gpu.AllowedMachines = []string{worker.ID}
	gpu.Needs.Platform = Platform{Required: []string{"fake"}}
	holder := submit(t, q, gpu)
	reply := check(t, q, worker, nil)
	if len(reply.Attempts) != 1 || reply.Attempts[0].Job == nil || reply.Attempts[0].ID != holder.ID {
		t.Fatalf("expected a one-card assignment, got %+v", reply.Attempts)
	}
	run := reply.Attempts[0].Job
	if err := q.Started(Result{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Machine: worker.ID, Session: worker.Session}); err != nil {
		t.Fatal(err)
	}

	cpu := cpuSpec(1, 1)
	cpu.Project = "fixture"
	cpu.AllowedMachines = []string{idle.ID}
	for range 2 {
		submit(t, q, cpu)
	}
	capture := func() {
		t.Helper()
		r, err := q.Checkin(Checkin{Machine: idle, period: time.Minute})
		if err != nil || r.Job != nil {
			t.Fatalf("expected a project-capacity refusal, got %+v, %v", r.Job, err)
		}
	}
	capture()
	start := clock.Now()
	clock.Advance(10 * time.Second)
	if _, err := q.ChangeHostMaintenance(idle.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	// Displace a published capture, as when the off-lock projector is busy
	// and a newer commit arrives before it claims the mailbox.
	q.mu.Lock()
	q.idleRevision++
	q.mu.Unlock()
	clock.Advance(10 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}

	query := "?sections=idle,usage&idle_intervals=true&idle_since=" + url.QueryEscape(start.Format(time.RFC3339Nano))
	s := readStatusOptions(t, q, query)
	if s.Idle == nil {
		t.Fatal("status missing idle report")
	}
	class := s.Idle.ByMachineClass[idleMachineClass([]*Machine{&idle})]
	if class == nil {
		t.Fatal("missing idle machine class", s.Idle.ByMachineClass)
	}
	assertSeconds := func(label string, hours, want float64) {
		t.Helper()
		if math.Abs(hours*3600-want) > 1e-9 {
			t.Errorf("%s: got %g s, want %g s", label, hours*3600, want)
		}
	}
	assertSeconds("idle while two jobs waited", s.Idle.MachineHours, 10)
	assertSeconds("actual first refusal", s.Idle.ByReason[predicateProjectCapacity], 10)
	assertSeconds("planned drain", class.States[idleHold], 10)
	assertSeconds("capture gap", class.States[idleUnknown], 10)
	assertSeconds("occupied physical host", s.Idle.ByMachineClass[idleMachineClass([]*Machine{&worker})].States[idleWorking], 20)
	if !slices.ContainsFunc(s.Usage.Cards, func(card CardUsage) bool {
		return card.Machine == worker.ID && card.IdleHours > 0 && card.GPUHours == 0
	}) {
		t.Fatalf("idle card on occupied machine missing from usage: %+v", s.Usage.Cards)
	}
	for _, r := range s.Idle.Intervals {
		if r.Host == idle.HostID && r.State == idleRefused {
			if r.Online == nil || !*r.Online || r.ReadyDemand == nil || *r.ReadyDemand != 2 ||
				r.InFlight == nil || *r.InFlight != 0 || r.ReleaseCustody == nil ||
				len(r.Targets) != 1 || len(r.Targets[0].Demand) != 2 {
				t.Fatalf("incomplete joined interval: %+v", r)
			}
			for _, d := range r.Targets[0].Demand {
				if d.Decision == nil || d.Decision.Outcome != AdmissionRejected ||
					d.Decision.Predicate != predicateProjectCapacity || d.Compatibility != "compatible" || d.Qualification != "accepted" {
					t.Fatalf("missing observed refusal or target fit: %+v", d)
				}
			}
			break
		}
	}
	q = reopenVariants(t, q, clock)
	after := readStatusOptions(t, q, query)
	assertSeconds("durable report after queue restart", after.Idle.MachineHours, 10)
}

func idleTestCapacity(context.Context) (RetentionCapacity, error) {
	return RetentionCapacity{MemoryAvailableBytes: 1 << 30, DiskAvailableBytes: 1 << 30}, nil
}

func BenchmarkIdleIntervalTick(b *testing.B) {
	q, clock := fixture(b)
	q.evidenceMaintenance.capacity = idleTestCapacity
	for i := range 5 {
		m := cpuMachine(fmt.Sprintf("worker-%d", i), fmt.Sprintf("host-%d", i), "cpu", 8, 32)
		check(b, q, m, nil)
	}
	for i := range 60 {
		s := cpuSpec(1, 1)
		s.Name = fmt.Sprintf("work-%d", i)
		submit(b, q, s)
	}
	index := q.publishedReads.Load()
	c := idleCapture{index: index}
	at := index.capture.source.clock.Now()
	previous, err := projectIdleCapture(c, at, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("projection", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := projectIdleCapture(c, at, previous); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("persistence", func(b *testing.B) {
		b.ReportAllocs()
		batch := make([]idleStored, 0, len(previous))
		for host, observed := range previous {
			h := idleHistory{}
			next := map[string]idleObservation{host: observed}
			h.observe(1, at, next, true)
			h.observe(2, at.Add(time.Second), next, true)
			batch = append(batch, h.open[host])
		}
		h := &idleHistory{db: q.idleHistory.db}
		b.ResetTimer()
		for i := range b.N {
			for k := range batch {
				batch[k].start = at.Add(time.Duration(i) * time.Second).UnixNano()
				batch[k].end = batch[k].start + int64(time.Second)
			}
			if err := h.persist(context.Background(), batch, 1<<30); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("queue", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
