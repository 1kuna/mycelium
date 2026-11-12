package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Synthetic incident shape: 80 production jobs, 30 experiments, 1,400 curves
// and five workers. The measured worker offers both CUDA and CPU targets.
func candidateCostFixture(t testing.TB) (*Queue, []Machine) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	q.checkpointProbe = func() bool { return true }
	workers := make([]Machine, 5)
	for i := range workers {
		id := fmt.Sprintf("worker-%d", i)
		m := cpuMachine(id, id, "cpu", 16, 128)
		m.ExtensionFingerprint, m.CPUExtensionFingerprint = "synthetic-cpu", "synthetic-cpu"
		m.Devices = []MachineDevice{}
		if i == 0 {
			m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", CapabilityProfile: &CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", MemoryLayout: "separate", Accelerators: []AcceleratorProfile{{Type: "cuda", MemoryBytes: 48 << 30}}}, MemoryMode: "fixed", Selector: "0", TotalMemoryGB: 48, BudgetGB: 48, FreeMemoryGB: 48, Identity: "synthetic-card", ExtensionFingerprint: "synthetic-cuda"}}
		}
		workers[i] = m
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 110 {
		id := fmt.Sprintf("queued-%03d", i)
		s := cpuSpec(2, 0)
		s.Project, s.Name = fmt.Sprintf("project-%d", i%3), fmt.Sprintf("program-%d", i%12)
		s.Run.Command = []string{"echo", s.Name}
		s.Needs.Memory = "auto"
		s.Needs.Platform = Platform{Required: []string{"cpu"}}
		s.Priority = "production"
		if i >= 80 {
			s.Priority = "experiment"
		}
		if i%3 == 0 {
			s.Needs.Platform = Platform{Required: []string{"cuda"}}
			s.Needs.MemoryGB = 4
		}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		j := &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now().Add(time.Duration(i) * time.Millisecond)}
		if !s.Needs.cpuOnly() {
			j.MemoryFallback = &SizeHint{MemoryGB: 4, HostMemoryGB: 4}
		}
		q.s.Jobs[id] = j
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	peak := 2.0
	for i := range 1400 {
		source := q.s.Jobs[fmt.Sprintf("queued-%03d", i%110)]
		provenance := *source
		// Half of the recipes are cold, with similar-family history only.
		if i%12 >= 6 {
			provenance.Spec.Run.Command = []string{"echo", fmt.Sprintf("retained-%d", i)}
		}
		if i >= 60 {
			provenance.Spec.Args = []string{fmt.Sprint(i / 5)}
		}
		m := q.machineTargets(q.s.Machines[workers[i%5].ID])[0]
		if !source.Spec.Needs.cpuOnly() {
			m = q.machineTargets(q.s.Machines[workers[0].ID])[1]
		}
		provenance.Machine, provenance.Session = m.ID, m.Session
		if m.Device != "" {
			provenance.AllocatedDevices = []string{m.Device}
		}
		key, why := q.workObservationKey(&provenance)
		if key == nil {
			t.Fatal(why)
		}
		q.s.WorkObservations[fmt.Sprint(i)] = WorkObservation{Key: *key, MemoryFamily: memoryFamily(source), MemoryConfiguration: memoryConfiguration(provenance.Spec.Needs, provenance.Spec.Args, provenance.Spec.Program), MemoryLayout: "separate", MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}, MemorySamples: 2, Samples: 2, RecordedAt: clock.Now(), LastSeconds: 1}
	}
	// Retained constrained attempts exercise same-OS diagnostic lower bounds.
	for i := range 160 {
		source := q.s.Jobs[fmt.Sprintf("queued-%03d", i%110)]
		j := *source
		j.ID, j.State, j.Attempt = fmt.Sprintf("retained-%03d", i), "succeeded", 1
		m := q.machineTargets(q.s.Machines[workers[i%5].ID])[0]
		if !j.Spec.Needs.cpuOnly() {
			m = q.machineTargets(q.s.Machines[workers[0].ID])[1]
			j.AllocatedDevices = []string{m.Device}
		}
		j.Machine, j.Session = m.ID, m.Session
		j.StartedAt, j.FinishedAt, j.ReleaseConfirmed = &clock.now, &clock.now, true
		key, why := q.workObservationKey(&j)
		if key == nil {
			t.Fatal(why)
		}
		j.Timing = &WorkTiming{Key: key, Execution: Execution{ID: j.ID, Attempt: 1}, Machine: m.ID, Session: m.Session}
		lower := 3.0
		j.MemoryAttempts = []AttemptMemoryFacts{{Attempt: 1, Machine: m.ID, Session: m.Session, MemoryPeak: &AttemptMemoryPeak{HostGB: &lower, Constrained: true}}}
		q.s.Jobs[j.ID] = &j
	}
	// Occupy the entire CPU allowance without preemptible work. All queued jobs
	// still participate in candidate planning, and successive check-ins are stable.
	for i, m := range workers {
		id := fmt.Sprintf("running-%d", i)
		s := cpuSpec(16, 8)
		s.Needs.Platform = Platform{Required: []string{"cpu"}}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		s.Priority = "production"
		q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "running", Attempt: 1, Machine: m.ID, Session: m.Session, StartedAt: &clock.now, Queued: clock.Now()}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return q, workers
}

func BenchmarkCheckinCandidateCost(b *testing.B) {
	q, workers := candidateCostFixture(b)
	// Exercise overdue actual decisions as well as unchanged diagnostics.
	q.clock.(*fakeClock).Advance(2 * time.Second)
	benchmarkCheckinCandidateCost(b, q, workers)
}

func BenchmarkCheckinCandidateCostIdle(b *testing.B) {
	q, workers := candidateCostFixture(b)
	if err := q.update(func() error {
		for id, j := range q.s.Jobs {
			if j.State == "queued" {
				q.deleteJob(id)
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	benchmarkCheckinCandidateCost(b, q, workers)
}

func benchmarkCheckinCandidateCost(b *testing.B, q *Queue, workers []Machine) {
	// Measure stable check-ins separately from the one-time explanation of a
	// newly observed wait. Both baseline and candidate use the same warm-up.
	m := workers[0]
	request := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: "running-0", Attempt: 1}, Session: m.Session, Started: true}}}
	if _, err := q.CheckinContext(context.Background(), request); err != nil {
		b.Fatal(err)
	}
	var total, maximum time.Duration
	q.observeUpdate = func(_, hold time.Duration) { total += hold; maximum = max(maximum, hold) }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := q.CheckinContext(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "hold-ns/op")
	b.ReportMetric(float64(maximum.Nanoseconds()), "max-hold-ns")
}

// Freeze the original planner's full per-target candidate sequence, resolved
// resource vectors and memory envelopes, not just its first winning job.
// #806 intentionally changes these envelopes: physical cold headroom, measured
// local uncertainty and uncalibrated transfers replace universal multipliers.
// The refreshed digests retain all 402 decisions for each history shape.
// The grouped fixture adds 480 decisions per shape, frozen before allocation
// caching, and both fixtures freeze the five actual next-pull placements too.
// ProbeFloorGB adds metadata in 180 retained-history envelopes; removing only
// that field reproduces the prior digest with identical decisions/resources.
// #1017 adds LaunchDriftGB/AvailableHostGB in those same 180 envelopes. Clearing
// only those fields reproduces e7757cb9 with all 402 decisions/resources intact.
func TestCandidateCostPlacementEquivalence(t *testing.T) {
	for _, fixture := range []struct {
		retained, grouped bool
	}{{true, false}, {false, false}, {true, true}, {false, true}} {
		retained := fixture.retained
		t.Run(fmt.Sprintf("retained=%v/grouped=%v", retained, fixture.grouped), func(t *testing.T) {
			q, _ := candidateCostFixture(t)
			q.mu.Lock()
			defer q.mu.Unlock()
			if fixture.grouped {
				owner := q.s.Machines["worker-0"]
				for i := 1; i < 3; i++ {
					device := owner.Devices[0]
					device.ID, device.Selector = fmt.Sprint(i), fmt.Sprint(i)
					device.Identity = fmt.Sprintf("synthetic-card-%d", i)
					device.FreeMemoryGB -= float64(i * 8)
					owner.Devices = append(owner.Devices, device)
				}
				demand := *q.s.Jobs["queued-000"]
				demand.ID = "group-demand"
				demand.Spec.Needs.MemoryGB = 80
				demand.MemoryFallback = &SizeHint{MemoryGB: 80, HostMemoryGB: 4}
				q.s.Jobs[demand.ID] = &demand
			}
			if !retained {
				for id, j := range q.s.Jobs {
					if j.State == "succeeded" {
						delete(q.s.Jobs, id)
					}
				}
			}
			for i := range 5 {
				delete(q.s.Jobs, fmt.Sprintf("running-%d", i))
			}
			q.refreshHostMemory()
			q.refreshHostCPU()
			type decision struct {
				Target, Job string
				Needs       ResourceNeeds
				Envelope    *JobMemoryEnvelope
			}
			var decisions []decision
			scan := q.placementScan()
			for _, m := range scan.machines {
				set := q.candidates(m, scan)

				excluded := map[string]bool{}
				for j := set.pick(excluded); j != nil; j = set.pick(excluded) {
					decisions = append(decisions, decision{targetKey(m), j.ID, j.Spec.Needs, j.MemoryEnvelope})
					excluded[j.ID] = true
				}
			}
			data, err := json.Marshal(decisions)
			if err != nil {
				t.Fatal(err)
			}
			digest := fmt.Sprintf("%x", sha256.Sum256(data))
			want := map[bool]string{true: "c10fc6597e76f5bc3f0298146a20dfc62e94cd6742f1eb10cc593beec95b9f61", false: "64f5571bc125dc317237aa5b4e477dbfa3646573bb59b9fba02f899bd9ed0b09"}[retained]
			if fixture.grouped {
				want = map[bool]string{true: "60e8a7095f2b2d4dceec8d575911b78f52256973c4a15f99817239512230b813", false: "7076ee26b7146d96beb0080f99f3748b16d4686251eb4551fee56bcc2d1fc285"}[retained]
			}
			if digest != want {
				t.Fatalf("placement digest %s (%d decisions), want %s", digest, len(decisions), want)
			}
			var placements []decision
			plan := q.assignmentPlan(scan)
			for _, m := range scan.machines {
				if j := plan[targetKey(m)]; j != nil {
					placements = append(placements, decision{targetKey(m), j.ID, j.Spec.Needs, j.MemoryEnvelope})
				}
			}
			data, err = json.Marshal(placements)
			if err != nil {
				t.Fatal(err)
			}
			want = "fbbc3c4d7b78a6932febae1378a5697cfdf9b216a5543d38cf0937b0c8fcf311"
			if fixture.grouped {
				want = "20e76e155167a57f7c12f38ce2f3e0de549c80530f8b85772bf5145f702de9d6"
			}
			if digest = fmt.Sprintf("%x", sha256.Sum256(data)); digest != want {
				t.Fatalf("assignment digest %s (%d placements), want %s", digest, len(placements), want)
			}
		})
	}
}
