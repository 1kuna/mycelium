package myce

import (
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
		q.s.WorkObservations[fmt.Sprint(i)] = WorkObservation{Key: *key, MemoryFamily: memoryFamily(source), MemoryConfiguration: memoryConfiguration(provenance.Spec.Needs, provenance.Spec.Args, provenance.Spec.Program), MemoryLayout: "separate", MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}, MemorySamples: 2, Samples: 2, RecordedAt: clock.Now(), Rate: 10, Units: 10, Seconds: 1}
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

// Freeze the original planner's full per-target candidate sequence, resolved
// resource vectors and memory envelopes, not just its first winning job.
// #806 intentionally changes these envelopes: physical cold headroom, measured
// local uncertainty and uncalibrated transfers replace universal multipliers.
// The refreshed digests retain all 402 decisions for each history shape.
// ProbeFloorGB adds metadata in 180 retained-history envelopes; removing only
// that field reproduces the prior digest with identical decisions/resources.
// #1017 adds LaunchDriftGB/AvailableHostGB in those same 180 envelopes. Clearing
// only those fields reproduces e7757cb9 with all 402 decisions/resources intact.
