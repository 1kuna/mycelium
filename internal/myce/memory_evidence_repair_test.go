package myce

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestMemoryEvidenceRepairRetainsNativeLimits(t *testing.T) {
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	prior, smaller := 8.0, 5.0
	for _, tc := range []struct {
		name        string
		peak        float64
		oom         bool
		stop        string
		charge      float64
		knownWorker bool
		want        float64
		envelope    *JobMemoryEnvelope
	}{
		{"same peak OOM", prior, true, "", 0, true, prior, nil},
		{"smaller OOM", smaller, true, "", 0, true, smaller, nil},
		{"pressure stop", smaller, false, memoryPressureCause, 4, true, smaller, nil},
		{"bystander stop", smaller, false, memoryPressureCause, 6, true, 0, nil},
		{"unresolved OOM", smaller, true, "", 0, false, prior, nil},
		{"pooled max event", smaller, false, "", 0, true, smaller, &JobMemoryEnvelope{Basis: pooledMemoryCapability}},
		{"legacy explicit max event", smaller, false, "", 0, true, smaller, &JobMemoryEnvelope{
			Basis: "learned", Enforcement: cgroupMemoryEnforcement, LimitHostGB: smaller}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := cpuSpec(1, 0)
			peak := &AttemptMemoryPeak{HostGB: &tc.peak}
			outcomeState := "succeeded"
			if tc.oom {
				outcomeState = "failed"
			}
			if tc.envelope != nil {
				peak.Events = &MemoryEventCounts{Max: 1}
			}
			j := &Job{ID: "job", Spec: spec, Attempt: 2, Copies: []Copy{{Attempt: 1, Machine: m.ID, Session: m.Session,
				StopCause: tc.stop, Memory: &MemoryPools{RAM: memoryPoolEstimate{ChargeGB: tc.charge}}}},
				MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: m.ID, Session: m.Session,
					MemoryPeak: peak, MemoryEnvelope: tc.envelope}},
				AttemptHistory: []AttemptOutcome{{Attempt: 1, Machine: m.ID, Session: m.Session, State: outcomeState, OutOfMemory: tc.oom}}}
			key := pooledEvidenceKey(j, &m)
			p := WorkObservation{Key: ObservationKey{Work: profileMemoryIdentity(j), Target: machineTarget(&m), Concurrency: 1, BatchSize: 1},
				MemoryFamily: key.Family, MemoryConfiguration: memoryConfiguration(spec.Needs, spec.Args, spec.Program),
				MemoryEscalation: &MemoryEscalationPrior{RecordedAt: time.Unix(1, 0), Peak: &AttemptMemoryPeak{HostGB: &prior}}}
			s := state{WorkObservations: map[string]WorkObservation{"legacy": p}, Jobs: map[string]*Job{j.ID: j}}
			if tc.knownWorker {
				s.Machines = map[string]*Machine{m.ID: &m}
			}
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "queue.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(memoryEvidenceSchema); err != nil {
				t.Fatal(err)
			}
			e := newMemoryEvidence()
			e.exact[key] = memoryEvidenceSeries{Lower: prior}
			got, err := repairMemoryEvidence(db, e, s)
			if err != nil {
				t.Fatal(err)
			}
			if lower := got.exact[key].Lower; lower != tc.want {
				t.Fatalf("retained floor = %g, want %g", lower, tc.want)
			}
		})
	}
}

func TestMemoryEvidenceRepairSeparationOnQueueOpen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "queue.db")
	clock := &fakeClock{now: time.Date(2026, 11, 24, 21, 0, 0, 0, time.UTC)}
	open := func() *Queue {
		t.Helper()
		q, err := Open(dbPath, dir, clock)
		if err != nil {
			t.Fatal(err)
		}
		fakeCheckpointAvailability(t, q, true)
		return q
	}
	q := open()
	m := m3Machine()
	m.GPU, m.CapabilityProfile.OS = "xpu", "linux"
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
	m.CapabilityProfile.Accelerators[0].Type = "xpu"
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 16.5, 16.5
	for i := range m.Devices {
		d := &m.Devices[i]
		d.Platform, d.TotalMemoryGB, d.BudgetGB, d.FreeMemoryGB = "xpu", 16, 16, 16
	}
	spec := spec("experiment")
	spec.Name, spec.Needs.MemoryGB, spec.Needs.Memory = "separation-replay", 0, "auto"
	spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	key := pooledEvidenceKey(&Job{Spec: spec}, allocationMachine(&m, []string{"0"}))
	card := key
	card.Pool = "card"
	priorHost, priorCard := 8.18972396850586, 10.12200927734375
	point := WorkObservation{Key: ObservationKey{Work: profileMemoryIdentity(&Job{Spec: spec}), Concurrency: 1, BatchSize: 1,
		Target: ExecutionTarget{OS: "linux", Platform: "xpu"}}, MemoryFamily: key.Family,
		MemoryConfiguration: memoryConfiguration(spec.Needs, spec.Args, spec.Program),
		MemoryEscalation:    &MemoryEscalationPrior{RecordedAt: clock.Now().Add(-time.Hour), Peak: &AttemptMemoryPeak{CombinedHostGB: &priorHost, DeviceGB: &priorCard}},
		MemoryPeak:          hostOnlyMemoryPeak(7.754799)}
	limit, diagnostic, ambiguous := point, point, point
	limit.MemoryFamily, diagnostic.MemoryFamily, ambiguous.MemoryFamily = "real-oom", "host-only", "counter-unavailable"
	oomPeak, unknownPeak := 6.0, 8.0
	limit.MemoryPeak = &AttemptMemoryPeak{HostGB: &oomPeak, Events: &MemoryEventCounts{OOM: 1}}
	limit.MemoryEscalation, diagnostic.MemoryEscalation, ambiguous.MemoryEscalation = nil, nil, nil
	ambiguous.MemoryPeak = &AttemptMemoryPeak{HostGB: &unknownPeak, Constrained: true}
	legacy := map[string]WorkObservation{"separation": point, "real-oom": limit, "host-only": diagnostic, "counter-unavailable": ambiguous}
	// Older runs hit an automatically generated ceiling on total cgroup
	// usage, including file cache. Some still completed with projected RAM
	// and device peaks greater than the imported floors.
	machineCheck(t, q, m)
	oldRAM, oldCard, ceiling, zero := 9.360404968261719, 10.122013092041016, 10.704219818115234, 0.0
	old := &Job{ID: "legacy-success", Spec: spec, State: "succeeded", Attempt: 1, Machine: m.ID, Session: m.Session,
		AllocatedDevices: []string{"0"},
		MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: m.ID, Session: m.Session,
			MemoryEnvelope: &JobMemoryEnvelope{Basis: "learned", Enforcement: cgroupMemoryEnforcement, HostGB: ceiling},
			MemoryPeak: &AttemptMemoryPeak{CombinedHostGB: &oldRAM, DeviceGB: &oldCard, CgroupGB: &ceiling,
				SwapGB: &zero, Events: &MemoryEventCounts{Max: 1320}, Constrained: true}}},
		AttemptHistory: []AttemptOutcome{{Attempt: 1, Machine: m.ID, Session: m.Session, State: "succeeded"}},
	}
	if err := q.update(func() error {
		for id, p := range legacy {
			q.putWorkObservation(id, p)
		}
		q.putJob(old.ID, old)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e := newMemoryEvidence().stage()
	// Reproduce v1's lossy maximum, including the floor that would remain if
	// only the accepted prior were removed. Seed subsequent pooled history.
	for _, p := range legacy {
		k, _ := legacyMemoryEvidenceKey(p)
		points := appendProjectedMemoryPeaks(nil, k, memoryEvidencePoint{Limited: true}, p.MemoryPeak)
		if p.MemoryEscalation != nil {
			points = appendProjectedMemoryPeaks(points, k, memoryEvidencePoint{Limited: true}, p.MemoryEscalation.Peak)
		}
		for _, observation := range points {
			e.observe(observation)
		}
	}
	peaks := []float64{7.497517, 7.511623, 7.570580, 7.477547, 7.535625, 7.476601, 7.520061, 7.505756, 7.429943, 7.577175,
		7.482315, 7.614784, 7.533760, 7.274387, 7.552650, 7.399387, 7.517384, 7.406250, 7.462635, 7.633610}
	for i, peak := range peaks {
		e.observe(memoryEvidencePoint{Key: key, Peak: peak})
		e.observeFeedback(2*priorHost, peak, false, true)
		if i < 10 {
			// Synthetic card telemetry from other runs for card calibration.
			// The 20 actual host samples had unavailable device telemetry.
			e.observe(memoryEvidencePoint{Key: card, Peak: 9})
		}
	}
	// Calibrate the 20-sample bucket without changing separation's window.
	other := key
	other.Family, other.Exact = "calibration", "calibration-exact"
	for range 30 {
		e.observe(memoryEvidencePoint{Key: other, Peak: 7})
	}
	if got := e.estimate(key, 0, e.confidence()); got.ChargeGB != 2*priorHost {
		t.Fatalf("fixture did not reproduce contaminated charge: %+v", got)
	}
	if got := e.estimate(card, 0, e.confidence()); got.ChargeGB != 2*priorCard {
		t.Fatalf("fixture did not reproduce contaminated card charge: %+v", got)
	}
	tx, err := q.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := e.persist(tx); err != nil {
		t.Fatal(err)
	}
	// v2 already ran and retained the imported floors as verified.
	if _, err := tx.Exec(`DELETE FROM memory_evidence WHERE kind='projection' AND key='v3'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(got *memoryEvidence) {
		t.Helper()
		ram, vram := got.estimate(key, 0, got.confidence()), got.estimate(card, 0, got.confidence())
		if ram.ChargeGB < 7.5 || ram.ChargeGB > 7.7 || ram.Evidence.Runs != len(peaks) || vram.ChargeGB != 9 {
			t.Fatalf("unexpected estimates after repair: RAM %+v, card %+v", ram, vram)
		}
		if !slices.Equal(got.exact[key].Peaks, peaks) || !reflect.DeepEqual(got.family, e.family) ||
			!reflect.DeepEqual(got.ratios, e.ratios) || !reflect.DeepEqual(got.first, e.first) || !slices.Equal(got.feedback, e.feedback) {
			t.Fatal("repair rewrote clean history or calibration")
		}
		// The v2 upgrade only revisits floors supported by the old automatic
		// ceilings. Unrelated floors may have lost their corroborating history.
		for family, lower := range map[string]float64{"real-oom": 6, "host-only": 7.754799, "counter-unavailable": 8} {
			k := key
			k.Family = family
			if got.exact[k].Lower != lower {
				t.Errorf("%s floor = %g, want %g", family, got.exact[k].Lower, lower)
			}
		}
	}
	q = open()
	check(q.memoryEvidence)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = open()
	defer q.Close()
	check(q.memoryEvidence)
	// Device telemetry can still be unavailable at a launch. A second job
	// can take the sibling card while the first retains its own card and RAM.
	machineCheck(t, q, m)
	first := submit(t, q, spec)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		stored := job(t, q, first.ID)
		t.Fatalf("first separation placement = %+v, state=%s reason=%s hold=%+v", a, stored.State, stored.Reason, stored.Hold)
	}
	if a.Memory == nil || a.Memory.RAM.ChargeGB < 7.5 || a.Memory.RAM.ChargeGB > 7.7 || a.Memory.Card == nil || a.Memory.Card.ChargeGB != 9 {
		t.Fatalf("first separation placement charges = %+v", a.Memory)
	}
	second := submit(t, q, spec)
	b := assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || len(a.AllocatedDevices) != 1 || len(b.AllocatedDevices) != 1 || a.AllocatedDevices[0] == b.AllocatedDevices[0] {
		stored := job(t, q, second.ID)
		t.Fatalf("second separation placement = %+v; first devices %v; state=%s reason=%s hold=%+v", b, a.AllocatedDevices, stored.State, stored.Reason, stored.Hold)
	}
}
