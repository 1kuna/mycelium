package myce

import (
	"database/sql"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestMemoryEvidenceRebuildsAttributedCUDAFromAttempts(t *testing.T) {
	now := time.Date(2026, 11, 25, 17, 0, 0, 0, time.UTC)
	m := sharedUnifiedMachine()
	host, first, gap, next := 15.0, 5.0, 40.0, 9.0
	s := state{Machines: map[string]*Machine{m.ID: &m}, Jobs: map[string]*Job{}}
	spec := sharedSpec("experiment", 10)
	for i, tc := range []struct {
		id      string
		device  *float64
		samples int
	}{
		{"first", &first, 3},
		{"missing counter", nil, 3},
		{"unreported tick", &gap, 2},
		{"repeat", &next, 3},
	} {
		s.Jobs[tc.id] = &Job{ID: tc.id, Spec: spec, State: "succeeded", Machine: m.ID, Session: m.Session,
			AllocatedDevices: []string{"0"}, Attempt: 1,
			AttemptHistory: []AttemptOutcome{{Attempt: 1, Machine: m.ID, Session: m.Session,
				State: "succeeded", EndedAt: now.Add(time.Duration(i) * time.Minute)}},
			MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: m.ID, Session: m.Session, LearningReason: "eligible",
				MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: tc.device,
					DeviceSampling: &DeviceMemorySampling{Complete: true, Samples: tc.samples, ElapsedSeconds: 17}}}},
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 2 {
		e, err := loadMemoryEvidence(db, s)
		if err != nil {
			t.Fatal(err)
		}
		key := pooledEvidenceKey(s.Jobs["first"], allocationMachine(&m, []string{"0"})).attributedRAM()
		if len(e.exact) != 1 || !slices.Equal(e.exact[key].Peaks, []float64{20, 24}) {
			t.Fatalf("attributed evidence includes incomplete or duplicate peaks: %v", e.exact)
		}
		ratios := e.ratios[memoryRatioKey{key.Platform, key.Pool, "exact", "", 1}]
		if !slices.Equal(ratios, []float64{1.2}) {
			t.Fatalf("attributed ratio used an unqualified or later peak: %v", ratios)
		}
	}
}

func TestMemoryEvidenceProjectionCarriesLegacyPeaksAcrossDriverChange(t *testing.T) {
	now := time.Date(2026, 10, 8, 1, 0, 0, 0, time.UTC)
	host, device, limited := 3.0, 2.0, 5.0
	key := ObservationKey{Target: ExecutionTarget{OS: "linux", Platform: "xpu"}, Work: "synthetic-work", Fingerprint: "old-driver"}
	point := WorkObservation{Key: key, MemoryFamily: "family", MemoryConfiguration: "config", MemoryLayout: "separate", MemoryRecordedAt: now,
		StaleReason: "driver or backend fingerprint changed", MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device},
		MemoryEscalation: &MemoryEscalationPrior{RecordedAt: now.Add(time.Hour), Peak: &AttemptMemoryPeak{HostGB: &limited}}}
	diagnostic := point
	diagnostic.MemoryRecordedAt, diagnostic.MemoryPeak = time.Time{}, hostOnlyMemoryPeak(limited)
	workloadLimit := diagnostic
	workloadLimit.MemoryFamily = "limited"
	workloadLimit.MemoryPeak = &AttemptMemoryPeak{HostGB: &limited, Events: &MemoryEventCounts{OOM: 1}}
	s := state{WorkObservations: map[string]WorkObservation{"accepted": point, "diagnostic": diagnostic, "workload-limit": workloadLimit}}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	run := func() *memoryEvidence {
		evidence, err := loadMemoryEvidence(db, s)
		if err != nil {
			t.Fatal(err)
		}
		return evidence
	}
	for _, evidence := range []*memoryEvidence{run(), run()} {
		ram := memoryEvidenceKey{memoryPlatform(key.Target, false), "ram", point.MemoryFamily, memoryEvidenceExact(key, point.MemoryConfiguration)}
		if got := evidence.exact[ram]; len(got.Peaks) != 1 || got.Peaks[0] != host || got.Lower != 0 {
			t.Fatalf("legacy RAM evidence was discarded or projected twice: %+v", got)
		}
		ram.Pool = "card"
		if got := evidence.exact[ram]; len(got.Peaks) != 1 || got.Peaks[0] != device || got.Lower != 0 {
			t.Fatalf("discrete card evidence was mixed with RAM: %+v", got)
		}
		ram.Pool, ram.Family = "ram", workloadLimit.MemoryFamily
		if got := evidence.exact[ram]; len(got.Peaks) != 0 || got.Lower != limited {
			t.Fatalf("workload OOM diagnostic was not imported as a floor: %+v", got)
		}
	}
}

func hostOnlyMemoryPeak(host float64) *AttemptMemoryPeak {
	zero := 0.0
	return &AttemptMemoryPeak{HostGB: &host, Constrained: true, SwapGB: &zero, Events: &MemoryEventCounts{},
		Pressure: &MemoryPressureEvidence{Reason: "host_swap_growth", HostSwapGrowthBytes: 1 << 29}}
}
