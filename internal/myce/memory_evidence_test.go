package myce

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

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
