package myce

import (
	"context"
	"testing"
	"time"
)

func TestExternalMemoryPersistsAcrossQueueRestart(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
		dir := t.TempDir()
		q, err := Open(dir+"/queue.db", dir+"/checkpoints", clock)
		if err != nil {
			t.Fatal(err)
		}
		m := machine("host", "fake")
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 64, UsedGB: 10, MyceliumGB: 2}
		if _, err = q.Checkin(Checkin{Machine: m}); err != nil {
			q.Close()
			t.Fatal(err)
		}
		if err = q.Close(); err != nil {
			t.Fatal(err)
		}
		q, err = Open(dir+"/queue.db", dir+"/checkpoints", clock)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		s, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Machines) != 1 || s.Machines[0].ExternalMemory == nil || s.Machines[0].ExternalMemory.Week.MaxGB != 8 {
			t.Fatal("history did not survive restart", s)
		}

	})

	t.Run("ExternalMemoryProbeRetainsPressureBeforeRecovery", func(t *testing.T) {
		old := workerMemoryRSS
		defer func() { workerMemoryRSS = old }()
		workerMemoryRSS = func(context.Context) (float64, error) { return 1, nil }
		available := 1.0
		w := Worker{HostMemoryReserveGB: 2, hostRawMemorySource: func(context.Context) (float64, error) { return available, nil }}
		clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
		q := &Queue{clock: clock}
		m := Machine{ID: "host", CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}
		for _, v := range []float64{1, 8} {
			available = v
			admission, raw, err := w.sampleHostMemory(context.Background())
			if admission != max(0, v-2) || raw != v || err != nil {
				t.Fatal(admission, raw, err)
			}
			m.MemoryObservation = w.hostMemoryObservation(context.Background(), m, raw, err)
			q.recordExternalMemory(&m)
			clock.Advance(time.Second)
		}
		h := q.externalMemoryStatus("host")
		if h.Week.Samples != 2 || h.Week.MaxGB != 14 {
			t.Fatal("pressure peak omitted", h)
		}
		s, err := SuggestHostBudget(16, 4, 0, h)
		if err != nil || s.SuggestedGB != 0 {
			t.Fatal("pressure peak did not limit suggestion", s, err)
		}

	})

	t.Run("ExternalMemoryBucketingRetentionAndSuggestion", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
		q := &Queue{clock: clock}
		m := Machine{ID: "worker", HostID: "host", MemoryObservation: &HostMemoryObservation{TotalGB: 24, UsedGB: 12, MyceliumGB: 2}}
		q.recordExternalMemory(&m)
		for range 2 {
			clock.now = clock.now.Add(time.Second)
			m.MemoryObservation.UsedGB = 6
			q.recordExternalMemory(&m)
		}
		h := q.externalMemoryStatus("host")
		suggestion, err := SuggestHostBudget(24, 4, 2, h)
		if err != nil || suggestion.SuggestedGB != 6 || !suggestion.DryRun {
			t.Fatalf("suggestion %+v %v", suggestion, err)
		}
		clock.now = clock.now.Add(8 * 24 * time.Hour)
		h = q.externalMemoryStatus("host")
		if h.Week.Summary() != "unknown (no samples)" {
			t.Fatal("missing samples displayed as measured", h)
		}
		if _, err = SuggestHostBudget(24, 4, 0, h); err == nil {
			t.Fatal("suggested without evidence")
		}

	})

	t.Run("ExternalMemorySuggestionGuardAndClamp", func(t *testing.T) {
		h := &ExternalMemoryHistory{Week: ExternalMemoryWindow{Samples: 1, MaxGB: 20}}
		s, err := SuggestHostBudget(32, 4, 1, h)
		if err != nil || s.HeadroomGB != 4 || s.SuggestedGB != 7 {
			t.Fatal(s, err)
		}
		s, err = SuggestHostBudget(16, 4, 0, h)
		if err != nil || s.SuggestedGB != 0 {
			t.Fatal(s, err)
		}
	})
}
