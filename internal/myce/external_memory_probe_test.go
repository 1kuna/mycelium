package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExternalMemoryProbeCurrentMemory(t *testing.T) {
	old := workerMemoryRSS
	defer func() { workerMemoryRSS = old }()
	workerMemoryRSS = func(context.Context) (float64, error) { return 3, nil }
	m := Machine{CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}
	w := Worker{HostMemoryReserveGB: 2}
	o := w.hostMemoryObservation(context.Background(), m, 8, nil)
	if o == nil || o.TotalGB != 16 || o.UsedGB != 8 || o.MyceliumGB != 3 {
		t.Fatal(o)
	}
	if o = w.hostMemoryObservation(context.Background(), m, 0, nil); o == nil || o.UsedGB != 16 {
		t.Fatal("zero raw availability lost", o)
	}
	if o = w.hostMemoryObservation(context.Background(), m, 8, errors.New("probe failed")); o != nil {
		t.Fatal("failed host sample accepted", o)
	}
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, errors.New("ps failed") }
	if o = w.hostMemoryObservation(context.Background(), m, 8, nil); o != nil {
		t.Fatal("failed RSS accepted", o)
	}
	rss, err := workerTreeRSS([]byte("1 0 100\n10 1 1048576\n11 10 1048576\n20 1 1048576\n"), 10)
	if err != nil || rss != 2 {
		t.Fatal(rss, err)
	}
}

func TestExternalMemoryProbeRetainsPressureBeforeRecovery(t *testing.T) {
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
}
