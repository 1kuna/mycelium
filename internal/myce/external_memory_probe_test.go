package myce

import (
	"context"
	"errors"
	"testing"
)

func TestExternalMemoryProbeCurrentMemory(t *testing.T) {
	old := workerMemoryRSS
	defer func() { workerMemoryRSS = old }()
	workerMemoryRSS = func(context.Context) (float64, error) { return 3, nil }
	m := Machine{CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}
	w := Worker{HostMemoryReserveGB: 2}
	o := w.hostMemoryObservation(context.Background(), m, 6, nil)
	if o == nil || o.TotalGB != 16 || o.UsedGB != 8 || o.MyceliumGB != 3 {
		t.Fatal(o)
	}
	if o = w.hostMemoryObservation(context.Background(), m, 0, nil); o != nil {
		t.Fatal("reserve-clamped sample accepted", o)
	}
	if o = w.hostMemoryObservation(context.Background(), m, 6, errors.New("probe failed")); o != nil {
		t.Fatal("failed host sample accepted", o)
	}
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, errors.New("ps failed") }
	if o = w.hostMemoryObservation(context.Background(), m, 6, nil); o != nil {
		t.Fatal("failed RSS accepted", o)
	}
	rss, err := workerTreeRSS([]byte("1 0 100\n10 1 1048576\n11 10 1048576\n20 1 1048576\n"), 10)
	if err != nil || rss != 2 {
		t.Fatal(rss, err)
	}
}
