package myce

import (
	"context"
	"testing"
)

func TestWorkerProbesMissingRSSLeavesMemoryUnknown(t *testing.T) {
	old := workerMemoryRSS
	defer func() { workerMemoryRSS = old }()
	workerMemoryRSS = nil
	w := Worker{}
	m := Machine{CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}
	if o := w.hostMemoryObservation(context.Background(), m, 8, nil); o != nil {
		t.Fatal("missing RSS became external-memory evidence", o)
	}
}
