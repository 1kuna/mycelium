package myce

import (
	"testing"
)

func portabilityRow(t *testing.T, r PortabilityReport, target ExecutionTarget) PortabilityTarget {
	t.Helper()
	for _, row := range r.Targets {
		if row.Target == target {
			return row
		}
	}
	t.Fatalf("missing target %+v in %+v", target, r)
	return PortabilityTarget{}
}

// The real dev API can observe mixed synthetic check-ins on a GPU-free laptop.
// Recovering prevents these metadata-only fixtures from accepting payloads.
