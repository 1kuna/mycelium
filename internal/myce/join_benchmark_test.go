package myce

import (
	"testing"
	"time"
)

func TestPlacementWithoutBenchmark(t *testing.T) {
	for _, profile := range []bool{false, true} {
		q, clock := fixture(t)
		m := machine("dedicated", "fake")
		if profile {
			// Hardware facts remain required even when throughput is unmeasured.
			m.CapabilityProfile.LogicalCPUs = 8
		}
		check(t, q, m, nil)
		j := submit(t, q, spec("experiment"))
		clock.Advance(time.Second)
		if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
			t.Fatal("job not placed without benchmark")
		}
	}
}
