package myce

import (
	"errors"
	"strconv"
	"testing"
)

// #1299: b70 drained on memory pressure while a production stage prepared;
// the drain arrived after admission and refused its main with 20+ GiB free.
// An assigned production attempt now launches through a memory-only drain
// raised after admission, unless headroom is really short; other hygiene
// faults and lower tiers keep the refusal, and placement still refuses.
func TestAssignedProductionLaunchThroughLateMemoryDrain(t *testing.T) {
	for _, tc := range []struct {
		name, priority, drain string
		available             float64
		allowed               bool
	}{
		{"memory-drain-plentiful", "production", "host_memory_pressure", 20, true},
		{"swap-drain-plentiful", "production", "host_swap_growth", 20, true},
		{"memory-drain-below-floor", "production", "host_memory_pressure", .1, false},
		{"memory-drain-unmeasured", "production", "host_memory_pressure", 0, false},
		{"disk-drain", "production", "host_disk_pressure", 20, false},
		{"experiment", "experiment", "host_memory_pressure", 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "fake")
			submit(t, q, spec(tc.priority))
			a := check(t, q, m, nil).Job
			if a == nil {
				t.Fatal("not assigned before the drain")
			}
			if err := q.update(func() error {
				stored := q.touchMachine(m.ID)
				stored.HostHealth = &HostHealth{DrainReason: tc.drain}
				stored.HostMemoryMeasured, stored.HostAvailableMemoryGB = tc.available > 0, tc.available
				if q.machineCapacityReason(stored) != tc.drain {
					t.Fatal("placement no longer refuses the drained host")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)})
			if tc.allowed != (err == nil) || !tc.allowed && !errors.Is(err, ErrStale) {
				t.Fatalf("launch err=%v, want allowed=%t", err, tc.allowed)
			}
			startErr := q.Started(result(a, false))
			if tc.allowed != (startErr == nil) {
				t.Fatalf("Started err=%v, want allowed=%t", startErr, tc.allowed)
			}
			if !tc.allowed {
				if got := recordedStartRefusal(q.s.Jobs[a.ID]); got != "host_launch" {
					t.Fatalf("refusal not recorded as host_launch: %q", got)
				}
				return
			}
			finish(t, q, a, true)
			if got := job(t, q, a.ID); got.State != "succeeded" {
				t.Fatalf("assigned attempt did not complete: %s %q", got.State, got.Reason)
			}
		})
	}
}
