package myce

import (
	"reflect"
	"testing"
)

// Unkeyed first-run estimates still depend on the target OS's similar work.
// No cache entry may turn one platform's footprint into another's bound.
func TestColdMemoryPlanningSeparatesTargetOS(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(1, 0)
	s.Needs.Platform = Platform{Required: []string{"cpu"}}
	s.Needs.Memory = "auto"
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	j := &Job{ID: "cold", Spec: s, State: "queued"}
	family := memoryFamily(j)
	q.s.WorkObservations = map[string]WorkObservation{}
	for i, os := range []string{"linux", "darwin"} {
		peak := float64(2 + i*2)
		q.s.WorkObservations[os] = WorkObservation{Key: ObservationKey{Component: "cpu", Target: ExecutionTarget{OS: os}}, MemoryFamily: family, MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}
	}
	q.memoryPlanning = newMemoryPlanningCache()
	for i, os := range []string{"linux", "darwin"} {
		m := cpuMachine("worker", "host", "cpu", 8, 64)
		m.CapabilityProfile.OS = os
		m.EffectiveHostMemoryBudgetGB = 64
		host, _, _, ok := q.coldMemoryBound(j, &m, memoryEstimate{}, 0)
		want := float64(2+i*2) * similarMemoryFactor
		if !ok || host != want {
			t.Fatalf("%s first-run bound = %v, %v; want %v", os, host, ok, want)
		}
	}
}

// Permanent qualification borrows inventory but cannot alter live availability
// or disk capacity while stripping temporary admission state.
func TestQualificationCapacityDoesNotChangeInventory(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.CapabilityProfile.Availability = &Availability{Available: false, Reason: "owner busy"}
	before := cloneMachine(&m)
	projected := q.qualificationCapacity(&m)
	if !projected.CapabilityProfile.Availability.Available {
		t.Fatal("capacity kept temporary availability")
	}
	if !reflect.DeepEqual(before, cloneMachine(&m)) {
		t.Fatal("qualification changed live inventory")
	}
}
