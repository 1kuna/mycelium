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
		q.s.WorkObservations[os] = WorkObservation{Key: ObservationKey{Host: "host", Component: "cpu", Target: ExecutionTarget{OS: os}}, MemoryFamily: family, MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}
	}
	q.memoryPlanning = newMemoryPlanningCache()
	for i, os := range []string{"linux", "darwin"} {
		m := cpuMachine("worker", "host", "cpu", 8, 64)
		m.CapabilityProfile.OS = os
		m.EffectiveHostMemoryBudgetGB = 64
		host, _, _, ok := q.coldMemoryBound(j, &m, memoryEstimate{}, 0)
		want := float64(2+i*2) + minimumHostMemoryGB
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

// The incident's retained constrained attempts must give the same lower bounds
// in ordinary admission and permanent-capacity views. A later pass must see
// collection and changed worker identity rather than lending old evidence.
func TestRetainedMemoryPlanningEquivalence(t *testing.T) {
	q, _ := candidateCostFixture(t)
	compare := func(wantEvidence bool) {
		t.Helper()
		planning := newMemoryPlanningCache()
		positive := false
		for _, j := range q.s.Jobs {
			if j.State != "queued" || !j.Spec.Needs.cpuOnly() {
				continue
			}
			probe := *j
			probe.Machine, probe.Session = "worker-1", q.s.Machines["worker-1"].Session
			key, why := q.workObservationKey(&probe)
			if key == nil {
				t.Fatal(why)
			}
			configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
			want := q.sameOSMemoryLowerBoundUncached(*key, configuration)
			positive = positive || want > 0
			view := Queue{clock: q.clock, s: q.s, memoryOrigin: q, memoryPlanning: planning}
			view.s.Jobs = nil
			if got := view.sameOSMemoryLowerBound(*key, configuration); got != want {
				t.Fatalf("%s lower bound %v, want %v", j.ID, got, want)
			}
		}
		if positive != wantEvidence {
			t.Fatalf("retained lower-bound evidence=%v, want %v", positive, wantEvidence)
		}
	}
	compare(true)
	// Exercise the real scopes that consume the index before collecting evidence.
	q.maintain()
	q.detectIncidentHealth()
	for id, j := range q.s.Jobs {
		if j.State == "succeeded" {
			delete(q.s.Jobs, id)
		}
	}
	q.s.Machines["worker-1"].ExtensionFingerprint = "changed-identity"
	compare(false)
}
