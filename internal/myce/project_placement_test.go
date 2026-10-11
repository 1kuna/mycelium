package myce

import (
	"strings"
	"testing"
)

func TestProjectMachineAllowList(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"spark"}}}); e != nil {
		t.Fatal(e)
	}
	a, b := machine("b70", "fake"), machine("spark", "fake")
	check(t, q, a, nil)
	check(t, q, b, nil)
	j := submit(t, q, spec("production"))
	if check(t, q, a, nil).Job != nil {
		t.Fatal("project escaped allow-list")
	}
	if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("eligible machine not used")
	}
	s := spec("experiment")
	s.AllowedMachines = []string{"b70"}
	if _, e := q.Submit(s); e == nil || !strings.Contains(e.Error(), "project test allowed_machines has no overlap") {
		t.Fatalf("clear rejection: %v", e)
	}
	// Offline workers can return: lack of current inventory is not impossibility.
	s.AllowedMachines = []string{"spark"}
	if _, e := q.Submit(s); e != nil {
		t.Fatal(e)
	}
}

func TestProjectAllowListRejectsKnownIncompatibleFleet(t *testing.T) {
	q, _ := fixture(t)
	check(t, q, machine("spark", "cuda"), nil)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"spark"}}}); e != nil {
		t.Fatal(e)
	}
	s := spec("production")
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	if _, e := q.Submit(s); e == nil || !strings.Contains(e.Error(), "no compatible machine in project test allowed_machines") {
		t.Fatalf("impossible submission %v", e)
	}
}
