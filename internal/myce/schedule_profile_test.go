package myce

import "testing"

func TestProfileTargetChangedBeforeStart(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "cpu")
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 32
	check(t, q, m, nil)
	s := Spec{Name: "profile", Project: "test", Class: "experiment", Targets: []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}, Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1}}
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("not assigned")
	}
	if !q.startPlacementAllowed(j) {
		t.Fatal("original target not allowed")
	}
	if err := q.update(func() error { q.s.Machines[m.ID].CapabilityProfile.Arch = "arm64"; return nil }); err != nil {
		t.Fatal(err)
	}
	if q.startPlacementAllowed(j) {
		t.Fatal("changed arch passed final launch gate")
	}
	if err := q.Started(result(j, true)); err == nil {
		t.Fatal("Started accepted changed arch")
	}
}
