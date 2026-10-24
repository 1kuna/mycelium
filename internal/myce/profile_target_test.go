package myce

import (
	"errors"
	"testing"
	"time"
)

func TestProfileTargetQualificationAndReplay(t *testing.T) {
	q, clock := fixture(t)
	s := Spec{Name: "profile", Project: "test", Class: "experiment", SubmissionKey: "profile-key", RequireTarget: true,
		Targets: []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}},
		Run:     RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 2, MemoryGB: 1}}
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" {
		t.Fatalf("missing target: %v", err)
	}
	m := machine("worker", "cpu")
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 32
	check(t, q, m, nil)
	first := submit(t, q, s)
	running := check(t, q, m, nil).Job
	if running == nil || running.ID != first.ID {
		t.Fatal("not assigned", running)
	}
	// Occupancy must not turn normal queueing into no_qualified_target.
	s.SubmissionKey = "second-key"
	second := submit(t, q, s)
	if second.State != "queued" {
		t.Fatal("expected queued", second.State)
	}
	// A same-ID worker changing OS cannot execute this profile.
	m.CapabilityProfile.OS = "darwin"
	if executionTargetMatches(s, &m) {
		t.Fatal("wrong OS matched")
	}
	if fitsResources(second, &m, 64) {
		t.Fatal("wrong OS passed dispatch/launch predicate")
	}
	clock.Advance(time.Minute + time.Second)
	s.SubmissionKey = "profile-key"
	adopted, err := q.Submit(s)
	if err != nil || adopted.ID != first.ID {
		t.Fatal("key replay requires no current qualification", adopted, err)
	}
}

func TestProfileTargetTemporaryHoldsStillQueue(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	m.TotalMemoryGB = 64
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 32
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 16, 0.1
	check(t, q, m, nil)
	if err := q.update(func() error {
		q.s.Policies = map[string]ProjectPolicy{"test": {AllowedMachines: []string{m.ID}}}
		live := q.s.Machines[m.ID]
		live.Gone, live.Leaving, live.Recovering = true, true, true
		live.CPUCapacityReason = "CPU allowance awaiting worker acknowledgement"
		live.CapabilityProfile.Availability = &Availability{Available: false, Reason: "owner active"}
		policy := q.s.HostMaintenance[hostID(live)]
		policy.Mode = "disabled"
		budget := 4
		policy.CPUBudgetCores = &budget
		q.s.HostMaintenance[hostID(live)] = policy
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "profile", Project: "test", Class: "experiment", RequireTarget: true, Targets: []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}, Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1}}
	for _, mode := range []string{"", "auto"} {
		s.Needs.Memory = mode
		if mode == "auto" {
			s.Needs.MemoryGB = 0
		}
		j := submit(t, q, s)
		if j.State != "queued" {
			t.Fatal("temporary hold did not queue", j)
		}
	}
	s.Needs.CPUCores = 8 // Physical capacity fits, but the persistent allowance does not.
	if _, err := q.Submit(s); err == nil {
		t.Fatal("permanent capacity mismatch accepted")
	}
	if q.s.Machines[m.ID].CapabilityProfile.Availability.Available {
		t.Fatal("qualification modified live owner availability")
	}
}
