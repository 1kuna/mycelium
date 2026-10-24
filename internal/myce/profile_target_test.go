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
