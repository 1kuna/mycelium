package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProfileTargetRefusalHostMemorySlack(t *testing.T) {
	q, _ := fixture(t)
	for _, id := range []string{"small", "closest", "also-close"} {
		m := machine(id, "cpu")
		m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
		m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 8, .125
		if id == "small" {
			m.HostMemoryBudgetGB = 4
		}
		check(t, q, m, nil)
	}
	server := httptest.NewServer(API{Queue: q, Token: "fixture-secret"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture-secret"}
	s := Spec{Name: "profile", Project: "test", Class: "experiment", RequireTarget: true,
		Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 2, MemoryGB: 8}}
	// Equal capacity ties use machine name, independently of map iteration.
	for range 10 {
		_, err := c.Submit(context.Background(), s)
		var api *HTTPError
		want := "host memory need 8.125 GiB (8 + 0.125 slack) exceeds allowance 8 GiB on also-close"
		if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, want) {
			t.Fatalf("refusal = %v; want %q", err, want)
		}
	}
}

func TestProfileTargetRefusalPlatform(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "cpu")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	check(t, q, m, nil)
	s := Spec{Name: "profile", Project: "test", Class: "experiment", RequireTarget: true,
		Targets: []ExecutionTarget{{OS: "darwin", Arch: "arm64", Platform: "cpu"}},
		Run:     RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 2, MemoryGB: 1}}
	_, err := q.Submit(s)
	var api *HTTPError
	want := "target linux/amd64/cpu does not match required darwin/arm64/cpu on worker"
	if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, want) {
		t.Fatalf("refusal = %v; want %q", err, want)
	}
}

func TestProfileTargetRefusalAutoEnvelope(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	m.UnifiedMemory = true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.TotalMemoryGB = 8
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 8, .125
	check(t, q, m, nil)
	s := Spec{Name: "profile", Project: "test", Class: "experiment", RequireTarget: true,
		Run:   RunSpec{Command: []string{"true"}, MemoryGB: 8},
		Needs: ResourceNeeds{Memory: "auto", Platform: Platform{Any: true}}}
	_, err := q.Submit(s)
	var api *HTTPError
	want := "memory envelope unavailable within target allowance: exploratory first run needs 8.000 GiB host member memory; 7.750 GiB shared headroom available on worker"
	if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, want) {
		t.Fatalf("refusal = %v; want %q", err, want)
	}
}

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
