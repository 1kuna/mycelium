package myce

import (
	"strings"
	"testing"
)

func TestJobMachineExclusionsPlacementAndResolve(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("worker-a", "fake"), machine("worker-b", "fake")
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.ExcludedMachines = []string{a.ID}
	report, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, target := range report.Targets {
		seen[target.Machine] = true
		if target.Machine == a.ID {
			if target.Eligible || !strings.Contains(target.Reason, "excluded_machines") {
				t.Fatalf("excluded target: %+v", target)
			}
		} else if !target.Eligible {
			t.Fatalf("eligible target: %+v", target)
		}
	}
	if !seen[a.ID] || !seen[b.ID] {
		t.Fatalf("missing targets: %+v", report.Targets)
	}
	j := submit(t, q, s)
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatal("excluded machine assigned", got.ID)
	}
	if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("eligible machine not assigned", got)
	}
	// New workers do not require an allow-list update.
	c := machine("worker-c", "fake")
	next := submit(t, q, s)
	if got := check(t, q, c, nil).Job; got == nil || got.ID != next.ID {
		t.Fatal("new machine excluded", got)
	}
}

func TestJobMachineExclusionsValidationAndReasons(t *testing.T) {
	q, _ := fixture(t)
	a := machine("worker-a", "fake")
	check(t, q, a, nil)
	s := spec("experiment")
	s.ExcludedMachines = []string{a.ID}
	j := submit(t, q, s)
	if got := job(t, q, j.ID); !strings.Contains(got.Reason, "excluded machines: "+a.ID) {
		t.Fatal("missing exclusion reason", got.Reason)
	}
	for _, name := range []string{"", " worker-a"} {
		bad := s
		bad.ExcludedMachines = []string{name}
		if _, err := q.Submit(bad); err == nil {
			t.Fatal("invalid exclusion accepted", name)
		}
	}
	s.AllowedMachines = []string{a.ID}
	if _, err := q.Submit(s); err == nil || !strings.Contains(err.Error(), "excluded_machines") {
		t.Fatal("exhausted job allow-list accepted", err)
	}
	s.AllowedMachines = nil
	s.RequireTarget = true
	if _, err := q.Submit(s); err == nil {
		t.Fatal("require_target accepted entirely excluded inventory")
	}
	s.RequireTarget = false
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{a.ID}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Submit(s); err == nil || !strings.Contains(err.Error(), "excluded_machines") {
		t.Fatal("exhausted project allow-list accepted", err)
	}
	report, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil || resolveFinding(report, "excluded_machines") == nil {
		t.Fatal("resolve omitted policy conflict", report, err)
	}
}

func TestJobMachineExclusionsLogCapabilityReason(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("worker-a", "fake"), machine("worker-b", "fake")
	a.Capabilities = append(a.Capabilities, "logs_none")
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Logs = "none"
	s.ExcludedMachines = []string{a.ID}
	j := &Job{Spec: s}
	if got := q.logsWaitingReason(j); !strings.Contains(got, "logs_none") {
		t.Fatal("excluded log-capable host masked waiting reason", got)
	}
}
