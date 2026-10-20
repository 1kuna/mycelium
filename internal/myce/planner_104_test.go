package myce

import (
	"slices"
	"testing"
)

func TestPlanner104HostMemoryDebt(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(map[bool]string{false: "host-victims", true: "local-victim"}[local], func(t *testing.T) {
			q, m := releaseTestHost(t, 16, 8, []releaseTestVictim{
				{"a", "experiment", 1, 6, false}, {"b", "experiment", 1, 6, false},
			})
			if local {
				q.s.Jobs["a"].Machine, q.s.Jobs["a"].Session = m.ID, m.Session
			}
			q.refreshHostMemory()
			m = q.s.Machines[m.ID]
			if m.hostReservationDebtGB != 4 {
				t.Fatalf("fixture debt = %v", m.hostReservationDebtGB)
			}
			d := &Job{ID: "demand", State: "queued", Spec: spec("production")}
			d.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 4}
			p := q.roomPlan(d, m, true)
			if p == nil || !slices.Equal(releaseTestIDs(p.jobs, p.slots), []string{"job/a", "job/b"}) {
				t.Fatalf("debt requires both releases; plan = %+v", p)
			}
			for _, j := range p.jobs {
				j.ReleaseConfirmed = true
			}
			q.refreshHostMemory()
			if !hostFits(m, 0, 4) {
				t.Fatal("chosen releases did not fit in the real ledger")
			}
		})
	}
}

func TestPlanner104WinnerKeepsModelCopy(t *testing.T) {
	q, m := releaseTestHost(t, 4, 8, []releaseTestVictim{{"a", "experiment", 1, 8, false}})
	s := spec("experiment")
	q.s.PreparationPlans = map[string]HostLaunch{
		"copy-a": {Machine: m.ID, Session: m.Session, Kind: "copy", Key: "weights", Attempt: "copy-a", Started: true, Spec: &s},
		"copy-b": {Machine: m.ID, Session: m.Session, Kind: "copy", Key: "other", Attempt: "copy-b", Started: true, Spec: &s},
	}
	d := &Job{ID: "winner", State: "queued", Spec: spec("production")}
	d.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 4, Model: "weights"}
	q.refreshHostMemory()
	p := q.roomPlan(d, q.s.Machines[m.ID], true)
	if p == nil || len(p.jobs) != 1 {
		t.Fatal("missing useful host release plan", p)
	}
	q.applyPlacement(p, d)
	copy := q.s.PreparationPlans["copy-a"]
	if copy.PriorityStop != nil || copy.StopAt != nil || copy.Demand != d.ID || copy.Spec == nil || copy.Spec.Priority != "production" {
		t.Fatalf("winner's copy was not preserved/promoted: %+v", copy)
	}
	if q.s.PreparationPlans["copy-b"].PriorityStop == nil {
		t.Fatal("unrelated lower-tier copy was not stopped")
	}
}

func TestPlanner104SharedBuildHandsCoreToDemand(t *testing.T) {
	q, m := releaseTestHost(t, 3, 64, []releaseTestVictim{
		{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false},
	})
	s := spec("experiment")
	q.s.PreparationPlans = map[string]HostLaunch{
		"build": {Machine: m.ID, Session: m.Session, Kind: "environment", Key: "shared", Attempt: "build", Started: true, Spec: &s},
	}
	d := &Job{ID: "winner", State: "queued", Spec: spec("production"), EnvironmentRecipes: map[string]EnvironmentRecipe{m.GPU: {Key: "shared"}}}
	d.Spec.Needs = ResourceNeeds{CPUCores: 2}
	q.refreshHostMemory()
	p := q.roomPlan(d, q.s.Machines[m.ID], true)
	if p == nil || !slices.Equal(releaseTestIDs(p.jobs, p.slots), []string{"job/a"}) {
		t.Fatalf("shared build must hand its core to the winner; plan = %+v", p)
	}
	q.applyPlacement(p, d)
	if q.s.PreparationPlans["build"].PriorityStop != nil {
		t.Fatal("shared build was stopped")
	}
	// Assignment covers the still-running shared build; the live ledger must
	// agree with the projection rather than create a second preparation core.
	p.jobs[0].ReleaseConfirmed = true
	d.Machine, d.Session, d.State = m.ID, m.Session, "running"
	q.s.Jobs[d.ID] = d
	q.refreshHostMemory()
	if q.s.Machines[m.ID].CPUInUseCores != 3 {
		t.Fatal("shared build was double charged")
	}
}
