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
