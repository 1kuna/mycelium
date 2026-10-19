package myce

import (
	"testing"
)

func TestMaintenanceFencesBatchModelsAndPreparation(t *testing.T) {
	q, _, m := syncFixture(t)
	if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
		t.Fatal(err)
	}
	m.Leaving = true
	m.EnvironmentBuilder = true
	if err := q.SyncModel(SyncRequest{Model: "a", To: m.ID}); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Model: "a", Machine: m.ID, Project: "test"}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	reply := check(t, q, m, nil)
	if !reply.AdmissionBlocked || reply.Job != nil || len(reply.ModelPlan) != 0 || len(reply.CopyPlan) != 0 || len(reply.EnvironmentPlan) != 0 {
		t.Fatalf("held admission: %+v", reply)
	}
	requireState(t, q, j.ID, "queued")
	if q.placementAllowed(job(t, q, j.ID), &m) {
		t.Fatal("held worker eligible for placement")
	}
	if err := setMaintenance(t, q, hostID(&m), "resume"); err != nil {
		t.Fatal(err)
	}
	m.Leaving = false
	q.s.Machines[m.ID].Leaving = false
	reply = check(t, q, m, nil)
	if reply.AdmissionBlocked {
		t.Fatal("hold became sticky")
	}
}
