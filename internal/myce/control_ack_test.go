package myce

import (
	"testing"
)

func TestClosedHostFencesBatchModelsAndPreparation(t *testing.T) {
	q, _, m := syncFixture(t)
	if err := closeQueueHost(q, hostID(&m), "test", nil); err != nil {
		t.Fatal(err)
	}
	m.Disabled = "test"
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
	if !reply.Leave || reply.Job != nil || len(reply.ModelPlan) != 0 || len(reply.CopyPlan) != 0 || len(reply.EnvironmentPlan) != 0 {
		t.Fatalf("held admission: %+v", reply)
	}
	requireState(t, q, j.ID, "queued")
	if q.placementAllowed(job(t, q, j.ID), &m) || modelFitsMachine(q.models.Models[0], &m) {
		t.Fatal("held worker eligible for placement")
	}
	q.s.HostIntents = nil
	m.Disabled = ""
	m.Leaving = false
	q.s.Machines[m.ID].Disabled = ""
	q.s.Machines[m.ID].Leaving = false
	reply = check(t, q, m, nil)
	if reply.AdmissionBlocked {
		t.Fatal("hold became sticky")
	}
}
