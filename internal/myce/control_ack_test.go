package myce

import (
	"testing"
)

func TestEnablePendingFencesBatchModelsAndPreparation(t *testing.T) {
	q, _, m := syncFixture(t)
	m.EnablePending = true
	m.EnvironmentBuilder = true
	if err := q.SyncModel(SyncRequest{Model: "a", To: m.ID}); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Model: "a", Machine: m.ID, Project: "test"}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	reply := check(t, q, m, nil)
	if !reply.AdmissionBlocked || reply.Leave || reply.Job != nil || len(reply.ModelPlan) != 0 || len(reply.CopyPlan) != 0 || len(reply.EnvironmentPlan) != 0 {
		t.Fatalf("held admission: %+v", reply)
	}
	requireState(t, q, j.ID, "queued")
	if q.placementAllowed(job(t, q, j.ID), &m) || modelFitsMachine(q.models.Models[0], &m) {
		t.Fatal("held worker eligible for placement")
	}
	m.EnablePending = false
	reply = check(t, q, m, nil)
	if reply.AdmissionBlocked {
		t.Fatal("hold became sticky")
	}
}
