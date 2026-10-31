package myce

import (
	"testing"
	"time"
)

// A control update immediately after capture must not alter the generation
// being projected: shared nested memory facts used to be detached by JSON.
func TestObservationCopyDetachedState(t *testing.T) {
	q, clock := fixture(t)
	m := machine("contact", "fake")
	m.Leaving = true
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	original := q.s.Jobs[j.ID]
	original.Spec.Run.Env = map[string]string{"INPUT": "before"}
	hostGB := 3.0
	q.s.WorkObservations = map[string]WorkObservation{"memory": {MemoryPeak: &AttemptMemoryPeak{HostGB: &hostGB}}}
	q.s.ExternalMemory = map[string]*externalMemoryRing{m.ID: {Buckets: map[int]*externalMemoryBucket{0: {Counts: map[uint8]uint32{1: 7}}}}}
	q.models.Models = []Model{{Name: "model", Env: map[string]string{"INPUT": "before"}, fleetToken: "worker-only"}}
	q.environments.Environments = map[string]map[string]EnvironmentRecipe{"env": {"fake": {Files: map[string][]byte{"input": []byte("before")}}}}
	q.calls = map[string]*ModelCall{"call": {ID: "call", Workload: &Workload{Units: 2}}}
	capturedAt := clock.Now()
	q.observeRead = func(_ string, _, _ time.Duration) {
		// This callback runs before detached projection. A real heartbeat commits
		// here, proving capture no longer owns control authority or the writer lock.
		clock.Advance(time.Second)
		if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		original.Spec.Run.Env["INPUT"] = "after"
		*q.s.WorkObservations["memory"].MemoryPeak.HostGB = 9
		q.s.ExternalMemory[m.ID].Buckets[0].Counts[1] = 99
		q.models.Models[0].Env["INPUT"] = "after"
		q.environments.Environments["env"]["fake"].Files["input"][0] = 'X'
		q.calls["call"].Workload.Units = 9
	}
	view, _, err := q.captureObservation("")
	if err != nil {
		t.Fatal(err)
	}
	if !view.clock.Now().Equal(capturedAt) || !view.s.Machines[m.ID].LastSeen.Equal(capturedAt) {
		t.Fatal("heartbeat changed the captured clock/contact")
	}
	if view.s.Jobs[j.ID].Spec.Run.Env["INPUT"] != "before" || *view.s.WorkObservations["memory"].MemoryPeak.HostGB != 3 || view.s.ExternalMemory[m.ID].Buckets[0].Counts[1] != 7 {
		t.Fatal("live mutation changed captured job or memory facts")
	}
	if view.models.Models[0].Env["INPUT"] != "before" || string(view.environments.Environments["env"]["fake"].Files["input"]) != "before" || view.calls["call"].Workload.Units != 2 {
		t.Fatal("live mutation changed captured configuration/call")
	}
	if view.models.Models[0].fleetToken != "" {
		t.Fatal("worker-only state escaped into observation")
	}
}
