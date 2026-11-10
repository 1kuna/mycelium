package myce

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// Calibration is shared by ordinary and permanent-capacity readers, but new
// observations, rollback and future evidence must change the very next result.
func TestMemoryTransferDerivationFences(t *testing.T) {
	q, clock := fixture(t)
	q.memoryPlanning = newMemoryPlanningCache()
	var from, to memoryTransferTarget
	for i, work := range []string{"one", "two"} {
		for _, host := range []string{"source", "target"} {
			peak := 1.0
			if host == "target" {
				peak = 2
			}
			key := ObservationKey{Work: work, Host: host, Component: "cpu", Fingerprint: "identity", Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}}
			q.putWorkObservation(host+work, WorkObservation{Key: key, MemoryConfiguration: "config", MemoryLayout: "separate", MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}})
			if i == 0 {
				target := transferTarget(key, "separate")
				if host == "source" {
					from = target
				} else {
					to = target
				}
			}
		}
	}
	check := func(known bool) float64 {
		t.Helper()
		got, ok := q.transferMemoryFactor(from, to, false)
		want, wantOK := q.transferMemoryFactorUncached(from, to, false)
		if ok != known || ok != wantOK || got != want {
			t.Fatalf("cached factor %v/%v; uncached %v/%v; known want %v", got, ok, want, wantOK, known)
		}
		view := Queue{clock: q.clock, s: q.s, memoryOrigin: q, memoryPlanning: newMemoryPlanningCache()}
		other, otherOK := view.transferMemoryFactor(from, to, false)
		if other != want || otherOK != wantOK {
			t.Fatal("capacity view changed transfer calibration")
		}
		return got
	}
	original := check(true)
	rollback := errors.New("rollback")
	err := q.update(func() error {
		point := q.s.WorkObservations["targetone"]
		peak := 12.0
		point.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		q.putWorkObservation("targetone", point)
		if check(true) == original {
			t.Fatal("new observation reused old calibration")
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if check(true) != original {
		t.Fatal("rolled-back calibration survived")
	}
	for _, work := range []string{"one", "two"} {
		point := q.s.WorkObservations["target"+work]
		point.MemoryRecordedAt = clock.Now().Add(time.Second)
		q.putWorkObservation("target"+work, point)
	}
	check(false)
	clock.now = clock.now.Add(time.Second)
	check(true)
	clock.now = clock.now.Add(-time.Second)
	check(false)
}

// Hash every recipe input through the projected snapshot and the complete job.
// Both explicit profile identities and input-derived identities must agree.
func TestCheckinMemoryIdentityProjectionEquivalence(t *testing.T) {
	q, _ := candidateCostFixture(t)
	drainAdmissionWait(t, q)
	i := 0
	for _, j := range q.liveJobs() {
		if j.State != "queued" {
			continue
		}
		j.Spec.Run.Env = map[string]string{"INPUT": "recipe"}
		j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"echo", "helper"}, Env: map[string]string{"HELPER": "recipe"}}}
		j.Spec.Environment = Environment{Name: "env", Platforms: []string{"linux-64"}}
		j.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": {Key: "frozen-env"}}
		if i%2 == 0 {
			j.Spec.ProfileMemory = "frozen-recipe"
		}
		i++
	}
	snapshot := q.snapshotCheckinMemoryIdentities(t.Context())
	if len(snapshot.values) == 0 {
		t.Fatal("missing recipes")
	}
	for j, identity := range snapshot.values {
		if identity != profileMemoryIdentity(j) {
			t.Fatalf("snapshot changed recipe identity for %s", j.ID)
		}
	}
	q.fileReferenceRevision++
	if snapshot.current(q) != nil {
		t.Fatal("revision fence accepted changed recipe")
	}
}

// Compare reusable reductions against a genuinely new catalog for each read,
// including the permanent-capacity views used by health and waiting reasons.
func TestMemorySizingDerivationMatchesFreshPass(t *testing.T) {
	q, _ := candidateCostFixture(t)
	drainAdmissionWait(t, q)
	q.memoryPlanning = newMemoryPlanningCache()
	for _, j := range q.liveJobs() {
		if j.State != "queued" {
			continue
		}
		q.evaluateTargets(j, func(_ targetFit, candidate *Job, m *Machine) bool {
			view := Queue{clock: q.clock, s: q.s, memoryOrigin: q, memoryPlanning: newMemoryPlanningCache()}
			view.s.Jobs, view.s.ModelSlots, view.s.HostReleases = nil, nil, nil
			view.s.Machines = map[string]*Machine{m.ID: m}
			first := view.memoryEstimate(candidate, m)
			q.memoryObservationIndex = nil
			q.memoryPlanning.observations = nil
			next := view.memoryEstimate(candidate, m)
			if !reflect.DeepEqual(first, next) {
				t.Fatal("reuse changed sizing")
			}
			return true
		})
	}
}
