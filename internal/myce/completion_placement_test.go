package myce

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Unknown completion estimates leave value/identity and legacy model routing intact.
// Equal-value commands use stable target identity; legacy servers retain ready-first
// routing and zero-as-unlimited, even with unrelated extension observations.
func TestCompletionUnknownPlacementUnchanged(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	other := m
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	e := q.s.Extensions[s.Extension]
	r := newExtensionReport(e, &other, s.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "other-check"
	other.ExtensionReports = []ExtensionReport{r}
	check(t, q, other, nil)
	q.s.WorkObservations = map[string]WorkObservation{"unrelated": {LastSeconds: 1, Samples: 1, RecordedAt: clock.Now()}}
	command := submit(t, q, s)
	run := check(t, q, other, nil).Job
	if run == nil || run.ID != command.ID {
		t.Fatal("equal-value command did not use stable identity", run)
	}
	models, _, warm := modelFixture(t)
	first := addModelCall(t, models, "experiment")
	reportModel(t, models, warm, "ready")
	cold := warm
	cold.ID, cold.Session = "two", "two-session"
	check(t, models, cold, nil)
	second := addModelCall(t, models, "experiment")
	decision := struct {
		Commands []string `json:"commands"`
		Models   []string `json:"models"`
		InFlight int      `json:"in_flight"`
	}{[]string{run.Machine}, []string{first.Machine, second.Machine}, models.s.ModelSlots[slotKey(warm.ID, "test")].InFlight}
	got, err := json.Marshal(decision)
	want := []byte(`{"commands":["other"],"models":["one","one"],"in_flight":2}`)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("legacy decision changed: %s, want %s (%v)", got, want, err)
	}
}

func completionFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, true)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target.Platform = "cpu"
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	warm := cpuMachine("warm", "warm-host", "cpu", 8, 32)
	warm.CapabilityProfile.OS, warm.CapabilityProfile.Arch = v.Target.OS, v.Target.Arch
	warm.ExtensionFingerprint = "cpu-v1"
	warm.EnvironmentBuilder = true
	cold := cloneMachine(&warm)
	cold.ID, cold.HostID, cold.Session = "cold", "cold-host", "cold-session"
	s := Spec{Name: "count", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	for _, m := range []*Machine{&warm, &cold} {
		r := newExtensionReport(e, m, s.workType())
		r.Setup, r.Qualified, r.CheckID = true, true, "check-"+m.ID
		m.ExtensionReports = []ExtensionReport{r}
		recipe, ok := extensionRecipe(e, m)
		if !ok {
			t.Fatal("missing environment")
		}
		m.EnvironmentBuildSeconds = map[string]float64{recipe.Key: 10}
		if m.ID == "warm" {
			m.Environments = []string{recipe.Key}
		}
		check(t, q, *m, nil)
	}
	return q, clock, warm, cold, s
}

func seedCompletion(t *testing.T, q *Queue, j *Job, m *Machine, seconds float64) {
	t.Helper()
	copy := *extensionJobs(j, m, true)[0]
	copy.Machine, copy.Session = m.ID, m.Session
	key, why := q.workObservationKey(&copy)
	if key == nil {
		t.Fatal(why)
	}
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, LastSeconds: seconds, Samples: 2, RecordedAt: q.clock.Now()}
}

// Placement learns from its own accepted work (MY-31): the target whose last
// accepted run of this kind of work was shorter wins, and status says why.
func TestCompletionLastDurationPlacementAndStatus(t *testing.T) {
	q, clock, warm, cold, s := completionFixture(t)
	j := submit(t, q, s)
	wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
	seedCompletion(t, q, q.s.Jobs[j.ID], wm, 10)
	seedCompletion(t, q, q.s.Jobs[j.ID], cm, 1)
	clock.Advance(time.Second)
	if better, known := q.completionBetter(q.s.Jobs[j.ID], cm, wm); !known || !better {
		t.Fatal("shorter last accepted duration did not win", better, known)
	}
	if got := check(t, q, warm, nil).Job; got != nil {
		t.Fatal("slower target stole work")
	}
	// Existing preparation finishes; the next pull can admit it.
	cold.Environments = warm.Environments
	run := check(t, q, cold, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("chosen worker did not receive work", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	finish(t, q, run, true)
	got := job(t, q, j.ID)
	if got.Timing == nil || !strings.Contains(got.Timing.PlacementReason, "last accepted duration 1.00s on this target") {
		t.Fatal("missing chosen reason", got.Timing)
	}
	var html bytes.Buffer
	snap, err := q.publicSnapshot()
	if err != nil || statusPage.Execute(&html, snap) != nil || !strings.Contains(html.String(), "last accepted duration 1.00s on this target") {
		t.Fatal("HTML hid reason", err)
	}
}

func TestCompletionExactEvidenceAndReleaseCosts(t *testing.T) {
	q, _, warm, cold, s := completionFixture(t)
	j := submit(t, q, s)
	live := q.s.Jobs[j.ID]
	wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
	seedCompletion(t, q, live, wm, 10)
	seedCompletion(t, q, live, cm, 1)
	live.Spec.Resumable, live.Attempt = true, 1
	if _, ok := q.estimateCompletion(live, wm); ok {
		t.Fatal("first queued retry reused full-work duration")
	}
	live.Spec.Resumable, live.Attempt = false, 0
	// Invalidated and forbidden records never rank placement.
	for id, p := range q.s.WorkObservations {
		p.StaleReason = "driver or backend fingerprint changed"
		q.s.WorkObservations[id] = p
	}
	if _, ok := q.completionBetter(live, cm, wm); ok {
		t.Fatal("stale durations ranked")
	}
	seedCompletion(t, q, live, wm, 10)
	seedCompletion(t, q, live, cm, 1)
	for id, p := range q.s.WorkObservations {
		if p.Key.Host == cm.HostID {
			p.Forbidden = true
			q.s.WorkObservations[id] = p
		}
	}
	if _, ok := q.estimateCompletion(live, cm); ok {
		t.Fatal("OOM record reused")
	}
	seedCompletion(t, q, live, cm, 1)
	// A duration cannot override existing useful-victim disruption ordering.
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1}, &placementPlan{machine: wm}) {
		t.Fatal("speed authorized extra victim")
	}
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1, cost: 1}, &placementPlan{machine: wm, stops: 1}) {
		t.Fatal("speed overrode lost work")
	}
	model, _, m := modelFixture(t)
	if _, ok := model.estimateCompletion(callJob(&ModelCall{Model: "test"}), &m); ok {
		t.Fatal("unqualified model invented a duration")
	}
}
