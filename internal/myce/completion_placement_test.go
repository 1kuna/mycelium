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
	q.s.WorkObservations = map[string]WorkObservation{"unrelated": {Rate: 1000, Samples: 1, RecordedAt: clock.Now()}}
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

func seedCompletion(t *testing.T, q *Queue, j *Job, m *Machine, rate float64) {
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
	q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, Rate: rate, Units: 10, Seconds: 10 / rate, Samples: 2, RecordedAt: q.clock.Now()}
}

func TestCompletionColdFastWarmSlowAndStatus(t *testing.T) {
	for _, units := range []float64{10, 100} {
		t.Run(map[float64]string{10: "small-warm", 100: "large-cold"}[units], func(t *testing.T) {
			q, clock, warm, cold, s := completionFixture(t)
			s.Workload.Units = units
			j := submit(t, q, s)
			wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
			seedCompletion(t, q, q.s.Jobs[j.ID], wm, 1)
			seedCompletion(t, q, q.s.Jobs[j.ID], cm, 10)
			clock.Advance(time.Second)
			// Small: warm=10, cold=10+1; large: warm=100, cold=10+10.
			better, known := q.completionBetter(q.s.Jobs[j.ID], cm, wm)
			if !known || better != (units == 100) {
				t.Fatal("wrong completion choice", better, known)
			}
			if units == 100 {
				if got := check(t, q, warm, nil).Job; got != nil {
					t.Fatal("warm slow stole large work")
				}
				// Existing preparation finishes; the next pull can admit it.
				cold.Environments = warm.Environments
				warm, cold = cold, warm
			}
			run := check(t, q, warm, nil).Job
			if run == nil || run.ID != j.ID {
				t.Fatal("chosen worker did not receive work", run)
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			finish(t, q, run, true)
			got := job(t, q, j.ID)
			if got.Timing == nil || !strings.Contains(JobTiming(got), "measured completion") || !strings.Contains(got.Timing.PlacementReason, "concurrency 1") {
				t.Fatal("missing chosen reason", got.Timing)
			}
			var html bytes.Buffer
			snap, err := q.publicSnapshot()
			if err != nil || statusPage.Execute(&html, snap) != nil || !strings.Contains(html.String(), "measured completion") {
				t.Fatal("HTML hid reason", err)
			}
		})
	}
}

func TestCompletionTransferExactEvidenceAndReleaseCosts(t *testing.T) {
	q, clock, warm, cold, s := completionFixture(t)
	j := submit(t, q, s)
	live := q.s.Jobs[j.ID]
	wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
	seedCompletion(t, q, live, wm, 1)
	seedCompletion(t, q, live, cm, 100)
	live.Spec.Resumable, live.Attempt = true, 1
	if _, ok := q.estimateCompletion(live, wm); ok {
		t.Fatal("first queued retry reused full-work estimate")
	}
	live.Spec.Resumable, live.Attempt = false, 0
	live.Spec.Files = FileManifest{"input": {SizeBytes: 100}}
	wm.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{{Resource: "network", Method: "file-store-download-8MiB-sha256-v1", Unit: "bytes/s", Rate: 100}}}
	cm.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{{Resource: "network", Method: "file-store-download-8MiB-sha256-v1", Unit: "bytes/s", Rate: 1}}}
	if better, known := q.completionBetter(live, cm, wm); !known || better {
		t.Fatal("transfer ignored")
	}
	// Unknown transfers and expired observations never masquerade as zero cost.
	cm.CapabilityProfile.Benchmark = nil
	if _, ok := q.estimateCompletion(live, cm); ok {
		t.Fatal("unknown transfer predicted")
	}
	live.Spec.Files = nil
	for id, p := range q.s.WorkObservations {
		p.RecordedAt = clock.Now().Add(-31 * 24 * time.Hour)
		q.s.WorkObservations[id] = p
	}
	if _, ok := q.completionBetter(live, cm, wm); ok {
		t.Fatal("stale rates ranked")
	}
	seedCompletion(t, q, live, wm, 1)
	seedCompletion(t, q, live, cm, 100)
	for id, p := range q.s.WorkObservations {
		if p.Key.Host == cm.HostID {
			p.Forbidden = true
			q.s.WorkObservations[id] = p
		}
	}
	if _, ok := q.estimateCompletion(live, cm); ok {
		t.Fatal("OOM point reused")
	}
	seedCompletion(t, q, live, cm, 100)
	// The rate cannot override existing useful-victim disruption ordering.
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1}, &placementPlan{machine: wm}) {
		t.Fatal("speed authorized extra victim")
	}
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1, cost: 1}, &placementPlan{machine: wm, stops: 1}) {
		t.Fatal("speed overrode lost work")
	}
	model, _, m := modelFixture(t)
	if _, ok := model.estimateCompletion(callJob(&ModelCall{Model: "test"}), &m); ok {
		t.Fatal("unqualified model invented estimate")
	}
}
