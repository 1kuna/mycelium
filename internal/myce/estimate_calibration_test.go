package myce

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// Replay the unchanged raw estimator and calibration on the same accepted
// history. Every held-out prediction precedes its outcome; no future samples
// leak into either forecast. Units change on continuation to test normalization.
func TestEstimateCalibrationHeldOutContinuation(t *testing.T) {
	for _, biased := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbiased", true: "biased"}[biased], func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			rawSquared, correctedSquared := 0.0, 0.0
			for i := range 80 {
				seconds := 2.0
				if biased {
					seconds = float64(1 + 2*(i%2))
				}
				if i >= 60 {
					s.Workload = &Workload{InputShape: "small-v1", Units: 20}
					seconds *= 2
				}
				j := startObservation(t, q, m, s)
				timing := q.s.Jobs[j.ID].Timing
				if i >= 60 {
					e := timing.ServiceEstimate
					if e == nil || e.Samples != i-1 {
						t.Fatalf("held-out forecast lost paired evidence: %+v", timing)
					}
					rawSquared += math.Pow(e.RawSeconds-seconds, 2)
					correctedSquared += math.Pow(*timing.PredictedSeconds-seconds, 2)
					if !biased && (e.CorrectionSeconds != 0 || *timing.PredictedSeconds != seconds) {
						t.Fatal("unbiased history acquired bias", e)
					}
				}
				clock.Advance(time.Duration(seconds * float64(time.Second)))
				if err := q.Complete(cleanRepeatedResult(&j)); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("20 held-out forecasts: raw MSE %.6f, calibrated MSE %.6f", rawSquared/20, correctedSquared/20)
			if biased && correctedSquared >= rawSquared {
				t.Fatal("calibration did not improve held-out error")
			}
		})
	}
}

func TestEstimateCalibrationPairsAndRestart(t *testing.T) {
	q, clock, m, _, s := repeatedFixture(t)
	teachRepeated(t, q, clock, m, s, 2*time.Second)           // no prediction: no pair
	second := teachRepeated(t, q, clock, m, s, 4*time.Second) // raw 2, error +2
	if err := q.Complete(cleanRepeatedResult(&second)); err != nil {
		t.Fatal(err)
	}
	third := teachRepeated(t, q, clock, m, s, 4*time.Second) // raw 3, corrected 5; error +1, not -1
	got := job(t, q, third.ID)
	if e := got.Timing.ServiceEstimate; e == nil || e.RawSeconds != 3 || e.CorrectionSeconds != 2 || e.Samples != 1 || *got.Timing.PredictedSeconds != 5 {
		t.Fatal("missing frozen raw/correction", got.Timing)
	}
	key := got.Timing.RepeatedKey
	p := q.s.WorkObservations[identityHash(key)]
	if p.Calibration == nil || p.Calibration.Samples != 2 || p.Calibration.MeanError != 1.5 {
		t.Fatal("duplicate counted or corrected prediction trained", p.Calibration)
	}
	// Existing release-preservation receipts cover calibration in the same row.
	before := preservationSnapshot(t, q)
	fourth := teachRepeated(t, q, clock, m, s, 3*time.Second)
	page, err := q.JobLedger(fourth.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyCurvePreservation(before, preservationSnapshot(t, q), []JobLedgerPage{page}); err != nil {
		t.Fatal("calibration lacks accepted ledger provenance", err)
	}
	base := &Job{Spec: got.Spec, State: "queued"}
	estimate, ok := q.estimateCompletion(base, q.s.Machines[m.ID])
	if !ok || estimate.calibration == nil || estimate.calibration.Samples != 3 {
		t.Fatal("completion ranking lost calibration", estimate)
	}
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	check(t, reopened, m, nil)
	after, ok := reopened.estimateCompletion(base, reopened.s.Machines[m.ID])
	if !ok || after.seconds() != estimate.seconds() || *after.calibration != *estimate.calibration {
		t.Fatal("calibration changed on restart", after, estimate)
	}
	var writesBefore, writesAfter int
	reopened.db.QueryRow("SELECT total_changes()").Scan(&writesBefore)
	snap, err := reopened.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var html bytes.Buffer
	if err := statusPage.Execute(&html, snap); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(snap)
	if err != nil || !bytes.Contains(data, []byte(`"service_estimate"`)) || !strings.Contains(html.String(), "raw service") || !strings.Contains(JobTiming(got), "accepted pairs") {
		t.Fatal("status omitted raw/correction/evidence", err)
	}
	reopened.db.QueryRow("SELECT total_changes()").Scan(&writesAfter)
	if writesAfter != writesBefore {
		t.Fatal("status trained or persisted calibration")
	}
	clock.Advance(workCurveMaxAge + time.Second)
	if _, ok := reopened.estimateCompletion(base, reopened.s.Machines[m.ID]); ok {
		t.Fatal("expired forecast ranked")
	}
}

func TestEstimateCalibrationRejectsIneligiblePairs(t *testing.T) {
	for _, scenario := range []string{"failed", "interrupted", "stale attempt", "stale session", "changed runtime", "changed command", "mixed occupancy", "constrained", "unknown pressure", "expired prediction"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, _, s := repeatedFixture(t)
			teachRepeated(t, q, clock, m, s, time.Second)
			teachRepeated(t, q, clock, m, s, 2*time.Second)
			j := startObservation(t, q, m, s)
			if q.s.Jobs[j.ID].Timing.ServiceEstimate.Samples != 1 {
				t.Fatal("fixture lacks trained pair")
			}
			key := q.s.Jobs[j.ID].Timing.RepeatedKey
			before := *q.s.WorkObservations[identityHash(key)].Calibration
			r := cleanRepeatedResult(&j)
			switch scenario {
			case "failed":
				r.Success = false
			case "interrupted":
				r.Interrupted = true
			case "stale attempt":
				r.Attempt++
			case "stale session":
				r.Session = "old-session"
			case "changed runtime":
				m.ExtensionFingerprint = "runtime-v2"
				check(t, q, m, &j)
			case "changed command":
				if err := q.update(func() error { q.touchJob(j.ID).Spec.Run.Command = []string{"echo", "changed"}; return nil }); err != nil {
					t.Fatal(err)
				}
			case "mixed occupancy":
				other := submit(t, q, cpuSpec(1, .1))
				if err := q.update(func() error { q.assign(q.s.Jobs[other.ID], q.s.Machines[m.ID]); return nil }); err != nil {
					t.Fatal(err)
				}
				finish(t, q, q.s.Jobs[other.ID], true)
			case "constrained":
				r.MemoryPeak.Constrained = true
			case "unknown pressure":
				r.MemoryPeak = nil
			case "expired prediction":
				clock.Advance(workCurveMaxAge + time.Second)
			}
			clock.Advance(time.Second)
			err := q.Complete(r)
			if strings.HasPrefix(scenario, "stale ") {
				if !errors.Is(err, ErrStale) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			after := q.s.WorkObservations[identityHash(key)].Calibration
			if after != nil && *after != before {
				t.Fatal("ineligible outcome trained calibration", after, before)
			}
		})
	}
}

func TestEstimateCalibrationUnknownAndPhysicalFallback(t *testing.T) {
	q, clock, m, _, s := repeatedFixture(t)
	j := teachRepeated(t, q, clock, m, s, time.Second)
	key := q.s.Jobs[j.ID].Timing.RepeatedKey
	id := identityHash(key)
	for _, scenario := range []string{"missing", "changed estimator", "future", "stale", "forbidden", "invalidated", "negative duration"} {
		p := q.s.WorkObservations[id]
		p.Calibration = &ServiceCalibration{Estimator: serviceEstimator, Samples: 2, MeanError: 1, MinError: 0, MaxError: 2, RecordedAt: clock.Now()}
		switch scenario {
		case "missing":
			p.Calibration = nil
		case "changed estimator":
			p.Calibration.Estimator = "different-raw-estimator"
		case "future":
			p.Calibration.RecordedAt = clock.Now().Add(time.Second)
		case "stale":
			p.Calibration.RecordedAt = clock.Now().Add(-workCurveMaxAge - time.Second)
		case "forbidden":
			p.Forbidden = true
		case "invalidated":
			p.StaleReason = "changed configuration"
		case "negative duration":
			p.Calibration.MeanError = -2
		}
		q.s.WorkObservations[id] = p
		seconds, e := q.calibratedService(key, 1, 1)
		if seconds != 1 || e.CorrectionSeconds != 0 || e.Reason == "" {
			t.Fatal("unsafe correction or unexplained fallback", scenario, seconds, e)
		}
		p.Forbidden, p.StaleReason = false, ""
		q.s.WorkObservations[id] = p
	}
}

func TestEstimateCalibrationPlacementBoundaries(t *testing.T) {
	for _, rule := range []string{"ranking", "busy and custody", "scarcity", "tier", "fairness"} {
		t.Run(rule, func(t *testing.T) {
			q, clock, slow, fast, s := repeatedFixture(t)
			var trained Job
			for _, duration := range []time.Duration{time.Second, time.Second, 5 * time.Second} {
				teachRepeated(t, q, clock, fast, s, duration)
				trained = teachRepeated(t, q, clock, slow, s, 3*time.Second)
			}
			// Forecast the same canonical resource contract as submission; CPU
			// memory_gb is normalized into host_memory_gb by admission.
			s = trained.Spec
			s.AllowedMachines = nil
			probe := &Job{Spec: s, State: "queued"}
			a, aok := q.estimateCompletion(probe, q.s.Machines[slow.ID])
			b, bok := q.estimateCompletion(probe, q.s.Machines[fast.ID])
			if !aok || !bok || a.calibration.RawSeconds <= b.calibration.RawSeconds || a.seconds() >= b.seconds() || b.calibration.Samples != 2 {
				t.Fatal("fixture must reverse raw ranking using accepted error", a, b)
			}
			switch rule {
			case "scarcity":
				// The calibrated winner is scarcer; value still comes first.
				fast.CapabilityProfile.SystemMemoryBytes = 32 << 30
				check(t, q, fast, nil)
				wanted := submit(t, q, s)
				if check(t, q, slow, nil).Job != nil {
					t.Fatal("calibration overrode scarcity")
				}
				if got := check(t, q, fast, nil).Job; got == nil || got.ID != wanted.ID {
					t.Fatal("cheaper admissible host lost", got)
				}
				return
			case "tier", "fairness":
				other := s
				other.Project = "fresh-project"
				if rule == "tier" {
					preemptionPolicy(t, q)
					other.Project = "urgent"
				}
				wanted := submit(t, q, other)
				clock.Advance(time.Millisecond)
				lower := submit(t, q, s)
				if got := check(t, q, slow, nil).Job; got == nil || got.ID != wanted.ID {
					t.Fatal("calibration changed fair/higher-tier winner", got)
				}
				if got := check(t, q, fast, nil).Job; got == nil || got.ID != lower.ID {
					t.Fatal("spare admissible capacity went unused", got)
				}
				return
			}
			wanted := submit(t, q, s)
			if check(t, q, fast, nil).Job != nil {
				t.Fatal("raw winner stole calibrated placement")
			}
			run := check(t, q, slow, nil).Job
			if run == nil || run.ID != wanted.ID {
				t.Fatal("calibrated winner lost", run)
			}
			if rule == "ranking" {
				return
			}
			waiting := submit(t, q, s)
			if check(t, q, slow, run).Job != nil {
				t.Fatal("forecast reused occupied cores")
			}
			if got := check(t, q, fast, nil).Job; got == nil || got.ID != waiting.ID {
				t.Fatal("forecast reserved future capacity", got)
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			stale := cleanRepeatedResult(run)
			stale.Attempt++
			if err := q.Complete(stale); !errors.Is(err, ErrStale) {
				t.Fatal("stale outcome released custody", err)
			}
			got := job(t, q, run.ID)
			if got.State != "running" || got.Attempt != run.Attempt || got.Machine != slow.ID || priorityStop(got) != nil {
				t.Fatal("calibration moved running work or broke exact custody", got)
			}
		})
	}
}

func TestEstimateCalibrationQualificationAndParity(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	e := registeredExtension(t, q, s.Extension)
	e.Version = "calibration-parity"
	other := e.Variants[0]
	other.Target.Arch = "arm64"
	e.Variants = append(e.Variants, other)
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s.Extension = extensionKey(e)
	report := newExtensionReport(e, &m, s.workType())
	report.Setup, report.Qualified, report.CheckID = true, true, "native-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	for _, duration := range []time.Duration{time.Second, 2 * time.Second} {
		j := startObservation(t, q, m, s)
		clock.Advance(duration)
		if err := q.Complete(cleanRepeatedResult(&j)); err != nil {
			t.Fatal(err)
		}
	}
	s.Class = "production"
	production := submit(t, q, s)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("calibration bypassed MY-19 parity")
	}
	if got := job(t, q, production.ID); !strings.Contains(got.Reason, "MY-19") {
		t.Fatal("missing parity reason", got.Reason)
	}
	policy := q.PriorityPolicy()
	policy.Policy.Projects[s.Project] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{s.workType(): {hostID(&m)}}}
	if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
		t.Fatal(err)
	}
	m.ExtensionReports[0].Qualified = false
	check(t, q, m, nil)
	if _, ok := q.estimateCompletion(q.s.Jobs[production.ID], q.s.Machines[m.ID]); ok {
		t.Fatal("calibration bypassed qualification")
	}
	m.ExtensionReports[0].Qualified = true
	got := check(t, q, m, nil).Job
	if got == nil || got.ID != production.ID {
		t.Fatal("owner-approved qualified work did not place", got)
	}
	if !strings.Contains(q.s.Jobs[production.ID].Timing.PlacementReason, "1 accepted pairs") {
		t.Fatal("placement did not retain calibration evidence")
	}
}

func TestEstimateCalibrationBenchmarkCannotTrainOrReplacePairs(t *testing.T) {
	q, clock, m, e, s := curveFixture(t)
	key, reason := curveKey(e, q.s.Machines[m.ID], s.workType())
	if key == nil {
		t.Fatal(reason)
	}
	point := WorkObservation{Key: *key, Source: "idle reference", SampleID: "first-benchmark", Rate: 5, Units: 10, Seconds: 2, LatencySeconds: 2, RecordedAt: clock.Now(),
		Calibration: &ServiceCalibration{Estimator: serviceEstimator, Samples: 100, MeanError: 100, RecordedAt: clock.Now()}}
	publishCurves(t, q, m, []WorkObservation{point})
	id := identityHash(key)
	if got := q.s.WorkObservations[id]; got.Samples != 1 || got.Calibration != nil {
		t.Fatal("unpaired benchmark introduced calibration", got)
	}
	j := startObservation(t, q, m, s)
	clock.Advance(4 * time.Second)
	if err := q.Complete(cleanRepeatedResult(&j)); err != nil {
		t.Fatal(err)
	}
	prior := q.s.WorkObservations[id].Calibration
	if prior == nil || prior.Samples != 1 || prior.MeanError != .2 {
		t.Fatal("accepted pair not recorded", prior)
	}
	point.SampleID, point.RecordedAt = "second-benchmark", clock.Now()
	publishCurves(t, q, m, []WorkObservation{point})
	if got := q.s.WorkObservations[id].Calibration; got == nil || *got != *prior {
		t.Fatal("benchmark replaced accepted calibration", got, prior)
	}
}

func TestEstimateCalibrationProgressIsNotAttemptIdentity(t *testing.T) {
	q, clock, m, _, s := repeatedFixture(t)
	teachRepeated(t, q, clock, m, s, time.Second)
	j := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	r := cleanRepeatedResult(&j)
	r.Progress = 7
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	key := q.s.Jobs[j.ID].Timing.RepeatedKey
	if c := q.s.WorkObservations[identityHash(key)].Calibration; c == nil || c.Samples != 1 || c.MeanError != 1 {
		t.Fatal("accepted progress report mistaken for a different attempt", c)
	}
}
