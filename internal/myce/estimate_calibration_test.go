package myce

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// Replay the unchanged raw estimator and calibration on the same accepted
// history. Every held-out prediction precedes its outcome; no future samples
// leak into either forecast. Units change on continuation to test normalization.

func TestEstimateCalibrationPairsAndRestart(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
	})

	t.Run("EstimateCalibrationProgressIsNotAttemptIdentity", func(t *testing.T) {
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

	})

	t.Run("EstimateCalibrationInvalidationCannotRevivePairs", func(t *testing.T) {
		q, clock, m, s := observationFixture(t)
		var last Job
		for _, duration := range []time.Duration{time.Second, 2 * time.Second} {
			last = startObservation(t, q, m, s)
			clock.Advance(duration)
			if err := q.Complete(cleanRepeatedResult(&last)); err != nil {
				t.Fatal(err)
			}
		}
		originalFingerprint := m.ExtensionFingerprint
		m.ExtensionFingerprint = "changed-runtime"
		check(t, q, m, nil)
		m.ExtensionFingerprint = originalFingerprint
		check(t, q, m, nil)
		// The first accepted completion re-establishes raw speed, without a prior
		// forecast. It cannot rehabilitate the old error aggregate as a side effect.
		unpaired := startObservation(t, q, m, s)
		if timing := job(t, q, unpaired.ID).Timing; timing.PredictedSeconds != nil || timing.ServiceEstimate != nil {
			t.Fatal("invalidated history predicted the first restored run", timing)
		}
		clock.Advance(4 * time.Second)
		if err := q.Complete(cleanRepeatedResult(&unpaired)); err != nil {
			t.Fatal(err)
		}
		next := startObservation(t, q, m, s)
		e := job(t, q, next.ID).Timing.ServiceEstimate
		if e == nil || e.RawSeconds != 4 || e.CorrectionSeconds != 0 || e.Samples != 0 || e.Reason != "no accepted matching prediction/outcome pairs" {
			t.Fatal("new raw speed reused invalidated correction", e)
		}
		clock.Advance(4 * time.Second)
		if err := q.Complete(cleanRepeatedResult(&next)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEstimateCalibrationRejectsIneligiblePairs(t *testing.T) {
	for _, scenario := range []string{"stale attempt", "stale session", "changed runtime", "changed command"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, _, s := repeatedFixture(t)
			teachRepeated(t, q, clock, m, s, time.Second)
			teachRepeated(t, q, clock, m, s, 2*time.Second)
			j := startObservation(t, q, m, s)
			key := q.s.Jobs[j.ID].Timing.RepeatedKey
			before := *q.s.WorkObservations[identityHash(key)].Calibration
			r := cleanRepeatedResult(&j)
			switch scenario {
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
	if offered := check(t, q, m, nil).Job; offered != nil {
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
