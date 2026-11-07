package myce

import (
	"fmt"
	"math"
	"time"
)

// Calibration learns service error only: Started through accepted completion.
// Setup and transfer have no paired phase outcomes here and must not absorb it.
// Errors are seconds per declared unit (one job for exact repeated commands).
// An immutable aggregate lives beside its exact WorkObservation, not in a new
// store. The version fences changes to the raw estimator/normalization contract.
const serviceEstimator = "raw-service-v1"

type ServiceCalibration struct {
	Estimator  string    `json:"estimator"`
	Samples    int       `json:"samples"`
	MeanError  float64   `json:"mean_error_per_unit"`
	MinError   float64   `json:"min_error_per_unit"`
	MaxError   float64   `json:"max_error_per_unit"`
	RecordedAt time.Time `json:"recorded_at"`
}

// ServiceEstimate explains the frozen forecast without treating observed error
// range as a confidence interval, a deadline, or a capacity reservation.
type ServiceEstimate struct {
	Estimator         string  `json:"estimator"`
	RawSeconds        float64 `json:"raw_seconds"`
	CorrectionSeconds float64 `json:"correction_seconds"`
	Samples           int     `json:"samples"`
	Reason            string  `json:"reason,omitempty"`
	Uncertainty       string  `json:"uncertainty"`
}

func finiteError(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (q *Queue) calibrationPrior(p WorkObservation) (*ServiceCalibration, string) {
	c := p.Calibration
	switch {
	case p.Forbidden || p.StaleReason != "":
		return nil, "calibration evidence forbidden or invalidated"
	case c == nil:
		return nil, "no accepted matching prediction/outcome pairs"
	case c.Estimator != serviceEstimator:
		return nil, "raw estimator changed; calibration unknown"
	case q.clock.Now().Before(c.RecordedAt) || q.clock.Now().Sub(c.RecordedAt) > workCurveMaxAge:
		// Share the raw curve's existing age fence. No additional fleet-tuned
		// history window or minimum sample count is introduced.
		return nil, "calibration outside matching history validity"
	case c.Samples <= 0 || !finiteError(c.MeanError) || !finiteError(c.MinError) || !finiteError(c.MaxError):
		return nil, "calibration evidence invalid"
	}
	return c, ""
}

func (q *Queue) calibratedService(key *ObservationKey, raw, units float64) (float64, *ServiceEstimate) {
	e := &ServiceEstimate{Estimator: serviceEstimator, RawSeconds: raw, Uncertainty: "service error unknown; preparation and transfer uncalibrated"}
	if key == nil || !validRate(raw) || !validRate(units) {
		e.Reason = "comparable raw service prediction unavailable"
		return raw, e
	}
	p, ok := q.s.WorkObservations[identityHash(key)]
	if !ok || p.Key != *key {
		e.Reason = "no accepted matching prediction/outcome pairs"
		return raw, e
	}
	c, reason := q.calibrationPrior(p)
	if c == nil {
		e.Reason = reason
		return raw, e
	}
	e.Samples = c.Samples
	e.CorrectionSeconds = c.MeanError * units
	e.Uncertainty = fmt.Sprintf("observed raw service error %.2f–%.2fs; future error uncertain; preparation and transfer uncalibrated", c.MinError*units, c.MaxError*units)
	if c.Samples == 1 {
		e.Uncertainty += "; one pair; variability unknown"
	}
	corrected := raw + e.CorrectionSeconds
	// A service duration must be positive and finite. An additive correction
	// that violates that physical bound costs its ranking benefit; retain raw.
	if !validRate(corrected) {
		e.CorrectionSeconds = 0
		e.Reason = "correction cannot give a positive finite duration; retained raw service"
		return raw, e
	}
	return corrected, e
}

func (e *ServiceEstimate) text() string {
	text := fmt.Sprintf("raw service %.2fs; correction %+.2fs (%d accepted pairs); %s", e.RawSeconds, e.CorrectionSeconds, e.Samples, e.Uncertainty)
	if e.Reason != "" {
		text += "; " + e.Reason
	}
	return text
}

// Called only inside the existing accepted, exact-attempt learning transaction,
// before its single putWorkObservation/ledger event. Never train on corrected
// predictions (which would feed the controller's own correction back into it).
func (q *Queue) recordServiceCalibration(j *Job, r Result, old WorkObservation, point *WorkObservation) {
	t := j.Timing
	if t == nil || t.ServiceEstimate == nil || t.ServiceEstimate.Estimator != serviceEstimator || t.Reason != "" || t.ActualSeconds == nil ||
		!r.Success || r.Interrupted || r.Forced || r.OutOfMemory || r.ReleasePending ||
		t.Execution.ID != r.ID || t.Execution.Attempt != r.Attempt || t.Machine != r.Machine || t.Session != r.Session ||
		old.Forbidden || old.StaleReason != "" || point.Key.Check ||
		t.HistoryRecordedAt == nil || q.clock.Now().Before(*t.HistoryRecordedAt) || q.clock.Now().Sub(*t.HistoryRecordedAt) > workCurveMaxAge ||
		r.MemoryPeak != nil && r.MemoryPeak.constrained() {
		return
	}
	key, units := t.Key, 0.0
	if t.RepeatedKey != nil {
		key, units = t.RepeatedKey, 1
	} else if j.Spec.Workload != nil && j.Batch == nil {
		units = j.Spec.Workload.Units
	}
	if key == nil || *key != point.Key || !validRate(units) || !validRate(t.ServiceEstimate.RawSeconds) || !validRate(*t.ActualSeconds) {
		return
	}
	errorPerUnit := (*t.ActualSeconds - t.ServiceEstimate.RawSeconds) / units
	if !finiteError(errorPerUnit) {
		return
	}
	c := ServiceCalibration{Estimator: serviceEstimator, Samples: 1, MeanError: errorPerUnit, MinError: errorPerUnit, MaxError: errorPerUnit, RecordedAt: q.clock.Now()}
	if prior, _ := q.calibrationPrior(old); prior != nil {
		c.Samples += prior.Samples
		c.MeanError = prior.MeanError + (errorPerUnit-prior.MeanError)/float64(c.Samples)
		c.MinError, c.MaxError = min(prior.MinError, errorPerUnit), max(prior.MaxError, errorPerUnit)
	}
	if finiteError(c.MeanError) {
		point.Calibration = &c
	}
}

func (q *Queue) calibrateWorkTiming(j *Job) {
	t := j.Timing
	if t == nil || t.Reason != "" || t.PredictedSeconds == nil {
		return
	}
	key, units := t.Key, 0.0
	if t.RepeatedKey != nil {
		key, units = t.RepeatedKey, 1
	} else if j.Spec.Workload != nil && j.Batch == nil {
		units = j.Spec.Workload.Units
	}
	if !validRate(units) {
		return
	}
	prediction, evidence := q.calibratedService(key, *t.PredictedSeconds, units)
	t.PredictedSeconds, t.ServiceEstimate = &prediction, evidence
}
