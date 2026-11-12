package myce

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
)

// Quality is publisher-supplied evidence frozen in the exact extension recipe.
// MarginOfError measures uncertainty of the difference against ReferenceValue,
// in the metric's units. Nil means unknown, never a zero-error observation.
type QualityMeasurement struct {
	Reference      string   `json:"reference" yaml:"reference"`
	Value          *float64 `json:"value" yaml:"value" jsonschema:"nullable"`
	ReferenceValue *float64 `json:"reference_value" yaml:"reference_value" jsonschema:"nullable"`
	MarginOfError  *float64 `json:"margin_of_error" yaml:"margin_of_error" jsonschema:"nullable"`
}

func finiteQuality(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validateQualityTolerance(s Spec) error {
	if len(s.QualityTolerance) > 0 && !s.extensionJob() {
		return errors.New("quality_tolerance requires an extension job")
	}
	for metric, tolerance := range s.QualityTolerance {
		if !extensionName.MatchString(metric) || !finiteQuality(tolerance) || tolerance < 0 {
			return errors.New("quality_tolerance requires metric names and finite nonnegative absolute differences")
		}
	}
	return nil
}

func (e Extension) validateQuality(quality map[string]map[string]QualityMeasurement) error {
	for work, metrics := range quality {
		if !slices.Contains(extensionWorks(e), work) {
			return errors.New("quality requires an exact declared work type")
		}
		for metric, m := range metrics {
			if !extensionName.MatchString(metric) || strings.TrimSpace(m.Reference) == "" {
				return errors.New("quality requires a metric name and frozen reference identity")
			}
			for _, v := range []*float64{m.Value, m.ReferenceValue, m.MarginOfError} {
				if v != nil && !finiteQuality(*v) {
					return errors.New("quality values must be finite or null (unknown)")
				}
			}
			if m.MarginOfError != nil && *m.MarginOfError < 0 {
				return errors.New("quality margin_of_error must be nonnegative")
			}
		}
	}
	return nil
}

func (e Extension) hasQuality(work string) bool {
	return slices.ContainsFunc(e.Variants, func(v ExtensionVariant) bool { return len(v.Quality[work]) > 0 })
}

// One eligibility rule serves scheduling, retargeting and status. Known failures
// cannot be overridden by host approval. Unknown evidence retains that fallback.
func variantQuality(e Extension, s Spec, m *Machine) (measured bool, reason string) {
	metrics := map[string]bool{}
	for metric := range s.QualityTolerance {
		metrics[metric] = true
	}
	for _, v := range e.Variants {
		for metric := range v.Quality[s.workType()] {
			metrics[metric] = true
		}
	}
	v := e.variant(machineTarget(m))
	if v == nil || len(metrics) == 0 {
		return false, ""
	}
	names := make([]string, 0, len(metrics))
	for metric := range metrics {
		names = append(names, metric)
	}
	slices.Sort(names)
	measured = true
	for _, metric := range names {
		observation, ok := v.Quality[s.workType()][metric]
		if !ok || observation.Value == nil || observation.ReferenceValue == nil || observation.MarginOfError == nil {
			measured = false
			continue
		}
		limit, explicit := s.QualityTolerance[metric]
		if !explicit {
			limit = *observation.MarginOfError
		}
		delta := math.Abs(*observation.Value - *observation.ReferenceValue)
		if delta > limit {
			return false, fmt.Sprintf("quality metric %s out of tolerance: absolute difference %g exceeds %g", metric, delta, limit)
		}
	}
	return measured, ""
}

// Existing production bindings require positive parity evidence even for legacy
// registrations. Measurements cannot widen an explicit owner host list.
func (q *Queue) productionParityKnown(j *Job, m *Machine) bool {
	if !j.Spec.extensionJob() || j.Spec.ExtensionCheck != nil {
		return false
	}
	if !q.productionParityAllowed(j, m) {
		return false
	}
	approved, _ := q.parityApproved(j, m)
	for _, e := range j.Extensions {
		copy := *j
		copy.Extensions = []Extension{e}
		if qualifiedExtension(&copy, m) == nil {
			continue
		}
		measured, reason := variantQuality(e, j.Spec, m)
		if reason == "" && (measured || approved) {
			return true
		}
	}
	return false
}
