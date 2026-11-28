package myce

import (
	"maps"
	"math"
	"slices"
	"time"
)

const (
	initialMemoryConfidence = 0.90
	memoryPeakWindow        = 20
	memoryRatioWindow       = 200
)

// Host and card observations use separate histories even when recorded by
// the same attempt. Card count is part of the exact identity.
type memoryEvidenceKey struct {
	Platform string
	Pool     string
	Family   string
	Exact    string
}

type memoryEvidencePoint struct {
	Key     memoryEvidenceKey
	At      time.Time
	Peak    float64
	Limited bool
}

type memoryPoolEstimate struct {
	TypicalGB    float64                `json:"typical_gb"`
	ChargeGB     float64                `json:"charge_gb"`
	Evidence     memoryEstimateEvidence `json:"evidence"`
	Alone        bool                   `json:"alone,omitempty"`
	HeldGB       float64                `json:"held_gb,omitempty"`
	PeakGB       float64                `json:"peak_gb,omitempty"`
	Learned      bool                   `json:"learned,omitempty"`
	RetryFloorGB float64                `json:"-"`
	CapacityWait bool                   `json:"-"`
}

type memoryEstimateEvidence struct {
	Level string `json:"level"`
	Runs  int    `json:"runs"`
}

type memoryEvidenceSeries struct {
	Peaks     []float64
	Lower     float64
	Raise     *memoryRaise `json:",omitempty"`
	HighWater float64      `json:",omitempty"`
	// Trailing entries of Peaks, with an empty identity for an unbound sample.
	// Older queues can still use Peaks without input coverage.
	CardInputs []string `json:",omitempty"`
}

type memoryRatioKey struct {
	Platform string
	Pool     string
	Level    string
	From     string
	Bucket   int
}

type memoryEvidence struct {
	exact            map[memoryEvidenceKey]memoryEvidenceSeries
	family           map[memoryEvidenceKey]memoryEvidenceSeries
	first            map[memoryEvidenceKey][]float64
	ratios           map[memoryRatioKey][]float64
	foreign          map[memoryEvidenceKey]map[string]bool
	writes           map[memoryEvidenceRowKey]any
	feedback         []float64
	owners           map[memoryEvidenceKey]memoryOwnersHold
	gpuSharing       map[gpuSharingKey]gpuSharingHistory
	gpuSharingWrites map[gpuSharingKey]gpuSharingHistory
}

func (e *memoryEvidence) stage() *memoryEvidence {
	return &memoryEvidence{
		exact: maps.Clone(e.exact), family: maps.Clone(e.family), first: maps.Clone(e.first),
		ratios: maps.Clone(e.ratios), foreign: maps.Clone(e.foreign), writes: map[memoryEvidenceRowKey]any{},
		feedback:   e.feedback,
		owners:     maps.Clone(e.owners),
		gpuSharing: e.gpuSharing,
	}
}

// The initial uncovered tail is the most a full window of outcomes can move
// the target. Rule Q bounds the upper end to what the calibration ring can
// actually estimate. A clean footprint contributes the unused fraction of its
// charge. An overrun that forced a stop contributes one whole outcome.
func (e *memoryEvidence) confidence() float64 {
	if len(e.feedback) == 0 {
		return initialMemoryConfidence
	}
	var sum float64
	for _, event := range e.feedback {
		sum += event
	}
	priorWeight := initialMemoryConfidence / (1 - initialMemoryConfidence)
	return min(float64(memoryRatioWindow)/float64(memoryRatioWindow+1), initialMemoryConfidence+(1-initialMemoryConfidence)*sum/(priorWeight+float64(len(e.feedback))))
}

func (e *memoryEvidence) observeFeedback(charge, peak float64, pressure, clean bool) {
	if charge <= 0 || peak <= 0 {
		return
	}
	var delta float64
	if pressure && peak > charge {
		delta = 1
	} else if clean {
		delta = -max(0, 1-peak/charge)
	} else {
		return
	}
	e.feedback = appendMemoryValue(e.feedback, delta, memoryRatioWindow)
	e.write(memoryEvidenceRowKey{kind: "feedback"}, e.feedback)
}

func newMemoryEvidence() *memoryEvidence {
	return &memoryEvidence{
		exact:      map[memoryEvidenceKey]memoryEvidenceSeries{},
		family:     map[memoryEvidenceKey]memoryEvidenceSeries{},
		first:      map[memoryEvidenceKey][]float64{},
		ratios:     map[memoryRatioKey][]float64{},
		foreign:    map[memoryEvidenceKey]map[string]bool{},
		owners:     map[memoryEvidenceKey]memoryOwnersHold{},
		gpuSharing: map[gpuSharingKey]gpuSharingHistory{},
	}
}

func (k memoryEvidenceKey) familyKey() memoryEvidenceKey {
	k.Exact = ""
	return k
}

func (k memoryEvidenceKey) platformKey() memoryEvidenceKey {
	return memoryEvidenceKey{Platform: k.Platform, Pool: k.Pool}
}

func (k memoryEvidenceKey) workKey() memoryEvidenceKey {
	k.Platform, k.Exact = "", ""
	return k
}

func memoryRatioBucket(n int) int {
	switch {
	case n >= 20:
		return 20
	case n >= 10:
		return 10
	case n >= 5:
		return 5
	case n >= 3:
		return 3
	default:
		return n
	}
}

// The ceiling of (n+1)c is also the amount of evidence required. Comparing
// that rank to n implements Rule Q without rounding c/(1-c) separately.
func memoryQuantile(values []float64, confidence float64) (float64, bool) {
	rank := int(math.Ceil(float64(len(values)+1) * confidence))
	if rank < 1 || rank > len(values) {
		return 0, false
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[rank-1], true
}

func memoryMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[(len(sorted)-1)/2]/2 + sorted[len(sorted)/2]/2
}

func appendMemoryValue[T any](values []T, value T, limit int) []T {
	if len(values) == limit {
		values = values[1:]
	}
	return append(slices.Clone(values), value)
}

func (e *memoryEvidence) observeRatio(k memoryEvidenceKey, level, from string, previous []float64, peak float64) {
	if len(previous) == 0 {
		return
	}
	key := memoryRatioKey{k.Platform, k.Pool, level, from, memoryRatioBucket(len(previous))}
	ratio := peak / slices.Max(previous)
	if validRate(ratio) {
		e.ratios[key] = appendMemoryValue(e.ratios[key], ratio, memoryRatioWindow)
		e.write(memoryEvidenceRowKey{kind: "ratio", ratio: key}, e.ratios[key])
	}
}

func (e *memoryEvidence) observe(p memoryEvidencePoint) {
	if !validRate(p.Peak) || p.Key.Platform == "" || p.Key.Family == "" || p.Key.Exact == "" {
		return
	}
	k := p.Key
	exact := e.exact[k]
	if p.Limited {
		exact.Lower = max(exact.Lower, p.Peak)
		e.exact[k] = exact
		e.write(memoryEvidenceRowKey{kind: "exact", key: k}, exact)
		return
	}
	familyKey, platformKey, workKey := k.familyKey(), k.platformKey(), k.workKey()
	family := e.family[familyKey]
	e.observeRatio(k, "exact", "", exact.Peaks, p.Peak)
	e.observeRatio(k, "family", "", family.Peaks, p.Peak)
	for other := range e.foreign[workKey] {
		if other != k.Platform {
			foreignKey := memoryEvidenceKey{Platform: other, Pool: k.Pool, Family: k.Family}
			e.observeRatio(k, "foreign", other, e.family[foreignKey].Peaks, p.Peak)
		}
	}
	if len(family.Peaks) == 0 {
		e.first[platformKey] = appendMemoryValue(e.first[platformKey], p.Peak, memoryRatioWindow)
		e.write(memoryEvidenceRowKey{kind: "first", key: platformKey}, e.first[platformKey])
		if e.foreign[workKey] == nil {
			e.foreign[workKey] = map[string]bool{}
		} else {
			e.foreign[workKey] = maps.Clone(e.foreign[workKey])
		}
		e.foreign[workKey][k.Platform] = true
	}
	exact.Peaks = appendMemoryValue(exact.Peaks, p.Peak, memoryPeakWindow)
	family.Peaks = appendMemoryValue(family.Peaks, p.Peak, memoryPeakWindow)
	e.exact[k], e.family[familyKey] = exact, family
	e.write(memoryEvidenceRowKey{kind: "exact", key: k}, exact)
	e.write(memoryEvidenceRowKey{kind: "family", key: familyKey}, family)
}

func (e *memoryEvidence) estimate(k memoryEvidenceKey, hint, confidence float64) memoryPoolEstimate {
	return e.estimateWithFloor(k, hint, confidence, e.exact[k].Lower)
}

func (e *memoryEvidence) estimateWithFloor(k memoryEvidenceKey, hint, confidence, lower float64) memoryPoolEstimate {
	var typical, charge float64
	finish := func(level string, runs int, alone bool) memoryPoolEstimate {
		typical = max(typical, lower)
		return memoryPoolEstimate{TypicalGB: typical, ChargeGB: max(typical, charge, 2*lower), Evidence: memoryEstimateEvidence{Level: level, Runs: runs}, Alone: alone}
	}
	exact, family := e.exact[k], e.family[k.familyKey()]
	for _, candidate := range []struct {
		level string
		peaks []float64
	}{{"exact", exact.Peaks}, {"family", family.Peaks}} {
		n := len(candidate.peaks)
		if n == 0 {
			continue
		}
		peak := slices.Max(candidate.peaks)
		median := memoryMedian(candidate.peaks)
		if factor, ok := memoryQuantile(e.ratios[memoryRatioKey{k.Platform, k.Pool, candidate.level, "", memoryRatioBucket(n)}], confidence); ok && validRate(factor*peak) {
			typical, charge = max(typical, median), max(charge, factor*peak)
			return finish(candidate.level, n, false)
		}
		typical, charge = max(typical, median), max(charge, peak)
	}
	if len(family.Peaks) == 0 && validRate(hint) {
		typical, charge = hint, hint
		return finish("hint", 0, k.Pool == "card")
	}
	// A different platform contributes only where the destination has learned
	// the transfer error. Use the most supported calibrated relationship.
	var foreign memoryPoolEstimate
	for _, other := range slices.Sorted(maps.Keys(e.foreign[k.workKey()])) {
		if other == k.Platform {
			continue
		}
		peaks := e.family[memoryEvidenceKey{Platform: other, Pool: k.Pool, Family: k.Family}].Peaks
		ratios := e.ratios[memoryRatioKey{k.Platform, k.Pool, "foreign", other, memoryRatioBucket(len(peaks))}]
		factor, ok := memoryQuantile(ratios, confidence)
		if ok && len(ratios) > foreign.Evidence.Runs && validRate(factor*slices.Max(peaks)) {
			foreign = memoryPoolEstimate{TypicalGB: memoryMedian(peaks) * memoryMedian(ratios), ChargeGB: factor * slices.Max(peaks), Evidence: memoryEstimateEvidence{Runs: len(ratios)}}
		}
	}
	if foreign.Evidence.Runs > 0 {
		typical, charge = max(typical, foreign.TypicalGB), max(charge, foreign.ChargeGB)
		return finish("foreign", foreign.Evidence.Runs, k.Pool == "card" && len(family.Peaks) == 0)
	}
	prior := e.first[k.platformKey()]
	if bound, ok := memoryQuantile(prior, confidence); ok {
		typical, charge = max(typical, memoryMedian(prior)), max(charge, bound)
		return finish("platform", len(prior), k.Pool == "card" && len(family.Peaks) == 0)
	}
	if validRate(hint) {
		// A measured peak raises the estimate's floor. Keep the authored
		// charge until a calibration has enough history to replace it.
		charge = max(charge, hint)
		return finish("hint", len(family.Peaks), false)
	}
	return finish("unknown", 0, true)
}
