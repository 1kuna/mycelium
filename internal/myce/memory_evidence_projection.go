package myce

import (
	"cmp"
	"slices"
	"strings"
)

// Legacy retained observations contain maxima, not the individual samples
// that produced them. Project each row once. Component availability, driver
// identity and resource policy are irrelevant to the new evidence key.
func projectMemoryEvidence(points map[string]WorkObservation) []memoryEvidencePoint {
	var out []memoryEvidencePoint
	for _, p := range points {
		if p.MemoryFamily == "" || p.MemoryConfiguration == "" || p.Key.Target.OS == "" || p.Key.Target.Platform == "" {
			continue
		}
		key := memoryEvidenceKey{
			Platform: memoryPlatform(p.Key.Target, p.MemoryLayout == "unified"),
			Family:   p.MemoryFamily,
			Exact:    memoryEvidenceExact(p.Key, p.MemoryConfiguration),
		}
		if p.MemoryPeak != nil {
			out = appendProjectedMemoryPeaks(out, key, memoryEvidencePoint{At: p.MemoryRecordedAt, Limited: p.MemoryPeak.constrained()}, p.MemoryPeak)
		}
		if prior := p.MemoryEscalation; prior != nil && prior.Peak != nil {
			out = appendProjectedMemoryPeaks(out, key, memoryEvidencePoint{At: prior.RecordedAt, Limited: true}, prior.Peak)
		}
	}
	slices.SortFunc(out, func(a, b memoryEvidencePoint) int {
		return cmp.Or(a.At.Compare(b.At), cmp.Compare(a.Key.Platform, b.Key.Platform), cmp.Compare(a.Key.Pool, b.Key.Pool), cmp.Compare(a.Key.Family, b.Key.Family), cmp.Compare(a.Key.Exact, b.Key.Exact), cmp.Compare(a.Peak, b.Peak))
	})
	return out
}

func memoryEvidenceExact(k ObservationKey, configuration string) string {
	return identityHash(struct {
		Work, Extension, Configuration string
		Reference                      WorkReference
		Concurrency, BatchSize         int
		Check                          bool
	}{k.Work, k.ExtensionDigest, configuration, k.Reference, k.Concurrency, k.BatchSize, k.Check})
}

func memoryPlatform(target ExecutionTarget, unified bool) string {
	layout := "discrete"
	if unified {
		layout = "unified"
	}
	if target.Platform == "cpu" {
		layout = "host"
	}
	return target.OS + "/" + target.Platform + "/" + layout
}

func appendProjectedMemoryPeaks(out []memoryEvidencePoint, key memoryEvidenceKey, p memoryEvidencePoint, peak *AttemptMemoryPeak) []memoryEvidencePoint {
	key.Pool, p.Peak = "ram", max(memoryValue(peak.HostGB), memoryValue(peak.CombinedHostGB))
	if validRate(p.Peak) {
		p.Key = key
		out = append(out, p)
	}
	if strings.HasSuffix(key.Platform, "/discrete") {
		key.Pool, p.Peak = "card", memoryValue(peak.DeviceGB)
		if validRate(p.Peak) {
			p.Key = key
			out = append(out, p)
		}
	}
	return out
}

func memoryValue(v *float64) float64 {
	if v == nil || !validMemoryValue(*v) {
		return 0
	}
	return *v
}
