package scheduler

import (
	"sort"

	"mycelium/internal/domain"
)

type scoredCandidate struct {
	candidate candidate
	score     int
	parts     map[string]any
}

func (p *Placer) scoreCandidates(job domain.Job, candidates []candidate) []scoredCandidate {
	out := make([]scoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		speed := int(c.node.SpeedClass.TokensPerSecRef)
		fitTightness := c.node.Accelerators[0].VRAMTotalMB / 1024
		score := speed
		if effectiveSpeed(job.SpeedPref) == domain.SpeedLatency {
			score += speed * 10
		} else {
			score -= fitTightness
		}
		out = append(out, scoredCandidate{
			candidate: c,
			score:     score,
			parts: map[string]any{
				"speed":         speed,
				"fit_tightness": fitTightness,
				"speed_pref":    effectiveSpeed(job.SpeedPref),
			},
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		if out[i].candidate.node.ID != out[j].candidate.node.ID {
			return out[i].candidate.node.ID < out[j].candidate.node.ID
		}
		return out[i].candidate.acc[0] < out[j].candidate.acc[0]
	})
	return out
}
