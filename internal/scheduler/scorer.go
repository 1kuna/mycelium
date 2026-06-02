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

func (p *Placer) scoreCandidates(job domain.Job, preset domain.Preset, candidates []candidate) []scoredCandidate {
	out := make([]scoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		speed := int(c.node.SpeedClass.TokensPerSecRef)
		unitMB := unitVRAMTotal(c.node, c.acc)
		claimMB := c.claim.WeightsMB + c.claim.KVReservedMB
		slackGB := 0
		if unitMB > claimMB {
			slackGB = (unitMB - claimMB) / 1024
		}
		locality := 0
		if preset.NodeID != "" && preset.NodeID == c.node.ID {
			locality = 500
		}
		score := speed
		switch effectiveSpeed(job.SpeedPref) {
		case domain.SpeedLatency:
			score = speed*20 + slackGB
		case domain.SpeedAuto:
			score = speed*10 + slackGB + locality
		default:
			score = speed + locality + (10000 - slackGB)
		}
		out = append(out, scoredCandidate{
			candidate: c,
			score:     score,
			parts: map[string]any{
				"speed":          speed,
				"slack_gb":       slackGB,
				"model_locality": locality > 0,
				"speed_pref":     effectiveSpeed(job.SpeedPref),
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

func unitVRAMTotal(node domain.Node, acc []int) int {
	total := 0
	for _, want := range acc {
		for _, accelerator := range node.Accelerators {
			if accelerator.Index == want {
				total += accelerator.VRAMTotalMB
				break
			}
		}
	}
	return total
}
