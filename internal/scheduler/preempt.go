package scheduler

import (
	"context"
	"fmt"
	"sort"

	"mycelium/internal/domain"
)

type preemptResult struct {
	candidate candidate
	claim     domain.Claim
	victim    domain.ModelInstance
	victims   []domain.ModelInstance
	requeued  []string
	trace     []domain.TraceStep
}

func (p *Placer) tryPreempt(job domain.Job, fleet domain.FleetSnapshot, claim domain.Claim) (preemptResult, bool) {
	result, ok, _ := p.tryPreemptWithClaims(job, fleet, func(domain.Node, []int) (domain.Claim, error) {
		return claim, nil
	})
	return result, ok
}

func (p *Placer) tryPreemptForPreset(ctx context.Context, job domain.Job, preset domain.Preset, contextLen, concurrency int, fleet domain.FleetSnapshot) (preemptResult, bool, error) {
	return p.tryPreemptWithClaims(job, fleet, func(node domain.Node, accSet []int) (domain.Claim, error) {
		return p.estimateCandidateClaim(ctx, preset, contextLen, concurrency, node, accSet)
	}, func(node domain.Node) bool {
		if _, drop := presetNodeMismatch(preset, node); drop {
			return false
		}
		if _, drop := presetBackendMismatch(preset, node); drop {
			return false
		}
		_, drop := nodeDiskDropReason(preset, node, fleet)
		return !drop
	})
}

func (p *Placer) tryPreemptWithClaims(job domain.Job, fleet domain.FleetSnapshot, claimFor func(domain.Node, []int) (domain.Claim, error), nodeGuards ...func(domain.Node) bool) (preemptResult, bool, error) {
	if !hardPreemptionAllowed(job) {
		return preemptResult{trace: []domain.TraceStep{{Step: "preempt", Result: "hard preemption not allowed"}}}, false, nil
	}

	var results []preemptResult
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady {
			continue
		}
		if !preemptNodeAllowed(node, nodeGuards) {
			continue
		}
		for _, accSet := range acceleratorSets(node) {
			if _, ok := nodeSelectorMismatch(job.NodeSelector, node); ok {
				continue
			}
			claim, err := claimFor(node, accSet)
			if err != nil {
				return preemptResult{}, false, err
			}
			victims := eligibleVictims(job, node, accSet, fleet.Instances)
			remaining := append([]domain.ModelInstance(nil), fleet.Instances...)
			var selected []domain.ModelInstance
			for _, victim := range victims {
				remaining = removeInstance(remaining, victim.ID)
				selected = append(selected, victim)
				if !p.allocator.Fits(node, accSet, remaining, claim) {
					continue
				}
				result := preemptResult{
					candidate: candidate{node: node, acc: accSet},
					claim:     claim,
					victim:    victim,
					victims:   append([]domain.ModelInstance(nil), selected...),
					trace: []domain.TraceStep{{
						Step:   "preempt",
						Result: fmt.Sprintf("victims=%v target=%s", instanceIDs(selected), node.ID),
					}},
				}
				for _, selectedVictim := range selected {
					if !p.canReplaceVictim(selectedVictim, node.ID, fleet, remaining) {
						result.requeued = append(result.requeued, selectedVictim.ID)
					}
				}
				if len(result.requeued) == 0 {
					result.trace = append(result.trace, domain.TraceStep{Step: "replace", Result: fmt.Sprintf("replaced=%v", instanceIDs(selected))})
				} else {
					result.trace = append(result.trace, domain.TraceStep{Step: "replace", Result: fmt.Sprintf("requeued=%v", result.requeued)})
				}
				results = append(results, result)
				break
			}
		}
	}
	if len(results) == 0 {
		return preemptResult{}, false, nil
	}
	sort.Slice(results, func(i, j int) bool {
		return victimLess(results[i].victim, results[j].victim)
	})
	return results[0], true, nil
}

func hardPreemptionAllowed(job domain.Job) bool {
	switch job.Preemption {
	case domain.PreemptHard:
		return true
	case domain.PreemptHardForInteractive:
		return job.Priority == domain.PriorityInteractive
	default:
		return false
	}
}

func eligibleVictims(job domain.Job, node domain.Node, acc []int, instances []domain.ModelInstance) []domain.ModelInstance {
	var victims []domain.ModelInstance
	for _, inst := range instances {
		if inst.NodeID != node.ID || !overlaps(inst.AcceleratorSet, acc) {
			continue
		}
		if inst.Pinned {
			continue
		}
		if priorityRank(inst.Priority) < priorityRank(job.Priority) {
			victims = append(victims, inst)
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		return victimLess(victims[i], victims[j])
	})
	return victims
}

func victimLess(left, right domain.ModelInstance) bool {
	if priorityRank(left.Priority) != priorityRank(right.Priority) {
		return priorityRank(left.Priority) < priorityRank(right.Priority)
	}
	if left.InFlight != right.InFlight {
		return left.InFlight < right.InFlight
	}
	return left.ID < right.ID
}

func (p *Placer) canReplaceVictim(victim domain.ModelInstance, originalNodeID string, fleet domain.FleetSnapshot, instances []domain.ModelInstance) bool {
	for _, node := range fleet.Nodes {
		if node.ID == originalNodeID || node.Status != domain.NodeReady {
			continue
		}
		if preset, ok := p.presets[victim.PresetID]; ok {
			if _, drop := presetNodeMismatch(preset, node); drop {
				continue
			}
			if _, drop := presetBackendMismatch(preset, node); drop {
				continue
			}
			if _, drop := nodeDiskDropReason(preset, node, fleet); drop {
				continue
			}
		}
		for _, acc := range node.Accelerators {
			if p.allocator.Fits(node, []int{acc.Index}, instances, victim.Claim) {
				return true
			}
		}
	}
	return false
}

func preemptNodeAllowed(node domain.Node, guards []func(domain.Node) bool) bool {
	for _, guard := range guards {
		if guard != nil && !guard(node) {
			return false
		}
	}
	return true
}

func removeInstance(instances []domain.ModelInstance, id string) []domain.ModelInstance {
	out := make([]domain.ModelInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.ID != id {
			out = append(out, inst)
		}
	}
	return out
}

func instanceIDs(instances []domain.ModelInstance) []string {
	out := make([]string, 0, len(instances))
	for _, inst := range instances {
		out = append(out, inst.ID)
	}
	return out
}

func priorityRank(p domain.Priority) int {
	switch p {
	case domain.PriorityBackground:
		return 1
	case domain.PriorityNormal:
		return 2
	case domain.PriorityInteractive:
		return 3
	default:
		return 2
	}
}

func overlaps(left, right []int) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := make(map[int]struct{}, len(left))
	for _, v := range left {
		seen[v] = struct{}{}
	}
	for _, v := range right {
		if _, ok := seen[v]; ok {
			return true
		}
	}
	return false
}
