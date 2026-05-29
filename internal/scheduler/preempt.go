package scheduler

import (
	"fmt"
	"sort"

	"mycelium/internal/domain"
)

type preemptResult struct {
	candidate candidate
	victim    domain.ModelInstance
	requeued  []string
	trace     []domain.TraceStep
}

func (p *Placer) tryPreempt(job domain.Job, fleet domain.FleetSnapshot, claim domain.Claim) (preemptResult, bool) {
	if !hardPreemptionAllowed(job) {
		return preemptResult{trace: []domain.TraceStep{{Step: "preempt", Result: "hard preemption not allowed"}}}, false
	}

	var results []preemptResult
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady {
			continue
		}
		for _, acc := range node.Accelerators {
			accSet := []int{acc.Index}
			victims := eligibleVictims(job, node, accSet, fleet.Instances)
			for _, victim := range victims {
				remaining := removeInstance(fleet.Instances, victim.ID)
				if !p.allocator.Fits(node, accSet, remaining, claim) {
					continue
				}
				result := preemptResult{
					candidate: candidate{node: node, acc: accSet},
					victim:    victim,
					trace: []domain.TraceStep{{
						Step:   "preempt",
						Result: fmt.Sprintf("victim=%s target=%s", victim.ID, node.ID),
					}},
				}
				if !p.canReplaceVictim(victim, node.ID, fleet, remaining) {
					result.requeued = []string{victim.ID}
					result.trace = append(result.trace, domain.TraceStep{Step: "replace", Result: fmt.Sprintf("requeued=%s", victim.ID)})
				} else {
					result.trace = append(result.trace, domain.TraceStep{Step: "replace", Result: fmt.Sprintf("replaced=%s", victim.ID)})
				}
				results = append(results, result)
				break
			}
		}
	}
	if len(results) == 0 {
		return preemptResult{}, false
	}
	sort.Slice(results, func(i, j int) bool {
		return victimLess(results[i].victim, results[j].victim)
	})
	return results[0], true
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
		for _, acc := range node.Accelerators {
			if p.allocator.Fits(node, []int{acc.Index}, instances, victim.Claim) {
				return true
			}
		}
	}
	return false
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
