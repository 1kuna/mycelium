package scheduler

import (
	"context"
	"fmt"
	"sort"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type candidate struct {
	node  domain.Node
	acc   []int
	claim domain.Claim
}

func (c candidate) name() string {
	return fmt.Sprintf("%s:%v", c.node.ID, c.acc)
}

func (p *Placer) filterCandidates(fleet domain.FleetSnapshot, claim domain.Claim, requireEmpty ...bool) ([]candidate, domain.TraceStep) {
	var candidates []candidate
	dropped := map[string]string{}
	dedicated := len(requireEmpty) > 0 && requireEmpty[0]
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady {
			dropped[node.ID] = string(node.Status)
			continue
		}
		nodeFit := false
		for _, accSet := range acceleratorSets(node) {
			if dedicated && hasOverlappingInstance(node.ID, accSet, fleet.Instances) {
				dropped[node.ID] = "dedicated_unit"
				continue
			}
			if !p.allocator.CanStackLoad(node, accSet, fleet.Instances) {
				dropped[node.ID] = "load_in_flight"
				continue
			}
			if p.allocator.Fits(node, accSet, fleet.Instances, claim) {
				candidates = append(candidates, candidate{node: node, acc: accSet, claim: claim})
				nodeFit = true
			}
		}
		if !nodeFit {
			if _, exists := dropped[node.ID]; !exists {
				dropped[node.ID] = "fit"
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].node.ID != candidates[j].node.ID {
			return candidates[i].node.ID < candidates[j].node.ID
		}
		return acceleratorSetLess(candidates[i].acc, candidates[j].acc)
	})
	return candidates, domain.TraceStep{
		Step: "filter",
		Data: map[string]any{"kept": candidateNames(candidates), "dropped": dropped},
	}
}

func acceleratorSets(node domain.Node) [][]int {
	accs := make([]int, 0, len(node.Accelerators))
	for _, acc := range node.Accelerators {
		accs = append(accs, acc.Index)
	}
	sort.Ints(accs)
	var sets [][]int
	for mask := 1; mask < 1<<len(accs); mask++ {
		set := make([]int, 0, len(accs))
		for i, acc := range accs {
			if mask&(1<<i) != 0 {
				set = append(set, acc)
			}
		}
		sets = append(sets, set)
	}
	sort.Slice(sets, func(i, j int) bool {
		return acceleratorSetLess(sets[i], sets[j])
	})
	return sets
}

func acceleratorSetLess(left, right []int) bool {
	if len(left) != len(right) {
		return len(left) < len(right)
	}
	for i := range left {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return false
}

func hasOverlappingInstance(nodeID string, acc []int, instances []domain.ModelInstance) bool {
	for _, inst := range instances {
		if inst.NodeID == nodeID && overlaps(inst.AcceleratorSet, acc) {
			return true
		}
	}
	return false
}

func (p *Placer) filterPlacementCandidates(ctx context.Context, job domain.Job, preset domain.Preset, contextLen int, fleet domain.FleetSnapshot, requireEmpty ...bool) ([]candidate, domain.TraceStep, error) {
	var candidates []candidate
	dropped := map[string]string{}
	dedicated := len(requireEmpty) > 0 && requireEmpty[0]
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady {
			dropped[node.ID] = string(node.Status)
			continue
		}
		if reason, ok := nodeSelectorMismatch(job.NodeSelector, node); ok {
			dropped[node.ID] = reason
			continue
		}
		nodeFit := false
		for _, accSet := range acceleratorSets(node) {
			if dedicated && hasOverlappingInstance(node.ID, accSet, fleet.Instances) {
				dropped[node.ID] = "dedicated_unit"
				continue
			}
			if !p.allocator.CanStackLoad(node, accSet, fleet.Instances) {
				dropped[node.ID] = "load_in_flight"
				continue
			}
			claim, err := p.estimateCandidateClaim(ctx, preset, contextLen, 1, node, accSet)
			if err != nil {
				return nil, domain.TraceStep{}, err
			}
			if p.allocator.Fits(node, accSet, fleet.Instances, claim) {
				candidates = append(candidates, candidate{node: node, acc: accSet, claim: claim})
				nodeFit = true
			}
		}
		if !nodeFit {
			if _, exists := dropped[node.ID]; !exists {
				dropped[node.ID] = "fit"
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].node.ID != candidates[j].node.ID {
			return candidates[i].node.ID < candidates[j].node.ID
		}
		return acceleratorSetLess(candidates[i].acc, candidates[j].acc)
	})
	return candidates, domain.TraceStep{
		Step: "filter",
		Data: map[string]any{"kept": candidateNames(candidates), "dropped": dropped},
	}, nil
}

func (p *Placer) estimateCandidateClaim(ctx context.Context, preset domain.Preset, contextLen, concurrency int, node domain.Node, acc []int) (domain.Claim, error) {
	if unitEstimator, ok := p.estimator.(ports.UnitResourceEstimator); ok {
		return unitEstimator.EstimateForUnit(ctx, preset, contextLen, concurrency, node, acc)
	}
	return p.estimator.Estimate(ctx, preset, contextLen, concurrency)
}

func nodeSelectorMismatch(selector map[string]string, node domain.Node) (string, bool) {
	for key, want := range selector {
		if node.Labels[key] != want {
			return "label." + key, true
		}
	}
	return "", false
}

func candidateNames(candidates []candidate) []string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.name())
	}
	return out
}
