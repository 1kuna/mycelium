package sharding

import (
	"context"
	"fmt"
	"math"
	"sort"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Planner struct {
	Estimator ports.ResourceEstimator
	Allocator ports.Allocator
}

func (p Planner) Plan(ctx context.Context, preset domain.Preset, fleet domain.FleetSnapshot, shards int, contextLen int) (domain.ShardPlan, error) {
	if err := ctx.Err(); err != nil {
		return domain.ShardPlan{}, err
	}
	if p.Estimator == nil || p.Allocator == nil {
		return domain.ShardPlan{}, fmt.Errorf("shard planner is not configured")
	}
	if preset.ID == "" {
		return domain.ShardPlan{}, fmt.Errorf("preset id is required")
	}
	if shards < 2 {
		return domain.ShardPlan{}, fmt.Errorf("shards must be at least 2")
	}
	if contextLen == 0 {
		contextLen = preset.ContextLength
	}
	fullClaim, err := p.Estimator.Estimate(ctx, preset, contextLen, 1)
	if err != nil {
		return domain.ShardPlan{}, err
	}
	claim := splitClaim(fullClaim, shards)
	candidates := p.candidates(fleet, claim)
	if len(candidates) < shards {
		return domain.ShardPlan{
			ID:         planID(preset, shards),
			PresetID:   preset.ID,
			ModelRef:   preset.ModelRef,
			Backend:    preset.Backend,
			ContextLen: contextLen,
			Trace: []domain.TraceStep{{
				Step:   "fit",
				Result: fmt.Sprintf("only %d shard candidates fit %d requested shards", len(candidates), shards),
			}},
		}, domain.ErrNoFit
	}
	assignments := make([]domain.ShardAssignment, 0, shards)
	traceNodes := make([]string, 0, shards)
	for rank, candidate := range candidates[:shards] {
		assignments = append(assignments, domain.ShardAssignment{
			Rank:           rank,
			NodeID:         candidate.node.ID,
			AcceleratorSet: append([]int(nil), candidate.acc...),
			Claim:          claim,
		})
		traceNodes = append(traceNodes, candidate.node.ID)
	}
	return domain.ShardPlan{
		ID:          planID(preset, shards),
		PresetID:    preset.ID,
		ModelRef:    preset.ModelRef,
		Backend:     preset.Backend,
		ContextLen:  contextLen,
		Assignments: assignments,
		Trace: []domain.TraceStep{{
			Step:   "select",
			Result: "selected fastest fitting distinct nodes",
			Data:   map[string]any{"nodes": traceNodes, "shard_claim": claim},
		}},
	}, nil
}

type candidate struct {
	node domain.Node
	acc  []int
}

func (p Planner) candidates(fleet domain.FleetSnapshot, claim domain.Claim) []candidate {
	existing := instancesByNode(fleet.Instances)
	out := make([]candidate, 0, len(fleet.Nodes))
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady || len(node.Accelerators) == 0 {
			continue
		}
		acc := []int{node.Accelerators[0].Index}
		if p.Allocator.Fits(node, acc, existing[node.ID], claim) {
			out = append(out, candidate{node: node, acc: acc})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].node.SpeedClass.TokensPerSecRef != out[j].node.SpeedClass.TokensPerSecRef {
			return out[i].node.SpeedClass.TokensPerSecRef > out[j].node.SpeedClass.TokensPerSecRef
		}
		return out[i].node.ID < out[j].node.ID
	})
	return out
}

func splitClaim(claim domain.Claim, shards int) domain.Claim {
	return domain.Claim{
		WeightsMB:    int(math.Ceil(float64(claim.WeightsMB) / float64(shards))),
		KVReservedMB: int(math.Ceil(float64(claim.KVReservedMB) / float64(shards))),
	}
}

func instancesByNode(instances []domain.ModelInstance) map[string][]domain.ModelInstance {
	out := map[string][]domain.ModelInstance{}
	for _, inst := range instances {
		out[inst.NodeID] = append(out[inst.NodeID], inst)
	}
	return out
}

func planID(preset domain.Preset, shards int) string {
	return fmt.Sprintf("shard-%s-%d", preset.ID, shards)
}

var _ ports.ShardPlanner = Planner{}
