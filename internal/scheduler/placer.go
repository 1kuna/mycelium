package scheduler

import (
	"context"
	"fmt"
	"sort"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Placer struct {
	estimator ports.ResourceEstimator
	allocator ports.Allocator
	clock     ports.Clock
	presets   map[string]domain.Preset
}

func NewPlacer(estimator ports.ResourceEstimator, allocator ports.Allocator, clock ports.Clock, presets ...domain.Preset) *Placer {
	p := &Placer{
		estimator: estimator,
		allocator: allocator,
		clock:     clock,
		presets:   map[string]domain.Preset{},
	}
	for _, preset := range presets {
		p.presets[preset.ID] = preset
		if _, exists := p.presets[preset.ModelRef]; !exists {
			p.presets[preset.ModelRef] = preset
		}
	}
	return p
}

func (p *Placer) Place(ctx context.Context, job domain.Job, fleet domain.FleetSnapshot) (domain.PlacementDecision, error) {
	if err := ctx.Err(); err != nil {
		return domain.PlacementDecision{}, err
	}

	preset, err := p.resolvePreset(job)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "resolve",
			Result: err.Error(),
		}}}, err
	}

	contextLen := job.ContextRequest
	if contextLen == 0 {
		contextLen = preset.ContextLength
	}
	claim, err := p.estimator.Estimate(ctx, preset, contextLen, 1)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "estimate",
			Result: err.Error(),
		}}}, err
	}

	trace := []domain.TraceStep{{
		Step:   "estimate",
		Result: fmt.Sprintf("weights=%dMB kv=%dMB @ctx%d", claim.WeightsMB, claim.KVReservedMB, contextLen),
	}}

	if warm, ok := p.selectWarmInstance(job, preset, fleet); ok {
		trace = append(trace,
			domain.TraceStep{Step: "filter", Result: "warm compatible instance available"},
			domain.TraceStep{Step: "select", Result: "warm instance selected"},
			domain.TraceStep{Step: "score", Result: "warm instance locality"},
			domain.TraceStep{Step: "admit", Result: "batched onto warm instance"},
		)
		return domain.PlacementDecision{
			JobID:            job.ID,
			InstanceID:       warm.ID,
			NodeID:           warm.NodeID,
			AcceleratorSet:   append([]int(nil), warm.AcceleratorSet...),
			Claim:            warm.Claim,
			Action:           domain.ActionWarmInstance,
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
		}, nil
	}

	candidates, filterTrace := p.filterCandidates(fleet, claim)
	trace = append(trace, filterTrace)
	if len(candidates) > 0 {
		scored := p.scoreCandidates(job, candidates)
		winner := scored[0]
		trace = append(trace,
			domain.TraceStep{Step: "select", Data: map[string]any{"candidates": candidateNames(candidates), "speed_pref": effectiveSpeed(job.SpeedPref)}},
			domain.TraceStep{Step: "score", Result: fmt.Sprintf("winner=%s score=%d", winner.candidate.name(), winner.score), Data: winner.parts},
			domain.TraceStep{Step: "admit", Result: "loaded new instance"},
		)
		return domain.PlacementDecision{
			JobID:            job.ID,
			NodeID:           winner.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), winner.candidate.acc...),
			Claim:            claim,
			Action:           actionForSpeed(job.SpeedPref),
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
		}, nil
	}

	preempted, ok := p.tryPreempt(job, fleet, claim)
	if ok {
		trace = append(trace, preempted.trace...)
		return domain.PlacementDecision{
			JobID:            job.ID,
			NodeID:           preempted.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), preempted.candidate.acc...),
			Claim:            claim,
			Action:           domain.ActionHardPreempted,
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
			Preempted:        []string{preempted.victim.ID},
			Requeued:         preempted.requeued,
		}, nil
	}

	trace = append(trace, domain.TraceStep{Step: "admit", Result: "queued: no fit"})
	return domain.PlacementDecision{
		JobID:            job.ID,
		Claim:            claim,
		Action:           domain.ActionQueued,
		SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
		Trace:            trace,
	}, nil
}

func (p *Placer) resolvePreset(job domain.Job) (domain.Preset, error) {
	if job.PresetID != "" {
		preset, ok := p.presets[job.PresetID]
		if !ok {
			return domain.Preset{}, fmt.Errorf("unknown preset %q", job.PresetID)
		}
		return preset, nil
	}
	if job.Model == "" {
		return domain.Preset{}, fmt.Errorf("job %q has no model or preset", job.ID)
	}
	preset, ok := p.presets[job.Model]
	if !ok {
		return domain.Preset{}, fmt.Errorf("unknown model %q", job.Model)
	}
	return preset, nil
}

func (p *Placer) selectWarmInstance(job domain.Job, preset domain.Preset, fleet domain.FleetSnapshot) (domain.ModelInstance, bool) {
	if effectiveSpeed(job.SpeedPref) == domain.SpeedLatency {
		return domain.ModelInstance{}, false
	}
	ready := readyNodesByID(fleet.Nodes)
	var matches []domain.ModelInstance
	for _, inst := range fleet.Instances {
		if inst.PresetID == preset.ID && inst.State == domain.InstReady {
			if _, ok := ready[inst.NodeID]; ok {
				matches = append(matches, inst)
			}
		}
	}
	if len(matches) == 0 {
		return domain.ModelInstance{}, false
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].InFlight != matches[j].InFlight {
			return matches[i].InFlight < matches[j].InFlight
		}
		return matches[i].ID < matches[j].ID
	})
	return matches[0], true
}

func effectiveSpeed(speed domain.SpeedPref) domain.SpeedPref {
	if speed == "" {
		return domain.SpeedThroughput
	}
	return speed
}

func actionForSpeed(speed domain.SpeedPref) domain.PlacementAction {
	if effectiveSpeed(speed) == domain.SpeedLatency {
		return domain.ActionDedicatedUnit
	}
	return domain.ActionLoadedNew
}

func readyNodesByID(nodes []domain.Node) map[string]domain.Node {
	out := make(map[string]domain.Node, len(nodes))
	for _, node := range nodes {
		if node.Status == domain.NodeReady {
			out[node.ID] = node
		}
	}
	return out
}

var _ ports.Placer = (*Placer)(nil)
