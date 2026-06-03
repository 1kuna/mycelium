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
		for _, model := range append([]string{preset.ModelRef}, preset.Aliases...) {
			if model == "" {
				continue
			}
			if _, exists := p.presets[model]; !exists {
				p.presets[model] = preset
			}
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
	if err := validatePresetForJob(job, preset); err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "filter",
			Result: err.Error(),
		}}}, err
	}

	contextLen := job.ContextRequest
	if contextLen == 0 {
		contextLen = preset.ContextLength
	}
	concurrency := expectedConcurrency(job)

	trace := []domain.TraceStep{{
		Step:   "estimate",
		Result: fmt.Sprintf("backend-aware @ctx%d x%d", contextLen, concurrency),
	}}
	var fallbackClaim domain.Claim
	if _, ok := p.estimator.(ports.UnitResourceEstimator); !ok {
		claim, err := p.estimator.Estimate(ctx, preset, contextLen, concurrency)
		if err != nil {
			return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
				Step:   "estimate",
				Result: err.Error(),
			}}}, err
		}
		fallbackClaim = claim
		trace[0].Result = fmt.Sprintf("weights=%dMB kv=%dMB @ctx%d x%d", claim.WeightsMB, claim.KVReservedMB, contextLen, concurrency)
	}

	if effectiveSpeed(job.SpeedPref) == domain.SpeedThroughput {
		if warm, ok, err := p.selectWarmInstance(ctx, job, preset, contextLen, concurrency, fleet); err != nil {
			return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
				Step:   "estimate",
				Result: err.Error(),
			})}, err
		} else if ok {
			return warmDecision(job, preset, warm, trace, "warm compatible instance available"), nil
		}
	}

	candidates, filterTrace, err := p.filterPlacementCandidates(ctx, job, preset, contextLen, concurrency, fleet, effectiveSpeed(job.SpeedPref) == domain.SpeedLatency)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
			Step:   "estimate",
			Result: err.Error(),
		})}, err
	}
	trace = append(trace, filterTrace)
	if len(candidates) > 0 {
		scored := p.scoreCandidates(job, preset, candidates)
		winner := scored[0]
		trace = append(trace,
			domain.TraceStep{Step: "select", Data: map[string]any{"candidates": candidateNames(candidates), "speed_pref": effectiveSpeed(job.SpeedPref)}},
			domain.TraceStep{Step: "score", Result: fmt.Sprintf("winner=%s score=%d", winner.candidate.name(), winner.score), Data: winner.parts},
			domain.TraceStep{Step: "admit", Result: "loaded new instance"},
		)
		return domain.PlacementDecision{
			JobID:            job.ID,
			Preset:           preset,
			NodeID:           winner.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), winner.candidate.acc...),
			Claim:            winner.candidate.claim,
			Action:           actionForSpeed(job.SpeedPref),
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
		}, nil
	}

	if effectiveSpeed(job.SpeedPref) == domain.SpeedAuto {
		if warm, ok, err := p.selectWarmInstance(ctx, job, preset, contextLen, concurrency, fleet); err != nil {
			return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
				Step:   "estimate",
				Result: err.Error(),
			})}, err
		} else if ok {
			return warmDecision(job, preset, warm, trace, "warm compatible instance available after cold no-fit"), nil
		}
	}

	preempted, ok, err := p.tryPreemptForPreset(ctx, job, preset, contextLen, concurrency, fleet)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
			Step:   "estimate",
			Result: err.Error(),
		})}, err
	}
	if ok {
		trace = append(trace, preempted.trace...)
		return domain.PlacementDecision{
			JobID:            job.ID,
			Preset:           preset,
			NodeID:           preempted.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), preempted.candidate.acc...),
			Claim:            preempted.claim,
			Action:           domain.ActionHardPreempted,
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
			Preempted:        instanceIDs(preempted.victims),
			Requeued:         preempted.requeued,
			Replacements:     preempted.replaced,
		}, nil
	}

	trace = append(trace, domain.TraceStep{Step: "admit", Result: "queued: no fit"})
	return domain.PlacementDecision{
		JobID:            job.ID,
		Preset:           preset,
		Claim:            fallbackClaim,
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

func validatePresetForJob(job domain.Job, preset domain.Preset) error {
	if len(preset.Capabilities) == 0 {
		return fmt.Errorf("preset %q has no schedulable capabilities", preset.ID)
	}
	if job.TaskType == "" {
		return nil
	}
	for _, capability := range preset.Capabilities {
		if string(capability) == job.TaskType {
			return nil
		}
	}
	return fmt.Errorf("preset %q does not support task_type %q", preset.ID, job.TaskType)
}

func (p *Placer) selectWarmInstance(ctx context.Context, job domain.Job, preset domain.Preset, contextLen, concurrency int, fleet domain.FleetSnapshot) (domain.ModelInstance, bool, error) {
	if effectiveSpeed(job.SpeedPref) == domain.SpeedLatency {
		return domain.ModelInstance{}, false, nil
	}
	ready := readyNodesByID(fleet.Nodes)
	var matches []domain.ModelInstance
	for _, inst := range fleet.Instances {
		if inst.PresetID == preset.ID && inst.State == domain.InstReady {
			if node, ok := ready[inst.NodeID]; ok {
				if _, mismatch := nodeSelectorMismatch(job.NodeSelector, node); mismatch {
					continue
				}
				if _, mismatch := presetNodeMismatch(preset, node); mismatch {
					continue
				}
				if _, mismatch := presetBackendMismatch(preset, node); mismatch {
					continue
				}
				if _, drop := nodeDiskDropReason(preset, node, fleet); drop {
					continue
				}
				claim, err := p.estimateCandidateClaim(ctx, preset, contextLen, concurrency, node, inst.AcceleratorSet)
				if err != nil {
					return domain.ModelInstance{}, false, err
				}
				claim.WeightsMB = 0
				if !p.allocator.CanStackLoad(node, inst.AcceleratorSet, fleet.Instances) || !p.allocator.Fits(node, inst.AcceleratorSet, fleet.Instances, claim) {
					continue
				}
				matches = append(matches, inst)
			}
		}
	}
	if len(matches) == 0 {
		return domain.ModelInstance{}, false, nil
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].InFlight != matches[j].InFlight {
			return matches[i].InFlight < matches[j].InFlight
		}
		return matches[i].ID < matches[j].ID
	})
	return matches[0], true, nil
}

func warmDecision(job domain.Job, preset domain.Preset, warm domain.ModelInstance, trace []domain.TraceStep, filterResult string) domain.PlacementDecision {
	trace = append(trace,
		domain.TraceStep{Step: "filter", Result: filterResult},
		domain.TraceStep{Step: "select", Result: "warm instance selected"},
		domain.TraceStep{Step: "score", Result: "warm instance locality"},
		domain.TraceStep{Step: "admit", Result: "batched onto warm instance"},
	)
	return domain.PlacementDecision{
		JobID:            job.ID,
		Preset:           preset,
		InstanceID:       warm.ID,
		NodeID:           warm.NodeID,
		AcceleratorSet:   append([]int(nil), warm.AcceleratorSet...),
		Claim:            warm.Claim,
		Action:           domain.ActionWarmInstance,
		SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
		Trace:            trace,
	}
}

func effectiveSpeed(speed domain.SpeedPref) domain.SpeedPref {
	if speed == "" {
		return domain.SpeedThroughput
	}
	return speed
}

func expectedConcurrency(job domain.Job) int {
	if job.ExpectedConcurrency <= 0 {
		return 1
	}
	return job.ExpectedConcurrency
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
