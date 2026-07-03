package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/modelruntime"
	"mycelium/internal/ports"
)

type Placer struct {
	estimator ports.ResourceEstimator
	allocator ports.Allocator
	clock     ports.Clock
	presets   map[string]domain.Preset
	byModel   map[string][]domain.Preset
}

func NewPlacer(estimator ports.ResourceEstimator, allocator ports.Allocator, clock ports.Clock, presets ...domain.Preset) *Placer {
	p := &Placer{
		estimator: estimator,
		allocator: allocator,
		clock:     clock,
		presets:   map[string]domain.Preset{},
		byModel:   map[string][]domain.Preset{},
	}
	for _, preset := range presets {
		p.presets[preset.ID] = preset
		for _, model := range append([]string{preset.ModelRef}, preset.Aliases...) {
			if model == "" {
				continue
			}
			p.byModel[model] = append(p.byModel[model], preset)
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

	presets, err := p.resolvePresetCandidates(job)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "resolve",
			Result: err.Error(),
		}}}, err
	}

	if len(presets) == 1 {
		return p.placePreset(ctx, job, presets[0], fleet)
	}

	var best *placementCandidateDecision
	var queued *domain.PlacementDecision
	var rejected []string
	for _, preset := range presets {
		decision, score, err := p.placePresetScored(ctx, job, preset, fleet)
		if err != nil {
			rejected = append(rejected, fmt.Sprintf("%s:%s", preset.ID, err.Error()))
			continue
		}
		if decision.Action == domain.ActionQueued {
			if queued == nil {
				copy := decision
				queued = &copy
			}
			continue
		}
		candidate := placementCandidateDecision{decision: decision, score: score}
		if best == nil || candidate.betterThan(*best, fleet) {
			best = &candidate
		}
	}
	if best != nil {
		best.decision.Trace = prependModelGroupTrace(job, presets, best.decision.Trace, rejected)
		return best.decision, nil
	}
	if queued != nil {
		queued.Trace = prependModelGroupTrace(job, presets, queued.Trace, rejected)
		return *queued, nil
	}
	err = fmt.Errorf("no preset for model %q can satisfy job: %s", job.Model, strings.Join(rejected, "; "))
	return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
		Step:   "resolve",
		Result: err.Error(),
		Data:   map[string]any{"model_group": presetIDs(presets), "rejected": rejected},
	}}}, err
}

type placementCandidateDecision struct {
	decision domain.PlacementDecision
	score    int
}

func (d placementCandidateDecision) betterThan(other placementCandidateDecision, fleet domain.FleetSnapshot) bool {
	if actionRank(d.decision.Action) != actionRank(other.decision.Action) {
		return actionRank(d.decision.Action) > actionRank(other.decision.Action)
	}
	if d.score != other.score {
		return d.score > other.score
	}
	if nodeSpeed(d.decision.NodeID, fleet) != nodeSpeed(other.decision.NodeID, fleet) {
		return nodeSpeed(d.decision.NodeID, fleet) > nodeSpeed(other.decision.NodeID, fleet)
	}
	if d.decision.Preset.ContextLength != other.decision.Preset.ContextLength {
		return d.decision.Preset.ContextLength < other.decision.Preset.ContextLength
	}
	if d.decision.NodeID != other.decision.NodeID {
		return d.decision.NodeID < other.decision.NodeID
	}
	return d.decision.Preset.ID < other.decision.Preset.ID
}

func actionRank(action domain.PlacementAction) int {
	switch action {
	case domain.ActionWarmInstance:
		return 40
	case domain.ActionLoadedNew, domain.ActionDedicatedUnit:
		return 30
	case domain.ActionHardPreempted:
		return 20
	default:
		return 0
	}
}

func nodeSpeed(nodeID string, fleet domain.FleetSnapshot) float64 {
	for _, node := range fleet.Nodes {
		if node.ID == nodeID {
			return node.SpeedClass.TokensPerSecRef
		}
	}
	return 0
}

func prependModelGroupTrace(job domain.Job, presets []domain.Preset, trace []domain.TraceStep, rejected []string) []domain.TraceStep {
	data := map[string]any{"model": job.Model, "candidate_presets": presetIDs(presets)}
	if len(rejected) > 0 {
		data["rejected_presets"] = append([]string(nil), rejected...)
	}
	out := []domain.TraceStep{{
		Step:   "resolve",
		Result: "model group",
		Data:   data,
	}}
	return append(out, trace...)
}

func presetIDs(presets []domain.Preset) []string {
	ids := make([]string, 0, len(presets))
	for _, preset := range presets {
		ids = append(ids, preset.ID)
	}
	return ids
}

func (p *Placer) placePreset(ctx context.Context, job domain.Job, preset domain.Preset, fleet domain.FleetSnapshot) (domain.PlacementDecision, error) {
	decision, _, err := p.placePresetScored(ctx, job, preset, fleet)
	return decision, err
}

func (p *Placer) placePresetScored(ctx context.Context, job domain.Job, preset domain.Preset, fleet domain.FleetSnapshot) (domain.PlacementDecision, int, error) {
	if err := validatePresetForJob(job, preset); err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "filter",
			Result: err.Error(),
		}}}, 0, err
	}

	contextLen := job.ContextRequest
	if contextLen == 0 {
		contextLen = modelruntime.EffectiveContextLength(preset)
	}
	if cap := modelruntime.EffectiveContextLength(preset); cap > 0 && contextLen > cap {
		err := fmt.Errorf("preset %q context cap %d is below requested context %d", preset.ID, cap, contextLen)
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: []domain.TraceStep{{
			Step:   "filter",
			Result: err.Error(),
		}}}, 0, err
	}
	launchPreset := preset
	launchPreset.ContextLength = contextLen
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
			}}}, 0, err
		}
		fallbackClaim = claim
		trace[0].Result = fmt.Sprintf("weights=%dMB kv=%dMB @ctx%d x%d", claim.WeightsMB, claim.KVReservedMB, contextLen, concurrency)
	}

	if effectiveSpeed(job.SpeedPref) == domain.SpeedThroughput && !job.ForceNewInstance {
		if warm, ok, err := p.selectWarmInstance(ctx, job, preset, contextLen, concurrency, fleet); err != nil {
			return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
				Step:   "estimate",
				Result: err.Error(),
			})}, 0, err
		} else if ok {
			return warmDecision(job, launchPreset, warm, trace, "warm compatible instance available"), warmScore(warm, fleet), nil
		}
	}

	candidates, filterTrace, err := p.filterPlacementCandidates(ctx, job, preset, contextLen, concurrency, fleet, effectiveSpeed(job.SpeedPref) == domain.SpeedLatency)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
			Step:   "estimate",
			Result: err.Error(),
		})}, 0, err
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
			Preset:           launchPreset,
			NodeID:           winner.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), winner.candidate.acc...),
			Claim:            winner.candidate.claim,
			Action:           actionForSpeed(job.SpeedPref),
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
		}, winner.score, nil
	}

	if effectiveSpeed(job.SpeedPref) == domain.SpeedAuto && !job.ForceNewInstance {
		if warm, ok, err := p.selectWarmInstance(ctx, job, preset, contextLen, concurrency, fleet); err != nil {
			return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
				Step:   "estimate",
				Result: err.Error(),
			})}, 0, err
		} else if ok {
			return warmDecision(job, launchPreset, warm, trace, "warm compatible instance available after cold no-fit"), warmScore(warm, fleet), nil
		}
	}

	preempted, ok, err := p.tryPreemptForPreset(ctx, job, preset, contextLen, concurrency, fleet)
	if err != nil {
		return domain.PlacementDecision{JobID: job.ID, SpeedPrefApplied: job.SpeedPref, Trace: append(trace, domain.TraceStep{
			Step:   "estimate",
			Result: err.Error(),
		})}, 0, err
	}
	if ok {
		trace = append(trace, preempted.trace...)
		return domain.PlacementDecision{
			JobID:            job.ID,
			Preset:           launchPreset,
			NodeID:           preempted.candidate.node.ID,
			AcceleratorSet:   append([]int(nil), preempted.candidate.acc...),
			Claim:            preempted.claim,
			Action:           domain.ActionHardPreempted,
			SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
			Trace:            trace,
			Preempted:        instanceIDs(preempted.victims),
			Requeued:         preempted.requeued,
			Replacements:     preempted.replaced,
		}, 0, nil
	}

	trace = append(trace, domain.TraceStep{Step: "admit", Result: "queued: no fit"})
	return domain.PlacementDecision{
		JobID:            job.ID,
		Preset:           launchPreset,
		Claim:            fallbackClaim,
		Action:           domain.ActionQueued,
		SpeedPrefApplied: effectiveSpeed(job.SpeedPref),
		Trace:            trace,
	}, 0, nil
}

func warmScore(inst domain.ModelInstance, fleet domain.FleetSnapshot) int {
	score := int(nodeSpeed(inst.NodeID, fleet)) * 100
	score -= inst.InFlight
	return score
}

func (p *Placer) resolvePreset(job domain.Job) (domain.Preset, error) {
	presets, err := p.resolvePresetCandidates(job)
	if err != nil {
		return domain.Preset{}, err
	}
	return presets[0], nil
}

func (p *Placer) resolvePresetCandidates(job domain.Job) ([]domain.Preset, error) {
	if job.PresetID != "" {
		preset, ok := p.presets[job.PresetID]
		if !ok {
			return nil, fmt.Errorf("unknown preset %q", job.PresetID)
		}
		return []domain.Preset{preset}, nil
	}
	if job.Model == "" {
		return nil, fmt.Errorf("job %q has no model or preset", job.ID)
	}
	if preset, ok := p.presets[job.Model]; ok && preset.ID == job.Model {
		return []domain.Preset{preset}, nil
	}
	presets, ok := p.byModel[job.Model]
	if !ok || len(presets) == 0 {
		return nil, fmt.Errorf("unknown model %q", job.Model)
	}
	return append([]domain.Preset(nil), presets...), nil
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
				if !p.allocator.Fits(node, inst.AcceleratorSet, fleet.Instances, claim) {
					continue
				}
				inst.Claim = claim
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
