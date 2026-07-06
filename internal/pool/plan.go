// Package pool plans replica worker pools for a model across the fleet.
//
// The planner is pure logic over a fleet snapshot, mirroring the scheduler's
// node-compatibility rules and the allocator's safe-fit math, so a pool plan
// never promises replicas that owner admission would refuse. Refusals carry
// the exact setup gap (artifact staging, backend runtime, capacity) instead of
// a generic failure.
package pool

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

// Allocator is the safe-fit contract the planner simulates against.
// *lease.Allocator satisfies it.
type Allocator interface {
	Fits(node domain.Node, acc []int, existing []domain.ModelInstance, want domain.Claim) bool
	CanStackLoad(node domain.Node, acc []int, existing []domain.ModelInstance) bool
}

// Options bound a replica pool plan.
type Options struct {
	// ContextLength per replica; 0 uses each preset's effective context.
	ContextLength int
	// Concurrency is the expected concurrency envelope per replica; 0 means 1.
	Concurrency int
	// MaxPerNode caps additional replicas per node; 0 means no cap.
	MaxPerNode int
	// MaxTotal caps additional replicas across the fleet; 0 means no cap.
	MaxTotal int
}

// NodePlan is one node's share of a pool plan.
type NodePlan struct {
	NodeID          string       `json:"node_id"`
	NodeName        string       `json:"node_name,omitempty"`
	PresetID        string       `json:"preset_id,omitempty"`
	Backend         string       `json:"backend,omitempty"`
	ContextLength   int          `json:"context_length,omitempty"`
	Existing        int          `json:"existing_replicas"`
	Additional      int          `json:"additional_replicas"`
	ClaimPerReplica domain.Claim `json:"claim_per_replica,omitempty"`
	// SerialColdLoads is set for catastrophic-OOM nodes where replica cold
	// loads must be launched one at a time, never stacked.
	SerialColdLoads bool   `json:"serial_cold_loads,omitempty"`
	Refusal         string `json:"refusal,omitempty"`
	SetupGap        string `json:"setup_gap,omitempty"`
}

// Plan is the fleet-wide replica plan for one model.
type Plan struct {
	Model           string     `json:"model"`
	Concurrency     int        `json:"concurrency_per_replica"`
	TotalExisting   int        `json:"total_existing_replicas"`
	TotalAdditional int        `json:"total_additional_replicas"`
	Nodes           []NodePlan `json:"nodes"`
}

// BuildPlan computes the maximum additional safe replicas of a model per node.
// presets are the candidate presets already resolved for the model (one model
// may have per-node presets on a heterogeneous fleet).
func BuildPlan(ctx context.Context, estimator ports.ResourceEstimator, alloc Allocator, model string, presets []domain.Preset, fleet domain.FleetSnapshot, opts Options) (Plan, error) {
	if estimator == nil || alloc == nil {
		return Plan{}, fmt.Errorf("pool planner requires an estimator and an allocator")
	}
	if strings.TrimSpace(model) == "" {
		return Plan{}, fmt.Errorf("pool planner requires a model")
	}
	if len(presets) == 0 {
		return Plan{}, fmt.Errorf("no presets resolved for model %q", model)
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	plan := Plan{Model: model, Concurrency: concurrency}
	presetIDs := make(map[string]struct{}, len(presets))
	for _, preset := range presets {
		presetIDs[preset.ID] = struct{}{}
	}
	nodes := append([]domain.Node(nil), fleet.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	remainingTotal := opts.MaxTotal
	for _, node := range nodes {
		nodePlan := NodePlan{
			NodeID:          node.ID,
			NodeName:        node.Name,
			Existing:        existingReplicas(node.ID, presetIDs, fleet.Instances),
			SerialColdLoads: node.OOMSeverity == domain.OOMCatastrophic,
		}
		plan.TotalExisting += nodePlan.Existing
		preset, refusal, gap := compatiblePreset(node, presets)
		if refusal != "" {
			nodePlan.Refusal, nodePlan.SetupGap = refusal, gap
			plan.Nodes = append(plan.Nodes, nodePlan)
			continue
		}
		nodePlan.PresetID = preset.ID
		nodePlan.Backend = string(preset.Backend)
		contextLen := opts.ContextLength
		if contextLen <= 0 {
			contextLen = preset.ContextLength
		}
		nodePlan.ContextLength = contextLen
		if preset.ContextLength > 0 && contextLen > preset.ContextLength {
			nodePlan.Refusal = fmt.Sprintf("preset context cap %d below requested %d", preset.ContextLength, contextLen)
			nodePlan.SetupGap = "register or launch a larger-context preset on this node; a low-context route must not pose as a full-context worker"
			plan.Nodes = append(plan.Nodes, nodePlan)
			continue
		}
		claim, err := estimator.Estimate(ctx, preset, contextLen, concurrency)
		if err != nil {
			return Plan{}, fmt.Errorf("estimate claim for preset %q on node %q: %w", preset.ID, node.ID, err)
		}
		nodePlan.ClaimPerReplica = claim
		if warm := warmWorkerHeadroom(node.ID, preset.ID, fleet.Instances, concurrency); warm > 0 {
			if opts.MaxPerNode > 0 && warm > opts.MaxPerNode {
				warm = opts.MaxPerNode
			}
			if opts.MaxTotal > 0 && warm > remainingTotal {
				warm = remainingTotal
			}
			nodePlan.Additional = warm
			if opts.MaxTotal > 0 {
				remainingTotal -= warm
			}
			plan.TotalAdditional += warm
			plan.Nodes = append(plan.Nodes, nodePlan)
			continue
		}
		if hasActivePresetInstance(node.ID, preset.ID, fleet.Instances) {
			nodePlan.Refusal = "warm instance concurrency envelope is full"
			nodePlan.SetupGap = "release held work, wait for in-flight requests to finish, or launch a distinct replica through an owner path that supports another warm instance"
			plan.Nodes = append(plan.Nodes, nodePlan)
			continue
		}
		cap := opts.MaxPerNode
		if opts.MaxTotal > 0 && (cap <= 0 || remainingTotal < cap) {
			cap = remainingTotal
		}
		additional, refusal, gap := simulateReplicas(alloc, node, preset, claim, fleet.Instances, cap)
		nodePlan.Additional = additional
		if additional == 0 && refusal != "" {
			nodePlan.Refusal, nodePlan.SetupGap = refusal, gap
		}
		if opts.MaxTotal > 0 {
			remainingTotal -= additional
		}
		plan.TotalAdditional += additional
		plan.Nodes = append(plan.Nodes, nodePlan)
	}
	return plan, nil
}

// compatiblePreset picks the first preset this node can serve, mirroring the
// scheduler's hard placement rules (artifact locality, backend labels).
func compatiblePreset(node domain.Node, presets []domain.Preset) (domain.Preset, string, string) {
	if node.Status != domain.NodeReady {
		return domain.Preset{}, "node status " + string(node.Status),
			"bring the node back to ready before pooling replicas on it"
	}
	sawArtifactMismatch := false
	sawBackendMismatch := false
	for _, preset := range presets {
		if preset.NodeID != "" && preset.NodeID != node.ID {
			sawArtifactMismatch = true
			continue
		}
		if reason := backendMismatch(preset, node); reason != "" {
			sawBackendMismatch = true
			continue
		}
		return preset, "", ""
	}
	if sawBackendMismatch {
		return domain.Preset{}, "no compatible backend runtime",
			"the node does not advertise a backend for this model; verify engine readiness or register a compatible per-node preset"
	}
	if sawArtifactMismatch {
		return domain.Preset{}, "model artifact not local",
			"stage the model artifact to this node (myce models stage) or register a per-node preset"
	}
	return domain.Preset{}, "no candidate preset", "register a preset this node can serve"
}

// backendMismatch mirrors the scheduler's preset/backend label compatibility.
func backendMismatch(preset domain.Preset, node domain.Node) string {
	if preset.Backend == "" {
		return ""
	}
	if backends := node.Labels[domain.LabelPeerBackends]; backends != "" {
		for _, backend := range strings.Split(backends, ",") {
			if strings.TrimSpace(backend) == string(preset.Backend) {
				return ""
			}
		}
		return "label." + domain.LabelPeerBackends
	}
	backend := node.Labels[domain.LabelPeerBackend]
	if backend == "" || backend == string(preset.Backend) {
		return ""
	}
	return "label." + domain.LabelPeerBackend
}

// simulateReplicas adds hypothetical replicas one at a time against the real
// allocator until the node cannot safely hold another.
func simulateReplicas(alloc Allocator, node domain.Node, preset domain.Preset, claim domain.Claim, instances []domain.ModelInstance, cap int) (int, string, string) {
	simulated := capacityActiveInstances(instances)
	count := 0
	for cap <= 0 || count < cap {
		accSet, ok := firstFittingAcceleratorSet(alloc, node, simulated, claim)
		if !ok {
			break
		}
		simulated = append(simulated, domain.ModelInstance{
			ID:             fmt.Sprintf("pool-sim-%s-%d", node.ID, count),
			PresetID:       preset.ID,
			NodeID:         node.ID,
			AcceleratorSet: accSet,
			Claim:          claim,
			State:          domain.InstReady,
		})
		count++
	}
	if count == 0 {
		return 0, "insufficient safe capacity",
			"no accelerator set fits another replica under max_util and the oom-severity margin; free capacity or lower the context/concurrency envelope"
	}
	return count, "", ""
}

func capacityActiveInstances(instances []domain.ModelInstance) []domain.ModelInstance {
	active := make([]domain.ModelInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.State == domain.InstError {
			continue
		}
		active = append(active, inst)
	}
	return active
}

func warmWorkerHeadroom(nodeID, presetID string, instances []domain.ModelInstance, envelope int) int {
	if envelope <= 0 {
		envelope = 1
	}
	headroom := 0
	for _, inst := range instances {
		if inst.NodeID != nodeID || inst.PresetID != presetID {
			continue
		}
		if inst.State == domain.InstLoading || inst.State == domain.InstStopping {
			return 0
		}
		if inst.State != domain.InstReady {
			continue
		}
		available := envelope - inst.InFlight
		if available > 0 {
			headroom += available
		}
	}
	return headroom
}

func hasActivePresetInstance(nodeID, presetID string, instances []domain.ModelInstance) bool {
	for _, inst := range instances {
		if inst.NodeID != nodeID || inst.PresetID != presetID {
			continue
		}
		if inst.State == domain.InstReady || inst.State == domain.InstLoading {
			return true
		}
	}
	return false
}

// firstFittingAcceleratorSet walks accelerator subsets smallest-first so
// replicas pack one unit at a time before spanning multiple accelerators.
func firstFittingAcceleratorSet(alloc Allocator, node domain.Node, existing []domain.ModelInstance, claim domain.Claim) ([]int, bool) {
	for _, accSet := range acceleratorSets(node) {
		if alloc.Fits(node, accSet, existing, claim) {
			return accSet, true
		}
	}
	return nil, false
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
		if len(sets[i]) != len(sets[j]) {
			return len(sets[i]) < len(sets[j])
		}
		for k := range sets[i] {
			if sets[i][k] != sets[j][k] {
				return sets[i][k] < sets[j][k]
			}
		}
		return false
	})
	return sets
}

func existingReplicas(nodeID string, presetIDs map[string]struct{}, instances []domain.ModelInstance) int {
	count := 0
	for _, inst := range instances {
		if inst.NodeID != nodeID {
			continue
		}
		if _, ok := presetIDs[inst.PresetID]; !ok {
			continue
		}
		if inst.State == domain.InstReady || inst.State == domain.InstLoading {
			count++
		}
	}
	return count
}
