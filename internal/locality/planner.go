package locality

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Store interface {
	ListNodes(ctx context.Context) ([]domain.Node, error)
	ListPresets(ctx context.Context) ([]domain.Preset, error)
	ListModelLocalities(ctx context.Context) ([]domain.ModelLocality, error)
	ListInstances(ctx context.Context) ([]domain.ModelInstance, error)
	ListReservations(ctx context.Context) ([]domain.Reservation, error)
	Metrics(ctx context.Context, project string) ([]domain.RunMetric, error)
	SaveLocalityPlan(ctx context.Context, plan domain.LocalityPlan) error
}

type Planner struct {
	Store Store
	Clock ports.Clock
}

type PlanRequest struct {
	ID      string
	Project string
}

type Report struct {
	Nodes      []domain.Node          `json:"nodes"`
	Presets    []domain.Preset        `json:"presets"`
	Localities []domain.ModelLocality `json:"localities"`
}

func (p Planner) Report(ctx context.Context) (Report, error) {
	if p.Store == nil {
		return Report{}, fmt.Errorf("locality planner store is required")
	}
	nodes, err := p.Store.ListNodes(ctx)
	if err != nil {
		return Report{}, err
	}
	presets, err := p.Store.ListPresets(ctx)
	if err != nil {
		return Report{}, err
	}
	localities, err := p.Store.ListModelLocalities(ctx)
	if err != nil {
		return Report{}, err
	}
	return Report{Nodes: nodes, Presets: presets, Localities: localities}, nil
}

func (p Planner) Plan(ctx context.Context, req PlanRequest) (domain.LocalityPlan, error) {
	if p.Store == nil {
		return domain.LocalityPlan{}, fmt.Errorf("locality planner store is required")
	}
	now := p.now().UTC()
	planID := req.ID
	if planID == "" {
		planID = fmt.Sprintf("locality-%d", now.UnixNano())
	}
	nodes, err := p.Store.ListNodes(ctx)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	presets, err := p.Store.ListPresets(ctx)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	localities, err := p.Store.ListModelLocalities(ctx)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	instances, err := p.Store.ListInstances(ctx)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	reservations, err := p.Store.ListReservations(ctx)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	metrics, err := p.Store.Metrics(ctx, req.Project)
	if err != nil {
		return domain.LocalityPlan{}, err
	}
	presetsByID := map[string]domain.Preset{}
	for _, preset := range presets {
		presetsByID[preset.ID] = preset
	}
	readyByPreset := map[string]bool{}
	protected := protectedLocalities(instances, reservations, localities)
	plan := domain.LocalityPlan{ID: planID, CreatedAt: now}
	for _, loc := range localities {
		if loc.State == domain.ModelLocalityReady {
			readyByPreset[loc.PresetID] = true
		}
		_, presetExists := presetsByID[loc.PresetID]
		if presetExists {
			plan.Actions = append(plan.Actions, domain.LocalityAction{
				ID:             actionID(domain.LocalityActionKeep, loc.NodeID, loc.PresetID),
				Kind:           domain.LocalityActionKeep,
				PresetID:       loc.PresetID,
				NodeID:         loc.NodeID,
				Source:         loc.Source,
				ArtifactSizeMB: loc.ArtifactSizeMB,
				Reason:         "model already staged",
				State:          loc.State,
			})
			continue
		}
		if !loc.Managed {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("unmanaged stale locality %s is not evictable", loc.ID))
			continue
		}
		if protected[loc.ID] || loc.Pinned || loc.Warm {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("managed stale locality %s is protected", loc.ID))
			continue
		}
		plan.Actions = append(plan.Actions, domain.LocalityAction{
			ID:             actionID(domain.LocalityActionEvict, loc.NodeID, loc.PresetID),
			Kind:           domain.LocalityActionEvict,
			PresetID:       loc.PresetID,
			NodeID:         loc.NodeID,
			Source:         loc.ModelRef,
			ArtifactSizeMB: loc.ArtifactSizeMB,
			Reason:         "managed preset no longer exists",
			State:          domain.ModelLocalityPlanned,
		})
	}
	demand := demandByPreset(metrics)
	sort.SliceStable(presets, func(i, j int) bool {
		if demand[presets[i].ID] == demand[presets[j].ID] {
			return presets[i].ID < presets[j].ID
		}
		return demand[presets[i].ID] > demand[presets[j].ID]
	})
	for _, preset := range presets {
		if readyByPreset[preset.ID] {
			continue
		}
		node, reason, ok := chooseNode(nodes, preset, demand[preset.ID])
		if !ok {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("preset %s not staged: %s", preset.ID, reason))
			continue
		}
		plan.Actions = append(plan.Actions, domain.LocalityAction{
			ID:             actionID(domain.LocalityActionStage, node.ID, preset.ID),
			Kind:           domain.LocalityActionStage,
			PresetID:       preset.ID,
			NodeID:         node.ID,
			Source:         preset.ModelRef,
			ArtifactSizeMB: preset.ArtifactSizeMB,
			Reason:         reason,
			State:          domain.ModelLocalityPlanned,
		})
	}
	sort.SliceStable(plan.Actions, func(i, j int) bool { return plan.Actions[i].ID < plan.Actions[j].ID })
	sort.Strings(plan.Warnings)
	if err := p.Store.SaveLocalityPlan(ctx, plan); err != nil {
		return domain.LocalityPlan{}, err
	}
	return plan, nil
}

func (p Planner) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return clock.System{}.Now()
}

func protectedLocalities(instances []domain.ModelInstance, reservations []domain.Reservation, localities []domain.ModelLocality) map[string]bool {
	ids := map[string]bool{}
	for _, inst := range instances {
		ids[modelLocalityID(inst.NodeID, inst.PresetID)] = true
	}
	for _, res := range reservations {
		if res.PresetID == "" || res.NodeID == "" {
			continue
		}
		ids[modelLocalityID(res.NodeID, res.PresetID)] = true
	}
	for _, loc := range localities {
		if loc.Warm || loc.Pinned {
			ids[loc.ID] = true
		}
	}
	return ids
}

func demandByPreset(metrics []domain.RunMetric) map[string]int {
	out := map[string]int{}
	for _, metric := range metrics {
		if metric.PresetID == "" {
			continue
		}
		out[metric.PresetID]++
	}
	return out
}

func chooseNode(nodes []domain.Node, preset domain.Preset, demand int) (domain.Node, string, bool) {
	if preset.ArtifactSizeMB <= 0 {
		return domain.Node{}, "preset has no artifact size proof", false
	}
	if hasUnsafeVLLMUtilization(preset.LaunchArgs) {
		return domain.Node{}, "preset has unsafe vllm gpu memory utilization", false
	}
	if preset.NodeID != "" {
		for _, node := range nodes {
			if node.ID != preset.NodeID {
				continue
			}
			reason, ok := nodeFitsPreset(node, preset)
			if !ok {
				return domain.Node{}, node.ID + ":" + reason, false
			}
			return node, fmt.Sprintf("selected declared source node %s with demand=%d disk/memory fit", node.ID, demand), true
		}
		return domain.Node{}, fmt.Sprintf("declared source node %s not found", preset.NodeID), false
	}
	type candidate struct {
		node  domain.Node
		score float64
	}
	candidates := make([]candidate, 0, len(nodes))
	var drops []string
	for _, node := range nodes {
		reason, ok := nodeFitsPreset(node, preset)
		if !ok {
			drops = append(drops, node.ID+":"+reason)
			continue
		}
		freeAfter := node.DiskFreeMB - preset.ArtifactSizeMB
		score := float64(demand*1000000 + freeAfter)
		score += node.SpeedClass.TokensPerSecRef * 100
		candidates = append(candidates, candidate{node: node, score: score})
	}
	if len(candidates) == 0 {
		sort.Strings(drops)
		return domain.Node{}, strings.Join(drops, ", "), false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].node.ID < candidates[j].node.ID
		}
		return candidates[i].score > candidates[j].score
	})
	return candidates[0].node, fmt.Sprintf("selected by demand=%d disk/memory fit", demand), true
}

func nodeFitsPreset(node domain.Node, preset domain.Preset) (string, bool) {
	if node.Status != domain.NodeReady {
		return "not ready", false
	}
	if backend := node.Labels[domain.LabelPeerBackend]; backend != "" && domain.Backend(backend) != preset.Backend {
		return "backend " + backend, false
	}
	if node.DiskTotalMB <= 0 || node.DiskFreeMB <= 0 {
		return "missing disk facts", false
	}
	minFree := node.DiskMinFreeRatio
	if minFree == 0 {
		minFree = domain.DefaultDiskMinFreeRatio
	}
	freeAfter := node.DiskFreeMB - preset.ArtifactSizeMB
	if freeAfter < 0 || float64(freeAfter)/float64(node.DiskTotalMB) < minFree {
		return "disk floor", false
	}
	usableMemory := usableAcceleratorMemoryMB(node)
	if preset.EstWeightsMB > 0 && usableMemory > 0 && preset.EstWeightsMB > usableMemory {
		return "memory cap", false
	}
	return "", true
}

func usableAcceleratorMemoryMB(node domain.Node) int {
	var total int
	for _, acc := range node.Accelerators {
		total += acc.VRAMTotalMB
	}
	if total == 0 {
		return 0
	}
	maxUtil := node.MaxUtil
	if maxUtil == 0 {
		maxUtil = 1
	}
	return int(float64(total) * maxUtil)
}

func hasUnsafeVLLMUtilization(args []string) bool {
	for i, arg := range args {
		if arg == "--gpu-memory-utilization" && i+1 < len(args) {
			value, err := strconv.ParseFloat(args[i+1], 64)
			return err == nil && value >= 0.90
		}
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			value, err := strconv.ParseFloat(strings.TrimPrefix(arg, "--gpu-memory-utilization="), 64)
			return err == nil && value >= 0.90
		}
	}
	return false
}

func actionID(kind domain.LocalityActionKind, nodeID, presetID string) string {
	return string(kind) + ":" + nodeID + ":" + presetID
}

func modelLocalityID(nodeID, presetID string) string {
	return nodeID + ":" + presetID
}
