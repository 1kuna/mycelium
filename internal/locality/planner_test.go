package locality

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPlannerStagesMissingPresetsAndKeepsReadyLocality(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{
		nodes: []domain.Node{
			readyNode("mac", domain.BackendLlamaCpp, 200000, 120000, 24000, 0.8, 10),
			readyNode("b70", domain.BackendLlamaCpp, 400000, 240000, 48000, 0.8, 80),
		},
		presets: []domain.Preset{
			preset("small", domain.BackendLlamaCpp, 6000, 6000),
			preset("large", domain.BackendLlamaCpp, 30000, 30000),
		},
		localities: []domain.ModelLocality{{
			ID:             "mac:small",
			PresetID:       "small",
			NodeID:         "mac",
			State:          domain.ModelLocalityReady,
			ModelRef:       "/models/small.gguf",
			ArtifactSizeMB: 6000,
			Managed:        true,
			UpdatedAt:      time.Unix(1, 0).UTC(),
		}},
		metrics: []domain.RunMetric{
			{JobID: "m1", PresetID: "large", At: time.Unix(1, 0).UTC()},
			{JobID: "m2", PresetID: "large", At: time.Unix(2, 0).UTC()},
		},
	}
	planner := Planner{Store: store, Clock: mocks.NewFakeClock(time.Unix(10, 0).UTC())}
	report, err := planner.Report(ctx)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if len(report.Nodes) != 2 || len(report.Localities) != 1 {
		t.Fatalf("report = %+v", report)
	}
	plan, err := planner.Plan(ctx, PlanRequest{ID: "plan-a"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if store.savedPlan.ID != "plan-a" || store.savedPlan.CreatedAt != time.Unix(10, 0).UTC() {
		t.Fatalf("saved plan = %+v", store.savedPlan)
	}
	if action := actionByID(plan, "keep:mac:small"); action.Kind != domain.LocalityActionKeep || action.State != domain.ModelLocalityReady {
		t.Fatalf("keep action = %+v", action)
	}
	if action := actionByID(plan, "stage:b70:large"); action.Kind != domain.LocalityActionStage || !strings.Contains(action.Reason, "demand=2") {
		t.Fatalf("stage action = %+v", action)
	}
}

func TestPlannerRejectsUnfitAndUnsafePresets(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{
		nodes: []domain.Node{
			readyNode("tiny-disk", domain.BackendVLLM, 100000, 20000, 20000, 0.8, 10),
			readyNode("tiny-memory", domain.BackendVLLM, 400000, 300000, 10000, 0.5, 50),
		},
		presets: []domain.Preset{
			preset("no-size", domain.BackendVLLM, 0, 1000),
			preset("too-large", domain.BackendVLLM, 90000, 90000),
			withLaunchArgs(preset("unsafe", domain.BackendVLLM, 1000, 1000), "--gpu-memory-utilization", "0.90"),
			preset("unsupported-backend", domain.BackendMLX, 1000, 1000),
		},
	}
	planner := Planner{Store: store, Clock: mocks.NewFakeClock(time.Unix(20, 0).UTC())}
	plan, err := planner.Plan(ctx, PlanRequest{ID: "plan-b"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("unexpected actions = %+v", plan.Actions)
	}
	joined := strings.Join(plan.Warnings, "\n")
	for _, want := range []string{"no-size", "disk floor", "unsafe vllm", "unsupported-backend"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("warnings missing %q: %s", want, joined)
		}
	}
}

func TestPlannerEvictsOnlyUnprotectedManagedStaleLocalities(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{
		localities: []domain.ModelLocality{
			{ID: "node-a:stale", PresetID: "stale", NodeID: "node-a", State: domain.ModelLocalityReady, Managed: true},
			{ID: "node-a:pinned", PresetID: "pinned", NodeID: "node-a", State: domain.ModelLocalityReady, Managed: true, Pinned: true},
			{ID: "node-a:user", PresetID: "user", NodeID: "node-a", State: domain.ModelLocalityReady, Managed: false},
		},
		instances: []domain.ModelInstance{fixtures.MakeInstance(fixtures.WithInstancePreset("live"), fixtures.OnNode("node-a"))},
		reservations: []domain.Reservation{
			{ID: "res-live", NodeID: "node-a", PresetID: "live"},
		},
	}
	planner := Planner{Store: store, Clock: mocks.NewFakeClock(time.Unix(30, 0).UTC())}
	plan, err := planner.Plan(ctx, PlanRequest{ID: "plan-c"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if action := actionByID(plan, "evict:node-a:stale"); action.Kind != domain.LocalityActionEvict {
		t.Fatalf("evict action = %+v", action)
	}
	joined := strings.Join(plan.Warnings, "\n")
	if !strings.Contains(joined, "pinned") || !strings.Contains(joined, "unmanaged") {
		t.Fatalf("warnings = %s", joined)
	}
}

func TestPlannerRequiresStoreAndPropagatesStoreFailures(t *testing.T) {
	if _, err := (Planner{}).Report(context.Background()); err == nil {
		t.Fatal("Report accepted nil store")
	}
	if _, err := (Planner{}).Plan(context.Background(), PlanRequest{}); err == nil {
		t.Fatal("Plan accepted nil store")
	}
	store := &fakeStore{err: context.Canceled}
	if _, err := (Planner{Store: store}).Plan(context.Background(), PlanRequest{}); err == nil {
		t.Fatal("Plan swallowed store error")
	}
}

func TestPlannerHelperEdges(t *testing.T) {
	if demand := demandByPreset([]domain.RunMetric{{PresetID: ""}, {PresetID: "p"}}); demand["p"] != 1 || len(demand) != 1 {
		t.Fatalf("demand = %+v", demand)
	}
	if usable := usableAcceleratorMemoryMB(domain.Node{Accelerators: []domain.Accelerator{{VRAMTotalMB: 100}}}); usable != 100 {
		t.Fatalf("default util memory = %d", usable)
	}
	if usable := usableAcceleratorMemoryMB(domain.Node{}); usable != 0 {
		t.Fatalf("no accelerator memory = %d", usable)
	}
	if !hasUnsafeVLLMUtilization([]string{"--gpu-memory-utilization=0.91"}) {
		t.Fatal("equal-form unsafe utilization not detected")
	}
	if hasUnsafeVLLMUtilization([]string{"--gpu-memory-utilization", "not-a-number"}) {
		t.Fatal("malformed utilization treated as unsafe")
	}
	_, reason, ok := chooseNode([]domain.Node{{ID: "down"}}, preset("p", domain.BackendLlamaCpp, 1, 1), 0)
	if ok || !strings.Contains(reason, "not ready") {
		t.Fatalf("down node choice ok=%t reason=%q", ok, reason)
	}
}

type fakeStore struct {
	nodes        []domain.Node
	presets      []domain.Preset
	localities   []domain.ModelLocality
	instances    []domain.ModelInstance
	reservations []domain.Reservation
	metrics      []domain.RunMetric
	savedPlan    domain.LocalityPlan
	err          error
}

func (s *fakeStore) ListNodes(context.Context) ([]domain.Node, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]domain.Node(nil), s.nodes...), nil
}

func (s *fakeStore) ListPresets(context.Context) ([]domain.Preset, error) {
	return append([]domain.Preset(nil), s.presets...), nil
}

func (s *fakeStore) ListModelLocalities(context.Context) ([]domain.ModelLocality, error) {
	return append([]domain.ModelLocality(nil), s.localities...), nil
}

func (s *fakeStore) ListInstances(context.Context) ([]domain.ModelInstance, error) {
	return append([]domain.ModelInstance(nil), s.instances...), nil
}

func (s *fakeStore) ListReservations(context.Context) ([]domain.Reservation, error) {
	return append([]domain.Reservation(nil), s.reservations...), nil
}

func (s *fakeStore) Metrics(context.Context, string) ([]domain.RunMetric, error) {
	return append([]domain.RunMetric(nil), s.metrics...), nil
}

func (s *fakeStore) SaveLocalityPlan(_ context.Context, plan domain.LocalityPlan) error {
	s.savedPlan = plan
	return nil
}

func readyNode(id string, backend domain.Backend, diskTotal, diskFree, memory int, maxUtil float64, speed float64) domain.Node {
	return domain.Node{
		ID:               id,
		Name:             id,
		Status:           domain.NodeReady,
		Labels:           map[string]string{domain.LabelPeerBackend: string(backend)},
		DiskTotalMB:      diskTotal,
		DiskFreeMB:       diskFree,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		MaxUtil:          maxUtil,
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: memory}},
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: speed},
	}
}

func preset(id string, backend domain.Backend, artifact, weights int) domain.Preset {
	return domain.Preset{
		ID:             id,
		ModelRef:       "/models/" + id + ".gguf",
		Backend:        backend,
		ContextLength:  4096,
		Capabilities:   []domain.Capability{domain.CapabilityChat},
		ArtifactSizeMB: artifact,
		EstWeightsMB:   weights,
	}
}

func withLaunchArgs(p domain.Preset, args ...string) domain.Preset {
	p.LaunchArgs = args
	return p
}

func actionByID(plan domain.LocalityPlan, id string) domain.LocalityAction {
	for _, action := range plan.Actions {
		if action.ID == id {
			return action
		}
	}
	return domain.LocalityAction{}
}
