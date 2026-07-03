package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/pool"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

type signalingAutoScaleStore struct {
	*autoScaleTestStore
	listCalled chan struct{}
	listErr    error
}

func (s *signalingAutoScaleStore) ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	select {
	case s.listCalled <- struct{}{}:
	default:
	}
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.autoScaleTestStore.ListReconciliationTasks(ctx, jobID)
}

func TestStartAutoScalerGuardsAndTicks(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(500, 0).UTC())
	// Guard branches: disabled policy and nil dependencies return immediately.
	startAutoScaler(context.Background(), &scheduler.Service{}, newAutoScaleTestStore(), &autoScaleTestFleet{}, estimate.NewInMemory(), lease.NewAllocator(), domain.AutonomyPolicy{}, clk, 0, nil, "", "")
	startAutoScaler(context.Background(), nil, nil, nil, nil, nil, domain.AutonomyPolicy{AutoScale: true}, clk, 0, nil, "", "")

	// Enabled: the loop must fire a tick after the default interval (interval
	// <= 0 exercises the default fallback) and stop when the context ends.
	store := &signalingAutoScaleStore{autoScaleTestStore: newAutoScaleTestStore(), listCalled: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAutoScaler(ctx, &scheduler.Service{}, store, &autoScaleTestFleet{}, estimate.NewInMemory(), lease.NewAllocator(), domain.AutonomyPolicy{AutoScale: true}, clk, 0, nil, "", "")
	deadline := time.After(5 * time.Second)
	for fired := false; !fired; {
		clk.Advance(defaultAutoScaleInterval)
		select {
		case <-store.listCalled:
			fired = true
		case <-deadline:
			t.Fatalf("autoscaler tick never fired")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
}

func TestAutoScalerTickPropagatesStoreErrors(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(600, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	store := &signalingAutoScaleStore{autoScaleTestStore: newAutoScaleTestStore(), listCalled: make(chan struct{}, 1), listErr: fmt.Errorf("list boom")}
	scaler := &autoScaler{
		Store:     store,
		Runtime:   &autoScaleTestRuntime{},
		Fleet:     &autoScaleTestFleet{},
		Estimator: estimate.NewInMemory(),
		Allocator: lease.NewAllocator(),
		Clock:     clk,
		Policy:    domain.AutonomyPolicy{AutoScale: true},
		Presets:   presetMap([]domain.Preset{preset}),
		Project:   "default",
		failures:  map[autoScaleKey]autoScaleFailure{},
	}
	if _, err := scaler.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "list boom") {
		t.Fatalf("Tick error = %v", err)
	}
	var nilScaler *autoScaler
	if report, err := nilScaler.Tick(context.Background()); err != nil || report != (autoScaleReport{}) {
		t.Fatalf("nil scaler tick = %+v %v", report, err)
	}
}

func TestOpenAutoScaleDemandFiltersNonDemandTasks(t *testing.T) {
	// Only the source + open-status + identity filters gate autoscale input:
	// manual reconcile-lane tasks are protected by the demand_signal source,
	// while kind/candidate variants remain valid demand evidence.
	allowed := demandTask("keep", "m", "p", "n")
	wrongSource := demandTask("wrong-source", "m", "p", "n")
	wrongSource.Source = "operator"
	closed := demandTask("closed", "m", "p", "n")
	closed.Status = domain.ReconciliationTaskDone
	anonymous := demandTask("anonymous", "", "", "n")

	open := openAutoScaleDemand([]domain.ReconciliationTaskRecord{wrongSource, allowed, closed, anonymous})
	if len(open) != 1 || open[0].ID != "keep" {
		t.Fatalf("open demand = %+v", open)
	}

	// Deterministic ordering by model, preset, node, id.
	a := demandTask("b-id", "m1", "p1", "n2")
	b := demandTask("a-id", "m1", "p1", "n1")
	c := demandTask("c-id", "m0", "p9", "n9")
	sorted := openAutoScaleDemand([]domain.ReconciliationTaskRecord{a, b, c})
	if sorted[0].ID != "c-id" || sorted[1].ID != "a-id" || sorted[2].ID != "b-id" {
		t.Fatalf("sort order = %s %s %s", sorted[0].ID, sorted[1].ID, sorted[2].ID)
	}
}

func TestAutoScalerTaskModelAndPresetResolution(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-x"), func(p *domain.Preset) {
		p.ModelRef = ""
		p.Aliases = []string{"alias-x"}
	})
	scaler := &autoScaler{Presets: presetMap([]domain.Preset{preset})}

	named := demandTask("t1", "explicit-model", "preset-x", "n")
	if got := scaler.taskModel(named); got != "explicit-model" {
		t.Fatalf("explicit model = %q", got)
	}
	unnamed := demandTask("t2", "", "preset-x", "n")
	if got := scaler.taskModel(unnamed); got != "alias-x" {
		t.Fatalf("alias fallback = %q", got)
	}
	unknown := demandTask("t3", "", "preset-missing", "n")
	if got := scaler.taskModel(unknown); got != "preset-missing" {
		t.Fatalf("missing preset fallback = %q", got)
	}

	if got := autoscalePresetsForModel(scaler.Presets, "alias-x"); len(got) != 1 || got[0].ID != "preset-x" {
		t.Fatalf("alias resolution = %+v", got)
	}
	if got := autoscalePresetsForModel(scaler.Presets, "preset-x"); len(got) != 1 {
		t.Fatalf("id resolution = %+v", got)
	}
	if got := autoscalePresetsForModel(scaler.Presets, "nope"); got != nil {
		t.Fatalf("no-match resolution = %+v", got)
	}
}

func TestAutoScalerCloseGroupTasksBranches(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(700, 0).UTC())
	store := newAutoScaleTestStore()
	scaler := &autoScaler{Store: store, Clock: clk, failures: map[autoScaleKey]autoScaleFailure{}}

	// Model-scoped task (no node) closes done when anything scaled.
	modelTask := demandTask("model-task", "m", "p", "")
	scaled := map[string]autoScaleLaunchResult{"node-a": {Instance: domain.ModelInstance{ID: "inst-1"}, Lease: domain.Lease{ID: "lease-1"}}}
	if err := scaler.closeGroupTasks(ctx, []domain.ReconciliationTaskRecord{modelTask}, scaled, nil, false); err != nil {
		t.Fatalf("close scaled: %v", err)
	}
	if store.tasks["model-task"].Status != domain.ReconciliationTaskDone {
		t.Fatalf("model task = %+v", store.tasks["model-task"])
	}

	// Model-scoped task with only refusals gets the joined per-node reasons.
	refusedTask := demandTask("refused-task", "m", "p", "")
	refusals := map[string]string{"node-b": "no compatible backend runtime", "node-a": "insufficient safe capacity"}
	if err := scaler.closeGroupTasks(ctx, []domain.ReconciliationTaskRecord{refusedTask}, nil, refusals, false); err != nil {
		t.Fatalf("close refused: %v", err)
	}
	saved := store.tasks["refused-task"]
	if saved.Status != domain.ReconciliationTaskFailed || saved.Blocker != "autoscale_setup_gap" {
		t.Fatalf("refused task = %+v", saved)
	}
	if want := "node-a: insufficient safe capacity; node-b: no compatible backend runtime"; saved.Error != want {
		t.Fatalf("joined refusals = %q, want %q", saved.Error, want)
	}

	// Node task with no plan output closes as no-capacity when the planner
	// found nothing additional.
	noCap := demandTask("nocap-task", "m", "p", "node-c")
	if err := scaler.closeGroupTasks(ctx, []domain.ReconciliationTaskRecord{noCap}, nil, nil, true); err != nil {
		t.Fatalf("close no-additional: %v", err)
	}
	if store.tasks["nocap-task"].Blocker != "autoscale_no_capacity" {
		t.Fatalf("no-capacity task = %+v", store.tasks["nocap-task"])
	}

	if got := countClosedRefusals([]domain.ReconciliationTaskRecord{modelTask, noCap}, nil, refusals, true); got != 2 {
		t.Fatalf("countClosedRefusals = %d", got)
	}
}

func TestAutoScalerRecordFailureForTasks(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(800, 0).UTC())
	store := newAutoScaleTestStore(demandTask("fail-task", "m", "preset-a", "node-a"))
	scaler := &autoScaler{Store: store, Clock: clk, Policy: domain.AutonomyPolicy{AutoScale: true, ScaleCooldownMS: 1000}, failures: map[autoScaleKey]autoScaleFailure{}}

	cause := fmt.Errorf("plan boom")
	tasks := []domain.ReconciliationTaskRecord{store.tasks["fail-task"]}
	for i := 0; i < 3; i++ {
		if err := scaler.recordFailureForTasks(ctx, tasks, cause); err != nil {
			t.Fatalf("recordFailureForTasks %d: %v", i, err)
		}
	}
	key := autoScaleKey{PresetID: "preset-a", NodeID: "node-a"}
	if scaler.failures[key].Count < 3 {
		t.Fatalf("failure streak = %+v", scaler.failures[key])
	}
	if store.tasks["fail-task"].Status != domain.ReconciliationTaskFailed {
		t.Fatalf("breaker did not mark task failed: %+v", store.tasks["fail-task"])
	}
}

type brokenAutoScaleRuntime struct {
	instance domain.ModelInstance
	lease    domain.Lease
}

func (r *brokenAutoScaleRuntime) SubmitWithPayload(context.Context, domain.Job, []byte, ...scheduler.SubmitHooks) (scheduler.Result, error) {
	return scheduler.Result{Instance: r.instance, Lease: r.lease}, nil
}

func (r *brokenAutoScaleRuntime) FinishJob(context.Context, domain.Job, domain.Lease, error) error {
	return nil
}

func TestAutoScalerLaunchReplicaRejectsIncompleteResults(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(900, 0).UTC())
	group := autoScaleDemandGroup{Model: "m", Concurrency: 1}
	nodePlan := pool.NodePlan{NodeID: "node-a", PresetID: "preset-a", ContextLength: 1024}

	noInstance := &autoScaler{Store: newAutoScaleTestStore(), Runtime: &brokenAutoScaleRuntime{}, Clock: clk, Project: "p", failures: map[autoScaleKey]autoScaleFailure{}}
	if _, err := noInstance.launchReplica(ctx, group, nodePlan, 1); err == nil || !strings.Contains(err.Error(), "no instance") {
		t.Fatalf("missing instance err = %v", err)
	}

	noLease := &autoScaler{Store: newAutoScaleTestStore(), Runtime: &brokenAutoScaleRuntime{instance: domain.ModelInstance{ID: "inst-1"}}, Clock: clk, Project: "p", failures: map[autoScaleKey]autoScaleFailure{}}
	if _, err := noLease.launchReplica(ctx, group, nodePlan, 1); err == nil || !strings.Contains(err.Error(), "no lease") {
		t.Fatalf("missing lease err = %v", err)
	}
}
