package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAutoScalerDemandGrowsReplicasOnTwoNodesWithinCaps(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{
		autoscaleNode("node-a", 1000),
		autoscaleNode("node-b", 1000),
	}, Instances: []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("pool-held"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode("pool-node"), func(i *domain.ModelInstance) {
			i.ReservationID = "pool-worker"
			i.Pinned = true
		}),
	}}}
	store := newAutoScaleTestStore(demandTask("demand-a", "vision-model", preset.ID, "node-a"), demandTask("demand-b", "vision-model", preset.ID, "node-b"))
	runtime := &autoScaleTestRuntime{store: store, fleet: fleet}
	scaler := testAutoScaler(store, runtime, fleet, clk, preset, domain.AutonomyPolicy{
		AutoScale:                  true,
		MaxReplicasPerModelPerNode: 1,
		MaxReplicasPerModel:        3,
		ScaleCooldownMS:            1000,
	})

	report, err := scaler.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Planned != 2 || report.Scaled != 2 {
		t.Fatalf("report = %+v, want 2 planned/scaled", report)
	}
	if len(runtime.submits) != 2 || len(runtime.finishes) != 2 {
		t.Fatalf("submits=%d finishes=%d", len(runtime.submits), len(runtime.finishes))
	}
	for _, job := range runtime.submits {
		if job.Priority != domain.PriorityBackground || job.SpeedPref != domain.SpeedThroughput || !job.ForceNewInstance {
			t.Fatalf("autoscale job did not force a background new-instance prewarm: %+v", job)
		}
		if job.NodeSelector[domain.LabelNodeID] == "" {
			t.Fatalf("autoscale job was not node pinned: %+v", job)
		}
	}
	if store.instances["inst-node-a-1"].Origin != domain.InstanceOriginAutoscale || store.instances["inst-node-b-2"].Origin != domain.InstanceOriginAutoscale {
		t.Fatalf("autoscale provenance missing: %+v", store.instances)
	}
	if store.instances["pool-held"].Origin != "" {
		t.Fatalf("pool-held worker was marked autoscale: %+v", store.instances["pool-held"])
	}
	if store.tasks["demand-a"].Status != domain.ReconciliationTaskDone || store.tasks["demand-b"].Status != domain.ReconciliationTaskDone {
		t.Fatalf("demand tasks not closed as done: %+v", store.tasks)
	}
}

func TestAutoScalerCatastrophicNodeLaunchesStrictlySerial(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(200, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	node := autoscaleNode("spark", 1000)
	node.OOMSeverity = domain.OOMCatastrophic
	node.Accelerators = []domain.Accelerator{
		{Index: 0, Vendor: "nvidia", Kind: "gb10", VRAMTotalMB: 1000},
		{Index: 1, Vendor: "nvidia", Kind: "gb10", VRAMTotalMB: 1000},
	}
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	store := newAutoScaleTestStore(demandTask("demand-spark", "vision-model", preset.ID, "spark"))
	runtime := &autoScaleTestRuntime{store: store, fleet: fleet}
	scaler := testAutoScaler(store, runtime, fleet, clk, preset, domain.AutonomyPolicy{
		AutoScale:                  true,
		MaxReplicasPerModelPerNode: 2,
		MaxReplicasPerModel:        2,
		ScaleCooldownMS:            1000,
	})

	report, err := scaler.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Scaled != 2 {
		t.Fatalf("report = %+v, want 2 scaled", report)
	}
	want := []string{"submit:spark:autoscale", "finish:spark:autoscale", "submit:spark:autoscale", "finish:spark:autoscale"}
	if strings.Join(runtime.events, "|") != strings.Join(want, "|") {
		t.Fatalf("events = %v, want strict submit/finish serial order %v", runtime.events, want)
	}
	if runtime.maxActiveByNode["spark"] != 1 {
		t.Fatalf("max active loads on catastrophic node = %d, want 1", runtime.maxActiveByNode["spark"])
	}
}

func TestAutoScalerFailureCooldownAndCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(300, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}}}
	store := newAutoScaleTestStore(demandTask("demand-a", "vision-model", preset.ID, "node-a"))
	runtime := &autoScaleTestRuntime{store: store, fleet: fleet, failNodes: map[string]error{"node-a": errors.New("owner admission rejected")}}
	scaler := testAutoScaler(store, runtime, fleet, clk, preset, domain.AutonomyPolicy{
		AutoScale:                  true,
		MaxReplicasPerModelPerNode: 1,
		MaxReplicasPerModel:        1,
		ScaleCooldownMS:            1000,
	})

	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := scaler.Tick(ctx); err != nil {
			t.Fatalf("Tick attempt %d: %v", attempt, err)
		}
		if attempt == 1 {
			if _, err := scaler.Tick(ctx); err != nil {
				t.Fatalf("cooldown tick: %v", err)
			}
			if len(runtime.submits) != 1 {
				t.Fatalf("cooldown allowed retry before deadline: submits=%d", len(runtime.submits))
			}
		}
		clk.Advance(time.Second)
	}
	if len(runtime.submits) != 3 {
		t.Fatalf("submits after breaker = %d, want 3", len(runtime.submits))
	}
	task := store.tasks["demand-a"]
	if task.Status != domain.ReconciliationTaskFailed || task.Blocker != "autoscale_circuit_breaker" || !strings.Contains(task.Error, "owner admission rejected") {
		t.Fatalf("breaker task = %+v", task)
	}
	clk.Advance(time.Second)
	if _, err := scaler.Tick(ctx); err != nil {
		t.Fatalf("post-breaker Tick: %v", err)
	}
	if len(runtime.submits) != 3 {
		t.Fatalf("breaker did not stop retries: submits=%d", len(runtime.submits))
	}
}

func TestStartAutoScalerDisabledDoesNothingDespiteDemand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := mocks.NewFakeClock(time.Unix(400, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}}}
	store := newAutoScaleTestStore(demandTask("demand-a", "vision-model", preset.ID, "node-a"))

	startAutoScaler(ctx, &scheduler.Service{}, store, fleet, &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(100, 0)}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, domain.AutonomyPolicy{AutoScale: false}, clk, time.Second, presetMap([]domain.Preset{preset}), "default")
	clk.Advance(time.Second)
	if clk.TimerCount() != 0 {
		t.Fatalf("disabled autoscaler created timers: %d", clk.TimerCount())
	}
	if fleet.snapshots != 0 {
		t.Fatalf("disabled autoscaler read fleet snapshot %d times", fleet.snapshots)
	}
}

func TestAutoScalerIdleDemandDoesNoPlanningWork(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(500, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}}}
	store := newAutoScaleTestStore()
	estimator := &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(100, 0)}
	scaler := &autoScaler{
		Store:     store,
		Runtime:   &autoScaleTestRuntime{store: store, fleet: fleet},
		Fleet:     fleet,
		Estimator: estimator,
		Allocator: &mocks.Allocator{FitsVal: true, CanStackLoadVal: true},
		Clock:     clk,
		Policy:    domain.AutonomyPolicy{AutoScale: true, MaxReplicasPerModel: 1, ScaleCooldownMS: 1000},
		Presets:   presetMap([]domain.Preset{preset}),
		Project:   "default",
		failures:  map[autoScaleKey]autoScaleFailure{},
	}
	report, err := scaler.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.DemandTasks != 0 || fleet.snapshots != 0 || len(estimator.Calls) != 0 {
		t.Fatalf("idle autoscale did planning work: report=%+v snapshots=%d estimates=%d", report, fleet.snapshots, len(estimator.Calls))
	}
}

func TestAutoScalerRefusesMissingPresetAndReachedCap(t *testing.T) {
	ctx := context.Background()
	clk := mocks.NewFakeClock(time.Unix(550, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithModelRef("vision-model"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1024))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}, Instances: []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("manual-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode("node-a")),
	}}}
	store := newAutoScaleTestStore(
		demandTask("demand-cap", "vision-model", preset.ID, "node-a"),
		demandTask("demand-missing", "missing-model", "missing-preset", "node-a"),
	)
	runtime := &autoScaleTestRuntime{store: store, fleet: fleet}
	scaler := testAutoScaler(store, runtime, fleet, clk, preset, domain.AutonomyPolicy{
		AutoScale:                  true,
		MaxReplicasPerModelPerNode: 1,
		MaxReplicasPerModel:        1,
		ScaleCooldownMS:            1000,
	})

	report, err := scaler.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Refused != 2 || len(runtime.submits) != 0 {
		t.Fatalf("report=%+v submits=%d, want two refusals and no launches", report, len(runtime.submits))
	}
	if store.tasks["demand-cap"].Blocker != "autoscale_cap_reached" {
		t.Fatalf("cap task = %+v", store.tasks["demand-cap"])
	}
	if store.tasks["demand-missing"].Blocker != "autoscale_no_preset" {
		t.Fatalf("missing task = %+v", store.tasks["demand-missing"])
	}
}

func testAutoScaler(store *autoScaleTestStore, runtime *autoScaleTestRuntime, fleet *autoScaleTestFleet, clk *mocks.FakeClock, preset domain.Preset, policy domain.AutonomyPolicy) *autoScaler {
	return &autoScaler{
		Store:     store,
		Runtime:   runtime,
		Fleet:     fleet,
		Estimator: &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(100, 0)},
		Allocator: &mocks.Allocator{FitsVal: true, CanStackLoadVal: true},
		Clock:     clk,
		Policy:    policy,
		Presets:   presetMap([]domain.Preset{preset}),
		Project:   "default",
		failures:  map[autoScaleKey]autoScaleFailure{},
	}
}

func autoscaleNode(id string, vram int) domain.Node {
	return fixtures.MakeNode(fixtures.WithNodeID(id), fixtures.WithVRAM(vram), fixtures.WithMaxUtil(1))
}

func demandTask(id, model, presetID, nodeID string) domain.ReconciliationTaskRecord {
	now := time.Unix(1, 0).UTC()
	return domain.ReconciliationTaskRecord{
		ID:                  id,
		Kind:                "prewarm_replica",
		Status:              domain.ReconciliationTaskAllowed,
		NodeID:              nodeID,
		PresetID:            presetID,
		Model:               model,
		ContextLength:       1024,
		ExpectedConcurrency: 1,
		AutomaticCandidate:  true,
		Reason:              "queued",
		Source:              "demand_signal",
		CreatedAt:           now,
		UpdatedAt:           now,
		LastSeen:            now,
		Hits:                1,
	}
}

type autoScaleTestFleet struct {
	mu        sync.Mutex
	snapshot  domain.FleetSnapshot
	snapshots int
}

func (f *autoScaleTestFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots++
	return cloneFleetSnapshot(f.snapshot), nil
}

func (f *autoScaleTestFleet) addInstance(inst domain.ModelInstance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot.Instances = append(f.snapshot.Instances, inst)
}

func cloneFleetSnapshot(in domain.FleetSnapshot) domain.FleetSnapshot {
	return domain.FleetSnapshot{
		Nodes:     append([]domain.Node(nil), in.Nodes...),
		Instances: append([]domain.ModelInstance(nil), in.Instances...),
	}
}

type autoScaleTestStore struct {
	mu        sync.Mutex
	tasks     map[string]domain.ReconciliationTaskRecord
	instances map[string]domain.ModelInstance
}

func newAutoScaleTestStore(tasks ...domain.ReconciliationTaskRecord) *autoScaleTestStore {
	store := &autoScaleTestStore{tasks: map[string]domain.ReconciliationTaskRecord{}, instances: map[string]domain.ModelInstance{}}
	for _, task := range tasks {
		store.tasks[task.ID] = task
	}
	store.instances["pool-held"] = fixtures.MakeInstance(fixtures.WithInstanceID("pool-held"), func(i *domain.ModelInstance) {
		i.ReservationID = "pool-worker"
		i.Pinned = true
	})
	return store
}

func (s *autoScaleTestStore) ListReconciliationTasks(context.Context, string) ([]domain.ReconciliationTaskRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.ReconciliationTaskRecord, 0, len(s.tasks))
	for _, task := range s.tasks {
		out = append(out, task)
	}
	return out, nil
}

func (s *autoScaleTestStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = task
	return nil
}

func (s *autoScaleTestStore) SaveInstance(_ context.Context, inst domain.ModelInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[inst.ID] = inst
	return nil
}

type autoScaleTestRuntime struct {
	store           *autoScaleTestStore
	fleet           *autoScaleTestFleet
	failNodes       map[string]error
	submits         []domain.Job
	finishes        []domain.Job
	events          []string
	activeByNode    map[string]int
	maxActiveByNode map[string]int
	nextInstance    int
}

func (r *autoScaleTestRuntime) SubmitWithPayload(_ context.Context, job domain.Job, _ []byte, _ ...scheduler.SubmitHooks) (scheduler.Result, error) {
	nodeID := job.NodeSelector[domain.LabelNodeID]
	r.submits = append(r.submits, job)
	if r.activeByNode == nil {
		r.activeByNode = map[string]int{}
		r.maxActiveByNode = map[string]int{}
	}
	r.activeByNode[nodeID]++
	if r.activeByNode[nodeID] > r.maxActiveByNode[nodeID] {
		r.maxActiveByNode[nodeID] = r.activeByNode[nodeID]
	}
	r.events = append(r.events, fmt.Sprintf("submit:%s:autoscale", nodeID))
	if err := r.failNodes[nodeID]; err != nil {
		r.activeByNode[nodeID]--
		return scheduler.Result{}, err
	}
	r.nextInstance++
	inst := domain.ModelInstance{
		ID:             fmt.Sprintf("inst-%s-%d", nodeID, r.nextInstance),
		PresetID:       job.PresetID,
		NodeID:         nodeID,
		AcceleratorSet: []int{0},
		Claim:          fixtures.MakeClaim(100, 0),
		State:          domain.InstReady,
		Priority:       job.Priority,
	}
	r.fleet.addInstance(inst)
	lease := domain.Lease{ID: "lease-" + inst.ID, JobID: job.ID, InstanceID: inst.ID, NodeID: nodeID, AcceleratorSet: inst.AcceleratorSet, Claim: inst.Claim, Priority: job.Priority}
	return scheduler.Result{
		Decision: domain.PlacementDecision{JobID: job.ID, Preset: domain.Preset{ID: job.PresetID}, NodeID: nodeID, AcceleratorSet: inst.AcceleratorSet, Claim: inst.Claim, Action: domain.ActionDedicatedUnit},
		Instance: inst,
		Lease:    lease,
	}, nil
}

func (r *autoScaleTestRuntime) FinishJob(_ context.Context, job domain.Job, _ domain.Lease, _ error) error {
	nodeID := job.NodeSelector[domain.LabelNodeID]
	r.finishes = append(r.finishes, job)
	r.events = append(r.events, fmt.Sprintf("finish:%s:autoscale", nodeID))
	r.activeByNode[nodeID]--
	return nil
}
