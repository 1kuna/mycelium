package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestServiceQueuesNoFitJobs(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(1, 0).UTC())
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionQueued}},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
	}

	result, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Decision.Action != domain.ActionQueued || service.Queue.Len() != 1 {
		t.Fatalf("result=%+v queue=%d", result, service.Queue.Len())
	}
	if got := store.jobs["job-a"].Status; got != domain.JobQueued {
		t.Fatalf("job status = %s", got)
	}
}

func TestServiceRejectsQueuedJobWhenHookRequiresSynchronousOwnership(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(1, 1).UTC())
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionQueued}},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
	}

	result, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), SubmitHooks{RejectQueued: true})
	if err == nil || !strings.Contains(err.Error(), "queued: no instance available") {
		t.Fatalf("Submit err = %v", err)
	}
	if result.Decision.Action != domain.ActionQueued {
		t.Fatalf("result = %+v", result)
	}
	if service.Queue.Len() != 0 {
		t.Fatalf("queue len = %d", service.Queue.Len())
	}
	if got := store.jobs["job-a"]; got.Status != domain.JobFailed || !strings.Contains(got.Error, "queued: no instance available") {
		t.Fatalf("job = %+v", got)
	}
}

func TestServiceRejectsCoordinatedQueuedJobWhenHookRequiresSynchronousOwnership(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(1, 2).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	coordinator := &mocks.Coordinator{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued}}
	store := &runtimeStore{}
	queue := NewQueue(clock)
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{},
		Nodes:       staticNodes{},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       queue,
		Store:       store,
		Clock:       clock,
	}

	result, err := service.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`), SubmitHooks{RejectQueued: true})
	if err == nil || !strings.Contains(err.Error(), "queued: no instance available") {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
	if result.Decision.Action != domain.ActionQueued {
		t.Fatalf("result = %+v", result)
	}
	if queue.Len() != 0 {
		t.Fatalf("queue len = %d", queue.Len())
	}
	if got := store.jobs[job.ID]; got.Status != domain.JobFailed || !strings.Contains(got.Error, "queued: no instance available") {
		t.Fatalf("job = %+v", got)
	}
	if got := strings.Join(coordinator.Calls, ","); !strings.Contains(got, "fail:job-a:job \"job-a\" queued: no instance available") {
		t.Fatalf("coordinator calls = %s", got)
	}
}

func TestServiceLoadsAndGrantsLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(12))
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	result, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID), fixtures.Background))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Instance.PresetID != preset.ID || result.Lease.ID != "lease_offer_job-a" {
		t.Fatalf("result = %+v", result)
	}
	if got := store.jobs["job-a"].Status; got != domain.JobRunning {
		t.Fatalf("job status = %s", got)
	}
	if len(store.leases) != 1 || len(store.instances) != 1 {
		t.Fatalf("leases=%+v instances=%+v", store.leases, store.instances)
	}
	if strings.Join(admission.Calls, ",") != "offer:job-a,commit:offer_job-a:1,bind-instance:lease_offer_job-a:inst_1" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
	if len(agent.Loaded) != 1 || agent.Loaded[0].Priority != domain.PriorityBackground {
		t.Fatalf("loaded priority = %+v", agent.Loaded)
	}
}

func TestServiceLoadsPlacementTunedContext(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 10).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(32768), fixtures.WithMaxUtil(0.85))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	catalogPreset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithContextLength(262144))
	launchPreset := catalogPreset
	launchPreset.ContextLength = 81920
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{
			JobID:          "job-a",
			Preset:         launchPreset,
			NodeID:         node.ID,
			AcceleratorSet: []int{0},
			Claim:          fixtures.MakeClaim(16039, 5120),
			Action:         domain.ActionLoadedNew,
		}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
		Presets: map[string]domain.Preset{
			catalogPreset.ID: catalogPreset,
		},
	}

	if _, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(catalogPreset.ID))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(agent.Loaded) != 1 {
		t.Fatalf("loads = %+v", agent.Loaded)
	}
	if agent.Loaded[0].Preset.ContextLength != 81920 {
		t.Fatalf("load preset context = %d, want 81920", agent.Loaded[0].Preset.ContextLength)
	}
}

func TestServiceUsesWarmInstanceWithOwnerAdmission(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 30).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("warm-a"), fixtures.OnNode(node.ID))
	admission := &mocks.AdmissionController{}
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, AcceleratorSet: inst.AcceleratorSet, Claim: inst.Claim, Action: domain.ActionWarmInstance}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
	}

	result, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Instance.ID != inst.ID || result.Lease.InstanceID != inst.ID {
		t.Fatalf("result = %+v", result)
	}
	if got := store.jobs["job-a"].Status; got != domain.JobRunning {
		t.Fatalf("job status = %s", got)
	}
	if strings.Join(admission.Calls, ",") != "offer:job-a,commit:offer_job-a:1" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
}

func TestServiceSubmitWithPayloadUsesPeerCoordinator(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 45).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(12))
	store := &runtimeStore{}
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), Action: domain.ActionLoadedNew},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), GrantedAt: clock.Now()},
	}
	jobLog := &recordingJobLog{}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Coordinator: coordinator,
		JobLog:      jobLog,
		Queue:       NewQueue(clock),
		Store:       store,
		Clock:       clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	result, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), []byte(`{"job":"a"}`))
	if err != nil {
		t.Fatalf("SubmitWithPayload: %v", err)
	}
	if result.Lease.ID != "owner-lease-a" || result.Lease.InstanceID == "" || result.Instance.PresetID != preset.ID {
		t.Fatalf("result = %+v", result)
	}
	if strings.Join(coordinator.Calls, ",") != "claim:job-a,plan:job-a,commit:job-a,running:job-a" {
		t.Fatalf("coordinator calls = %+v", coordinator.Calls)
	}
	if jobLog.job.ID != "job-a" || string(jobLog.payload) != `{"job":"a"}` {
		t.Fatalf("job log = %+v payload=%s", jobLog.job, jobLog.payload)
	}
	if strings.Join(admission.Calls, ",") != "bind-instance:owner-lease-a:"+result.Instance.ID {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
	if store.jobs["job-a"].Status != domain.JobRunning || len(store.leases) != 1 || len(store.instances) != 1 {
		t.Fatalf("store jobs=%+v leases=%+v instances=%+v", store.jobs, store.leases, store.instances)
	}
}

func TestServiceSubmitRejectsCoordinatorBypass(t *testing.T) {
	_, err := (&Service{Coordinator: &mocks.Coordinator{}}).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")))
	if err == nil || !strings.Contains(err.Error(), "SubmitWithPayload") {
		t.Fatalf("Submit bypass err = %v", err)
	}
}

func TestServiceSubmitWithPayloadSkipsBindForBoundWarmOwnerLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 46).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("warm-a"), fixtures.OnNode(node.ID))
	admission := &mocks.AdmissionController{BindErr: errors.New("bind should not be called")}
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, AcceleratorSet: inst.AcceleratorSet, Claim: inst.Claim, Action: domain.ActionWarmInstance},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: "job-a", NodeID: node.ID, InstanceID: inst.ID, Claim: inst.Claim, GrantedAt: clock.Now()},
	}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
	}

	result, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), []byte(`{"job":"warm"}`))
	if err != nil {
		t.Fatalf("SubmitWithPayload: %v", err)
	}
	if result.Decision.Action != domain.ActionWarmInstance || result.Lease.InstanceID != inst.ID {
		t.Fatalf("result = %+v", result)
	}
	if strings.Join(admission.Calls, ",") != "" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
}

func TestServiceSubmitWithPayloadUsesQueuedCommitOutcome(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 465).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew},
		Outcome: domain.CommitOutcome{
			Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued},
			Lease:    domain.Lease{ID: "queued-owner-lease", JobID: job.ID},
		},
	}
	store := &runtimeStore{}
	queue := NewQueue(clock)
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       queue,
		Store:       store,
		Clock:       clock,
	}

	result, err := service.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`))
	if err != nil {
		t.Fatalf("SubmitWithPayload: %v", err)
	}
	if result.Decision.Action != domain.ActionQueued || result.Lease.ID != "queued-owner-lease" {
		t.Fatalf("result = %+v", result)
	}
	gotJob, gotPayload, ok := queue.DequeueWithPayload()
	if !ok || gotJob.Status != domain.JobQueued || string(gotPayload) != `{"job":"a"}` {
		t.Fatalf("queued job=%+v payload=%s ok=%v", gotJob, gotPayload, ok)
	}
	if store.jobs[job.ID].Status != domain.JobQueued || strings.Join(coordinator.Calls, ",") != "claim:job-a,plan:job-a,commit:job-a" {
		t.Fatalf("store=%+v calls=%+v", store.jobs[job.ID], coordinator.Calls)
	}

	saveErr := errors.New("save queued outcome")
	failingStore := &runtimeStore{saveJobErr: saveErr, saveJobErrAt: 2}
	failingService := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: &mocks.Coordinator{
			Decision: coordinator.Decision,
			Outcome: domain.CommitOutcome{
				Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued},
				Lease:    domain.Lease{ID: "queued-owner-lease", JobID: job.ID},
			},
		},
		JobLog: &recordingJobLog{},
		Queue:  NewQueue(clock),
		Store:  failingStore,
		Clock:  clock,
	}
	if _, err := failingService.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`)); !errors.Is(err, saveErr) {
		t.Fatalf("queued outcome save err = %v", err)
	}
}

func TestServiceSubmitWithPayloadRejectsMismatchedBoundOwnerLeaseWithoutUnloadingWarm(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 48).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("warm-a"), fixtures.OnNode(node.ID))
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, AcceleratorSet: inst.AcceleratorSet, Claim: inst.Claim, Action: domain.ActionWarmInstance},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: "job-a", NodeID: node.ID, InstanceID: "other-instance", Claim: inst.Claim, GrantedAt: clock.Now()},
	}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
	}

	_, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), []byte(`{"job":"warm"}`))
	if err == nil || !strings.Contains(err.Error(), "other-instance") {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
	if strings.Contains(strings.Join(agent.Calls, ","), "unload") {
		t.Fatalf("warm instance was unloaded: %+v", agent.Calls)
	}
	if !strings.Contains(strings.Join(coordinator.Calls, ","), "release:job-a") {
		t.Fatalf("coordinator did not release warm lease: %+v", coordinator.Calls)
	}
}

func TestServiceSubmitWithPayloadFallsBackWithoutCoordinator(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 47).UTC())
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionQueued}},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
	}
	result, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), []byte(`{"job":"a"}`))
	if err != nil || result.Decision.Action != domain.ActionQueued {
		t.Fatalf("SubmitWithPayload fallback result=%+v err=%v", result, err)
	}
}

func TestServiceSubmitWithPayloadReleasesCoordinatorOnLoadFailure(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 50).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	loadErr := errors.New("load failed")
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1)},
	}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: &mocks.NodeAgent{LoadErr: loadErr}}},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
		Presets:     map[string]domain.Preset{preset.ID: preset},
	}

	_, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), []byte(`{"job":"a"}`))
	if !errors.Is(err, loadErr) {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
	if !strings.Contains(strings.Join(coordinator.Calls, ","), "release:job-a") {
		t.Fatalf("coordinator calls = %+v", coordinator.Calls)
	}
}

func TestServiceSubmitWithPayloadReleasesCoordinatorWithCleanupContextOnCanceledLoad(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 51).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	coordinator := &cleanupContextCoordinator{Coordinator: mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1)},
	}}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: &mocks.NodeAgent{LoadErr: context.Canceled}}},
		Coordinator: coordinator,
		JobLog:      &recordingJobLog{},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
		Presets:     map[string]domain.Preset{preset.ID: preset},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.SubmitWithPayload(ctx, fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), []byte(`{"job":"a"}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
	if coordinator.releaseContextErr != nil {
		t.Fatalf("release used canceled context: %v", coordinator.releaseContextErr)
	}
	if !strings.Contains(strings.Join(coordinator.Calls, ","), "release:job-a") {
		t.Fatalf("coordinator calls = %+v", coordinator.Calls)
	}
	if cleanupContext(nil).Err() != nil {
		t.Fatal("nil cleanup context should be usable")
	}
}

func TestServiceSubmitWithPayloadCoordinatorErrorPaths(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 52).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))
	decision := domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}
	errBoom := errors.New("boom")
	base := func() *Service {
		admission := &mocks.AdmissionController{}
		return &Service{
			Placer:      fakePlacer{},
			Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
			Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Coordinator: &mocks.Coordinator{Decision: decision, Lease: domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID, Claim: decision.Claim}},
			JobLog:      &recordingJobLog{},
			Queue:       NewQueue(clock),
			Store:       &runtimeStore{},
			Clock:       clock,
			Presets:     map[string]domain.Preset{preset.ID: preset},
		}
	}
	checks := []struct {
		name    string
		mutate  func(*Service)
		run     func(*Service) (Result, error)
		wantErr error
		want    string
	}{
		{
			name: "validate",
			mutate: func(s *Service) {
				s.Placer = nil
			},
			want: "not fully configured",
		},
		{
			name: "missing job log",
			mutate: func(s *Service) {
				s.JobLog = nil
			},
			want: "missing a job log",
		},
		{
			name: "empty job id",
			run: func(s *Service) (Result, error) {
				return s.SubmitWithPayload(context.Background(), domain.Job{PresetID: preset.ID}, []byte(`{}`))
			},
			want: "job id",
		},
		{
			name: "initial save job",
			mutate: func(s *Service) {
				s.Store = &runtimeStore{saveJobErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "job log",
			mutate: func(s *Service) {
				s.JobLog = &recordingJobLog{err: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "claim",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Err: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "plan",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{PlanErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "queued commit",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued}, CommitErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "queued save",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued}}
				s.Store = &runtimeStore{saveJobErr: errBoom, saveJobErrAt: 2}
			},
			wantErr: errBoom,
		},
		{
			name: "queued success",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued}}
			},
		},
		{
			name: "fleet",
			mutate: func(s *Service) {
				s.Fleet = staticFleet{err: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "fleet release",
			mutate: func(s *Service) {
				s.Fleet = staticFleet{err: errBoom}
				s.Coordinator = &mocks.Coordinator{Decision: decision, Lease: domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID, Claim: decision.Claim}, ReleaseErr: errors.New("release")}
			},
			wantErr: errBoom,
		},
		{
			name: "preempt",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{
					Decision: domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Action: domain.ActionHardPreempted, Preempted: []string{"missing"}},
					Lease:    domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID},
				}
			},
			want: "preempted instance",
		},
		{
			name: "preempt release",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{
					Decision:   domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Action: domain.ActionHardPreempted, Preempted: []string{"missing"}},
					Lease:      domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID},
					ReleaseErr: errors.New("release"),
				}
			},
			want: "preempted instance",
		},
		{
			name: "commit",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Decision: decision, CommitErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "hook",
			run: func(s *Service) (Result, error) {
				return s.SubmitWithPayload(context.Background(), job, []byte(`{}`), SubmitHooks{BeforeColdLoad: func(context.Context, domain.PlacementDecision) error {
					return errBoom
				}})
			},
			wantErr: errBoom,
		},
		{
			name: "hook release",
			mutate: func(s *Service) {
				s.Coordinator = &mocks.Coordinator{Decision: decision, Lease: domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID, Claim: decision.Claim}, ReleaseErr: errors.New("release")}
			},
			run: func(s *Service) (Result, error) {
				return s.SubmitWithPayload(context.Background(), job, []byte(`{}`), SubmitHooks{BeforeColdLoad: func(context.Context, domain.PlacementDecision) error {
					return errBoom
				}})
			},
			wantErr: errBoom,
		},
		{
			name: "load release",
			mutate: func(s *Service) {
				s.Nodes = staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: &mocks.NodeAgent{LoadErr: errBoom}}}
				s.Coordinator = &mocks.Coordinator{Decision: decision, Lease: domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID, Claim: decision.Claim}, ReleaseErr: errors.New("release")}
			},
			wantErr: errBoom,
		},
		{
			name: "save instance",
			mutate: func(s *Service) {
				s.Store = &runtimeStore{saveInstanceErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "bind instance",
			mutate: func(s *Service) {
				s.Owners = staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{BindErr: errBoom}}}
			},
			wantErr: errBoom,
		},
		{
			name: "bind cleanup",
			mutate: func(s *Service) {
				agent := mocks.NewNodeAgent(node)
				agent.UnloadErr = errors.New("unload cleanup")
				s.Nodes = staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}}
				s.Owners = staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{BindErr: errBoom}}}
			},
			wantErr: errBoom,
		},
		{
			name: "save lease",
			mutate: func(s *Service) {
				s.Store = &runtimeStore{saveLeaseErr: errBoom}
			},
			wantErr: errBoom,
		},
		{
			name: "final save job",
			mutate: func(s *Service) {
				s.Store = &runtimeStore{saveJobErr: errBoom, saveJobErrAt: 2}
			},
			wantErr: errBoom,
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			service := base()
			if check.mutate != nil {
				check.mutate(service)
			}
			run := check.run
			if run == nil {
				run = func(s *Service) (Result, error) {
					return s.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`))
				}
			}
			_, err := run(service)
			switch {
			case check.wantErr != nil && !errors.Is(err, check.wantErr):
				t.Fatalf("err = %v", err)
			case check.want != "" && (err == nil || !strings.Contains(err.Error(), check.want)):
				t.Fatalf("err = %v", err)
			case check.wantErr == nil && check.want == "" && err != nil:
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestServiceSubmitWithPayloadQueuesReplanableCommitErrorWithPayload(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 53).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))
	claim := fixtures.MakeClaim(1, 1)
	store := &runtimeStore{}
	queue := NewQueue(clock)
	service := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: &mocks.Coordinator{
			Decision:  domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew},
			CommitErr: domain.ErrStaleFence,
		},
		JobLog:  &recordingJobLog{},
		Queue:   queue,
		Store:   store,
		Clock:   clock,
		Presets: map[string]domain.Preset{preset.ID: preset},
	}

	_, err := service.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`))
	if !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
	if got := store.jobs[job.ID].Status; got != domain.JobQueued {
		t.Fatalf("job status = %s", got)
	}
	gotJob, gotPayload, ok := queue.DequeueWithPayload()
	if !ok || gotJob.ID != job.ID || string(gotPayload) != `{"job":"a"}` {
		t.Fatalf("queued job=%+v payload=%s ok=%v", gotJob, gotPayload, ok)
	}

	saveErr := errors.New("save queued")
	failingStore := &runtimeStore{saveJobErr: saveErr, saveJobErrAt: 2}
	failingService := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: &mocks.Coordinator{
			Decision:  domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew},
			CommitErr: domain.ErrNoFit,
		},
		JobLog:  &recordingJobLog{},
		Queue:   NewQueue(clock),
		Store:   failingStore,
		Clock:   clock,
		Presets: map[string]domain.Preset{preset.ID: preset},
	}
	if _, err := failingService.SubmitWithPayload(context.Background(), job, []byte(`{"job":"a"}`)); !errors.Is(err, domain.ErrNoFit) || !errors.Is(err, saveErr) {
		t.Fatalf("SubmitWithPayload joined err = %v", err)
	}
}

func TestServiceDrainUsesCoordinatedPayload(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 55).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))
	claim := fixtures.MakeClaim(1, 1)
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew},
		Lease:    domain.Lease{ID: "owner-lease-a", JobID: job.ID, NodeID: node.ID, Claim: claim},
	}
	jobLog := &recordingJobLog{}
	queue := NewQueue(clock)
	queue.EnqueueWithPayload(job, []byte(`{"job":"a"}`))
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Coordinator: coordinator,
		JobLog:      jobLog,
		Queue:       queue,
		Store:       &runtimeStore{},
		Clock:       clock,
		Presets:     map[string]domain.Preset{preset.ID: preset},
	}

	results, err := service.Drain(context.Background(), 1)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(results) != 1 || results[0].Lease.ID != "owner-lease-a" {
		t.Fatalf("drain results = %+v", results)
	}
	if string(jobLog.payload) != `{"job":"a"}` {
		t.Fatalf("job log payload = %s", jobLog.payload)
	}
	if strings.Join(coordinator.Calls, ",") != "claim:job-a,plan:job-a,commit:job-a,running:job-a" {
		t.Fatalf("coordinator calls = %+v", coordinator.Calls)
	}
}

func TestServiceDrainRejectsCoordinatedJobWithoutPayload(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 57).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	queue := NewQueue(clock)
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-a")))
	agent := mocks.NewNodeAgent(node)
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Coordinator: &mocks.Coordinator{},
		JobLog:      &recordingJobLog{},
		Queue:       queue,
		Store:       &runtimeStore{},
		Clock:       clock,
	}

	_, err := service.Drain(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "no rescue payload") {
		t.Fatalf("Drain err = %v", err)
	}
	if strings.Join(agent.Calls, ",") != "" {
		t.Fatalf("local load ran without coordinator payload: %+v", agent.Calls)
	}
}

func TestServiceSubmitWithPayloadRejectsMissingCoordinatorLeaseForColdLoad(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 54).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: &mocks.Coordinator{
			Decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew},
			Lease:    domain.Lease{},
		},
		JobLog:  &recordingJobLog{},
		Queue:   NewQueue(clock),
		Store:   &runtimeStore{},
		Clock:   clock,
		Presets: map[string]domain.Preset{preset.ID: preset},
	}

	_, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), []byte(`{"job":"a"}`))
	if err == nil || !strings.Contains(err.Error(), "returned no owner lease") {
		t.Fatalf("SubmitWithPayload err = %v", err)
	}
}

func TestServiceSubmitWithPayloadUsesLocalLeaseForCoordinatedWarmInstance(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 56).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("warm-a"), fixtures.OnNode(node.ID))
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Coordinator: &mocks.Coordinator{
			Decision: domain.PlacementDecision{JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, Claim: inst.Claim, Action: domain.ActionWarmInstance},
			Lease:    domain.Lease{JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID, Claim: inst.Claim},
		},
		JobLog: &recordingJobLog{},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
	}

	result, err := service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), []byte(`{"job":"a"}`))
	if err != nil {
		t.Fatalf("SubmitWithPayload: %v", err)
	}
	if result.Lease.ID != "lease-job-a" || result.Lease.InstanceID != inst.ID || result.Instance.ID != inst.ID {
		t.Fatalf("result = %+v", result)
	}
	if _, ok := store.leases[result.Lease.ID]; !ok {
		t.Fatalf("lease not stored: %+v", store.leases)
	}
}

func TestDecisionWithOwnerLeaseCopiesOwnerFields(t *testing.T) {
	decision := decisionWithOwnerLease(
		domain.PlacementDecision{NodeID: "planned-node", InstanceID: "planned-inst", AcceleratorSet: []int{9}, Claim: fixtures.MakeClaim(1, 1)},
		domain.Lease{NodeID: "owner-node", InstanceID: "owner-inst", AcceleratorSet: []int{0, 2}, Claim: fixtures.MakeClaim(3, 4)},
	)
	if decision.NodeID != "owner-node" || decision.InstanceID != "owner-inst" || decision.Claim.WeightsMB != 3 {
		t.Fatalf("decision = %+v", decision)
	}
	if got := fmt.Sprint(decision.AcceleratorSet); got != "[0 2]" {
		t.Fatalf("accelerators = %s", got)
	}
	decision.AcceleratorSet[0] = 99
	lease := domain.Lease{AcceleratorSet: []int{1}}
	copied := decisionWithOwnerLease(domain.PlacementDecision{}, lease)
	copied.AcceleratorSet[0] = 5
	if lease.AcceleratorSet[0] != 1 {
		t.Fatalf("lease accelerator set was aliased: %+v", lease.AcceleratorSet)
	}
}

func TestServiceReleaseJobUsesCoordinatorThenDeletesStoreLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 55).UTC())
	store := &runtimeStore{leases: map[string]domain.Lease{"lease-a": {ID: "lease-a", JobID: "job-a"}}}
	coordinator := &mocks.Coordinator{}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{},
		Nodes:       staticNodes{},
		Coordinator: coordinator,
		Queue:       NewQueue(clock),
		Store:       store,
		Clock:       clock,
	}

	if err := service.ReleaseJob(context.Background(), domain.Lease{ID: "lease-a", JobID: "job-a"}); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}
	if _, ok := store.leases["lease-a"]; ok || strings.Join(coordinator.Calls, ",") != "release:job-a" {
		t.Fatalf("leases=%+v coordinator=%+v", store.leases, coordinator.Calls)
	}
	if err := service.ReleaseJob(context.Background(), domain.Lease{}); err != nil {
		t.Fatalf("empty ReleaseJob: %v", err)
	}
	if err := (&Service{}).ReleaseJob(context.Background(), domain.Lease{ID: "lease-a"}); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("unconfigured ReleaseJob err = %v", err)
	}
	noCoordinator := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{leases: map[string]domain.Lease{"lease-b": {ID: "lease-b"}}},
		Clock:  clock,
	}
	if err := noCoordinator.ReleaseJob(context.Background(), domain.Lease{ID: "lease-b"}); err != nil {
		t.Fatalf("fallback ReleaseJob: %v", err)
	}
	releaseErr := errors.New("release")
	withReleaseErr := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{},
		Nodes:       staticNodes{},
		Coordinator: &mocks.Coordinator{ReleaseErr: releaseErr},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
	}
	if err := withReleaseErr.ReleaseJob(context.Background(), domain.Lease{JobID: "job-b"}); !errors.Is(err, releaseErr) {
		t.Fatalf("coordinator ReleaseJob err = %v", err)
	}
	fallbackAdmission := &mocks.AdmissionController{}
	fallbackStore := &runtimeStore{leases: map[string]domain.Lease{"lease-sticky": {ID: "lease-sticky", JobID: "job-sticky", NodeID: "node-a"}}}
	withCoordinatorFallback := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{},
		Nodes:       staticNodes{},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{"node-a": fallbackAdmission}},
		Coordinator: &mocks.Coordinator{ReleaseErr: fmt.Errorf("job %q is not claimed by this coordinator", "job-sticky")},
		Queue:       NewQueue(clock),
		Store:       fallbackStore,
		Clock:       clock,
	}
	if err := withCoordinatorFallback.ReleaseJob(context.Background(), domain.Lease{ID: "lease-sticky", JobID: "job-sticky", NodeID: "node-a"}); err != nil {
		t.Fatalf("coordinator fallback ReleaseJob: %v", err)
	}
	if _, ok := fallbackStore.leases["lease-sticky"]; ok || strings.Join(fallbackAdmission.Calls, ",") != "release:lease-sticky" {
		t.Fatalf("fallback leases=%+v admission=%+v", fallbackStore.leases, fallbackAdmission.Calls)
	}
	withCoordinatorNoStoreLease := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{},
		Nodes:       staticNodes{},
		Coordinator: &mocks.Coordinator{},
		Queue:       NewQueue(clock),
		Store:       &runtimeStore{},
		Clock:       clock,
	}
	if err := withCoordinatorNoStoreLease.ReleaseJob(context.Background(), domain.Lease{JobID: "job-c"}); err != nil {
		t.Fatalf("coordinator-only ReleaseJob: %v", err)
	}
}

func TestServiceRunsColdLoadHookBeforeLoading(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(11, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(12))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}
	calls := []string{}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), SubmitHooks{
		BeforeColdLoad: func(_ context.Context, decision domain.PlacementDecision) error {
			calls = append(calls, "hook:"+decision.NodeID)
			calls = append(calls, agent.Calls...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if strings.Join(calls, ",") != "hook:node-a" || strings.Join(agent.Calls, ",") != "load:preset-a" {
		t.Fatalf("hook calls=%+v agent calls=%+v", calls, agent.Calls)
	}

	hookErr := errors.New("hook")
	_, err = service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-hook"), fixtures.WithPreset(preset.ID)), SubmitHooks{
		BeforeColdLoad: func(context.Context, domain.PlacementDecision) error {
			return hookErr
		},
	})
	if !errors.Is(err, hookErr) {
		t.Fatalf("hook err = %v", err)
	}
	if got := service.Store.(*runtimeStore).jobs["job-hook"].Status; got != domain.JobFailed {
		t.Fatalf("hook failed status = %s", got)
	}
	if err := runBeforeColdLoadHook(context.Background(), domain.PlacementDecision{}, []SubmitHooks{{}}); err != nil {
		t.Fatalf("nil hook err = %v", err)
	}
}

func TestServiceOwnerAdmissionErrorsFailBeforeLoad(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(12, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{CommitErr: domain.ErrStaleFence}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("Submit err = %v", err)
	}
	if strings.Join(agent.Calls, ",") != "" {
		t.Fatalf("load happened after stale owner commit: %+v", agent.Calls)
	}
	if got := service.Store.(*runtimeStore).jobs["job-a"].Status; got != domain.JobFailed {
		t.Fatalf("job status = %s", got)
	}
}

func TestServiceReleasesOwnerAdmissionOnLoadFailure(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(13, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	agent.LoadErr = errors.New("load")
	admission := &mocks.AdmissionController{}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if err == nil || !strings.Contains(err.Error(), "load") {
		t.Fatalf("Submit err = %v", err)
	}
	if strings.Join(admission.Calls, ",") != "offer:job-a,commit:offer_job-a:1,release:lease_offer_job-a" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
}

func TestServiceOwnerAdmissionReleaseErrorsAreLoud(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(14, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	releaseErr := errors.New("release owner")
	base := func(agent *mocks.NodeAgent, admission *mocks.AdmissionController) *Service {
		return &Service{
			Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
			Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Queue:  NewQueue(clock),
			Store:  &runtimeStore{},
			Clock:  clock,
			Presets: map[string]domain.Preset{
				preset.ID: preset,
			},
		}
	}

	admission := &mocks.AdmissionController{ReleaseErr: releaseErr}
	_, err := base(mocks.NewNodeAgent(node), admission).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), SubmitHooks{
		BeforeColdLoad: func(context.Context, domain.PlacementDecision) error {
			return errors.New("hook")
		},
	})
	if !errors.Is(err, releaseErr) {
		t.Fatalf("hook release err = %v", err)
	}

	agent := mocks.NewNodeAgent(node)
	agent.LoadErr = errors.New("load")
	admission = &mocks.AdmissionController{ReleaseErr: releaseErr}
	_, err = base(agent, admission).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if !errors.Is(err, releaseErr) {
		t.Fatalf("load release err = %v", err)
	}

	admission = &mocks.AdmissionController{ReleaseErr: releaseErr}
	_, err = base(mocks.NewNodeAgent(node), admission).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if err != nil {
		t.Fatalf("successful submit should keep owner lease instead of releasing it: %v", err)
	}
}

func TestServiceLocalBindFailureReleasesOwnerLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(15, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	bindErr := errors.New("bind")
	base := func(admission *mocks.AdmissionController) *Service {
		return &Service{
			Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
			Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Queue:  NewQueue(clock),
			Store:  &runtimeStore{},
			Clock:  clock,
			Presets: map[string]domain.Preset{
				preset.ID: preset,
			},
		}
	}

	admission := &mocks.AdmissionController{BindErr: bindErr}
	_, err := base(admission).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if !errors.Is(err, bindErr) || !strings.Contains(strings.Join(admission.Calls, ","), "release:lease_offer_job-a") {
		t.Fatalf("bind err=%v calls=%+v", err, admission.Calls)
	}

	releaseErr := errors.New("release")
	admission = &mocks.AdmissionController{BindErr: bindErr, ReleaseErr: releaseErr}
	_, err = base(admission).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if !errors.Is(err, bindErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("bind+release err = %v", err)
	}
}

func TestServiceReleaseOwnerNoopsWithoutLeaseID(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(16, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	hookErr := errors.New("hook")
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admissionOnly{}}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}
	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), SubmitHooks{
		BeforeColdLoad: func(context.Context, domain.PlacementDecision) error { return hookErr },
	})
	if !errors.Is(err, hookErr) {
		t.Fatalf("hook err = %v", err)
	}
}

func TestServiceCommitOwnerAdmissionBranches(t *testing.T) {
	service := &Service{}
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	if lease, owner, err := service.commitOwnerAdmission(context.Background(), job, domain.PlacementDecision{Action: domain.ActionQueued}); err != nil || lease.ID != "" || owner != nil {
		t.Fatalf("queued admission = %+v %v %v", lease, owner, err)
	}
	if _, _, err := service.commitOwnerAdmission(context.Background(), job, domain.PlacementDecision{Action: domain.ActionLoadedNew}); err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("missing owner err = %v", err)
	}
	if _, _, err := service.commitOwnerAdmission(context.Background(), job, domain.PlacementDecision{NodeID: "node-a", Action: domain.ActionLoadedNew}); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("missing resolver err = %v", err)
	}
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{}}
	if _, _, err := service.commitOwnerAdmission(context.Background(), job, domain.PlacementDecision{NodeID: "node-a", Action: domain.ActionLoadedNew}); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("missing admission err = %v", err)
	}
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{"node-a": &mocks.AdmissionController{OfferErr: domain.ErrNoFit}}}
	if _, _, err := service.commitOwnerAdmission(context.Background(), job, domain.PlacementDecision{NodeID: "node-a", Action: domain.ActionLoadedNew}); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("offer err = %v", err)
	}
}

func TestServiceReleaseJobUsesLocalOwnerAdmission(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(17, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	lease := domain.Lease{ID: "lease-a", JobID: "job-a", NodeID: node.ID}
	admission := &mocks.AdmissionController{}
	service := &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{leases: map[string]domain.Lease{lease.ID: lease}},
		Clock:  clock,
	}
	if err := service.ReleaseJob(context.Background(), lease); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}
	if strings.Join(admission.Calls, ",") != "release:lease-a" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}

	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{}}
	if err := service.ReleaseJob(context.Background(), lease); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("missing owner err = %v", err)
	}
	releaseErr := errors.New("release")
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{ReleaseErr: releaseErr}}}
	if err := service.ReleaseJob(context.Background(), lease); !errors.Is(err, releaseErr) {
		t.Fatalf("owner release err = %v", err)
	}
}

func TestServiceFinalizeOwnerLeaseFillsMissingClaim(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(18, 0).UTC())
	service := &Service{Clock: clock}
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode("node-a"))
	decision := domain.PlacementDecision{Claim: fixtures.MakeClaim(1, 2), AcceleratorSet: []int{0}}
	lease := service.finalizeOwnerLease(job, inst, decision, domain.Lease{ID: "owner-lease"})
	if lease.Claim != decision.Claim || lease.Priority != job.Priority || lease.ExpiresAt.IsZero() {
		t.Fatalf("lease = %+v", lease)
	}
}

func TestServiceEnactsPreemptionAndRequeuesVictim(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(20, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	targetPreset := fixtures.MakePreset(fixtures.WithPresetID("target-preset"), fixtures.WithWeights(22))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(victimPreset.ID), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	agent := mocks.NewNodeAgent(node)
	victimLease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: node.ID, Claim: victim.Claim}
	admission := &mocks.AdmissionController{LeaseForInstVal: victimLease, LeaseForInstFound: true}
	agent.Instances = []domain.ModelInstance{victim}
	store := &runtimeStore{
		instances: map[string]domain.ModelInstance{victim.ID: victim},
		leases:    map[string]domain.Lease{victimLease.ID: victimLease},
		jobs:      map[string]domain.Job{"victim-job": fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(victimPreset.ID), fixtures.Background)},
	}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{
			JobID:            "job-a",
			NodeID:           node.ID,
			Claim:            fixtures.MakeClaim(22, 4),
			Action:           domain.ActionHardPreempted,
			Preempted:        []string{victim.ID},
			Requeued:         []string{victim.ID},
			SpeedPrefApplied: domain.SpeedThroughput,
		}},
		Fleet: staticFleet{fleet: domain.FleetSnapshot{
			Nodes:     []domain.Node{node},
			Instances: []domain.ModelInstance{victim},
		}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
		Presets: map[string]domain.Preset{
			targetPreset.ID: targetPreset,
			victimPreset.ID: victimPreset,
		},
	}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(targetPreset.ID), fixtures.Interactive, fixtures.HardForInteractive))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, ok := store.instances[victim.ID]; ok {
		t.Fatalf("victim still stored: %+v", store.instances)
	}
	if service.Queue.Len() != 1 {
		t.Fatalf("requeue len = %d", service.Queue.Len())
	}
	requeued, ok := service.Queue.Dequeue()
	if !ok || requeued.ID != "victim-job" || requeued.Priority != domain.PriorityBackground {
		t.Fatalf("requeued job = %+v ok=%v", requeued, ok)
	}
	if len(agent.Calls) < 2 || agent.Calls[0] != "unload:victim-a" || agent.Calls[1] != "load:target-preset" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestServiceEnactsPreemptionAndLoadsReplacementVictim(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(20, 0).UTC())
	targetNode := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	replacementNode := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	targetPreset := fixtures.MakePreset(fixtures.WithPresetID("target-preset"), fixtures.WithWeights(22))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim-a"),
		fixtures.WithInstancePreset(victimPreset.ID),
		fixtures.OnNode(targetNode.ID),
		fixtures.WithClaim(fixtures.MakeClaim(7, 3)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	victim.InFlight = 1
	targetAgent := mocks.NewNodeAgent(targetNode)
	targetAgent.Instances = []domain.ModelInstance{victim}
	replacementAgent := mocks.NewNodeAgent(replacementNode)
	victimLease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: targetNode.ID, Claim: victim.Claim, Priority: victim.Priority}
	targetAdmission := &mocks.AdmissionController{LeaseForInstVal: victimLease, LeaseForInstFound: true}
	replacementAdmission := &mocks.AdmissionController{}
	store := &runtimeStore{
		instances: map[string]domain.ModelInstance{victim.ID: victim},
		leases:    map[string]domain.Lease{victimLease.ID: victimLease},
		jobs:      map[string]domain.Job{"victim-job": fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(victimPreset.ID), fixtures.Background)},
	}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{
			JobID:     "job-a",
			NodeID:    targetNode.ID,
			Claim:     fixtures.MakeClaim(22, 4),
			Action:    domain.ActionHardPreempted,
			Preempted: []string{victim.ID},
			Replacements: []domain.Replacement{{
				InstanceID:     victim.ID,
				NodeID:         replacementNode.ID,
				AcceleratorSet: []int{0},
			}},
			SpeedPrefApplied: domain.SpeedThroughput,
		}},
		Fleet: staticFleet{fleet: domain.FleetSnapshot{
			Nodes:     []domain.Node{targetNode, replacementNode},
			Instances: []domain.ModelInstance{victim},
		}},
		Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{
			targetNode.ID:      targetAgent,
			replacementNode.ID: replacementAgent,
		}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{
			targetNode.ID:      targetAdmission,
			replacementNode.ID: replacementAdmission,
		}},
		Queue: NewQueue(clock),
		Store: store,
		Clock: clock,
		Presets: map[string]domain.Preset{
			targetPreset.ID: targetPreset,
			victimPreset.ID: victimPreset,
		},
	}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(targetPreset.ID), fixtures.Interactive, fixtures.HardForInteractive))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if service.Queue.Len() != 0 {
		t.Fatalf("replacement should not queue victim, len = %d", service.Queue.Len())
	}
	if len(replacementAgent.Loaded) != 1 || replacementAgent.Loaded[0].Preset.ID != victimPreset.ID || replacementAgent.Loaded[0].JobID != "victim-job" {
		t.Fatalf("replacement loads = %+v", replacementAgent.Loaded)
	}
	if replacementAgent.Loaded[0].Claim != victim.Claim || replacementAgent.Loaded[0].AcceleratorSet[0] != 0 {
		t.Fatalf("replacement load request = %+v", replacementAgent.Loaded[0])
	}
	if !strings.Contains(strings.Join(targetAgent.Calls, ","), "unload:victim-a") || !strings.Contains(strings.Join(targetAgent.Calls, ","), "load:target-preset") {
		t.Fatalf("target agent calls = %+v", targetAgent.Calls)
	}
	if !strings.Contains(strings.Join(replacementAdmission.Calls, ","), "offer:victim-job,commit:offer_victim-job:1,bind-instance:lease_offer_victim-job:inst_1") {
		t.Fatalf("replacement admission calls = %+v", replacementAdmission.Calls)
	}
}

func TestServiceReplacementPreemptionValidationAndFallback(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(20, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(victimPreset.ID), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	lease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: node.ID, Claim: victim.Claim}

	service := &Service{
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true}}},
		Store: &runtimeStore{
			leases:    map[string]domain.Lease{lease.ID: lease},
			instances: map[string]domain.ModelInstance{victim.ID: victim},
			jobs:      map[string]domain.Job{"victim-job": fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(victimPreset.ID), fixtures.Background)},
		},
		Queue:   NewQueue(clock),
		Clock:   clock,
		Presets: map[string]domain.Preset{victimPreset.ID: victimPreset},
	}
	err := service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{
		Replacements: []domain.Replacement{{InstanceID: victim.ID, NodeID: node.ID, AcceleratorSet: []int{0}}},
	}, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err == nil || !strings.Contains(err.Error(), "was not preempted") {
		t.Fatalf("unpreempted replacement err = %v", err)
	}

	noLeaseAdmission := &mocks.AdmissionController{}
	noLeaseAgent := mocks.NewNodeAgent(node)
	service = &Service{
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: noLeaseAgent}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: noLeaseAdmission}},
		Store:  &runtimeStore{instances: map[string]domain.ModelInstance{victim.ID: victim}, leases: map[string]domain.Lease{}},
		Queue:  NewQueue(clock),
		Clock:  clock,
	}
	err = service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{
		JobID:        "job-a",
		Preempted:    []string{victim.ID},
		Replacements: []domain.Replacement{{InstanceID: victim.ID, NodeID: node.ID, AcceleratorSet: []int{0}}},
	}, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err != nil || service.Queue.Len() != 0 {
		t.Fatalf("no-lease replacement err=%v queue=%d", err, service.Queue.Len())
	}

	failingAdmission := &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true}
	service = &Service{
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: failingAdmission}},
		Store: &runtimeStore{
			leases:    map[string]domain.Lease{lease.ID: lease},
			instances: map[string]domain.ModelInstance{victim.ID: victim},
		},
		Queue:   NewQueue(clock),
		Clock:   clock,
		Presets: map[string]domain.Preset{},
	}
	err = service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{
		JobID:        "job-a",
		Preempted:    []string{victim.ID},
		Replacements: []domain.Replacement{{InstanceID: victim.ID, NodeID: node.ID, AcceleratorSet: []int{0}}},
	}, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err == nil || !strings.Contains(err.Error(), "job \"victim-job\" not found") {
		t.Fatalf("replacement failure should include queue failure, got %v", err)
	}
}

func TestServiceReplacePreemptedInstanceErrorPaths(t *testing.T) {
	errBoom := errors.New("boom")
	for _, tt := range []struct {
		name   string
		mutate func(*replacementHarness)
		want   string
	}{
		{
			name: "missing replacement node",
			mutate: func(h *replacementHarness) {
				h.fleet.Nodes = nil
			},
			want: "replacement node",
		},
		{
			name: "unknown preset",
			mutate: func(h *replacementHarness) {
				h.service.Presets = map[string]domain.Preset{}
			},
			want: "unknown replacement preset",
		},
		{
			name: "owner resolver error",
			mutate: func(h *replacementHarness) {
				h.service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{}}
			},
			want: "unreachable",
		},
		{
			name: "job reader error",
			mutate: func(h *replacementHarness) {
				h.store.jobs = nil
			},
			want: "not found",
		},
		{
			name: "offer error",
			mutate: func(h *replacementHarness) {
				h.admission.OfferErr = errBoom
			},
			want: "boom",
		},
		{
			name: "commit error",
			mutate: func(h *replacementHarness) {
				h.admission.CommitErr = errBoom
			},
			want: "boom",
		},
		{
			name: "tuning error releases owner",
			mutate: func(h *replacementHarness) {
				h.replacement.AcceleratorSet = []int{0, 1}
				h.admission.ReleaseErr = errBoom
			},
			want: "selected accelerator",
		},
		{
			name: "tuning error with empty owner lease",
			mutate: func(h *replacementHarness) {
				h.replacement.AcceleratorSet = []int{0, 1}
				h.service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{h.node.ID: emptyCommitAdmission{}}}
			},
			want: "selected accelerator",
		},
		{
			name: "agent resolver error releases owner",
			mutate: func(h *replacementHarness) {
				h.service.Nodes = staticNodes{agents: map[string]*mocks.NodeAgent{}}
			},
			want: "unreachable",
		},
		{
			name: "load error releases owner",
			mutate: func(h *replacementHarness) {
				h.agent.LoadErr = errBoom
			},
			want: "boom",
		},
		{
			name: "bind error unloads and releases",
			mutate: func(h *replacementHarness) {
				h.admission.BindErr = errBoom
			},
			want: "boom",
		},
		{
			name: "save instance error",
			mutate: func(h *replacementHarness) {
				h.store.saveInstanceErr = errBoom
			},
			want: "boom",
		},
		{
			name: "save lease error",
			mutate: func(h *replacementHarness) {
				h.store.saveLeaseErr = errBoom
			},
			want: "boom",
		},
		{
			name: "save job error",
			mutate: func(h *replacementHarness) {
				h.store.saveJobErr = errBoom
			},
			want: "boom",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newReplacementHarness()
			tt.mutate(&h)
			err := h.service.replacePreemptedInstance(context.Background(), h.lease, h.victim, h.replacement, h.fleet)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestServiceReplacePreemptedInstanceFallbackJobAndZeroClaim(t *testing.T) {
	h := newReplacementHarness()
	h.service.Store = storeWithoutJobReader{}
	if err := h.service.replacePreemptedInstance(context.Background(), h.lease, h.victim, h.replacement, h.fleet); err != nil {
		t.Fatalf("fallback replacement job: %v", err)
	}

	h = newReplacementHarness()
	h.service.Nodes = zeroClaimResolver{node: h.node}
	if err := h.service.replacePreemptedInstance(context.Background(), h.lease, h.victim, h.replacement, h.fleet); err != nil {
		t.Fatalf("zero claim replacement: %v", err)
	}
	got := h.store.instances["inst-zero"]
	if got.Claim != h.victim.Claim {
		t.Fatalf("zero claim was not filled: %+v", got)
	}

	h = newReplacementHarness()
	h.lease.JobID = ""
	h.service.Store = storeWithoutJobReader{}
	if err := h.service.replacePreemptedInstance(context.Background(), h.lease, h.victim, h.replacement, h.fleet); err == nil || !strings.Contains(err.Error(), "no owner lease job") {
		t.Fatalf("missing lease job err = %v", err)
	}
}

func TestWithOwnerReleaseJoinsReleaseError(t *testing.T) {
	errBoom := errors.New("boom")
	if err := withOwnerRelease(errBoom, func() error { return nil }); !errors.Is(err, errBoom) {
		t.Fatalf("plain release err = %v", err)
	}
	releaseErr := errors.New("release")
	err := withOwnerRelease(errBoom, func() error { return releaseErr })
	if !errors.Is(err, errBoom) || !errors.Is(err, releaseErr) {
		t.Fatalf("joined err = %v", err)
	}
}

func TestServiceEvictsIdleWarmVictimWithoutOwnerLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(20, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	targetPreset := fixtures.MakePreset(fixtures.WithPresetID("target-preset"), fixtures.WithWeights(22))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(victimPreset.ID), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	store := &runtimeStore{
		instances: map[string]domain.ModelInstance{victim.ID: victim},
		leases:    map[string]domain.Lease{},
		jobs:      map[string]domain.Job{},
	}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{
			JobID:            "job-a",
			NodeID:           node.ID,
			Claim:            fixtures.MakeClaim(22, 4),
			Action:           domain.ActionHardPreempted,
			Preempted:        []string{victim.ID},
			SpeedPrefApplied: domain.SpeedThroughput,
		}},
		Fleet: staticFleet{fleet: domain.FleetSnapshot{
			Nodes:     []domain.Node{node},
			Instances: []domain.ModelInstance{victim},
		}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
		Presets: map[string]domain.Preset{
			targetPreset.ID: targetPreset,
			victimPreset.ID: victimPreset,
		},
	}

	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(targetPreset.ID), fixtures.Interactive, fixtures.HardForInteractive))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, ok := store.instances[victim.ID]; ok {
		t.Fatalf("victim still stored: %+v", store.instances)
	}
	if service.Queue.Len() != 0 {
		t.Fatalf("idle eviction should not requeue, len = %d", service.Queue.Len())
	}
	if !strings.Contains(strings.Join(agent.Calls, ","), "unload:victim-a") {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	if strings.Join(admission.Calls, ",") != "lease-for-instance:victim-a,offer:job-a,commit:offer_job-a:1,bind-instance:lease_offer_job-a:inst_1" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
}

func TestServiceCoordinatedPreemptionUsesOwnerLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(21, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	agent := mocks.NewNodeAgent(node)
	lease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: node.ID, Claim: victim.Claim}
	admission := &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true}
	store := &runtimeStore{
		instances: map[string]domain.ModelInstance{victim.ID: victim},
		leases:    map[string]domain.Lease{lease.ID: lease},
	}
	service := &Service{
		Coordinator: &mocks.Coordinator{},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:       NewQueue(clock),
		Store:       store,
		Clock:       clock,
	}

	requester := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	err := service.enactPreemption(context.Background(), requester, domain.PlacementDecision{
		JobID:     "job-a",
		Preempted: []string{victim.ID},
	}, domain.FleetSnapshot{Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("enactPreemption: %v", err)
	}
	if strings.Join(admission.Calls, ",") != "lease-for-instance:victim-a,release:lease-victim" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
	if strings.Join(agent.Calls, ",") != "unload:victim-a" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	if _, ok := store.leases[lease.ID]; ok {
		t.Fatalf("victim lease still stored: %+v", store.leases)
	}
	if _, ok := store.instances[victim.ID]; ok {
		t.Fatalf("victim instance still stored: %+v", store.instances)
	}
}

func TestServiceCoordinatedPreemptionRequeuesVictimWithPayload(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(21, 10).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	victimJob := fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(preset.ID), fixtures.Background)
	victimLease := domain.Lease{ID: "lease-victim", JobID: victimJob.ID, InstanceID: victim.ID, NodeID: node.ID, Claim: victim.Claim, Priority: victim.Priority}
	admission := &mocks.AdmissionController{LeaseForInstVal: victimLease, LeaseForInstFound: true}
	agent := mocks.NewNodeAgent(node)
	coordinator := &mocks.Coordinator{
		Decision: domain.PlacementDecision{JobID: victimJob.ID, NodeID: node.ID, Claim: victim.Claim, Action: domain.ActionLoadedNew},
		Lease:    domain.Lease{ID: "owner-victim", JobID: victimJob.ID, NodeID: node.ID, Claim: victim.Claim},
	}
	jobLog := &recordingJobLog{
		jobs:     map[string]domain.Job{victimJob.ID: victimJob},
		payloads: map[string][]byte{victimJob.ID: []byte(`{"job":"victim"}`)},
	}
	queue := NewQueue(clock)
	store := &runtimeStore{
		instances: map[string]domain.ModelInstance{victim.ID: victim},
		leases:    map[string]domain.Lease{victimLease.ID: victimLease},
		jobs:      map[string]domain.Job{victimJob.ID: victimJob},
	}
	service := &Service{
		Placer:      fakePlacer{},
		Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Coordinator: coordinator,
		JobLog:      jobLog,
		Queue:       queue,
		Store:       store,
		Clock:       clock,
		Presets:     map[string]domain.Preset{preset.ID: preset},
	}

	err := service.enactPreemption(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.Interactive, fixtures.HardForInteractive), domain.PlacementDecision{
		JobID:     "job-a",
		Preempted: []string{victim.ID},
		Requeued:  []string{victim.ID},
	}, domain.FleetSnapshot{Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("enactPreemption: %v", err)
	}
	requeued, payload, ok := queue.DequeueWithPayload()
	if !ok || requeued.ID != victimJob.ID || string(payload) != `{"job":"victim"}` {
		t.Fatalf("queued job=%+v payload=%s ok=%v", requeued, payload, ok)
	}
	queue.EnqueueWithPayload(requeued, payload)

	results, err := service.Drain(context.Background(), 1)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(results) != 1 || results[0].Lease.ID != "owner-victim" || results[0].Instance.PresetID != preset.ID {
		t.Fatalf("drain results = %+v", results)
	}
	if strings.Join(coordinator.Calls, ",") != "claim:victim-job,plan:victim-job,commit:victim-job,running:victim-job" {
		t.Fatalf("coordinator calls = %+v", coordinator.Calls)
	}
	if strings.Join(agent.Calls, ",") != "unload:victim-a,load:victim-preset" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestServiceCoordinatedPreemptionRejectsMissingRequeuePayload(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(21, 20).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	victimJob := fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.Background)
	lease := domain.Lease{ID: "lease-victim", JobID: victimJob.ID, InstanceID: victim.ID, NodeID: node.ID, Claim: victim.Claim, Priority: victim.Priority}
	queue := NewQueue(clock)
	service := &Service{
		Coordinator: &mocks.Coordinator{},
		JobLog: &recordingJobLog{
			jobs:     map[string]domain.Job{victimJob.ID: victimJob},
			payloads: map[string][]byte{victimJob.ID: nil},
		},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true}}},
		Queue:  queue,
		Store: &runtimeStore{
			instances: map[string]domain.ModelInstance{victim.ID: victim},
			leases:    map[string]domain.Lease{lease.ID: lease},
			jobs:      map[string]domain.Job{victimJob.ID: victimJob},
		},
		Clock: clock,
	}

	err := service.enactPreemption(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.Interactive, fixtures.HardForInteractive), domain.PlacementDecision{
		JobID:     "job-a",
		Preempted: []string{victim.ID},
		Requeued:  []string{victim.ID},
	}, domain.FleetSnapshot{Instances: []domain.ModelInstance{victim}})
	if err == nil || !strings.Contains(err.Error(), "no rescue payload") {
		t.Fatalf("enactPreemption err = %v", err)
	}
	if queue.Len() != 0 {
		t.Fatalf("missing payload should not enqueue poison job, len = %d", queue.Len())
	}
}

func TestServiceOwnerBindingAndCleanupErrors(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(22, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(node.ID))

	if err := (&Service{}).bindOwnerInstance(context.Background(), node.ID, "lease-a", inst.ID); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("missing owner resolver err = %v", err)
	}
	if err := (&Service{Owners: staticNodes{}}).bindOwnerInstance(context.Background(), node.ID, "lease-a", inst.ID); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("owner resolver err = %v", err)
	}
	if err := (&Service{Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admissionOnly{}}}}).bindOwnerInstance(context.Background(), node.ID, "lease-a", inst.ID); err == nil || !strings.Contains(err.Error(), "lease binding") {
		t.Fatalf("unsupported binding err = %v", err)
	}
	for _, tc := range []struct {
		name       string
		ownerLease domain.Lease
		lease      domain.Lease
		want       string
	}{
		{name: "bound mismatch", ownerLease: domain.Lease{ID: "lease-a", InstanceID: "other"}, lease: domain.Lease{ID: "lease-a", InstanceID: inst.ID, NodeID: node.ID}, want: "other"},
		{name: "missing instance", lease: domain.Lease{ID: "lease-a", NodeID: node.ID}, want: "instance"},
		{name: "missing lease id", lease: domain.Lease{InstanceID: inst.ID, NodeID: node.ID}, want: "lease id"},
		{name: "missing node", lease: domain.Lease{ID: "lease-a", InstanceID: inst.ID}, want: "owner node"},
	} {
		if err := (&Service{}).ensureOwnerLeaseBound(context.Background(), tc.ownerLease, tc.lease); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s err = %v", tc.name, err)
		}
	}
	if err := (&Service{Nodes: staticNodes{}, Coordinator: &mocks.Coordinator{}, Queue: NewQueue(clock)}).cleanupCoordinatedLoad(context.Background(), "job-a", inst); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("cleanup node err = %v", err)
	}
}

func TestServiceSubmitErrorPaths(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(30, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	base := func(store *runtimeStore) *Service {
		admission := &mocks.AdmissionController{}
		return &Service{
			Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
			Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Queue:  NewQueue(clock),
			Store:  store,
			Clock:  clock,
			Presets: map[string]domain.Preset{
				preset.ID: preset,
			},
		}
	}
	if _, err := (&Service{}).Submit(context.Background(), fixtures.MakeJob()); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("validate err = %v", err)
	}
	if _, err := base(&runtimeStore{}).Submit(context.Background(), domain.Job{}); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("job id err = %v", err)
	}
	saveErr := errors.New("save job")
	if _, err := base(&runtimeStore{saveJobErr: saveErr, saveJobErrAt: 1}).Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID))); !errors.Is(err, saveErr) {
		t.Fatalf("initial save err = %v", err)
	}
	fleetErr := errors.New("fleet")
	service := base(&runtimeStore{})
	service.Fleet = staticFleet{err: fleetErr}
	if _, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID))); !errors.Is(err, fleetErr) {
		t.Fatalf("fleet err = %v", err)
	}
	placeErr := errors.New("place")
	service = base(&runtimeStore{})
	service.Placer = fakePlacer{decision: domain.PlacementDecision{JobID: "job-a"}, err: placeErr}
	if _, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))); !errors.Is(err, placeErr) {
		t.Fatalf("place err = %v", err)
	}
	if got := service.Store.(*runtimeStore).jobs["job-a"].Status; got != domain.JobFailed {
		t.Fatalf("failed job status = %s", got)
	}
	if _, err := (&Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionQueued}},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{saveJobErr: saveErr, saveJobErrAt: 2},
		Clock:  clock,
	}).Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"))); !errors.Is(err, saveErr) {
		t.Fatalf("queued save err = %v", err)
	}
	for _, tc := range []struct {
		name  string
		store *runtimeStore
	}{
		{name: "instance", store: &runtimeStore{saveInstanceErr: errors.New("instance")}},
		{name: "lease", store: &runtimeStore{saveLeaseErr: errors.New("lease")}},
		{name: "final job", store: &runtimeStore{saveJobErr: errors.New("final"), saveJobErrAt: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := base(tc.store).Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)))
			if err == nil {
				t.Fatal("expected persistence error")
			}
		})
	}
	preemptService := base(&runtimeStore{})
	preemptService.Placer = fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionHardPreempted, Preempted: []string{"missing"}}}
	if _, err := preemptService.Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID))); err == nil {
		t.Fatal("expected preemption error")
	}
	resolveService := base(&runtimeStore{})
	resolveService.Placer = fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", Action: domain.ActionLoadedNew}}
	if _, err := resolveService.Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID))); err == nil {
		t.Fatal("expected resolve error")
	}
}

func TestServiceLocalSubmitCompensationErrorBranches(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(31, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	base := func(store *runtimeStore, agent *mocks.NodeAgent, admission *mocks.AdmissionController, decision domain.PlacementDecision) *Service {
		return &Service{
			Placer: fakePlacer{decision: decision},
			Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Queue:  NewQueue(clock),
			Store:  store,
			Clock:  clock,
			Presets: map[string]domain.Preset{
				preset.ID: preset,
			},
		}
	}
	loadDecision := domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}
	for _, tc := range []struct {
		name  string
		store *runtimeStore
	}{
		{name: "instance", store: &runtimeStore{saveInstanceErr: errors.New("save instance")}},
		{name: "lease", store: &runtimeStore{saveLeaseErr: errors.New("save lease")}},
		{name: "job", store: &runtimeStore{saveJobErr: errors.New("save job"), saveJobErrAt: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := mocks.NewNodeAgent(node)
			agent.UnloadErr = errors.New("cleanup unload")
			_, err := base(tc.store, agent, &mocks.AdmissionController{}, loadDecision).Submit(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)))
			if err == nil || !strings.Contains(err.Error(), "cleanup unload") {
				t.Fatalf("Submit err = %v", err)
			}
		})
	}

	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	lease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: node.ID}
	admission := &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true, ReleaseErr: errors.New("owner release")}
	decision := domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionHardPreempted, Preempted: []string{victim.ID}, Replacements: []domain.Replacement{{InstanceID: "not-preempted", NodeID: node.ID}}}
	service := base(&runtimeStore{leases: map[string]domain.Lease{lease.ID: lease}}, mocks.NewNodeAgent(node), admission, decision)
	service.Fleet = staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}}}
	_, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if err == nil || !strings.Contains(err.Error(), "not-preempted") || !strings.Contains(err.Error(), "owner release") {
		t.Fatalf("preemption/release err = %v", err)
	}

	admission = &mocks.AdmissionController{LeaseForInstVal: lease, LeaseForInstFound: true}
	service = base(&runtimeStore{leases: map[string]domain.Lease{lease.ID: lease}}, mocks.NewNodeAgent(node), admission, decision)
	service.Fleet = staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}}}
	_, err = service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if err == nil || !strings.Contains(err.Error(), "not-preempted") || strings.Contains(err.Error(), "owner release") {
		t.Fatalf("preemption err = %v", err)
	}
}

func TestServiceCompleteFailAndReleaseLifecycleBranches(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(32, 0).UTC())
	store := &runtimeStore{}
	service := leaseLifecycleService(clock, store)
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))

	if err := (&Service{}).CompleteJob(context.Background(), job, domain.Lease{}); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("complete validate err = %v", err)
	}
	saveErr := errors.New("save job")
	if err := leaseLifecycleService(clock, &runtimeStore{saveJobErr: saveErr}).CompleteJob(context.Background(), job, domain.Lease{}); !errors.Is(err, saveErr) {
		t.Fatalf("complete save err = %v", err)
	}
	if err := service.CompleteJob(context.Background(), domain.Job{ID: job.ID, Error: "old"}, domain.Lease{}); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	if got := store.jobs[job.ID]; got.Status != domain.JobDone || got.Error != "" {
		t.Fatalf("complete job = %+v", got)
	}
	completeErr := errors.New("complete coordinator")
	coordinator := &mocks.Coordinator{CompleteErr: completeErr}
	service.Coordinator = coordinator
	if err := service.CompleteJob(context.Background(), job, domain.Lease{JobID: job.ID}); !errors.Is(err, completeErr) {
		t.Fatalf("coordinator complete err = %v", err)
	}

	if err := (&Service{}).FailJob(context.Background(), job, domain.Lease{}, errors.New("cause")); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("fail validate err = %v", err)
	}
	if err := leaseLifecycleService(clock, &runtimeStore{saveJobErr: saveErr}).FailJob(context.Background(), job, domain.Lease{}, errors.New("cause")); !errors.Is(err, saveErr) {
		t.Fatalf("fail save err = %v", err)
	}
	service = leaseLifecycleService(clock, &runtimeStore{})
	if err := service.FailJob(context.Background(), job, domain.Lease{}, nil); err != nil {
		t.Fatalf("FailJob nil cause: %v", err)
	}
	if got := service.Store.(*runtimeStore).jobs[job.ID]; got.Status != domain.JobFailed || got.Error != "" {
		t.Fatalf("failed job nil cause = %+v", got)
	}
	failErr := errors.New("fail coordinator")
	service.Coordinator = &mocks.Coordinator{FailErr: failErr}
	if err := service.FailJob(context.Background(), job, domain.Lease{JobID: job.ID}, errors.New("cause")); !errors.Is(err, failErr) {
		t.Fatalf("coordinator fail err = %v", err)
	}

	finishStore := &runtimeStore{leases: map[string]domain.Lease{"lease-finish": {ID: "lease-finish", JobID: "job-finish"}}}
	finishCoordinator := &mocks.Coordinator{}
	finishService := leaseLifecycleService(clock, finishStore)
	finishService.Coordinator = finishCoordinator
	finishJob := fixtures.MakeJob(fixtures.WithJobID("job-finish"))
	finishLease := domain.Lease{ID: "lease-finish", JobID: finishJob.ID}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := finishService.FinishJob(canceledCtx, finishJob, finishLease, nil); err != nil {
		t.Fatalf("FinishJob success: %v", err)
	}
	if got := finishStore.jobs[finishJob.ID]; got.Status != domain.JobDone || got.Error != "" {
		t.Fatalf("finished job = %+v", got)
	}
	if _, ok := finishStore.leases[finishLease.ID]; ok || strings.Join(finishCoordinator.Calls, ",") != "complete:job-finish,release:job-finish" {
		t.Fatalf("finish leases=%+v coordinator=%+v", finishStore.leases, finishCoordinator.Calls)
	}

	finishStore = &runtimeStore{leases: map[string]domain.Lease{"lease-fail": {ID: "lease-fail", JobID: "job-fail"}}}
	finishCoordinator = &mocks.Coordinator{}
	finishService = leaseLifecycleService(clock, finishStore)
	finishService.Coordinator = finishCoordinator
	finishJob = fixtures.MakeJob(fixtures.WithJobID("job-fail"))
	finishLease = domain.Lease{ID: "lease-fail", JobID: finishJob.ID}
	cause := errors.New("upstream failed")
	if err := finishService.FinishJob(context.Background(), finishJob, finishLease, cause); err != nil {
		t.Fatalf("FinishJob failure: %v", err)
	}
	if got := finishStore.jobs[finishJob.ID]; got.Status != domain.JobFailed || got.Error != cause.Error() {
		t.Fatalf("failed finish job = %+v", got)
	}
	if _, ok := finishStore.leases[finishLease.ID]; ok || strings.Join(finishCoordinator.Calls, ",") != "fail:job-fail:upstream failed,release:job-fail" {
		t.Fatalf("fail finish leases=%+v coordinator=%+v", finishStore.leases, finishCoordinator.Calls)
	}

	completeErr = errors.New("finish complete")
	finishService = leaseLifecycleService(clock, &runtimeStore{})
	finishService.Coordinator = &mocks.Coordinator{CompleteErr: completeErr}
	if err := finishService.FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-terminal")), domain.Lease{JobID: "job-terminal"}, nil); !errors.Is(err, completeErr) {
		t.Fatalf("FinishJob terminal err = %v", err)
	}
	if err := (&Service{Coordinator: &mocks.Coordinator{}}).FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-invalid")), domain.Lease{JobID: "job-invalid"}, nil); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("FinishJob validate err = %v", err)
	}
	failFinishErr := errors.New("finish fail")
	finishService = leaseLifecycleService(clock, &runtimeStore{})
	finishService.Coordinator = &mocks.Coordinator{FailErr: failFinishErr}
	if err := finishService.FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-terminal-fail")), domain.Lease{JobID: "job-terminal-fail"}, cause); !errors.Is(err, failFinishErr) {
		t.Fatalf("FinishJob coordinator fail err = %v", err)
	}

	localFinishStore := &runtimeStore{leases: map[string]domain.Lease{"lease-local": {ID: "lease-local", JobID: "job-local"}}}
	localFinish := leaseLifecycleService(clock, localFinishStore)
	localLease := domain.Lease{ID: "lease-local", JobID: "job-local"}
	if err := localFinish.FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-local")), localLease, nil); err != nil {
		t.Fatalf("local FinishJob complete: %v", err)
	}
	if got := localFinishStore.jobs["job-local"]; got.Status != domain.JobDone || got.Error != "" {
		t.Fatalf("local complete job = %+v", got)
	}
	if _, ok := localFinishStore.leases[localLease.ID]; ok {
		t.Fatalf("local complete lease still present: %+v", localFinishStore.leases)
	}

	localFinishStore = &runtimeStore{leases: map[string]domain.Lease{"lease-local-fail": {ID: "lease-local-fail", JobID: "job-local-fail"}}}
	localFinish = leaseLifecycleService(clock, localFinishStore)
	localLease = domain.Lease{ID: "lease-local-fail", JobID: "job-local-fail"}
	if err := localFinish.FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-local-fail")), localLease, cause); err != nil {
		t.Fatalf("local FinishJob fail: %v", err)
	}
	if got := localFinishStore.jobs["job-local-fail"]; got.Status != domain.JobFailed || got.Error != cause.Error() {
		t.Fatalf("local failed job = %+v", got)
	}

	localFinish = leaseLifecycleService(clock, &runtimeStore{saveJobErr: completeErr})
	if err := localFinish.FinishJob(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-local-terminal")), domain.Lease{}, nil); !errors.Is(err, completeErr) {
		t.Fatalf("local FinishJob terminal err = %v", err)
	}

	listErr := errors.New("list leases")
	if err := leaseLifecycleService(clock, &runtimeStore{listLeaseErr: listErr}).Release(context.Background(), "lease-a"); !errors.Is(err, listErr) {
		t.Fatalf("release list err = %v", err)
	}
	ownerAdmission := &mocks.AdmissionController{}
	ownerService := leaseLifecycleService(clock, &runtimeStore{})
	ownerService.Owners = staticNodes{admissions: map[string]ports.AdmissionController{"node-a": ownerAdmission}}
	if err := ownerService.ReleaseJob(context.Background(), domain.Lease{ID: "lease-a", NodeID: "node-a"}); err != nil {
		t.Fatalf("owner ReleaseJob: %v", err)
	}
	if strings.Join(ownerAdmission.Calls, ",") != "release:lease-a" {
		t.Fatalf("owner calls = %+v", ownerAdmission.Calls)
	}
}

func TestServiceCoordinatedCompensationErrorBranches(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(33, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	payload := []byte(`{"job":"a"}`)
	loadDecision := domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}
	base := func(store *runtimeStore, agent *mocks.NodeAgent, coordinator *mocks.Coordinator, decision domain.PlacementDecision) *Service {
		if coordinator.Decision.JobID == "" {
			coordinator.Decision = decision
		}
		if coordinator.Lease.ID == "" {
			coordinator.Lease = domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: decision.Claim}
		}
		admission := &mocks.AdmissionController{}
		return &Service{
			Placer:      fakePlacer{decision: decision},
			Fleet:       staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:       staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Owners:      staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
			Coordinator: coordinator,
			Queue:       NewQueue(clock),
			Store:       store,
			JobLog:      &recordingJobLog{},
			Clock:       clock,
			Presets: map[string]domain.Preset{
				preset.ID: preset,
			},
		}
	}
	for _, tc := range []struct {
		name  string
		store *runtimeStore
	}{
		{name: "instance", store: &runtimeStore{saveInstanceErr: errors.New("save instance")}},
		{name: "lease", store: &runtimeStore{saveLeaseErr: errors.New("save lease")}},
		{name: "job", store: &runtimeStore{saveJobErr: errors.New("save job"), saveJobErrAt: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := mocks.NewNodeAgent(node)
			agent.UnloadErr = errors.New("cleanup unload")
			coordinator := &mocks.Coordinator{Decision: loadDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: loadDecision.Claim}}
			_, err := base(tc.store, agent, coordinator, loadDecision).SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload)
			if err == nil || !strings.Contains(err.Error(), "cleanup unload") {
				t.Fatalf("SubmitWithPayload err = %v", err)
			}
		})
	}
	agent := mocks.NewNodeAgent(node)
	agent.UnloadErr = errors.New("cleanup unload")
	coordinator := &mocks.Coordinator{Decision: loadDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: loadDecision.Claim}, RunningErr: errors.New("mark running")}
	_, err := base(&runtimeStore{}, agent, coordinator, loadDecision).SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload)
	if err == nil || !strings.Contains(err.Error(), "mark running") || !strings.Contains(err.Error(), "cleanup unload") {
		t.Fatalf("mark running err = %v", err)
	}
	coordinator = &mocks.Coordinator{Decision: loadDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: loadDecision.Claim}, RunningErr: errors.New("mark running clean")}
	_, err = base(&runtimeStore{}, mocks.NewNodeAgent(node), coordinator, loadDecision).SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload)
	if err == nil || !strings.Contains(err.Error(), "mark running clean") {
		t.Fatalf("clean mark running err = %v", err)
	}

	hookErr := errors.New("hook failed")
	releaseErr := errors.New("coordinator release")
	coordinator = &mocks.Coordinator{Decision: loadDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: loadDecision.Claim}, ReleaseErr: releaseErr}
	_, err = base(&runtimeStore{}, mocks.NewNodeAgent(node), coordinator, loadDecision).SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload, SubmitHooks{BeforeColdLoad: func(context.Context, domain.PlacementDecision) error {
		return hookErr
	}})
	if !errors.Is(err, hookErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("hook release err = %v", err)
	}

	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	victimLease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: node.ID}
	preemptAdmission := &mocks.AdmissionController{LeaseForInstVal: victimLease, LeaseForInstFound: true}
	preemptDecision := domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionHardPreempted, Preempted: []string{victim.ID}, Replacements: []domain.Replacement{{InstanceID: "not-preempted", NodeID: node.ID}}}
	coordinator = &mocks.Coordinator{Decision: preemptDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: preemptDecision.Claim}, ReleaseErr: releaseErr}
	service := base(&runtimeStore{leases: map[string]domain.Lease{victimLease.ID: victimLease}}, mocks.NewNodeAgent(node), coordinator, preemptDecision)
	service.Fleet = staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}}}
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{node.ID: preemptAdmission}}
	_, err = service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload)
	if err == nil || !strings.Contains(err.Error(), "not-preempted") || !strings.Contains(err.Error(), "coordinator release") {
		t.Fatalf("coordinated preemption release err = %v", err)
	}

	coordinator = &mocks.Coordinator{Decision: preemptDecision, Lease: domain.Lease{ID: "lease-owner", JobID: "job-a", NodeID: node.ID, Claim: preemptDecision.Claim}}
	service = base(&runtimeStore{leases: map[string]domain.Lease{victimLease.ID: victimLease}}, mocks.NewNodeAgent(node), coordinator, preemptDecision)
	service.Fleet = staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}}}
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{node.ID: preemptAdmission}}
	_, err = service.SubmitWithPayload(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)), payload)
	if err == nil || !strings.Contains(err.Error(), "not-preempted") || strings.Contains(err.Error(), "coordinator release") {
		t.Fatalf("coordinated preemption err = %v", err)
	}
}

func TestServiceDirectRemainingBranchContracts(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(node.ID))
	service := &Service{Nodes: staticNodes{}, Store: &runtimeStore{}}
	if err := service.finishPreemption(context.Background(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{}, map[string]domain.Lease{}, map[string]domain.ModelInstance{}); err == nil || !strings.Contains(err.Error(), "missing from preemption inspection") {
		t.Fatalf("missing preempted victim err = %v", err)
	}
	preemptLease := domain.Lease{ID: "lease-a", JobID: "job-a", InstanceID: inst.ID, NodeID: node.ID}
	preemptDecision := domain.PlacementDecision{Preempted: []string{inst.ID}}
	preemptLeases := map[string]domain.Lease{inst.ID: preemptLease}
	preemptVictims := map[string]domain.ModelInstance{inst.ID: inst}
	service = &Service{
		Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Store: &runtimeStore{},
	}
	if err := service.finishPreemption(context.Background(), preemptDecision, domain.FleetSnapshot{}, preemptLeases, preemptVictims); err == nil || !strings.Contains(err.Error(), "owner admission resolver") {
		t.Fatalf("missing owner resolver err = %v", err)
	}
	service = &Service{
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{},
		Store:  &runtimeStore{},
	}
	if err := service.finishPreemption(context.Background(), preemptDecision, domain.FleetSnapshot{}, preemptLeases, preemptVictims); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("owner resolver err = %v", err)
	}
	releaseErr := errors.New("owner release")
	service = &Service{
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{ReleaseErr: releaseErr}}},
		Store:  &runtimeStore{},
	}
	if err := service.finishPreemption(context.Background(), preemptDecision, domain.FleetSnapshot{}, preemptLeases, preemptVictims); !errors.Is(err, releaseErr) {
		t.Fatalf("owner release err = %v", err)
	}
	service = &Service{Nodes: staticNodes{}, Store: &runtimeStore{}}
	if err := service.cleanupLoadedInstance(context.Background(), inst, true, func() error { return nil }); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("cleanup missing node err = %v", err)
	}
	targets := admissionPreemptions(domain.Job{ID: "job-a"}, domain.PlacementDecision{Preempted: []string{"inst-a"}}, map[string]domain.Lease{
		"inst-a": {ID: "lease-a"},
	})
	if len(targets) != 1 || targets[0].Reason != "preempted for job-a" {
		t.Fatalf("preemption targets = %+v", targets)
	}
}

func TestServiceDrain(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(40, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}, admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}
	if _, err := service.Drain(context.Background(), 0); err == nil || !strings.Contains(err.Error(), "positive") {
		t.Fatalf("limit err = %v", err)
	}
	results, err := service.Drain(context.Background(), 1)
	if err != nil || len(results) != 0 {
		t.Fatalf("empty drain = %+v %v", results, err)
	}
	service.Queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	results, err = service.Drain(context.Background(), 1)
	if err != nil || len(results) != 1 || results[0].Lease.ID != "lease_offer_job-a" {
		t.Fatalf("drain = %+v %v", results, err)
	}
	service.Queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-b"), fixtures.WithPreset(preset.ID)))
	service.Store = &runtimeStore{saveJobErr: errors.New("save"), saveJobErrAt: 1}
	if _, err := service.Drain(context.Background(), 1); err == nil {
		t.Fatal("expected submit error")
	}
}

func TestServiceDrainSkipsNoFitHeadJob(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(41, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: jobDecisionPlacer{decisions: map[string]domain.PlacementDecision{
			"job-blocked": {JobID: "job-blocked", Action: domain.ActionQueued},
			"job-ready":   {JobID: "job-ready", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew},
		}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}, admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}
	service.Queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-blocked"), fixtures.WithPreset(preset.ID), fixtures.Interactive))
	service.Queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-ready"), fixtures.WithPreset(preset.ID), fixtures.Background))

	results, err := service.Drain(context.Background(), 1)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(results) != 1 || results[0].Decision.JobID != "job-ready" {
		t.Fatalf("drain results = %+v", results)
	}
	if service.Queue.Len() != 1 {
		t.Fatalf("queue len = %d", service.Queue.Len())
	}
	remaining, ok := service.Queue.Dequeue()
	if !ok || remaining.ID != "job-blocked" {
		t.Fatalf("remaining = %+v ok=%v", remaining, ok)
	}
}

func TestServiceReleaseAndExpireLeases(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	store := &runtimeStore{leases: map[string]domain.Lease{
		"expired": {ID: "expired", ExpiresAt: time.Unix(99, 0).UTC()},
		"future":  {ID: "future", ExpiresAt: time.Unix(101, 0).UTC()},
		"open":    {ID: "open"},
	}}
	service := leaseLifecycleService(clock, store)

	if err := service.Release(context.Background(), "future"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, ok := store.leases["future"]; ok {
		t.Fatalf("future lease was not released: %+v", store.leases)
	}
	expired, err := service.ExpireLeases(context.Background())
	if err != nil {
		t.Fatalf("ExpireLeases: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired = %d", expired)
	}
	if _, ok := store.leases["expired"]; ok {
		t.Fatalf("expired lease still stored: %+v", store.leases)
	}
	if _, ok := store.leases["open"]; !ok {
		t.Fatalf("open lease should remain: %+v", store.leases)
	}
}

func TestServiceReleaseJobFallsBackToOwnerLeaseAfterCoordinatorRestart(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	lease := domain.Lease{ID: "lease-spark-29", JobID: "gateway-6-1", NodeID: "spark", ExpiresAt: time.Unix(99, 0).UTC()}
	store := &runtimeStore{leases: map[string]domain.Lease{lease.ID: lease}}
	admission := &mocks.AdmissionController{}
	service := leaseLifecycleService(clock, store)
	service.Coordinator = &mocks.Coordinator{ReleaseErr: fmt.Errorf("job %q has no committed lease", lease.JobID)}
	service.Owners = staticNodes{admissions: map[string]ports.AdmissionController{lease.NodeID: admission}}

	expired, err := service.ExpireLeases(context.Background())
	if err != nil {
		t.Fatalf("ExpireLeases: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired = %d", expired)
	}
	if _, ok := store.leases[lease.ID]; ok {
		t.Fatalf("stale lease still stored: %+v", store.leases)
	}
	if got := strings.Join(admission.Calls, ","); got != "release:"+lease.ID {
		t.Fatalf("admission calls = %s", got)
	}
}

func TestServiceLeaseLifecycleErrors(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	if err := (&Service{}).Release(context.Background(), "lease-a"); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("release validate err = %v", err)
	}
	if err := leaseLifecycleService(clock, &runtimeStore{}).Release(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "lease id") {
		t.Fatalf("release id err = %v", err)
	}
	deleteErr := errors.New("delete lease")
	if err := leaseLifecycleService(clock, &runtimeStore{deleteLeaseErr: deleteErr}).Release(context.Background(), "lease-a"); !errors.Is(err, deleteErr) {
		t.Fatalf("release delete err = %v", err)
	}
	listErr := errors.New("list leases")
	if _, err := leaseLifecycleService(clock, &runtimeStore{listLeaseErr: listErr}).ExpireLeases(context.Background()); !errors.Is(err, listErr) {
		t.Fatalf("expire list err = %v", err)
	}
	if _, err := (&Service{}).ExpireLeases(context.Background()); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("expire validate err = %v", err)
	}
	if expired, err := leaseLifecycleService(clock, &runtimeStore{
		leases:         map[string]domain.Lease{"expired": {ID: "expired", ExpiresAt: time.Unix(99, 0).UTC()}},
		deleteLeaseErr: deleteErr,
	}).ExpireLeases(context.Background()); !errors.Is(err, deleteErr) || expired != 0 {
		t.Fatalf("expire delete err/count = %v %d", err, expired)
	}
}

func TestServiceResolveAndPreemptionErrors(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(50, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(0))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("warm-a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	preemptLease := domain.Lease{ID: "lease-a", JobID: "victim-job", InstanceID: inst.ID, NodeID: node.ID, Claim: inst.Claim}
	goodPreemptAdmission := func() *mocks.AdmissionController {
		return &mocks.AdmissionController{LeaseForInstVal: preemptLease, LeaseForInstFound: true}
	}
	service := &Service{
		Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
		Store: &runtimeStore{instances: map[string]domain.ModelInstance{inst.ID: inst}},
		Queue: NewQueue(clock),
		Clock: clock,
		Presets: map[string]domain.Preset{
			preset.ID:       preset,
			preset.ModelRef: preset,
		},
	}
	got, err := service.resolveInstance(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{InstanceID: inst.ID}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
	if err != nil || got.ID != inst.ID {
		t.Fatalf("warm resolve = %+v %v", got, err)
	}
	errorChecks := []struct {
		name string
		fn   func() error
	}{
		{name: "missing warm", fn: func() error {
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{InstanceID: "missing"}, domain.FleetSnapshot{})
			return err
		}},
		{name: "missing node decision", fn: func() error {
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Action: domain.ActionLoadedNew}, domain.FleetSnapshot{})
			return err
		}},
		{name: "unknown preset", fn: func() error {
			_, err := service.resolvePreset(fixtures.MakeJob(fixtures.WithPreset("missing")))
			return err
		}},
		{name: "unknown model", fn: func() error {
			_, err := service.resolvePreset(domain.Job{ID: "job-a", Model: "missing"})
			return err
		}},
		{name: "missing model", fn: func() error {
			_, err := service.resolvePreset(domain.Job{ID: "job-a"})
			return err
		}},
		{name: "node agent", fn: func() error {
			missing := fixtures.MakeNode(fixtures.WithNodeID("missing"))
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{NodeID: missing.ID}, domain.FleetSnapshot{Nodes: []domain.Node{missing}})
			return err
		}},
		{name: "missing selected node", fn: func() error {
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{NodeID: node.ID}, domain.FleetSnapshot{})
			return err
		}},
		{name: "resolve preset in load", fn: func() error {
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset("missing")), domain.PlacementDecision{NodeID: node.ID}, domain.FleetSnapshot{Nodes: []domain.Node{node}})
			return err
		}},
		{name: "tuning", fn: func() error {
			badNode := node
			badNode.Accelerators = []domain.Accelerator{{Index: 0, VRAMTotalMB: 1}, {Index: 1, VRAMTotalMB: 0}}
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{NodeID: node.ID, AcceleratorSet: []int{0, 1}}, domain.FleetSnapshot{Nodes: []domain.Node{badNode}})
			return err
		}},
		{name: "load", fn: func() error {
			agent := mocks.NewNodeAgent(node)
			agent.LoadErr = errors.New("load")
			service.Nodes = staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}}
			_, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{NodeID: node.ID}, domain.FleetSnapshot{Nodes: []domain.Node{node}})
			service.Nodes = staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}}
			return err
		}},
		{name: "missing preempted", fn: func() error {
			return service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{"missing"}}, domain.FleetSnapshot{})
		}},
		{name: "preempt node", fn: func() error {
			return (&Service{Nodes: staticNodes{}, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: &runtimeStore{leases: map[string]domain.Lease{preemptLease.ID: preemptLease}}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "unload", fn: func() error {
			agent := mocks.NewNodeAgent(node)
			agent.UnloadErr = errors.New("unload")
			return (&Service{Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}}, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: &runtimeStore{leases: map[string]domain.Lease{preemptLease.ID: preemptLease}}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "delete", fn: func() error {
			return (&Service{Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: &runtimeStore{leases: map[string]domain.Lease{preemptLease.ID: preemptLease}, deleteErr: errors.New("delete")}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt owner resolver missing", fn: func() error {
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt owner unreachable", fn: func() error {
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Owners: staticNodes{}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt lease inspection unsupported", fn: func() error {
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admissionOnly{}}}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt lease inspection error", fn: func() error {
			admission := &mocks.AdmissionController{LeaseForInstErr: errors.New("lease lookup")}
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt missing owner lease", fn: func() error {
			admission := &mocks.AdmissionController{}
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}, Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "coordinated preempt delete lease", fn: func() error {
			admission := &mocks.AdmissionController{LeaseForInstVal: domain.Lease{ID: "lease-a", InstanceID: inst.ID, NodeID: node.ID}, LeaseForInstFound: true}
			return (&Service{Coordinator: &mocks.Coordinator{}, Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}}, Store: &runtimeStore{deleteLeaseErr: errors.New("delete lease")}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "missing requeued", fn: func() error {
			return service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Requeued: []string{"missing"}}, domain.FleetSnapshot{})
		}},
		{name: "requeue missing owner lease job", fn: func() error {
			return service.enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "requeue store cannot read jobs", fn: func() error {
			return (&Service{Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: storeWithoutJobReader{lease: preemptLease}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}, Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "requeue job read", fn: func() error {
			return (&Service{Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: &runtimeStore{leases: map[string]domain.Lease{preemptLease.ID: preemptLease}}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}, Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "requeue save", fn: func() error {
			return (&Service{Nodes: service.Nodes, Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: goodPreemptAdmission()}}, Store: &runtimeStore{leases: map[string]domain.Lease{preemptLease.ID: preemptLease}, jobs: map[string]domain.Job{"victim-job": fixtures.MakeJob(fixtures.WithJobID("victim-job"))}, saveJobErr: errors.New("save"), saveJobErrAt: 1}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), fixtures.MakeJob(), domain.PlacementDecision{Preempted: []string{inst.ID}, Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
	}
	for _, check := range errorChecks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.fn(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if got, err := service.resolvePreset(domain.Job{ID: "job-model", Model: preset.ModelRef}); err != nil || got.ID != preset.ID {
		t.Fatalf("resolve model = %+v %v", got, err)
	}
	loaded, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{NodeID: node.ID, Claim: fixtures.MakeClaim(3, 4)}, domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err != nil {
		t.Fatalf("resolve load: %v", err)
	}
	if loaded.Claim != (domain.Claim{WeightsMB: 3, KVReservedMB: 4}) {
		t.Fatalf("claim = %+v", loaded.Claim)
	}
}

func TestServiceResolveWarmInstanceUsesOwnerNode(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(50, 0).UTC())
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(0))
	colliding := fixtures.MakeInstance(fixtures.WithInstanceID("inst_1"), fixtures.WithInstancePreset("other"), fixtures.OnNode(nodeA.ID))
	target := fixtures.MakeInstance(fixtures.WithInstanceID("inst_1"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(nodeB.ID))
	service := &Service{
		Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{
			nodeA.ID: mocks.NewNodeAgent(nodeA),
			nodeB.ID: mocks.NewNodeAgent(nodeB),
		}},
		Queue: NewQueue(clock),
		Clock: clock,
	}

	got, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{
		InstanceID: target.ID,
		NodeID:     nodeB.ID,
		Action:     domain.ActionWarmInstance,
	}, domain.FleetSnapshot{Instances: []domain.ModelInstance{colliding, target}})
	if err != nil {
		t.Fatalf("resolveInstance: %v", err)
	}
	if got.NodeID != nodeB.ID || got.PresetID != preset.ID {
		t.Fatalf("resolved wrong warm instance: %+v", got)
	}
}

func TestServiceRequeuePayloadValidation(t *testing.T) {
	errBoom := errors.New("boom")
	for _, tt := range []struct {
		name   string
		log    JobLog
		jobID  string
		want   string
		wantOK bool
	}{
		{name: "missing reader", log: putOnlyJobLog{}, jobID: "job-a", want: "payload reader"},
		{name: "reader error", log: &recordingJobLog{readErr: errBoom}, jobID: "job-a", want: "boom"},
		{name: "wrong job", log: &recordingJobLog{jobs: map[string]domain.Job{"job-a": fixtures.MakeJob(fixtures.WithJobID("other"))}, payloads: map[string][]byte{"job-a": []byte(`{}`)}}, jobID: "job-a", want: "returned"},
		{name: "empty payload", log: &recordingJobLog{jobs: map[string]domain.Job{"job-a": fixtures.MakeJob(fixtures.WithJobID("job-a"))}, payloads: map[string][]byte{"job-a": nil}}, jobID: "job-a", want: "no rescue payload"},
		{name: "ok", log: &recordingJobLog{jobs: map[string]domain.Job{"job-a": fixtures.MakeJob(fixtures.WithJobID("job-a"))}, payloads: map[string][]byte{"job-a": []byte(`{"job":"a"}`)}}, jobID: "job-a", wantOK: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := (&Service{JobLog: tt.log}).requeuePayload(context.Background(), tt.jobID)
			if tt.wantOK {
				if err != nil || string(payload) != `{"job":"a"}` {
					t.Fatalf("payload=%s err=%v", payload, err)
				}
				payload[0] = '['
				payloadAgain, err := (&Service{JobLog: tt.log}).requeuePayload(context.Background(), tt.jobID)
				if err != nil || string(payloadAgain) != `{"job":"a"}` {
					t.Fatalf("payload was not cloned: %s err=%v", payloadAgain, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

type replacementHarness struct {
	service     *Service
	node        domain.Node
	victim      domain.ModelInstance
	lease       domain.Lease
	replacement domain.Replacement
	fleet       domain.FleetSnapshot
	agent       *mocks.NodeAgent
	admission   *mocks.AdmissionController
	store       *runtimeStore
}

func newReplacementHarness() replacementHarness {
	clock := mocks.NewFakeClock(time.Unix(60, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-b"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	preset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"), fixtures.WithWeights(1))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim-a"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode("node-a"),
		fixtures.WithClaim(fixtures.MakeClaim(7, 3)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	lease := domain.Lease{ID: "lease-victim", JobID: "victim-job", InstanceID: victim.ID, NodeID: victim.NodeID, Claim: victim.Claim, Priority: victim.Priority}
	replacement := domain.Replacement{InstanceID: victim.ID, NodeID: node.ID, AcceleratorSet: []int{0}}
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	store := &runtimeStore{
		jobs: map[string]domain.Job{
			"victim-job": fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(preset.ID), fixtures.Background),
		},
	}
	service := &Service{
		Nodes:   staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Owners:  staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Store:   store,
		Clock:   clock,
		Presets: map[string]domain.Preset{preset.ID: preset},
	}
	return replacementHarness{
		service:     service,
		node:        node,
		victim:      victim,
		lease:       lease,
		replacement: replacement,
		fleet:       domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}},
		agent:       agent,
		admission:   admission,
		store:       store,
	}
}

func leaseLifecycleService(clock *mocks.FakeClock, store *runtimeStore) *Service {
	return &Service{
		Placer: fakePlacer{},
		Fleet:  staticFleet{},
		Nodes:  staticNodes{},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
	}
}

type fakePlacer struct {
	decision domain.PlacementDecision
	err      error
}

func (p fakePlacer) Place(context.Context, domain.Job, domain.FleetSnapshot) (domain.PlacementDecision, error) {
	return p.decision, p.err
}

type jobDecisionPlacer struct {
	decisions map[string]domain.PlacementDecision
	err       error
}

func (p jobDecisionPlacer) Place(_ context.Context, job domain.Job, _ domain.FleetSnapshot) (domain.PlacementDecision, error) {
	if p.err != nil {
		return domain.PlacementDecision{}, p.err
	}
	decision := p.decisions[job.ID]
	if decision.JobID == "" {
		decision.JobID = job.ID
	}
	return decision, nil
}

type cleanupContextCoordinator struct {
	mocks.Coordinator
	releaseContextErr error
}

func (c *cleanupContextCoordinator) Release(ctx context.Context, jobID string) error {
	c.releaseContextErr = ctx.Err()
	if c.releaseContextErr != nil {
		return c.releaseContextErr
	}
	return c.Coordinator.Release(ctx, jobID)
}

type staticFleet struct {
	fleet domain.FleetSnapshot
	err   error
}

func (f staticFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	if f.err != nil {
		return domain.FleetSnapshot{}, f.err
	}
	return f.fleet, nil
}

type staticNodes struct {
	agents     map[string]*mocks.NodeAgent
	admissions map[string]ports.AdmissionController
}

func (n staticNodes) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent := n.agents[nodeID]
	if agent == nil {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

func (n staticNodes) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	admission := n.admissions[nodeID]
	if admission == nil {
		return nil, domain.ErrUnreachable
	}
	return admission, nil
}

type admissionOnly struct{}

func (admissionOnly) Offer(context.Context, domain.AdmissionRequest) (domain.LeaseOffer, error) {
	return domain.LeaseOffer{}, nil
}

func (admissionOnly) Commit(context.Context, string, uint64) (domain.Lease, error) {
	return domain.Lease{}, nil
}

func (admissionOnly) Release(context.Context, string) error {
	return nil
}

func (admissionOnly) Preempt(context.Context, string, string) error {
	return nil
}

type inspectableAdmission struct {
	admissionOnly
	lease domain.Lease
	found bool
	err   error
}

func (a inspectableAdmission) LeaseForJob(context.Context, string) (domain.Lease, bool, error) {
	return a.lease, a.found, a.err
}

func (a inspectableAdmission) LeaseForInstance(context.Context, string) (domain.Lease, bool, error) {
	return a.lease, a.found, a.err
}

type emptyCommitAdmission struct {
	admissionOnly
}

func (emptyCommitAdmission) Offer(context.Context, domain.AdmissionRequest) (domain.LeaseOffer, error) {
	return domain.LeaseOffer{OfferID: "offer-empty", Fence: 1}, nil
}

func (emptyCommitAdmission) Commit(context.Context, string, uint64) (domain.Lease, error) {
	return domain.Lease{}, nil
}

type runtimeStore struct {
	jobs            map[string]domain.Job
	leases          map[string]domain.Lease
	instances       map[string]domain.ModelInstance
	saveJobErr      error
	saveJobErrAt    int
	saveJobCalls    int
	saveLeaseErr    error
	listLeaseErr    error
	deleteLeaseErr  error
	saveInstanceErr error
	deleteErr       error
}

type storeWithoutJobReader struct {
	lease domain.Lease
}

func (s storeWithoutJobReader) SaveJob(context.Context, domain.Job) error { return nil }
func (s storeWithoutJobReader) SaveLease(context.Context, domain.Lease) error {
	return nil
}
func (s storeWithoutJobReader) ListLeases(context.Context) ([]domain.Lease, error) {
	return []domain.Lease{s.lease}, nil
}
func (s storeWithoutJobReader) DeleteLease(context.Context, string) error { return nil }
func (s storeWithoutJobReader) SaveInstance(context.Context, domain.ModelInstance) error {
	return nil
}
func (s storeWithoutJobReader) DeleteInstance(context.Context, string) error { return nil }

type recordingJobLog struct {
	job      domain.Job
	payload  []byte
	err      error
	readErr  error
	jobs     map[string]domain.Job
	payloads map[string][]byte
}

func (l *recordingJobLog) PutJob(_ context.Context, job domain.Job, payload []byte) error {
	if l.err != nil {
		return l.err
	}
	if l.jobs == nil {
		l.jobs = map[string]domain.Job{}
	}
	if l.payloads == nil {
		l.payloads = map[string][]byte{}
	}
	l.job = job
	l.payload = append([]byte(nil), payload...)
	l.jobs[job.ID] = job
	l.payloads[job.ID] = append([]byte(nil), payload...)
	return nil
}

func (l *recordingJobLog) Job(_ context.Context, jobID string) (domain.Job, []byte, error) {
	if l.readErr != nil {
		return domain.Job{}, nil, l.readErr
	}
	if l.jobs != nil {
		job, ok := l.jobs[jobID]
		if !ok {
			return domain.Job{}, nil, fmt.Errorf("job %q not found in job log", jobID)
		}
		return job, append([]byte(nil), l.payloads[jobID]...), nil
	}
	if l.job.ID == jobID {
		return l.job, append([]byte(nil), l.payload...), nil
	}
	return domain.Job{}, nil, fmt.Errorf("job %q not found in job log", jobID)
}

type putOnlyJobLog struct{}

func (putOnlyJobLog) PutJob(context.Context, domain.Job, []byte) error { return nil }

func (s *runtimeStore) SaveJob(_ context.Context, job domain.Job) error {
	s.saveJobCalls++
	if s.saveJobErr != nil && (s.saveJobErrAt == 0 || s.saveJobCalls == s.saveJobErrAt) {
		return s.saveJobErr
	}
	if s.jobs == nil {
		s.jobs = map[string]domain.Job{}
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *runtimeStore) Job(_ context.Context, id string) (domain.Job, error) {
	if s.jobs == nil {
		return domain.Job{}, fmt.Errorf("job %q not found", id)
	}
	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, fmt.Errorf("job %q not found", id)
	}
	return job, nil
}

func (s *runtimeStore) SaveLease(_ context.Context, lease domain.Lease) error {
	if s.saveLeaseErr != nil {
		return s.saveLeaseErr
	}
	if s.leases == nil {
		s.leases = map[string]domain.Lease{}
	}
	s.leases[lease.ID] = lease
	return nil
}

func (s *runtimeStore) ListLeases(_ context.Context) ([]domain.Lease, error) {
	if s.listLeaseErr != nil {
		return nil, s.listLeaseErr
	}
	leases := make([]domain.Lease, 0, len(s.leases))
	for _, lease := range s.leases {
		leases = append(leases, lease)
	}
	return leases, nil
}

func (s *runtimeStore) DeleteLease(_ context.Context, id string) error {
	if s.deleteLeaseErr != nil {
		return s.deleteLeaseErr
	}
	delete(s.leases, id)
	return nil
}

func (s *runtimeStore) SaveInstance(_ context.Context, inst domain.ModelInstance) error {
	if s.saveInstanceErr != nil {
		return s.saveInstanceErr
	}
	if s.instances == nil {
		s.instances = map[string]domain.ModelInstance{}
	}
	s.instances[inst.ID] = inst
	return nil
}

func (s *runtimeStore) DeleteInstance(_ context.Context, id string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.instances, id)
	return nil
}
