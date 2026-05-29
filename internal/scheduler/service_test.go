package scheduler

import (
	"context"
	"errors"
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

func TestServiceLoadsAndGrantsLease(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(10, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(12))
	store := &runtimeStore{}
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
		Queue:  NewQueue(clock),
		Store:  store,
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	result, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID)))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Instance.PresetID != preset.ID || result.Lease.ID != "lease-job-a" {
		t.Fatalf("result = %+v", result)
	}
	if got := store.jobs["job-a"].Status; got != domain.JobRunning {
		t.Fatalf("job status = %s", got)
	}
	if len(store.leases) != 1 || len(store.instances) != 1 {
		t.Fatalf("leases=%+v instances=%+v", store.leases, store.instances)
	}
}

func TestServiceRunsColdLoadHookBeforeLoading(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(11, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	agent := mocks.NewNodeAgent(node)
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(12))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(12, 3), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
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

func TestServiceEnactsPreemptionAndRequeuesVictim(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(20, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	targetPreset := fixtures.MakePreset(fixtures.WithPresetID("target-preset"), fixtures.WithWeights(22))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset(victimPreset.ID), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground))
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{victim}
	store := &runtimeStore{instances: map[string]domain.ModelInstance{victim.ID: victim}}
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
		Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}},
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
	if _, ok := store.instances[victim.ID]; ok {
		t.Fatalf("victim still stored: %+v", store.instances)
	}
	if service.Queue.Len() != 1 {
		t.Fatalf("requeue len = %d", service.Queue.Len())
	}
	if len(agent.Calls) < 2 || agent.Calls[0] != "unload:victim-a" || agent.Calls[1] != "load:target-preset" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestServiceSubmitErrorPaths(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(30, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	base := func(store *runtimeStore) *Service {
		return &Service{
			Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
			Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
			Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
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

func TestServiceDrain(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(40, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 1), Action: domain.ActionLoadedNew}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: mocks.NewNodeAgent(node)}},
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
	if err != nil || len(results) != 1 || results[0].Lease.ID != "lease-job-a" {
		t.Fatalf("drain = %+v %v", results, err)
	}
	service.Queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("job-b"), fixtures.WithPreset(preset.ID)))
	service.Store = &runtimeStore{saveJobErr: errors.New("save"), saveJobErrAt: 1}
	if _, err := service.Drain(context.Background(), 1); err == nil {
		t.Fatal("expected submit error")
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
			return service.enactPreemption(context.Background(), domain.PlacementDecision{Preempted: []string{"missing"}}, domain.FleetSnapshot{})
		}},
		{name: "preempt node", fn: func() error {
			return (&Service{Nodes: staticNodes{}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "unload", fn: func() error {
			agent := mocks.NewNodeAgent(node)
			agent.UnloadErr = errors.New("unload")
			return (&Service{Nodes: staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}}, Store: &runtimeStore{}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "delete", fn: func() error {
			return (&Service{Nodes: service.Nodes, Store: &runtimeStore{deleteErr: errors.New("delete")}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), domain.PlacementDecision{Preempted: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
		}},
		{name: "missing requeued", fn: func() error {
			return service.enactPreemption(context.Background(), domain.PlacementDecision{Requeued: []string{"missing"}}, domain.FleetSnapshot{})
		}},
		{name: "requeue save", fn: func() error {
			return (&Service{Store: &runtimeStore{saveJobErr: errors.New("save"), saveJobErrAt: 1}, Queue: NewQueue(clock)}).enactPreemption(context.Background(), domain.PlacementDecision{Requeued: []string{inst.ID}}, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
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
	agents map[string]*mocks.NodeAgent
}

func (n staticNodes) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent := n.agents[nodeID]
	if agent == nil {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
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
