package scheduler

import (
	"context"
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

type fakePlacer struct {
	decision domain.PlacementDecision
	err      error
}

func (p fakePlacer) Place(context.Context, domain.Job, domain.FleetSnapshot) (domain.PlacementDecision, error) {
	return p.decision, p.err
}

type staticFleet struct {
	fleet domain.FleetSnapshot
}

func (f staticFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
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
	jobs      map[string]domain.Job
	leases    map[string]domain.Lease
	instances map[string]domain.ModelInstance
}

func (s *runtimeStore) SaveJob(_ context.Context, job domain.Job) error {
	if s.jobs == nil {
		s.jobs = map[string]domain.Job{}
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *runtimeStore) SaveLease(_ context.Context, lease domain.Lease) error {
	if s.leases == nil {
		s.leases = map[string]domain.Lease{}
	}
	s.leases[lease.ID] = lease
	return nil
}

func (s *runtimeStore) SaveInstance(_ context.Context, inst domain.ModelInstance) error {
	if s.instances == nil {
		s.instances = map[string]domain.ModelInstance{}
	}
	s.instances[inst.ID] = inst
	return nil
}

func (s *runtimeStore) DeleteInstance(_ context.Context, id string) error {
	delete(s.instances, id)
	return nil
}
