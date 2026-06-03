package e2e

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/node"
	"mycelium/internal/peer"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPhase6PeerSubmitAnywhereRecordsSubmitterCoordinator(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(600, 0).UTC())
	registry := peer.NewJobRegistry()
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	jobA := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	jobB := fixtures.MakeJob(fixtures.WithJobID("job-b"))
	sourceA := peerJobLog{jobs: map[string]domain.Job{jobA.ID: jobA}, payloads: map[string][]byte{jobA.ID: []byte(`{"job":"a"}`)}}
	sourceB := peerJobLog{jobs: map[string]domain.Job{jobB.ID: jobB}, payloads: map[string][]byte{jobB.ID: []byte(`{"job":"b"}`)}}
	placerA := &peerScriptedPlacer{decisions: []domain.PlacementDecision{{JobID: jobA.ID, InstanceID: "warm-a", NodeID: nodeA.ID, Action: domain.ActionWarmInstance}}}
	placerB := &peerScriptedPlacer{decisions: []domain.PlacementDecision{{JobID: jobB.ID, InstanceID: "warm-b", NodeID: nodeB.ID, Action: domain.ActionWarmInstance}}}
	owners := peerOwnerResolver{owners: map[string]ports.AdmissionController{
		nodeA.ID: &mocks.AdmissionController{},
		nodeB.ID: &mocks.AdmissionController{},
	}}

	coordA := peer.NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), sourceA, registry, placerA, peerFleetSource{}, owners, clock)
	coordB := peer.NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-b")), sourceB, registry, placerB, peerFleetSource{}, owners, clock)
	for _, run := range []struct {
		coord *peer.Coordinator
		job   domain.Job
	}{
		{coordA, jobA},
		{coordB, jobB},
	} {
		if err := run.coord.ClaimJob(ctx, run.job.ID); err != nil {
			t.Fatalf("ClaimJob %s: %v", run.job.ID, err)
		}
		plan, err := run.coord.Plan(ctx, run.job.ID)
		if err != nil {
			t.Fatalf("Plan %s: %v", run.job.ID, err)
		}
		if _, err := run.coord.Commit(ctx, plan); err != nil {
			t.Fatalf("Commit %s: %v", run.job.ID, err)
		}
	}

	records := recordsByJob(t, registry)
	if records["job-a"].Coordinator != "peer-a" || records["job-b"].Coordinator != "peer-b" {
		t.Fatalf("records = %+v", records)
	}
}

func TestPhase6PeerOwnerRaceStaleFenceReplans(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(610, 0).UTC())
	registry := peer.NewJobRegistry()
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	claim := fixtures.MakeClaim(600, 0)
	jobA := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	jobB := fixtures.MakeJob(fixtures.WithJobID("job-b"))
	sourceA := peerJobLog{jobs: map[string]domain.Job{jobA.ID: jobA}, payloads: map[string][]byte{jobA.ID: []byte(`{"job":"a"}`)}}
	sourceB := peerJobLog{jobs: map[string]domain.Job{jobB.ID: jobB}, payloads: map[string][]byte{jobB.ID: []byte(`{"job":"b"}`)}}
	ownerA := &peerRaceAdmission{
		offers: []domain.LeaseOffer{
			{OfferID: "offer-a1", JobID: jobA.ID, NodeID: nodeA.ID, Claim: claim, Fence: 1},
			{OfferID: "offer-a2", JobID: jobB.ID, NodeID: nodeA.ID, Claim: claim, Fence: 1},
		},
		leases:     []domain.Lease{{ID: "lease-a", JobID: jobA.ID, NodeID: nodeA.ID, Claim: claim}},
		commitErrs: []error{nil, domain.ErrStaleFence},
	}
	ownerB := &peerRaceAdmission{
		offers: []domain.LeaseOffer{{OfferID: "offer-b", JobID: jobB.ID, NodeID: nodeB.ID, Claim: claim, Fence: 2}},
		leases: []domain.Lease{{ID: "lease-b", JobID: jobB.ID, NodeID: nodeB.ID, Claim: claim}},
	}
	owners := peerOwnerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: ownerA, nodeB.ID: ownerB}}
	coordA := peer.NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		sourceA,
		registry,
		&peerScriptedPlacer{decisions: []domain.PlacementDecision{{JobID: jobA.ID, NodeID: nodeA.ID, Claim: claim, Action: domain.ActionLoadedNew}}},
		peerFleetSource{fleet: domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}}},
		owners,
		clock,
	)
	coordB := peer.NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-b")),
		sourceB,
		registry,
		&peerScriptedPlacer{decisions: []domain.PlacementDecision{
			{JobID: jobB.ID, NodeID: nodeA.ID, Claim: claim, Action: domain.ActionLoadedNew},
			{JobID: jobB.ID, NodeID: nodeB.ID, Claim: claim, Action: domain.ActionLoadedNew},
		}},
		peerFleetSource{fleet: domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}}},
		owners,
		clock,
	)

	mustPeerClaimPlanCommit(t, ctx, coordA, jobA.ID)
	if err := coordB.ClaimJob(ctx, jobB.ID); err != nil {
		t.Fatalf("ClaimJob B: %v", err)
	}
	planB, err := coordB.Plan(ctx, jobB.ID)
	if err != nil {
		t.Fatalf("Plan B: %v", err)
	}
	leaseB, err := coordB.Commit(ctx, planB)
	if err != nil {
		t.Fatalf("Commit B: %v", err)
	}
	if leaseB.NodeID != nodeB.ID || ownerA.commitCalls != 2 || ownerB.commitCalls != 1 {
		t.Fatalf("leaseB=%+v ownerA commits=%d ownerB commits=%d", leaseB, ownerA.commitCalls, ownerB.commitCalls)
	}
	if recordsByJob(t, registry)[jobB.ID].AssignedNode != nodeB.ID {
		t.Fatalf("registry = %+v", recordsByJob(t, registry))
	}
}

func TestPhase6PeerOwnerRejectsDirectStaleFence(t *testing.T) {
	ctx := context.Background()
	admission := node.NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(time.Unix(620, 0).UTC()))
	preset := fixtures.MakePreset(fixtures.WithArtifactSize(1), fixtures.WithWeights(1))
	first, err := admission.Offer(ctx, domain.AdmissionRequest{Job: fixtures.MakeJob(fixtures.WithJobID("job-a")), Preset: preset, Claim: fixtures.MakeClaim(200, 0)})
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := admission.Offer(ctx, domain.AdmissionRequest{Job: fixtures.MakeJob(fixtures.WithJobID("job-b")), Preset: preset, Claim: fixtures.MakeClaim(200, 0)})
	if err != nil {
		t.Fatalf("second Offer: %v", err)
	}
	if _, err := admission.Commit(ctx, first.OfferID, first.Fence); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if _, err := admission.Commit(ctx, second.OfferID, second.Fence); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("stale Commit err = %v", err)
	}
}

func TestPhase6PeerCoordinatedPreemptionUsesOwnerAuthority(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(630, 0).UTC())
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim-a"),
		fixtures.OnNode(nodeA.ID),
		fixtures.WithInstancePreset("victim-preset"),
		fixtures.WithClaim(fixtures.MakeClaim(700, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	victim.InFlight = 1
	targetPreset := fixtures.MakePreset(fixtures.WithPresetID("target-preset"), fixtures.WithWeights(600), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1))
	admission := node.NewAdmission(nodeA, lease.NewAllocator(), clock)
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID(victim.PresetID), fixtures.WithArtifactSize(1), fixtures.WithWeights(1))
	victimJob := fixtures.MakeJob(fixtures.WithJobID("victim-job"), fixtures.WithPreset(victimPreset.ID), fixtures.Background)
	victimOffer, err := admission.Offer(ctx, domain.AdmissionRequest{Job: victimJob, Preset: victimPreset, Claim: victim.Claim})
	if err != nil {
		t.Fatalf("victim Offer: %v", err)
	}
	victimLease, err := admission.Commit(ctx, victimOffer.OfferID, victimOffer.Fence)
	if err != nil {
		t.Fatalf("victim Commit: %v", err)
	}
	if err := admission.BindInstance(ctx, victimLease.ID, victim.ID); err != nil {
		t.Fatalf("victim BindInstance: %v", err)
	}
	agent := mocks.NewNodeAgent(nodeA)
	agent.Instances = []domain.ModelInstance{victim}
	registry := peer.NewJobRegistry()
	victimRequest, err := peer.EncodeRescuePayload(victimJob, []byte(`{"job":"victim"}`))
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	if err := registry.Put(ctx, domain.JobRecord{
		JobID:        victimJob.ID,
		Coordinator:  "peer-b",
		AssignedNode: nodeA.ID,
		Status:       domain.JobRunning,
		Request:      victimRequest,
		UpdatedAt:    clock.Now(),
	}); err != nil {
		t.Fatalf("registry Put victim: %v", err)
	}
	jobLog := peer.NewRescueJobLog(peer.NewJobLog(), registry, nil)
	fleet := peerFleetSource{fleet: domain.FleetSnapshot{Nodes: []domain.Node{nodeA}, Instances: []domain.ModelInstance{victim}}}
	owners := peerOwnerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: admission}}
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock, targetPreset)
	coord := peer.NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), jobLog, registry, placer, fleet, owners, clock)
	service := &scheduler.Service{
		Placer:      placer,
		Fleet:       fleet,
		Nodes:       peerNodeResolver{agents: map[string]ports.NodeAgent{nodeA.ID: agent}},
		Owners:      owners,
		Coordinator: coord,
		JobLog:      jobLog,
		Queue:       scheduler.NewQueue(clock),
		Store: &peerRuntimeStore{
			jobs:      map[string]domain.Job{victimJob.ID: victimJob},
			instances: map[string]domain.ModelInstance{victim.ID: victim},
			leases:    map[string]domain.Lease{victimLease.ID: victimLease},
		},
		Clock:   clock,
		Presets: map[string]domain.Preset{targetPreset.ID: targetPreset},
	}

	result, err := service.SubmitWithPayload(ctx, fixtures.MakeJob(fixtures.WithJobID("job-target"), fixtures.WithPreset(targetPreset.ID), fixtures.Interactive, fixtures.HardForInteractive), []byte(`{"job":"target"}`))
	if err != nil {
		t.Fatalf("SubmitWithPayload: %v", err)
	}
	if result.Decision.Action != domain.ActionHardPreempted || result.Decision.Preempted[0] != victim.ID || service.Queue.Len() != 1 {
		t.Fatalf("decision=%+v queue=%d", result.Decision, service.Queue.Len())
	}
	if _, found, err := admission.LeaseForInstance(ctx, victim.ID); err != nil || found {
		t.Fatalf("victim lease still live found=%v err=%v", found, err)
	}
	if got, found, err := admission.LeaseForInstance(ctx, result.Instance.ID); err != nil || !found || got.JobID != "job-target" {
		t.Fatalf("target lease = %+v found=%v err=%v", got, found, err)
	}
	if !reflect.DeepEqual(agent.Calls, []string{"unload:victim-a", "load:target-preset"}) {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestPhase6PeerNoSelfPreferenceAndPartitionSafety(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(640, 0).UTC())
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"), fixtures.WithWeights(100), fixtures.WithKVPerToken(0), fixtures.WithContextLength(1))
	local := fixtures.MakeNode(fixtures.WithNodeID("local-peer"))
	remote := fixtures.MakeNode(fixtures.WithNodeID("remote-peer"))
	warm := fixtures.MakeInstance(fixtures.WithInstanceID("warm-remote"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(remote.ID), fixtures.WithClaim(fixtures.MakeClaim(100, 0)))
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock, preset)
	job := fixtures.MakeJob(fixtures.WithJobID("job-warm"), fixtures.WithPreset(preset.ID))
	coord := peer.NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("local-peer")),
		peerJobLog{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"warm"}`)}},
		peer.NewJobRegistry(),
		placer,
		peerFleetSource{fleet: domain.FleetSnapshot{Nodes: []domain.Node{local, remote}, Instances: []domain.ModelInstance{warm}}},
		peerOwnerResolver{},
		clock,
	)
	if err := coord.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	plan, err := coord.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.NodeID != remote.ID || plan.InstanceID != warm.ID {
		t.Fatalf("self-preferred plan = %+v", plan)
	}

	unreachable := fixtures.MakeNode(fixtures.WithNodeID("partitioned-peer"))
	unreachable.Status = domain.NodeUnreachable
	ready := fixtures.MakeNode(fixtures.WithNodeID("reachable-peer"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	partitionJob := fixtures.MakeJob(fixtures.WithJobID("job-partition"), fixtures.WithPreset(preset.ID), fixtures.Latency)
	partitionRegistry := peer.NewJobRegistry()
	partitionCoord := peer.NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("local-peer")),
		peerJobLog{jobs: map[string]domain.Job{partitionJob.ID: partitionJob}, payloads: map[string][]byte{partitionJob.ID: []byte(`{"job":"partition"}`)}},
		partitionRegistry,
		placer,
		peerFleetSource{fleet: domain.FleetSnapshot{Nodes: []domain.Node{unreachable, ready}, Instances: []domain.ModelInstance{
			fixtures.MakeInstance(fixtures.WithInstanceID("warm-partitioned"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(unreachable.ID), fixtures.WithClaim(fixtures.MakeClaim(100, 0))),
		}}},
		peerOwnerResolver{owners: map[string]ports.AdmissionController{ready.ID: node.NewAdmission(ready, lease.NewAllocator(), clock)}},
		clock,
	)
	mustPeerClaimPlanCommit(t, ctx, partitionCoord, partitionJob.ID)
	partitionRecords := recordsByJob(t, partitionRegistry)
	if partitionRecords[partitionJob.ID].AssignedNode != ready.ID {
		t.Fatalf("partition records = %+v", partitionRecords)
	}
}

func TestPhase6PeerDeathRecoveryViaHeartbeat(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(650, 0).UTC())
	registry := peer.NewJobRegistry()
	if err := registry.Merge(ctx, []domain.JobRecord{
		peerRecoveryRecord("queued", "peer-dead", "", domain.JobQueued),
		peerRecoveryRecord("finished-at-owner", "peer-dead", "node-live", domain.JobRunning),
		peerRecoveryRecord("other", "peer-other", "", domain.JobQueued),
	}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	rescued := []string{}
	recovery := peer.Recovery{
		Registry: registry,
		Owners: peerLeaseInspectors{inspectors: map[string]ports.LeaseInspector{
			"node-live": &mocks.AdmissionController{LeaseForJobVal: domain.Lease{ID: "lease-live", JobID: "finished-at-owner"}, LeaseForJobFound: true},
		}},
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}
	deadPeer := fixtures.MakePeer(fixtures.WithPeerID("peer-dead"))
	deadPeer.Addresses = []string{"127.0.0.1:1"}
	heartbeat := &peer.Heartbeat{
		Self:      fixtures.MakePeer(fixtures.WithPeerID("peer-live")),
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{deadPeer}},
		Clock:     clock,
		MaxMisses: 1,
		Probe: func(context.Context, domain.Peer) error {
			return domain.ErrUnreachable
		},
		OnDead: func(ctx context.Context, dead domain.Peer) error {
			_, err := recovery.RecoverPeer(ctx, dead.ID)
			return err
		},
	}

	dead, err := heartbeat.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != "peer-dead" || !reflect.DeepEqual(rescued, []string{"queued"}) {
		t.Fatalf("dead=%+v rescued=%+v", dead, rescued)
	}
}

func TestPhase6RegistryReplicationRecoveryWithSeparateStores(t *testing.T) {
	ctx := context.Background()
	liveStore, err := storesqlite.Open(t.TempDir() + "/live.db")
	if err != nil {
		t.Fatalf("open live store: %v", err)
	}
	defer liveStore.Close()
	deadStore, err := storesqlite.Open(t.TempDir() + "/dead.db")
	if err != nil {
		t.Fatalf("open dead store: %v", err)
	}
	defer deadStore.Close()
	queued := peerRecoveryRecord("queued", "peer-dead", "", domain.JobQueued)
	queued.Request = []byte(`{"job":"queued"}`)
	finishedAtOwner := peerRecoveryRecord("finished-at-owner", "peer-dead", "node-live", domain.JobRunning)
	finishedAtOwner.Request = []byte(`{"job":"finished"}`)
	if err := deadStore.Put(ctx, queued); err != nil {
		t.Fatalf("put queued: %v", err)
	}
	if err := deadStore.Put(ctx, finishedAtOwner); err != nil {
		t.Fatalf("put finished: %v", err)
	}
	replicator := peer.RegistryReplicator{
		Local: liveStore,
		Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{ID: "peer-live"},
			{ID: "peer-dead"},
		}},
		Client: separateStoreRegistryClient{stores: map[string]*storesqlite.Store{"peer-dead": deadStore}},
		SelfID: "peer-live",
	}
	if err := replicator.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	rescued := []string{}
	recovery := peer.Recovery{
		Registry: liveStore,
		Owners: peerLeaseInspectors{inspectors: map[string]ports.LeaseInspector{
			"node-live": &mocks.AdmissionController{LeaseForJobVal: domain.Lease{ID: "lease-live", JobID: finishedAtOwner.JobID}, LeaseForJobFound: true},
		}},
		Rescue: func(_ context.Context, rec domain.JobRecord) error {
			rescued = append(rescued, rec.JobID)
			return nil
		},
	}
	count, err := recovery.RecoverPeer(ctx, "peer-dead")
	if err != nil {
		t.Fatalf("RecoverPeer: %v", err)
	}
	if count != 1 || !reflect.DeepEqual(rescued, []string{"queued"}) {
		t.Fatalf("count=%d rescued=%+v", count, rescued)
	}
}

func mustPeerClaimPlanCommit(t *testing.T, ctx context.Context, coord *peer.Coordinator, jobID string) domain.Lease {
	t.Helper()
	if err := coord.ClaimJob(ctx, jobID); err != nil {
		t.Fatalf("ClaimJob %s: %v", jobID, err)
	}
	plan, err := coord.Plan(ctx, jobID)
	if err != nil {
		t.Fatalf("Plan %s: %v", jobID, err)
	}
	lease, err := coord.Commit(ctx, plan)
	if err != nil {
		t.Fatalf("Commit %s: %v", jobID, err)
	}
	return lease
}

func recordsByJob(t *testing.T, registry ports.JobRegistry) map[string]domain.JobRecord {
	t.Helper()
	records, err := registry.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	out := map[string]domain.JobRecord{}
	for _, rec := range records {
		out[rec.JobID] = rec
	}
	return out
}

type peerJobLog struct {
	jobs     map[string]domain.Job
	payloads map[string][]byte
}

func (l *peerJobLog) PutJob(_ context.Context, job domain.Job, payload []byte) error {
	if l.jobs == nil {
		l.jobs = map[string]domain.Job{}
	}
	if l.payloads == nil {
		l.payloads = map[string][]byte{}
	}
	l.jobs[job.ID] = job
	l.payloads[job.ID] = append([]byte(nil), payload...)
	return nil
}

func (l peerJobLog) Job(_ context.Context, jobID string) (domain.Job, []byte, error) {
	job, ok := l.jobs[jobID]
	if !ok {
		return domain.Job{}, nil, errors.New("job not found")
	}
	payload := append([]byte(nil), l.payloads[jobID]...)
	if len(payload) == 0 {
		return domain.Job{}, nil, errors.New("payload not found")
	}
	return job, payload, nil
}

type peerFleetSource struct {
	fleet domain.FleetSnapshot
	err   error
}

func (f peerFleetSource) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return f.fleet, f.err
}

type peerOwnerResolver struct {
	owners map[string]ports.AdmissionController
	err    error
}

func (r peerOwnerResolver) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	if r.err != nil {
		return nil, r.err
	}
	owner := r.owners[nodeID]
	if owner == nil {
		return nil, domain.ErrUnreachable
	}
	return owner, nil
}

type peerNodeResolver struct {
	agents map[string]ports.NodeAgent
}

func (r peerNodeResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent := r.agents[nodeID]
	if agent == nil {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

type peerScriptedPlacer struct {
	decisions []domain.PlacementDecision
}

func (p *peerScriptedPlacer) Place(context.Context, domain.Job, domain.FleetSnapshot) (domain.PlacementDecision, error) {
	if len(p.decisions) == 0 {
		return domain.PlacementDecision{}, errors.New("no decision")
	}
	decision := p.decisions[0]
	if len(p.decisions) > 1 {
		p.decisions = p.decisions[1:]
	}
	return decision, nil
}

type peerRaceAdmission struct {
	offers      []domain.LeaseOffer
	leases      []domain.Lease
	commitErrs  []error
	offerCalls  int
	commitCalls int
}

func (a *peerRaceAdmission) Offer(context.Context, domain.AdmissionRequest) (domain.LeaseOffer, error) {
	if a.offerCalls >= len(a.offers) {
		return domain.LeaseOffer{}, errors.New("no offer")
	}
	offer := a.offers[a.offerCalls]
	a.offerCalls++
	return offer, nil
}

func (a *peerRaceAdmission) Commit(context.Context, string, uint64) (domain.Lease, error) {
	idx := a.commitCalls
	a.commitCalls++
	if idx < len(a.commitErrs) && a.commitErrs[idx] != nil {
		return domain.Lease{}, a.commitErrs[idx]
	}
	if len(a.leases) == 0 {
		return domain.Lease{}, errors.New("no lease")
	}
	lease := a.leases[0]
	if len(a.leases) > 1 {
		a.leases = a.leases[1:]
	}
	return lease, nil
}

func (a *peerRaceAdmission) Release(context.Context, string) error {
	return nil
}

func (a *peerRaceAdmission) Preempt(context.Context, string, string) error {
	return nil
}

type peerLeaseInspectors struct {
	inspectors map[string]ports.LeaseInspector
}

func (r peerLeaseInspectors) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	inspector := r.inspectors[nodeID]
	if inspector == nil {
		return nil, domain.ErrUnreachable
	}
	return inspector, nil
}

type separateStoreRegistryClient struct {
	stores map[string]*storesqlite.Store
}

func (c separateStoreRegistryClient) Snapshot(ctx context.Context, peer domain.Peer) ([]domain.JobRecord, error) {
	store := c.stores[peer.ID]
	if store == nil {
		return nil, domain.ErrUnreachable
	}
	return store.Snapshot(ctx)
}

func (c separateStoreRegistryClient) Push(ctx context.Context, peer domain.Peer, records []domain.JobRecord) error {
	store := c.stores[peer.ID]
	if store == nil {
		return domain.ErrUnreachable
	}
	for _, rec := range records {
		if err := store.Put(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

type peerRuntimeStore struct {
	jobs      map[string]domain.Job
	leases    map[string]domain.Lease
	instances map[string]domain.ModelInstance
}

func (s *peerRuntimeStore) SaveJob(_ context.Context, job domain.Job) error {
	if s.jobs == nil {
		s.jobs = map[string]domain.Job{}
	}
	s.jobs[job.ID] = job
	return nil
}

func (s *peerRuntimeStore) Job(_ context.Context, id string) (domain.Job, error) {
	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, errors.New("job not found")
	}
	return job, nil
}

func (s *peerRuntimeStore) SaveLease(_ context.Context, lease domain.Lease) error {
	if s.leases == nil {
		s.leases = map[string]domain.Lease{}
	}
	s.leases[lease.ID] = lease
	return nil
}

func (s *peerRuntimeStore) ListLeases(context.Context) ([]domain.Lease, error) {
	leases := make([]domain.Lease, 0, len(s.leases))
	for _, lease := range s.leases {
		leases = append(leases, lease)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].ID < leases[j].ID })
	return leases, nil
}

func (s *peerRuntimeStore) DeleteLease(_ context.Context, id string) error {
	delete(s.leases, id)
	return nil
}

func (s *peerRuntimeStore) SaveInstance(_ context.Context, inst domain.ModelInstance) error {
	if s.instances == nil {
		s.instances = map[string]domain.ModelInstance{}
	}
	s.instances[inst.ID] = inst
	return nil
}

func (s *peerRuntimeStore) DeleteInstance(_ context.Context, id string) error {
	delete(s.instances, id)
	return nil
}

func peerRecoveryRecord(id, coordinator, nodeID string, status domain.JobStatus) domain.JobRecord {
	return domain.JobRecord{
		JobID:        id,
		Coordinator:  coordinator,
		AssignedNode: nodeID,
		Status:       status,
		Request:      []byte(`{"job":"` + id + `"}`),
		UpdatedAt:    time.Unix(650, 0).UTC(),
	}
}
