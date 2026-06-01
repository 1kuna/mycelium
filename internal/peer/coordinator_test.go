package peer

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

func TestCoordinatorClaimPlanCommitRelease(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	claim := fixtures.MakeClaim(10, 2)
	registry := NewJobRegistry()
	admission := &mocks.AdmissionController{
		OfferVal: domain.LeaseOffer{OfferID: "offer-a", JobID: job.ID, NodeID: node.ID, Claim: claim, Fence: 7},
		LeaseVal: domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: node.ID, Claim: claim},
	}
	coordinator := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		registry,
		&scriptedPlacer{decisions: []domain.PlacementDecision{{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}}},
		staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		ownerResolver{owners: map[string]ports.AdmissionController{node.ID: admission}},
		clock,
	)

	if err := coordinator.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	plan, err := coordinator.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	lease, err := coordinator.Commit(ctx, plan)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.ID != "lease-a" || lease.NodeID != node.ID {
		t.Fatalf("lease = %+v", lease)
	}
	if err := coordinator.Release(ctx, job.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if strings.Join(admission.Calls, ",") != "offer:job-a,commit:offer-a:7,release:lease-a" {
		t.Fatalf("admission calls = %+v", admission.Calls)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Status != domain.JobDone || snap[0].AssignedNode != node.ID {
		t.Fatalf("registry = %+v", snap)
	}
	assertRescuePayload(t, snap[0].Request, job.ID, `{"job":"a"}`)
}

func TestCoordinatorReplansOnOwnerContention(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(110, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	claim := fixtures.MakeClaim(10, 2)
	registry := NewJobRegistry()
	placer := &scriptedPlacer{decisions: []domain.PlacementDecision{
		{JobID: job.ID, NodeID: nodeA.ID, Claim: claim, Action: domain.ActionLoadedNew},
		{JobID: job.ID, NodeID: nodeB.ID, Claim: claim, Action: domain.ActionLoadedNew},
	}}
	ownerA := &scriptedAdmission{offers: []domain.LeaseOffer{{OfferID: "offer-a", JobID: job.ID, NodeID: nodeA.ID, Claim: claim, Fence: 1}}, commitErrs: []error{domain.ErrStaleFence}}
	ownerB := &scriptedAdmission{offers: []domain.LeaseOffer{{OfferID: "offer-b", JobID: job.ID, NodeID: nodeB.ID, Claim: claim, Fence: 2}}, leases: []domain.Lease{{ID: "lease-b", JobID: job.ID, NodeID: nodeB.ID, Claim: claim}}}
	coordinator := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		registry,
		placer,
		staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}}},
		ownerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: ownerA, nodeB.ID: ownerB}},
		clock,
	)
	if err := coordinator.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	plan, err := coordinator.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	lease, err := coordinator.Commit(ctx, plan)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.NodeID != nodeB.ID || strings.Join(placer.calls, ",") != "job-a,job-a" {
		t.Fatalf("lease=%+v placer calls=%+v", lease, placer.calls)
	}
	if strings.Join(ownerA.calls, ",") != "offer:job-a,commit:offer-a:1" || strings.Join(ownerB.calls, ",") != "offer:job-a,commit:offer-b:2" {
		t.Fatalf("ownerA=%+v ownerB=%+v", ownerA.calls, ownerB.calls)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Status != domain.JobRunning || snap[0].AssignedNode != nodeB.ID || snap[0].Fence != 2 {
		t.Fatalf("registry = %+v", snap)
	}
}

func TestCoordinatorWarmInstanceCommitsOwnerLease(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(115, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-warm"))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-warm"), fixtures.OnNode(node.ID))
	registry := NewJobRegistry()
	admission := &mocks.AdmissionController{
		OfferVal: domain.LeaseOffer{OfferID: "offer-warm", JobID: job.ID, NodeID: node.ID, Claim: domain.Claim{KVReservedMB: inst.Claim.KVReservedMB}, InstanceID: inst.ID, Fence: 3},
		LeaseVal: domain.Lease{ID: "lease-warm", JobID: job.ID, NodeID: node.ID, InstanceID: inst.ID, Claim: domain.Claim{KVReservedMB: inst.Claim.KVReservedMB}},
	}
	coordinator := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"warm"}`)}},
		registry,
		&scriptedPlacer{decisions: []domain.PlacementDecision{{
			JobID:      job.ID,
			InstanceID: inst.ID,
			NodeID:     node.ID,
			Claim:      inst.Claim,
			Action:     domain.ActionWarmInstance,
		}}},
		staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}},
		ownerResolver{owners: map[string]ports.AdmissionController{node.ID: admission}},
		clock,
	)

	mustClaim(t, coordinator, job.ID)
	plan, err := coordinator.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	lease, err := coordinator.Commit(ctx, plan)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.ID != "lease-warm" || lease.InstanceID != inst.ID || lease.NodeID != node.ID {
		t.Fatalf("warm lease = %+v", lease)
	}
	if err := coordinator.Release(ctx, job.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if strings.Join(admission.Calls, ",") != "offer:job-warm,commit:offer-warm:3,release:lease-warm" {
		t.Fatalf("warm owner calls = %+v", admission.Calls)
	}
	assertLatestStatus(t, registry, domain.JobDone, node.ID)
}

func TestCoordinatorEncryptsPrivateRegistryPayload(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(116, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-private"))
	job.Handling = domain.HandlingPrivate
	key := []byte("0123456789abcdef0123456789abcdef")
	registry := NewJobRegistry()
	coordinator := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"secret":"payload"}`)}},
		registry,
		&scriptedPlacer{},
		staticPeerFleet{},
		ownerResolver{},
		clock,
		WithPrivatePayloadKey(key),
	)

	if err := coordinator.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Handling != domain.HandlingPrivate || strings.Contains(string(snap[0].Request), "secret") || strings.Contains(string(snap[0].Request), job.ID) {
		t.Fatalf("private record = %+v request=%s", snap, snap[0].Request)
	}
	gotJob, gotBody, err := DecodeRescuePayloadWithKey(snap[0].Request, key)
	if err != nil {
		t.Fatalf("DecodeRescuePayloadWithKey: %v", err)
	}
	if gotJob.ID != job.ID || gotJob.Handling != domain.HandlingPrivate || string(gotBody) != `{"secret":"payload"}` {
		t.Fatalf("decoded job=%+v body=%s", gotJob, gotBody)
	}
}

func TestCoordinatorQueuesAndExhaustsReplans(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(120, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	claim := fixtures.MakeClaim(10, 2)
	queuedRegistry := NewJobRegistry()
	queued := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		queuedRegistry,
		&scriptedPlacer{decisions: []domain.PlacementDecision{{JobID: job.ID, Action: domain.ActionQueued}}},
		staticPeerFleet{},
		ownerResolver{owners: map[string]ports.AdmissionController{}},
		clock,
	)
	if err := queued.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("queued ClaimJob: %v", err)
	}
	plan, err := queued.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("queued Plan: %v", err)
	}
	lease, err := queued.Commit(ctx, plan)
	if err != nil || lease.ID != "" {
		t.Fatalf("queued Commit lease=%+v err=%v", lease, err)
	}
	assertLatestStatus(t, queuedRegistry, domain.JobQueued, "")

	staleRegistry := NewJobRegistry()
	owner := &scriptedAdmission{
		offers:     []domain.LeaseOffer{{OfferID: "offer-a", JobID: job.ID, NodeID: node.ID, Claim: claim, Fence: 1}},
		commitErrs: []error{domain.ErrStaleFence},
	}
	stale := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		staleRegistry,
		&scriptedPlacer{decisions: []domain.PlacementDecision{{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}}},
		staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		ownerResolver{owners: map[string]ports.AdmissionController{node.ID: owner}},
		clock,
		WithMaxReplans(0),
	)
	if err := stale.ClaimJob(ctx, job.ID); err != nil {
		t.Fatalf("stale ClaimJob: %v", err)
	}
	plan, err = stale.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("stale Plan: %v", err)
	}
	if _, err := stale.Commit(ctx, plan); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("stale Commit err = %v", err)
	}
	assertLatestStatus(t, staleRegistry, domain.JobQueued, "")
}

func TestCoordinatorErrorPaths(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(130, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	claim := fixtures.MakeClaim(10, 2)
	source := jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}}
	base := func(registry ports.JobRegistry, placer ports.Placer, owners AdmissionResolver) *Coordinator {
		return NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), source, registry, placer, staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}, owners, clock)
	}

	if err := (&Coordinator{}).ClaimJob(ctx, job.ID); err == nil {
		t.Fatal("unconfigured ClaimJob succeeded")
	}
	if err := base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{}).ClaimJob(ctx, ""); err == nil {
		t.Fatal("empty ClaimJob succeeded")
	}
	if err := NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), jobSource{err: errors.New("source")}, NewJobRegistry(), &scriptedPlacer{}, staticPeerFleet{}, ownerResolver{}, clock).ClaimJob(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("source err = %v", err)
	}
	if err := NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), jobSourceFunc(func(context.Context, string) (domain.Job, []byte, error) {
		return fixtures.MakeJob(fixtures.WithJobID("other")), []byte(`{}`), nil
	}), NewJobRegistry(), &scriptedPlacer{}, staticPeerFleet{}, ownerResolver{}, clock).ClaimJob(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("mismatch err = %v", err)
	}
	if err := NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), jobSourceFunc(func(context.Context, string) (domain.Job, []byte, error) {
		return job, nil, nil
	}), NewJobRegistry(), &scriptedPlacer{}, staticPeerFleet{}, ownerResolver{}, clock).ClaimJob(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "rescue") {
		t.Fatalf("payload err = %v", err)
	}
	privateJob := fixtures.MakeJob(fixtures.WithJobID("job-private"))
	privateJob.Handling = domain.HandlingPrivate
	if err := NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), jobSource{jobs: map[string]domain.Job{privateJob.ID: privateJob}, payloads: map[string][]byte{privateJob.ID: []byte(`{"secret":true}`)}}, NewJobRegistry(), &scriptedPlacer{}, staticPeerFleet{}, ownerResolver{}, clock).ClaimJob(ctx, privateJob.ID); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("private key err = %v", err)
	}

	if _, err := (&Coordinator{}).Plan(ctx, job.ID); err == nil {
		t.Fatal("unconfigured Plan succeeded")
	}
	coordinator := base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{})
	if _, err := coordinator.Plan(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "not claimed") {
		t.Fatalf("unclaimed Plan err = %v", err)
	}
	fleetErr := errors.New("fleet")
	coordinator = NewCoordinator(fixtures.MakePeer(fixtures.WithPeerID("peer-a")), source, NewJobRegistry(), &scriptedPlacer{}, staticPeerFleet{err: fleetErr}, ownerResolver{}, clock)
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Plan(ctx, job.ID); !errors.Is(err, fleetErr) {
		t.Fatalf("fleet err = %v", err)
	}
	placeErr := errors.New("place")
	coordinator = base(NewJobRegistry(), &scriptedPlacer{errs: []error{placeErr}}, ownerResolver{})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Plan(ctx, job.ID); !errors.Is(err, placeErr) {
		t.Fatalf("place err = %v", err)
	}
	registryErr := errors.New("registry")
	coordinator = base(&failingRegistry{err: registryErr}, &scriptedPlacer{decisions: []domain.PlacementDecision{{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}}}, ownerResolver{})
	coordinator.claimed = map[string]claimedJob{job.ID: {job: job, payload: []byte(`{"job":"a"}`)}}
	if _, err := coordinator.Plan(ctx, job.ID); !errors.Is(err, registryErr) {
		t.Fatalf("plan registry err = %v", err)
	}

	if _, err := (&Coordinator{}).Commit(ctx, domain.PlacementDecision{JobID: job.ID}); err == nil {
		t.Fatal("unconfigured Commit succeeded")
	}
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{})
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{}); err == nil {
		t.Fatal("empty Commit plan succeeded")
	}
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID}); err == nil || !strings.Contains(err.Error(), "not claimed") {
		t.Fatalf("unclaimed Commit err = %v", err)
	}
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, Action: domain.ActionLoadedNew}); err == nil || !strings.Contains(err.Error(), "owner node") {
		t.Fatalf("missing owner err = %v", err)
	}
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("owner resolver err = %v", err)
	}
	offerErr := errors.New("offer")
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{owners: map[string]ports.AdmissionController{node.ID: &scriptedAdmission{offerErrs: []error{offerErr}}}})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}); !errors.Is(err, offerErr) {
		t.Fatalf("offer err = %v", err)
	}
	coordinator = base(NewJobRegistry(), &scriptedPlacer{errs: []error{placeErr}}, ownerResolver{owners: map[string]ports.AdmissionController{node.ID: &scriptedAdmission{offerErrs: []error{domain.ErrNoFit}}}})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}); !errors.Is(err, placeErr) {
		t.Fatalf("offer replan place err = %v", err)
	}
	commitErr := errors.New("commit")
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{owners: map[string]ports.AdmissionController{node.ID: &scriptedAdmission{
		offers:     []domain.LeaseOffer{{OfferID: "offer-a", JobID: job.ID, NodeID: node.ID, Claim: claim, Fence: 1}},
		commitErrs: []error{commitErr},
	}}})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}); !errors.Is(err, commitErr) {
		t.Fatalf("commit err = %v", err)
	}
	coordinator = base(NewJobRegistry(), &scriptedPlacer{errs: []error{placeErr}}, ownerResolver{owners: map[string]ports.AdmissionController{node.ID: &scriptedAdmission{
		offers:     []domain.LeaseOffer{{OfferID: "offer-a", JobID: job.ID, NodeID: node.ID, Claim: claim, Fence: 1}},
		commitErrs: []error{domain.ErrStaleFence},
	}}})
	mustClaim(t, coordinator, job.ID)
	if _, err := coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: node.ID, Claim: claim, Action: domain.ActionLoadedNew}); !errors.Is(err, placeErr) {
		t.Fatalf("replan place err = %v", err)
	}

	if err := (&Coordinator{}).Release(ctx, job.ID); err == nil {
		t.Fatal("unconfigured Release succeeded")
	}
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{})
	if err := coordinator.Release(ctx, ""); err == nil {
		t.Fatal("empty Release succeeded")
	}
	mustClaim(t, coordinator, job.ID)
	if err := coordinator.Release(ctx, job.ID); err == nil || !strings.Contains(err.Error(), "no committed lease") {
		t.Fatalf("missing lease err = %v", err)
	}
	coordinator.leases[job.ID] = domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: node.ID}
	if err := coordinator.Release(ctx, job.ID); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("release owner err = %v", err)
	}
	releaseErr := errors.New("release")
	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{owners: map[string]ports.AdmissionController{node.ID: &scriptedAdmission{releaseErr: releaseErr}}})
	mustClaim(t, coordinator, job.ID)
	coordinator.leases[job.ID] = domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: node.ID}
	if err := coordinator.Release(ctx, job.ID); !errors.Is(err, releaseErr) {
		t.Fatalf("release err = %v", err)
	}

	coordinator = base(NewJobRegistry(), &scriptedPlacer{}, ownerResolver{})
	coordinator.maxReplans = -1
	if err := coordinator.validate(); err == nil || !strings.Contains(err.Error(), "replans") {
		t.Fatalf("negative replans err = %v", err)
	}
	coordinator.maxReplans = 0
	coordinator.claimed = nil
	coordinator.leases = nil
	if err := coordinator.validate(); err != nil || coordinator.claimed == nil || coordinator.leases == nil {
		t.Fatalf("validate initialized maps err=%v claimed=%v leases=%v", err, coordinator.claimed, coordinator.leases)
	}
	if _, err := coordinator.claimedJob(""); err == nil {
		t.Fatal("empty claimedJob succeeded")
	}
	if err := coordinator.record(ctx, "missing", domain.JobFailed, "", 0); err == nil {
		t.Fatal("record for missing job succeeded")
	}
	coordinator.claimed[job.ID] = claimedJob{job: job}
	if err := coordinator.record(ctx, job.ID, domain.JobFailed, "", 0); err == nil || !strings.Contains(err.Error(), "body") {
		t.Fatalf("record rescue payload err = %v", err)
	}
	if coordinator.shouldReplan(errors.New("other"), 0) {
		t.Fatal("non-replanable error replanned")
	}
}

func TestCoordinatorOfferNoFitReplansAndRecordFailure(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(140, 0).UTC())
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	claim := fixtures.MakeClaim(10, 2)
	registry := NewJobRegistry()
	placer := &scriptedPlacer{decisions: []domain.PlacementDecision{
		{JobID: job.ID, NodeID: nodeA.ID, Claim: claim, Action: domain.ActionLoadedNew},
		{JobID: job.ID, NodeID: nodeB.ID, Claim: claim, Action: domain.ActionLoadedNew},
	}}
	ownerA := &scriptedAdmission{offerErrs: []error{domain.ErrNoFit}}
	ownerB := &scriptedAdmission{
		offers: []domain.LeaseOffer{{OfferID: "offer-b", JobID: job.ID, NodeID: nodeB.ID, Claim: claim, Fence: 2}},
		leases: []domain.Lease{{ID: "lease-b", JobID: job.ID, NodeID: nodeB.ID, Claim: claim}},
	}
	coordinator := NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		registry,
		placer,
		staticPeerFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}}},
		ownerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: ownerA, nodeB.ID: ownerB}},
		clock,
	)
	mustClaim(t, coordinator, job.ID)
	plan, err := coordinator.Plan(ctx, job.ID)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	lease, err := coordinator.Commit(ctx, plan)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.NodeID != nodeB.ID {
		t.Fatalf("lease = %+v", lease)
	}

	failRegistry := &flakyRegistry{failAt: 2, records: map[string]domain.JobRecord{}}
	coordinator = NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		failRegistry,
		&scriptedPlacer{},
		staticPeerFleet{},
		ownerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: &scriptedAdmission{
			offers: []domain.LeaseOffer{{OfferID: "offer-a", JobID: job.ID, NodeID: nodeA.ID, Claim: claim, Fence: 1}},
			leases: []domain.Lease{{ID: "lease-a", JobID: job.ID, NodeID: nodeA.ID, Claim: claim}},
		}}},
		clock,
	)
	mustClaim(t, coordinator, job.ID)
	_, err = coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, NodeID: nodeA.ID, Claim: claim, Action: domain.ActionLoadedNew})
	if !errors.Is(err, errFlakyRegistry) {
		t.Fatalf("running record err = %v", err)
	}

	warmFailRegistry := &flakyRegistry{failAt: 2, records: map[string]domain.JobRecord{}}
	coordinator = NewCoordinator(
		fixtures.MakePeer(fixtures.WithPeerID("peer-a")),
		jobSource{jobs: map[string]domain.Job{job.ID: job}, payloads: map[string][]byte{job.ID: []byte(`{"job":"a"}`)}},
		warmFailRegistry,
		&scriptedPlacer{},
		staticPeerFleet{},
		ownerResolver{owners: map[string]ports.AdmissionController{nodeA.ID: &scriptedAdmission{
			offers: []domain.LeaseOffer{{OfferID: "offer-warm", JobID: job.ID, NodeID: nodeA.ID, Claim: claim, InstanceID: "warm-a", Fence: 1}},
			leases: []domain.Lease{{ID: "lease-warm", JobID: job.ID, NodeID: nodeA.ID, InstanceID: "warm-a", Claim: claim}},
		}}},
		clock,
	)
	mustClaim(t, coordinator, job.ID)
	_, err = coordinator.Commit(ctx, domain.PlacementDecision{JobID: job.ID, InstanceID: "warm-a", NodeID: nodeA.ID, Claim: claim, Action: domain.ActionWarmInstance})
	if !errors.Is(err, errFlakyRegistry) {
		t.Fatalf("warm running record err = %v", err)
	}
}

type jobSource struct {
	jobs     map[string]domain.Job
	payloads map[string][]byte
	err      error
}

type jobSourceFunc func(context.Context, string) (domain.Job, []byte, error)

func (f jobSourceFunc) Job(ctx context.Context, jobID string) (domain.Job, []byte, error) {
	return f(ctx, jobID)
}

func (s jobSource) Job(_ context.Context, jobID string) (domain.Job, []byte, error) {
	if s.err != nil {
		return domain.Job{}, nil, s.err
	}
	job, ok := s.jobs[jobID]
	if !ok {
		return domain.Job{}, nil, fmt.Errorf("job not found")
	}
	return job, append([]byte(nil), s.payloads[jobID]...), nil
}

type staticPeerFleet struct {
	fleet domain.FleetSnapshot
	err   error
}

func (f staticPeerFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return f.fleet, f.err
}

type ownerResolver struct {
	owners map[string]ports.AdmissionController
	err    error
}

func (r ownerResolver) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	if r.err != nil {
		return nil, r.err
	}
	owner := r.owners[nodeID]
	if owner == nil {
		return nil, domain.ErrUnreachable
	}
	return owner, nil
}

type scriptedPlacer struct {
	decisions []domain.PlacementDecision
	errs      []error
	calls     []string
}

func (p *scriptedPlacer) Place(_ context.Context, job domain.Job, _ domain.FleetSnapshot) (domain.PlacementDecision, error) {
	p.calls = append(p.calls, job.ID)
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return domain.PlacementDecision{JobID: job.ID}, err
		}
	}
	if len(p.decisions) == 0 {
		return domain.PlacementDecision{}, fmt.Errorf("no decision")
	}
	decision := p.decisions[0]
	if len(p.decisions) > 1 {
		p.decisions = p.decisions[1:]
	}
	return decision, nil
}

type scriptedAdmission struct {
	offers     []domain.LeaseOffer
	offerErrs  []error
	leases     []domain.Lease
	commitErrs []error
	releaseErr error
	calls      []string
}

func (a *scriptedAdmission) Offer(_ context.Context, req domain.AdmissionRequest) (domain.LeaseOffer, error) {
	job := req.Job
	a.calls = append(a.calls, "offer:"+job.ID)
	if len(a.offerErrs) > 0 {
		err := a.offerErrs[0]
		a.offerErrs = a.offerErrs[1:]
		if err != nil {
			return domain.LeaseOffer{}, err
		}
	}
	if len(a.offers) == 0 {
		return domain.LeaseOffer{}, fmt.Errorf("no offer")
	}
	offer := a.offers[0]
	if len(a.offers) > 1 {
		a.offers = a.offers[1:]
	}
	return offer, nil
}

func (a *scriptedAdmission) Commit(_ context.Context, offerID string, fence uint64) (domain.Lease, error) {
	a.calls = append(a.calls, fmt.Sprintf("commit:%s:%d", offerID, fence))
	if len(a.commitErrs) > 0 {
		err := a.commitErrs[0]
		a.commitErrs = a.commitErrs[1:]
		if err != nil {
			return domain.Lease{}, err
		}
	}
	if len(a.leases) == 0 {
		return domain.Lease{}, fmt.Errorf("no lease")
	}
	lease := a.leases[0]
	if len(a.leases) > 1 {
		a.leases = a.leases[1:]
	}
	return lease, nil
}

func (a *scriptedAdmission) Release(_ context.Context, leaseID string) error {
	a.calls = append(a.calls, "release:"+leaseID)
	return a.releaseErr
}

func (a *scriptedAdmission) Preempt(context.Context, string, string) error {
	return nil
}

func assertLatestStatus(t *testing.T, registry *JobRegistry, status domain.JobStatus, nodeID string) {
	t.Helper()
	snap, err := registry.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 1 || snap[0].Status != status || snap[0].AssignedNode != nodeID {
		t.Fatalf("registry = %+v", snap)
	}
}

func mustClaim(t *testing.T, coordinator *Coordinator, jobID string) {
	t.Helper()
	if err := coordinator.ClaimJob(context.Background(), jobID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
}

func assertRescuePayload(t *testing.T, data []byte, jobID, body string) {
	t.Helper()
	job, gotBody, err := DecodeRescuePayload(data)
	if err != nil {
		t.Fatalf("DecodeRescuePayload: %v", err)
	}
	if job.ID != jobID || string(gotBody) != body {
		t.Fatalf("rescue payload job=%+v body=%s", job, gotBody)
	}
}

type failingRegistry struct {
	err error
}

func (r *failingRegistry) Put(context.Context, domain.JobRecord) error {
	return r.err
}

func (r *failingRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	return nil, r.err
}

func (r *failingRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	return nil, r.err
}

var errFlakyRegistry = errors.New("flaky registry")

type flakyRegistry struct {
	failAt  int
	puts    int
	records map[string]domain.JobRecord
}

func (r *flakyRegistry) Put(_ context.Context, rec domain.JobRecord) error {
	r.puts++
	if r.puts == r.failAt {
		return errFlakyRegistry
	}
	if r.records == nil {
		r.records = map[string]domain.JobRecord{}
	}
	r.records[rec.JobID] = rec
	return nil
}

func (r *flakyRegistry) Watch(context.Context, string) (<-chan domain.JobRecord, error) {
	ch := make(chan domain.JobRecord)
	close(ch)
	return ch, nil
}

func (r *flakyRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	out := make([]domain.JobRecord, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec)
	}
	return out, nil
}
