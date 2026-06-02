package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAdmissionConformance(t *testing.T) {
	contract.RunAdmissionControllerConformance(t, "node-admission",
		func() ports.AdmissionController {
			return NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
		},
		fixtures.MakeJob(), fixtures.MakeClaim(3, 4))
}

func TestAdmissionOfferAndCommitGrantLeaseWithFence(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithNodeID("node-a")), lease.NewAllocator(), clock)
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	claim := fixtures.MakeClaim(100, 20)

	offer, err := admission.Offer(context.Background(), admissionReq(job, claim))
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	if offer.JobID != job.ID || offer.NodeID != "node-a" || offer.Claim != claim || offer.Fence != 1 {
		t.Fatalf("offer = %+v", offer)
	}

	lease, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.JobID != job.ID || lease.NodeID != "node-a" || lease.Claim != claim || lease.GrantedAt != clock.Now() {
		t.Fatalf("lease = %+v", lease)
	}
	if admission.fence != 2 || len(admission.offers) != 0 || len(admission.leases) != 1 {
		t.Fatalf("state fence=%d offers=%d leases=%d", admission.fence, len(admission.offers), len(admission.leases))
	}
}

func TestAdmissionRejectsStaleFence(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	first, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(10, 1)))
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(10, 1)))
	if err != nil {
		t.Fatalf("second Offer: %v", err)
	}
	if _, err := admission.Commit(context.Background(), first.OfferID, first.Fence); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if _, err := admission.Commit(context.Background(), second.OfferID, second.Fence); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("second Commit err = %v", err)
	}
}

func TestAdmissionConcurrentCommitsSerializeAtOwner(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	first, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(600, 0)))
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(600, 0)))
	if err != nil {
		t.Fatalf("second Offer: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, offer := range []domain.LeaseOffer{first, second} {
		wg.Add(1)
		go func(offer domain.LeaseOffer) {
			defer wg.Done()
			<-start
			_, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence)
			errs <- err
		}(offer)
	}
	close(start)
	wg.Wait()
	close(errs)

	var successes, stale int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrStaleFence):
			stale++
		default:
			t.Fatalf("unexpected commit error: %v", err)
		}
	}
	if successes != 1 || stale != 1 || len(admission.leases) != 1 {
		t.Fatalf("successes=%d stale=%d leases=%d", successes, stale, len(admission.leases))
	}
}

func TestAdmissionReleaseAndPreemptRemoveOccupancy(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	leaseA := commitAdmissionLease(t, admission, "job-a", fixtures.MakeClaim(700, 0))
	if got, found, err := admission.LeaseForJob(context.Background(), "job-a"); err != nil || !found || got.ID != leaseA.ID {
		t.Fatalf("LeaseForJob = %+v %v %v", got, found, err)
	}
	if err := admission.BindInstance(context.Background(), leaseA.ID, "inst-a"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	if got, found, err := admission.LeaseForInstance(context.Background(), "inst-a"); err != nil || !found || got.ID != leaseA.ID {
		t.Fatalf("LeaseForInstance = %+v %v %v", got, found, err)
	}
	if got, found, err := admission.LeaseForInstance(context.Background(), "missing"); err != nil || found || got.ID != "" {
		t.Fatalf("missing LeaseForInstance = %+v %v %v", got, found, err)
	}
	if got, found, err := admission.LeaseForJob(context.Background(), "missing"); err != nil || found || got.ID != "" {
		t.Fatalf("missing LeaseForJob = %+v %v %v", got, found, err)
	}
	if _, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("blocked")), fixtures.MakeClaim(400, 0))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("blocked Offer err = %v", err)
	}
	if err := admission.Release(context.Background(), leaseA.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(admission.leases) != 0 {
		t.Fatalf("release left leases: %+v", admission.leases)
	}

	leaseB := commitAdmissionLease(t, admission, "job-b", fixtures.MakeClaim(700, 0))
	if err := admission.Preempt(context.Background(), leaseB.ID, "higher priority"); err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if len(admission.leases) != 0 {
		t.Fatalf("preempt left leases: %+v", admission.leases)
	}
	if _, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID("admitted")), fixtures.MakeClaim(400, 0))); err != nil {
		t.Fatalf("Offer after release/preempt: %v", err)
	}
}

func TestAdmissionPersistsOwnerLeasesAndFenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	clock := mocks.NewFakeClock(time.Unix(700, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))

	first := NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	leaseA := commitAdmissionLease(t, first, "job-a", fixtures.MakeClaim(700, 0))
	if err := first.BindInstance(ctx, leaseA.ID, "inst-a"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}

	restarted := NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	got, found, err := restarted.LeaseForJob(ctx, "job-a")
	if err != nil || !found || got.ID != leaseA.ID || got.InstanceID != "inst-a" {
		t.Fatalf("restored lease = %+v found=%v err=%v", got, found, err)
	}
	if _, err := restarted.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("blocked")), fixtures.MakeClaim(400, 0))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("restored occupancy no-fit err = %v", err)
	}
	if err := restarted.Release(ctx, leaseA.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	offer, err := restarted.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(100, 0)))
	if err != nil {
		t.Fatalf("Offer after restart release: %v", err)
	}
	if offer.Fence != 3 {
		t.Fatalf("offer fence after restart = %d", offer.Fence)
	}
}

func TestAdmissionRequestTargetingWarmInstancesAndPinnedLeases(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(710, 0).UTC())
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("warm-a"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(900, 0)),
	)
	admission := NewAdmission(
		node,
		lease.NewAllocator(),
		clock,
		WithAdmissionInstances(func() []domain.ModelInstance { return []domain.ModelInstance{warm} }),
		WithPinnedReservations("", "pin-a"),
	)

	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("wrong-node")),
		Claim:  fixtures.MakeClaim(1, 0),
		NodeID: "node-b",
	}); err == nil || !strings.Contains(err.Error(), "targeted node") {
		t.Fatalf("wrong node err = %v", err)
	}
	offer, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:            fixtures.MakeJob(fixtures.WithJobID("job-acc")),
		Claim:          fixtures.MakeClaim(100, 0),
		NodeID:         node.ID,
		AcceleratorSet: []int{0},
		ReservationID:  "pin-a",
	})
	if err != nil {
		t.Fatalf("accelerator offer: %v", err)
	}
	if offer.ReservationID != "pin-a" || len(offer.AcceleratorSet) != 1 || offer.AcceleratorSet[0] != 0 {
		t.Fatalf("offer = %+v", offer)
	}
	pinned, err := admission.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("pinned commit: %v", err)
	}
	if !pinned.Pinned {
		t.Fatalf("pinned lease = %+v", pinned)
	}
	preempter := fixtures.MakeJob(fixtures.WithJobID("preempt"), fixtures.Interactive, fixtures.HardForInteractive)
	if err := admission.PreemptForJob(ctx, preempter, pinned.ID, "pinned"); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("pinned preempt err = %v", err)
	}
	if err := admission.Release(ctx, pinned.ID); err != nil {
		t.Fatalf("release pinned: %v", err)
	}

	warmOffer, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:        fixtures.MakeJob(fixtures.WithJobID("job-warm")),
		Claim:      fixtures.MakeClaim(900, 50),
		NodeID:     node.ID,
		InstanceID: warm.ID,
	})
	if err != nil {
		t.Fatalf("warm offer: %v", err)
	}
	if warmOffer.Claim.WeightsMB != 0 || warmOffer.Claim.KVReservedMB != 50 || warmOffer.InstanceID != warm.ID {
		t.Fatalf("warm offer = %+v", warmOffer)
	}
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:        fixtures.MakeJob(fixtures.WithJobID("job-missing-warm")),
		Claim:      fixtures.MakeClaim(1, 0),
		NodeID:     node.ID,
		InstanceID: "missing",
	}); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("missing warm err = %v", err)
	}
}

func TestAdmissionCountsOwnerRunningInstances(t *testing.T) {
	running := []domain.ModelInstance{fixtures.MakeInstance(
		fixtures.WithInstanceID("running-a"),
		fixtures.OnNode("node_test"),
		fixtures.WithClaim(fixtures.MakeClaim(700, 0)),
	)}
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithAdmissionInstances(func() []domain.ModelInstance {
			return append([]domain.ModelInstance(nil), running...)
		}),
	)
	if _, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(400, 0))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("running instance no-fit err = %v", err)
	}
	running = nil
	if _, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(400, 0))); err != nil {
		t.Fatalf("offer after clearing running instances: %v", err)
	}
}

func TestAdmissionDoesNotDoubleCountBoundLeaseAndLiveInstance(t *testing.T) {
	ctx := context.Background()
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	var running []domain.ModelInstance
	admission := NewAdmission(
		node,
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithAdmissionInstances(func() []domain.ModelInstance {
			return append([]domain.ModelInstance(nil), running...)
		}),
	)

	leaseA := commitAdmissionLease(t, admission, "job-a", fixtures.MakeClaim(700, 0))
	if _, err := admission.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("blocked-loading")), fixtures.MakeClaim(400, 0))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("loading occupancy err = %v", err)
	}

	running = []domain.ModelInstance{fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-a"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(700, 0)),
	)}
	if err := admission.BindInstance(ctx, leaseA.ID, "inst-a"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	if _, err := admission.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("fits-after-bind")), fixtures.MakeClaim(300, 0))); err != nil {
		t.Fatalf("bound lease should count once: %v", err)
	}
}

func TestAdmissionWarmLeasesReserveIncrementalKVOnly(t *testing.T) {
	ctx := context.Background()
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("warm-a"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(600, 0)),
	)
	admission := NewAdmission(
		node,
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithAdmissionInstances(func() []domain.ModelInstance { return []domain.ModelInstance{warm} }),
	)

	first, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:        fixtures.MakeJob(fixtures.WithJobID("warm-1")),
		Claim:      fixtures.MakeClaim(600, 100),
		NodeID:     node.ID,
		InstanceID: warm.ID,
	})
	if err != nil {
		t.Fatalf("first warm Offer: %v", err)
	}
	if _, err := admission.Commit(ctx, first.OfferID, first.Fence); err != nil {
		t.Fatalf("first warm Commit: %v", err)
	}
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:        fixtures.MakeJob(fixtures.WithJobID("warm-fits")),
		Claim:      fixtures.MakeClaim(600, 300),
		NodeID:     node.ID,
		InstanceID: warm.ID,
	}); err != nil {
		t.Fatalf("second warm should fit with weights counted once: %v", err)
	}
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:        fixtures.MakeJob(fixtures.WithJobID("warm-blocked")),
		Claim:      fixtures.MakeClaim(600, 350),
		NodeID:     node.ID,
		InstanceID: warm.ID,
	}); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("second warm overflow err = %v", err)
	}
}

func TestAdmissionSynthesizesBoundLeaseOccupancy(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	admission := NewAdmission(node, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	admission.leases["lease-a"] = admissionLease{
		lease: domain.Lease{
			ID:             "lease-a",
			JobID:          "job-a",
			InstanceID:     "inst-a",
			NodeID:         node.ID,
			AcceleratorSet: []int{0},
			Claim:          fixtures.MakeClaim(400, 50),
			Priority:       domain.PriorityNormal,
		},
		acceleratorSet: []int{0},
		priority:       domain.PriorityNormal,
	}
	admission.leases["lease-loading"] = admissionLease{
		lease: domain.Lease{
			ID:             "lease-loading",
			JobID:          "job-loading",
			NodeID:         node.ID,
			AcceleratorSet: []int{0},
			Claim:          fixtures.MakeClaim(1, 1),
			Priority:       domain.PriorityBackground,
		},
		acceleratorSet: []int{0},
		priority:       domain.PriorityBackground,
	}
	instances := admission.instancesLocked()
	if len(instances) != 2 || instances[0].ID != "inst-a" || instances[0].Claim != (fixtures.MakeClaim(400, 50)) || instances[1].ID != "lease-loading" {
		t.Fatalf("synthesized instances = %+v", instances)
	}

	live := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-b"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(400, 50)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	admission = NewAdmission(
		node,
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithAdmissionInstances(func() []domain.ModelInstance { return []domain.ModelInstance{live} }),
	)
	admission.leases["lease-b"] = admissionLease{
		lease: domain.Lease{
			ID:             "lease-b",
			JobID:          "job-b",
			InstanceID:     live.ID,
			NodeID:         node.ID,
			AcceleratorSet: []int{0},
			Claim:          fixtures.MakeClaim(0, 25),
			Priority:       domain.PriorityInteractive,
		},
		acceleratorSet: []int{0},
		priority:       domain.PriorityInteractive,
	}
	instances = admission.instancesLocked()
	if len(instances) != 1 || instances[0].Claim != (fixtures.MakeClaim(400, 75)) || instances[0].Priority != domain.PriorityInteractive {
		t.Fatalf("grouped live instance = %+v", instances)
	}
}

func TestAdmissionFailsLoudOnBadInputsAndUnavailableCapacity(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5)), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := admission.Offer(canceled, admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 1))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Offer err = %v", err)
	}
	if _, err := NewAdmission(fixtures.MakeNode(), nil, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))).Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 1))); err == nil || !strings.Contains(err.Error(), "allocator") {
		t.Fatalf("missing allocator err = %v", err)
	}
	if _, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(600, 0))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("no fit err = %v", err)
	}
	if _, err := admission.Commit(canceled, "missing", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Commit err = %v", err)
	}
	if _, err := admission.Commit(context.Background(), "missing", 1); err == nil {
		t.Fatal("expected missing offer error")
	}
	if err := admission.Release(canceled, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Release err = %v", err)
	}
	if err := admission.Release(context.Background(), "missing"); err == nil {
		t.Fatal("expected missing release error")
	}
	if err := admission.Preempt(canceled, "missing", "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Preempt err = %v", err)
	}
	if err := admission.Preempt(context.Background(), "missing", "test"); err == nil {
		t.Fatal("expected missing preempt error")
	}
	leaseA := commitAdmissionLease(t, admission, "job-a", fixtures.MakeClaim(100, 0))
	if err := admission.BindInstance(context.Background(), "", "inst-a"); err == nil || !strings.Contains(err.Error(), "lease id") {
		t.Fatalf("empty bind lease err = %v", err)
	}
	if err := admission.BindInstance(context.Background(), leaseA.ID, ""); err == nil || !strings.Contains(err.Error(), "instance id") {
		t.Fatalf("empty bind instance err = %v", err)
	}
	if err := admission.BindInstance(context.Background(), "missing", "inst-a"); err == nil {
		t.Fatal("expected missing bind error")
	}
	if err := admission.BindInstance(context.Background(), leaseA.ID, "inst-a"); err != nil {
		t.Fatalf("bind inst-a: %v", err)
	}
	if err := admission.BindInstance(context.Background(), leaseA.ID, "inst-b"); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("rebind err = %v", err)
	}
	if _, _, err := admission.LeaseForInstance(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "instance id") {
		t.Fatalf("empty LeaseForInstance err = %v", err)
	}
	if err := admission.Preempt(context.Background(), leaseA.ID, ""); err == nil || !strings.Contains(err.Error(), "reason") {
		t.Fatalf("empty preempt reason err = %v", err)
	}
	if _, _, err := admission.LeaseForJob(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("empty LeaseForJob err = %v", err)
	}
	if _, _, err := admission.LeaseForJob(canceled, "job-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled LeaseForJob err = %v", err)
	}
	if _, _, err := admission.LeaseForInstance(canceled, "inst-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled LeaseForInstance err = %v", err)
	}
	if err := admission.BindInstance(canceled, leaseA.ID, "inst-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled BindInstance err = %v", err)
	}

	maintenance := NewAdmission(fixtures.MakeNode(fixtures.Maintenance), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	if _, err := maintenance.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 1))); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("maintenance offer err = %v", err)
	}
}

func TestAdmissionRejectsOfferThatNoLongerFitsAtCommit(t *testing.T) {
	running := []domain.ModelInstance{}
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithAdmissionInstances(func() []domain.ModelInstance {
			return append([]domain.ModelInstance(nil), running...)
		}),
	)
	offer, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(600, 0)))
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	running = append(running, fixtures.MakeInstance(fixtures.WithClaim(fixtures.MakeClaim(500, 0))))
	if _, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("Commit after capacity change err = %v", err)
	}
}

func TestAdmissionRejectsExpiredOffer(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	admission := NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), clock, WithAdmissionOfferTTL(time.Second))
	offer, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 1)))
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	clock.Advance(time.Second)
	if _, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired Commit err = %v", err)
	}
	if len(admission.offers) != 0 || len(admission.leases) != 0 {
		t.Fatalf("expired offer state offers=%d leases=%d", len(admission.offers), len(admission.leases))
	}
}

func TestAdmissionAppliesSubmitterPolicyToOffersAndPreemption(t *testing.T) {
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		WithSubmitterPolicy(SubmitterPolicy{Rules: map[string]SubmitterRule{
			"submitter-a":  {MaxPriority: domain.PriorityNormal, AllowPrivate: true},
			"guest": {},
		}}),
	)
	claim := fixtures.MakeClaim(100, 0)

	missingSubmitter := fixtures.MakeJob(fixtures.WithJobID("job-missing"))
	if _, err := admission.Offer(context.Background(), admissionReq(missingSubmitter, claim)); err == nil || !strings.Contains(err.Error(), "submitter") {
		t.Fatalf("missing submitter err = %v", err)
	}
	unknown := fixtures.MakeJob(fixtures.WithJobID("job-unknown"))
	unknown.Submitter = "stranger"
	if _, err := admission.Offer(context.Background(), admissionReq(unknown, claim)); err == nil || !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("unknown submitter err = %v", err)
	}
	tooHigh := fixtures.MakeJob(fixtures.WithJobID("job-too-high"), fixtures.Interactive)
	tooHigh.Submitter = "guest"
	if _, err := admission.Offer(context.Background(), admissionReq(tooHigh, claim)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("too-high priority err = %v", err)
	}
	privateGuest := fixtures.MakeJob(fixtures.WithJobID("job-private-guest"), fixtures.Background)
	privateGuest.Submitter = "guest"
	privateGuest.Handling = domain.HandlingPrivate
	if _, err := admission.Offer(context.Background(), admissionReq(privateGuest, claim)); err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("private guest err = %v", err)
	}
	unknownPriority := fixtures.MakeJob(fixtures.WithJobID("job-unknown-priority"))
	unknownPriority.Submitter = "guest"
	unknownPriority.Priority = domain.Priority("custom")
	if _, err := admission.Offer(context.Background(), admissionReq(unknownPriority, claim)); err != nil {
		t.Fatalf("unknown priority should rank below policy max: %v", err)
	}

	victim := fixtures.MakeJob(fixtures.WithJobID("job-victim"), fixtures.Background)
	victim.Submitter = "submitter-a"
	victim.Handling = domain.HandlingPrivate
	offer, err := admission.Offer(context.Background(), admissionReq(victim, claim))
	if err != nil {
		t.Fatalf("victim Offer: %v", err)
	}
	lease, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("victim Commit: %v", err)
	}
	if err := admission.PreemptForJob(context.Background(), tooHigh, lease.ID, "policy check"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("preempt requester err = %v", err)
	}
	preempter := fixtures.MakeJob(fixtures.WithJobID("job-preempt"))
	preempter.Submitter = "submitter-a"
	preempter.Handling = domain.HandlingPrivate
	preempter.Preemption = domain.PreemptHard
	if err := admission.PreemptForJob(context.Background(), preempter, lease.ID, "policy check"); err != nil {
		t.Fatalf("PreemptForJob: %v", err)
	}
}

func TestAdmissionPreemptForJobFailsLoudOnPolicyAndLeaseState(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Unix(720, 0).UTC())
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), clock)
	leaseA := commitAdmissionLease(t, admission, "job-a", fixtures.MakeClaim(100, 0))
	if err := admission.PreemptForJob(ctx, fixtures.MakeJob(fixtures.WithJobID("soft")), leaseA.ID, "soft"); err == nil || !strings.Contains(err.Error(), "hard preemption") {
		t.Fatalf("soft preempt err = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	hard := fixtures.MakeJob(fixtures.WithJobID("hard"), fixtures.Interactive, fixtures.HardForInteractive)
	if err := admission.PreemptForJob(canceled, hard, leaseA.ID, "cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preempt err = %v", err)
	}
	if err := admission.PreemptForJob(ctx, hard, "missing", "missing"); err == nil || !strings.Contains(err.Error(), "unknown lease") {
		t.Fatalf("missing preempt err = %v", err)
	}
	samePriority := fixtures.MakeJob(fixtures.WithJobID("same"))
	samePriority.Preemption = domain.PreemptHard
	if err := admission.PreemptForJob(ctx, samePriority, leaseA.ID, "same"); err == nil || !strings.Contains(err.Error(), "cannot preempt") {
		t.Fatalf("same priority err = %v", err)
	}
	leaseB := commitAdmissionLease(t, admission, "job-b", fixtures.MakeClaim(100, 0))
	if err := admission.PreemptForJob(ctx, hard, leaseB.ID, ""); err == nil || !strings.Contains(err.Error(), "reason") {
		t.Fatalf("empty reason err = %v", err)
	}
}

func TestAdmissionStoreErrorsAreLoud(t *testing.T) {
	ctx := context.Background()
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	clock := mocks.NewFakeClock(time.Unix(730, 0).UTC())
	loadErr := errors.New("load state")
	saveErr := errors.New("save state")

	newWithStore := func(store *fakeAdmissionStateStore) *Admission {
		return NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	}
	if _, err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).Offer(ctx, admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 0))); !errors.Is(err, loadErr) {
		t.Fatalf("offer load err = %v", err)
	}
	if _, err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).Commit(ctx, "offer-a", 1); !errors.Is(err, loadErr) {
		t.Fatalf("commit load err = %v", err)
	}
	if err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).Release(ctx, "lease-a"); !errors.Is(err, loadErr) {
		t.Fatalf("release load err = %v", err)
	}
	if err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).Preempt(ctx, "lease-a", "test"); !errors.Is(err, loadErr) {
		t.Fatalf("preempt load err = %v", err)
	}
	if _, _, err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).LeaseForJob(ctx, "job-a"); !errors.Is(err, loadErr) {
		t.Fatalf("lease-for-job load err = %v", err)
	}
	if _, _, err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).LeaseForInstance(ctx, "inst-a"); !errors.Is(err, loadErr) {
		t.Fatalf("lease-for-instance load err = %v", err)
	}
	if err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).BindInstance(ctx, "lease-a", "inst-a"); !errors.Is(err, loadErr) {
		t.Fatalf("bind load err = %v", err)
	}
	hard := fixtures.MakeJob(fixtures.WithJobID("hard"), fixtures.Interactive, fixtures.HardForInteractive)
	if err := newWithStore(&fakeAdmissionStateStore{loadErr: loadErr}).PreemptForJob(ctx, hard, "lease-a", "test"); !errors.Is(err, loadErr) {
		t.Fatalf("preempt-for-job load err = %v", err)
	}
	if _, err := newWithStore(&fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1}, saveErr: saveErr}).Offer(ctx, admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 0))); !errors.Is(err, saveErr) {
		t.Fatalf("offer save err = %v", err)
	}

	offer := domain.LeaseOffer{OfferID: "offer-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 0), AcceleratorSet: []int{0}, Fence: 1, ExpiresAt: clock.Now().Add(-time.Second)}
	expiredStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a"))}}}, saveErr: saveErr}
	if _, err := newWithStore(expiredStore).Commit(ctx, offer.OfferID, offer.Fence); !errors.Is(err, saveErr) {
		t.Fatalf("expired save err = %v", err)
	}

	offer.ExpiresAt = clock.Now().Add(time.Minute)
	commitStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a"))}}}, saveErr: saveErr}
	if _, err := newWithStore(commitStore).Commit(ctx, offer.OfferID, offer.Fence); !errors.Is(err, saveErr) {
		t.Fatalf("commit save err = %v", err)
	}

	leaseRecord := domain.Lease{ID: "lease-a", JobID: "job-a", NodeID: node.ID, AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(1, 0)}
	bindStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Leases: []domain.AdmissionLeaseRecord{{Lease: leaseRecord}}}, saveErr: saveErr}
	if err := newWithStore(bindStore).BindInstance(ctx, leaseRecord.ID, "inst-a"); !errors.Is(err, saveErr) {
		t.Fatalf("bind save err = %v", err)
	}
	releaseStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Leases: []domain.AdmissionLeaseRecord{{Lease: leaseRecord}}}, saveErr: saveErr}
	if err := newWithStore(releaseStore).Release(ctx, leaseRecord.ID); !errors.Is(err, saveErr) {
		t.Fatalf("release save err = %v", err)
	}
}

func TestAdmissionRestoresZeroFenceAndOffers(t *testing.T) {
	ctx := context.Background()
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	clock := mocks.NewFakeClock(time.Unix(740, 0).UTC())
	offer := domain.LeaseOffer{OfferID: "offer-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 0), AcceleratorSet: []int{0}, Fence: 1, ExpiresAt: clock.Now().Add(time.Minute)}
	store := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{
		NodeID: node.ID,
		Fence:  0,
		Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a"))}},
	}}
	admission := NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	lease, err := admission.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit restored offer: %v", err)
	}
	if lease.ID == "" || lease.JobID != "job-a" {
		t.Fatalf("lease = %+v", lease)
	}
}

func commitAdmissionLease(t *testing.T, admission *Admission, jobID string, claim domain.Claim) domain.Lease {
	t.Helper()
	offer, err := admission.Offer(context.Background(), admissionReq(fixtures.MakeJob(fixtures.WithJobID(jobID)), claim))
	if err != nil {
		t.Fatalf("Offer %s: %v", jobID, err)
	}
	lease, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit %s: %v", jobID, err)
	}
	return lease
}

func admissionReq(job domain.Job, claim domain.Claim) domain.AdmissionRequest {
	return domain.AdmissionRequest{Job: job, Claim: claim}
}

type fakeAdmissionStateStore struct {
	state   domain.AdmissionState
	found   bool
	loadErr error
	saveErr error
	saved   []domain.AdmissionState
}

func (s *fakeAdmissionStateStore) AdmissionState(context.Context, string) (domain.AdmissionState, bool, error) {
	if s.loadErr != nil {
		return domain.AdmissionState{}, false, s.loadErr
	}
	return s.state, s.found, nil
}

func (s *fakeAdmissionStateStore) SaveAdmissionState(_ context.Context, state domain.AdmissionState) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.state = state
	s.found = true
	s.saved = append(s.saved, state)
	return nil
}
