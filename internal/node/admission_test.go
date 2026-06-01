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

	offer, err := admission.Offer(context.Background(), job, claim)
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
	admission := NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
	first, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(10, 1))
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(10, 1))
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
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
	first, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(600, 0))
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(600, 0))
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
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
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
	if _, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("blocked")), fixtures.MakeClaim(400, 0)); !errors.Is(err, domain.ErrNoFit) {
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
	if _, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("admitted")), fixtures.MakeClaim(400, 0)); err != nil {
		t.Fatalf("Offer after release/preempt: %v", err)
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
		mocks.NewFakeClock(time.Now()),
		WithAdmissionInstances(func() []domain.ModelInstance {
			return append([]domain.ModelInstance(nil), running...)
		}),
	)
	if _, err := admission.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(400, 0)); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("running instance no-fit err = %v", err)
	}
	running = nil
	if _, err := admission.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(400, 0)); err != nil {
		t.Fatalf("offer after clearing running instances: %v", err)
	}
}

func TestAdmissionFailsLoudOnBadInputsAndUnavailableCapacity(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5)), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := admission.Offer(canceled, fixtures.MakeJob(), fixtures.MakeClaim(1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Offer err = %v", err)
	}
	if _, err := NewAdmission(fixtures.MakeNode(), nil, mocks.NewFakeClock(time.Now())).Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(1, 1)); err == nil || !strings.Contains(err.Error(), "allocator") {
		t.Fatalf("missing allocator err = %v", err)
	}
	if _, err := admission.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(600, 0)); !errors.Is(err, domain.ErrNoFit) {
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

	maintenance := NewAdmission(fixtures.MakeNode(fixtures.Maintenance), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
	if _, err := maintenance.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(1, 1)); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("maintenance offer err = %v", err)
	}
}

func TestAdmissionRejectsOfferThatNoLongerFitsAtCommit(t *testing.T) {
	running := []domain.ModelInstance{}
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Now()),
		WithAdmissionInstances(func() []domain.ModelInstance {
			return append([]domain.ModelInstance(nil), running...)
		}),
	)
	offer, err := admission.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(600, 0))
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
	offer, err := admission.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(1, 1))
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

func commitAdmissionLease(t *testing.T, admission *Admission, jobID string, claim domain.Claim) domain.Lease {
	t.Helper()
	offer, err := admission.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID(jobID)), claim)
	if err != nil {
		t.Fatalf("Offer %s: %v", jobID, err)
	}
	lease, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit %s: %v", jobID, err)
	}
	return lease
}
