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
		fixtures.MakeJob(), fixtures.MakePreset(), fixtures.MakeClaim(3, 4))
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
	preempting := fixtures.MakeJob(fixtures.WithJobID("job-c"), fixtures.Interactive, fixtures.HardForInteractive)
	if err := admission.PreemptForJob(context.Background(), preempting, leaseB.ID, "higher priority"); err != nil {
		t.Fatalf("PreemptForJob: %v", err)
	}
	if len(admission.leases) != 1 || admission.leases[leaseB.ID].state != domain.AdmissionLeasePreempting {
		t.Fatalf("preempt should preserve cleanup evidence as preempting lease, got %+v", admission.leases)
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
		Preset: fixtures.MakePreset(),
		Claim:  fixtures.MakeClaim(1, 0),
		NodeID: "node-b",
	}); err == nil || !strings.Contains(err.Error(), "targeted node") {
		t.Fatalf("wrong node err = %v", err)
	}
	offer, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:            fixtures.MakeJob(fixtures.WithJobID("job-acc")),
		Preset:         fixtures.MakePreset(),
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

func TestAdmissionOfferPreemptionValidation(t *testing.T) {
	ctx := context.Background()
	victimLease := func(t *testing.T, opts ...func(*domain.Job)) (*Admission, domain.Lease) {
		t.Helper()
		admission := NewAdmission(
			fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
			lease.NewAllocator(),
			mocks.NewFakeClock(time.Unix(760, 0).UTC()),
		)
		victim := fixtures.MakeJob(append([]func(*domain.Job){fixtures.WithJobID("victim"), fixtures.Background}, opts...)...)
		lease := commitAdmissionLease(t, admission, victim.ID, fixtures.MakeClaim(400, 0))
		return admission, lease
	}
	preemptReq := func(job domain.Job, targets ...domain.PreemptionTarget) domain.AdmissionRequest {
		req := admissionReq(job, fixtures.MakeClaim(700, 0))
		req.Preemptions = targets
		return req
	}
	hardJob := func(id string) domain.Job {
		return fixtures.MakeJob(fixtures.WithJobID(id), fixtures.Interactive, fixtures.HardForInteractive)
	}

	cases := []struct {
		name     string
		job      func(domain.Lease) domain.Job
		targets  func(domain.Lease) []domain.PreemptionTarget
		mutate   func(*Admission, domain.Lease)
		contains string
	}{{
		name: "soft job",
		job:  func(domain.Lease) domain.Job { return fixtures.MakeJob(fixtures.WithJobID("soft")) },
		targets: func(l domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: l.ID, Reason: "replace"}}
		},
		contains: "hard preemption",
	}, {
		name:     "missing lease id",
		job:      func(domain.Lease) domain.Job { return hardJob("missing-lease-id") },
		targets:  func(domain.Lease) []domain.PreemptionTarget { return []domain.PreemptionTarget{{Reason: "replace"}} },
		contains: "lease id",
	}, {
		name:     "missing reason",
		job:      func(domain.Lease) domain.Job { return hardJob("missing-reason") },
		targets:  func(l domain.Lease) []domain.PreemptionTarget { return []domain.PreemptionTarget{{LeaseID: l.ID}} },
		contains: "reason",
	}, {
		name: "duplicate",
		job:  func(domain.Lease) domain.Job { return hardJob("duplicate") },
		targets: func(l domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: l.ID, Reason: "one"}, {LeaseID: l.ID, Reason: "two"}}
		},
		contains: "duplicate",
	}, {
		name: "unknown",
		job:  func(domain.Lease) domain.Job { return hardJob("unknown") },
		targets: func(domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: "missing", Reason: "replace"}}
		},
		contains: "unknown",
	}, {
		name: "pinned",
		job:  func(domain.Lease) domain.Job { return hardJob("pinned") },
		mutate: func(admission *Admission, l domain.Lease) {
			record := admission.leases[l.ID]
			record.lease.Pinned = true
			admission.leases[l.ID] = record
		},
		targets: func(l domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: l.ID, Reason: "replace"}}
		},
		contains: "pinned",
	}, {
		name: "same priority",
		job: func(domain.Lease) domain.Job {
			return fixtures.MakeJob(fixtures.WithJobID("same-priority"), fixtures.Background, fixtures.Hard)
		},
		targets: func(l domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: l.ID, Reason: "replace"}}
		},
		contains: "cannot preempt",
	}, {
		name: "instance mismatch",
		job:  func(domain.Lease) domain.Job { return hardJob("instance-mismatch") },
		mutate: func(admission *Admission, l domain.Lease) {
			if err := admission.BindInstance(ctx, l.ID, "inst-victim"); err != nil {
				t.Fatalf("BindInstance: %v", err)
			}
		},
		targets: func(l domain.Lease) []domain.PreemptionTarget {
			return []domain.PreemptionTarget{{LeaseID: l.ID, InstanceID: "other-inst", Reason: "replace"}}
		},
		contains: "not owner-bound",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admission, lease := victimLease(t)
			if tc.mutate != nil {
				tc.mutate(admission, lease)
			}
			_, err := admission.Offer(ctx, preemptReq(tc.job(lease), tc.targets(lease)...))
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("Offer err = %v, want %q", err, tc.contains)
			}
		})
	}
}

func TestAdmissionPreemptionCommitIsOwnerAtomic(t *testing.T) {
	ctx := context.Background()
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Unix(770, 0).UTC()),
	)
	victim := fixtures.MakeJob(fixtures.WithJobID("victim"), fixtures.Background)
	victimLease := commitAdmissionLease(t, admission, victim.ID, fixtures.MakeClaim(700, 0))
	if err := admission.BindInstance(ctx, victimLease.ID, "inst-victim"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	preemptingJob := fixtures.MakeJob(fixtures.WithJobID("replacement"), fixtures.Interactive, fixtures.HardForInteractive)
	req := admissionReq(preemptingJob, fixtures.MakeClaim(700, 0))
	req.Preemptions = []domain.PreemptionTarget{{LeaseID: victimLease.ID, InstanceID: "inst-victim", Reason: "higher priority replacement"}}
	offer, err := admission.Offer(ctx, req)
	if err != nil {
		t.Fatalf("Offer with preemption: %v", err)
	}
	if len(admission.offers[offer.OfferID].preemptions) != 1 {
		t.Fatalf("stored preemptions = %+v", admission.offers[offer.OfferID].preemptions)
	}
	if !preemptionLeaseIDs(req.Preemptions)[victimLease.ID] || !preemptionLeaseIDs(req.Preemptions)["inst-victim"] {
		t.Fatalf("preemption ids missing lease/instance: %+v", preemptionLeaseIDs(req.Preemptions))
	}

	lease, err := admission.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit with preemption: %v", err)
	}
	if lease.JobID != preemptingJob.ID {
		t.Fatalf("replacement lease = %+v", lease)
	}
	if _, found, err := admission.LeaseForJob(ctx, victim.ID); err != nil || !found {
		t.Fatalf("victim lease missing before lifecycle cleanup found=%v err=%v", found, err)
	}
	if len(admission.leases) != 2 {
		t.Fatalf("leases after preempting commit = %+v", admission.leases)
	}
}

func TestAdmissionCommitRevalidatesPreemptionTargets(t *testing.T) {
	ctx := context.Background()
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Unix(780, 0).UTC()),
	)
	victimLease := commitAdmissionLease(t, admission, "victim", fixtures.MakeClaim(700, 0))
	req := admissionReq(fixtures.MakeJob(fixtures.WithJobID("replacement"), fixtures.Interactive, fixtures.HardForInteractive), fixtures.MakeClaim(700, 0))
	req.Preemptions = []domain.PreemptionTarget{{LeaseID: victimLease.ID, Reason: "higher priority replacement"}}
	offer, err := admission.Offer(ctx, req)
	if err != nil {
		t.Fatalf("Offer with preemption: %v", err)
	}
	record := admission.leases[victimLease.ID]
	record.lease.Pinned = true
	admission.leases[victimLease.ID] = record

	if _, err := admission.Commit(ctx, offer.OfferID, offer.Fence); err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("Commit revalidation err = %v", err)
	}
}

func TestAdmissionRollsBackOfferWhenStateSaveFails(t *testing.T) {
	ctx := context.Background()
	store := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: "node-a", Fence: 1}}
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Unix(785, 0).UTC()),
		WithAdmissionStateStore(store),
	)
	saveErr := errors.New("save failed")
	store.saveErr = saveErr

	_, err := admission.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(100, 0)))
	if !errors.Is(err, saveErr) {
		t.Fatalf("Offer err = %v", err)
	}
	if admission.nextOffer != 0 || len(admission.offers) != 0 || admission.fence != 1 {
		t.Fatalf("offer mutation leaked next=%d fence=%d offers=%+v", admission.nextOffer, admission.fence, admission.offers)
	}

	store.saveErr = nil
	offer, err := admission.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(100, 0)))
	if err != nil {
		t.Fatalf("Offer retry: %v", err)
	}
	if offer.OfferID != "offer-node-a-1" || len(admission.offers) != 1 {
		t.Fatalf("retry offer = %+v offers=%+v", offer, admission.offers)
	}
}

func TestAdmissionRollsBackPreemptingCommitWhenStateSaveFails(t *testing.T) {
	ctx := context.Background()
	store := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: "node-a", Fence: 1}}
	admission := NewAdmission(
		fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Unix(786, 0).UTC()),
		WithAdmissionStateStore(store),
	)
	victim := fixtures.MakeJob(fixtures.WithJobID("victim"), fixtures.Background)
	victimLease := commitAdmissionLease(t, admission, victim.ID, fixtures.MakeClaim(700, 0))
	if err := admission.BindInstance(ctx, victimLease.ID, "inst-victim"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	preemptingJob := fixtures.MakeJob(fixtures.WithJobID("replacement"), fixtures.Interactive, fixtures.HardForInteractive)
	req := admissionReq(preemptingJob, fixtures.MakeClaim(700, 0))
	req.Preemptions = []domain.PreemptionTarget{{LeaseID: victimLease.ID, InstanceID: "inst-victim", Reason: "higher priority replacement"}}
	offer, err := admission.Offer(ctx, req)
	if err != nil {
		t.Fatalf("Offer with preemption: %v", err)
	}
	beforeFence := admission.fence
	saveErr := errors.New("save failed")
	store.saveErr = saveErr

	_, err = admission.Commit(ctx, offer.OfferID, offer.Fence)
	if !errors.Is(err, saveErr) {
		t.Fatalf("Commit err = %v", err)
	}
	if admission.fence != beforeFence || len(admission.leases) != 1 {
		t.Fatalf("preempting commit leaked fence=%d leases=%+v", admission.fence, admission.leases)
	}
	if _, found, err := admission.LeaseForJob(ctx, victim.ID); err != nil || !found {
		t.Fatalf("victim lease after rollback found=%v err=%v", found, err)
	}
	if _, found, err := admission.LeaseForJob(ctx, preemptingJob.ID); err != nil || found {
		t.Fatalf("replacement lease after rollback found=%v err=%v", found, err)
	}
	if _, ok := admission.offers[offer.OfferID]; !ok {
		t.Fatalf("offer was removed after rollback: %+v", admission.offers)
	}

	store.saveErr = nil
	lease, err := admission.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit retry: %v", err)
	}
	if lease.JobID != preemptingJob.ID || len(admission.leases) != 2 {
		t.Fatalf("retry lease=%+v leases=%+v", lease, admission.leases)
	}
}

func TestAdmissionSelectAcceleratorWithPreemptionExclusions(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	node.Accelerators = append(node.Accelerators, node.Accelerators[0])
	node.Accelerators[1].Index = 1
	live := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("inst-0"), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(900, 0))),
		fixtures.MakeInstance(fixtures.WithInstanceID("inst-1"), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(900, 0))),
	}
	live[1].AcceleratorSet = []int{1}
	admission := NewAdmission(
		node,
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Unix(790, 0).UTC()),
		WithAdmissionInstances(func() []domain.ModelInstance { return append([]domain.ModelInstance(nil), live...) }),
	)

	admission.mu.Lock()
	got, ok := admission.selectAcceleratorSetLocked(domain.AdmissionRequest{}, fixtures.MakeClaim(500, 0), map[string]bool{"inst-0": true})
	admission.mu.Unlock()
	if !ok || len(got) != 1 || got[0] != 0 {
		t.Fatalf("select with exclusion = %v ok=%v", got, ok)
	}

	admission.mu.Lock()
	got, ok = admission.selectAcceleratorSetLocked(domain.AdmissionRequest{}, fixtures.MakeClaim(500, 0), map[string]bool{"missing": true})
	admission.mu.Unlock()
	if ok || got != nil {
		t.Fatalf("select without useful exclusion = %v ok=%v", got, ok)
	}

	admission.mu.Lock()
	got, ok = admission.selectAcceleratorSetLocked(domain.AdmissionRequest{InstanceID: "inst-0", AcceleratorSet: []int{1}}, fixtures.MakeClaim(1, 0), nil)
	admission.mu.Unlock()
	if ok || got != nil {
		t.Fatalf("warm instance accelerator mismatch = %v ok=%v", got, ok)
	}

	maintenance := NewAdmission(fixtures.MakeNode(fixtures.Maintenance), lease.NewAllocator(), mocks.NewFakeClock(time.Unix(800, 0).UTC()))
	maintenance.mu.Lock()
	got, ok = maintenance.selectAcceleratorSetLocked(domain.AdmissionRequest{}, fixtures.MakeClaim(1, 0), map[string]bool{"lease-a": true})
	maintenance.mu.Unlock()
	if ok || got != nil {
		t.Fatalf("maintenance select = %v ok=%v", got, ok)
	}

	if !sameAdmissionAcceleratorSet([]int{1, 0}, []int{0, 1}) {
		t.Fatal("sameAdmissionAcceleratorSet should ignore order")
	}
	if sameAdmissionAcceleratorSet([]int{0}, []int{0, 1}) {
		t.Fatal("different-length accelerator sets matched")
	}
	if sameAdmissionAcceleratorSet([]int{0, 2}, []int{0, 1}) {
		t.Fatal("different accelerator sets matched")
	}
}

func TestAdmissionEnforcesDiskHeadroomAtOffer(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1), fixtures.WithDisk(1000, 350))
	admission := NewAdmission(node, lease.NewAllocator(), clock)
	job := fixtures.MakeJob(fixtures.WithJobID("disk-job"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("disk-preset"), fixtures.WithArtifactSize(120), fixtures.WithWeights(100))
	req := domain.AdmissionRequest{Job: job, Preset: preset, Claim: fixtures.MakeClaim(100, 0), NodeID: node.ID}

	if _, err := admission.Offer(ctx, req); !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "disk floor") {
		t.Fatalf("disk Offer err = %v", err)
	}

	node = fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1), fixtures.WithDisk(1000, 250))
	admission = NewAdmission(node, lease.NewAllocator(), clock)
	req.NodeID = node.ID
	if _, err := admission.Offer(ctx, req); !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "free disk") {
		t.Fatalf("floor Offer err = %v", err)
	}

	localPreset := fixtures.MakePreset(fixtures.WithPresetID("local-preset"), fixtures.WithPresetNode(node.ID), fixtures.WithArtifactSize(120), fixtures.WithWeights(100))
	localReq := domain.AdmissionRequest{Job: fixtures.MakeJob(fixtures.WithJobID("local-job")), Preset: localPreset, Claim: fixtures.MakeClaim(100, 0), NodeID: node.ID}
	if _, err := admission.Offer(ctx, localReq); !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "free disk") {
		t.Fatalf("local preset still needs current headroom err = %v", err)
	}
}

func TestAdmissionRequiresPresetForColdDiskProof(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithDisk(1000, 500), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission := NewAdmission(node, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))

	_, err := admission.Offer(context.Background(), domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("missing-preset")),
		Claim:  fixtures.MakeClaim(100, 0),
		NodeID: node.ID,
	})
	if !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "missing preset") {
		t.Fatalf("missing preset err = %v", err)
	}
}

func TestAdmissionDiskProofBranches(t *testing.T) {
	ctx := context.Background()
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))

	noTelemetry := fixtures.MakeNode(fixtures.WithDisk(0, 0), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission := NewAdmission(noTelemetry, lease.NewAllocator(), clock)
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("no-disk-telemetry")),
		Claim:  fixtures.MakeClaim(100, 0),
		NodeID: noTelemetry.ID,
	}); err != nil {
		t.Fatalf("unknown disk telemetry should not block owner admission: %v", err)
	}

	invalidRatio := fixtures.MakeNode(fixtures.WithDisk(1000, 500), fixtures.WithDiskMinFreeRatio(1.1), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission = NewAdmission(invalidRatio, lease.NewAllocator(), clock)
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("invalid-ratio")),
		Preset: fixtures.MakePreset(fixtures.WithArtifactSize(1), fixtures.WithWeights(1)),
		Claim:  fixtures.MakeClaim(1, 0),
		NodeID: invalidRatio.ID,
	}); !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "invalid disk_min_free_ratio") {
		t.Fatalf("invalid ratio err = %v", err)
	}

	localNode := fixtures.MakeNode(fixtures.WithDisk(1000, 260), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission = NewAdmission(localNode, lease.NewAllocator(), clock)
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("local-artifact")),
		Preset: fixtures.MakePreset(fixtures.WithPresetNode(localNode.ID), fixtures.WithArtifactSize(120), fixtures.WithWeights(100)),
		Claim:  fixtures.MakeClaim(100, 0),
		NodeID: localNode.ID,
	}); err != nil {
		t.Fatalf("local artifact should not consume staging disk: %v", err)
	}

	estimateOnly := fixtures.MakeNode(fixtures.WithDisk(1000, 500), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission = NewAdmission(estimateOnly, lease.NewAllocator(), clock)
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("estimate-artifact")),
		Preset: fixtures.MakePreset(fixtures.WithArtifactSize(0), fixtures.WithWeights(50)),
		Claim:  fixtures.MakeClaim(50, 0),
		NodeID: estimateOnly.ID,
	}); err != nil {
		t.Fatalf("estimated weights should prove artifact disk when explicit size is absent: %v", err)
	}

	defaultRatio := fixtures.MakeNode(fixtures.WithDisk(1000, 500), fixtures.WithDiskMinFreeRatio(0), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	admission = NewAdmission(defaultRatio, lease.NewAllocator(), clock)
	if _, err := admission.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("default-ratio")),
		Preset: fixtures.MakePreset(fixtures.WithArtifactSize(1), fixtures.WithWeights(1)),
		Claim:  fixtures.MakeClaim(1, 0),
		NodeID: defaultRatio.ID,
	}); err != nil {
		t.Fatalf("zero disk ratio should use default floor: %v", err)
	}

	if required, err := admissionArtifactRequired(domain.Preset{}, estimateOnly, domain.Claim{}); err != nil || required != 0 {
		t.Fatalf("zero cold claim required=%d err=%v", required, err)
	}
	if _, err := admissionArtifactRequired(fixtures.MakePreset(fixtures.WithArtifactSize(-1)), estimateOnly, fixtures.MakeClaim(1, 0)); err == nil || !strings.Contains(err.Error(), "invalid artifact") {
		t.Fatalf("negative artifact err = %v", err)
	}
	if _, err := admissionArtifactRequired(fixtures.MakePreset(fixtures.WithArtifactSize(0), fixtures.WithWeights(0)), estimateOnly, fixtures.MakeClaim(1, 0)); err == nil || !strings.Contains(err.Error(), "no artifact size proof") {
		t.Fatalf("missing size proof err = %v", err)
	}
}

func TestAdmissionRechecksDiskHeadroomAtCommitAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1), fixtures.WithDisk(1000, 500))
	preset := fixtures.MakePreset(fixtures.WithPresetID("disk-preset"), fixtures.WithArtifactSize(100), fixtures.WithWeights(100))
	first := NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	offer, err := first.Offer(ctx, domain.AdmissionRequest{
		Job:    fixtures.MakeJob(fixtures.WithJobID("disk-commit")),
		Preset: preset,
		Claim:  fixtures.MakeClaim(100, 0),
		NodeID: node.ID,
	})
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}

	node.DiskFreeMB = 320
	restarted := NewAdmission(node, lease.NewAllocator(), clock, WithAdmissionStateStore(store))
	if _, err := restarted.Commit(ctx, offer.OfferID, offer.Fence); !errors.Is(err, domain.ErrNoFit) || !strings.Contains(err.Error(), "disk floor") {
		t.Fatalf("commit disk err = %v", err)
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
	if err := admission.Preempt(context.Background(), "missing", "test"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected disabled preempt error, got %v", err)
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
	if err := admission.Preempt(context.Background(), leaseA.ID, ""); err == nil || !strings.Contains(err.Error(), "disabled") {
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
			"submitter-a": {MaxPriority: domain.PriorityNormal, AllowPrivate: true},
			"guest":       {},
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
	if err := admission.PreemptForJob(ctx, hard, leaseA.ID, "first"); err != nil {
		t.Fatalf("first preempt err = %v", err)
	}
	if err := admission.PreemptForJob(ctx, hard, leaseA.ID, "again"); err == nil || !strings.Contains(err.Error(), "already preempting") {
		t.Fatalf("already preempting err = %v", err)
	}
	req := admissionReq(hard, fixtures.MakeClaim(100, 0))
	req.Preemptions = []domain.PreemptionTarget{{LeaseID: leaseA.ID, Reason: "already preempting"}}
	if _, err := admission.Offer(ctx, req); err == nil || !strings.Contains(err.Error(), "already preempting") {
		t.Fatalf("offer already preempting err = %v", err)
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
	if _, err := newWithStore(&fakeAdmissionStateStore{saveErr: saveErr}).Offer(ctx, admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 0))); !errors.Is(err, saveErr) {
		t.Fatalf("initial save err = %v", err)
	}
	if _, err := newWithStore(&fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1}, saveErr: saveErr}).Offer(ctx, admissionReq(fixtures.MakeJob(), fixtures.MakeClaim(1, 0))); !errors.Is(err, saveErr) {
		t.Fatalf("offer save err = %v", err)
	}

	offer := domain.LeaseOffer{OfferID: "offer-a", JobID: "job-a", NodeID: node.ID, Claim: fixtures.MakeClaim(1, 0), AcceleratorSet: []int{0}, Fence: 1, ExpiresAt: clock.Now().Add(-time.Second)}
	storedPreset := fixtures.MakePreset(fixtures.WithArtifactSize(1), fixtures.WithWeights(1))
	expiredStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a")), Preset: storedPreset}}}, saveErr: saveErr}
	if _, err := newWithStore(expiredStore).Commit(ctx, offer.OfferID, offer.Fence); !errors.Is(err, saveErr) {
		t.Fatalf("expired save err = %v", err)
	}

	offer.ExpiresAt = clock.Now().Add(time.Minute)
	commitStore := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{NodeID: node.ID, Fence: 1, Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a")), Preset: storedPreset}}}, saveErr: saveErr}
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
		Offers: []domain.AdmissionOfferRecord{{Offer: offer, Job: fixtures.MakeJob(fixtures.WithJobID("job-a")), Preset: fixtures.MakePreset(fixtures.WithArtifactSize(1), fixtures.WithWeights(1))}},
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

func TestReconcileAdmissionStateDropsStartupPoison(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(900, 0).UTC()
	store := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{
		NodeID:    "node-a",
		Fence:     4,
		NextOffer: 3,
		NextLease: 2,
		Offers: []domain.AdmissionOfferRecord{{
			Offer: domain.LeaseOffer{OfferID: "expired", JobID: "old-offer", NodeID: "node-a", ExpiresAt: now.Add(-time.Second)},
			Job:   fixtures.MakeJob(fixtures.WithJobID("old-offer")),
		}},
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: domain.Lease{ID: "unbound", JobID: "failed-job", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(900, 0)},
		}, {
			Lease: domain.Lease{ID: "missing", JobID: "missing-inst", InstanceID: "gone", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(100, 0)},
		}, {
			Lease: domain.Lease{ID: "live", JobID: "live-job", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(100, 0)},
		}},
	}}
	live := []domain.ModelInstance{fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode("node-a"), fixtures.WithClaim(fixtures.MakeClaim(100, 0)))}

	result, err := ReconcileAdmissionState(ctx, store, "node-a", live, now)
	if err != nil {
		t.Fatalf("ReconcileAdmissionState: %v", err)
	}
	if result.DroppedExpiredOffers != 1 || result.DroppedUnboundLeases != 1 || result.DroppedMissingInstanceLeases != 1 {
		t.Fatalf("result = %+v", result)
	}
	if store.state.Fence != 5 || len(store.state.Offers) != 0 || len(store.state.Leases) != 1 || store.state.Leases[0].Lease.ID != "live" {
		t.Fatalf("state = %+v", store.state)
	}

	admission := NewAdmission(fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1)), lease.NewAllocator(), mocks.NewFakeClock(now), WithAdmissionStateStore(store), WithAdmissionInstances(func() []domain.ModelInstance { return live }))
	if _, err := admission.Offer(ctx, admissionReq(fixtures.MakeJob(fixtures.WithJobID("admitted")), fixtures.MakeClaim(800, 0))); err != nil {
		t.Fatalf("Offer after reconcile: %v", err)
	}
}

func TestReconcileAdmissionStateDedupesJobInstanceLeases(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(910, 0).UTC()
	store := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{
		NodeID: "node-a",
		Fence:  7,
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: domain.Lease{ID: "zero", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, GrantedAt: now.Add(-time.Second)},
		}, {
			Lease: domain.Lease{ID: "weighted", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(600, 0), GrantedAt: now},
		}},
	}}
	live := []domain.ModelInstance{fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode("node-a"), fixtures.WithClaim(fixtures.MakeClaim(600, 0)))}

	result, err := ReconcileAdmissionState(ctx, store, "node-a", live, now)
	if err != nil {
		t.Fatalf("ReconcileAdmissionState: %v", err)
	}
	if result.DroppedDuplicateLeases != 1 {
		t.Fatalf("result = %+v", result)
	}
	if len(store.state.Leases) != 1 || store.state.Leases[0].Lease.ID != "weighted" || store.state.Fence != 8 {
		t.Fatalf("state = %+v", store.state)
	}
}

func TestReconcileAdmissionStateBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(920, 0).UTC()
	if result, err := ReconcileAdmissionState(ctx, nil, "node-a", nil, now); err != nil || result != (AdmissionReconcileResult{}) {
		t.Fatalf("nil store result=%+v err=%v", result, err)
	}
	if _, err := ReconcileAdmissionState(ctx, &fakeAdmissionStateStore{}, "", nil, now); err == nil || !strings.Contains(err.Error(), "node id") {
		t.Fatalf("empty node err = %v", err)
	}
	loadErr := errors.New("load")
	if _, err := ReconcileAdmissionState(ctx, &fakeAdmissionStateStore{loadErr: loadErr}, "node-a", nil, now); !errors.Is(err, loadErr) {
		t.Fatalf("load err = %v", err)
	}
	if result, err := ReconcileAdmissionState(ctx, &fakeAdmissionStateStore{}, "node-a", nil, now); err != nil || result != (AdmissionReconcileResult{}) {
		t.Fatalf("missing state result=%+v err=%v", result, err)
	}

	live := []domain.ModelInstance{fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode("node-a"))}
	unchanged := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{
		NodeID: "node-a",
		Fence:  0,
		Offers: []domain.AdmissionOfferRecord{{
			Offer: domain.LeaseOffer{OfferID: "offer-a", JobID: "job-a", NodeID: "node-a", ExpiresAt: now.Add(time.Minute)},
		}},
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: domain.Lease{ID: "lease-a", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}},
		}},
	}}
	if result, err := ReconcileAdmissionState(ctx, unchanged, "node-a", live, now); err != nil || result != (AdmissionReconcileResult{}) || len(unchanged.saved) != 0 {
		t.Fatalf("unchanged result=%+v saved=%d err=%v", result, len(unchanged.saved), err)
	}

	saveErr := errors.New("save")
	expiredLease := &fakeAdmissionStateStore{found: true, saveErr: saveErr, state: domain.AdmissionState{
		NodeID: "node-a",
		Fence:  1,
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: domain.Lease{ID: "expired", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, ExpiresAt: now.Add(-time.Second)},
		}},
	}}
	if _, err := ReconcileAdmissionState(ctx, expiredLease, "node-a", live, now); !errors.Is(err, saveErr) {
		t.Fatalf("save err = %v", err)
	}

	tie := &fakeAdmissionStateStore{found: true, state: domain.AdmissionState{
		NodeID: "node-a",
		Fence:  1,
		Leases: []domain.AdmissionLeaseRecord{{
			Lease: domain.Lease{ID: "newer", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(100, 0), GrantedAt: now},
		}, {
			Lease: domain.Lease{ID: "older", JobID: "job-a", InstanceID: "inst-a", NodeID: "node-a", AcceleratorSet: []int{0}, Claim: fixtures.MakeClaim(100, 0), GrantedAt: now.Add(-time.Second)},
		}},
	}}
	if result, err := ReconcileAdmissionState(ctx, tie, "node-a", live, now); err != nil || result.DroppedDuplicateLeases != 1 {
		t.Fatalf("tie result=%+v err=%v", result, err)
	}
	if len(tie.state.Leases) != 1 || tie.state.Leases[0].Lease.ID != "newer" {
		t.Fatalf("tie state = %+v", tie.state)
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
	return domain.AdmissionRequest{Job: job, Preset: fixtures.MakePreset(), Claim: claim}
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
