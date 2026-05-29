package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const defaultAdmissionOfferTTL = 30 * time.Second

type Admission struct {
	mu        sync.Mutex
	node      domain.Node
	allocator ports.Allocator
	clock     ports.Clock
	offerTTL  time.Duration
	fence     uint64
	nextOffer int
	nextLease int
	offers    map[string]admissionOffer
	leases    map[string]admissionLease
}

type admissionOffer struct {
	offer          domain.LeaseOffer
	job            domain.Job
	acceleratorSet []int
}

type admissionLease struct {
	lease          domain.Lease
	acceleratorSet []int
	priority       domain.Priority
}

type AdmissionOption func(*Admission)

func NewAdmission(node domain.Node, allocator ports.Allocator, clock ports.Clock, opts ...AdmissionOption) *Admission {
	admission := &Admission{
		node:      node,
		allocator: allocator,
		clock:     clock,
		offerTTL:  defaultAdmissionOfferTTL,
		fence:     1,
		offers:    map[string]admissionOffer{},
		leases:    map[string]admissionLease{},
	}
	for _, opt := range opts {
		opt(admission)
	}
	return admission
}

func WithAdmissionOfferTTL(ttl time.Duration) AdmissionOption {
	return func(a *Admission) {
		a.offerTTL = ttl
	}
}

func (a *Admission) Offer(ctx context.Context, job domain.Job, claim domain.Claim) (domain.LeaseOffer, error) {
	if err := ctx.Err(); err != nil {
		return domain.LeaseOffer{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.allocator == nil {
		return domain.LeaseOffer{}, fmt.Errorf("admission allocator is not configured")
	}
	acceleratorSet, ok := a.firstFitLocked(claim)
	if !ok {
		return domain.LeaseOffer{}, fmt.Errorf("%w: node %q cannot offer capacity for job %q", domain.ErrNoFit, a.node.ID, job.ID)
	}

	a.nextOffer++
	offer := domain.LeaseOffer{
		OfferID:   fmt.Sprintf("offer-%s-%d", a.node.ID, a.nextOffer),
		JobID:     job.ID,
		NodeID:    a.node.ID,
		Claim:     claim,
		Fence:     a.fence,
		ExpiresAt: a.clock.Now().Add(a.offerTTL),
	}
	a.offers[offer.OfferID] = admissionOffer{offer: offer, job: job, acceleratorSet: acceleratorSet}
	return offer, nil
}

func (a *Admission) Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error) {
	if err := ctx.Err(); err != nil {
		return domain.Lease{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	offer, ok := a.offers[offerID]
	if !ok {
		return domain.Lease{}, fmt.Errorf("unknown lease offer %q", offerID)
	}
	if fence != a.fence || fence != offer.offer.Fence {
		return domain.Lease{}, domain.ErrStaleFence
	}
	if !offer.offer.ExpiresAt.IsZero() && !a.clock.Now().Before(offer.offer.ExpiresAt) {
		delete(a.offers, offerID)
		return domain.Lease{}, fmt.Errorf("lease offer %q expired at %s", offerID, offer.offer.ExpiresAt.Format(time.RFC3339))
	}
	if !a.fitsLocked(offer.acceleratorSet, offer.offer.Claim) {
		return domain.Lease{}, fmt.Errorf("%w: node %q can no longer fit offer %q", domain.ErrNoFit, a.node.ID, offerID)
	}

	a.nextLease++
	lease := domain.Lease{
		ID:         fmt.Sprintf("lease-%s-%d", a.node.ID, a.nextLease),
		JobID:      offer.offer.JobID,
		InstanceID: fmt.Sprintf("admission-%s", offer.offer.OfferID),
		NodeID:     a.node.ID,
		Claim:      offer.offer.Claim,
		GrantedAt:  a.clock.Now(),
	}
	a.leases[lease.ID] = admissionLease{lease: lease, acceleratorSet: offer.acceleratorSet, priority: offer.job.Priority}
	delete(a.offers, offerID)
	a.fence++
	return lease, nil
}

func (a *Admission) Release(ctx context.Context, leaseID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	return a.removeLeaseLocked(leaseID, "release")
}

func (a *Admission) Preempt(ctx context.Context, leaseID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if reason == "" {
		return fmt.Errorf("preempt lease %q: reason is required", leaseID)
	}
	return a.removeLeaseLocked(leaseID, "preempt")
}

func (a *Admission) firstFitLocked(claim domain.Claim) ([]int, bool) {
	if a.node.Status != domain.NodeReady {
		return nil, false
	}
	for _, accelerator := range a.node.Accelerators {
		acceleratorSet := []int{accelerator.Index}
		if a.fitsLocked(acceleratorSet, claim) {
			return acceleratorSet, true
		}
	}
	return nil, false
}

func (a *Admission) fitsLocked(acceleratorSet []int, claim domain.Claim) bool {
	instances := a.instancesLocked()
	return a.allocator.CanStackLoad(a.node, acceleratorSet, instances) && a.allocator.Fits(a.node, acceleratorSet, instances, claim)
}

func (a *Admission) instancesLocked() []domain.ModelInstance {
	instances := make([]domain.ModelInstance, 0, len(a.leases))
	for _, record := range a.leases {
		instances = append(instances, domain.ModelInstance{
			ID:             record.lease.InstanceID,
			NodeID:         record.lease.NodeID,
			AcceleratorSet: append([]int(nil), record.acceleratorSet...),
			Claim:          record.lease.Claim,
			State:          domain.InstReady,
			Priority:       record.priority,
		})
	}
	return instances
}

func (a *Admission) removeLeaseLocked(leaseID, op string) error {
	if _, ok := a.leases[leaseID]; !ok {
		return fmt.Errorf("%s unknown lease %q", op, leaseID)
	}
	delete(a.leases, leaseID)
	a.fence++
	return nil
}

var _ ports.AdmissionController = (*Admission)(nil)
