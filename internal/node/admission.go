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
	instances func() []domain.ModelInstance
	policy    SubmitterPolicy
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

func WithAdmissionInstances(instances func() []domain.ModelInstance) AdmissionOption {
	return func(a *Admission) {
		a.instances = instances
	}
}

func WithSubmitterPolicy(policy SubmitterPolicy) AdmissionOption {
	return func(a *Admission) {
		a.policy = policy
	}
}

type SubmitterPolicy struct {
	Rules map[string]SubmitterRule
}

type SubmitterRule struct {
	MaxPriority  domain.Priority
	AllowPrivate bool
}

func (a *Admission) Offer(ctx context.Context, job domain.Job, claim domain.Claim) (domain.LeaseOffer, error) {
	if err := ctx.Err(); err != nil {
		return domain.LeaseOffer{}, err
	}
	if err := a.authorize(job); err != nil {
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

func (a *Admission) PreemptForJob(ctx context.Context, job domain.Job, leaseID, reason string) error {
	if err := a.authorize(job); err != nil {
		return err
	}
	return a.Preempt(ctx, leaseID, reason)
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
		ID:        fmt.Sprintf("lease-%s-%d", a.node.ID, a.nextLease),
		JobID:     offer.offer.JobID,
		NodeID:    a.node.ID,
		Claim:     offer.offer.Claim,
		GrantedAt: a.clock.Now(),
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

func (a *Admission) LeaseForJob(ctx context.Context, jobID string) (domain.Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Lease{}, false, err
	}
	if jobID == "" {
		return domain.Lease{}, false, fmt.Errorf("job id is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, record := range a.leases {
		if record.lease.JobID == jobID {
			return record.lease, true, nil
		}
	}
	return domain.Lease{}, false, nil
}

func (a *Admission) LeaseForInstance(ctx context.Context, instanceID string) (domain.Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Lease{}, false, err
	}
	if instanceID == "" {
		return domain.Lease{}, false, fmt.Errorf("instance id is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, record := range a.leases {
		if record.lease.InstanceID == instanceID {
			return record.lease, true, nil
		}
	}
	return domain.Lease{}, false, nil
}

func (a *Admission) BindInstance(ctx context.Context, leaseID, instanceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if leaseID == "" {
		return fmt.Errorf("lease id is required")
	}
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.leases[leaseID]
	if !ok {
		return fmt.Errorf("bind unknown lease %q", leaseID)
	}
	if record.lease.InstanceID != "" && record.lease.InstanceID != instanceID {
		return fmt.Errorf("lease %q is already bound to instance %q", leaseID, record.lease.InstanceID)
	}
	record.lease.InstanceID = instanceID
	a.leases[leaseID] = record
	return nil
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
	if a.instances != nil {
		instances = append(instances, a.instances()...)
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

func (a *Admission) authorize(job domain.Job) error {
	if len(a.policy.Rules) == 0 {
		return nil
	}
	if job.Submitter == "" {
		return fmt.Errorf("submitter is required by admission policy")
	}
	rule, ok := a.policy.Rules[job.Submitter]
	if !ok {
		return fmt.Errorf("submitter %q is not authorized", job.Submitter)
	}
	if job.Handling == domain.HandlingPrivate && !rule.AllowPrivate {
		return fmt.Errorf("submitter %q is not authorized for private handling", job.Submitter)
	}
	if priorityRank(job.Priority) > priorityRank(rule.maxPriority()) {
		return fmt.Errorf("submitter %q priority %q exceeds maximum %q", job.Submitter, job.Priority, rule.maxPriority())
	}
	return nil
}

func (r SubmitterRule) maxPriority() domain.Priority {
	if r.MaxPriority == "" {
		return domain.PriorityBackground
	}
	return r.MaxPriority
}

func priorityRank(priority domain.Priority) int {
	switch priority {
	case domain.PriorityInteractive:
		return 3
	case domain.PriorityNormal, "":
		return 2
	case domain.PriorityBackground:
		return 1
	default:
		return 0
	}
}

var _ ports.AdmissionController = (*Admission)(nil)
var _ ports.PolicyPreempter = (*Admission)(nil)
var _ ports.LeaseInspector = (*Admission)(nil)
var _ ports.LeaseBinder = (*Admission)(nil)
