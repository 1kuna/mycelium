package node

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const defaultAdmissionOfferTTL = 30 * time.Second

type Admission struct {
	mu                 sync.Mutex
	node               domain.Node
	allocator          ports.Allocator
	clock              ports.Clock
	store              AdmissionStateStore
	offerTTL           time.Duration
	instances          func() []domain.ModelInstance
	policy             SubmitterPolicy
	pinnedReservations map[string]bool
	loaded             bool
	fence              uint64
	nextOffer          int
	nextLease          int
	offers             map[string]admissionOffer
	leases             map[string]admissionLease
}

type AdmissionStateStore interface {
	AdmissionState(ctx context.Context, nodeID string) (domain.AdmissionState, bool, error)
	SaveAdmissionState(ctx context.Context, state domain.AdmissionState) error
}

type AdmissionReconcileResult struct {
	DroppedExpiredOffers         int
	DroppedUnboundLeases         int
	DroppedMissingInstanceLeases int
	DroppedDuplicateLeases       int
}

type admissionOffer struct {
	offer          domain.LeaseOffer
	job            domain.Job
	preset         domain.Preset
	acceleratorSet []int
	instanceID     string
	reservationID  string
	pinned         bool
	preemptions    []domain.PreemptionTarget
}

type admissionLease struct {
	lease          domain.Lease
	acceleratorSet []int
	priority       domain.Priority
	state          domain.AdmissionLeaseState
}

type admissionSnapshot struct {
	fence     uint64
	nextOffer int
	nextLease int
	offers    map[string]admissionOffer
	leases    map[string]admissionLease
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

func WithAdmissionStateStore(store AdmissionStateStore) AdmissionOption {
	return func(a *Admission) {
		a.store = store
	}
}

func WithPinnedReservations(ids ...string) AdmissionOption {
	return func(a *Admission) {
		if a.pinnedReservations == nil {
			a.pinnedReservations = map[string]bool{}
		}
		for _, id := range ids {
			if id != "" {
				a.pinnedReservations[id] = true
			}
		}
	}
}

func ReconcileAdmissionState(ctx context.Context, store AdmissionStateStore, nodeID string, live []domain.ModelInstance, now time.Time) (AdmissionReconcileResult, error) {
	if store == nil {
		return AdmissionReconcileResult{}, nil
	}
	if nodeID == "" {
		return AdmissionReconcileResult{}, fmt.Errorf("node id is required")
	}
	state, found, err := store.AdmissionState(ctx, nodeID)
	if err != nil || !found {
		return AdmissionReconcileResult{}, err
	}
	if state.Fence == 0 {
		state.Fence = 1
	}
	result := AdmissionReconcileResult{}
	changed := false

	offers := state.Offers[:0]
	for _, rec := range state.Offers {
		if !rec.Offer.ExpiresAt.IsZero() && !now.Before(rec.Offer.ExpiresAt) {
			result.DroppedExpiredOffers++
			changed = true
			continue
		}
		offers = append(offers, rec)
	}
	state.Offers = offers

	liveInstances := map[string]bool{}
	for _, inst := range live {
		if inst.ID != "" {
			liveInstances[inst.ID] = true
		}
	}
	keptByJobInstance := map[string]int{}
	leases := make([]domain.AdmissionLeaseRecord, 0, len(state.Leases))
	for _, rec := range state.Leases {
		lease := rec.Lease
		if !lease.ExpiresAt.IsZero() && !now.Before(lease.ExpiresAt) {
			result.DroppedUnboundLeases++
			changed = true
			continue
		}
		if lease.InstanceID == "" {
			result.DroppedUnboundLeases++
			changed = true
			continue
		}
		if !liveInstances[lease.InstanceID] {
			result.DroppedMissingInstanceLeases++
			changed = true
			continue
		}
		key := lease.JobID + "\x00" + lease.InstanceID
		if existing, ok := keptByJobInstance[key]; ok {
			if betterAdmissionLease(lease, leases[existing].Lease) {
				leases[existing] = rec
			}
			result.DroppedDuplicateLeases++
			changed = true
			continue
		}
		keptByJobInstance[key] = len(leases)
		leases = append(leases, rec)
	}
	state.Leases = leases
	if !changed {
		return result, nil
	}
	state.Fence++
	if err := store.SaveAdmissionState(ctx, state); err != nil {
		return AdmissionReconcileResult{}, err
	}
	return result, nil
}

func betterAdmissionLease(left, right domain.Lease) bool {
	leftClaim := left.Claim.WeightsMB + left.Claim.KVReservedMB
	rightClaim := right.Claim.WeightsMB + right.Claim.KVReservedMB
	if leftClaim != rightClaim {
		return leftClaim > rightClaim
	}
	return left.GrantedAt.After(right.GrantedAt)
}

type SubmitterPolicy struct {
	Rules map[string]SubmitterRule
}

type SubmitterRule struct {
	MaxPriority  domain.Priority
	AllowPrivate bool
}

func (a *Admission) Offer(ctx context.Context, req domain.AdmissionRequest) (domain.LeaseOffer, error) {
	if err := ctx.Err(); err != nil {
		return domain.LeaseOffer{}, err
	}
	job := req.Job
	if err := a.authorize(job); err != nil {
		return domain.LeaseOffer{}, err
	}
	if req.NodeID != "" && req.NodeID != a.node.ID {
		return domain.LeaseOffer{}, fmt.Errorf("admission request targeted node %q but owner is %q", req.NodeID, a.node.ID)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadStateLocked(ctx); err != nil {
		return domain.LeaseOffer{}, err
	}
	if a.allocator == nil {
		return domain.LeaseOffer{}, fmt.Errorf("admission allocator is not configured")
	}
	if err := a.validatePreemptionsLocked(req.Job, req.Preemptions); err != nil {
		return domain.LeaseOffer{}, err
	}
	preempting := preemptionLeaseIDs(req.Preemptions)
	claim := incrementalClaim(req)
	acceleratorSet, ok := a.selectAcceleratorSetLocked(req, claim, preempting)
	if !ok {
		return domain.LeaseOffer{}, fmt.Errorf("%w: node %q cannot offer capacity for job %q", domain.ErrNoFit, a.node.ID, job.ID)
	}
	if err := a.diskSafeLocked(req.Preset, req.InstanceID, claim, job.ID); err != nil {
		return domain.LeaseOffer{}, err
	}

	before := a.snapshotLocked()
	a.nextOffer++
	offer := domain.LeaseOffer{
		OfferID:        fmt.Sprintf("offer-%s-%d", a.node.ID, a.nextOffer),
		JobID:          job.ID,
		NodeID:         a.node.ID,
		Claim:          claim,
		AcceleratorSet: append([]int(nil), acceleratorSet...),
		InstanceID:     req.InstanceID,
		ReservationID:  req.ReservationID,
		Fence:          a.fence,
		ExpiresAt:      a.clock.Now().Add(a.offerTTL),
	}
	a.offers[offer.OfferID] = admissionOffer{
		offer:          offer,
		job:            job,
		preset:         req.Preset,
		acceleratorSet: append([]int(nil), acceleratorSet...),
		instanceID:     req.InstanceID,
		reservationID:  req.ReservationID,
		pinned:         a.pinnedReservations[req.ReservationID],
		preemptions:    clonePreemptions(req.Preemptions),
	}
	if err := a.saveStateWithRollbackLocked(ctx, before); err != nil {
		return domain.LeaseOffer{}, err
	}
	return offer, nil
}

func (a *Admission) PreemptForJob(ctx context.Context, job domain.Job, leaseID, reason string) error {
	if err := a.authorize(job); err != nil {
		return err
	}
	if !admissionHardPreemptionAllowed(job) {
		return fmt.Errorf("job %q does not allow hard preemption", job.ID)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadStateLocked(ctx); err != nil {
		return err
	}
	record, ok := a.leases[leaseID]
	if !ok {
		return fmt.Errorf("preempt unknown lease %q", leaseID)
	}
	if record.lease.Pinned {
		return fmt.Errorf("lease %q is pinned and cannot be preempted", leaseID)
	}
	if admissionLeaseState(record) == domain.AdmissionLeasePreempting {
		return fmt.Errorf("lease %q is already preempting", leaseID)
	}
	if priorityRank(job.Priority) <= priorityRank(record.priority) {
		return fmt.Errorf("job %q priority %q cannot preempt lease %q priority %q", job.ID, job.Priority, leaseID, record.priority)
	}
	if reason == "" {
		return fmt.Errorf("preempt lease %q: reason is required", leaseID)
	}
	before := a.snapshotLocked()
	record.state = domain.AdmissionLeasePreempting
	a.leases[leaseID] = record
	a.fence++
	return a.saveStateWithRollbackLocked(ctx, before)
}

func (a *Admission) Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error) {
	if err := ctx.Err(); err != nil {
		return domain.Lease{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadStateLocked(ctx); err != nil {
		return domain.Lease{}, err
	}
	offer, ok := a.offers[offerID]
	if !ok {
		return domain.Lease{}, fmt.Errorf("unknown lease offer %q", offerID)
	}
	if fence != a.fence || fence != offer.offer.Fence {
		return domain.Lease{}, domain.ErrStaleFence
	}
	if !offer.offer.ExpiresAt.IsZero() && !a.clock.Now().Before(offer.offer.ExpiresAt) {
		before := a.snapshotLocked()
		delete(a.offers, offerID)
		if err := a.saveStateWithRollbackLocked(ctx, before); err != nil {
			return domain.Lease{}, err
		}
		return domain.Lease{}, fmt.Errorf("lease offer %q expired at %s", offerID, offer.offer.ExpiresAt.Format(time.RFC3339))
	}
	if err := a.validatePreemptionsLocked(offer.job, offer.preemptions); err != nil {
		return domain.Lease{}, err
	}
	preempting := preemptionLeaseIDs(offer.preemptions)
	if !a.fitsLockedExcept(offer.acceleratorSet, offer.offer.Claim, preempting) {
		return domain.Lease{}, fmt.Errorf("%w: node %q can no longer fit offer %q", domain.ErrNoFit, a.node.ID, offerID)
	}
	if err := a.diskSafeLocked(offer.preset, offer.instanceID, offer.offer.Claim, offer.job.ID); err != nil {
		return domain.Lease{}, err
	}
	before := a.snapshotLocked()
	for _, target := range offer.preemptions {
		record := a.leases[target.LeaseID]
		record.state = domain.AdmissionLeasePreempting
		a.leases[target.LeaseID] = record
	}
	a.nextLease++
	lease := domain.Lease{
		ID:             fmt.Sprintf("lease-%s-%d", a.node.ID, a.nextLease),
		JobID:          offer.offer.JobID,
		InstanceID:     offer.instanceID,
		NodeID:         a.node.ID,
		AcceleratorSet: append([]int(nil), offer.acceleratorSet...),
		Claim:          offer.offer.Claim,
		Priority:       offer.job.Priority,
		ReservationID:  offer.reservationID,
		Pinned:         offer.pinned,
		GrantedAt:      a.clock.Now(),
	}
	a.leases[lease.ID] = admissionLease{lease: lease, acceleratorSet: offer.acceleratorSet, priority: offer.job.Priority, state: domain.AdmissionLeaseActive}
	delete(a.offers, offerID)
	a.fence++
	if err := a.saveStateWithRollbackLocked(ctx, before); err != nil {
		return domain.Lease{}, err
	}
	return lease, nil
}

func (a *Admission) Release(ctx context.Context, leaseID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadStateLocked(ctx); err != nil {
		return err
	}
	return a.removeLeaseLocked(ctx, leaseID, "release")
}

func (a *Admission) Preempt(ctx context.Context, leaseID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.loadStateLocked(ctx); err != nil {
		return err
	}
	return fmt.Errorf("direct lease preemption is disabled; use policy-aware owner admission preemptions")
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
	if err := a.loadStateLocked(ctx); err != nil {
		return domain.Lease{}, false, err
	}
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
	if err := a.loadStateLocked(ctx); err != nil {
		return domain.Lease{}, false, err
	}
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
	if err := a.loadStateLocked(ctx); err != nil {
		return err
	}
	record, ok := a.leases[leaseID]
	if !ok {
		return fmt.Errorf("bind unknown lease %q", leaseID)
	}
	if record.lease.InstanceID != "" && record.lease.InstanceID != instanceID {
		return fmt.Errorf("lease %q is already bound to instance %q", leaseID, record.lease.InstanceID)
	}
	before := a.snapshotLocked()
	record.lease.InstanceID = instanceID
	a.leases[leaseID] = record
	return a.saveStateWithRollbackLocked(ctx, before)
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

func (a *Admission) selectAcceleratorSetLocked(req domain.AdmissionRequest, claim domain.Claim, excluding map[string]bool) ([]int, bool) {
	if req.InstanceID != "" {
		for _, inst := range a.instancesLocked() {
			if inst.ID == req.InstanceID {
				acceleratorSet := append([]int(nil), inst.AcceleratorSet...)
				if len(req.AcceleratorSet) > 0 && !sameAdmissionAcceleratorSet(req.AcceleratorSet, acceleratorSet) {
					return nil, false
				}
				return acceleratorSet, a.fitsLockedExcept(acceleratorSet, claim, excluding)
			}
		}
		return nil, false
	}
	if len(req.AcceleratorSet) > 0 {
		acceleratorSet := append([]int(nil), req.AcceleratorSet...)
		return acceleratorSet, a.fitsLockedExcept(acceleratorSet, claim, excluding)
	}
	if len(excluding) == 0 {
		return a.firstFitLocked(claim)
	}
	if a.node.Status != domain.NodeReady {
		return nil, false
	}
	for _, accelerator := range a.node.Accelerators {
		acceleratorSet := []int{accelerator.Index}
		if a.fitsLockedExcept(acceleratorSet, claim, excluding) {
			return acceleratorSet, true
		}
	}
	return nil, false
}

func sameAdmissionAcceleratorSet(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]int(nil), left...)
	right = append([]int(nil), right...)
	sort.Ints(left)
	sort.Ints(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (a *Admission) fitsLocked(acceleratorSet []int, claim domain.Claim) bool {
	instances := a.instancesLocked()
	return a.allocator.CanStackLoad(a.node, acceleratorSet, instances) && a.allocator.Fits(a.node, acceleratorSet, instances, claim)
}

func (a *Admission) fitsLockedExcept(acceleratorSet []int, claim domain.Claim, excluding map[string]bool) bool {
	if len(excluding) == 0 {
		return a.fitsLocked(acceleratorSet, claim)
	}
	instances := a.instancesLocked()
	filtered := instances[:0]
	for _, inst := range instances {
		if excluding[inst.ID] {
			continue
		}
		filtered = append(filtered, inst)
	}
	return a.allocator.CanStackLoad(a.node, acceleratorSet, filtered) && a.allocator.Fits(a.node, acceleratorSet, filtered, claim)
}

func (a *Admission) instancesLocked() []domain.ModelInstance {
	live := map[string]domain.ModelInstance{}
	if a.instances != nil {
		for _, inst := range a.instances() {
			if inst.ID != "" {
				live[inst.ID] = inst
			}
		}
	}

	bound := map[string]domain.ModelInstance{}
	for _, inst := range live {
		bound[inst.ID] = cloneInstance(inst)
	}
	for _, record := range a.leases {
		if admissionLeaseState(record) == domain.AdmissionLeasePreempting {
			continue
		}
		lease := record.lease
		if lease.InstanceID == "" {
			bound[lease.ID] = domain.ModelInstance{
				ID:             lease.ID,
				NodeID:         lease.NodeID,
				AcceleratorSet: append([]int(nil), record.acceleratorSet...),
				Claim:          lease.Claim,
				State:          domain.InstLoading,
				Priority:       record.priority,
				Loading:        true,
			}
			continue
		}
		inst := bound[lease.InstanceID]
		if inst.ID == "" {
			inst = domain.ModelInstance{
				ID:             lease.InstanceID,
				NodeID:         lease.NodeID,
				AcceleratorSet: append([]int(nil), record.acceleratorSet...),
				State:          domain.InstReady,
				Priority:       record.priority,
			}
		}
		if inst.Claim.WeightsMB == 0 && lease.Claim.WeightsMB > 0 {
			inst.Claim.WeightsMB = lease.Claim.WeightsMB
		}
		if lease.Claim.WeightsMB == 0 {
			inst.Claim.KVReservedMB += lease.Claim.KVReservedMB
		} else if inst.Claim.KVReservedMB == 0 {
			inst.Claim.KVReservedMB = lease.Claim.KVReservedMB
		}
		if priorityRank(record.priority) > priorityRank(inst.Priority) {
			inst.Priority = record.priority
		}
		bound[lease.InstanceID] = inst
	}

	instances := make([]domain.ModelInstance, 0, len(bound))
	for _, inst := range bound {
		instances = append(instances, inst)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	return instances
}

func cloneInstance(inst domain.ModelInstance) domain.ModelInstance {
	inst.AcceleratorSet = append([]int(nil), inst.AcceleratorSet...)
	return inst
}

func (a *Admission) removeLeaseLocked(ctx context.Context, leaseID, op string) error {
	if _, ok := a.leases[leaseID]; !ok {
		return fmt.Errorf("%s unknown lease %q", op, leaseID)
	}
	before := a.snapshotLocked()
	delete(a.leases, leaseID)
	a.fence++
	return a.saveStateWithRollbackLocked(ctx, before)
}

func (a *Admission) loadStateLocked(ctx context.Context) error {
	if a.loaded {
		return nil
	}
	if a.store == nil {
		a.loaded = true
		return nil
	}
	state, found, err := a.store.AdmissionState(ctx, a.node.ID)
	if err != nil {
		return err
	}
	if !found {
		if err := a.saveStateLocked(ctx); err != nil {
			return err
		}
		a.loaded = true
		return nil
	}
	if state.Fence == 0 {
		state.Fence = 1
	}
	a.fence = state.Fence
	a.nextOffer = state.NextOffer
	a.nextLease = state.NextLease
	a.offers = map[string]admissionOffer{}
	for _, rec := range state.Offers {
		a.offers[rec.Offer.OfferID] = admissionOffer{
			offer:          rec.Offer,
			job:            rec.Job,
			preset:         rec.Preset,
			acceleratorSet: append([]int(nil), rec.Offer.AcceleratorSet...),
			instanceID:     rec.Offer.InstanceID,
			reservationID:  rec.Offer.ReservationID,
			pinned:         a.pinnedReservations[rec.Offer.ReservationID],
			preemptions:    clonePreemptions(rec.Preemptions),
		}
	}
	a.leases = map[string]admissionLease{}
	for _, rec := range state.Leases {
		state := rec.State
		if state == "" {
			state = domain.AdmissionLeaseActive
		}
		a.leases[rec.Lease.ID] = admissionLease{
			lease:          rec.Lease,
			acceleratorSet: append([]int(nil), rec.Lease.AcceleratorSet...),
			priority:       rec.Lease.Priority,
			state:          state,
		}
	}
	a.loaded = true
	return nil
}

func (a *Admission) validatePreemptionsLocked(job domain.Job, targets []domain.PreemptionTarget) error {
	if len(targets) == 0 {
		return nil
	}
	if !admissionHardPreemptionAllowed(job) {
		return fmt.Errorf("job %q does not allow hard preemption", job.ID)
	}
	seen := map[string]bool{}
	for _, target := range targets {
		if target.LeaseID == "" {
			return fmt.Errorf("preemption target lease id is required")
		}
		if target.Reason == "" {
			return fmt.Errorf("preempt lease %q: reason is required", target.LeaseID)
		}
		if seen[target.LeaseID] {
			return fmt.Errorf("duplicate preemption target lease %q", target.LeaseID)
		}
		seen[target.LeaseID] = true
		record, ok := a.leases[target.LeaseID]
		if !ok {
			return fmt.Errorf("preempt unknown lease %q", target.LeaseID)
		}
		if record.lease.Pinned {
			return fmt.Errorf("lease %q is pinned and cannot be preempted", target.LeaseID)
		}
		if admissionLeaseState(record) == domain.AdmissionLeasePreempting {
			return fmt.Errorf("lease %q is already preempting", target.LeaseID)
		}
		if priorityRank(job.Priority) <= priorityRank(record.priority) {
			return fmt.Errorf("job %q priority %q cannot preempt lease %q priority %q", job.ID, job.Priority, target.LeaseID, record.priority)
		}
		if target.InstanceID != "" && record.lease.InstanceID != "" && record.lease.InstanceID != target.InstanceID {
			return fmt.Errorf("preemption target %q points at instance %q, not owner-bound instance %q", target.LeaseID, target.InstanceID, record.lease.InstanceID)
		}
	}
	return nil
}

func preemptionLeaseIDs(targets []domain.PreemptionTarget) map[string]bool {
	if len(targets) == 0 {
		return nil
	}
	out := make(map[string]bool, len(targets)*2)
	for _, target := range targets {
		out[target.LeaseID] = true
		if target.InstanceID != "" {
			out[target.InstanceID] = true
		}
	}
	return out
}

func clonePreemptions(targets []domain.PreemptionTarget) []domain.PreemptionTarget {
	if len(targets) == 0 {
		return nil
	}
	return append([]domain.PreemptionTarget(nil), targets...)
}

func (a *Admission) saveStateLocked(ctx context.Context) error {
	if a.store == nil {
		return nil
	}
	state := domain.AdmissionState{
		NodeID:    a.node.ID,
		Fence:     a.fence,
		NextOffer: a.nextOffer,
		NextLease: a.nextLease,
		Offers:    make([]domain.AdmissionOfferRecord, 0, len(a.offers)),
		Leases:    make([]domain.AdmissionLeaseRecord, 0, len(a.leases)),
	}
	for _, rec := range a.offers {
		state.Offers = append(state.Offers, domain.AdmissionOfferRecord{
			Offer:       rec.offer,
			Job:         rec.job,
			Preset:      rec.preset,
			Preemptions: clonePreemptions(rec.preemptions),
		})
	}
	for _, rec := range a.leases {
		state.Leases = append(state.Leases, domain.AdmissionLeaseRecord{Lease: rec.lease, State: admissionLeaseState(rec)})
	}
	return a.store.SaveAdmissionState(ctx, state)
}

func (a *Admission) saveStateWithRollbackLocked(ctx context.Context, before admissionSnapshot) error {
	if err := a.saveStateLocked(ctx); err != nil {
		a.restoreLocked(before)
		return err
	}
	return nil
}

func (a *Admission) snapshotLocked() admissionSnapshot {
	snap := admissionSnapshot{
		fence:     a.fence,
		nextOffer: a.nextOffer,
		nextLease: a.nextLease,
		offers:    make(map[string]admissionOffer, len(a.offers)),
		leases:    make(map[string]admissionLease, len(a.leases)),
	}
	for id, offer := range a.offers {
		snap.offers[id] = cloneAdmissionOffer(offer)
	}
	for id, lease := range a.leases {
		snap.leases[id] = cloneAdmissionLease(lease)
	}
	return snap
}

func (a *Admission) restoreLocked(snap admissionSnapshot) {
	a.fence = snap.fence
	a.nextOffer = snap.nextOffer
	a.nextLease = snap.nextLease
	a.offers = snap.offers
	a.leases = snap.leases
}

func cloneAdmissionOffer(offer admissionOffer) admissionOffer {
	offer.offer.AcceleratorSet = append([]int(nil), offer.offer.AcceleratorSet...)
	offer.acceleratorSet = append([]int(nil), offer.acceleratorSet...)
	offer.preemptions = clonePreemptions(offer.preemptions)
	return offer
}

func cloneAdmissionLease(lease admissionLease) admissionLease {
	lease.lease.AcceleratorSet = append([]int(nil), lease.lease.AcceleratorSet...)
	lease.acceleratorSet = append([]int(nil), lease.acceleratorSet...)
	return lease
}

func admissionLeaseState(lease admissionLease) domain.AdmissionLeaseState {
	if lease.state == "" {
		return domain.AdmissionLeaseActive
	}
	return lease.state
}

func incrementalClaim(req domain.AdmissionRequest) domain.Claim {
	claim := req.Claim
	if req.InstanceID != "" {
		claim.WeightsMB = 0
	}
	return claim
}

func (a *Admission) diskSafeLocked(preset domain.Preset, instanceID string, claim domain.Claim, jobID string) error {
	if instanceID != "" {
		return nil
	}
	if a.node.DiskTotalMB <= 0 {
		return nil
	}
	requiredMB, err := admissionArtifactRequired(preset, a.node, claim)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrNoFit, err)
	}
	minFreeRatio := a.node.DiskMinFreeRatio
	if minFreeRatio == 0 {
		minFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	if minFreeRatio <= 0 || minFreeRatio >= 1 {
		return fmt.Errorf("%w: node %q has invalid disk_min_free_ratio %.3f", domain.ErrNoFit, a.node.ID, minFreeRatio)
	}
	floorMB := int(math.Ceil(float64(a.node.DiskTotalMB) * minFreeRatio))
	if a.node.DiskFreeMB <= floorMB {
		return fmt.Errorf("%w: node %q free disk %dMB is at configured floor %dMB for job %q", domain.ErrNoFit, a.node.ID, a.node.DiskFreeMB, floorMB, jobID)
	}
	if a.node.DiskFreeMB-requiredMB <= floorMB {
		return fmt.Errorf("%w: node %q would cross disk floor %dMB by staging %dMB for job %q", domain.ErrNoFit, a.node.ID, floorMB, requiredMB, jobID)
	}
	return nil
}

func admissionArtifactRequired(preset domain.Preset, node domain.Node, claim domain.Claim) (int, error) {
	if preset.ID == "" {
		if claim.WeightsMB == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf("cold admission is missing preset artifact proof")
	}
	if preset.NodeID != "" && preset.NodeID == node.ID {
		return 0, nil
	}
	if preset.ArtifactSizeMB > 0 {
		return preset.ArtifactSizeMB, nil
	}
	if preset.ArtifactSizeMB < 0 {
		return 0, fmt.Errorf("preset %q has invalid artifact size %dMB", preset.ID, preset.ArtifactSizeMB)
	}
	if preset.EstWeightsMB > 0 {
		return preset.EstWeightsMB, nil
	}
	return 0, fmt.Errorf("preset %q has no artifact size proof", preset.ID)
}

func admissionHardPreemptionAllowed(job domain.Job) bool {
	switch job.Preemption {
	case domain.PreemptHard:
		return true
	case domain.PreemptHardForInteractive:
		return job.Priority == domain.PriorityInteractive
	default:
		return false
	}
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
