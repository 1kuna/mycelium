package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/trace"
)

const defaultMaxReplans = 2

type JobSource interface {
	Job(ctx context.Context, jobID string) (domain.Job, []byte, error)
}

type FleetSource interface {
	Snapshot(ctx context.Context) (domain.FleetSnapshot, error)
}

type AdmissionResolver interface {
	AdmissionController(nodeID string) (ports.AdmissionController, error)
}

type RegistryRecordPusher interface {
	PushRecord(ctx context.Context, rec domain.JobRecord) error
}

type Coordinator struct {
	selfID            string
	jobs              JobSource
	registry          ports.JobRegistry
	registryPusher    RegistryRecordPusher
	placer            ports.Placer
	fleet             FleetSource
	owners            AdmissionResolver
	clock             ports.Clock
	maxReplans        int
	privatePayloadKey []byte
	trace             *trace.Trace

	mu           sync.Mutex
	claimed      map[string]claimedJob
	leases       map[string]domain.Lease
	released     map[string]domain.Lease
	fences       map[string]uint64
	lastRecordAt time.Time
}

type claimedJob struct {
	job     domain.Job
	payload []byte
}

type CoordinatorOption func(*Coordinator)

func WithMaxReplans(n int) CoordinatorOption {
	return func(c *Coordinator) {
		c.maxReplans = n
	}
}

func WithPrivatePayloadKey(key []byte) CoordinatorOption {
	return func(c *Coordinator) {
		c.privatePayloadKey = append([]byte(nil), key...)
	}
}

func WithTrace(tr *trace.Trace) CoordinatorOption {
	return func(c *Coordinator) {
		c.trace = tr
	}
}

func WithRegistryPusher(pusher RegistryRecordPusher) CoordinatorOption {
	return func(c *Coordinator) {
		c.registryPusher = pusher
	}
}

func NewCoordinator(self domain.Peer, jobs JobSource, registry ports.JobRegistry, placer ports.Placer, fleet FleetSource, owners AdmissionResolver, clock ports.Clock, opts ...CoordinatorOption) *Coordinator {
	c := &Coordinator{
		selfID:     self.ID,
		jobs:       jobs,
		registry:   registry,
		placer:     placer,
		fleet:      fleet,
		owners:     owners,
		clock:      clock,
		maxReplans: defaultMaxReplans,
		claimed:    map[string]claimedJob{},
		leases:     map[string]domain.Lease{},
		released:   map[string]domain.Lease{},
		fences:     map[string]uint64{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Coordinator) ClaimJob(ctx context.Context, jobID string) error {
	if err := c.validate(); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("job id is required")
	}
	var job domain.Job
	var payload []byte
	if err := c.step("coordinator/job_source", map[string]any{"job_id": jobID}, func() error {
		var err error
		job, payload, err = c.jobs.Job(ctx, jobID)
		return err
	}); err != nil {
		return err
	}
	if job.ID != jobID {
		return fmt.Errorf("job source returned %q for requested %q", job.ID, jobID)
	}
	if len(payload) == 0 {
		return fmt.Errorf("job %q has no rescue payload", jobID)
	}
	c.mu.Lock()
	c.claimed[jobID] = claimedJob{job: job, payload: append([]byte(nil), payload...)}
	c.mu.Unlock()
	return c.recordStep(ctx, jobID, domain.JobPlacing, "", 0)
}

func (c *Coordinator) Plan(ctx context.Context, jobID string) (domain.PlacementDecision, error) {
	if err := c.validate(); err != nil {
		return domain.PlacementDecision{}, err
	}
	claimed, err := c.claimedJob(jobID)
	if err != nil {
		return domain.PlacementDecision{}, err
	}
	var fleet domain.FleetSnapshot
	if err := c.step("coordinator/fleet_snapshot", map[string]any{"job_id": jobID}, func() error {
		var err error
		fleet, err = c.fleet.Snapshot(ctx)
		return err
	}); err != nil {
		return domain.PlacementDecision{}, err
	}
	var decision domain.PlacementDecision
	if err := c.step("coordinator/place", map[string]any{"job_id": jobID}, func() error {
		var err error
		decision, err = c.placer.Place(ctx, claimed.job, fleet)
		return err
	}); err != nil {
		_ = c.recordStep(ctx, jobID, domain.JobFailed, "", 0)
		return decision, err
	}
	status := domain.JobPlacing
	if decision.Action == domain.ActionQueued {
		status = domain.JobQueued
	}
	if err := c.recordStep(ctx, jobID, status, decision.NodeID, 0); err != nil {
		return domain.PlacementDecision{}, err
	}
	return decision, nil
}

func (c *Coordinator) Commit(ctx context.Context, plan domain.PlacementDecision) (domain.Lease, error) {
	if err := c.validate(); err != nil {
		return domain.Lease{}, err
	}
	if plan.JobID == "" {
		return domain.Lease{}, fmt.Errorf("plan job id is required")
	}
	claimed, err := c.claimedJob(plan.JobID)
	if err != nil {
		return domain.Lease{}, err
	}
	replans := 0
	for {
		if plan.Action == domain.ActionQueued {
			return domain.Lease{}, c.recordStep(ctx, plan.JobID, domain.JobQueued, "", 0)
		}
		if plan.NodeID == "" {
			return domain.Lease{}, fmt.Errorf("plan for job %q has no owner node", plan.JobID)
		}
		var owner ports.AdmissionController
		if err := c.step("coordinator/resolve_owner", map[string]any{"job_id": plan.JobID, "node_id": plan.NodeID}, func() error {
			var err error
			owner, err = c.owners.AdmissionController(plan.NodeID)
			return err
		}); err != nil {
			_ = c.recordStep(ctx, plan.JobID, domain.JobQueued, "", 0)
			return domain.Lease{}, err
		}
		var offer domain.LeaseOffer
		if err := c.step("coordinator/owner_offer", map[string]any{"job_id": plan.JobID, "node_id": plan.NodeID, "instance_id": plan.InstanceID}, func() error {
			var err error
			var preemptions []domain.PreemptionTarget
			preemptions, err = c.admissionPreemptions(ctx, claimed.job, plan, owner)
			if err != nil {
				return err
			}
			offer, err = owner.Offer(ctx, domain.AdmissionRequest{
				Job:            claimed.job,
				Preset:         plan.Preset,
				Claim:          plan.Claim,
				NodeID:         plan.NodeID,
				AcceleratorSet: append([]int(nil), plan.AcceleratorSet...),
				InstanceID:     plan.InstanceID,
				Preemptions:    preemptions,
			})
			return err
		}); err != nil {
			if c.shouldReplan(err, replans) {
				replans++
				plan, err = c.Plan(ctx, plan.JobID)
				if err != nil {
					return domain.Lease{}, err
				}
				continue
			}
			_ = c.recordStep(ctx, plan.JobID, domain.JobFailed, plan.NodeID, 0)
			return domain.Lease{}, err
		}
		var lease domain.Lease
		if err := c.step("coordinator/owner_commit", map[string]any{"job_id": plan.JobID, "offer_id": offer.OfferID, "fence": offer.Fence}, func() error {
			var err error
			lease, err = owner.Commit(ctx, offer.OfferID, offer.Fence)
			return err
		}); err != nil {
			if c.shouldReplan(err, replans) {
				replans++
				plan, err = c.Plan(ctx, plan.JobID)
				if err != nil {
					return domain.Lease{}, err
				}
				continue
			}
			if replanable(err) {
				_ = c.recordStep(ctx, plan.JobID, domain.JobQueued, "", offer.Fence)
			} else {
				_ = c.recordStep(ctx, plan.JobID, domain.JobFailed, plan.NodeID, offer.Fence)
			}
			return domain.Lease{}, err
		}
		c.mu.Lock()
		c.leases[plan.JobID] = lease
		c.fences[plan.JobID] = offer.Fence
		delete(c.released, plan.JobID)
		c.mu.Unlock()
		status := domain.JobRunning
		if plan.InstanceID == "" {
			status = domain.JobLoading
		}
		if err := c.recordStep(ctx, plan.JobID, status, plan.NodeID, offer.Fence); err != nil {
			return domain.Lease{}, err
		}
		return lease, nil
	}
}

func (c *Coordinator) MarkRunning(ctx context.Context, jobID string) error {
	if err := c.validate(); err != nil {
		return err
	}
	lease, err := c.statusLease(jobID)
	if err != nil {
		return err
	}
	return c.recordStep(ctx, jobID, domain.JobRunning, lease.NodeID, c.statusFence(jobID))
}

func (c *Coordinator) Release(ctx context.Context, jobID string) error {
	if err := c.validate(); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("job id is required")
	}
	lease, err := c.lease(jobID)
	if err != nil {
		return err
	}
	if lease.ID != "" {
		var owner ports.AdmissionController
		if err := c.step("coordinator/resolve_owner", map[string]any{"job_id": jobID, "node_id": lease.NodeID}, func() error {
			var err error
			owner, err = c.owners.AdmissionController(lease.NodeID)
			return err
		}); err != nil {
			return withCleanupEvidence(err, c.recordCleanupFailure(ctx, jobID, lease, err))
		}
		if err := c.step("coordinator/owner_release", map[string]any{"job_id": jobID, "lease_id": lease.ID}, func() error {
			return owner.Release(ctx, lease.ID)
		}); err != nil {
			return withCleanupEvidence(err, c.recordCleanupFailure(ctx, jobID, lease, err))
		}
	}
	c.mu.Lock()
	delete(c.leases, jobID)
	c.released[jobID] = lease
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Complete(ctx context.Context, jobID string) error {
	if err := c.validate(); err != nil {
		return err
	}
	lease, err := c.statusLease(jobID)
	if err != nil {
		return err
	}
	return c.recordStep(ctx, jobID, domain.JobDone, lease.NodeID, c.statusFence(jobID))
}

func (c *Coordinator) Fail(ctx context.Context, jobID string, cause error) error {
	if err := c.validate(); err != nil {
		return err
	}
	lease, err := c.statusLease(jobID)
	if err != nil {
		return err
	}
	note := ""
	if cause != nil {
		note = cause.Error()
	}
	return c.recordWithNote(ctx, jobID, domain.JobFailed, lease.NodeID, c.statusFence(jobID), note)
}

func (c *Coordinator) validate() error {
	if c.selfID == "" || c.jobs == nil || c.registry == nil || c.placer == nil || c.fleet == nil || c.owners == nil || c.clock == nil {
		return fmt.Errorf("peer coordinator is not fully configured")
	}
	if c.maxReplans < 0 {
		return fmt.Errorf("max replans must be non-negative")
	}
	if c.claimed == nil {
		c.claimed = map[string]claimedJob{}
	}
	if c.leases == nil {
		c.leases = map[string]domain.Lease{}
	}
	if c.released == nil {
		c.released = map[string]domain.Lease{}
	}
	if c.fences == nil {
		c.fences = map[string]uint64{}
	}
	return nil
}

func (c *Coordinator) claimedJob(jobID string) (claimedJob, error) {
	if jobID == "" {
		return claimedJob{}, fmt.Errorf("job id is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	claimed, ok := c.claimed[jobID]
	if !ok {
		return claimedJob{}, fmt.Errorf("job %q is not claimed by this coordinator", jobID)
	}
	claimed.payload = append([]byte(nil), claimed.payload...)
	return claimed, nil
}

func (c *Coordinator) lease(jobID string) (domain.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.leases[jobID]
	if !ok {
		return domain.Lease{}, fmt.Errorf("job %q has no committed lease", jobID)
	}
	return lease, nil
}

func (c *Coordinator) statusLease(jobID string) (domain.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lease, ok := c.leases[jobID]; ok {
		return lease, nil
	}
	if lease, ok := c.released[jobID]; ok {
		return lease, nil
	}
	if _, ok := c.claimed[jobID]; ok {
		return domain.Lease{JobID: jobID}, nil
	}
	return domain.Lease{}, fmt.Errorf("job %q is not claimed by this coordinator", jobID)
}

func (c *Coordinator) statusFence(jobID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fences[jobID]
}

func (c *Coordinator) record(ctx context.Context, jobID string, status domain.JobStatus, assignedNode string, fence uint64) error {
	return c.recordWithNote(ctx, jobID, status, assignedNode, fence, "")
}

func (c *Coordinator) recordWithNote(ctx context.Context, jobID string, status domain.JobStatus, assignedNode string, fence uint64, note string) error {
	return c.recordWithDetails(ctx, jobID, status, assignedNode, fence, note, false, "")
}

func (c *Coordinator) recordWithDetails(ctx context.Context, jobID string, status domain.JobStatus, assignedNode string, fence uint64, note string, cleanupRequired bool, cleanupError string) error {
	claimed, err := c.claimedJob(jobID)
	if err != nil {
		return err
	}
	request, err := EncodeRescuePayloadWithKey(claimed.job, claimed.payload, c.privatePayloadKey)
	if err != nil {
		return err
	}
	rec := domain.JobRecord{
		JobID:           jobID,
		Coordinator:     c.selfID,
		AssignedNode:    assignedNode,
		Status:          status,
		Request:         request,
		Handling:        claimed.job.Handling,
		RecoveryNote:    note,
		CleanupRequired: cleanupRequired,
		CleanupError:    cleanupError,
		Fence:           fence,
		UpdatedAt:       c.nextRecordTime(),
	}
	if err := c.registry.Put(ctx, rec); err != nil {
		return err
	}
	if c.registryPusher == nil || !requiresRescueCopy(status) {
		return nil
	}
	if err := c.registryPusher.PushRecord(ctx, rec); err != nil {
		degraded := rec
		degraded.RecoveryNote = appendRecoveryNote(note, fmt.Sprintf("registry rescue replication failed: %v", err))
		degraded.UpdatedAt = c.nextRecordTime()
		if evidenceErr := c.registry.Put(ctx, degraded); evidenceErr != nil {
			return errors.Join(fmt.Errorf("registry rescue replication for job %q: %w", jobID, err), fmt.Errorf("record degraded registry evidence: %w", evidenceErr))
		}
		return nil
	}
	return nil
}

func (c *Coordinator) recordStep(ctx context.Context, jobID string, status domain.JobStatus, assignedNode string, fence uint64) error {
	return c.step("coordinator/record", map[string]any{"job_id": jobID, "status": string(status), "node_id": assignedNode, "fence": fence}, func() error {
		return c.record(ctx, jobID, status, assignedNode, fence)
	})
}

func (c *Coordinator) nextRecordTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now().UTC()
	if !now.After(c.lastRecordAt) {
		now = c.lastRecordAt.Add(time.Nanosecond)
	}
	c.lastRecordAt = now
	return now
}

func (c *Coordinator) shouldReplan(err error, replans int) bool {
	if !replanable(err) {
		return false
	}
	if replans >= c.maxReplans {
		return false
	}
	return true
}

func (c *Coordinator) step(op string, input map[string]any, fn func() error) error {
	if c.trace == nil {
		return fn()
	}
	return c.trace.Do(op, input, fn)
}

func requiresRescueCopy(status domain.JobStatus) bool {
	switch status {
	case domain.JobQueued, domain.JobPlacing, domain.JobLoading, domain.JobRunning:
		return true
	default:
		return false
	}
}

func appendRecoveryNote(existing, next string) string {
	if existing == "" {
		return next
	}
	return existing + "; " + next
}

func withCleanupEvidence(err, evidenceErr error) error {
	if evidenceErr != nil {
		return errors.Join(err, evidenceErr)
	}
	return err
}

func (c *Coordinator) recordCleanupFailure(ctx context.Context, jobID string, lease domain.Lease, cause error) error {
	records, err := c.registry.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("record terminal cleanup evidence for job %q: %w", jobID, err)
	}
	status := domain.JobFailed
	assignedNode := lease.NodeID
	fence := c.statusFence(jobID)
	note := fmt.Sprintf("terminal cleanup failed: %v", cause)
	for _, rec := range records {
		if rec.JobID != jobID {
			continue
		}
		status = rec.Status
		if rec.AssignedNode != "" {
			assignedNode = rec.AssignedNode
		}
		if rec.Fence > fence {
			fence = rec.Fence
		}
		note = appendRecoveryNote(rec.RecoveryNote, note)
		break
	}
	fence++
	return c.recordWithDetails(ctx, jobID, status, assignedNode, fence, note, true, cause.Error())
}

func (c *Coordinator) admissionPreemptions(ctx context.Context, job domain.Job, plan domain.PlacementDecision, owner ports.AdmissionController) ([]domain.PreemptionTarget, error) {
	if len(plan.Preempted) == 0 {
		return nil, nil
	}
	inspector, ok := owner.(ports.LeaseInspector)
	if !ok {
		return nil, fmt.Errorf("owner admission for node %q does not expose lease inspection", plan.NodeID)
	}
	targets := make([]domain.PreemptionTarget, 0, len(plan.Preempted))
	for _, instanceID := range plan.Preempted {
		lease, found, err := inspector.LeaseForInstance(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		if !found || lease.ID == "" {
			return nil, fmt.Errorf("preempted instance %q has no owner lease on node %q", instanceID, plan.NodeID)
		}
		reason := "preempted"
		if plan.JobID != "" {
			reason += " for " + plan.JobID
		} else if job.ID != "" {
			reason += " for " + job.ID
		}
		targets = append(targets, domain.PreemptionTarget{LeaseID: lease.ID, InstanceID: instanceID, Reason: reason})
	}
	return targets, nil
}

func replanable(err error) bool {
	return errors.Is(err, domain.ErrStaleFence) || errors.Is(err, domain.ErrNoFit)
}

var _ ports.Coordinator = (*Coordinator)(nil)
