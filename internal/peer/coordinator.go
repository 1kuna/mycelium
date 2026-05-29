package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
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

type Coordinator struct {
	selfID     string
	jobs       JobSource
	registry   ports.JobRegistry
	placer     ports.Placer
	fleet      FleetSource
	owners     AdmissionResolver
	clock      ports.Clock
	maxReplans int

	mu           sync.Mutex
	claimed      map[string]claimedJob
	leases       map[string]domain.Lease
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
	job, payload, err := c.jobs.Job(ctx, jobID)
	if err != nil {
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
	return c.record(ctx, jobID, domain.JobPlacing, "", 0)
}

func (c *Coordinator) Plan(ctx context.Context, jobID string) (domain.PlacementDecision, error) {
	if err := c.validate(); err != nil {
		return domain.PlacementDecision{}, err
	}
	claimed, err := c.claimedJob(jobID)
	if err != nil {
		return domain.PlacementDecision{}, err
	}
	fleet, err := c.fleet.Snapshot(ctx)
	if err != nil {
		return domain.PlacementDecision{}, err
	}
	decision, err := c.placer.Place(ctx, claimed.job, fleet)
	if err != nil {
		_ = c.record(ctx, jobID, domain.JobFailed, "", 0)
		return decision, err
	}
	status := domain.JobPlacing
	if decision.Action == domain.ActionQueued {
		status = domain.JobQueued
	}
	if err := c.record(ctx, jobID, status, decision.NodeID, 0); err != nil {
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
			return domain.Lease{}, c.record(ctx, plan.JobID, domain.JobQueued, "", 0)
		}
		if plan.NodeID == "" {
			return domain.Lease{}, fmt.Errorf("plan for job %q has no owner node", plan.JobID)
		}
		if plan.InstanceID != "" || plan.Action == domain.ActionWarmInstance {
			lease := domain.Lease{JobID: plan.JobID, InstanceID: plan.InstanceID, NodeID: plan.NodeID, Claim: plan.Claim}
			c.mu.Lock()
			c.leases[plan.JobID] = lease
			c.mu.Unlock()
			if err := c.record(ctx, plan.JobID, domain.JobRunning, plan.NodeID, 0); err != nil {
				return domain.Lease{}, err
			}
			return lease, nil
		}
		owner, err := c.owners.AdmissionController(plan.NodeID)
		if err != nil {
			_ = c.record(ctx, plan.JobID, domain.JobQueued, "", 0)
			return domain.Lease{}, err
		}
		offer, err := owner.Offer(ctx, claimed.job, plan.Claim)
		if err != nil {
			if c.shouldReplan(err, replans) {
				replans++
				plan, err = c.Plan(ctx, plan.JobID)
				if err != nil {
					return domain.Lease{}, err
				}
				continue
			}
			_ = c.record(ctx, plan.JobID, domain.JobFailed, plan.NodeID, 0)
			return domain.Lease{}, err
		}
		lease, err := owner.Commit(ctx, offer.OfferID, offer.Fence)
		if err != nil {
			if c.shouldReplan(err, replans) {
				replans++
				plan, err = c.Plan(ctx, plan.JobID)
				if err != nil {
					return domain.Lease{}, err
				}
				continue
			}
			if replanable(err) {
				_ = c.record(ctx, plan.JobID, domain.JobQueued, "", offer.Fence)
			} else {
				_ = c.record(ctx, plan.JobID, domain.JobFailed, plan.NodeID, offer.Fence)
			}
			return domain.Lease{}, err
		}
		c.mu.Lock()
		c.leases[plan.JobID] = lease
		c.mu.Unlock()
		if err := c.record(ctx, plan.JobID, domain.JobRunning, plan.NodeID, offer.Fence); err != nil {
			return domain.Lease{}, err
		}
		return lease, nil
	}
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
		owner, err := c.owners.AdmissionController(lease.NodeID)
		if err != nil {
			return err
		}
		if err := owner.Release(ctx, lease.ID); err != nil {
			return err
		}
	}
	c.mu.Lock()
	delete(c.leases, jobID)
	c.mu.Unlock()
	return c.record(ctx, jobID, domain.JobDone, lease.NodeID, 0)
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

func (c *Coordinator) record(ctx context.Context, jobID string, status domain.JobStatus, assignedNode string, fence uint64) error {
	claimed, err := c.claimedJob(jobID)
	if err != nil {
		return err
	}
	request, err := EncodeRescuePayload(claimed.job, claimed.payload)
	if err != nil {
		return err
	}
	return c.registry.Put(ctx, domain.JobRecord{
		JobID:        jobID,
		Coordinator:  c.selfID,
		AssignedNode: assignedNode,
		Status:       status,
		Request:      request,
		Fence:        fence,
		UpdatedAt:    c.nextRecordTime(),
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

func replanable(err error) bool {
	return errors.Is(err, domain.ErrStaleFence) || errors.Is(err, domain.ErrNoFit)
}

var _ ports.Coordinator = (*Coordinator)(nil)
