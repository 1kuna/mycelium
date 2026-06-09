package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type FleetSource interface {
	Snapshot(ctx context.Context) (domain.FleetSnapshot, error)
}

type NodeResolver interface {
	NodeAgent(nodeID string) (ports.NodeAgent, error)
}

type AdmissionResolver interface {
	AdmissionController(nodeID string) (ports.AdmissionController, error)
}

type RuntimeStore interface {
	SaveJob(ctx context.Context, job domain.Job) error
	SaveLease(ctx context.Context, lease domain.Lease) error
	ListLeases(ctx context.Context) ([]domain.Lease, error)
	DeleteLease(ctx context.Context, id string) error
	SaveInstance(ctx context.Context, inst domain.ModelInstance) error
	DeleteInstance(ctx context.Context, id string) error
}

type JobReader interface {
	Job(ctx context.Context, id string) (domain.Job, error)
}

type JobLog interface {
	PutJob(ctx context.Context, job domain.Job, payload []byte) error
}

type JobPayloadReader interface {
	Job(ctx context.Context, jobID string) (domain.Job, []byte, error)
}

type Service struct {
	Placer      ports.Placer
	Fleet       FleetSource
	Nodes       NodeResolver
	Owners      AdmissionResolver
	Coordinator ports.Coordinator
	JobLog      JobLog
	Queue       *Queue
	Store       RuntimeStore
	Clock       ports.Clock
	Presets     map[string]domain.Preset
}

type Result struct {
	Decision domain.PlacementDecision
	Instance domain.ModelInstance
	Lease    domain.Lease
}

type SubmitHooks struct {
	BeforeColdLoad func(ctx context.Context, decision domain.PlacementDecision) error
}

func (s *Service) Submit(ctx context.Context, job domain.Job, hooks ...SubmitHooks) (Result, error) {
	if s.Coordinator != nil {
		return Result{}, fmt.Errorf("coordinated scheduler service requires SubmitWithPayload")
	}
	return s.submitLocal(ctx, job, hooks...)
}

func (s *Service) SubmitWithPayload(ctx context.Context, job domain.Job, payload []byte, hooks ...SubmitHooks) (Result, error) {
	if s.Coordinator != nil {
		return s.submitCoordinated(ctx, job, payload, hooks...)
	}
	return s.submitLocal(ctx, job, hooks...)
}

func (s *Service) submitLocal(ctx context.Context, job domain.Job, hooks ...SubmitHooks) (Result, error) {
	if err := s.validate(); err != nil {
		return Result{}, err
	}
	if job.ID == "" {
		return Result{}, fmt.Errorf("job id is required")
	}
	job.Status = domain.JobPlacing
	if err := s.Store.SaveJob(ctx, job); err != nil {
		return Result{}, err
	}

	fleet, err := s.Fleet.Snapshot(ctx)
	if err != nil {
		return Result{}, err
	}
	decision, err := s.Placer.Place(ctx, job, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	if decision.Action == domain.ActionQueued {
		job.Status = domain.JobQueued
		s.Queue.Enqueue(job)
		if err := s.Store.SaveJob(ctx, job); err != nil {
			return Result{Decision: decision}, err
		}
		return Result{Decision: decision}, nil
	}

	preemptedLeases, preemptedVictims, err := s.inspectPreemptions(ctx, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	ownerLease, owner, err := s.commitOwnerAdmission(ctx, job, decision, preemptedLeases)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	releaseOwner := func() error {
		if ownerLease.ID == "" {
			return nil
		}
		if err := owner.Release(cleanupContext(ctx), ownerLease.ID); err != nil {
			return err
		}
		ownerLease = domain.Lease{}
		return nil
	}
	if err := s.finishPreemption(ctx, decision, fleet, preemptedLeases, preemptedVictims); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := releaseOwner(); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	if decision.InstanceID == "" {
		if err := runBeforeColdLoadHook(ctx, decision, hooks); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			if releaseErr := releaseOwner(); releaseErr != nil {
				return Result{Decision: decision}, releaseErr
			}
			return Result{Decision: decision}, err
		}
	}
	inst, err := s.resolveInstance(ctx, job, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := releaseOwner(); releaseErr != nil {
			return Result{Decision: decision}, releaseErr
		}
		return Result{Decision: decision}, err
	}
	lease := s.finalizeOwnerLease(job, inst, decision, ownerLease)
	if ownerLease.ID != "" {
		if err := s.ensureOwnerLeaseBound(ctx, ownerLease, lease); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			if releaseErr := releaseOwner(); releaseErr != nil {
				return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, releaseErr)
			}
			return Result{Decision: decision, Instance: inst, Lease: lease}, err
		}
	}
	job.Status = domain.JobRunning
	if err := s.Store.SaveInstance(ctx, inst); err != nil {
		if cleanupErr := s.cleanupLoadedInstance(ctx, inst, decision.InstanceID == "", releaseOwner); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveLease(ctx, lease); err != nil {
		if cleanupErr := s.cleanupLoadedInstance(ctx, inst, decision.InstanceID == "", releaseOwner); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
		if cleanupErr := s.cleanupLoadedInstance(ctx, inst, decision.InstanceID == "", releaseOwner); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	return Result{Decision: decision, Instance: inst, Lease: lease}, nil
}

func (s *Service) submitCoordinated(ctx context.Context, job domain.Job, payload []byte, hooks ...SubmitHooks) (Result, error) {
	if err := s.validate(); err != nil {
		return Result{}, err
	}
	if s.JobLog == nil {
		return Result{}, fmt.Errorf("coordinated scheduler service is missing a job log")
	}
	if job.ID == "" {
		return Result{}, fmt.Errorf("job id is required")
	}
	job.Status = domain.JobPlacing
	if err := s.Store.SaveJob(ctx, job); err != nil {
		return Result{}, err
	}
	if err := s.JobLog.PutJob(ctx, job, payload); err != nil {
		return Result{}, err
	}
	if err := s.Coordinator.ClaimJob(ctx, job.ID); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{}, err
	}
	decision, err := s.Coordinator.Plan(ctx, job.ID)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	if decision.Action == domain.ActionQueued {
		if _, err := s.Coordinator.Commit(ctx, decision); err != nil {
			return Result{Decision: decision}, err
		}
		job.Status = domain.JobQueued
		s.Queue.EnqueueWithPayload(job, payload)
		if err := s.Store.SaveJob(ctx, job); err != nil {
			return Result{Decision: decision}, err
		}
		return Result{Decision: decision}, nil
	}

	outcome, err := s.Coordinator.Commit(ctx, decision)
	if err != nil {
		if coordinatedQueueErr(err) {
			job.Status = domain.JobQueued
			s.Queue.EnqueueWithPayload(job, payload)
			if saveErr := s.Store.SaveJob(ctx, job); saveErr != nil {
				return Result{Decision: decision}, errors.Join(err, saveErr)
			}
			return Result{Decision: decision}, err
		}
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	decision = outcome.Decision
	ownerLease := outcome.Lease
	if decision.Action == domain.ActionQueued {
		job.Status = domain.JobQueued
		s.Queue.EnqueueWithPayload(job, payload)
		if err := s.Store.SaveJob(ctx, job); err != nil {
			return Result{Decision: decision}, err
		}
		return Result{Decision: decision, Lease: ownerLease}, nil
	}
	if decision.InstanceID == "" && ownerLease.ID == "" {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, fmt.Errorf("coordinated cold load for job %q returned no owner lease", job.ID)
	}
	decision = decisionWithOwnerLease(decision, ownerLease)
	fleet, err := s.Fleet.Snapshot(ctx)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := s.Coordinator.Release(cleanupContext(ctx), job.ID); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	preemptedLeases, preemptedVictims, err := s.inspectPreemptions(ctx, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := s.Coordinator.Release(cleanupContext(ctx), job.ID); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	if err := s.finishPreemption(ctx, decision, fleet, preemptedLeases, preemptedVictims); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := s.Coordinator.Release(cleanupContext(ctx), job.ID); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	if decision.InstanceID == "" {
		if err := runBeforeColdLoadHook(ctx, decision, hooks); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			if releaseErr := s.Coordinator.Release(cleanupContext(ctx), job.ID); releaseErr != nil {
				return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
			}
			return Result{Decision: decision, Lease: ownerLease}, err
		}
	}
	inst, err := s.resolveInstance(ctx, job, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := s.Coordinator.Release(cleanupContext(ctx), job.ID); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	lease := s.finalizeOwnerLease(job, inst, decision, ownerLease)
	if ownerLease.ID != "" {
		if err := s.ensureOwnerLeaseBound(ctx, ownerLease, lease); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			cleanupErr := s.Coordinator.Release(ctx, job.ID)
			if decision.InstanceID == "" {
				cleanupErr = s.cleanupCoordinatedLoad(ctx, job.ID, inst)
			}
			if cleanupErr != nil {
				return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
			}
			return Result{Decision: decision, Instance: inst, Lease: lease}, err
		}
	}
	job.Status = domain.JobRunning
	if err := s.Store.SaveInstance(ctx, inst); err != nil {
		if cleanupErr := s.cleanupCoordinatedLoad(ctx, job.ID, inst); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveLease(ctx, lease); err != nil {
		if cleanupErr := s.cleanupCoordinatedLoad(ctx, job.ID, inst); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
		if cleanupErr := s.cleanupCoordinatedLoad(ctx, job.ID, inst); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Coordinator.MarkRunning(ctx, job.ID); err != nil {
		if cleanupErr := s.cleanupCoordinatedLoad(ctx, job.ID, inst); cleanupErr != nil {
			return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
		}
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	return Result{Decision: decision, Instance: inst, Lease: lease}, nil
}

func (s *Service) Drain(ctx context.Context, limit int) ([]Result, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	results := make([]Result, 0, limit)
	for len(results) < limit {
		job, payload, ok := s.Queue.DequeueWithPayload()
		if !ok {
			return results, nil
		}
		var result Result
		var err error
		if s.Coordinator != nil {
			if len(payload) == 0 {
				return results, fmt.Errorf("queued job %q has no rescue payload for coordinated drain", job.ID)
			}
			result, err = s.SubmitWithPayload(ctx, job, payload)
		} else {
			result, err = s.Submit(ctx, job)
		}
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *Service) Release(ctx context.Context, leaseID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if leaseID == "" {
		return fmt.Errorf("lease id is required")
	}
	leases, err := s.Store.ListLeases(ctx)
	if err != nil {
		return err
	}
	for _, lease := range leases {
		if lease.ID == leaseID {
			return s.ReleaseJob(ctx, lease)
		}
	}
	return s.Store.DeleteLease(ctx, leaseID)
}

func (s *Service) ReleaseJob(ctx context.Context, lease domain.Lease) error {
	if err := s.validate(); err != nil {
		return err
	}
	if lease.JobID == "" {
		if lease.ID == "" {
			return nil
		}
		if s.Owners != nil && lease.NodeID != "" {
			return s.releaseOwnerLease(ctx, lease)
		}
		return s.Store.DeleteLease(ctx, lease.ID)
	}
	if s.Coordinator != nil {
		if err := s.Coordinator.Release(ctx, lease.JobID); err != nil {
			if lease.ID != "" && s.Owners != nil && releaseErrAllowsOwnerFallback(err) {
				return s.releaseOwnerLease(ctx, lease)
			}
			return err
		}
	} else if s.Owners != nil && lease.NodeID != "" && lease.ID != "" {
		return s.releaseOwnerLease(ctx, lease)
	}
	if lease.ID == "" {
		return nil
	}
	return s.Store.DeleteLease(ctx, lease.ID)
}

func (s *Service) CompleteJob(ctx context.Context, job domain.Job, lease domain.Lease) error {
	if err := s.validate(); err != nil {
		return err
	}
	job.Status = domain.JobDone
	job.Error = ""
	if s.Coordinator != nil && lease.JobID != "" {
		if err := s.Coordinator.Complete(ctx, lease.JobID); err != nil {
			return err
		}
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
		return err
	}
	return nil
}

func (s *Service) FailJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error {
	if err := s.validate(); err != nil {
		return err
	}
	job.Status = domain.JobFailed
	if cause != nil {
		job.Error = cause.Error()
	}
	if s.Coordinator != nil && lease.JobID != "" {
		if err := s.Coordinator.Fail(ctx, lease.JobID, cause); err != nil {
			return err
		}
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
		return err
	}
	return nil
}

func (s *Service) FinishJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error {
	cleanupCtx := cleanupContext(ctx)
	if s.Coordinator != nil && lease.JobID != "" {
		if err := s.validate(); err != nil {
			return err
		}
		if cause == nil {
			job.Status = domain.JobDone
			job.Error = ""
			if err := s.Coordinator.Complete(cleanupCtx, lease.JobID); err != nil {
				return err
			}
		} else {
			job.Status = domain.JobFailed
			job.Error = cause.Error()
			if err := s.Coordinator.Fail(cleanupCtx, lease.JobID, cause); err != nil {
				return err
			}
		}
		saveErr := s.Store.SaveJob(cleanupCtx, job)
		releaseErr := s.ReleaseJob(cleanupCtx, lease)
		return errors.Join(saveErr, releaseErr)
	}
	var terminalErr error
	if cause == nil {
		terminalErr = s.CompleteJob(cleanupCtx, job, lease)
	} else {
		terminalErr = s.FailJob(cleanupCtx, job, lease, cause)
	}
	if terminalErr != nil {
		return terminalErr
	}
	return s.ReleaseJob(cleanupCtx, lease)
}

func (s *Service) releaseOwnerLease(ctx context.Context, lease domain.Lease) error {
	owner, err := s.Owners.AdmissionController(lease.NodeID)
	if err != nil {
		return err
	}
	if err := owner.Release(ctx, lease.ID); err != nil {
		return err
	}
	return s.Store.DeleteLease(ctx, lease.ID)
}

func releaseErrAllowsOwnerFallback(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not claimed by this coordinator") || strings.Contains(msg, "has no committed lease")
}

func (s *Service) ExpireLeases(ctx context.Context) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	leases, err := s.Store.ListLeases(ctx)
	if err != nil {
		return 0, err
	}
	now := s.Clock.Now()
	expired := 0
	for _, lease := range leases {
		if lease.ExpiresAt.IsZero() || lease.ExpiresAt.After(now) {
			continue
		}
		if err := s.ReleaseJob(ctx, lease); err != nil {
			return expired, err
		}
		expired++
	}
	return expired, nil
}

func (s *Service) validate() error {
	if s.Placer == nil || s.Fleet == nil || s.Nodes == nil || s.Queue == nil || s.Store == nil || s.Clock == nil {
		return fmt.Errorf("scheduler service is not fully configured")
	}
	return nil
}

func (s *Service) commitOwnerAdmission(ctx context.Context, job domain.Job, decision domain.PlacementDecision, preemptedLeaseArgs ...map[string]domain.Lease) (domain.Lease, ports.AdmissionController, error) {
	preemptedLeases := map[string]domain.Lease{}
	if len(preemptedLeaseArgs) > 0 && preemptedLeaseArgs[0] != nil {
		preemptedLeases = preemptedLeaseArgs[0]
	}
	if decision.Action == domain.ActionQueued {
		return domain.Lease{}, nil, nil
	}
	if decision.NodeID == "" {
		return domain.Lease{}, nil, fmt.Errorf("placement action %q did not select an owner for admission", decision.Action)
	}
	if s.Owners == nil {
		return domain.Lease{}, nil, fmt.Errorf("owner admission resolver is not configured")
	}
	owner, err := s.Owners.AdmissionController(decision.NodeID)
	if err != nil {
		return domain.Lease{}, nil, err
	}
	preset := decision.Preset
	if preset.ID == "" {
		if resolved, resolveErr := s.resolvePreset(job); resolveErr == nil {
			preset = resolved
		}
	}
	offer, err := owner.Offer(ctx, domain.AdmissionRequest{
		Job:            job,
		Preset:         preset,
		Claim:          decision.Claim,
		NodeID:         decision.NodeID,
		AcceleratorSet: append([]int(nil), decision.AcceleratorSet...),
		InstanceID:     decision.InstanceID,
		Preemptions:    admissionPreemptions(job, decision, preemptedLeases),
	})
	if err != nil {
		return domain.Lease{}, nil, err
	}
	lease, err := owner.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		return domain.Lease{}, nil, err
	}
	return lease, owner, nil
}

func decisionWithOwnerLease(decision domain.PlacementDecision, lease domain.Lease) domain.PlacementDecision {
	if lease.NodeID != "" {
		decision.NodeID = lease.NodeID
	}
	if lease.InstanceID != "" {
		decision.InstanceID = lease.InstanceID
	}
	if len(lease.AcceleratorSet) > 0 {
		decision.AcceleratorSet = append([]int(nil), lease.AcceleratorSet...)
	}
	if lease.Claim != (domain.Claim{}) {
		decision.Claim = lease.Claim
	}
	return decision
}

func runBeforeColdLoadHook(ctx context.Context, decision domain.PlacementDecision, hooks []SubmitHooks) error {
	for _, hook := range hooks {
		if hook.BeforeColdLoad == nil {
			continue
		}
		if err := hook.BeforeColdLoad(ctx, decision); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) inspectPreemptions(ctx context.Context, decision domain.PlacementDecision, fleet domain.FleetSnapshot) (map[string]domain.Lease, map[string]domain.ModelInstance, error) {
	preemptedLeases := map[string]domain.Lease{}
	preemptedVictims := map[string]domain.ModelInstance{}
	for _, victimID := range decision.Preempted {
		victim, ok := instanceByID(fleet.Instances, victimID)
		if !ok {
			return nil, nil, fmt.Errorf("preempted instance %q is missing from fleet snapshot", victimID)
		}
		preemptedVictims[victimID] = victim
		lease, err := s.inspectOwnerLease(ctx, victim)
		if err != nil {
			return nil, nil, err
		}
		if lease.ID != "" {
			preemptedLeases[victim.ID] = lease
		}
	}
	return preemptedLeases, preemptedVictims, nil
}

func (s *Service) finishPreemption(ctx context.Context, decision domain.PlacementDecision, fleet domain.FleetSnapshot, preemptedLeases map[string]domain.Lease, preemptedVictims map[string]domain.ModelInstance) error {
	cleanupCtx := cleanupContext(ctx)
	preempted := map[string]struct{}{}
	for _, victimID := range decision.Preempted {
		if _, ok := preemptedVictims[victimID]; !ok {
			return fmt.Errorf("preempted instance %q is missing from preemption inspection", victimID)
		}
		preempted[victimID] = struct{}{}
	}
	for _, replacement := range decision.Replacements {
		if _, ok := preempted[replacement.InstanceID]; !ok {
			return fmt.Errorf("replacement instance %q was not preempted", replacement.InstanceID)
		}
	}
	for _, victimID := range decision.Preempted {
		victim := preemptedVictims[victimID]
		agent, err := s.Nodes.NodeAgent(victim.NodeID)
		if err != nil {
			return err
		}
		if err := agent.Unload(cleanupCtx, victim.ID); err != nil {
			return err
		}
		if err := s.Store.DeleteInstance(cleanupCtx, victim.ID); err != nil {
			return err
		}
		if lease, ok := preemptedLeases[victim.ID]; ok && lease.ID != "" {
			if s.Owners == nil {
				return fmt.Errorf("owner admission resolver is not configured")
			}
			owner, err := s.Owners.AdmissionController(victim.NodeID)
			if err != nil {
				return err
			}
			if err := owner.Release(cleanupCtx, lease.ID); err != nil {
				return err
			}
			if err := s.Store.DeleteLease(cleanupCtx, lease.ID); err != nil {
				return err
			}
		}
	}
	for _, replacement := range decision.Replacements {
		victim := preemptedVictims[replacement.InstanceID]
		lease, ok := preemptedLeases[replacement.InstanceID]
		if !ok || lease.JobID == "" {
			continue
		}
		if err := s.replacePreemptedInstance(ctx, lease, victim, replacement, fleet); err != nil {
			if queueErr := s.queuePreemptedJob(ctx, replacement.InstanceID, preemptedLeases); queueErr != nil {
				return errors.Join(err, queueErr)
			}
		}
	}
	for _, instanceID := range decision.Requeued {
		if err := s.queuePreemptedJob(ctx, instanceID, preemptedLeases); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) enactPreemption(ctx context.Context, _ domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) error {
	preemptedLeases, preemptedVictims, err := s.inspectPreemptions(ctx, decision, fleet)
	if err != nil {
		return err
	}
	return s.finishPreemption(ctx, decision, fleet, preemptedLeases, preemptedVictims)
}

func (s *Service) replacePreemptedInstance(ctx context.Context, lease domain.Lease, victim domain.ModelInstance, replacement domain.Replacement, fleet domain.FleetSnapshot) error {
	node, ok := nodeByID(fleet.Nodes, replacement.NodeID)
	if !ok {
		return fmt.Errorf("replacement node %q is missing from fleet snapshot", replacement.NodeID)
	}
	preset, ok := s.Presets[victim.PresetID]
	if !ok {
		return fmt.Errorf("unknown replacement preset %q", victim.PresetID)
	}
	owner, err := s.Owners.AdmissionController(replacement.NodeID)
	if err != nil {
		return err
	}
	replacementJob, err := s.replacementJob(ctx, lease, victim)
	if err != nil {
		return err
	}
	offer, err := owner.Offer(ctx, domain.AdmissionRequest{
		Job:            replacementJob,
		Preset:         preset,
		Claim:          victim.Claim,
		NodeID:         replacement.NodeID,
		AcceleratorSet: append([]int(nil), replacement.AcceleratorSet...),
		ReservationID:  lease.ReservationID,
	})
	if err != nil {
		return err
	}
	ownerLease, err := owner.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		return err
	}
	releaseOwner := func() error {
		if ownerLease.ID == "" {
			return nil
		}
		err := owner.Release(cleanupContext(ctx), ownerLease.ID)
		ownerLease = domain.Lease{}
		return err
	}
	decision := domain.PlacementDecision{
		JobID:          replacementJob.ID,
		NodeID:         replacement.NodeID,
		AcceleratorSet: append([]int(nil), replacement.AcceleratorSet...),
		Claim:          victim.Claim,
	}
	preset, err = tuneLaunchForPlacement(preset, decision, node)
	if err != nil {
		return withOwnerRelease(err, releaseOwner)
	}
	agent, err := s.Nodes.NodeAgent(replacement.NodeID)
	if err != nil {
		return withOwnerRelease(err, releaseOwner)
	}
	inst, err := agent.Load(ctx, domain.LoadRequest{
		JobID:          replacementJob.ID,
		Preset:         preset,
		Claim:          victim.Claim,
		AcceleratorSet: append([]int(nil), replacement.AcceleratorSet...),
		ReservationID:  lease.ReservationID,
		Priority:       victim.Priority,
	})
	if err != nil {
		return withOwnerRelease(err, releaseOwner)
	}
	if inst.Claim == (domain.Claim{}) {
		inst.Claim = victim.Claim
	}
	replacementLease := s.finalizeOwnerLease(replacementJob, inst, decision, ownerLease)
	replacementLease.ReservationID = lease.ReservationID
	replacementLease.Pinned = lease.Pinned
	if err := s.ensureOwnerLeaseBound(ctx, ownerLease, replacementLease); err != nil {
		unloadErr := agent.Unload(ctx, inst.ID)
		releaseErr := releaseOwner()
		return errors.Join(err, unloadErr, releaseErr)
	}
	savedInstance := false
	savedLease := false
	cleanupReplacement := func() error {
		cleanupCtx := cleanupContext(ctx)
		unloadErr := agent.Unload(cleanupCtx, inst.ID)
		releaseErr := releaseOwner()
		var deleteInstanceErr error
		var deleteLeaseErr error
		if savedInstance {
			deleteInstanceErr = s.Store.DeleteInstance(cleanupCtx, inst.ID)
		}
		if savedLease {
			deleteLeaseErr = s.Store.DeleteLease(cleanupCtx, replacementLease.ID)
		}
		return errors.Join(unloadErr, releaseErr, deleteInstanceErr, deleteLeaseErr)
	}
	replacementJob.Status = domain.JobRunning
	if err := s.Store.SaveInstance(ctx, inst); err != nil {
		return errors.Join(err, cleanupReplacement())
	}
	savedInstance = true
	if err := s.Store.SaveLease(ctx, replacementLease); err != nil {
		return errors.Join(err, cleanupReplacement())
	}
	savedLease = true
	if err := s.Store.SaveJob(ctx, replacementJob); err != nil {
		return errors.Join(err, cleanupReplacement())
	}
	return nil
}

func withOwnerRelease(err error, release func() error) error {
	if releaseErr := release(); releaseErr != nil {
		return errors.Join(err, releaseErr)
	}
	return err
}

func (s *Service) cleanupLoadedInstance(ctx context.Context, inst domain.ModelInstance, cold bool, release func() error) error {
	cleanupCtx := cleanupContext(ctx)
	var unloadErr error
	var deleteErr error
	if cold && inst.ID != "" {
		agent, err := s.Nodes.NodeAgent(inst.NodeID)
		if err != nil {
			unloadErr = err
		} else {
			unloadErr = agent.Unload(cleanupCtx, inst.ID)
		}
		deleteErr = s.Store.DeleteInstance(cleanupCtx, inst.ID)
	}
	return errors.Join(unloadErr, deleteErr, release())
}

func cleanupContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func coordinatedQueueErr(err error) bool {
	return errors.Is(err, domain.ErrStaleFence) || errors.Is(err, domain.ErrNoFit)
}

func (s *Service) replacementJob(ctx context.Context, lease domain.Lease, victim domain.ModelInstance) (domain.Job, error) {
	if reader, ok := s.Store.(JobReader); ok && lease.JobID != "" {
		job, err := reader.Job(ctx, lease.JobID)
		if err != nil {
			return domain.Job{}, err
		}
		return job, nil
	}
	if lease.JobID == "" {
		return domain.Job{}, fmt.Errorf("preempted instance %q has no owner lease job", victim.ID)
	}
	return domain.Job{
		ID:       lease.JobID,
		PresetID: victim.PresetID,
		Priority: victim.Priority,
	}, nil
}

func (s *Service) queuePreemptedJob(ctx context.Context, instanceID string, preemptedLeases map[string]domain.Lease) error {
	lease, ok := preemptedLeases[instanceID]
	if !ok || lease.JobID == "" {
		return fmt.Errorf("requeued instance %q has no owner lease job", instanceID)
	}
	job, err := s.requeueJob(ctx, lease.JobID)
	if err != nil {
		return err
	}
	if s.Coordinator != nil {
		payload, err := s.requeuePayload(ctx, job.ID)
		if err != nil {
			return err
		}
		s.Queue.EnqueueWithPayload(job, payload)
	} else {
		s.Queue.Enqueue(job)
	}
	return s.Store.SaveJob(ctx, job)
}

func (s *Service) requeuePayload(ctx context.Context, jobID string) ([]byte, error) {
	reader, ok := s.JobLog.(JobPayloadReader)
	if !ok {
		return nil, fmt.Errorf("coordinated requeue for job %q requires a job payload reader", jobID)
	}
	job, payload, err := reader.Job(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.ID != jobID {
		return nil, fmt.Errorf("job payload reader returned %q for requested %q", job.ID, jobID)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("job %q has no rescue payload for coordinated requeue", jobID)
	}
	return payload, nil
}

func (s *Service) inspectOwnerLease(ctx context.Context, victim domain.ModelInstance) (domain.Lease, error) {
	if s.Owners == nil {
		return domain.Lease{}, fmt.Errorf("owner admission resolver is not configured")
	}
	owner, err := s.Owners.AdmissionController(victim.NodeID)
	if err != nil {
		return domain.Lease{}, err
	}
	inspector, ok := owner.(ports.LeaseInspector)
	if !ok {
		return domain.Lease{}, fmt.Errorf("owner admission for node %q does not expose lease inspection", victim.NodeID)
	}
	lease, found, err := inspector.LeaseForInstance(ctx, victim.ID)
	if err != nil {
		return domain.Lease{}, err
	}
	if !found {
		return domain.Lease{}, nil
	}
	return lease, nil
}

func admissionPreemptions(job domain.Job, decision domain.PlacementDecision, preemptedLeases map[string]domain.Lease) []domain.PreemptionTarget {
	if len(decision.Preempted) == 0 {
		return nil
	}
	targets := make([]domain.PreemptionTarget, 0, len(decision.Preempted))
	for _, instanceID := range decision.Preempted {
		lease := preemptedLeases[instanceID]
		if lease.ID == "" {
			continue
		}
		reason := "preempted"
		if decision.JobID != "" {
			reason += " for " + decision.JobID
		} else if job.ID != "" {
			reason += " for " + job.ID
		}
		targets = append(targets, domain.PreemptionTarget{
			LeaseID:    lease.ID,
			InstanceID: instanceID,
			Reason:     reason,
		})
	}
	return targets
}

func (s *Service) requeueJob(ctx context.Context, jobID string) (domain.Job, error) {
	reader, ok := s.Store.(JobReader)
	if !ok {
		return domain.Job{}, fmt.Errorf("runtime store cannot load original job %q for requeue", jobID)
	}
	job, err := reader.Job(ctx, jobID)
	if err != nil {
		return domain.Job{}, err
	}
	job.Status = domain.JobQueued
	return job, nil
}

func (s *Service) bindOwnerInstance(ctx context.Context, nodeID, leaseID, instanceID string) error {
	if s.Owners == nil {
		return fmt.Errorf("owner admission resolver is not configured")
	}
	owner, err := s.Owners.AdmissionController(nodeID)
	if err != nil {
		return err
	}
	binder, ok := owner.(ports.LeaseBinder)
	if !ok {
		return fmt.Errorf("owner admission for node %q does not expose lease binding", nodeID)
	}
	return binder.BindInstance(ctx, leaseID, instanceID)
}

func (s *Service) ensureOwnerLeaseBound(ctx context.Context, ownerLease, lease domain.Lease) error {
	if ownerLease.InstanceID != "" {
		if ownerLease.InstanceID != lease.InstanceID {
			return fmt.Errorf("owner lease %q is bound to instance %q, not %q", ownerLease.ID, ownerLease.InstanceID, lease.InstanceID)
		}
		return nil
	}
	if lease.InstanceID == "" {
		return fmt.Errorf("owner lease %q has no instance to bind", lease.ID)
	}
	if lease.ID == "" {
		return fmt.Errorf("owner lease id is required")
	}
	if lease.NodeID == "" {
		return fmt.Errorf("owner lease %q has no owner node", lease.ID)
	}
	return s.bindOwnerInstance(ctx, lease.NodeID, lease.ID, lease.InstanceID)
}

func (s *Service) cleanupCoordinatedLoad(ctx context.Context, jobID string, inst domain.ModelInstance) error {
	agent, err := s.Nodes.NodeAgent(inst.NodeID)
	if err != nil {
		return err
	}
	cleanupCtx := cleanupContext(ctx)
	unloadErr := agent.Unload(cleanupCtx, inst.ID)
	releaseErr := s.Coordinator.Release(cleanupCtx, jobID)
	deleteErr := s.Store.DeleteInstance(cleanupCtx, inst.ID)
	return errors.Join(unloadErr, releaseErr, deleteErr)
}

func (s *Service) resolveInstance(ctx context.Context, job domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) (domain.ModelInstance, error) {
	if decision.InstanceID != "" {
		inst, ok := instanceByNodeAndID(fleet.Instances, decision.NodeID, decision.InstanceID)
		if !ok {
			return domain.ModelInstance{}, fmt.Errorf("selected instance %q on node %q is missing from fleet snapshot", decision.InstanceID, decision.NodeID)
		}
		return inst, nil
	}
	if decision.NodeID == "" {
		return domain.ModelInstance{}, fmt.Errorf("placement action %q did not select a node", decision.Action)
	}
	node, ok := nodeByID(fleet.Nodes, decision.NodeID)
	if !ok {
		return domain.ModelInstance{}, fmt.Errorf("selected node %q is missing from fleet snapshot", decision.NodeID)
	}
	var err error
	preset := decision.Preset
	if preset.ID == "" {
		preset, err = s.resolvePreset(job)
		if err != nil {
			return domain.ModelInstance{}, err
		}
	}
	preset, err = tuneLaunchForPlacement(preset, decision, node)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	agent, err := s.Nodes.NodeAgent(decision.NodeID)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	inst, err := agent.Load(ctx, domain.LoadRequest{
		JobID:          job.ID,
		Preset:         preset,
		Claim:          decision.Claim,
		AcceleratorSet: append([]int(nil), decision.AcceleratorSet...),
		Priority:       job.Priority,
	})
	if err != nil {
		return domain.ModelInstance{}, err
	}
	if inst.Claim == (domain.Claim{}) {
		inst.Claim = decision.Claim
	}
	return inst, nil
}

func (s *Service) resolvePreset(job domain.Job) (domain.Preset, error) {
	if job.PresetID != "" {
		if preset, ok := s.Presets[job.PresetID]; ok {
			return preset, nil
		}
		return domain.Preset{}, fmt.Errorf("unknown preset %q", job.PresetID)
	}
	if job.Model != "" {
		if preset, ok := s.Presets[job.Model]; ok {
			return preset, nil
		}
		return domain.Preset{}, fmt.Errorf("unknown model %q", job.Model)
	}
	return domain.Preset{}, fmt.Errorf("job %q has no model or preset", job.ID)
}

func (s *Service) finalizeOwnerLease(job domain.Job, inst domain.ModelInstance, decision domain.PlacementDecision, lease domain.Lease) domain.Lease {
	now := s.Clock.Now()
	if lease.ID == "" {
		lease.ID = "lease-" + job.ID
	}
	lease.JobID = job.ID
	lease.InstanceID = inst.ID
	lease.NodeID = inst.NodeID
	if len(lease.AcceleratorSet) == 0 {
		lease.AcceleratorSet = append([]int(nil), decision.AcceleratorSet...)
	}
	if lease.Claim == (domain.Claim{}) {
		lease.Claim = decision.Claim
	}
	if lease.Priority == "" {
		lease.Priority = job.Priority
	}
	if lease.GrantedAt.IsZero() {
		lease.GrantedAt = now.UTC()
	}
	if lease.ExpiresAt.IsZero() {
		lease.ExpiresAt = now.Add(30 * time.Minute).UTC()
	}
	return lease
}

func instanceByID(instances []domain.ModelInstance, id string) (domain.ModelInstance, bool) {
	for _, inst := range instances {
		if inst.ID == id {
			return inst, true
		}
	}
	return domain.ModelInstance{}, false
}

func instanceByNodeAndID(instances []domain.ModelInstance, nodeID, id string) (domain.ModelInstance, bool) {
	for _, inst := range instances {
		if inst.ID == id && (nodeID == "" || inst.NodeID == nodeID) {
			return inst, true
		}
	}
	return domain.ModelInstance{}, false
}

func nodeByID(nodes []domain.Node, id string) (domain.Node, bool) {
	for _, node := range nodes {
		if node.ID == id {
			return node, true
		}
	}
	return domain.Node{}, false
}
