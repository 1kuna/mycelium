package scheduler

import (
	"context"
	"errors"
	"fmt"
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

type JobLog interface {
	PutJob(ctx context.Context, job domain.Job, payload []byte) error
}

type Service struct {
	Placer             ports.Placer
	Fleet              FleetSource
	Nodes              NodeResolver
	Owners             AdmissionResolver
	Coordinator        ports.Coordinator
	PreSend            ports.PreSendNegotiator
	JobLog             JobLog
	Queue              *Queue
	Store              RuntimeStore
	Clock              ports.Clock
	Presets            map[string]domain.Preset
	PreSendNegotiation bool
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

	if err := s.enactPreemption(ctx, job, decision, fleet); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	ownerLease, owner, err := s.commitOwnerAdmission(ctx, job, decision)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	releaseOwner := func() error {
		if ownerLease.ID == "" {
			return nil
		}
		if err := owner.Release(ctx, ownerLease.ID); err != nil {
			return err
		}
		ownerLease = domain.Lease{}
		return nil
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
	if err := releaseOwner(); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision, Instance: inst}, err
	}
	lease := s.grantLease(job, inst, decision)
	job.Status = domain.JobRunning
	if err := s.Store.SaveInstance(ctx, inst); err != nil {
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveLease(ctx, lease); err != nil {
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
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
		s.Queue.Enqueue(job)
		if err := s.Store.SaveJob(ctx, job); err != nil {
			return Result{Decision: decision}, err
		}
		return Result{Decision: decision}, nil
	}

	fleet, err := s.Fleet.Snapshot(ctx)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	decision, err = s.negotiatePreSend(ctx, job, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	if err := s.enactPreemption(ctx, job, decision, fleet); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	ownerLease, err := s.Coordinator.Commit(ctx, decision)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	if decision.InstanceID == "" && ownerLease.ID == "" {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, fmt.Errorf("coordinated cold load for job %q returned no owner lease", job.ID)
	}
	if ownerLease.NodeID != "" {
		decision.NodeID = ownerLease.NodeID
	}
	if ownerLease.Claim != (domain.Claim{}) {
		decision.Claim = ownerLease.Claim
	}
	if decision.InstanceID == "" {
		if err := runBeforeColdLoadHook(ctx, decision, hooks); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			if releaseErr := s.Coordinator.Release(ctx, job.ID); releaseErr != nil {
				return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
			}
			return Result{Decision: decision, Lease: ownerLease}, err
		}
	}
	inst, err := s.resolveInstance(ctx, job, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		if releaseErr := s.Coordinator.Release(ctx, job.ID); releaseErr != nil {
			return Result{Decision: decision, Lease: ownerLease}, errors.Join(err, releaseErr)
		}
		return Result{Decision: decision, Lease: ownerLease}, err
	}
	lease := ownerLease
	lease.JobID = job.ID
	lease.NodeID = inst.NodeID
	lease.InstanceID = inst.ID
	if lease.ID == "" && decision.InstanceID != "" {
		lease.ID = "lease-" + job.ID
	}
	if lease.GrantedAt.IsZero() {
		lease.GrantedAt = s.Clock.Now().UTC()
	}
	if lease.ExpiresAt.IsZero() {
		lease.ExpiresAt = s.Clock.Now().Add(30 * time.Minute).UTC()
	}
	if ownerLease.ID != "" {
		if err := s.bindOwnerInstance(ctx, lease.NodeID, lease.ID, lease.InstanceID); err != nil {
			job.Status = domain.JobFailed
			_ = s.Store.SaveJob(ctx, job)
			if cleanupErr := s.cleanupCoordinatedLoad(ctx, job.ID, inst); cleanupErr != nil {
				return Result{Decision: decision, Instance: inst, Lease: lease}, errors.Join(err, cleanupErr)
			}
			return Result{Decision: decision, Instance: inst, Lease: lease}, err
		}
	}
	job.Status = domain.JobRunning
	if err := s.Store.SaveInstance(ctx, inst); err != nil {
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveLease(ctx, lease); err != nil {
		return Result{Decision: decision, Instance: inst, Lease: lease}, err
	}
	if err := s.Store.SaveJob(ctx, job); err != nil {
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
		job, ok := s.Queue.Dequeue()
		if !ok {
			return results, nil
		}
		result, err := s.Submit(ctx, job)
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
		return s.Release(ctx, lease.ID)
	}
	if s.Coordinator != nil {
		if err := s.Coordinator.Release(ctx, lease.JobID); err != nil {
			return err
		}
	}
	if lease.ID == "" {
		return nil
	}
	return s.Store.DeleteLease(ctx, lease.ID)
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
		if err := s.Store.DeleteLease(ctx, lease.ID); err != nil {
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

func (s *Service) commitOwnerAdmission(ctx context.Context, job domain.Job, decision domain.PlacementDecision) (domain.Lease, ports.AdmissionController, error) {
	if decision.InstanceID != "" || decision.Action == domain.ActionQueued {
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
	offer, err := owner.Offer(ctx, job, decision.Claim)
	if err != nil {
		return domain.Lease{}, nil, err
	}
	lease, err := owner.Commit(ctx, offer.OfferID, offer.Fence)
	if err != nil {
		return domain.Lease{}, nil, err
	}
	return lease, owner, nil
}

func (s *Service) negotiatePreSend(ctx context.Context, job domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) (domain.PlacementDecision, error) {
	if !s.PreSendNegotiation {
		return decision, nil
	}
	if s.PreSend == nil {
		return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation is enabled but no negotiator is configured")
	}
	return s.PreSend.Negotiate(ctx, job, decision, fleet)
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

func (s *Service) enactPreemption(ctx context.Context, job domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) error {
	for _, victimID := range decision.Preempted {
		victim, ok := instanceByID(fleet.Instances, victimID)
		if !ok {
			return fmt.Errorf("preempted instance %q is missing from fleet snapshot", victimID)
		}
		if s.Coordinator != nil {
			if err := s.preemptOwnerLease(ctx, job, decision, victim); err != nil {
				return err
			}
		}
		agent, err := s.Nodes.NodeAgent(victim.NodeID)
		if err != nil {
			return err
		}
		if err := agent.Unload(ctx, victim.ID); err != nil {
			return err
		}
		if err := s.Store.DeleteInstance(ctx, victim.ID); err != nil {
			return err
		}
	}
	for _, instanceID := range decision.Requeued {
		victim, ok := instanceByID(fleet.Instances, instanceID)
		if !ok {
			return fmt.Errorf("requeued instance %q is missing from fleet snapshot", instanceID)
		}
		job := domain.Job{
			ID:       "requeued-" + instanceID,
			PresetID: victim.PresetID,
			Project:  "preempted",
			Priority: domain.PriorityBackground,
			Status:   domain.JobQueued,
		}
		s.Queue.Enqueue(job)
		if err := s.Store.SaveJob(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) preemptOwnerLease(ctx context.Context, job domain.Job, decision domain.PlacementDecision, victim domain.ModelInstance) error {
	if s.Owners == nil {
		return fmt.Errorf("owner admission resolver is not configured")
	}
	owner, err := s.Owners.AdmissionController(victim.NodeID)
	if err != nil {
		return err
	}
	inspector, ok := owner.(ports.LeaseInspector)
	if !ok {
		return fmt.Errorf("owner admission for node %q does not expose lease inspection", victim.NodeID)
	}
	lease, found, err := inspector.LeaseForInstance(ctx, victim.ID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("preempted instance %q has no owner lease", victim.ID)
	}
	preempter, ok := owner.(ports.PolicyPreempter)
	if !ok {
		return fmt.Errorf("owner admission for node %q does not expose policy-aware preemption", victim.NodeID)
	}
	reason := "preempted"
	if decision.JobID != "" {
		reason += " for " + decision.JobID
	}
	if err := preempter.PreemptForJob(ctx, job, lease.ID, reason); err != nil {
		return err
	}
	return s.Store.DeleteLease(ctx, lease.ID)
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

func (s *Service) cleanupCoordinatedLoad(ctx context.Context, jobID string, inst domain.ModelInstance) error {
	agent, err := s.Nodes.NodeAgent(inst.NodeID)
	if err != nil {
		return err
	}
	unloadErr := agent.Unload(ctx, inst.ID)
	releaseErr := s.Coordinator.Release(ctx, jobID)
	return errors.Join(unloadErr, releaseErr)
}

func (s *Service) resolveInstance(ctx context.Context, job domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) (domain.ModelInstance, error) {
	if decision.InstanceID != "" {
		inst, ok := instanceByID(fleet.Instances, decision.InstanceID)
		if !ok {
			return domain.ModelInstance{}, fmt.Errorf("selected instance %q is missing from fleet snapshot", decision.InstanceID)
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
	preset, err := s.resolvePreset(job)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	preset, err = tuneLaunchForPlacement(preset, decision, node)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	agent, err := s.Nodes.NodeAgent(decision.NodeID)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	inst, err := agent.Load(ctx, preset)
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

func (s *Service) grantLease(job domain.Job, inst domain.ModelInstance, decision domain.PlacementDecision) domain.Lease {
	now := s.Clock.Now()
	return domain.Lease{
		ID:         "lease-" + job.ID,
		JobID:      job.ID,
		InstanceID: inst.ID,
		NodeID:     inst.NodeID,
		Claim:      decision.Claim,
		GrantedAt:  now.UTC(),
		ExpiresAt:  now.Add(30 * time.Minute).UTC(),
	}
}

func instanceByID(instances []domain.ModelInstance, id string) (domain.ModelInstance, bool) {
	for _, inst := range instances {
		if inst.ID == id {
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
