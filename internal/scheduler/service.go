package scheduler

import (
	"context"
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

type RuntimeStore interface {
	SaveJob(ctx context.Context, job domain.Job) error
	SaveLease(ctx context.Context, lease domain.Lease) error
	SaveInstance(ctx context.Context, inst domain.ModelInstance) error
	DeleteInstance(ctx context.Context, id string) error
}

type Service struct {
	Placer  ports.Placer
	Fleet   FleetSource
	Nodes   NodeResolver
	Queue   *Queue
	Store   RuntimeStore
	Clock   ports.Clock
	Presets map[string]domain.Preset
}

type Result struct {
	Decision domain.PlacementDecision
	Instance domain.ModelInstance
	Lease    domain.Lease
}

func (s *Service) Submit(ctx context.Context, job domain.Job) (Result, error) {
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

	if err := s.enactPreemption(ctx, decision, fleet); err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
	}
	inst, err := s.resolveInstance(ctx, job, decision, fleet)
	if err != nil {
		job.Status = domain.JobFailed
		_ = s.Store.SaveJob(ctx, job)
		return Result{Decision: decision}, err
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

func (s *Service) validate() error {
	if s.Placer == nil || s.Fleet == nil || s.Nodes == nil || s.Queue == nil || s.Store == nil || s.Clock == nil {
		return fmt.Errorf("scheduler service is not fully configured")
	}
	return nil
}

func (s *Service) enactPreemption(ctx context.Context, decision domain.PlacementDecision, fleet domain.FleetSnapshot) error {
	for _, victimID := range decision.Preempted {
		victim, ok := instanceByID(fleet.Instances, victimID)
		if !ok {
			return fmt.Errorf("preempted instance %q is missing from fleet snapshot", victimID)
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
	preset, err := s.resolvePreset(job)
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
