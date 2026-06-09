package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/trace"
)

type LeaseInspectorResolver interface {
	LeaseInspector(nodeID string) (ports.LeaseInspector, error)
	JobStatusInspector(nodeID string) (ports.JobStatusInspector, error)
}

type admissionResolver interface {
	AdmissionController(nodeID string) (ports.AdmissionController, error)
}

type RescueFunc func(ctx context.Context, rec domain.JobRecord) error

type Recovery struct {
	Registry ports.JobRegistry
	Owners   LeaseInspectorResolver
	Rescue   RescueFunc
	Clock    ports.Clock
	Trace    *trace.Trace
}

func (r Recovery) RecoverPeer(ctx context.Context, deadPeerID string) (int, error) {
	if r.Registry == nil || r.Rescue == nil {
		return 0, fmt.Errorf("peer recovery is not fully configured")
	}
	if deadPeerID == "" {
		return 0, fmt.Errorf("dead peer id is required")
	}
	var records []domain.JobRecord
	if err := r.step("recovery/snapshot", map[string]any{"dead_peer": deadPeerID}, func() error {
		var err error
		records, err = r.Registry.Snapshot(ctx)
		return err
	}); err != nil {
		return 0, err
	}
	rescued := 0
	var recordErrs []error
	for _, rec := range records {
		if !r.shouldConsider(deadPeerID, rec) {
			continue
		}
		var decision rescuePlan
		if err := r.step("recovery/decide", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode}, func() error {
			var err error
			decision, err = r.rescueDecision(ctx, deadPeerID, rec)
			return err
		}); err != nil {
			recordErrs = append(recordErrs, err)
			continue
		}
		switch decision.action {
		case rescueCleanup:
			if decision.lease.ID != "" {
				if err := r.step("recovery/cleanup_release", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode, "lease_id": decision.lease.ID}, func() error {
					return r.releaseOwnerLease(ctx, rec.AssignedNode, decision.lease.ID)
				}); err != nil {
					recordErrs = append(recordErrs, err)
					continue
				}
			}
			if err := r.step("recovery/cleanup_clear", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID}, func() error {
				return r.recordCleanupCleared(ctx, rec)
			}); err != nil {
				recordErrs = append(recordErrs, err)
				continue
			}
		case rescueNow:
			if decision.lease.ID != "" {
				if err := r.step("recovery/release_orphan", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode, "lease_id": decision.lease.ID}, func() error {
					return r.releaseOwnerLease(ctx, rec.AssignedNode, decision.lease.ID)
				}); err != nil {
					recordErrs = append(recordErrs, err)
					continue
				}
			}
			if err := r.step("recovery/rescue", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID}, func() error {
				return r.Rescue(ctx, rec)
			}); err != nil {
				recordErrs = append(recordErrs, err)
				continue
			}
			rescued++
		case rescuePartition:
			if err := r.step("recovery/partition", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode}, func() error {
				return r.recordPartition(ctx, rec)
			}); err != nil {
				recordErrs = append(recordErrs, err)
				continue
			}
		}
	}
	return rescued, errors.Join(recordErrs...)
}

func (r Recovery) step(op string, input map[string]any, fn func() error) error {
	if r.Trace == nil {
		return fn()
	}
	return r.Trace.Do(op, input, fn)
}

func (r Recovery) shouldConsider(deadPeerID string, rec domain.JobRecord) bool {
	if rec.CleanupRequired {
		return rec.Coordinator == deadPeerID || rec.AssignedNode == deadPeerID
	}
	return unfinished(rec.Status) && (rec.Coordinator == deadPeerID || rec.AssignedNode == deadPeerID)
}

type rescuePlan struct {
	action rescueAction
	lease  domain.Lease
}

type rescueAction int

const (
	rescueSkip rescueAction = iota
	rescueCleanup
	rescueNow
	rescuePartition
)

func (r Recovery) rescueDecision(ctx context.Context, deadPeerID string, rec domain.JobRecord) (rescuePlan, error) {
	if rec.CleanupRequired {
		if rec.AssignedNode == "" || rec.AssignedNode == deadPeerID {
			return rescuePlan{action: rescueSkip}, nil
		}
		if r.Owners == nil {
			return rescuePlan{action: rescueSkip}, fmt.Errorf("lease inspector resolver is not configured")
		}
		owner, err := r.Owners.LeaseInspector(rec.AssignedNode)
		if err != nil {
			if errors.Is(err, domain.ErrUnreachable) {
				return rescuePlan{action: rescuePartition}, nil
			}
			return rescuePlan{action: rescueSkip}, err
		}
		lease, found, err := owner.LeaseForJob(ctx, rec.JobID)
		if err != nil {
			if errors.Is(err, domain.ErrUnreachable) {
				return rescuePlan{action: rescuePartition}, nil
			}
			return rescuePlan{action: rescueSkip}, err
		}
		if found && lease.ID == "" {
			return rescuePlan{action: rescueSkip}, fmt.Errorf("owner %q returned lease for job %q without lease id", rec.AssignedNode, rec.JobID)
		}
		return rescuePlan{action: rescueCleanup, lease: lease}, nil
	}
	if rec.PayloadRedacted {
		return rescuePlan{action: rescueSkip}, nil
	}
	if rec.AssignedNode == "" || rec.Status == domain.JobQueued || rec.Status == domain.JobPlacing {
		return rescuePlan{action: rescueNow}, nil
	}
	if rec.AssignedNode == deadPeerID {
		return rescuePlan{action: rescueNow}, nil
	}
	if r.Owners == nil {
		return rescuePlan{action: rescueSkip}, fmt.Errorf("lease inspector resolver is not configured")
	}
	status, found, err := r.ownerJobStatus(ctx, rec.AssignedNode, rec.JobID)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return rescuePlan{action: rescuePartition}, nil
		}
		return rescuePlan{action: rescueSkip}, err
	}
	if found && !unfinished(status) {
		return rescuePlan{action: rescueSkip}, nil
	}

	owner, err := r.Owners.LeaseInspector(rec.AssignedNode)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return rescuePlan{action: rescuePartition}, nil
		}
		return rescuePlan{action: rescueSkip}, err
	}
	lease, found, err := owner.LeaseForJob(ctx, rec.JobID)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return rescuePlan{action: rescuePartition}, nil
		}
		return rescuePlan{action: rescueSkip}, err
	}
	if !found {
		return rescuePlan{action: rescueNow}, nil
	}
	if lease.ID == "" {
		return rescuePlan{action: rescueSkip}, fmt.Errorf("owner %q returned lease for job %q without lease id", rec.AssignedNode, rec.JobID)
	}
	return rescuePlan{action: rescueNow, lease: lease}, nil
}

func (r Recovery) ownerJobStatus(ctx context.Context, nodeID, jobID string) (domain.JobStatus, bool, error) {
	if r.Owners == nil {
		return "", false, fmt.Errorf("lease inspector resolver is not configured")
	}
	statusInspector, err := r.Owners.JobStatusInspector(nodeID)
	if err != nil {
		return "", false, err
	}
	return statusInspector.JobStatus(ctx, jobID)
}

func (r Recovery) releaseOwnerLease(ctx context.Context, nodeID, leaseID string) error {
	resolver, ok := r.Owners.(admissionResolver)
	if !ok {
		return fmt.Errorf("admission resolver is required to release orphaned lease %q on owner %q", leaseID, nodeID)
	}
	owner, err := resolver.AdmissionController(nodeID)
	if err != nil {
		return err
	}
	return owner.Release(ctx, leaseID)
}

func (r Recovery) recordPartition(ctx context.Context, rec domain.JobRecord) error {
	rec.RecoveryNote = fmt.Sprintf("partition: owner %q could not be checked while recovering dead peer for coordinator %q", rec.AssignedNode, rec.Coordinator)
	rec.Fence++
	now := r.clock().Now().UTC()
	if !now.After(rec.UpdatedAt) {
		now = rec.UpdatedAt.Add(time.Nanosecond)
	}
	rec.UpdatedAt = now
	return r.Registry.Put(ctx, rec)
}

func (r Recovery) recordCleanupCleared(ctx context.Context, rec domain.JobRecord) error {
	rec.CleanupRequired = false
	rec.CleanupError = ""
	rec.RecoveryNote = appendRecoveryNote(rec.RecoveryNote, "terminal cleanup recovered")
	rec.Fence++
	now := r.clock().Now().UTC()
	if !now.After(rec.UpdatedAt) {
		now = rec.UpdatedAt.Add(time.Nanosecond)
	}
	rec.UpdatedAt = now
	return r.Registry.Put(ctx, rec)
}

func (r Recovery) clock() ports.Clock {
	if r.Clock != nil {
		return r.Clock
	}
	return clock.System{}
}

func unfinished(status domain.JobStatus) bool {
	switch status {
	case domain.JobQueued, domain.JobPlacing, domain.JobLoading, domain.JobRunning:
		return true
	default:
		return false
	}
}
