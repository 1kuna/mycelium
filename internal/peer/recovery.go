package peer

import (
	"context"
	"errors"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type LeaseInspectorResolver interface {
	LeaseInspector(nodeID string) (ports.LeaseInspector, error)
}

type RescueFunc func(ctx context.Context, rec domain.JobRecord) error

type Recovery struct {
	Registry ports.JobRegistry
	Owners   LeaseInspectorResolver
	Rescue   RescueFunc
}

func (r Recovery) RecoverPeer(ctx context.Context, deadPeerID string) (int, error) {
	if r.Registry == nil || r.Rescue == nil {
		return 0, fmt.Errorf("peer recovery is not fully configured")
	}
	if deadPeerID == "" {
		return 0, fmt.Errorf("dead peer id is required")
	}
	records, err := r.Registry.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	rescued := 0
	for _, rec := range records {
		if !r.shouldConsider(deadPeerID, rec) {
			continue
		}
		shouldRescue, err := r.shouldRescue(ctx, rec)
		if err != nil {
			return rescued, err
		}
		if !shouldRescue {
			continue
		}
		if err := r.Rescue(ctx, rec); err != nil {
			return rescued, err
		}
		rescued++
	}
	return rescued, nil
}

func (r Recovery) shouldConsider(deadPeerID string, rec domain.JobRecord) bool {
	if rec.PayloadRedacted {
		return false
	}
	return rec.Coordinator == deadPeerID && unfinished(rec.Status)
}

func (r Recovery) shouldRescue(ctx context.Context, rec domain.JobRecord) (bool, error) {
	if rec.AssignedNode == "" || rec.Status == domain.JobQueued || rec.Status == domain.JobPlacing {
		return true, nil
	}
	if r.Owners == nil {
		return false, fmt.Errorf("lease inspector resolver is not configured")
	}
	owner, err := r.Owners.LeaseInspector(rec.AssignedNode)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return true, nil
		}
		return false, err
	}
	_, found, err := owner.LeaseForJob(ctx, rec.JobID)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return true, nil
		}
		return false, err
	}
	return !found, nil
}

func unfinished(status domain.JobStatus) bool {
	switch status {
	case domain.JobQueued, domain.JobPlacing, domain.JobLoading, domain.JobRunning:
		return true
	default:
		return false
	}
}
