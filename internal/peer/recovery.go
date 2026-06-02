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
	for _, rec := range records {
		if !r.shouldConsider(deadPeerID, rec) {
			continue
		}
		var decision rescueDecision
		if err := r.step("recovery/decide", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode}, func() error {
			var err error
			decision, err = r.rescueDecision(ctx, deadPeerID, rec)
			return err
		}); err != nil {
			return rescued, err
		}
		switch decision {
		case rescueNow:
			if err := r.step("recovery/rescue", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID}, func() error {
				return r.Rescue(ctx, rec)
			}); err != nil {
				return rescued, err
			}
			rescued++
		case rescuePartition:
			if err := r.step("recovery/partition", map[string]any{"dead_peer": deadPeerID, "job_id": rec.JobID, "owner": rec.AssignedNode}, func() error {
				return r.recordPartition(ctx, rec)
			}); err != nil {
				return rescued, err
			}
		case rescueSkip:
			continue
		}
	}
	return rescued, nil
}

func (r Recovery) step(op string, input map[string]any, fn func() error) error {
	if r.Trace == nil {
		return fn()
	}
	return r.Trace.Do(op, input, fn)
}

func (r Recovery) shouldConsider(deadPeerID string, rec domain.JobRecord) bool {
	if rec.PayloadRedacted {
		return false
	}
	return unfinished(rec.Status) && (rec.Coordinator == deadPeerID || rec.AssignedNode == deadPeerID)
}

type rescueDecision int

const (
	rescueSkip rescueDecision = iota
	rescueNow
	rescuePartition
)

func (r Recovery) rescueDecision(ctx context.Context, deadPeerID string, rec domain.JobRecord) (rescueDecision, error) {
	if rec.AssignedNode == "" || rec.Status == domain.JobQueued || rec.Status == domain.JobPlacing {
		return rescueNow, nil
	}
	if rec.AssignedNode == deadPeerID {
		return rescueNow, nil
	}
	if r.Owners == nil {
		return rescueSkip, fmt.Errorf("lease inspector resolver is not configured")
	}
	owner, err := r.Owners.LeaseInspector(rec.AssignedNode)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return rescuePartition, nil
		}
		return rescueSkip, err
	}
	_, found, err := owner.LeaseForJob(ctx, rec.JobID)
	if err != nil {
		if errors.Is(err, domain.ErrUnreachable) {
			return rescuePartition, nil
		}
		return rescueSkip, err
	}
	if !found {
		return rescueNow, nil
	}
	return rescueSkip, nil
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
