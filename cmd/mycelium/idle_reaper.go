package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
)

const defaultIdleReaperInterval = 5 * time.Second

type idleReaperStore interface {
	ListInstances(ctx context.Context) ([]domain.ModelInstance, error)
	DeleteInstance(ctx context.Context, id string) error
}

type idleReaper struct {
	Store  idleReaperStore
	Fleet  scheduler.FleetSource
	Nodes  scheduler.NodeResolver
	Clock  ports.Clock
	Policy domain.AutonomyPolicy
}

type idleReapReport struct {
	Scanned int
	Reaped  int
}

func startIdleReaper(ctx context.Context, store idleReaperStore, fleet scheduler.FleetSource, nodes scheduler.NodeResolver, policy domain.AutonomyPolicy, clk ports.Clock, interval time.Duration) {
	if policy.IdleTTLMS <= 0 || store == nil || fleet == nil || nodes == nil || clk == nil {
		return
	}
	if interval <= 0 {
		interval = defaultIdleReaperInterval
	}
	reaper := &idleReaper{
		Store:  store,
		Fleet:  fleet,
		Nodes:  nodes,
		Clock:  clk,
		Policy: policy,
	}
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			if _, err := reaper.Tick(ctx); err != nil {
				log.Printf("mycelium idle reap tick failed: %v", err)
			}
		}
	}()
}

func (r *idleReaper) Tick(ctx context.Context) (idleReapReport, error) {
	if r == nil || r.Policy.IdleTTLMS <= 0 {
		return idleReapReport{}, nil
	}
	if r.Store == nil || r.Fleet == nil || r.Nodes == nil || r.Clock == nil {
		return idleReapReport{}, fmt.Errorf("idle reaper is not fully configured")
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return idleReapReport{}, err
	}
	stored, err := r.Store.ListInstances(ctx)
	if err != nil {
		return idleReapReport{}, err
	}
	byID := map[string]domain.ModelInstance{}
	for _, inst := range stored {
		byID[inst.ID] = inst
	}
	candidates := append([]domain.ModelInstance(nil), fleet.Instances...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].NodeID != candidates[j].NodeID {
			return candidates[i].NodeID < candidates[j].NodeID
		}
		return candidates[i].ID < candidates[j].ID
	})
	report := idleReapReport{Scanned: len(candidates)}
	for _, live := range candidates {
		inst := mergeIdleStoredInstance(live, byID[live.ID])
		if !r.reapEligible(inst) {
			continue
		}
		agent, err := r.Nodes.NodeAgent(inst.NodeID)
		if err != nil {
			return report, err
		}
		if err := agent.Unload(ctx, inst.ID); err != nil {
			return report, err
		}
		if err := r.Store.DeleteInstance(ctx, inst.ID); err != nil {
			return report, err
		}
		report.Reaped++
	}
	return report, nil
}

func mergeIdleStoredInstance(live, stored domain.ModelInstance) domain.ModelInstance {
	if live.Origin == "" {
		live.Origin = stored.Origin
	}
	if live.CreatedAt.IsZero() {
		live.CreatedAt = stored.CreatedAt
	}
	if live.LastActivityAt.IsZero() {
		live.LastActivityAt = stored.LastActivityAt
	}
	return live
}

func (r *idleReaper) reapEligible(inst domain.ModelInstance) bool {
	if inst.Origin != domain.InstanceOriginAutoscale {
		return false
	}
	if inst.Pinned || inst.ReservationID != "" || inst.State != domain.InstReady || inst.InFlight != 0 {
		return false
	}
	activity := inst.LastActivityAt
	if activity.IsZero() {
		activity = inst.CreatedAt
	}
	if activity.IsZero() {
		return false
	}
	now := r.Clock.Now()
	if cooldown := time.Duration(r.Policy.ScaleCooldownMS) * time.Millisecond; cooldown > 0 {
		if inst.CreatedAt.IsZero() || now.Before(inst.CreatedAt.Add(cooldown)) {
			return false
		}
	}
	return now.Sub(activity) > time.Duration(r.Policy.IdleTTLMS)*time.Millisecond
}
