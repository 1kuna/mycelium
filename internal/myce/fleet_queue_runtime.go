package myce

import (
	"context"
	"errors"
	"time"
)

// ConfigureFleetService is used only by the installed queue startup path. An
// unmanaged foreground queue remains usable, but cannot update its own service.
func (q *Queue) ConfigureFleetService(c ServiceConfig) error {
	if c.Role != "queue" || c.HostID == "" || c.Config == "" || c.Binary == "" || c.Service == "" {
		return errors.New("fleet queue requires saved installed settings")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	copy := c
	q.fleetService = &copy
	q.fleetQueue = &fleetQueueRuntime{Session: ID(), Host: c.HostID, OS: c.Detection.OS, Arch: c.Detection.Arch}
	return nil
}

func (q *Queue) runFleetQueueRuntime(ctx context.Context) {
	q.mu.RLock()
	c := q.fleetService
	q.mu.RUnlock()
	if c == nil {
		return
	}
	client, err := fleetClient(*c)
	if err != nil {
		return
	}
	observer := &workerFleetRuntime{config: *c, client: client, clock: q.clock, collect: func(ctx context.Context, c ServiceConfig, session string) (*FleetRuntimeProof, error) {
		p, err := CollectFleetRuntime(ctx, c, session)
		if err == nil && p != nil {
			p.ObservedAt = q.clock.Now()
		}
		return p, err
	}, launch: LaunchFleetUpdate}
	q.mu.RLock()
	session := q.fleetQueue.Session
	q.mu.RUnlock()
	machine := Machine{ID: "@queue", HostID: c.HostID, Session: session}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var generation uint64
	for {
		q.observeFleetQueue(ctx, observer, machine, &generation)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// observeFleetQueue is one pass of the queue's own runtime loop. The queue never
// checks in, so it gates its instruction fetch on its own state instead of a
// reply: it asks only while its own update is due (#1393).
func (q *Queue) observeFleetQueue(ctx context.Context, observer *workerFleetRuntime, machine Machine, generation *uint64) {
	q.mu.RLock()
	observer.instructionGated, observer.instructionWaiting = true, q.fleetQueueInstructionWaiting(machine.HostID)
	q.mu.RUnlock()
	proof := observer.observe(ctx, machine, true)
	q.mu.Lock()
	if q.fleetQueue != nil {
		if proof != nil && (q.fleetQueue.Proof == nil || !proof.ObservedAt.Equal(q.fleetQueue.Proof.ObservedAt)) {
			q.noteFleetProof(q.fleetQueue.Proof, proof, q.clock.Now())
		}
		q.fleetQueue.Proof = cloneFleetProof(proof)
		if *generation != observer.generation {
			*generation = observer.generation
			q.fleetQueue.ObservedAt = q.clock.Now()
		}
	}
	q.mu.Unlock()
}
