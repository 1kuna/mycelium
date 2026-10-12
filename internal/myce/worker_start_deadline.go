package myce

import (
	"context"
	"errors"
	"time"
)

// Preparation may finish after expiry. Ask the queue again immediately before dispatch.
func (w Worker) authorizeStart(j Job, session string) error {
	if j.Spec.StartBy == nil {
		return nil
	}
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	before := clock.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var permit StartPermit
	if err := w.Client.Call(ctx, "POST", "/api/workers/start-permit", Result{Machine: w.Machine.ID, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, &permit); err != nil {
		return err
	}
	// Count the whole round trip against the budget; worker clock skew cannot extend it.
	if !clock.Now().Before(before.Add(time.Duration(permit.RemainingSeconds * float64(time.Second)))) {
		return errors.New("production start permit expired in transit")
	}
	return nil
}
