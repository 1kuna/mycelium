package myce

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var errWorkerStartAuthorization = errors.New("queue start authorization unavailable")

// A positive reply means this exact attempt is durably started, before execution.
func (w Worker) authorizeStart(j Job, session string) error {
	if err := w.checkAvailability(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var reply struct {
		OK        bool   `json:"ok"`
		StartGate string `json:"start_gate"`
	}
	if err := w.Client.Call(ctx, "POST", "/api/workers/started", Result{Machine: w.Machine.ID, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, &reply); err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartAuthorization, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol {
		return fmt.Errorf("queue start-gate protocol mismatch: upgrade queue and workers together")
	}
	return w.checkAvailability()
}
