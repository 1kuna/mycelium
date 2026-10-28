package myce

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var errWorkerStartAuthorization = errors.New("queue start authorization unavailable")

// Distinguish the Started boundary while retaining the authorization family
// used by adapters to keep control-plane failures out of stage-failure paths.
var errWorkerStartHandoff = fmt.Errorf("queue start hand-off unavailable: %w", errWorkerStartAuthorization)

// Match the normal control-request budget. Measured queue mutex waits reached
// 3.54s (#225), exceeding both former start deadlines (2s and 3s). Ten seconds
// admits those bursts while bounding a hung queue without opening either gate.
const workerStartRequestTimeout = 10 * time.Second

// A positive reply means this exact attempt is durably started, before execution.
func (w Worker) authorizeStart(j Job, session string) error {
	if err := w.checkAvailability(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), workerStartRequestTimeout)
	defer cancel()
	var reply struct {
		OK        bool   `json:"ok"`
		StartGate string `json:"start_gate"`
	}
	if err := w.Client.Call(ctx, "POST", "/api/workers/started", Result{InputsStagedAt: j.InputsStagedAt, Machine: w.Machine.ID, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, &reply); err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartHandoff, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol {
		return fmt.Errorf("queue start-gate protocol mismatch: upgrade queue and workers together")
	}
	return w.checkAvailability()
}
