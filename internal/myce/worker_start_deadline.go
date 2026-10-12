package myce

import (
	"context"
	"time"
)

// A positive reply means this exact attempt is durably started, before execution.
func (w Worker) authorizeStart(j Job, session string) error {
	if j.Spec.StartBy == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return w.Client.Call(ctx, "POST", "/api/workers/started", Result{Machine: w.Machine.ID, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, nil)
}
