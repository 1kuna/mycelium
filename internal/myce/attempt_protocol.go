package myce

import (
	"context"
	"errors"
)

const keyedAttemptProtocol = "keyed-attempt-v1"

// Every supported release addresses reports and controls by exact attempt key.
func normalizeAttemptCheckin(c Checkin) (Checkin, error) {
	if c.AttemptProtocol != keyedAttemptProtocol || c.Running != nil || c.Started {
		return c, currentProtocolError("keyed-attempt-v1 requires its revision and forbids unkeyed running/started fields")
	}
	seen := map[attemptKey]bool{}
	for _, a := range c.Attempts {
		if a.ID == "" || a.Attempt <= 0 || a.Session == "" || a.Session != c.Machine.Session {
			return c, currentProtocolError("attempt report requires job, positive attempt and the reporting worker session")
		}
		key := attemptKey{a.ID, a.Attempt, a.Session}
		if seen[key] {
			return c, currentProtocolError("duplicate attempt report key")
		}
		seen[key] = true
	}
	return c, nil
}

func (w Worker) checkin(ctx context.Context, c Checkin) (Reply, error) {
	if w.Client.Clock == nil {
		w.Client.Clock = w.Clock
	}
	if w.Client.publicationWire != nil && !w.Client.publicationWire.Load() {
		c.Machine.ResultPublications = nil
	}
	if w.Client.inventoryWire != nil && !w.Client.inventoryWire.Load() {
		c.Machine.InventoryProbe = nil
	}
	c.period = checkinPeriod(w.Interval)
	c.AttemptProtocol = keyedAttemptProtocol
	c = w.deviceTelemetryCheckin(c)
	if w.toolReport != nil {
		c = w.toolReport.checkin(c)
	}
	c = w.diskBudgetCheckin(c)
	c, expirySent := w.checkpointExpiryCheckin(c)
	// Client.Call sends the gate only to a queue that advertised it.
	gateSent := c.Machine.HostRAMGate != nil && w.Client.hostRAMGateWire != nil && w.Client.hostRAMGateWire.Load()
	shadowSent := c.Machine.MemoryShadow != nil && w.Client.memoryShadowWire != nil && w.Client.memoryShadowWire.Load()
	if !shadowSent {
		c.Machine.MemoryShadow = nil
	}
	var reply Reply
	err := w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	if shadowSent && gateSent && w.Client.hostRAMGateWire.Load() && !w.Client.memoryShadowWire.Load() && requestDecodeRefused(err) {
		// The immediately preceding queue may know the RAM gate and inventory.
		// Keep its legacy admission inputs when only the shadow field is new.
		c.Machine.MemoryShadow, shadowSent = nil, false
		err = w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	}
	var refusal *HTTPError
	if (c.Machine.InventoryProbe != nil || gateSent || shadowSent) && errors.As(err, &refusal) && refusal.Status == 400 && refusal.Code == "request_invalid" {
		// Rolling upgrades: predecessor queues still consume device reasons
		// and the ordinary ledger. A sent gate means the queue was replaced
		// since it advertised the gate; drop both in this one retry.
		if c.Machine.InventoryProbe != nil && w.Client.inventoryWire != nil {
			w.Client.inventoryWire.Store(false)
		}
		c.Machine.InventoryProbe, c.Machine.HostRAMGate, c.Machine.MemoryShadow = nil, nil, nil
		err = w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	}
	if len(c.Machine.ResultPublications) > 0 && errors.As(err, &refusal) && refusal.Status == 400 && refusal.Code == "request_invalid" {
		c.Machine.ResultPublications = nil
		if w.Client.publicationWire != nil {
			w.Client.publicationWire.Store(false)
		}
		err = w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	}
	if (hasDiskBudgetWireFields(c) || hasForeignDetailFields(c) || c.CheckpointExpiryProtocol != "") && errors.As(err, &refusal) && refusal.Status == 400 && refusal.Code == "request_invalid" {
		// A strict predecessor queue (for example after a queue rollback)
		// refuses the additive field sets; retry once with none.
		c = withoutCheckpointExpiryFields(withoutForeignDetailFields(withoutDiskBudgetFields(c)))
		expirySent = 0
		if w.diskBudgetWire != nil {
			w.diskBudgetWire.supported, w.diskBudgetWire.foreign = false, false
		}
		err = w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	}
	if err != nil {
		var he *HTTPError
		if errors.As(err, &he) && (he.Status == 400 && he.Code == "request_invalid" || he.Status == 426) {
			return Reply{}, currentProtocolError("queue rejected keyed-attempt-v1: upgrade the queue before this worker; saved attempt custody is retained")
		}
		return Reply{}, err
	}
	validated, err := workerAttemptReply(c, reply)
	if err != nil {
		return Reply{}, err
	}
	if w.diskBudgetWire != nil {
		w.diskBudgetWire.supported = reply.WorkerDiskBudgetProtocol == workerDiskBudgetProtocol &&
			reply.WorkerDiskHeadroomProtocol == workerDiskHeadroomProtocol &&
			reply.WorkerDiskTrendProtocol == workerDiskTrendProtocol
		w.diskBudgetWire.foreign = reply.HostForeignProtocol == hostForeignProtocol
	}
	if w.toolReport != nil {
		w.toolReport.accept(c, reply)
	}
	w.acceptCheckpointExpiryReply(expirySent, reply)
	// A lost/discarded assignment leaves no local custody to stop. Validate
	// the whole reply first, then explicitly release only unheld own-session
	// keys; omission alone is never the queue's release evidence.
	for _, a := range reply.Attempts {
		if !a.Stop || a.Session != c.Machine.Session {
			continue
		}
		held := false
		for _, report := range c.Attempts {
			if report.ID == a.ID && report.Attempt == a.Attempt && report.Session == a.Session {
				held = true
				break
			}
		}
		if !held {
			ack := Result{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Machine: c.Machine.ID, Session: a.Session, ReleaseOnly: true}
			if err := w.Client.Call(ctx, "POST", "/api/workers/complete", ack, nil); err != nil && !stale(err) {
				return Reply{}, err
			}
		}
	}
	return validated, nil
}

// Validate the whole collection before mutation. The supervisor consumes Attempts.
func workerAttemptReply(c Checkin, reply Reply) (Reply, error) {
	if reply.AttemptProtocol != keyedAttemptProtocol {
		return Reply{}, currentProtocolError("queue lacks keyed-attempt-v1: upgrade the queue before this worker; saved attempt custody is retained")
	}
	if reply.Job != nil || reply.Stop || reply.Kill || reply.Hold || reply.PriorityStop != nil || reply.GraceSeconds != 0 || reply.UserCancel {
		return Reply{}, currentProtocolError("keyed-attempt-v1 queue returned unkeyed attempt decisions")
	}
	// A cold session can be idle while the queue learns its cadence.
	// Empty is a complete idle response, with no assignment to index or launch.
	if len(reply.Attempts) == 0 {
		return reply, nil
	}
	seen := map[attemptKey]bool{}
	for _, a := range reply.Attempts {
		if a.ID == "" || a.Attempt <= 0 || a.Session == "" {
			return Reply{}, currentProtocolError("queue attempt decision requires job, positive attempt and session")
		}
		key := attemptKey{a.ID, a.Attempt, a.Session}
		if seen[key] {
			return Reply{}, currentProtocolError("duplicate attempt decision key")
		}
		seen[key] = true
		if a.Job != nil {
			if c.Recovering {
				return Reply{}, currentProtocolError("queue offered work during startup recovery; upgrade the queue before this worker; saved attempt custody is retained")
			}
			for _, held := range c.Attempts {
				if held.ID == a.ID && held.Attempt == a.Attempt && held.Session == a.Session {
					return Reply{}, currentProtocolError("queue reassigned an owned attempt")
				}
			}
			if a.Job.ID != a.ID || a.Job.Attempt != a.Attempt || a.Job.Session != a.Session || a.Session != c.Machine.Session || a.Job.Machine != c.Machine.ID || a.Stop || a.Kill || a.Hold {
				return Reply{}, currentProtocolError("queue assignment does not match its attempt key or worker")
			}
			continue
		}

	}
	return reply, nil
}

// Identity excludes progress and other mutable report facts.
type attemptKey struct {
	job     string
	attempt int
	session string
}

func (k attemptKey) matches(e Execution) bool { return k.job == e.ID && k.attempt == e.Attempt }

func attemptOffer(j *Job) AttemptDirective {
	return AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: j}
}
