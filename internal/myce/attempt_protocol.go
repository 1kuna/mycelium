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

func attemptReply(c Checkin, reply Reply) Reply {
	reply.AttemptProtocol = keyedAttemptProtocol
	if len(reply.Attempts) == 0 && reply.Job != nil {
		reply.Attempts = []AttemptDirective{{Execution: Execution{ID: reply.Job.ID, Attempt: reply.Job.Attempt}, Session: reply.Job.Session, Job: reply.Job, Hold: reply.Hold, Stop: reply.Stop, Kill: reply.Kill, GraceSeconds: reply.GraceSeconds, PriorityStop: reply.PriorityStop, UserCancel: reply.UserCancel}}
	}
	// Never send an unaddressed assignment or stop to a keyed worker.
	reply.Job, reply.PriorityStop = nil, nil
	reply.Hold, reply.Stop, reply.Kill, reply.UserCancel, reply.GraceSeconds = false, false, false, false, 0
	return reply
}

func (w Worker) checkin(ctx context.Context, c Checkin) (Reply, error) {
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
	var reply Reply
	err := w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply)
	var refusal *HTTPError
	if c.Machine.InventoryProbe != nil && errors.As(err, &refusal) && refusal.Status == 400 && refusal.Code == "request_invalid" {
		// Rolling upgrades: predecessor queues still consume device reasons.
		c.Machine.InventoryProbe = nil
		if w.Client.inventoryWire != nil {
			w.Client.inventoryWire.Store(false)
		}
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
		w.diskBudgetWire.supported = reply.WorkerDiskBudgetProtocol == workerDiskBudgetProtocol
		w.diskBudgetWire.headroom = reply.WorkerDiskHeadroomProtocol == workerDiskHeadroomProtocol
		w.diskBudgetWire.foreign = reply.HostForeignProtocol == hostForeignProtocol
		w.diskBudgetWire.trend = reply.WorkerDiskTrendProtocol == workerDiskTrendProtocol
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
