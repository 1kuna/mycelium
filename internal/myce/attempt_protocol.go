package myce

import (
	"context"
	"errors"
)

const keyedAttemptProtocol = "keyed-attempt-v1"

// Adapt only the wire representation. The existing one-attempt queue decisions
// remain authoritative until concurrent admission is implemented separately.
func normalizeAttemptCheckin(c Checkin) (Checkin, error) {
	if c.AttemptProtocol == "" {
		if len(c.Attempts) != 0 {
			return c, currentProtocolError("keyed attempts require attempt_protocol keyed-attempt-v1")
		}
		return c, nil
	}
	if c.AttemptProtocol != keyedAttemptProtocol || c.Running != nil || c.Started {
		return c, currentProtocolError("keyed-attempt-v1 forbids legacy running/started fields")
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
	if c.AttemptProtocol == "" {
		return reply
	}
	reply.AttemptProtocol = keyedAttemptProtocol
	if len(reply.Attempts) == 0 && reply.Job != nil {
		reply.Attempts = []AttemptDirective{{Execution: Execution{ID: reply.Job.ID, Attempt: reply.Job.Attempt}, Session: reply.Job.Session, Job: reply.Job, Hold: reply.Hold, Stop: reply.Stop, Kill: reply.Kill, GraceSeconds: reply.GraceSeconds, PriorityStop: reply.PriorityStop}}
	}
	// Never send an unaddressed assignment or stop to a keyed worker.
	reply.Job, reply.PriorityStop = nil, nil
	reply.Hold, reply.Stop, reply.Kill, reply.GraceSeconds = false, false, false, 0
	return reply
}

func (w Worker) checkin(ctx context.Context, c Checkin) (Reply, error) {
	c.AttemptProtocol = keyedAttemptProtocol
	if c.Running != nil {
		c.Attempts = []AttemptReport{{Execution: *c.Running, Session: c.Machine.Session, Started: c.Started}}
	}
	c.Running, c.Started = nil, false
	var reply Reply
	if err := w.Client.Call(ctx, "POST", "/api/workers/checkin", c, &reply); err != nil {
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

// Validate the whole collection before mutation. Singular projections below are
// retained for benchmark/direct callers; the M2 supervisor consumes Attempts.
func workerAttemptReply(c Checkin, reply Reply) (Reply, error) {
	if reply.AttemptProtocol != keyedAttemptProtocol {
		return Reply{}, currentProtocolError("queue lacks keyed-attempt-v1: upgrade the queue before this worker; saved attempt custody is retained")
	}
	if reply.Job != nil || reply.Stop || reply.Kill || reply.Hold || reply.PriorityStop != nil || reply.GraceSeconds != 0 {
		return Reply{}, currentProtocolError("keyed-attempt-v1 queue returned unkeyed attempt decisions")
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
			reply.Job = a.Job
			continue
		}
		if len(c.Attempts) == 1 {
			current := c.Attempts[0]
			if current.ID == a.ID && current.Attempt == a.Attempt && current.Session == a.Session {
				reply.Stop, reply.Kill, reply.Hold = a.Stop, a.Kill, a.Hold
				reply.GraceSeconds, reply.PriorityStop = a.GraceSeconds, a.PriorityStop
			}
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
