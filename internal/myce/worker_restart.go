package myce

import (
	"context"
	"errors"
	"log"
	"net/url"
	"time"
)

// A worker that stops to restart (installed TERM, foreground SIGTERM) keeps its
// session and every unpublished result in its spool (#2166). It says so on the
// way out, so the queue holds that session's attempts for the configured
// restart grace rather than the learned check-in allowance, which is about two
// check-in periods (#2172). Any service restart is longer than that: systemd's
// RestartSec alone is 3 s. A worker that dies without the notice is still lost
// at the learned allowance; one that announced and never returns is lost when
// the grace runs out. The notice never moves contact or LastSeen.
type workerRestart struct {
	Session string    `json:"session"`
	At      time.Time `json:"at"`
}

const workerRestartPath = "/restart"

// AnnounceRestart records that the named worker session is restarting. A
// machine already declared gone, or a different session, gets nothing.
func (q *Queue) AnnounceRestart(id, session string, requestContexts ...context.Context) error {
	return q.updateRequest(requestContext(requestContexts), func() error {
		q.maintain()
		m := q.s.Machines[id]
		if m == nil || m.Gone || session == "" || m.Session != session {
			return errors.New("machine session is not in the fleet")
		}
		if q.workerRestartGrace <= 0 {
			return nil // The operator turned restart grace off.
		}
		if q.s.WorkerRestarts == nil {
			q.s.WorkerRestarts = map[string]workerRestart{}
		}
		q.s.WorkerRestarts[id] = workerRestart{Session: session, At: q.clock.Now()}
		return nil
	}, nil)
}

// workerRestartHeld reports whether m's current session announced a restart
// whose grace has not yet run out.
func (q *Queue) workerRestartHeld(m *Machine, now time.Time) bool {
	r, ok := q.s.WorkerRestarts[m.ID]
	return ok && !m.Gone && r.Session == m.Session && q.workerRestartGrace > 0 && now.Before(r.At.Add(q.workerRestartGrace))
}

// settleWorkerRestart drops a notice that no longer protects anything and
// reports whether it ran out while still naming m's session.
func (q *Queue) settleWorkerRestart(m *Machine, now time.Time) (lapsed bool) {
	r, ok := q.s.WorkerRestarts[m.ID]
	if !ok || q.workerRestartHeld(m, now) {
		return false
	}
	delete(q.s.WorkerRestarts, m.ID)
	return !m.Gone && r.Session == m.Session
}

// A check-in that reached the queue after the notice comes from the restarted
// process. One already in flight from the stopping process arrived earlier.
// The restart gap is not a check-in interval: learning it would stretch the
// returned worker's loss allowance to the restart's length.
func (q *Queue) workerReturned(id string, contact *checkinContact) {
	r, ok := q.s.WorkerRestarts[id]
	if !ok {
		return
	}
	if contact == nil || contact.arrived.After(r.At) {
		delete(q.s.WorkerRestarts, id)
		q.discontinueContactCadence(id)
	}
}

// announceRestart sends the restart notice in the background. Its result only
// matters while the queue's learned loss allowance, about two check-in periods,
// is still running, so it gets that long, and no less than the RFC 6298
// one-second response floor. The caller waits on the returned channel before
// exiting; an unreachable queue costs at most that bound.
func (w Worker) announceRestart(session string) <-chan struct{} {
	done := make(chan struct{})
	bound := max(2*checkinPeriod(w.Interval), clientRecoveryPeriod)
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		path := "/api/workers/" + url.PathEscape(w.Machine.ID) + workerRestartPath
		if err := w.Client.Call(ctx, "POST", path, map[string]string{"session": session}, nil); err != nil {
			// An older queue has no such route. The ordinary loss allowance applies.
			log.Printf("restart notice not delivered: %v", err)
		}
	}()
	return done
}
