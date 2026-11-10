package myce

import "context"

// Single-attempt scheduling fixtures use concise inputs, translated to the same
// keyed requests as current workers. Omission never acknowledges release.
func keyedFixtureCheckin(c Checkin) Checkin {
	c.AttemptProtocol = keyedAttemptProtocol
	if c.Running != nil {
		c.Attempts = []AttemptReport{{Execution: *c.Running, Session: c.Machine.Session, Started: c.Started}}
	}
	c.Running, c.Started = nil, false
	return c
}
func fixtureReply(c Checkin, r Reply) Reply {
	for _, a := range r.Attempts {
		if a.Job != nil {
			r.Job = a.Job
		}
		if len(c.Attempts) == 1 && c.Attempts[0].Execution == a.Execution && c.Attempts[0].Session == a.Session {
			r.Stop, r.Kill, r.Hold = a.Stop, a.Kill, a.Hold
			r.GraceSeconds, r.PriorityStop, r.UserCancel = a.GraceSeconds, a.PriorityStop, a.UserCancel
		}
	}
	return r
}
func (q *Queue) Checkin(c Checkin) (Reply, error) { return q.CheckinContext(context.Background(), c) }
func (q *Queue) CheckinContext(ctx context.Context, c Checkin) (Reply, error) {
	if c.AttemptProtocol != "" {
		return q.CheckinKeyedContext(ctx, c)
	}
	c = keyedFixtureCheckin(c)
	r, err := q.CheckinKeyedContext(ctx, c)
	return fixtureReply(c, r), err
}
func (w Worker) fixtureCheckin(ctx context.Context, c Checkin) (Reply, error) {
	c = keyedFixtureCheckin(c)
	r, err := w.checkin(ctx, c)
	return fixtureReply(c, r), err
}
