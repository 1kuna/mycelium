package myce

import (
	"sync"
	"time"
)

// Two complete minute windows reject a burst, while catching the recorded
// thousands/minute overload and ~190 three-second waiters within 2–3 minutes.
const requestHealthSlowPerMinute = 100
const requestHealthWaitPerMinute = 20
const requestHealthClientPerMinute = 1800

var requestHealthKinds = [...]string{"cli", "python", "unknown"}

type requestHealthMinute struct {
	at            time.Time
	slow, waiting uint64
	clients       [3]uint64
}

// Fixed memory, independent of request/identity cardinality. This mutex is never
// acquired under q.mu. Windows are process-local; restart discards old traffic.
type requestHealth struct {
	mu      sync.Mutex
	started time.Time
	minutes [3]requestHealthMinute
}

type requestHealthSnapshot struct {
	at       time.Time
	overload bool
	clients  [3]bool
}

func (h *requestHealth) minute(now time.Time) *requestHealthMinute {
	if h.started.IsZero() {
		h.started = now
	}
	at := now.Truncate(time.Minute)
	i := (at.Unix() / 60) % 3
	if i < 0 {
		i += 3
	}
	b := &h.minutes[i]
	if !b.at.Equal(at) {
		*b = requestHealthMinute{at: at}
	}
	return b
}

func (h *requestHealth) recordClient(now time.Time, kind string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.minute(now)
	for i, k := range requestHealthKinds {
		if kind == k {
			b.clients[i]++
			return
		}
	}
}

func (h *requestHealth) recordRequest(now time.Time, route string, duration, wait time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.minute(now)
	// Worker events and batched consumer waits intentionally long-poll. Actual
	// mutex wait still counts; only intentional response duration is excluded.
	intentionalWait := route == "GET /api/workers/{id}/events" || route == "POST /api/wait"
	if !intentionalWait && duration > requestSlowThreshold {
		b.slow++
	}
	if wait >= time.Second {
		b.waiting++
	}
}

func (h *requestHealth) snapshot(now time.Time) requestHealthSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.minute(now)
	s := requestHealthSnapshot{at: now, overload: true, clients: [3]bool{true, true, true}}
	for n := 1; n <= 2; n++ {
		at := now.Truncate(time.Minute).Add(-time.Duration(n) * time.Minute)
		var b requestHealthMinute
		for _, candidate := range h.minutes {
			if candidate.at.Equal(at) {
				b = candidate
			}
		}
		// An initial partial minute is not evidence of a sustained rate.
		complete := !at.Before(h.started)
		s.overload = s.overload && complete && (b.slow >= requestHealthSlowPerMinute || b.waiting >= requestHealthWaitPerMinute)
		total := b.clients[0] + b.clients[1] + b.clients[2]
		for i, count := range b.clients {
			s.clients[i] = s.clients[i] && complete && count >= requestHealthClientPerMinute && count >= total-total/5
		}
	}
	return s
}

// Tick computes the bounded summary outside q.mu and commits only these fixed
// booleans through the ordinary keyed episode/cooldown path. Ignore an older
// concurrently captured tick rather than clearing newer evidence.
func (q *Queue) applyRequestHealth(s requestHealthSnapshot) {
	if s.at.Before(q.requestHealthAt) {
		return
	}
	q.requestHealthAt = s.at
	q.observeHealth("queue-overload", s.overload, 0, HealthAlert{Kind: "queue_overload"})
	for i, kind := range requestHealthKinds {
		q.observeHealth("client-flood/"+kind, s.clients[i], 0, HealthAlert{Kind: "client_request_flood", ClientKind: kind})
	}
}
