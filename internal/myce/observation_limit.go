package myce

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Five reads/second/source, burst ten: the incident produced 27.4 slow
// job reads/second. A batch long-poll needs 0.05/s while unchanged. The burst
// permits status/log/result inspection without letting one source queue work
// indefinitely ahead of worker control. Identity is the transport peer, never
// a self-selected header, User-Agent, port, or shared fleet token.
const observationRate = 5.0
const observationBurst = 10.0

type observationBucket struct {
	tokens float64
	at     time.Time
}
type observationLimit struct {
	mu      sync.Mutex
	clock   Clock
	clients map[string]observationBucket
}

func newObservationLimit(clock Clock) *observationLimit {
	return &observationLimit{clock: clock, clients: map[string]observationBucket{}}
}
func observationRequest(r *http.Request) bool {
	// All worker and launch/report routes are exempt, including GET event streams.
	if workerProtocolEndpoint(r.URL.Path) {
		return false
	}
	return r.Method == "GET" || r.Method == "HEAD" || r.Method == "POST" && r.URL.Path == "/api/wait"
}
func (l *observationLimit) allow(peer string) bool {
	if host, _, err := net.SplitHostPort(peer); err == nil {
		peer = host
	}
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.clients[peer]
	if !ok {
		// Reclaim fully replenished idle buckets. The table's 4096-entry ceiling
		// bounds memory on public listeners; a new source at capacity is refused,
		// never evicts a busy source to reset its allowance.
		if len(l.clients) >= 4096 {
			for key, v := range l.clients {
				if now.Sub(v.at) >= time.Minute {
					delete(l.clients, key)
				}
			}
			if len(l.clients) >= 4096 {
				return false
			}
		}
		b = observationBucket{tokens: observationBurst, at: now}
	}
	if elapsed := now.Sub(b.at).Seconds(); elapsed > 0 {
		b.tokens = min(observationBurst, b.tokens+elapsed*observationRate)
		b.at = now
	}
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	l.clients[peer] = b
	return allowed
}
func (l *observationLimit) admit(w http.ResponseWriter, r *http.Request) bool {
	if !observationRequest(r) || l.allow(r.RemoteAddr) {
		return true
	}
	w.Header().Set("Retry-After", "1")
	respond(w, nil, &HTTPError{Status: 429, Code: "client_rate_limited", Message: "consumer observation rate exceeded; retry after one second"})
	return false
}
