package myce

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// One source can legitimately observe 31 concurrent jobs every two seconds
// plus fetch results (about 16 reads/s). Twenty reads/s and a burst of 100
// accommodate that measured workload; managed batched waits and parent-exit
// cleanup now prevent the orphaned-waiter flood that motivated the old cap.
// Identity is the transport peer, never a self-selected header, User-Agent,
// port, or shared fleet token.
const observationRate = 20.0
const observationBurst = 100.0

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
	return observationRouteClass(r) != ""
}

// Finite classes only: never retain a URL, job identity, query or peer address.
func observationRouteClass(r *http.Request) string {
	// All worker and launch/report routes are exempt, including GET event streams.
	if workerProtocolEndpoint(r.URL.Path) {
		return ""
	}
	if r.Method == "POST" {
		if r.URL.Path == "/api/wait" {
			return "batch_wait"
		}
		return ""
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		return ""
	}
	// File transfer, instruction/catalog polls and environment references are
	// also worker dependencies. Limit consumer observation projections, never
	// the data/control routes required to launch or finish work.
	switch r.URL.Path {
	case "/":
		return "page"
	case "/api/status":
		return "status"
	case "/api/inspection":
		return "inspection"
	case "/api/usage":
		return "usage"
	case "/api/portability":
		return "portability"
	}
	if strings.HasPrefix(r.URL.Path, "/api/jobs/") {
		if r.URL.Query().Get("view") == "wait" {
			return "job_wait"
		}
		return "job_full"
	}
	return ""
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
