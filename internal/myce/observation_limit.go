package myce

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Admission state is proportional to requests actually in flight, never peer
// identities. Cost statistics use the finite route classes already used by
// rejection attribution. No address, raw URL, job ID or client version is kept.
type observationLimit struct {
	mu       sync.Mutex
	clock    Clock
	pressure *queuePressure
	index    *waitStateIndex
	active   map[*observationRead]struct{}
	costs    map[string]observationCost
}
type observationCost struct {
	mean    float64
	samples uint64
}
type observationRead struct {
	route       string
	tier, scope int
	started     time.Time
	healthy     bool
	epoch       uint64
}

func newObservationLimit(clock Clock, q *Queue) *observationLimit {
	return &observationLimit{clock: clock, pressure: q.mu.enablePressure(clock),
		index: q.waitIndex(), active: make(map[*observationRead]struct{}), costs: make(map[string]observationCost)}
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
		if id, _ := strings.CutPrefix(r.URL.Path, "/api/jobs/"); id != "" && !strings.Contains(id, "/") && r.URL.Query().Get("view") == "wait" {
			return "job_wait"
		}
		return "job_full"
	}
	return ""
}

// Scope is a value ordering, not a concurrency/rate tuning parameter. Broad
// projections are expendable before a selected job's diagnosis, then scalar
// completion reads. Within a class/scope, this installation's measured cost wins.
func observationScope(route string) int {
	switch route {
	case "job_wait":
		return 0
	case "job_full":
		return 1
	default:
		return 2
	}
}
func (l *observationLimit) better(a, b *observationRead) bool {
	if a.tier != b.tier {
		return a.tier < b.tier
	}
	if a.scope != b.scope {
		return a.scope < b.scope
	}
	cost := func(route string) float64 {
		if c := l.costs[route]; c.samples != 0 {
			return c.mean
		}
		return math.Inf(1) // unknown work cannot claim to be cheaper
	}
	return cost(a.route) < cost(b.route)
}

func (l *observationLimit) admit(w http.ResponseWriter, r *http.Request) (func(), bool) {
	route := observationRouteClass(r)
	// Batch waits use the independent scalar index, never the queue mutex.
	// Shedding these would push compatible clients back to expensive polling.
	if route == "" || route == "batch_wait" {
		return func() {}, true
	}
	tier, production := l.index.observationPriority("")
	if id, ok := strings.CutPrefix(r.URL.Path, "/api/jobs/"); ok {
		id, _, _ = strings.Cut(id, "/")
		tier, production = l.index.observationPriority(id)
	}
	if production {
		return func() {}, true
	}
	read := &observationRead{route: route, tier: tier, scope: observationScope(route), started: l.clock.Now()}
	l.mu.Lock()
	pressure, delay, epoch := l.pressure.measurement()
	var best *observationRead
	for active := range l.active {
		if best == nil || l.better(active, best) {
			best = active
		}
	}
	// Under pressure permit a higher-value read through the in-flight frontier.
	// Equal/lower-value reads wait for actual work to finish. With no observation
	// in flight allow a probe: progress, not a fixed timer, restores admission.
	if pressure && best != nil && !l.better(read, best) {
		l.mu.Unlock()
		// Retry-After is integer seconds; round the measured live wait up to
		// the protocol's smallest positive unit, rather than a refill schedule.
		seconds := max(int64(1), int64(math.Ceil(delay.Seconds())))
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		respond(w, nil, &HTTPError{Status: 429, Code: "client_rate_limited", Message: "queue control is delayed; retry observation after Retry-After"})
		return nil, false
	}
	read.healthy = !pressure
	read.epoch = epoch
	l.active[read] = struct{}{}
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.active, read)
		pressure, _, epoch := l.pressure.measurement()
		if d := l.clock.Now().Sub(read.started); read.healthy && !pressure && read.epoch == epoch && d > 0 {
			cost := l.costs[route]
			cost.samples++
			cost.mean += (float64(d) - cost.mean) / float64(cost.samples)
			l.costs[route] = cost
		}
	}, true
}
