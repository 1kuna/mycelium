package myce

import (
	"slices"
	"sort"
	"sync"
	"time"
)

// Diagnostic space/uncertainty bounds, shared with the host-health statistical
// model: 64 observations retain a distribution without request-sized history;
// eight observations populate quartiles, three confirmations reject one spike.
// These are statistical confidence choices, never rates or installation sizes.
const alertHistorySize = hostHealthHistorySize
const alertWarmup = hostHealthWarmup
const alertConfirmation = hostHealthConfirmation

type alertSeries []float64

func (s alertSeries) band() (float64, float64) {
	v := slices.Clone(s)
	var resolution float64
	for i := 1; i < len(v); i++ {
		resolution = max(resolution, absHealth(v[i]-v[i-1]))
	}
	sort.Float64s(v)
	lo, hi := v[len(v)/4], v[len(v)*3/4]
	// Tukey's far-outlier fence; adjacent variation provides measured resolution
	// for a flat or quantized baseline, without a machine-dependent epsilon.
	spread := max(hi-lo, resolution)
	return lo - 3*spread, hi + 3*spread
}
func (s *alertSeries) add(v float64) {
	*s = append(*s, v)
	if len(*s) > alertHistorySize {
		*s = (*s)[1:]
	}
}
func (s alertSeries) high(v float64) bool {
	if len(s) < alertWarmup {
		return false
	}
	_, hi := s.band()
	return v > hi
}

type requestHealthCost struct {
	count          uint64
	duration, wait time.Duration
}
type requestHealthRoute struct{ duration, wait alertSeries }

var requestHealthKinds = [...]string{"cli", "python", "unknown"}

// All aggregation and statistical work happens outside q.mu. Only normalized
// protocol routes and the three existing client families enter bounded state.
// A maintenance tick closes an interval; its measured duration replaces minute
// windows. Restart requires fresh local history, never imported fleet numbers.
type requestHealth struct {
	mu                  sync.Mutex
	started             time.Time
	costs               map[string]requestHealthCost
	routes              map[string]*requestHealthRoute
	clients             [3]uint64
	rates               [3]alertSeries
	adverse, recovering int
	active              bool
	result              requestHealthSnapshot
}
type requestHealthSnapshot struct {
	at       time.Time
	overload bool
	clients  [3]bool
}

func (h *requestHealth) start(now time.Time) {
	if h.started.IsZero() {
		h.started = now
	}
}
func (h *requestHealth) recordClient(now time.Time, kind string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.start(now)
	for i, k := range requestHealthKinds {
		if kind == k {
			h.clients[i]++
			return
		}
	}
}
func (h *requestHealth) recordRequest(now time.Time, route string, duration, wait time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.start(now)
	if h.costs == nil {
		h.costs = map[string]requestHealthCost{}
	}
	if h.routes == nil {
		h.routes = map[string]*requestHealthRoute{}
	}
	b := h.costs[route]
	b.count++
	// Long-poll duration is intentional; its actual mutex wait remains evidence.
	if route != "GET /api/workers/{id}/events" && route != "POST /api/wait" {
		b.duration += max(0, duration)
	}
	b.wait += max(0, wait)
	h.costs[route] = b
	if h.routes[route] == nil {
		h.routes[route] = &requestHealthRoute{}
	}
}
func (h *requestHealth) snapshot(now time.Time) requestHealthSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started.IsZero() || !now.After(h.started) {
		s := h.result
		s.at = now
		return s
	}
	elapsed := now.Sub(h.started).Seconds()
	bad := false
	for route, b := range h.costs {
		base := h.routes[route]
		duration, wait := float64(b.duration)/float64(b.count), float64(b.wait)/float64(b.count)
		// Compare like routes: an expensive inspection cannot redefine check-in
		// latency. A latency regression or a queue-wait regression is degradation.
		adverse := base.duration.high(duration) || base.wait.high(wait)
		bad = bad || adverse
		if !adverse {
			base.duration.add(duration)
			base.wait.add(wait)
		}
	}
	var excess [3]float64
	var allExcess, totalBaseline float64
	for _, baseline := range h.rates {
		if len(baseline) >= alertWarmup {
			_, hi := baseline.band()
			totalBaseline += max(0, hi)
		}
	}
	for i, count := range h.clients {
		rate := float64(count) / elapsed
		if h.rates[i].high(rate) {
			_, hi := h.rates[i].band()
			scale := hi
			if scale <= 0 {
				scale = totalBaseline
			}
			if scale <= 0 {
				scale = rate
			}
			// Relative growth preserves the normal mix: scaling a 99:1 workload
			// proportionally must not blame the already dominant family.
			excess[i] = (rate - hi) / scale
			allExcess += excess[i]
		}
	}

	if bad {
		h.adverse++
		h.recovering = 0
	} else {
		h.adverse = 0
		h.recovering++
	}
	if h.adverse >= alertConfirmation {
		h.active = true
	}
	if h.recovering >= alertConfirmation || len(h.costs) == 0 {
		h.active = false
	}
	s := requestHealthSnapshot{at: now, overload: h.active}
	for i, count := range h.clients {
		// Attribute only under degradation, to a strict majority of excess demand.
		// Half is the part/whole majority identity, not a tuned dominance threshold.
		// Proportional growth of a balanced mix has no unique causal family.
		if h.active && bad && excess[i] > allExcess-excess[i] {
			s.clients[i] = true
		}
		if !bad {
			h.rates[i].add(float64(count) / elapsed)
		}
	}
	h.started = now
	h.costs = map[string]requestHealthCost{}
	h.clients = [3]uint64{}
	h.result = s
	return s
}

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
