package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/bits"
	"net/http"
	"sort"
	"sync"
	"time"
)

type requestSpanKey struct{}

// Wall time here is strictly observational, never used for queue decisions.
// A span belongs to one synchronous handler, not the queue or a goroutine ID.
type requestSpan struct {
	mu         sync.Mutex
	finished   bool
	started    time.Time
	wait, hold time.Duration
	writerWait time.Duration
	lastUnlock time.Time
	measured   bool
	submit     *submitRequestTiming
}

// Opt-in burst attribution. Wall timestamps are observations, never inputs to
// admission or scheduling. Durations are disjoint; work is the remainder.
type submitRequestTiming struct {
	Name                                                                string
	Started, Finished                                                   time.Time
	Admission, Decode, Wait, Hold, SQLiteCommit, Response, EvidenceWait time.Duration
}

func submitPhase(ctx context.Context, phase string) func() {
	s := spanFrom(ctx)
	if s == nil || s.submit == nil {
		return func() {}
	}
	started := time.Now()
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch phase {
		case "admission":
			s.submit.Admission += time.Since(started)
		case "decode":
			s.submit.Decode += time.Since(started)
		case "response":
			s.submit.Response += time.Since(started)
		case "evidence":
			s.submit.EvidenceWait += time.Since(started)
		}
	}
}

func spanFrom(ctx context.Context) *requestSpan {
	s, _ := ctx.Value(requestSpanKey{}).(*requestSpan)
	return s
}

func requestLock(ctx context.Context, mu sync.Locker) func() {
	s := spanFrom(ctx)
	if s == nil {
		mu.Lock()
		return mu.Unlock
	}
	started := time.Now()
	mu.Lock()
	locked := time.Now()
	return func() {
		end := time.Now()
		mu.Unlock()
		s.observe(locked.Sub(started), end.Sub(locked), end)
	}
}

// Observations own no cleanup transaction and can abandon a queued lock wait.
func requestObservationLock(ctx context.Context, mu sync.Locker) (func(), error) {
	timing := &queueUpdateTiming{}
	if err := timing.lockContext(ctx, mu); err != nil {
		return nil, err
	}
	// Response-cache refresh locks serialize off-lock builds, not queue work.
	// Including their lifetime here falsely attributes an entire status/job
	// projection (and waits for another projection) to the live queue mutex.
	if _, queue := mu.(*queueMutex); !queue {
		return mu.Unlock, nil
	}
	return func() { unlockTimedRequest(ctx, mu, timing) }, nil
}

// Duration representation bounds aggregate storage even for unmatched requests.
// Only ServeMux patterns enter this map; paths, queries and bodies never do.
type requestLatencies struct {
	mu     sync.Mutex
	routes map[string]*requestLatencySamples
}

// One bin per bit of a nanosecond duration: bounded by time.Duration's
// representation, independent of request volume or installation size. Exact
// observed maxima bound each bin; there is no raw-sample ring to overflow.
type requestLatencySamples struct {
	counts    [64]uint64
	maxima    [64]time.Duration
	count     uint64
	telemetry *telemetryBudget
}

type RequestLatency struct {
	Route   string  `json:"route"`
	Count   uint64  `json:"count"`
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
}

func (l *requestLatencies) record(route string, duration time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.routes == nil {
		l.routes = make(map[string]*requestLatencySamples)
	}
	v := l.routes[route]
	if v == nil {
		v = &requestLatencySamples{telemetry: &telemetryBudget{}}
		l.routes[route] = v
	}
	bin := bits.Len64(uint64(max(0, duration)))
	if bin > 0 {
		bin--
	}
	v.counts[bin]++
	v.maxima[bin] = max(v.maxima[bin], duration)
	v.count++
	return v.telemetry.admit(duration)
}

// p99 aggregates every completed route except one (the liveness probe itself),
// using the same exact per-bin maxima as snapshot.
func (l *requestLatencies) p99(exclude string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	var counts [64]uint64
	var maxima [64]time.Duration
	var total uint64
	for route, v := range l.routes {
		if route == exclude {
			continue
		}
		for i := range counts {
			counts[i] += v.counts[i]
			maxima[i] = max(maxima[i], v.maxima[i])
		}
		total += v.count
	}
	target := uint64(math.Ceil(.99 * float64(total)))
	var count uint64
	for i, n := range counts {
		count += n
		if total > 0 && count >= target {
			return maxima[i]
		}
	}
	return 0
}

func (l *requestLatencies) snapshot() []RequestLatency {
	l.mu.Lock()
	copies := make(map[string]requestLatencySamples, len(l.routes))
	for route, v := range l.routes {
		copies[route] = requestLatencySamples{counts: v.counts, maxima: v.maxima, count: v.count}
	}
	l.mu.Unlock()
	out := make([]RequestLatency, 0, len(copies))
	for route, v := range copies {
		percentile := func(p float64) float64 {
			target := uint64(math.Ceil(p * float64(v.count)))
			var count uint64
			for i, n := range v.counts {
				count += n
				if count >= target {
					return float64(v.maxima[i]) / float64(time.Millisecond)
				}
			}
			return 0
		}
		out = append(out, RequestLatency{Route: route, Count: v.count, Samples: int(min(v.count, uint64(math.MaxInt))), P50MS: percentile(.5), P95MS: percentile(.95), P99MS: percentile(.99)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Route < out[j].Route })
	return out
}

type requestResponse struct {
	http.ResponseWriter
	status int
}

func (w *requestResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *requestResponse) WriteHeader(status int) {
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *requestResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
func (w *requestResponse) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (q *Queue) measureRequests(next http.Handler, registered ...map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := &requestSpan{started: time.Now()}
		if q.observeSubmit != nil && r.Method == http.MethodPost && r.URL.Path == "/api/jobs" {
			s.submit = &submitRequestTiming{Started: s.started}
		}
		r = r.WithContext(context.WithValue(r.Context(), requestSpanKey{}, s))
		response := &requestResponse{ResponseWriter: w}
		defer func() {
			ended := time.Now()
			s.mu.Lock()
			s.finished = true
			wait, hold, lastUnlock, measured := s.wait, s.hold, s.lastUnlock, s.measured
			writerWait := s.writerWait
			var submit submitRequestTiming
			if s.submit != nil {
				submit = *s.submit
				submit.Finished, submit.Wait, submit.Hold = ended, wait, hold
			}
			s.mu.Unlock()
			if s.submit != nil {
				q.observeSubmit(submit)
			}
			duration := ended.Sub(s.started)
			route := "unmatched"
			if len(registered) > 0 {
				if _, ok := registered[0][r.Pattern]; ok {
					route = r.Pattern
				}
			}
			if q.observeRequest != nil {
				after := time.Duration(0)
				if !lastUnlock.IsZero() {
					after = ended.Sub(lastUnlock)
				}
				q.observeRequest(route, duration, wait, hold, after)
			}
			if route == "POST /api/workers/checkin" && response.status < 400 && measured {
				// Occupied queue work excludes body reads, reply writes and
				// mutex acquisition. Slow peers cannot redefine queue latency.
				q.requestBudgets.recordControl(max(0, hold-writerWait))
			}
			slow := q.requestLatencies.record(route, duration)
			q.requestHealth.recordRequest(q.clock.Now(), route, duration, wait)
			if !slow {
				return
			}
			status := response.status
			if status == 0 {
				status = http.StatusOK
			}
			after := time.Duration(0)
			if !lastUnlock.IsZero() {
				after = ended.Sub(lastUnlock)
			}
			phases := ""
			if route == "GET /api/fleet/instruction" {
				// Role only: hosts stay out of slow lines (#474); the idle-request
				// event names host and machine.
				phases = fmt.Sprintf(" writer_wait_ms=%.3f work_ms=%.3f caller=%s", float64(writerWait)/float64(time.Millisecond), float64(max(0, hold-writerWait))/float64(time.Millisecond), fleetInstructionCaller(r.URL.Query()))
			}
			logStarted := time.Now()
			log.Printf("event=slow_request route=%q status=%d duration_ms=%.3f mutex_measured=%t mutex_wait_ms=%.3f mutex_hold_ms=%.3f after_unlock_ms=%.3f%s", route, status, float64(duration)/float64(time.Millisecond), measured, float64(wait)/float64(time.Millisecond), float64(hold)/float64(time.Millisecond), float64(after)/float64(time.Millisecond), phases)
			q.requestLatencies.mu.Lock()
			budget := q.requestLatencies.routes[route].telemetry
			q.requestLatencies.mu.Unlock()
			budget.spent(time.Since(logStarted))
		}()
		next.ServeHTTP(response, r)
	})
}

func unlockTimedRequest(ctx context.Context, mu sync.Locker, timing *queueUpdateTiming) {
	s := spanFrom(ctx)
	if s == nil {
		mu.Unlock()
		return
	}
	end := time.Now()
	mu.Unlock()
	s.observe(timing.mutexWait, end.Sub(timing.lockedAt), end)
	if s.submit != nil {
		s.mu.Lock()
		s.submit.SQLiteCommit += timing.sqlCommit
		s.mu.Unlock()
	}
	if timing.measureWriter {
		s.mu.Lock()
		if !s.finished {
			s.writerWait += timing.writerWait
		}
		s.mu.Unlock()
	}
}

func requestContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 {
		return contexts[0]
	}
	return context.Background()
}

func requestReadLock(ctx context.Context, mu interface {
	RLock()
	RUnlock()
}) func() {
	s := spanFrom(ctx)
	if s == nil {
		mu.RLock()
		return mu.RUnlock
	}
	started := time.Now()
	mu.RLock()
	locked := time.Now()
	return func() {
		end := time.Now()
		mu.RUnlock()
		s.observe(locked.Sub(started), end.Sub(locked), end)
	}
}

// Model forwarding can observe queue state concurrently with the handler.
// Aggregate after releasing q.mu and ignore work that outlives the response.
func (s *requestSpan) observe(wait, hold time.Duration, unlocked time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	s.wait += wait
	s.hold += hold
	if unlocked.After(s.lastUnlock) {
		s.lastUnlock = unlocked
	}
	s.measured = true
}

// ServeMux can return a concrete redirect path as its pattern (notably for
// CONNECT). Record the actual registration strings and whitelist at completion,
// after dispatch has had its final opportunity to overwrite Request.Pattern.
// Registration finishes before any request is served; the map is read-only then.
type requestMux struct {
	*http.ServeMux
	patterns map[string]struct{}
}

func newRequestMux() *requestMux {
	return &requestMux{ServeMux: http.NewServeMux(), patterns: make(map[string]struct{})}
}
func (m *requestMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.ServeMux.HandleFunc(pattern, handler)
	m.patterns[pattern] = struct{}{}
}

// Only selected-job projection and its wire encoding teach row cost. Fleet
// diagnostics, disk reads, SQL usage history and mutex waiting are fixed or
// independent request work, not marginal costs of another history row.
type statusProjectionSample struct {
	budgets *requestBudgets
	elapsed time.Duration
}

func startStatusProjection(budgets []*requestBudgets) *statusProjectionSample {
	if len(budgets) == 0 {
		return nil
	}
	return &statusProjectionSample{budgets: budgets[0]}
}
func (sample *statusProjectionSample) measure() func() {
	if sample == nil {
		return func() {}
	}
	started := time.Now()
	return func() { sample.elapsed += time.Since(started) }
}
func (sample *statusProjectionSample) finish(jobs []*Job) {
	if sample == nil {
		return
	}
	done := sample.measure()
	data, err := json.Marshal(jobs)
	done()
	if err == nil {
		sample.budgets.recordProjection(len(jobs), len(data), sample.elapsed)
	}
}
