package myce

import (
	"context"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

const requestSlowThreshold = time.Second
const requestLatencyWindow = 256
const requestLatencyRoutes = 128

type requestSpanKey struct{}

// Wall time here is strictly observational, never used for queue decisions.
// A span belongs to one synchronous handler, not the queue or a goroutine ID.
type requestSpan struct {
	mu         sync.Mutex
	finished   bool
	started    time.Time
	wait, hold time.Duration
	lastUnlock time.Time
	measured   bool
}

func spanFrom(ctx context.Context) *requestSpan {
	s, _ := ctx.Value(requestSpanKey{}).(*requestSpan)
	return s
}

func requestLock(ctx context.Context, mu *sync.RWMutex) func() {
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

// Fixed samples and a fixed route cap bound memory even for unmatched requests.
// Only ServeMux patterns enter this map; paths, queries and bodies never do.
type requestLatencies struct {
	mu     sync.Mutex
	routes map[string]*requestLatencySamples
}
type requestLatencySamples struct {
	values     [requestLatencyWindow]time.Duration
	next, size int
	count      uint64
}

type RequestLatency struct {
	Route   string  `json:"route"`
	Count   uint64  `json:"count"`
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
}

func (l *requestLatencies) record(route string, duration time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.routes == nil {
		l.routes = make(map[string]*requestLatencySamples)
	}
	v := l.routes[route]
	if v == nil {
		if len(l.routes) >= requestLatencyRoutes {
			return
		}
		v = &requestLatencySamples{}
		l.routes[route] = v
	}
	v.values[v.next] = duration
	v.next = (v.next + 1) % requestLatencyWindow
	v.size = min(v.size+1, requestLatencyWindow)
	v.count++
}

func (l *requestLatencies) snapshot() []RequestLatency {
	l.mu.Lock()
	copies := make(map[string]requestLatencySamples, len(l.routes))
	for route, v := range l.routes {
		copies[route] = *v
	}
	l.mu.Unlock()
	out := make([]RequestLatency, 0, len(copies))
	for route, v := range copies {
		values := v.values[:v.size]
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		percentile := func(p float64) float64 {
			return float64(values[int(math.Ceil(p*float64(v.size)))-1]) / float64(time.Millisecond)
		}
		out = append(out, RequestLatency{Route: route, Count: v.count, Samples: v.size, P50MS: percentile(.5), P95MS: percentile(.95), P99MS: percentile(.99)})
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

func (q *Queue) measureRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := &requestSpan{started: time.Now()}
		r = r.WithContext(context.WithValue(r.Context(), requestSpanKey{}, s))
		response := &requestResponse{ResponseWriter: w}
		defer func() {
			ended := time.Now()
			s.mu.Lock()
			s.finished = true
			wait, hold, lastUnlock, measured := s.wait, s.hold, s.lastUnlock, s.measured
			s.mu.Unlock()
			duration := ended.Sub(s.started)
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			q.requestLatencies.record(route, duration)
			if duration <= requestSlowThreshold {
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
			log.Printf("event=slow_request route=%q status=%d duration_ms=%.3f mutex_measured=%t mutex_wait_ms=%.3f mutex_hold_ms=%.3f after_unlock_ms=%.3f", route, status, float64(duration)/float64(time.Millisecond), measured, float64(wait)/float64(time.Millisecond), float64(hold)/float64(time.Millisecond), float64(after)/float64(time.Millisecond))
		}()
		next.ServeHTTP(response, r)
	})
}

func unlockTimedRequest(ctx context.Context, mu *sync.RWMutex, timing *queueUpdateTiming) {
	s := spanFrom(ctx)
	if s == nil {
		mu.Unlock()
		return
	}
	end := time.Now()
	mu.Unlock()
	s.observe(timing.mutexWait, end.Sub(timing.lockedAt), end)
}

func requestContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 {
		return contexts[0]
	}
	return context.Background()
}

func requestReadLock(ctx context.Context, mu *sync.RWMutex) func() {
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
