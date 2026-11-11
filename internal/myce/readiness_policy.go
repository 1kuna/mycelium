package myce

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"
)

// The unmeasured transport seed is RFC 6298's initial retransmission timeout.
// It is not a load deadline. Completed local observations replace it; failed
// observations back off without a ceiling, retaining custody throughout.
const readinessInitialRTT = time.Second

type readinessLatency struct {
	Mean      time.Duration `json:"mean,omitempty"`
	Deviation time.Duration `json:"deviation,omitempty"`
}

func (l *readinessLatency) observe(d time.Duration) {
	d = max(d, time.Nanosecond)
	if l.Mean == 0 {
		l.Mean, l.Deviation = d, d/2
		return
	}
	delta := l.Mean - d
	if delta < 0 {
		delta = -delta
	}
	// RFC 6298 smoothing and deviation allowance track changing local latency
	// without storing an unbounded sample history or choosing fleet durations.
	l.Deviation = l.Deviation*3/4 + delta/4
	l.Mean = l.Mean*7/8 + d/8
}

func (l readinessLatency) budget() time.Duration {
	if l.Mean == 0 {
		return readinessInitialRTT
	}
	return addReadinessDuration(l.Mean, max(time.Nanosecond, scaleReadinessDuration(l.Deviation, 4)))
}

// Timeouts are lower bounds on latency, not completed samples. Recovery probes
// expand their allowance until a response can teach the new local envelope.
type readinessProbe struct {
	latency     readinessLatency
	uncertainty time.Duration
}

func (p *readinessProbe) budget() time.Duration { return max(p.latency.budget(), p.uncertainty) }
func (p *readinessProbe) observe(elapsed time.Duration, timedOut bool) {
	if timedOut {
		p.uncertainty = scaleReadinessDuration(max(p.budget(), elapsed), 2)
		return
	}
	p.latency.observe(elapsed)
	p.uncertainty = 0
}

func (l readinessLatency) cadence() time.Duration {
	if l.Mean == 0 {
		return readinessInitialRTT
	}
	return max(time.Nanosecond, l.Mean)
}

func scaleReadinessDuration(d time.Duration, n int64) time.Duration {
	if d > time.Duration(math.MaxInt64/n) {
		return time.Duration(math.MaxInt64)
	}
	return d * time.Duration(n)
}
func addReadinessDuration(a, b time.Duration) time.Duration {
	if a > time.Duration(math.MaxInt64)-b {
		return time.Duration(math.MaxInt64)
	}
	return a + b
}

// One successful operation's time is also the next idle between probes: serial
// probes cannot consume more time than they leave for the service being probed.
func readinessPause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(max(time.Nanosecond, duration))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type readinessHistory struct {
	Load readinessLatency `json:"load"`
	Gap  time.Duration    `json:"gap"`
}

type readinessProgress struct {
	activitySource   func() (time.Time, []*float64)
	activityAt       time.Time
	activityPeriod   time.Duration
	osObserver       bool
	activityValues   []float64
	activityRevision uint64
	activitySamples  int
	inactive         bool
	source           func() int64
	sourceValue      int64
	suspended        bool
	realStarted      time.Time
	mu               sync.Mutex
	clock            Clock
	started, last    time.Time
	realLast         time.Time // transport I/O must also terminate when a test Clock is stationary
	gap              time.Duration
	budget           time.Duration
	revision         uint64
	stage            int
	completed        float64
}

func newReadinessProgress(clock Clock, history readinessHistory) *readinessProgress {
	if clock == nil {
		clock = RealClock{}
	}
	now := clock.Now()
	budget := time.Duration(0)
	if history.Load.Mean > 0 {
		budget = max(history.Load.budget(), history.Gap)
	}
	return &readinessProgress{clock: clock, started: now, last: now, realLast: RealClock{}.Now(), realStarted: RealClock{}.Now(), budget: budget}
}

func (p *readinessProgress) advance() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	if !p.suspended {
		p.gap = max(p.gap, now.Sub(p.last), RealClock{}.Now().Sub(p.realLast))
	}
	p.last = now
	p.realLast = RealClock{}.Now()
	p.revision++
	p.inactive = false
}

// pause excludes control-plane waits from model latency and silence. Nested
// launch gates preserve an enclosing preparation phase's suspension.
func (p *readinessProgress) pause() func() {
	p.mu.Lock()
	wasSuspended := p.suspended
	pausedAt, realPausedAt := p.clock.Now(), RealClock{}.Now()
	p.suspended = true
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		p.last, p.realLast = p.clock.Now(), RealClock{}.Now()
		if !wasSuspended {
			p.started = p.started.Add(p.last.Sub(pausedAt))
			p.realStarted = p.realStarted.Add(p.realLast.Sub(realPausedAt))
		}
		p.suspended = wasSuspended
		p.mu.Unlock()
	}
}

func (p *readinessProgress) state() (time.Duration, bool) {
	p.mu.Lock()
	source := p.source
	activitySource := p.activitySource
	p.mu.Unlock()
	if source != nil {
		value := source()
		p.mu.Lock()
		advanced := value > p.sourceValue
		p.sourceValue = max(value, p.sourceValue)
		p.mu.Unlock()
		if advanced {
			p.advance()
		}
	}
	if activitySource != nil {
		at, values := activitySource()
		p.mu.Lock()
		fresh := !at.IsZero() && !at.Equal(p.activityAt)
		if fresh {
			p.activityAt = at
		}
		p.mu.Unlock()
		if fresh {
			p.observeActivity(values...)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Allow a repeat of the longest observed inter-progress gap, plus its
	// measurement uncertainty (one more gap). No whole-load wall deadline.
	budget := p.budget
	if budget > 0 {
		budget = max(budget, scaleReadinessDuration(p.gap, 2), scaleReadinessDuration(p.activityPeriod, 2))
	}
	return budget, budget > 0 && !p.suspended && max(p.clock.Now().Sub(p.last), RealClock{}.Now().Sub(p.realLast)) >= budget
}

func (p *readinessProgress) result() readinessHistory {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := readinessHistory{Gap: max(p.gap, p.clock.Now().Sub(p.last), RealClock{}.Now().Sub(p.realLast))}
	h.Load.observe(max(p.clock.Now().Sub(p.started), RealClock{}.Now().Sub(p.realStarted)))
	return h
}

// A repeated status/log line is not progress. Servers can emit this small
// monotonic record on captured stdout. Each stage is identified by
// an increasing numeric index; counters cannot regress within a stage.
func (p *readinessProgress) record(line []byte) {
	var v struct {
		Stage     int     `json:"stage"`
		Completed float64 `json:"completed"`
		Total     float64 `json:"total"`
	}
	if json.Unmarshal(line, &v) != nil || v.Stage < 1 || !validMemoryValue(v.Completed) || !validMemoryValue(v.Total) || v.Total <= 0 || v.Completed > v.Total {
		return
	}
	p.mu.Lock()
	stage := v.Stage
	advance := stage > p.stage || stage == p.stage && v.Completed > p.completed
	if advance {
		p.stage, p.completed = stage, v.Completed
	}
	p.mu.Unlock()
	if advance {
		p.advance()
	}
}

type readinessProgressKey struct{}

func readinessFromContext(ctx context.Context) *readinessProgress {
	p, _ := ctx.Value(readinessProgressKey{}).(*readinessProgress)
	return p
}
