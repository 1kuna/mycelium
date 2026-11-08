package myce

import (
	"context"
	"math"
	"reflect"
	"runtime"
	"time"
)

const hostProbeProtocol = "host-probe-v1"

// ProbeObservation describes one completed observation, not a renewable lease.
// Age is carried by the containing signal. No worker wall timestamp is trusted.
type ProbeObservation struct {
	PeriodSeconds float64 `json:"period_seconds"`
	CostSeconds   float64 `json:"cost_seconds"`
	ValidSeconds  float64 `json:"valid_seconds"`
}

func (p *ProbeObservation) valid() bool {
	return p != nil && validMemoryValue(p.PeriodSeconds) && p.PeriodSeconds > 0 &&
		validMemoryValue(p.CostSeconds) && p.CostSeconds > 0 &&
		validMemoryValue(p.ValidSeconds) && p.ValidSeconds >= p.PeriodSeconds+p.CostSeconds &&
		p.ValidSeconds < float64(math.MaxInt64)/float64(time.Second)
}

func (p *ProbeObservation) freshness() time.Duration {
	if p.valid() {
		return time.Duration(p.ValidSeconds * float64(time.Second))
	}
	// Legacy protocol only: old workers provide no measured timing evidence.
	return signalMaxAge
}

// The RFC 6298 estimator separates measured service cost from its variation.
// 1/8, 1/4 and four deviations are statistical smoothing/prediction coefficients,
// not durations tuned to a particular machine. State uses constant memory.
type probeEstimate struct {
	Mean      float64 `json:"mean"`
	Deviation float64 `json:"deviation"`
}

func (e *probeEstimate) observe(v float64) {
	if e.Mean == 0 {
		e.Mean, e.Deviation = v, v/2
		return
	}
	e.Deviation += (math.Abs(v-e.Mean) - e.Deviation) / 4
	e.Mean += (v - e.Mean) / 8
}
func (e probeEstimate) upper() float64 { return e.Mean + 4*e.Deviation }

type probeTiming struct {
	base       time.Duration // Existing worker control cadence, never a new polling SLA.
	cores      int
	cost       probeEstimate
	delivery   probeEstimate // Measured consumer/report gap, when the lane crosses that path.
	reported   time.Time
	lastChange time.Time
	change     probeEstimate
	next       time.Time
}

func (p *probeTiming) noteReport(now time.Time) {
	if !p.reported.IsZero() && now.After(p.reported) {
		// Includes the previous queue round trip and this report's preparation.
		// Only subsequently completed observations may use the prediction.
		p.delivery.observe(now.Sub(p.reported).Seconds())
	}
	p.reported = now
}

func (p *probeTiming) controlPeriod() time.Duration {
	if p.base > 0 {
		return p.base
	}
	return time.Second // The worker protocol's default heartbeat period.
}

func (p *probeTiming) observe(start, end time.Time, changed bool) *ProbeObservation {
	// Nanoseconds are time.Duration's representation resolution; a zero-cost
	// fake probe must not create a zero timer or a divide by zero.
	cost := max(time.Nanosecond, end.Sub(start)).Seconds()
	p.cost.observe(cost)
	if p.lastChange.IsZero() {
		p.lastChange = start
	}
	if changed && start.After(p.lastChange) {
		p.change.observe(start.Sub(p.lastChange).Seconds())
		p.lastChange = start
	}
	cores := p.cores
	if cores <= 0 {
		cores = runtime.NumCPU()
	}
	// Balance measurement CPU cost against time blind to a changing signal.
	// The geometric mean grows during quiet periods, but measured changes
	// constrain it to half their interval (two observations per change).
	stable := max(cost, end.Sub(p.lastChange).Seconds())
	period := max(p.controlPeriod().Seconds(), cost*(1+1/float64(cores)), math.Sqrt(cost*stable/float64(cores)))
	if p.change.Mean > 0 {
		period = max(p.controlPeriod().Seconds(), cost, min(period, p.change.Mean/2))
	}
	valid := period + max(cost, p.cost.upper()) + p.delivery.upper()
	result := &ProbeObservation{PeriodSeconds: period, CostSeconds: cost, ValidSeconds: valid}
	p.next = start.Add(time.Duration(period * float64(time.Second)))
	return result
}

type hostProbeResult[T any] struct {
	value      T
	err        error
	start, end time.Time
}

// One outstanding OS call per lane. A slow/uninterruptible probe never stalls
// control, grows a goroutine backlog, or renews the previous observation.
// The lane owner serializes step; source work holds no queue/snapshot mutex.
type hostProbeLane[T any] struct {
	clock       Clock
	timing      probeTiming
	discard     bool
	pending     <-chan hostProbeResult[T]
	done        <-chan struct{}
	value       T
	err         error
	known       bool
	at          time.Time
	observation *ProbeObservation
}

func (p *hostProbeLane[T]) step(ctx context.Context, source func(context.Context) (T, error)) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	completed := false
	var err error
	if p.pending != nil {
		select {
		case r := <-p.pending:
			p.pending, p.done, completed, err = nil, nil, true, r.err
			if p.discard {
				p.discard = false
				p.known = false
				return false, nil
			}
			p.observation = p.timing.observe(r.start, r.end, !p.known || !reflect.DeepEqual(r.value, p.value) || r.err != nil)
			p.known = r.err == nil && !r.end.Before(r.start)
			p.value, p.at, p.err = r.value, r.start, r.err
		default:
		}
	}
	if p.pending == nil && !p.clock.Now().Before(p.timing.next) {
		out := make(chan hostProbeResult[T], 1)
		done := make(chan struct{})
		p.done = done
		p.pending = out
		start := p.clock.Now()
		go func() {
			value, err := source(ctx)
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			out <- hostProbeResult[T]{value, err, start, p.clock.Now()}
			close(done)
		}()
	}
	return completed, err
}

func (p *hostProbeLane[T]) fresh(now time.Time) bool {
	return p.known && p.observation.valid() && !now.Before(p.at) && now.Sub(p.at) < p.observation.freshness()
}

// Owner withdrawal includes the model-control lane, which consumes local
// availability every 100ms. Faster probing cannot improve that delivery
// resolution; this is a runtime mechanism, not a calibrated host freshness age.
func hostProbeControlPeriod(interval time.Duration) time.Duration {
	return max(interval, 100*time.Millisecond)
}

// waitDuration wakes at the measured due time, rather than rounding up to a
// control tick. While an OS call is pending, control still checks expiry.
func (p *hostProbeLane[T]) waitDuration() time.Duration {
	wait := p.timing.controlPeriod()
	if p.pending == nil && p.clock != nil {
		return min(wait, max(0, p.timing.next.Sub(p.clock.Now())))
	}
	return wait
}

// Include time spent preparing a check-in or waiting behind queue transactions.
// Worker timestamps are never trusted; receipt and processing use Queue.Clock.
func ageHostObservations(m *Machine, received, processed time.Time) {
	elapsed := processed.Sub(received).Seconds()
	if elapsed < 0 {
		if a := machineAvailability(m); a != nil && a.Probe != nil {
			a.AgeSeconds = -1
		}
		if p := m.SharedHostPressure; p != nil {
			p.AgeSeconds = -1
		}
		if h := m.HostHealth; h != nil && h.Probe != nil {
			h.AgeSeconds = -1
		}
		return
	}
	if a := machineAvailability(m); a != nil && a.Probe != nil {
		a.AgeSeconds += elapsed
	}
	if p := m.SharedHostPressure; p != nil {
		p.AgeSeconds += elapsed
	}
	if h := m.HostHealth; h != nil && h.Probe != nil {
		h.AgeSeconds += elapsed
	}
}
