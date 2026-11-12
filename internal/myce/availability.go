package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Legacy observation protocol: retained only for workers without timing evidence.
const signalRefresh = 5 * time.Second
const signalMaxAge = 15 * time.Second

// OwnerSignals contains only aggregate OS state, never input or screen contents.
// Power is desktop (no battery), ac, battery, or unknown. Heat is nominal, fair,
// serious, critical, or unknown. Laptop stays true even if its power read fails.
type OwnerSignals struct {
	GPUHeat      string  `json:"gpu_heat,omitempty"`
	HeatCoverage string  `json:"heat_coverage,omitempty"`
	IdleSeconds  float64 `json:"idle_seconds"`
	IdleKnown    bool    `json:"idle_known"`
	Laptop       bool    `json:"laptop"`
	Power        string  `json:"power"`
	Heat         string  `json:"heat"`
}
type OwnerSignalSource func(context.Context) OwnerSignals

type Availability struct {
	AgeSeconds float64           `json:"age_seconds,omitempty"`
	Probe      *ProbeObservation `json:"probe,omitempty"`
	Mode       string            `json:"mode"`
	Available  bool              `json:"available"`
	Reason     string            `json:"reason,omitempty"`
	CheckedAt  time.Time         `json:"checked_at"`
	Signals    OwnerSignals      `json:"signals"`
}

func (a Availability) Summary() string {
	state := "available"
	if !a.Available {
		state = "waiting: " + a.Reason
	}
	return fmt.Sprintf("%s; %s; idle known=%t %.0fs, power=%s, heat=%s", a.Mode, state, a.Signals.IdleKnown, a.Signals.IdleSeconds, a.Signals.Power, a.Signals.Heat)
}

// availabilityPolicy is driven only by the injected Clock. A missing reading
// breaks the cooling dwell; a sampling gap after sleep breaks the idle window.
type availabilityPolicy struct {
	clock        Clock
	daily        bool
	laptop       bool
	desktop      bool
	last         time.Time
	awakeSince   time.Time
	hot          bool
	coolSince    time.Time
	heatRecovery time.Duration
	heatBounces  probeEstimate
	inputAt      time.Time
	inputPauses  probeEstimate
	probe        *ProbeObservation
}

func (p *availabilityPolicy) evaluate(s OwnerSignals) Availability {
	if math.IsNaN(s.IdleSeconds) || math.IsInf(s.IdleSeconds, 0) || s.IdleSeconds < 0 {
		s.IdleKnown = false
		s.IdleSeconds = 0
	}
	p.laptop = p.laptop || s.Laptop
	s.Laptop = p.laptop
	now := p.clock.Now()
	a := Availability{Mode: "dedicated", Available: true, CheckedAt: now, Signals: s, Probe: p.probe}
	resolution := time.Second
	if p.probe.valid() {
		resolution = time.Duration(p.probe.PeriodSeconds * float64(time.Second))
	}
	freshFor := p.probe.freshness()
	if p.daily {
		a.Mode = "daily"
	}
	if !p.last.IsZero() && (now.UnixNano()-p.last.UnixNano() > int64(freshFor) || now.Before(p.last)) {
		p.awakeSince = now
		p.inputAt = time.Time{}
		p.coolSince = time.Time{}
	}
	p.last = now
	wait := func(reason string) Availability { a.Available = false; a.Reason = reason; return a }
	heatReason := ""
	// Only an explicit desktop declaration permits the CPU waiver.
	// --laptop and detected battery hardware remain sticky vetoes.
	desktopWaiver := p.desktop && !p.laptop && (s.Power == "ac" || s.Power == "desktop") && s.HeatCoverage == "gpu-only"
	heat := s.Heat
	if desktopWaiver {
		heat = s.GPUHeat
	}
	if p.daily {
		switch heat {
		case "fair", "serious", "critical":
			if p.hot && !p.coolSince.IsZero() && now.After(p.coolSince) {
				p.heatBounces.observe(now.Sub(p.coolSince).Seconds())
			}
			p.hot = true
			p.coolSince = time.Time{}
			heatReason = "cooling down"
		case "nominal":
			if p.hot {
				if p.coolSince.IsZero() {
					p.coolSince = now
					p.heatRecovery = max(resolution, time.Duration(p.heatBounces.upper()*float64(time.Second)))
				}
				if now.Sub(p.coolSince) < p.heatRecovery {
					heatReason = "cooling down"
				} else {
					p.hot = false
					p.coolSince = time.Time{}
				}
			}
		default:
			p.coolSince = time.Time{}
			heatReason = "heat sensing unavailable"
			if s.HeatCoverage == "gpu-only" && !desktopWaiver {
				heatReason = "host CPU thermal coverage unavailable"
			}
		}
	}
	// Dedicated servers ignore presence and heat. Known battery-equipped machines
	// still need AC; --laptop makes this fail closed even if detection fails.
	if p.daily || p.laptop || s.Laptop || s.Power == "battery" {
		if s.Power != "desktop" && s.Power != "ac" {
			if s.Power == "battery" {
				return wait("on battery")
			}
			return wait("power source unavailable")
		}
		if (p.laptop || s.Laptop) && s.Power != "ac" {
			return wait("external power required")
		}
	}
	if !p.daily {
		return a
	}
	if !s.IdleKnown || math.IsNaN(s.IdleSeconds) || math.IsInf(s.IdleSeconds, 0) || s.IdleSeconds < 0 {
		return wait("owner activity unavailable")
	}
	// Input timestamps reveal pauses in this owner's active session. Do not
	// teach an overnight idle interval as the next within-session pause.
	inputAt := now.Add(-time.Duration(s.IdleSeconds * float64(time.Second)))
	idleWait := max(resolution, time.Duration(p.inputPauses.upper()*float64(time.Second)))
	if !p.inputAt.IsZero() && inputAt.After(p.inputAt) && inputAt.Sub(p.inputAt) <= idleWait {
		p.inputPauses.observe(inputAt.Sub(p.inputAt).Seconds())
	}
	if p.inputAt.IsZero() || inputAt.After(p.inputAt) {
		p.inputAt = inputAt
	}
	idleWait = max(resolution, time.Duration(p.inputPauses.upper()*float64(time.Second)))
	if s.IdleSeconds < idleWait.Seconds() || !p.awakeSince.IsZero() && now.Sub(p.awakeSince) < idleWait {
		return wait("waiting for owner idle recovery")
	}
	if heatReason != "" {
		return wait(heatReason)
	}
	if desktopWaiver {
		a.Reason = "CPU thermal coverage waived for desktop"
	}
	return a
}

// Independent of uploads/check-ins so owner return also interrupts offline work.
type availabilityMonitor struct {
	mu      sync.Mutex
	policy  availabilityPolicy
	current Availability
	work    context.Context
	cancel  context.CancelFunc
	lane    hostProbeLane[OwnerSignals]
}

func newAvailabilityMonitor(clock Clock, daily, laptop, desktop bool) *availabilityMonitor {
	m := &availabilityMonitor{policy: availabilityPolicy{clock: clock, daily: daily, laptop: laptop, desktop: desktop}}
	m.work, m.cancel = context.WithCancel(context.Background())
	m.current = m.policy.evaluate(OwnerSignals{Power: "unknown", Heat: "unknown"})
	if !m.current.Available {
		m.cancel()
	}
	return m
}
func (m *availabilityMonitor) sample(s OwnerSignals) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = m.policy.evaluate(s)
	if !m.current.Available {
		m.cancel()
	} else if m.work.Err() != nil {
		m.work, m.cancel = context.WithCancel(context.Background())
	}
}
func (m *availabilityMonitor) snapshot() Availability {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := m.current
	a.AgeSeconds = m.policy.clock.Now().Sub(a.CheckedAt).Seconds()
	if m.policy.daily || m.policy.laptop || a.Signals.Laptop || a.Signals.Power == "battery" {
		now := m.policy.clock.Now()
		if !validMemoryValue(a.AgeSeconds) || now.Before(a.CheckedAt) || now.UnixNano()-a.CheckedAt.UnixNano() >= int64(a.Probe.freshness()) {
			a.Available = false
			a.Reason = "owner signals stale"
		}
	}
	if !a.Available {
		m.cancel()
	}
	return a
}
func (m *availabilityMonitor) workContext() context.Context {
	m.snapshot() // cancel stale work before handing out its context
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.work
}

// refresh never waits for the OS; the original probe start stamps the fact.
func (m *availabilityMonitor) refresh(ctx context.Context, source OwnerSignalSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lane.clock == nil {
		m.lane.clock = m.policy.clock
	}
	completed, err := m.lane.step(ctx, func(ctx context.Context) (OwnerSignals, error) {
		return source(ctx), ctx.Err()
	})
	if completed {
		m.policy.probe = m.lane.observation
		signals := m.lane.value
		if err != nil {
			signals = OwnerSignals{Power: "unknown", Heat: "unknown"}
		}
		m.current = m.policy.evaluate(signals)
		m.current.CheckedAt = m.lane.at
		if !m.current.Available {
			m.cancel()
		} else if m.work.Err() != nil {
			m.work, m.cancel = context.WithCancel(context.Background())
		}
	}
}

func (m *availabilityMonitor) run(ctx context.Context, source OwnerSignalSource, wake chan<- struct{}) {
	m.lane.clock = m.policy.clock
	for {
		m.refresh(ctx, source)
		m.snapshot() // stale facts cancel offline work even while a probe is stuck
		select {
		case wake <- struct{}{}:
		default:
		}
		timer := time.NewTimer(m.lane.waitDuration())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-m.lane.done:
		}
		timer.Stop()
	}
}

var errWorkerUnavailable = errors.New("hypha availability withdrawn")

func (w Worker) checkAvailability() error {
	if w.availability != nil && !w.availability.snapshot().Available {
		return errWorkerUnavailable
	}
	return nil
}
func machineAvailability(m *Machine) *Availability {
	if m == nil || m.CapabilityProfile == nil {
		return nil
	}
	return m.CapabilityProfile.Availability
}
func availabilityBlocked(m *Machine) bool {
	a := machineAvailability(m)
	if m == nil {
		return true
	}
	_, _, capacityKnown := capabilityCapacity(m)
	return !capacityKnown || a != nil && !a.Available
}

// Limits are admission-only and shared by all resource workers on this host.
// Ready idle model servers aren't jobs; in-flight model calls are.
func (q *Queue) machineJobLimit(m *Machine) int {
	limit := 0
	host := hostID(m)
	for _, other := range q.s.Machines {
		if !other.Gone && hostID(other) == host && other.MaxJobs > 0 {
			if limit == 0 || other.MaxJobs < limit {
				limit = other.MaxJobs
			}
		}
	}
	if m.MaxJobs > 0 && (limit == 0 || m.MaxJobs < limit) {
		limit = m.MaxJobs
	}
	return limit
}
func (q *Queue) machineLimitReason(m *Machine) string {
	if q.workerSuspect(m, q.clock.Now()) {
		return "worker suspect; waiting for contact"
	}
	return q.machineCapacityReason(m)
}

func (q *Queue) machineCapacityReason(m *Machine) string {
	if reason := q.hostHygieneReason(m); reason != "" {
		return reason
	}
	limit := q.machineJobLimit(m)
	if limit == 0 {
		return ""
	}
	host := hostID(m)
	count := 0
	seen := map[string]bool{}
	for _, j := range q.liveJobs() {
		if (j.State == "running" || j.State == "stopping") && q.s.Machines[j.Machine] != nil && hostID(q.s.Machines[j.Machine]) == host {
			count++
			seen[releaseKey(HostRelease{Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt})] = true
		}
	}
	for key, r := range q.s.HostReleases {
		if r.Host == host && !r.Preparation && !seen[key] {
			count++
		}
	}
	for _, c := range q.calls {
		if c.Machine != "" && q.s.Machines[c.Machine] != nil && hostID(q.s.Machines[c.Machine]) == host {
			count++
		}
	}
	if count >= limit {
		return fmt.Sprintf("maximum simultaneous jobs reached (%d)", limit)
	}
	return ""
}
func (q *Queue) withdrawUnavailable(m *Machine) {
	if !availabilityBlocked(m) {
		return
	}
	for _, j := range q.liveJobs() {
		if j.Machine == m.ID && j.Session == m.Session && j.State == "running" {
			reason := "capacity unavailable"
			if a := machineAvailability(m); a != nil {
				reason = a.Reason
			}
			q.stop(j, "hypha waiting: "+reason)
			deadline := q.clock.Now().Add(10 * time.Second)
			if !j.Spec.Resumable {
				deadline = q.clock.Now()
			}
			j.StopAt = &deadline
		}
	}
	// Pinning never overrides owner availability. In-flight calls fail/retry.
	for _, c := range q.calls {
		if c.Machine == m.ID && c.Session == m.Session {
			c.Error = "hypha availability withdrawn"
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == m.ID {
			slot.State = "draining"
			slot.InFlight = 0
		}
	}
}

// Queue-clock origin replaces worker wall time. Report age must describe the
// original observation; successful contact cannot erase it.
func acceptHostAvailability(m *Machine, now time.Time) {
	a := machineAvailability(m)
	if a == nil {
		return
	}
	// Dedicated hosts do not depend on owner observations. Sensor age must not
	// turn their affirmative dedicated policy into an owner-activity withdrawal.
	if a.Mode != "daily" && !a.Signals.Laptop && a.Signals.Power != "battery" {
		return
	}
	if a.Probe == nil {
		a.CheckedAt = now
		return
	} // Legacy worker protocol.
	if !a.Probe.valid() || !validMemoryValue(a.AgeSeconds) || a.AgeSeconds >= a.Probe.freshness().Seconds() {
		a.Available = false
		a.Reason = "owner signals stale"
		return
	}
	a.CheckedAt = now.Add(-time.Duration(a.AgeSeconds * float64(time.Second)))
}
func hostAvailabilityFresh(m *Machine, now time.Time) bool {
	a := machineAvailability(m)
	return a != nil && (a.Probe == nil || a.Probe.valid()) && !now.Before(a.CheckedAt) && now.Sub(a.CheckedAt) < a.Probe.freshness()
}
