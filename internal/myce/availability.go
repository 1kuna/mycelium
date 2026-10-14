package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const ownerIdleWait = 5 * time.Minute
const signalRefresh = 5 * time.Second
const signalMaxAge = 15 * time.Second
const coolingWait = time.Minute

// OwnerSignals contains only aggregate OS state, never input or screen contents.
// Power is desktop (no battery), ac, battery, or unknown. Heat is nominal, fair,
// serious, critical, or unknown. Laptop stays true even if its power read fails.
type OwnerSignals struct {
	IdleSeconds float64 `json:"idle_seconds"`
	IdleKnown   bool    `json:"idle_known"`
	Laptop      bool    `json:"laptop"`
	Power       string  `json:"power"`
	Heat        string  `json:"heat"`
}
type OwnerSignalSource func(context.Context) OwnerSignals

type Availability struct {
	Mode      string       `json:"mode"`
	Available bool         `json:"available"`
	Reason    string       `json:"reason,omitempty"`
	CheckedAt time.Time    `json:"checked_at"`
	Signals   OwnerSignals `json:"signals"`
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
	clock      Clock
	daily      bool
	laptop     bool
	last       time.Time
	awakeSince time.Time
	hot        bool
	coolSince  time.Time
}

func (p *availabilityPolicy) evaluate(s OwnerSignals) Availability {
	if math.IsNaN(s.IdleSeconds) || math.IsInf(s.IdleSeconds, 0) || s.IdleSeconds < 0 {
		s.IdleKnown = false
		s.IdleSeconds = 0
	}
	p.laptop = p.laptop || s.Laptop
	s.Laptop = p.laptop
	now := p.clock.Now()
	a := Availability{Mode: "dedicated", Available: true, CheckedAt: now, Signals: s}
	if p.daily {
		a.Mode = "daily"
	}
	if !p.last.IsZero() && (now.UnixNano()-p.last.UnixNano() > int64(signalMaxAge) || now.Before(p.last)) {
		p.awakeSince = now
		p.coolSince = time.Time{}
	}
	p.last = now
	wait := func(reason string) Availability { a.Available = false; a.Reason = reason; return a }
	heatReason := ""
	if p.daily {
		switch s.Heat {
		case "fair", "serious", "critical":
			p.hot = true
			p.coolSince = time.Time{}
			heatReason = "cooling down"
		case "nominal":
			if p.hot {
				if p.coolSince.IsZero() {
					p.coolSince = now
				}
				if now.Sub(p.coolSince) < coolingWait {
					heatReason = "cooling down"
				} else {
					p.hot = false
				}
			}
		default:
			p.coolSince = time.Time{}
			heatReason = "heat sensing unavailable"
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
	if s.IdleSeconds < ownerIdleWait.Seconds() || !p.awakeSince.IsZero() && now.Sub(p.awakeSince) < ownerIdleWait {
		return wait("waiting for 5 idle minutes")
	}
	if heatReason != "" {
		return wait(heatReason)
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
}

func newAvailabilityMonitor(clock Clock, daily, laptop bool) *availabilityMonitor {
	m := &availabilityMonitor{policy: availabilityPolicy{clock: clock, daily: daily, laptop: laptop}}
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
	if m.policy.daily || m.policy.laptop || a.Signals.Laptop || a.Signals.Power == "battery" {
		now := m.policy.clock.Now()
		if now.Before(a.CheckedAt) || now.UnixNano()-a.CheckedAt.UnixNano() > int64(signalMaxAge) {
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
func (m *availabilityMonitor) run(ctx context.Context, source OwnerSignalSource, wake chan<- struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	next := m.policy.clock.Now()
	for {
		now := m.policy.clock.Now()
		if !now.Before(next) {
			probe, cancel := context.WithTimeout(ctx, 2*time.Second)
			m.sample(source(probe))
			cancel()
			next = now.Add(signalRefresh)
			select {
			case wake <- struct{}{}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
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
	} // V2/dedicated legacy contract.
	return m.CapabilityProfile.Availability
}
func availabilityBlocked(m *Machine) bool {
	a := machineAvailability(m)
	return a != nil && !a.Available
}

// Limits are admission-only and shared by all resource workers on this host.
// Ready idle model servers aren't jobs; in-flight model calls are.
func (q *Queue) machineLimitReason(m *Machine) string {
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
	if limit == 0 {
		return ""
	}
	count := 0
	for _, j := range q.s.Jobs {
		if (j.State == "running" || j.State == "stopping") && q.s.Machines[j.Machine] != nil && hostID(q.s.Machines[j.Machine]) == host {
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
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && j.Session == m.Session && j.State == "running" {
			q.stop(j, "hypha waiting: "+machineAvailability(m).Reason)
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
