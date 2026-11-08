package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"mycelium/internal/probeguard"
)

// Pressure readings are aggregate OS signals, never owner input or app names.
// AgeSeconds crosses clocks; ReceivedAt belongs exclusively to the queue.
type SharedHostPressure struct {
	Probe       *ProbeObservation       `json:"probe,omitempty"`
	Learning    *sharedPressureLearning `json:"learning,omitempty"` // Queue-owned; never accepted from a worker.
	MemoryKnown bool                    `json:"memory_known"`
	WakeKnown   bool                    `json:"wake_known"`
	MemoryLevel int                     `json:"memory_level"`  // Darwin: normal=1, warning=2, critical=4.
	MemoryPSI   float64                 `json:"memory_psi"`    // Linux memory.some avg10, percent.
	WakeDelayMS float64                 `json:"wake_delay_ms"` // Normal-priority timer wake lateness.
	AgeSeconds  float64                 `json:"age_seconds"`
	ReceivedAt  time.Time               `json:"received_at"`
	sampledAt   time.Time
}

const sharedHostPressureCause = "shared_host_pressure"

// Compatibility only for legacy pressure reports with no frozen probe validity.
// This does not control worker freshness or loss; host-probe-v1 uses its sample.
const legacyPressureDeliveryAllowance = 30 * time.Second

type sharedPressureLearning struct {
	Wake probeEstimate `json:"wake"`
	PSI  probeEstimate `json:"psi"`
	At   time.Time     `json:"at"`
}

func validSharedPressure(p *SharedHostPressure) bool {
	return p != nil && p.MemoryKnown && p.WakeKnown && (p.MemoryLevel == 0 || p.MemoryLevel == 1 || p.MemoryLevel == 2 || p.MemoryLevel == 4) &&
		validMemoryValue(p.MemoryPSI) && p.MemoryPSI <= 100 && validMemoryValue(p.WakeDelayMS) &&
		validMemoryValue(p.AgeSeconds) && (p.Probe == nil || p.Probe.valid()) && p.AgeSeconds < p.Probe.freshness().Seconds()
}

func (q *Queue) acceptSharedPressure(m, old *Machine) {
	defer q.recoverSharedHostBackoff(m)
	m.SharedHostYieldReason = ""
	m.SharedCPUEnforcement = sharedCPUEnforcementStatus(m)
	if q.s.HostMaintenance[hostID(m)].SharedHost == nil {
		m.SharedHostPressure = nil
		return
	}
	if old != nil && old.Session == m.Session {
		m.SharedHostYieldReason = old.SharedHostYieldReason
	}
	learned := sharedPressureLearning{}
	if old != nil && old.Session == m.Session && old.SharedHostPressure != nil && old.SharedHostPressure.Learning != nil {
		learned = *old.SharedHostPressure.Learning
	}
	p := m.SharedHostPressure
	if !validSharedPressure(p) {
		// Missing evidence invalidates admission, not session learning. Keep a
		// deliberately invalid observation carrying only queue-owned history.
		m.SharedHostPressure = &SharedHostPressure{Learning: &learned}
		return
	}
	p.ReceivedAt = q.clock.Now().Add(-time.Duration(p.AgeSeconds * float64(time.Second)))
	// Only fresh observations can train, and an adverse episode freezes the
	// healthy baseline. OS warning/critical levels remain physical signals.
	first := learned.At.IsZero()
	wakeHigh := !first && p.WakeDelayMS > learned.Wake.upper()
	psiHigh := !first && p.MemoryPSI > learned.PSI.upper()
	if p.MemoryLevel >= 2 {
		m.SharedHostYieldReason = "shared host memory pressure"
	} else if psiHigh || first && p.MemoryPSI > 0 {
		m.SharedHostYieldReason = "shared host memory PSI"
	} else if wakeHigh {
		m.SharedHostYieldReason = "shared host owner scheduling latency"
	} else if first || p.MemoryLevel <= 1 && p.MemoryPSI <= learned.PSI.Mean && p.WakeDelayMS <= learned.Wake.Mean {
		m.SharedHostYieldReason = ""
	}
	if m.SharedHostYieldReason == "" && (first || p.ReceivedAt.After(learned.At)) {
		learned.Wake.observe(p.WakeDelayMS)
		learned.PSI.observe(p.MemoryPSI)
		learned.At = p.ReceivedAt
	}
	p.Learning = &learned
}

func (q *Queue) sharedPressureReason(m *Machine) string {
	if m.SharedHostYieldReason != "" {
		return m.SharedHostYieldReason
	}
	if !q.sharedPressureFresh(m) {
		return "shared host pressure observation unknown or stale"
	}
	return ""
}

// Expected owner yield is temporary occupancy, not an infrastructure failure.
// Health and alerts distinguish it from missing/stale sensor evidence.
func (q *Queue) sharedPressureFresh(m *Machine) bool {
	p := m.SharedHostPressure
	now := q.clock.Now()
	if !validSharedPressure(p) || p.ReceivedAt.After(now) {
		return false
	}
	if p.Probe != nil {
		return now.Sub(p.ReceivedAt) < p.Probe.freshness()
	}
	// Legacy protocol: reuse bounded matching-session contact during queue delay. Successive
	// pending requests must never extend one observation beyond that budget.
	reportAt := p.ReceivedAt.Add(time.Duration(p.AgeSeconds * float64(time.Second)))
	contact := maxTime(reportAt, q.lastContact(m, now))
	return now.Sub(contact)+time.Duration(p.AgeSeconds*float64(time.Second)) < p.Probe.freshness() &&
		now.Sub(p.ReceivedAt) < p.Probe.freshness()+legacyPressureDeliveryAllowance
}

// hostPressureReason is the first owner-pressure reason among m's host
// workers: the yield latch or unknown/stale pressure evidence. It is
// temporary occupancy, never a qualification failure (#605).
func (q *Queue) hostPressureReason(m *Machine) string {
	for _, other := range q.s.Machines {
		if hostID(other) == hostID(m) && !other.Gone && !q.unreported[other.ID] && other.Replacement == nil {
			if reason := q.sharedPressureReason(other); reason != "" {
				return reason
			}
		}
	}
	return ""
}

// Planned stop intent is attached to the exact attempt. Existing delivery facts,
// checkpoint grace, cancellation precedence and cleanup custody remain in charge.
func (q *Queue) maintainSharedHosts() {
	for _, m := range q.s.Machines {
		if m.Gone || q.unreported[m.ID] || m.Replacement != nil || q.s.HostMaintenance[hostID(m)].SharedHost == nil || m.SharedHostYieldReason == "" {
			continue
		}
		for _, j := range q.liveJobs() {
			owner := q.s.Machines[j.Machine]
			if owner == nil || hostID(owner) != hostID(m) || j.State != "running" || cancelled(j) || plannedStop(j) {
				continue
			}
			for i := range j.Copies {
				c := &j.Copies[i]
				if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
					c.StopCause = sharedHostPressureCause
					c.PriorityStop = &PriorityPreemption{Cause: sharedHostPressureCause, Demand: "owner/" + hostID(m), Victim: j.ID, Policy: q.priorityMatch(j.Spec)}
					c.Recoverable = false
				}
			}
			q.stopFor(j, "owner", "pressure")
			j.Reason = m.SharedHostYieldReason
		}
	}
}

// Probe in the worker's bounded observation lane. Real elapsed time is sensor
// evidence; all freshness, admission and hysteresis decisions use Queue.Clock.
func localSharedHostPressure(ctx context.Context) (SharedHostPressure, error) {
	probeguard.Check()
	probeStart := time.Now()
	p := SharedHostPressure{MemoryKnown: true, WakeKnown: true}
	switch runtime.GOOS {
	case "darwin":
		b, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level").Output()
		if err != nil {
			return p, err
		}
		n, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || n != 1 && n != 2 && n != 4 {
			return p, errors.New("memory pressure level unavailable")
		}
		p.MemoryLevel = n
	case "linux":
		b, err := os.ReadFile("/proc/pressure/memory")
		if err != nil {
			return p, err
		}
		p.MemoryPSI, err = sharedMemoryPSI(b)
		if err != nil {
			return p, err
		}
	default:
		return p, errors.New("host pressure unavailable")
	}
	// Spend one measured sensor-cost share on an OS timer wake, rather than
	// twenty fixed 5ms ticks. This observes OS scheduling, not merely Go queues.
	quantum := max(time.Nanosecond, time.Since(probeStart)/time.Duration(runtime.NumCPU()))
	started := time.Now()
	timer := time.NewTimer(quantum)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return p, ctx.Err()
	case <-timer.C:
	}
	p.WakeDelayMS = max(0, time.Since(started)-quantum).Seconds() * 1000
	return p, nil
}

func sharedMemoryPSI(b []byte) (float64, error) {
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, f := range fields[1:] {
			if value, ok := strings.CutPrefix(f, "avg10="); ok {
				n, err := strconv.ParseFloat(value, 64)
				if err == nil && validMemoryValue(n) && n <= 100 {
					return n, nil
				}
			}
		}
	}
	return 0, errors.New("memory PSI unavailable")
}

// Pressure sampling has its own lane: a slow HTTP check-in must not delay
// consumption of a completed probe or the next refresh. A stuck source retains
// its original sample age; a completed failure immediately removes the sample.
type sharedPressureMonitor struct {
	mu       sync.Mutex
	clock    Clock
	source   func(context.Context) (SharedHostPressure, error)
	enabled  bool
	current  *SharedHostPressure
	lane     hostProbeLane[SharedHostPressure]
	reported time.Time
}

func (m *sharedPressureMonitor) snapshot(enabled bool) *SharedHostPressure {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	if !m.reported.IsZero() && now.After(m.reported) {
		// The gap includes the preceding check-in/queue round trip and the
		// next report's preparation. Learn even before profile activation.
		// Only future completed probes receive this prediction; a slow
		// consumer must never extend the current observation's frozen expiry.
		m.lane.timing.delivery.observe(now.Sub(m.reported).Seconds())
	}
	m.reported = now
	if m.enabled != enabled {
		m.enabled = enabled
		m.current = nil
		m.lane.known = false
		m.lane.timing.next = time.Time{}
		m.lane.discard = m.lane.pending != nil
	}
	if m.current == nil {
		return nil
	}
	p := *m.current
	p.AgeSeconds = now.Sub(p.sampledAt).Seconds()
	return &p
}

func (m *sharedPressureMonitor) sample(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.enabled {
		return
	}
	if m.lane.clock == nil {
		m.lane.clock = m.clock
	}
	if completed, err := m.lane.step(ctx, m.source); completed {
		m.current = nil
		if err == nil {
			p := m.lane.value
			p.sampledAt, p.Probe = m.lane.at, m.lane.observation
			m.current = &p
		}
	}
}

func (m *sharedPressureMonitor) run(ctx context.Context) {
	for {
		m.sample(ctx)
		m.mu.Lock()
		done := m.lane.done
		wait := m.lane.waitDuration()
		if !m.enabled {
			done = nil
			wait = m.lane.timing.controlPeriod()
		}
		m.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-done:
		}
		timer.Stop()
	}
}
