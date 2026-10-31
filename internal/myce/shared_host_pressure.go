package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/probeguard"
)

// Pressure readings are aggregate OS signals, never owner input or app names.
// AgeSeconds crosses clocks; ReceivedAt belongs exclusively to the queue.
type SharedHostPressure struct {
	MemoryKnown bool      `json:"memory_known"`
	WakeKnown   bool      `json:"wake_known"`
	MemoryLevel int       `json:"memory_level"`  // Darwin: normal=1, warning=2, critical=4.
	MemoryPSI   float64   `json:"memory_psi"`    // Linux memory.some avg10, percent.
	WakeDelayMS float64   `json:"wake_delay_ms"` // Normal-priority timer wake lateness.
	AgeSeconds  float64   `json:"age_seconds"`
	ReceivedAt  time.Time `json:"received_at"`
	sampledAt   time.Time
}

const sharedHostPressureCause = "shared_host_pressure"

// Wake-delay bands are informed by the laptop interference canary documented
// in shared-hosts.md. PSI bands bound stalled time to 100/10 ms per ten seconds;
// native Linux qualification is separate from this conservative policy.
const sharedWakeHighMS = 20.0
const sharedWakeLowMS = 5.0
const sharedPSIHigh = 1.0
const sharedPSILow = 0.1

func validSharedPressure(p *SharedHostPressure) bool {
	return p != nil && p.MemoryKnown && p.WakeKnown && (p.MemoryLevel == 0 || p.MemoryLevel == 1 || p.MemoryLevel == 2 || p.MemoryLevel == 4) &&
		validMemoryValue(p.MemoryPSI) && p.MemoryPSI <= 100 && validMemoryValue(p.WakeDelayMS) &&
		validMemoryValue(p.AgeSeconds) && p.AgeSeconds < signalMaxAge.Seconds()
}

func (q *Queue) acceptSharedPressure(m, old *Machine) {
	m.SharedHostYieldReason = ""
	m.SharedCPUEnforcement = sharedCPUEnforcementStatus(m)
	if q.s.HostMaintenance[hostID(m)].SharedHost == nil {
		m.SharedHostPressure = nil
		return
	}
	if old != nil {
		m.SharedHostYieldReason = old.SharedHostYieldReason
	}
	p := m.SharedHostPressure
	if !validSharedPressure(p) {
		m.SharedHostPressure = nil
		return
	}
	p.ReceivedAt = q.clock.Now().Add(-time.Duration(p.AgeSeconds * float64(time.Second)))
	if p.MemoryLevel >= 2 {
		m.SharedHostYieldReason = "shared host memory pressure"
	} else if p.MemoryPSI >= sharedPSIHigh {
		m.SharedHostYieldReason = "shared host memory PSI"
	} else if p.WakeDelayMS >= sharedWakeHighMS {
		m.SharedHostYieldReason = "shared host owner scheduling latency"
	} else if p.MemoryLevel <= 1 && p.MemoryPSI <= sharedPSILow && p.WakeDelayMS <= sharedWakeLowMS {
		m.SharedHostYieldReason = ""
	}
}

func (q *Queue) sharedPressureReason(m *Machine) string {
	if m.SharedHostYieldReason != "" {
		return m.SharedHostYieldReason
	}
	p := m.SharedHostPressure
	if !validSharedPressure(p) || p.ReceivedAt.After(q.clock.Now()) || q.clock.Now().Sub(p.ReceivedAt) >= signalMaxAge {
		return "shared host pressure observation unknown or stale"
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
	// A 5ms periodic foreground-class wake probe catches scheduling contention
	// without a busy loop. Twenty samples cost 100ms in a background lane.
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for i := 0; i < 20; i++ {
		select {
		case due := <-tick.C:
			p.WakeDelayMS = max(p.WakeDelayMS, time.Since(due).Seconds()*1000)
		case <-ctx.Done():
			return p, ctx.Err()
		}
	}
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
