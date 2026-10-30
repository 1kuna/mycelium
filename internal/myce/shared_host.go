package myce

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"syscall"
)

// SharedHostProfile protects additional interactive use. Size the reserve from
// representative owner bursts, not total RAM or a machine/deployment name.
// Zero in a check-in reply explicitly removes the profile.
type SharedHostProfile struct {
	InteractiveReserveGB float64 `json:"interactive_reserve_gb"`
}

func cloneSharedHost(p *SharedHostProfile) *SharedHostProfile {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}

// Profile admission is deliberately limited to native CPU commands with already
// prepared environments. Daemon children do not inherit native process priority.
func sharedHostJobFits(j *Job, m *Machine) bool {
	if m.SharedHostReason != "" {
		return false
	}
	return m.SharedHost == nil || j.Spec.Needs.cpuOnly() && !j.Spec.Run.Docker && !j.Spec.extensionJob() && environmentReady(j, m)
}

// Only bounded in-memory history is read here. Existing live reserves and static
// budgets remain upper bounds; an empty history never means zero external use.
func (q *Queue) sharedHostRoom(m *Machine, room, reserved, growth float64) float64 {
	m.SharedHostReason = ""
	p := q.s.HostMaintenance[hostID(m)].SharedHost
	if p == nil {
		if m.SharedHost != nil {
			m.SharedHostReason = "shared host profile awaiting worker acknowledgement"
			return 0
		}
		return room
	}
	if m.SharedHost == nil || *p != *m.SharedHost {
		m.SharedHostReason = "shared host profile awaiting worker acknowledgement"
		return 0
	}
	h := q.externalMemoryStatus(hostID(m))
	o := m.MemoryObservation
	if !validHostMemoryObservation(o) || h == nil || h.Week.Samples == 0 {
		m.SharedHostReason = "shared host requires live and historical external RAM observations"
		return 0
	}
	suggestion, err := SuggestHostBudget(o.TotalGB, p.InteractiveReserveGB, 0, h)
	if err != nil {
		m.SharedHostReason = err.Error()
		return 0
	}
	// Raw live availability subtracts the protected reserve once. The existing
	// after-reserve availability can only tighten this bound, never relax it.
	live := o.TotalGB - o.UsedGB - suggestion.HeadroomGB - growth
	return min(room, suggestion.SuggestedGB-reserved, live)
}

func validHostMemoryObservation(o *HostMemoryObservation) bool {
	return o != nil && validMemoryValue(o.TotalGB) && o.TotalGB > 0 && validMemoryValue(o.UsedGB) && o.UsedGB <= o.TotalGB && validMemoryValue(o.MyceliumGB) && o.MyceliumGB <= o.UsedGB
}

// The supervisor is still behind its execution gate. Its payload/descendants
// inherit nice=10, leaving the worker's control goroutines at normal priority.
func applySharedHostPriority(pid int, p *SharedHostProfile) error {
	if p == nil {
		return nil
	}
	priority, err := syscall.Getpriority(syscall.PRIO_PROCESS, pid)
	if err != nil {
		return fmt.Errorf("shared host command priority: %w", err)
	}
	// Linux exposes the kernel encoding (20-nice); Darwin returns nice directly.
	if runtime.GOOS == "linux" {
		priority = 20 - priority
	}
	if priority >= 10 {
		return nil
	}
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, pid, 10); err != nil {
		return fmt.Errorf("shared host command priority: %w", err)
	}
	return nil
}

// Launch rechecks current raw RAM. The queue retains the historical bound;
// this check protects owner growth between assignment and execution.
func (w Worker) sharedHostLiveMemory(ctx context.Context) (float64, error) {
	if w.Machine.SharedHost == nil {
		return w.HostMemorySource(ctx)
	}
	admission, raw, err := w.sampleHostMemory(ctx)
	if err != nil {
		return 0, err
	}
	if !validMemoryValue(raw) {
		return 0, errors.New("shared host raw memory unavailable")
	}
	reserve := w.Machine.SharedHost.InteractiveReserveGB
	if c := w.Machine.CapabilityProfile; c != nil {
		reserve = max(reserve, .25*max(0, float64(c.SystemMemoryBytes)/(1<<30)-raw))
	}
	return max(0, min(admission, raw-reserve)), nil
}
