package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime"
	"slices"
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
	if m.SharedHost == nil {
		return true
	}
	// Admission and launch also receive the physical worker's accelerator
	// view. CPU payloads use their prepared CPU recipes on either kind of host.
	if j.Spec.Needs.cpuOnly() {
		m = cpuMachineView(m)
	}
	if !j.Spec.Needs.cpuOnly() || j.Spec.Run.Docker || j.Spec.extensionJob() || !environmentReady(j, m) {
		return false
	}
	for _, side := range j.Spec.Run.Sidecars {
		if side.Environment != "" {
			r, ok := j.SidecarEnvironmentRecipes[side.Name][m.GPU]
			if !ok || !slices.Contains(m.Environments, r.Key) {
				return false
			}
		}
	}
	return true
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
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "linux" {
		if _, dev := q.clock.(*devClock); !dev && m.SharedCPUEnforcement != fmt.Sprintf("cgroup v2 cpu.weight=10 cpu.max=%d/100000", m.CPUBudgetCores*100000) {
			m.SharedHostReason = "shared host CPU time enforcement unavailable"
			return 0
		}
	}
	workers := 0
	for _, other := range q.s.Machines {
		if hostID(other) == hostID(m) && !other.Gone && !q.unreported[other.ID] && other.Replacement == nil {
			workers++
			if reason := q.sharedPressureReason(other); reason != "" {
				m.SharedHostReason = reason
				return 0
			}
		}
	}
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "linux" && workers > 1 {
		if _, dev := q.clock.(*devClock); !dev {
			m.SharedHostReason = "shared host aggregate CPU control requires one machine worker"
			return 0
		}
	}
	h := q.externalMemoryStatus(hostID(m))
	o := m.MemoryObservation
	if !validHostMemoryObservation(o) || h == nil || h.Week.Samples == 0 {
		m.SharedHostReason = "shared host requires live and historical external RAM observations"
		return 0
	}
	// This profile requires a measured additional burst reserve. The external
	// peak already contains observed owner growth; a percentage of that whole
	// baseline is not another independent burst measurement (#389).
	historical := math.Floor(max(0, o.TotalGB-h.Week.MaxGB-p.InteractiveReserveGB)*4) / 4
	// Raw live availability subtracts the protected reserve once. The existing
	// after-reserve availability can only tighten this bound, never relax it.
	live := o.TotalGB - o.UsedGB - p.InteractiveReserveGB - growth
	return min(room, historical-reserved, live)
}

func validHostMemoryObservation(o *HostMemoryObservation) bool {
	return o != nil && validMemoryValue(o.TotalGB) && o.TotalGB > 0 && validMemoryValue(o.UsedGB) && o.UsedGB <= o.TotalGB && validMemoryValue(o.MyceliumGB) && o.MyceliumGB <= o.UsedGB
}

// Establish Darwin payload priority after its execution gate opens. Successful
// external requests on the launching shell do not always survive to the payload.
// Standard tools apply nice and inheritable background policy from the payload
// itself without changing the worker's control process.
func sharedHostCommand(argv []string, p *SharedHostProfile) []string {
	if p == nil || runtime.GOOS != "darwin" {
		return argv
	}
	return append([]string{"/usr/bin/nice", "-n", "10", "/usr/sbin/taskpolicy", "-b"}, argv...)
}

var sharedHostSetBackgroundPriority = func(pid int) error {
	// Darwin sys/resource.h: PRIO_DARWIN_PROCESS, PRIO_DARWIN_BG.
	return syscall.Setpriority(4, pid, 0x1000)
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
	if priority < 10 {
		if err := syscall.Setpriority(syscall.PRIO_PROCESS, pid, 10); err != nil {
			return fmt.Errorf("shared host command priority: %w", err)
		}
	}
	if runtime.GOOS == "darwin" {
		// Darwin sys/resource.h. Apply only to the gated payload PID;
		// background CPU/I/O/timer/network policy survives fork/exec.
		if err := sharedHostSetBackgroundPriority(pid); err != nil {
			log.Printf("warning: shared host background task policy for payload pid %d: %v", pid, err)
		}
	}
	return nil
}

// Launch rechecks current raw RAM. The queue retains the historical bound;
// this check protects owner growth between assignment and execution.
func (w Worker) sharedHostLiveMemory(ctx context.Context) (float64, error) {
	cuda := hostHasUnifiedCUDA(&w.Machine)
	if w.Machine.SharedHost == nil && !cuda {
		return w.HostMemorySource(ctx)
	}
	admission, raw, err := w.sampleHostMemory(ctx)
	if err != nil {
		return 0, err
	}
	if !validMemoryValue(raw) {
		return 0, errors.New("raw host memory unavailable")
	}
	reserve := 0.0
	if w.Machine.SharedHost != nil {
		reserve = w.Machine.SharedHost.InteractiveReserveGB
	}
	if cuda {
		floor, ok := unifiedCUDAFloor(&w.Machine)
		if !ok {
			return 0, errors.New("unified CUDA physical capacity unavailable")
		}
		reserve = max(reserve, floor)
	}
	return max(0, min(admission, raw-reserve)), nil
}
