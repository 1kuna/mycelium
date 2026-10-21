package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Targets are views of one persisted machine, never additional workers or host
// allowances. The existing planner consumes one GPU (or CPU) view at a time.
func (q *Queue) machineTargets(m *Machine) []*Machine {
	if m.Replacement != nil {
		return nil
	}
	if m.Devices == nil {
		return []*Machine{m}
	}
	cpu := *m
	cpu.GPU, cpu.Device, cpu.FreeMemoryGB, cpu.TotalMemoryGB = "cpu", "", 0, 0
	cpu.UnifiedMemory, cpu.MemoryError = false, ""
	cpu.modelReservationDebtGB = 0
	cpu.ExtensionFingerprint = m.CPUExtensionFingerprint
	out := []*Machine{&cpu}
	for _, d := range m.Devices {
		target := d.machine(*m)
		reserved := 0.0
		for _, j := range q.s.Jobs {
			if !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") && batchOnTarget(j, &target) {
				reserved += j.Spec.Needs.MemoryGB
			}
		}
		for _, r := range q.s.HostReleases {
			if r.Machine != m.ID || r.Device != d.ID {
				continue
			}
			j := q.s.Jobs[r.Job]
			if j != nil && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt && (j.State == "running" || j.State == "stopping") && !j.ReleaseConfirmed {
				continue
			}
			reserved += r.MemoryGB
		}
		target.Models = nil
		for _, slot := range q.s.ModelSlots {
			if slotOnTarget(slot, &target) {
				if slotReservesMemory(slot) {
					reserved += q.slotMemory(slot)
				}
				if slot.State == "ready" {
					target.Models = append(target.Models, slot.Model)
				}
			}
		}
		target.modelReservationDebtGB = max(0, reserved-d.BudgetGB)
		target.FreeMemoryGB = min(target.FreeMemoryGB, max(0, d.BudgetGB-reserved))
		out = append(out, &target)
	}
	slices.SortFunc(out, func(a, b *Machine) int { return strings.Compare(a.Device, b.Device) })
	return out
}

func targetKey(m *Machine) string { return m.ID + "/" + m.Device }

func sameTarget(a, b *Machine) bool { return a.ID == b.ID && a.Device == b.Device }

func batchOnTarget(j *Job, m *Machine) bool {
	if j.Machine != m.ID {
		return false
	}
	if m.Devices == nil {
		return true
	}
	// A bounded legacy copy has no component evidence. Keep its original
	// machine-wide GPU exclusion until exact cleanup, without inventing a card.
	return !j.Spec.Needs.cpuOnly() && m.GPU != "cpu" && (len(j.AllocatedDevices) == 0 || slices.Contains(j.AllocatedDevices, m.Device))
}

func deviceAllowed(j *Job, m *Machine) bool {
	if m.Devices != nil && j.Spec.Needs.cpuOnly() {
		return m.GPU == "cpu"
	}
	g := j.Spec.Needs.GPU
	return g == nil || g.Devices == nil || slices.Contains(g.Devices, m.Device)
}

// Use the component's memory layout when charging the one host pool.
func allocationMachine(m *Machine, devices []string) *Machine {
	if m != nil && m.Devices != nil && len(devices) == 1 {
		for _, d := range m.Devices {
			if d.ID == devices[0] {
				target := d.machine(*m)
				return &target
			}
		}
	}
	return m
}

func (q *Queue) checkinMachineAttempts(m *Machine, reports []AttemptReport, progresses []attemptProgress, reply *Reply) {
	seen := map[attemptKey]bool{}
	cleanup := false
	for i := range reports {
		a := q.reconcileAttemptReport(m, &reports[i], progresses[i])
		reply.Attempts = append(reply.Attempts, a)
		seen[attemptKey{a.ID, a.Attempt, a.Session}] = true
		// Immediate cleanup of an unowned recovered copy must drain before
		// startup's fresh-session handoff. An owned stopping attempt, even
		// past its kill deadline, fences only its device and charged resources.
		j := q.s.Jobs[a.ID]
		owned := j != nil && j.Machine == m.ID && j.Session == a.Session && j.Attempt == a.Attempt && j.State == "stopping"
		cleanup = cleanup || a.Stop && a.Kill && !owned
	}
	for _, j := range q.s.Jobs {
		if j.Machine != m.ID || j.Session != m.Session || seen[jobKey(*j)] {
			continue
		}
		if j.State == "stopping" {
			if j.ReleaseConfirmed {
				q.retry(j, j.Reason)
				continue
			}
			reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: max(0, j.StopAt.Sub(q.clock.Now()).Seconds()), PriorityStop: priorityStop(j)})
		} else if j.State == "running" && !q.maintenanceBlocked(hostID(m)) && logPolicyFits(j.Spec.Logs, m) && q.checkpointReady(j, m) {
			out := cloneJob(j)
			reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: &out})
		}
	}
	// Let supervisors drain immediate-cleanup reports before offering more work.
	// Otherwise startup recovery can keep accepting and refusing replacements,
	// never reaching its empty collection and fresh-session handoff.
	if m.Leaving {
		active := len(reports) > 0
		for _, j := range q.s.Jobs {
			active = active || j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") && !j.ReleaseConfirmed
		}
		if !active && len(q.modelPlan(m.ID)) == 0 {
			m.Gone = true
			m.Models = nil
		}
		return
	}
	if cleanup {
		return
	}
	var best *Job
	var target *Machine
	for _, candidate := range q.machineTargets(m) {
		if !q.workerBatchIdle(candidate) {
			continue
		}
		j := q.pick(candidate)
		if j != nil && !q.betterPlacement(j, candidate) && (best == nil || q.before(j, best, candidate)) {
			best, target = j, candidate
		}
	}
	if best != nil {
		out := q.assign(best, target)
		reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: out.ID, Attempt: out.Attempt}, Session: out.Session, Job: &out})
	}
}

// Historical records carry explicit migration guidance; names are never parsed
// or used as aliases. M4 supplies these records when it retires source workers.
type MachineReplacement struct {
	Machine string `json:"machine"`
	Device  string `json:"device"`
}

func (q *Queue) validateMachineNames(names []string) error {
	for _, name := range names {
		if m := q.s.Machines[name]; m != nil && m.Replacement != nil {
			hint := fmt.Sprintf("machine %s was renamed; use allowed_machines: [%s]", name, m.Replacement.Machine)
			if m.Replacement.Device != "" {
				hint += fmt.Sprintf(" plus needs.gpu.devices: [%q]", m.Replacement.Device)
			}
			return &HTTPError{Status: 400, Code: "machine_renamed", Message: hint}
		}
	}
	return nil
}

func (q *Queue) machineDeviceStatus(copy, m *Machine) {
	for i := range copy.Devices {
		d := &copy.Devices[i]
		d.Attempts, d.Models, d.Releases = nil, nil, nil
		for _, target := range q.machineTargets(m) {
			if target.Device == d.ID {
				d.FreeMemoryGB = target.FreeMemoryGB
				break
			}
		}
		for _, j := range q.s.Jobs {
			if j.Machine == m.ID && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") && slices.Contains(j.AllocatedDevices, d.ID) {
				d.Attempts = append(d.Attempts, Execution{ID: j.ID, Attempt: j.Attempt})
			}
		}
		slices.SortFunc(d.Attempts, func(a, b Execution) int { return strings.Compare(a.ID, b.ID) })
		for _, slot := range q.s.ModelSlots {
			if slot.Machine == m.ID && slot.Device == d.ID {
				d.Models = append(d.Models, *slot)
			}
		}
		for _, r := range q.s.HostReleases {
			if r.Machine == m.ID && r.Device == d.ID {
				d.Releases = append(d.Releases, r)
			}
		}
	}
}

func batchReleaseKey(m *Machine) string {
	if m.Devices != nil {
		return targetKey(m)
	}
	return m.ID
}

// Recovery adopts an execution whose exact release is still charged. It cannot
// claim another device's slot or add capacity to the shared host allowance.
func (q *Queue) machineRecoveryFits(m *Machine, j *Job, copy Copy) bool {
	if m.Devices == nil || !j.Spec.Needs.cpuOnly() && len(copy.AllocatedDevices) == 0 {
		return !q.workerHasOtherAttempt(m.ID, m.Session, j.ID)
	}
	if _, retained := q.s.HostReleases[releaseKey(HostRelease{Job: j.ID, Machine: m.ID, Session: copy.Session, Attempt: copy.Attempt})]; !retained {
		return false
	}
	for _, target := range q.machineTargets(m) {
		if j.Spec.Needs.cpuOnly() && target.GPU == "cpu" || len(copy.AllocatedDevices) == 1 && target.GPU != "cpu" && target.Device == copy.AllocatedDevices[0] {
			return q.workerBatchIdle(target) && target.CPUCapacityReason == "" && target.CPUInUseCores <= target.EffectiveCPUCores && target.hostReservationDebtGB == 0 && target.modelReservationDebtGB == 0 && target.MemoryError == "" && deviceAllowed(j, target)
		}
	}
	return false
}
