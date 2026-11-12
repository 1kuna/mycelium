package myce

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
)

// CPU work uses the host allowance, never the accelerator's memory or reserve.
// Share this view between placement, observation recording and curve validation.
func cpuMachineView(m *Machine) *Machine {
	cpu := *m
	cpu.GPU, cpu.Device, cpu.ExtensionFingerprint = "cpu", "", m.CPUExtensionFingerprint
	cpu.FreeMemoryGB, cpu.TotalMemoryGB, cpu.ReservedMemoryGB = 0, 0, 0
	cpu.UnifiedMemory, cpu.MemoryError = false, ""
	cpu.modelReservationDebtGB = 0
	return &cpu
}

// Targets are views of one persisted machine, never additional workers or host
// allowances. The existing planner consumes one GPU (or CPU) view at a time.
func (q *Queue) machineTargets(m *Machine) []*Machine {
	if m.Replacement != nil {
		return nil
	}
	if m.Devices == nil {
		return []*Machine{m}
	}
	out := []*Machine{cpuMachineView(m)}
	for _, d := range m.Devices {
		target := d.machine(*m)
		reserved := 0.0
		for _, j := range q.liveJobs() {
			if batchReservesCapacity(j) && batchOnTarget(j, &target) {
				charge := sampledDeviceCharge(j, &target, q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now())
				reserved += charge
				target.batchTenants++
			}
		}
		for _, r := range q.s.HostReleases {
			if r.Machine != m.ID || !r.onDevice(d.ID) {
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

// Placement also offers card groups (MY-30, #105). Other inventory readers,
// status and qualification plans keep the per-card views.
func (q *Queue) placementTargets(m *Machine) []*Machine {
	targets := q.machineTargets(m)
	claims := q.sharedInputClaims(m)
	for _, target := range targets {
		// Legacy single-device targets are the queue's live machine: model
		// loading charges their memory in place. Preserve that identity while
		// attaching these transient, queue-owned claims.
		target.sharedInputClaims = claims
	}
	groups := groupTargets(targets)
	if len(m.Devices) <= 2 || len(groups) == 0 {
		return append(targets, groups...)
	}
	// Free-memory ordering can change during teardown. Keep an already chosen
	// group available as a candidate instead of silently substituting cards.
	for _, j := range q.liveJobs() {
		p := j.PreemptionPlacement
		if j.State != "queued" || p == nil || p.Machine != m.ID || p.Session != m.Session || !strings.Contains(p.Device, ",") || slices.ContainsFunc(groups, func(g *Machine) bool { return g.Device == p.Device }) {
			continue
		}
		members := []*Machine{}
		for _, id := range strings.Split(p.Device, ",") {
			for _, card := range targets {
				if card.Device == id {
					members = append(members, card)
				}
			}
		}
		if len(members) == len(strings.Split(p.Device, ",")) {
			g := groupView(members)
			for _, other := range groups {
				if len(other.group) == len(g.group) && other.GPU == g.GPU {
					g.groupFloorGB = other.groupFloorGB
					groups = append(groups, g)
					break
				}
			}
		}
	}
	return append(targets, groups...)
}

// A job uses several cards only when no single card of its host can hold it.
// Each group is the fewest same-backend discrete cards with the most free
// memory now; every card carries an equal share of the job's memory_gb.
func groupTargets(cards []*Machine) []*Machine {
	byPlatform := map[string][]*Machine{}
	for _, c := range cards {
		if c.GPU != "cpu" && c.Device != "" && !c.UnifiedMemory && c.TotalMemoryGB > 0 {
			byPlatform[c.GPU] = append(byPlatform[c.GPU], c)
		}
	}
	var out []*Machine
	for _, same := range byPlatform {
		slices.SortStableFunc(same, func(a, b *Machine) int { return cmp.Compare(b.FreeMemoryGB, a.FreeMemoryGB) })
		// The floor is the most any fewer cards hold in equal shares: k cards
		// hold k times the k-th largest card, whichever cards are free now.
		totals := make([]float64, len(same))
		for i, c := range same {
			totals[i] = c.TotalMemoryGB
		}
		slices.SortFunc(totals, func(a, b float64) int { return cmp.Compare(b, a) })
		floor := 0.0
		for n := 2; n <= len(same); n++ {
			floor = max(floor, float64(n-1)*totals[n-2])
			g := groupView(same[:n])
			g.groupFloorGB = floor
			out = append(out, g)
		}
	}
	slices.SortFunc(out, func(a, b *Machine) int { return strings.Compare(a.Device, b.Device) })
	return out
}

// The group copies its first card's view; capacity is the smallest card's
// times the count, so an equal share fits on every member.
func groupView(members []*Machine) *Machine {
	g := *members[0]
	g.group = make([]string, len(members))
	free, total := math.Inf(1), math.Inf(1)
	g.modelReservationDebtGB, g.batchTenants = 0, 0
	for i, m := range members {
		g.group[i] = m.Device
		free, total = min(free, m.FreeMemoryGB), min(total, m.TotalMemoryGB)
		g.modelReservationDebtGB += m.modelReservationDebtGB
		g.batchTenants += m.batchTenants
		if g.MemoryError == "" {
			g.MemoryError = m.MemoryError
		}
	}
	slices.SortFunc(g.group, func(a, b string) int { return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b)) })
	n := float64(len(members))
	g.groupMembers, g.Device, g.Models = slices.Clone(members), strings.Join(g.group, ","), nil
	g.FreeMemoryGB, g.TotalMemoryGB = n*free, n*total
	return &g
}

func targetDevices(m *Machine) []string {
	if len(m.group) > 0 {
		return m.group
	}
	return []string{m.Device}
}

// Each card of an attempt is charged an equal share of its accelerator memory.
func deviceShareGB(j *Job) float64 {
	return j.Spec.Needs.MemoryGB / float64(max(1, len(j.AllocatedDevices)))
}

func (r HostRelease) onDevice(id string) bool {
	return r.Device == id || slices.Contains(r.Devices, id)
}

func sameTarget(a, b *Machine) bool { return a.ID == b.ID && a.Device == b.Device }

// The current offer/execution reserves capacity until exact release. A job
// awaiting result publication stays running without retaining compute; a fresh
// attempt reserves again even when an earlier copy has confirmed release.
func batchReservesCapacity(j *Job) bool {
	return !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping")
}

func batchOnTarget(j *Job, m *Machine) bool {
	if j.Machine != m.ID {
		return false
	}
	if m.Devices == nil {
		return true
	}
	// A bounded legacy copy has no component evidence. Keep its original
	// machine-wide GPU exclusion until exact cleanup, without inventing a card.
	return !j.Spec.Needs.cpuOnly() && m.GPU != "cpu" && (len(j.AllocatedDevices) == 0 || slices.ContainsFunc(targetDevices(m), func(id string) bool { return slices.Contains(j.AllocatedDevices, id) }))
}

// A single-card batch attempt with a declared or learned memory vector may share
// its card when memory fits (MY-30/31, #538). Model-server,
// extension-check, default fan-out, exploratory and multi-card work keeps the card, as
// does a hint naming no device memory: its device figure is only a default.
func sharesDevice(j *Job) bool {
	n, e := j.Spec.Needs, j.MemoryEnvelope
	unstated := e != nil && (e.Basis == "exploratory" || e.Basis == "hint" && (j.MemoryFallback == nil || j.MemoryFallback.MemoryGB <= 0))
	// Host-only learning can retain a recipe default for device memory. The
	// attempt keeps the card until that vector is measured.
	unstated = unstated || e != nil && e.Basis == "learned" && strings.Contains(e.Source, "GPU recipe default")
	return !n.cpuOnly() && n.MemoryGB > 0 && n.Model == "" && j.Spec.ExtensionCheck == nil && (j.Spec.FanOut == nil || fanOutDeviceSharing(j)) && len(j.AllocatedDevices) <= 1 && !unstated
}

// XPU teardown can reset shared engines (#652). CUDA co-tenants can grow past
// reservations without fresh per-process evidence (#687). Both require every
// lower-tier holder to release before higher-tier admission.
func isolatesDeviceTiers(m *Machine) bool {
	return backendFor(m.GPU).isolateTiers
}

// deviceCoTenants counts the batch attempts on card m that demand may share it
// with. Every holder and every retained release there must share; memory
// charges, not this check, decide fit. A joining demand also never slows a
// higher tier's attempt. Tier-isolated newcomers require equal tiers. Recovery
// re-adopts custody it already held.
func (q *Queue) deviceCoTenants(m *Machine, demand *Job, joining bool) (int, bool) {
	if m.Devices == nil || m.GPU == "cpu" || len(m.groupMembers) > 0 || !sharesDevice(demand) {
		return 0, false
	}
	n := 0
	fanOut := fanOutDeviceSharing(demand)
	var holders []*Job
	for _, j := range q.liveJobs() {
		if j.ID == demand.ID || !batchOnTarget(j, m) || !batchReservesCapacity(j) {
			continue
		}
		tier := q.comparePriority(j.Spec, demand.Spec)
		if !sharesDevice(j) || joining && (tier < 0 || isolatesDeviceTiers(m) && tier != 0) {
			return 0, false
		}
		if joining && fanOutDeviceSharing(demand) != fanOutDeviceSharing(j) {
			return 0, false // Opt-in work cannot enlist an ordinary or default-off tenant.
		}
		if joining && unifiedCUDA(m) && !sharedDeviceSampleReady(q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now()) {
			return 0, false
		}
		n++
		fanOut = fanOut || fanOutDeviceSharing(j)
		holders = append(holders, j)
	}
	for _, r := range q.s.HostReleases {
		if !q.batchReleaseOnTarget(r, m) {
			continue
		}
		if j := q.s.Jobs[r.Job]; j == nil || r.Device == "" || len(r.Devices) > 1 || !sharesDevice(j) || joining && fanOutDeviceSharing(demand) != fanOutDeviceSharing(j) {
			return 0, false
		}
	}
	if joining && n > 0 && fanOut && !q.fanOutDeviceHeadroom(m, demand, holders) {
		return 0, false
	}
	return n, true
}

func (q *Queue) batchSlotFree(m *Machine, demand *Job) bool {
	if !q.deviceTiersSettled(m, demand, false) {
		return false
	}
	if q.workerBatchIdle(m) {
		return true
	}
	_, ok := q.deviceCoTenants(m, demand, true)
	return ok && q.cardSettled(m) && !m.Gone && !m.Leaving && !q.unreported[m.ID]
}

// Tier isolation covers both batch and engine custody. An engine demand may
// reuse its own model, but cannot start beside other different-tier holders.
func (q *Queue) deviceTiersSettled(m *Machine, demand *Job, engine bool) bool {
	if !isolatesDeviceTiers(m) || demand.Spec.Needs.cpuOnly() {
		return true
	}
	for _, j := range q.liveJobs() {
		if !j.Spec.Needs.cpuOnly() && batchOnTarget(j, m) && batchReservesCapacity(j) && q.comparePriority(j.Spec, demand.Spec) != 0 {
			return false
		}
	}
	for _, r := range q.s.HostReleases {
		if r.CPUOnly || !q.batchReleaseOnTarget(r, m) {
			continue
		}
		if j := q.s.Jobs[r.Job]; j == nil || q.comparePriority(j.Spec, demand.Spec) != 0 {
			return false
		}
	}
	cards := []*Machine{m}
	if len(m.groupMembers) > 0 {
		cards = m.groupMembers
	}
	for _, card := range cards {
		for _, s := range q.s.ModelSlots {
			if slotOnTarget(s, card) && slotReservesMemory(s) && (!engine || s.Model != demand.Spec.Needs.Model) && q.comparePriority(q.slotSpec(s), demand.Spec) != 0 {
				return false
			}
		}
	}
	return true
}

// A newcomer joins only running holders. A stopping holder or a finished one
// awaiting exact release keeps the card fenced until that release (custody).
func (q *Queue) cardSettled(m *Machine) bool {
	for _, j := range q.liveJobs() {
		if batchOnTarget(j, m) && batchReservesCapacity(j) && j.State != "running" {
			return false
		}
	}
	for _, r := range q.s.HostReleases {
		if j := q.s.Jobs[r.Job]; q.batchReleaseOnTarget(r, m) && (j == nil || j.Machine != r.Machine || j.Session != r.Session || j.Attempt != r.Attempt || j.State != "running") {
			return false
		}
	}
	return true
}

// Retained custody owns the command slot even after a job becomes terminal or
// advances to another attempt. Missing device evidence keeps legacy exclusion.
func (q *Queue) batchReleaseOnTarget(r HostRelease, m *Machine) bool {
	if r.Preparation || r.Machine != m.ID {
		return false
	}
	if m.Devices == nil {
		return true
	}
	if m.GPU == "cpu" {
		return false
	}
	// A CPU attempt can retain the worker's scalar compatibility device ID.
	// Its own persisted classification survives requeue and variant changes.
	if r.CPUOnly {
		return false
	}
	if r.Device != "" {
		return slices.ContainsFunc(targetDevices(m), r.onDevice)
	}
	return true
}

func deviceAllowed(j *Job, m *Machine) bool {
	if m.Devices != nil && j.Spec.Needs.cpuOnly() {
		return m.GPU == "cpu"
	}
	g := j.Spec.Needs.GPU
	return g == nil || g.Devices == nil || !slices.ContainsFunc(targetDevices(m), func(id string) bool { return !slices.Contains(g.Devices, id) })
}

// Use the component's memory layout when charging the one host pool. Several
// cards are one group view, so host RAM and cores are charged once.
func allocationMachine(m *Machine, devices []string) *Machine {
	return allocationMachineCached(m, devices, nil)
}

type allocationViewKey struct {
	owner   *Machine
	devices string
	group   bool
}

// Raw inventory projections differ from machineTargets' reservation-adjusted
// views. Reuse them only inside the existing immutable memory-planning pass;
// callers outside a pass still get fresh views after inventory changes.
func (q *Queue) planningAllocationMachine(m *Machine, devices []string) *Machine {
	return allocationMachineCached(m, devices, q.memoryPlanning)
}

func allocationMachineCached(m *Machine, devices []string, cache *memoryPlanningCache) *Machine {
	if m == nil || m.Devices == nil || len(devices) == 0 {
		return m
	}
	var key allocationViewKey
	if cache != nil {
		key = allocationViewKey{owner: m, devices: strings.Join(devices, ","), group: len(devices) > 1}
		if target := cache.allocations[key]; target != nil {
			return target
		}
	}
	target := buildAllocationMachine(m, devices, cache)
	if cache != nil {
		if cache.allocations == nil {
			cache.allocations = map[allocationViewKey]*Machine{}
		}
		cache.allocations[key] = target
	}
	return target
}

func buildAllocationMachine(m *Machine, devices []string, cache *memoryPlanningCache) *Machine {
	if m != nil && m.Devices != nil && len(devices) > 1 {
		members := []*Machine{}
		for _, id := range devices {
			if card := allocationMachineCached(m, []string{id}, cache); card != m {
				members = append(members, card)
			}
		}
		if len(members) == len(devices) {
			return groupView(members)
		}
	}
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

func (q *Queue) checkinMachineAttempts(m *Machine, reports []AttemptReport, progresses []attemptProgress, recovering bool, reply *Reply) {
	defer q.bodyPhaseFinish("placement", q.bodyPhaseStart())
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
	for _, j := range q.liveJobs() {
		if j.Machine != m.ID || j.Session != m.Session || seen[jobKey(*j)] {
			continue
		}
		if j.State == "stopping" {
			if j.ReleaseConfirmed {
				q.retry(j, j.Reason)
				continue
			}
			reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: max(0, j.StopAt.Sub(q.clock.Now()).Seconds()), PriorityStop: priorityStop(j), UserCancel: cancelled(j)})
		} else if !recovering && j.State == "running" && !j.ReleaseConfirmed && !q.maintenanceBlocked(hostID(m)) && logPolicyFits(j.Spec.Logs, m) && sharedInputsFit(j.Spec, m) && q.checkpointReady(j, m) {
			out := cloneJob(j)
			out.LedgerEntries = nil
			reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: &out})
		}
	}
	// Let supervisors drain immediate-cleanup reports before offering more work.
	// Otherwise startup recovery can keep accepting and refusing replacements,
	// never reaching its empty collection and fresh-session handoff.
	if m.Leaving {
		q.skipAdmissionSelection(m, predicateLeaving)
		active := len(reports) > 0
		for _, j := range q.liveJobs() {
			active = active || j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") && !j.ReleaseConfirmed
		}
		if !active && len(q.modelPlan(m.ID)) == 0 {
			m.Gone = true
			m.Models = nil
		}
		return
	}
	// Refuse admission before assign creates an attempt or history. Refunding
	// an offered identity would allow stale results to match a reused attempt.
	if recovering || cleanup {
		if recovering {
			q.skipAdmissionSelection(m, predicateRecovering)
		} else {
			q.skipAdmissionSelection(m, predicateCleanup)
		}
		return
	}
	q.skipAdmissionSelection(m, predicateSelectionNotReached)
	var best *Job
	var target *Machine
	scan := q.placementScan()
	for _, candidate := range q.placementTargets(m) {
		j := q.plannedPick(candidate, scan)
		if j != nil && q.batchSlotFree(candidate, j) && (best == nil || q.candidates(candidate, scan).before(j, best)) {
			best, target = j, candidate
		}
	}
	if best != nil {
		out := q.assign(best, target, scan)
		reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: out.ID, Attempt: out.Attempt}, Session: out.Session, Job: &out})
	}
}

// Historical records carry explicit migration guidance; names are never parsed
// or used as aliases. Retained state still carries retired source-worker identities.
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
		for _, j := range q.liveJobs() {
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
			if r.Machine == m.ID && r.onDevice(d.ID) {
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
		return !q.workerHasOtherAttempt(m.ID, m.Session, j.ID, copy.Attempt)
	}
	if _, retained := q.s.HostReleases[releaseKey(HostRelease{Job: j.ID, Machine: m.ID, Session: copy.Session, Attempt: copy.Attempt})]; !retained {
		return false
	}
	targets := q.machineTargets(m)
	if len(copy.AllocatedDevices) > 1 {
		cards := slices.DeleteFunc(slices.Clone(targets), func(t *Machine) bool { return t.GPU == "cpu" || !slices.Contains(copy.AllocatedDevices, t.Device) })
		if len(cards) != len(copy.AllocatedDevices) {
			return false
		}
		targets = []*Machine{groupView(cards)}
	}
	for _, target := range targets {
		if j.Spec.Needs.cpuOnly() && target.GPU == "cpu" || len(copy.AllocatedDevices) > 0 && target.GPU != "cpu" && slices.Equal(targetDevices(target), copy.AllocatedDevices) {
			own := HostRelease{Job: j.ID, Machine: m.ID, Session: copy.Session, Attempt: copy.Attempt}
			_, shared := q.deviceCoTenants(target, j, false)
			shared = shared && !target.Gone && !target.Leaving && !q.unreported[target.ID]
			return (q.workerBatchIdleExcept(target, &own) || shared) && target.CPUCapacityReason == "" && target.CPUInUseCores <= target.EffectiveCPUCores && target.hostReservationDebtGB == 0 && target.modelReservationDebtGB == 0 && target.MemoryError == "" && deviceAllowed(j, target)
		}
	}
	return false
}
