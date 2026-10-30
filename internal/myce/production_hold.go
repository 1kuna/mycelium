package myce

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Policies are keyed by physical host_id (worker name when host_id is unset).
type HostPolicy struct {
	ProductionHold bool `json:"production_hold" yaml:"production_hold"`
}
type HostState struct {
	Policy HostPolicy `json:"policy"`
	Active bool       `json:"active,omitempty"`
}

// Release obligations survive requeue/loss; only an execution acknowledgement or
// a fresh worker's completed cleanup can settle them.
type HostRelease struct {
	// Omitted older records retain conservative GPU-slot exclusion.
	CPUOnly                     bool       `json:"cpu_only,omitempty"`
	CPUCores                    int        `json:"cpu_cores,omitempty"`
	CleanupSince                *time.Time `json:"cleanup_since,omitempty"`
	EscalatedAt                 *time.Time `json:"escalated_at,omitempty"`
	HostChargeGB                float64    `json:"host_charge_gb,omitempty"`
	Docker                      bool       `json:"docker,omitempty"`
	CleanupFailed               bool       `json:"cleanup_failed,omitempty"`
	Device                      string     `json:"device,omitempty"`
	MemoryGB, HostMemoryGB      float64
	Preparation                 bool `json:"preparation,omitempty"`
	Host, Job, Machine, Session string
	Attempt                     int
	Until                       time.Time
}

func ReadHostPolicies(path string) (policies map[string]HostPolicy, err error) {
	defer func() { err = policyFileError(path, err) }()
	if path == "" {
		return nil, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var v struct {
		Hosts map[string]HostPolicy `yaml:"hosts"`
	}
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&v); e != nil {
		return nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("expected one host policy document")
	}
	return v.Hosts, nil
}
func validateHostPolicies(p map[string]HostPolicy) error {
	for host := range p {
		if strings.TrimSpace(host) == "" {
			return errors.New("host name is required")
		}
	}
	return nil
}

func (q *Queue) ConfigureHosts(p map[string]HostPolicy) error {
	if err := validateHostPolicies(p); err != nil {
		return err
	}
	normalized := map[string]HostPolicy{}
	for host, v := range p {
		normalized[host] = v
	}
	return q.update(func() error {
		for h := range normalized {
			if err := q.validateMachineNames([]string{h}); err != nil {
				return err
			}
		}
		if len(q.s.Hosts) > 0 {
			saved := map[string]HostPolicy{}
			for h, s := range q.s.Hosts {
				saved[h] = s.Policy
			}
			if !reflect.DeepEqual(saved, normalized) {
				return errors.New("host configuration differs from saved policy; refusing to relax or replace it")
			}
			return nil
		}
		q.s.Hosts = map[string]*HostState{}
		for h, v := range normalized {
			q.s.Hosts[h] = &HostState{Policy: v}
		}
		return nil
	})
}
func (q *Queue) held(host string) bool {
	s := q.s.Hosts[host]
	return s != nil && s.Policy.ProductionHold && s.Active
}
func releaseKey(r HostRelease) string {
	return fmt.Sprintf("%s/%s/%s/%d", r.Job, r.Machine, r.Session, r.Attempt)
}
func (q *Queue) retainRelease(j *Job) {
	if j.ReleaseConfirmed {
		return
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m == nil {
		return
	}
	policy := q.s.Hosts[hostID(m)]
	if j.Spec.Needs.CPUCores == 0 && (m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs == 0) && !q.maintenanceBlocked(hostID(m)) && hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) == 0 && j.Spec.StartBy == nil && !j.Spec.Run.Docker && m.HostMemoryBudgetGB == 0 && m.hostBudgetGB == 0 && q.machineJobLimit(m) == 0 && (policy == nil || !policy.Policy.ProductionHold) {
		return
	}
	if q.s.HostReleases == nil {
		q.s.HostReleases = map[string]HostRelease{}
	}
	r := HostRelease{CPUOnly: j.Spec.Needs.cpuOnly(), CPUCores: j.Spec.Needs.cores(), HostChargeGB: hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), Docker: j.Spec.Run.Docker, Device: m.Device, MemoryGB: j.Spec.Needs.MemoryGB, HostMemoryGB: j.Spec.Needs.HostMemoryGB, Host: hostID(m), Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}
	if j.StopAt != nil {
		r.Until = *j.StopAt
	}
	old := q.s.HostReleases[releaseKey(r)]
	r.CleanupFailed = old.CleanupFailed
	r.CleanupSince = old.CleanupSince
	r.EscalatedAt = old.EscalatedAt
	// A new stop resets an un-escalated boot/create timer (#74), but must
	// preserve an existing escalation until the release resolves.
	if r.Docker && (q.dockerReleaseLive(r) || old.EscalatedAt == nil && old.Until.IsZero() && !r.Until.IsZero()) {
		r.CleanupSince, r.EscalatedAt = nil, nil
	}
	q.s.HostReleases[releaseKey(r)] = r
}
func (q *Queue) settleRelease(r Result) bool {
	q.confirmCopyRelease(r)
	key := releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})
	_, ok := q.s.HostReleases[key]
	delete(q.s.HostReleases, key)
	return ok
}
func (q *Queue) reconcileHostReleases(c Checkin) {
	q.confirmPreviousCopies(c)
	for key, r := range q.s.HostReleases {
		if r.Docker || r.Machine != c.Machine.ID {
			continue
		}
		old := q.s.Machines[c.Machine.ID]
		fresh := c.Fresh && old != nil && c.PreviousSession == old.Session && c.Machine.Session != old.Session && r.Session == c.PreviousSession
		stopped := c.AttemptProtocol == "" && r.Session == c.Machine.Session && c.Running == nil && !r.Until.IsZero() && !r.Preparation && !r.CleanupFailed
		if r.Preparation {
			stopped = r.Session == c.Machine.Session && !q.backgroundPreparing(&c.Machine)
		}
		if fresh || stopped {
			q.confirmCopyRelease(Result{Execution: Execution{ID: r.Job, Attempt: r.Attempt}, Machine: r.Machine, Session: r.Session})
			delete(q.s.HostReleases, key)
		}
	}
}

// Placement policy is shared by batch, model, preparation and projections.
func (q *Queue) placementAllowed(j *Job, m *Machine) bool {
	return q.placementAllowedForUse(j, m, false)
}

func (q *Queue) placementAllowedForUse(j *Job, m *Machine, resident bool) bool {
	return (j.Spec.Needs.cpuOnly() || !q.deviceAdmissionBlocked(m)) && q.diskHeadroomReason(j, m) == "" && q.placementPolicyAllowed(j, m, resident)
}

// Recovery retains existing execution custody. Admission-time disk estimates
// apply only to new work: its writes may already be reflected in observed free.
func (q *Queue) placementPolicyAllowed(j *Job, m *Machine, resident bool) bool {
	if j.State == "queued" && (j.Spec.Priority == "production" || j.Spec.Class == "production") && q.fleetPausesProduction() {
		return false
	}
	if !dependencyReady(j) || m.Recovering || cancelled(j) || !dockerRuntimeFits(j, m) || q.maintenanceBlocked(hostID(m)) || m.HostMemoryPolicyMismatch || !logPolicyFits(j.Spec.Logs, m) {
		return false
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		if resident {
			model = q.residentModel(model, m)
		}
		if !logPolicyFits(model.Logs, m) || !modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID) {
			return false
		}
	}

	if availabilityBlocked(m) || (q.machineLimitReason(m) != "" && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping"))) || q.startupReleaseBlocker(hostID(m)) != "" || q.dockerBlocker(hostID(m)) != "" || !q.jobAllowsMachine(j, m.ID) {
		return false
	}
	if j.ProductionHost != "" && j.ProductionHost != hostID(m) {
		return false
	}
	if !q.held(hostID(m)) {
		return true
	}
	if j.Spec.Priority != "production" {
		return false
	}
	return q.hostBlockers(hostID(m)) == ""
}
func (q *Queue) hostBlockers(host string) string {
	text, _ := q.hostBlockerState(host)
	return text
}

// Unknown release gets attention only after ordinary kill/report latency.
const releaseAttentionDelay = 2 * time.Minute

type hostBlockerClass struct {
	Kind                   string
	OperatorActionRequired bool
}

func (q *Queue) releaseNeedsOperator(m *Machine, until *time.Time, failed bool) bool {
	if m == nil || m.Gone || failed {
		return true
	}
	now := q.clock.Now()
	if q.unreported[m.ID] && now.Before(q.restartUntil) {
		return false
	}
	return until != nil && !until.IsZero() && !now.Before(until.Add(releaseAttentionDelay))
}

func (q *Queue) hostBlockerState(host string) (string, hostBlockerClass) {
	count := 0
	remaining := 0.0
	unknown := []string{}
	needsOperator := false
	for _, r := range q.s.HostReleases {
		if r.Host != host {
			continue
		}
		// Live production is demand, not a blocker for another production job.
		j := q.s.Jobs[r.Job]
		if j != nil && j.Spec.Priority == "production" && j.Machine == r.Machine && j.Attempt == r.Attempt && (j.State == "running" || j.State == "stopping") && !q.unreported[r.Machine] && q.s.Machines[r.Machine] != nil && !q.s.Machines[r.Machine].Gone {
			continue
		}
		count++
		remaining = max(remaining, r.Until.Sub(q.clock.Now()).Seconds())
		if m := q.s.Machines[r.Machine]; m == nil || m.Gone || q.unreported[r.Machine] || r.CleanupFailed || r.Until.IsZero() || !q.clock.Now().Before(r.Until) {
			unknown = append(unknown, r.Machine)
			needsOperator = needsOperator || q.releaseNeedsOperator(m, &r.Until, r.CleanupFailed)
		}
	}
	for _, c := range q.calls {
		m := q.s.Machines[c.Machine]
		if m != nil && hostID(m) == host && c.Priority != "production" {
			count++
			if c.StopAt != nil {
				remaining = max(remaining, c.StopAt.Sub(q.clock.Now()).Seconds())
				if expired(c.StopAt, q.clock.Now()) {
					unknown = append(unknown, m.ID)
					needsOperator = needsOperator || q.releaseNeedsOperator(m, c.StopAt, false)
				}
			}
		}
	}
	for _, s := range q.s.ModelSlots {
		m := q.s.Machines[s.Machine]
		if m != nil && hostID(m) == host && (s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || !q.slotPinned(s) && s.State == "draining" || s.State == "unloading" || s.State == "unload_failed") {
			count++
			if s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || s.State == "unload_failed" || expired(s.StopAt, q.clock.Now()) {
				unknown = append(unknown, m.ID)
				needsOperator = needsOperator || q.releaseNeedsOperator(m, s.StopAt, s.State == "unload_failed")
			}
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return "release unknown: worker " + strings.Join(unknown, ", "), hostBlockerClass{Kind: "production_release_unknown", OperatorActionRequired: needsOperator}
	}
	if count > 0 {
		return fmt.Sprintf("stopping %d attempts; grace remaining %.0fs", count, remaining), hostBlockerClass{Kind: "production_stopping"}
	}
	return "", hostBlockerClass{}
}
func (q *Queue) holdReason(host string) string { return q.productionHoldDetail(host).Text }
func (q *Queue) productionHoldDetail(host string) HoldDetail {
	detail := HoldDetail{Host: host, Kind: "production_hold", Text: host + ": production hold"}
	if b, class := q.hostBlockerState(host); b != "" {
		return productionBlockerDetail(host, b, class)
	}
	return detail
}

// Classification is explicit: new blocker paths cannot silently become routine.
func productionBlockerDetail(host, text string, class hostBlockerClass) HoldDetail {
	detail := HoldDetail{Host: host, Kind: "unclassified", OperatorActionRequired: true, Text: host + ": production hold; " + text}
	switch class.Kind {
	case "production_stopping", "production_release_unknown":
		detail.Kind = class.Kind
		detail.OperatorActionRequired = class.OperatorActionRequired
	}
	return detail
}

func (q *Queue) holdWaitReason(j *Job) string {
	if reason := q.diskWaitingReason(j, j.ProductionHost); reason != "" {
		return reason
	}
	for _, m := range q.plannerMachines() {
		if q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			if b := q.dockerBlocker(hostID(m)); b != "" {
				return b
			}
		}
	}
	if j.ProductionHost != "" && q.held(j.ProductionHost) {
		if reason := q.productionOccupancyReason(j); reason != "" {
			return reason + "; production hold bound to " + j.ProductionHost + "; spillover requires production_parity_hosts approval and an immediately admissible qualified host"
		}
		return q.holdReason(j.ProductionHost)
	}
	if j.Spec.Priority == "production" && j.ProductionHost == "" {
		for _, m := range q.plannerMachines() {
			h := q.s.Hosts[hostID(m)]
			if h == nil || !h.Policy.ProductionHold || !q.jobAllowsMachine(j, m.ID) || !fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
				continue
			}
			for _, slot := range q.s.ModelSlots {
				if slotOnTarget(slot, m) && q.slotPinned(slot) && q.comparePriority(q.slotSpec(slot), j.Spec) <= 0 && slot.Model != j.Spec.Needs.Model {
					model, _ := q.model(slot.Model)
					if model.MemoryGB+j.Spec.Needs.MemoryGB > m.TotalMemoryGB {
						return "pinned model " + slot.Model + " on " + m.ID + " prevents production placement"
					}
				}
			}
		}
	}
	if j.Spec.Priority != "production" {
		for _, m := range q.plannerMachines() {
			if q.held(hostID(m)) && q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
				return "host " + hostID(m) + " reserved for production"
			}
		}
	}
	return ""
}

// A bound host stays authoritative unless equal-or-higher production prevents
// admission. Lower tiers, preparation and unresolved custody use the existing
// release planner; they do not authorize occupancy spillover.
func (q *Queue) productionOccupancyReason(j *Job) string {
	if j.State != "queued" || j.Spec.Priority != "production" || j.ProductionHost == "" {
		return ""
	}
	reason := ""
	for _, m := range q.plannerMachines() {
		if hostID(m) != j.ProductionHost || !q.placementAllowed(j, m) {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			if q.workerIdle(m, candidate) && !q.preparationActive(m) && !q.preparationStopping(m) && hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) && fitsResources(candidate, m, m.FreeMemoryGB) && environmentReady(candidate, m) {
				return "" // Another target on the bound host can admit it now.
			}
			if q.roomPlan(candidate, m, true) != nil {
				continue // Ordinary lower-tier reclamation can make room.
			}
			deviceBusy, productionRAM := false, false
			for _, r := range q.liveJobs() {
				owner := allocationMachine(q.s.Machines[r.Machine], r.AllocatedDevices)
				if owner == nil || hostID(owner) != j.ProductionHost || r.ReleaseConfirmed || r.Spec.Priority != "production" || q.comparePriority(r.Spec, j.Spec) > 0 || r.State != "running" && r.State != "stopping" {
					continue
				}
				deviceBusy = deviceBusy || batchOnTarget(r, m)
				productionRAM = productionRAM || hostNeed(owner, r.Spec.Needs.MemoryGB, r.Spec.Needs.HostMemoryGB) > 0
			}
			for _, s := range q.s.ModelSlots {
				owner := slotMachine(q.s.Machines[s.Machine], s)
				if owner == nil || hostID(owner) != j.ProductionHost || q.slotSpec(s).Priority != "production" || q.comparePriority(q.slotSpec(s), j.Spec) > 0 || !slotReservesMemory(s) || q.slotReclaimableFor(s, j.Spec) {
					continue
				}
				model, _ := q.model(s.Model)
				deviceBusy = deviceBusy || slotOnTarget(s, m) && m.FreeMemoryGB < candidate.Spec.Needs.MemoryGB
				productionRAM = productionRAM || hostNeed(owner, model.MemoryGB, model.HostMemoryGB) > 0
			}
			if productionRAM && hostFitFailure(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) == "host memory" {
				reason = fmt.Sprintf("waiting for RAM on %s: %.1f GiB needed, %.1f GiB admission headroom; equal-or-higher production reserves host RAM", m.ID, hostNeed(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB), m.FreeHostMemoryGB)
			} else if deviceBusy && !strings.HasPrefix(reason, "waiting for RAM") {
				reason = "waiting for admission on " + m.ID + ": device occupied by equal-or-higher production"
			}
		}
	}
	return reason
}

// Reuse dispatch's qualified, frozen extension and admission checks on a copy.
// This changes only queued host affinity; it creates no release or attempt.
func (q *Queue) productionSpilloverTarget(j *Job) string {
	approved := q.s.Policies[j.Spec.Project].ProductionParityHosts[j.Spec.workType()]
	if len(approved) == 0 || !j.Spec.extensionJob() || j.Spec.ExtensionCheck != nil || q.productionOccupancyReason(j) == "" {
		return ""
	}
	copy := *j
	copy.ProductionHost = ""
	scan := q.placementScan()
	var best *Machine
	now := q.clock.Now()
	for _, m := range scan.machines {
		if hostID(m) == j.ProductionHost || !contains(approved, hostID(m)) || q.unreported[m.ID] || m.LastSeen.After(now) || now.Sub(m.LastSeen) >= min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])) {
			continue
		}
		candidate := q.batchCandidateWithResources(&copy, m, nil)
		if candidate == nil || !q.workerIdle(m, candidate) || q.immediateCandidate(candidate, m) == nil {
			continue
		}
		if best == nil || q.targetPlacementBetter(j, m, best, scan) {
			best = m
		}
	}
	if best == nil {
		return ""
	}
	return hostID(best)
}

// Select once, before stopping anything. Occupancy is reclaimable; immutable
// capacity, environments, project/job restrictions and pins are not.
func (q *Queue) productionTarget(j *Job, call *ModelCall) string {
	scan := q.placementScan()
	machines := scan.machines
	var best *Machine
	bestStops := int(^uint(0) >> 1)
	bestFree := false
	for _, m := range machines {
		capacity := *m
		diskReason := ""
		if call == nil {
			diskReason = q.diskHeadroomReason(j, m)
		} else {
			// Calls reuse residency; only a new load can need cache writes.
			capacity.DiskVolumes = nil
			model, _ := q.model(call.Model)
			diskReason = q.modelLoadDiskHeadroomReason(model, m)
		}
		if diskReason != "" || q.dockerBlocker(hostID(m)) != "" || q.unreported[m.ID] || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			if !fitsResources(candidate, &capacity, m.TotalMemoryGB) {
				continue
			}
			model, hasModel := q.model(candidate.Spec.Needs.Model)
			if hasModel && call != nil {
				model = q.residentModel(model, m)
			}
			if hasModel && !q.modelFitsMachine(model, m) {
				continue
			}
			if call != nil && (m.ModelURL == "" || q.callExcludes(call, m.ID, targetDevice(m))) {
				continue
			}
			pinned := 0.0
			for _, s := range q.s.ModelSlots {
				if slotOnTarget(s, m) && (q.slotPinned(s) && q.comparePriority(q.slotSpec(s), candidate.Spec) <= 0 || s.State == "unload_failed") {
					model, _ := q.model(s.Model)
					if call == nil || s.Model != call.Model {
						pinned += model.MemoryGB
					}
				}
			}
			if m.TotalMemoryGB-pinned < candidate.Spec.Needs.MemoryGB {
				continue
			}
			slot := q.s.ModelSlots[targetSlotKey(m, candidate.Spec.Needs.Model)]
			// Calls reuse a ready/loading model's existing host-memory reservation.
			if call == nil || slot == nil || slot.State != "ready" && slot.State != "loading" {
				if _, _, ok := q.hostReleases(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB, "production", candidate.Spec); !ok {
					continue
				}
			}
			stops := 0
			for _, r := range q.liveJobs() {
				if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
					stops++
				}
			}
			// A hold is a custody commitment even when production is already busy.
			// Value only ranks immediately free fitting alternatives; the common
			// release planner still governs actual preemption after binding.
			freeNow := q.workerIdle(m, candidate) && !q.preparationActive(m) && !q.preparationStopping(m) && hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) && fitsResources(candidate, m, m.FreeMemoryGB)
			preferFreeTarget := best != nil && stops == 0 && bestStops == 0 && freeNow && (!bestFree || q.targetPlacementBetter(j, m, best, scan))
			if best == nil || stops < bestStops || preferFreeTarget {
				best, bestStops, bestFree = m, stops, freeNow
			}
		}
	}
	if best == nil {
		return ""
	}
	return hostID(best)
}
func (q *Queue) maintainHolds() {
	enabled := false
	for _, h := range q.s.Hosts {
		enabled = enabled || h.Policy.ProductionHold
	}
	if !enabled {
		return
	}
	now := q.clock.Now()
	demand := map[string]bool{}
	jobs := []*Job{}
	for _, j := range q.liveJobs() {
		if j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next > 0 {
			continue
		}
		if dependencyReady(j) && j.Spec.Priority == "production" && !terminal(j.State) && !cancelled(j) && !(j.State == "queued" && expired(j.Spec.GiveUpAt, now)) {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(a, b int) bool { return q.before(jobs[a], jobs[b], &Machine{}) })
	for _, j := range jobs {
		if target := q.productionSpilloverTarget(j); target != "" {
			j.ProductionHost = target
		}
		if j.State == "queued" && j.ProductionHost != "" && (q.dockerBlocker(j.ProductionHost) != "" || q.diskWaitingReason(j, j.ProductionHost) != "") {
			if target := q.productionTarget(j, nil); target != "" {
				j.ProductionHost = target
			}
		}
		if j.ProductionHost == "" {
			if m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); m != nil {
				if h := q.s.Hosts[hostID(m)]; h != nil && h.Policy.ProductionHold {
					j.ProductionHost = hostID(m)
				}
			} else {
				target := q.productionTarget(j, nil)
				if h := q.s.Hosts[target]; h != nil && h.Policy.ProductionHold {
					j.ProductionHost = target
				}
			}
		}
		demand[j.ProductionHost] = true
	}
	for _, c := range q.calls {
		if c.Priority == "production" && c.Error == "" {
			if c.ProductionHost == "" {
				j := callJob(c)
				model, _ := q.model(c.Model)
				j.Spec.Needs.MemoryGB = model.MemoryGB
				j.Spec.Needs.HostMemoryGB = model.HostMemoryGB
				j.Spec.Needs.Platform = Platform{Required: model.Platforms}
				target := q.productionTarget(j, c)
				if h := q.s.Hosts[target]; h != nil && h.Policy.ProductionHold {
					c.ProductionHost = target
				}
			}
			demand[c.ProductionHost] = true
		}
	}
	for host, h := range q.s.Hosts {
		if !h.Policy.ProductionHold {
			continue
		}
		if demand[host] {
			h.Active = true
		}
		if !h.Active {
			continue
		}
		for _, m := range q.s.Machines {
			if hostID(m) == host && q.backgroundPreparing(m) {
				if q.s.HostReleases == nil {
					q.s.HostReleases = map[string]HostRelease{}
				}
				r := HostRelease{Host: host, Job: "preparation", Machine: m.ID, Session: m.Session, Preparation: true, Until: now.Add(q.productionGrace)}
				key := releaseKey(r)
				if _, exists := q.s.HostReleases[key]; !exists {
					q.s.HostReleases[key] = r
				}
			}
		}
		for _, j := range q.liveJobs() {
			m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
			if m != nil && hostID(m) == host && (j.State == "running" || j.State == "stopping") {
				if q.comparePriority(j.Spec, q.hostPrioritySpec(host)) > 0 {
					q.stopBatchFor(j, q.hostPreemptionDemand(host))
				}
				q.retainRelease(j)
			}
		}
		for _, c := range q.calls {
			m := q.s.Machines[c.Machine]
			if m != nil && hostID(m) == host && q.comparePriority(callJob(c).Spec, q.hostPrioritySpec(host)) > 0 && c.Error == "" {
				if slot := q.s.ModelSlots[slotKey(c.Machine, c.Model, c.Device)]; slot != nil {
					if slot.PriorityStop == nil && q.preemptionProtected(slot.PreemptionCount, slot.RunSince) {
						continue
					}
					slot.ReleasingCalls = true
				}
				if c.StopAt == nil {
					c.PriorityStop = q.preemption(q.hostPreemptionDemand(host), c.ID)
					t := now.Add(q.productionGrace)
					c.StopAt = &t
				}
				if expired(c.StopAt, now) {
					c.Error = "preempted by production host hold"
				}
			}
		}
		for _, s := range q.s.ModelSlots {
			m := q.s.Machines[s.Machine]
			if m == nil || hostID(m) != host || q.slotPinned(s) && q.comparePriority(q.slotSpec(s), q.hostPrioritySpec(host)) <= 0 || s.State == "failed" {
				continue
			}
			if q.comparePriority(q.slotSpec(s), q.hostPrioritySpec(host)) > 0 && !q.slotReclaimableFor(s, q.hostPrioritySpec(host)) {
				continue
			}
			if s.State != "unload_failed" && s.State != "unloading" && (q.comparePriority(q.slotSpec(s), q.hostPrioritySpec(host)) > 0 && (!s.ProductionUsed || s.InFlight > 0) || q.unreported[m.ID]) {
				s.State = "draining"
			}
			if s.State == "draining" {
				if q.comparePriority(q.slotSpec(s), q.hostPrioritySpec(host)) > 0 && s.PriorityStop == nil {
					q.recordEnginePreemption(s, q.hostPreemptionDemand(host))
				}
				if s.StopAt == nil {
					t := now.Add(q.productionGrace)
					s.StopAt = &t
				}
				if expired(s.StopAt, now) {
					for _, c := range q.calls {
						if c.Machine == s.Machine && c.Model == s.Model && c.Priority != "production" {
							c.Error = "preempted by production host hold"
						}
					}
				}
			}
		}
		unsettled := false
		for _, r := range q.s.HostReleases {
			if r.Host == host {
				unsettled = true
			}
		}
		for _, s := range q.s.ModelSlots {
			m := q.s.Machines[s.Machine]
			if m != nil && hostID(m) == host && (s.ReleasingCalls || s.Recovering || m.Gone || q.unreported[m.ID] || s.State == "loading" || s.State == "draining" || s.State == "unloading" || s.State == "unload_failed" || s.InFlight > 0) {
				unsettled = true
			}
		}
		if demand[host] || unsettled {
			continue
		}
		// Ready idle residency remains available under the engine's own idle
		// timer. Only actual demand or unresolved releases keep takeover.
		h.Active = false
	}
}

func (q *Queue) backgroundPreparing(m *Machine) bool {
	if m.BenchmarkPending {
		return true
	}
	for _, r := range m.EnvironmentReports {
		if r.State == "building" {
			production := false
			for _, j := range q.liveJobs() {
				if j.State == "queued" && j.Spec.Priority == "production" && j.ProductionHost == hostID(m) && q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) && environmentKey(j, m) == r.Key {
					production = true
					break
				}
			}
			if !production {
				return true
			}
		}
	}
	for _, r := range m.CopyReports {
		if r.State == "copying" {
			production := false
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && s.Model == r.Model && s.Priority == "production" {
					production = true
				}
			}
			if !production {
				return true
			}
		}
	}
	return false
}

// Lost/stopping Docker executions fence the physical host, even without host policy.
func (q *Queue) dockerBlocker(host string) string { return q.dockerHoldDetail(host).Text }

func (q *Queue) dockerHoldDetail(host string) HoldDetail {
	best := HoldDetail{}
	severity := 0
	choose := func(detail HoldDetail, rank int) {
		if rank > severity || rank == severity && detail.Text < best.Text {
			best, severity = detail, rank
		}
	}
	for _, m := range q.s.Machines {
		if hostID(m) != host || m.DockerCleanup == "" {
			continue
		}
		kind, rank, action := "unclassified", 4, true
		if strings.HasPrefix(m.DockerCleanup, "cleanup unconfirmed:") {
			kind, rank, action = "docker_cleanup_waiting", 3, false
		}
		switch m.DockerCleanup {
		case "creating", "stopping", "released", "create pending", "start pending", "wait pending", "inspect pending", "stop pending", "kill pending", "logs pending", "remove pending", "network-create pending", "network-remove pending":
			kind, rank, action = "docker_transition", 1, false
		}
		choose(HoldDetail{Host: host, Kind: kind, OperatorActionRequired: action, Text: host + ": Docker cleanup unconfirmed; " + m.DockerCleanup}, rank)
	}
	for _, r := range q.s.HostReleases {
		if !r.Docker || r.Host != host || q.dockerReleaseLive(r) {
			continue
		}
		detail := HoldDetail{Host: host, Kind: "docker_cleanup_waiting", Text: host + ": Docker cleanup unconfirmed; waiting for attempt " + r.Job + fmt.Sprintf("/%d", r.Attempt)}
		rank := 2
		if r.EscalatedAt != nil {
			detail.Kind = "docker_cleanup_operator_action"
			detail.OperatorActionRequired = true
			detail.Text += "; operator action required (myce resolve-docker)"
			rank = 5
		}
		choose(detail, rank)
	}
	return best
}

// Escalation bounds silent waiting, never custody or physical memory ownership.
func (q *Queue) escalateDockerCleanup() {
	now := q.clock.Now()
	for key, r := range q.s.HostReleases {
		if !r.Docker {
			continue
		}
		if q.dockerReleaseLive(r) {
			r.CleanupSince, r.EscalatedAt = nil, nil
			q.s.HostReleases[key] = r
			continue
		}
		if r.CleanupSince == nil || !r.Until.IsZero() && r.CleanupSince.Before(r.Until) {
			since := now
			// Stopped attempts retain ordinary grace before the cleanup bound.
			if !r.Until.IsZero() {
				since = r.Until
			}
			r.CleanupSince = &since
		}
		if r.EscalatedAt == nil && !now.Before(r.CleanupSince.Add(2*time.Minute)) {
			r.EscalatedAt = &now
			log.Printf("event=docker_cleanup_escalated host=%q job=%q attempt=%d machine=%q session=%q operator_action_required=true", r.Host, r.Job, r.Attempt, r.Machine, r.Session)
		}
		q.s.HostReleases[key] = r
	}
}

func (q *Queue) dockerReleaseLive(r HostRelease) bool {
	j := q.s.Jobs[r.Job]
	m := q.s.Machines[r.Machine]
	return j != nil && j.Attempt == r.Attempt && j.Session == r.Session && j.Machine == r.Machine && j.State == "running" && j.StopAt == nil && m != nil && m.Session == r.Session && !m.Gone && !q.unreported[m.ID] && dockerStartingOrRunning(m.DockerCleanup) && !r.CleanupFailed
}

// Only healthy startup/running reports keep the exact current execution live.
// Stopping, errors, replacement sessions and lost workers retain custody.
func dockerStartingOrRunning(status string) bool {
	switch status {
	case "", "creating", "create pending", "network-create pending", "start pending":
		return true
	default:
		return false
	}
}
