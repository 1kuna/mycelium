package myce

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"maps"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

// Policies are keyed by physical host_id (worker name when host_id is unset).
// The default host scope fences the whole host for production. Device scope
// keeps the hold's release custody but leaves exclusivity to ordinary
// per-resource priority preemption, so siblings and the CPU stay usable.
type HostPolicy struct {
	ProductionHold      bool   `json:"production_hold" yaml:"production_hold"`
	ProductionHoldScope string `json:"production_hold_scope,omitempty" yaml:"production_hold_scope,omitempty"`
}

func (p HostPolicy) hostWide() bool { return p.ProductionHold && p.ProductionHoldScope == "" }

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

	// Every card of a multi-card attempt; MemoryGB is then each card's share.
	Devices []string `json:"devices,omitempty"`
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
	_, err := normalizeHostPolicies(p)
	return err
}

// An explicit host scope is the default, so it compares equal to an omitted one.
func normalizeHostPolicies(p map[string]HostPolicy) (map[string]HostPolicy, error) {
	out := map[string]HostPolicy{}
	for host, v := range p {
		if strings.TrimSpace(host) == "" {
			return nil, errors.New("host name is required")
		}
		switch v.ProductionHoldScope {
		case "", "host":
			v.ProductionHoldScope = ""
		case "device":
			if !v.ProductionHold {
				return nil, fmt.Errorf("host %s: production_hold_scope requires production_hold: true", host)
			}
		default:
			return nil, fmt.Errorf("host %s: production_hold_scope must be host or device", host)
		}
		out[host] = v
	}
	return out, nil
}

func (q *Queue) hostPolicies() map[string]HostPolicy {
	out := map[string]HostPolicy{}
	for h, s := range q.s.Hosts {
		out[h] = s.Policy
	}
	return out
}

// Unchanged hosts keep their activation; a changed policy starts inactive and
// the next maintenance pass re-derives it from live demand. Release custody is
// keyed by attempt, so it survives the change.
func (q *Queue) replaceHostPolicies(p map[string]HostPolicy) {
	next := map[string]*HostState{}
	for h, v := range p {
		if old := q.s.Hosts[h]; old != nil && old.Policy == v {
			next[h] = old
		} else {
			next[h] = &HostState{Policy: v}
		}
	}
	q.s.Hosts = next
}

// The startup file seeds host policy once; afterwards `myce policy set` owns it.
// Restarting with the last accepted file keeps the live policy. A different
// file is still refused, so an edit can never silently relax a hold.
func (q *Queue) ConfigureHosts(p map[string]HostPolicy) error {
	normalized, err := normalizeHostPolicies(p)
	if err != nil {
		return err
	}
	return q.update(func() error {
		if err := q.validateMachineNames(slices.Collect(maps.Keys(normalized))); err != nil {
			return err
		}
		// With no live or recorded hold, a file only adds holds, so it seeds as before.
		if len(q.s.Hosts) > 0 || len(q.s.HostConfig) > 0 {
			switch {
			case reflect.DeepEqual(q.hostPolicies(), normalized):
				q.s.HostConfig = normalized
			case !reflect.DeepEqual(q.s.HostConfig, normalized):
				return errors.New("host configuration differs from saved policy; refusing to relax or replace it; change live host policy with myce policy set")
			}
			return nil
		}
		q.s.HostConfig = normalized
		q.replaceHostPolicies(normalized)
		return nil
	})
}
func (q *Queue) held(host string) bool {
	s := q.s.Hosts[host]
	return s != nil && s.Policy.hostWide() && s.Active
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
	if j.Spec.Needs.CPUCores == 0 && (m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs == 0) && !q.maintenanceBlocked(hostID(m)) && hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) == 0 && j.Spec.StartBy == nil && !j.Spec.Run.Docker && m.HostMemoryBudgetGB == 0 && m.EffectiveHostMemoryBudgetGB == 0 && q.machineJobLimit(m) == 0 && (policy == nil || !policy.Policy.ProductionHold) {
		return
	}
	if q.s.HostReleases == nil {
		q.s.HostReleases = map[string]HostRelease{}
	}
	r := HostRelease{CPUOnly: j.Spec.Needs.cpuOnly(), CPUCores: jobCPUCharge(j, m), HostChargeGB: hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), Docker: j.Spec.Run.Docker, Device: m.Device, MemoryGB: deviceShareGB(j), HostMemoryGB: j.Spec.Needs.HostMemoryGB, Host: hostID(m), Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}
	if len(j.AllocatedDevices) > 1 {
		r.Device, r.Devices = j.AllocatedDevices[0], slices.Clone(j.AllocatedDevices)
	}
	if j.StopAt != nil {
		r.Until = *j.StopAt
	}
	old := q.s.HostReleases[releaseKey(r)]
	r.HostChargeGB = max(r.HostChargeGB, q.hostJobCharge(j), retainedHostCharge(j), old.HostChargeGB)
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
func (q *Queue) settleRelease(r Result, exact bool) bool {
	// Superseded copy custody is authoritative without an aggregate host charge.
	// Its stale payload must still commit the exact cleanup acknowledgement.
	// A current terminal result must reach complete's diagnostic publication path.
	knownSupersededCopy := false
	if j := q.retainedJob(r.ID); j != nil {
		for _, c := range j.Copies {
			if c.Attempt == r.Attempt && c.Machine == r.Machine && c.Session == r.Session {
				knownSupersededCopy = j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session
				break
			}
		}
	}
	q.confirmCopyRelease(r, exact)
	key := releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})
	_, ok := q.s.HostReleases[key]
	delete(q.s.HostReleases, key)
	return ok || knownSupersededCopy
}
func (q *Queue) reconcileHostReleases(c Checkin) {
	q.confirmPreviousCopies(c)
	for key, r := range q.s.HostReleases {
		if r.Docker || r.Machine != c.Machine.ID {
			continue
		}
		old := q.s.Machines[c.Machine.ID]
		fresh := c.Fresh && old != nil && c.PreviousSession == old.Session && c.Machine.Session != old.Session && r.Session == c.PreviousSession
		stopped := r.Preparation && r.Session == c.Machine.Session && !q.backgroundPreparing(&c.Machine)
		if fresh || stopped {
			q.confirmCopyRelease(Result{Execution: Execution{ID: r.Job, Attempt: r.Attempt}, Machine: r.Machine, Session: r.Session}, fresh)
			delete(q.s.HostReleases, key)
		}
	}
}

// Placement policy is shared by batch, model, preparation and projections.
func (q *Queue) placementAllowed(j *Job, m *Machine) bool {
	return q.placementAllowedForUse(j, m, false)
}

func (q *Queue) placementAllowedForUse(j *Job, m *Machine, resident bool) bool {
	if !q.admissionPass(j, m, !(j.StartedAt == nil && q.deviceFaultRetryBlocked(j, m)), predicateDeviceFaultRetry) {
		return false
	}
	return q.admissionPass(j, m, j.CommandDiscovery == nil || q.inventoryFresh(q.s.Machines[m.ID]), predicateInventory) && q.admissionPass(j, m, q.yieldPlacementAllows(j, m), predicateYieldPlacement) && q.admissionPass(j, m, j.Spec.Needs.cpuOnly() || !q.deviceAdmissionBlocked(m), predicateDeviceAdmission) && q.admissionHold(j, m, reasonHold("disk_headroom", q.diskHeadroomReason(j, m)), predicateDiskHeadroom) && q.placementPolicyAllowed(j, m, resident)
}

// Started preparation retains demand while contact is suspect. Resource,
// reservation and policy fences still apply; this cannot grant a new assignment.
func (q *Queue) placementDemandAllowed(j *Job, m *Machine) bool {
	if !q.admissionPass(j, m, !(j.StartedAt == nil && q.deviceFaultRetryBlocked(j, m)), predicateDeviceFaultRetry) {
		return false
	}
	return q.admissionPass(j, m, q.yieldPlacementAllows(j, m), predicateYieldPlacement) && q.admissionPass(j, m, j.Spec.Needs.cpuOnly() || !q.deviceAdmissionBlocked(m), predicateDeviceAdmission) && q.admissionHold(j, m, reasonHold("disk_headroom", q.diskHeadroomReason(j, m)), predicateDiskHeadroom) && q.admissionPass(j, m, q.reservationAllows(j, m), predicateReservation) && q.placementDemandPolicy(j, m, false)
}

// Recovery retains existing execution custody. Admission-time disk estimates
// apply only to new work: its writes may already be reflected in observed free.
func (q *Queue) placementPolicyAllowed(j *Job, m *Machine, resident bool) bool {
	return q.admissionPass(j, m, q.reservationAllows(j, m), predicateReservation) && q.placementPolicyWithoutReservation(j, m, resident)
}

func (q *Queue) placementPolicyWithoutReservation(j *Job, m *Machine, resident bool) bool {
	if !q.admissionPass(j, m, !(q.workerSuspect(m, q.clock.Now()) && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping"))), predicateWorkerSuspect) {
		return false
	}
	return q.placementDemandPolicy(j, m, resident)
}

// Useful demand survives temporary contact suspicion. This predicate grants no
// assignment or preparation authority; those paths still require fresh contact.
func (q *Queue) placementDemandPolicy(j *Job, m *Machine, resident bool) bool {
	if !q.admissionHold(j, m, q.variantParityHold(j, m), predicateVariantParity) {
		return false
	}
	if !q.admissionHold(j, m, q.fleetPauseHold(j), predicateProductionPaused) {
		return false
	}
	if (j.StartedAt == nil || j.State == "queued") && (!q.admissionHold(j, m, reasonHold("shared_host_backoff", q.sharedHostBackoffReason(j, m)), predicateSharedHostBackoff) || !q.admissionHold(j, m, reasonHold("storage_root", q.storageRootReason(j, m)), predicateStorageRoot)) {
		return false
	}
	if !q.admissionPass(j, m, dependencyReady(j), predicateDependency) || !q.admissionPass(j, m, !m.Recovering, predicateRecovering) || !q.admissionPass(j, m, !cancelled(j), predicateCancelled) || !q.admissionPass(j, m, dockerRuntimeFits(j, m), predicateDockerRuntime) || !q.admissionPass(j, m, !(q.maintenanceBlocked(hostID(m))), predicateMaintenance) || !q.admissionPass(j, m, logPolicyFits(j.Spec.Logs, m), predicateLogPolicy) {
		return false
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		if resident {
			model = q.residentModel(model, m)
		}
		if !q.admissionPass(j, m, logPolicyFits(model.Logs, m), predicateModelLogPolicy) || !q.admissionPass(j, m, modelFenceAllows(model, m), predicateModelFence) || !q.admissionPass(j, m, q.projectAllows(model.Project, m.ID), predicateModelProject) {
			return false
		}
	}

	if !q.admissionPass(j, m, !availabilityBlocked(m), predicateAvailability) || !q.admissionPass(j, m, !(q.machineCapacityReason(m) != "" && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping"))), predicateMachineCapacity) || !q.admissionHold(j, m, heldBy(q.startupReleaseHold(hostID(m))), predicateStartupRelease) || !q.admissionHold(j, m, heldBy(q.dockerHold(hostID(m))), predicateDockerRelease) || !q.admissionPass(j, m, q.jobAllowsMachine(j, m.ID), predicateAllowedMachine) {
		return false
	}
	if !q.admissionPass(j, m, !(j.ProductionHost != "" && j.ProductionHost != hostID(m)), predicateProductionHost) || !q.admissionHold(j, m, q.checkpointTargetAdmission(j, m), predicateCheckpointTarget) {
		return false
	}
	if !q.admissionHold(j, m, q.productionAdmissionHold(j, m), predicateProductionAdmission) {
		return false
	}
	if !q.held(hostID(m)) {
		return true
	}
	if !q.admissionPass(j, m, j.Spec.Priority == "production", predicateProductionHold) {
		return false
	}
	text, _ := q.hostBlockerState(hostID(m))
	return q.admissionPass(j, m, text == "", predicateHostRelease)
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
func (q *Queue) productionHold(host string) Hold {
	if b, class := q.hostBlockerState(host); b != "" {
		return productionBlockerHold(host, b, class)
	}
	return scopedHold("production_hold", host, host+": production hold", false)
}

// Classification is explicit: new blocker paths cannot silently become routine.
func productionBlockerHold(host, text string, class hostBlockerClass) Hold {
	detail := host + ": production hold; " + text
	switch class.Kind {
	case "production_stopping", "production_release_unknown":
		return scopedHold(class.Kind, host, detail, class.OperatorActionRequired)
	}
	return scopedHold("unclassified", host, detail, true)
}

// A fleet upgrade holds new production starts until queue health is verified.
// Placement refuses on this hold and the scheduler's wait reports it.
func (q *Queue) fleetPauseHold(j *Job) *Hold {
	if j.State == "queued" && (j.Spec.Priority == "production" || j.Spec.Class == "production") && q.fleetPausesProduction() {
		return newHold("production_paused", "production starts are paused until the fleet upgrade verifies queue health")
	}
	return nil
}

// Production, disk and Docker fences that hold this job.
func (q *Queue) holdWait(j *Job) *Hold {
	for _, m := range q.plannerMachines() {
		if q.jobAllowsMachine(j, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			if h := q.productionAdmissionHold(j, m); h != nil {
				return h
			}
		}
	}
	if h := q.diskWait(j, j.ProductionHost); h != nil {
		return h
	}
	for _, m := range q.plannerMachines() {
		if q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			if h := q.dockerHold(hostID(m)); h.Reason != "" {
				return newHold(h.Kind, h.Detail)
			}
		}
	}
	if j.ProductionHost != "" && q.held(j.ProductionHost) {
		if reason := q.productionOccupancyReason(j); reason != "" {
			return newHold("production_hold", reason+"; production hold bound to "+j.ProductionHost+"; spillover requires measured quality parity or the project's production_parity_hosts approval and an immediately admissible qualified host")
		}
		h := q.productionHold(j.ProductionHost)
		return newHold(h.Kind, h.Detail)
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
						return newHold("pinned_model", "pinned model "+slot.Model+" on "+m.ID+" prevents production placement")
					}
				}
			}
		}
	}
	if j.Spec.Priority != "production" {
		for _, m := range q.plannerMachines() {
			if q.held(hostID(m)) && q.projectAllows(j.Spec.Project, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
				return newHold("production_reserved", "host "+hostID(m)+" reserved for production")
			}
		}
	}
	return nil
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
	if !j.Spec.extensionJob() || j.Spec.ExtensionCheck != nil || q.productionOccupancyReason(j) == "" {
		return ""
	}
	copy := *j
	copy.ProductionHost = ""
	copy.RequireProductionParity = true
	scan := q.placementScan()
	var best *Machine
	now := q.clock.Now()
	for _, m := range scan.machines {
		if hostID(m) == j.ProductionHost || q.unreported[m.ID] || !q.workerFresh(m, now) {
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
		if !q.reservationAllows(j, m) || diskReason != "" || q.dockerHold(hostID(m)).Kind != "" || q.unreported[m.ID] || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			if q.variantParityReason(candidate, m) != "" || call == nil && j.ProductionHost != "" && j.ProductionHost != hostID(m) && j.Spec.extensionJob() && j.Spec.ExtensionCheck == nil && !q.productionParityKnown(candidate, m) {
				continue
			}
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
		enabled = enabled || h.Policy.hostWide()
	}
	now := q.clock.Now()
	demand := map[string]bool{}
	jobs := []*Job{}
	for _, j := range q.liveJobs() {
		// Live owner policy also fences bindings saved by older releases.
		// Only queued demand moves; existing execution/release custody stays.
		if j.State == "queued" && j.ProductionHost != "" && (j.Spec.Priority == "production" || jobClass(j.Spec) == "production") && j.Spec.ExtensionCheck == nil && !q.productionParityHostAllowed(j, j.ProductionHost) {
			q.touchJob(j.ID)
			j.ProductionHost = ""
		}
		if !enabled {
			continue
		}
		if j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next > 0 {
			continue
		}
		if dependencyReady(j) && j.Spec.Priority == "production" && (j.State == "queued" || j.State == "running" || j.State == "stopping") && !cancelled(j) && !(j.State == "queued" && expired(j.Spec.GiveUpAt, now)) {
			jobs = append(jobs, j)
		}
	}
	if !enabled {
		return
	}
	sort.Slice(jobs, func(a, b int) bool { return q.before(jobs[a], jobs[b], &Machine{}) })
	for _, j := range jobs {
		if target := q.productionSpilloverTarget(j); target != "" {
			if j.ProductionHost != target || !j.RequireProductionParity {
				q.touchJob(j.ID)
			}
			j.ProductionHost = target
			j.RequireProductionParity = true
		}
		if j.State == "queued" && j.ProductionHost != "" && (q.dockerHold(j.ProductionHost).Kind != "" || q.diskWait(j, j.ProductionHost) != nil) {
			if target := q.productionTarget(j, nil); target != "" {
				if j.ProductionHost != target || j.RequireProductionParity != (j.Spec.extensionJob() && j.Spec.ExtensionCheck == nil) {
					q.touchJob(j.ID)
				}
				j.ProductionHost = target
				j.RequireProductionParity = j.Spec.extensionJob() && j.Spec.ExtensionCheck == nil
			}
		}
		if j.ProductionHost == "" {
			if m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); m != nil {
				if h := q.s.Hosts[hostID(m)]; h != nil && h.Policy.hostWide() {
					q.touchJob(j.ID)
					j.ProductionHost = hostID(m)
				}
			} else {
				target := q.productionTarget(j, nil)
				if h := q.s.Hosts[target]; h != nil && h.Policy.hostWide() {
					q.touchJob(j.ID)
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
				if h := q.s.Hosts[target]; h != nil && h.Policy.hostWide() {
					c.ProductionHost = target
				}
			}
			demand[c.ProductionHost] = true
		}
	}
	for host, h := range q.s.Hosts {
		if !h.Policy.hostWide() {
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

// Lost/stopping Docker executions fence the physical host, even without host
// policy. The zero Hold means no fence.
func (q *Queue) dockerHold(host string) Hold {
	best := Hold{}
	severity := 0
	choose := func(h Hold, rank int) {
		if rank > severity || rank == severity && h.Detail < best.Detail {
			best, severity = h, rank
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
		choose(scopedHold(kind, host, host+": Docker cleanup unconfirmed; "+m.DockerCleanup, action), rank)
	}
	for _, r := range q.s.HostReleases {
		if !r.Docker || r.Host != host || q.dockerReleaseLive(r) {
			continue
		}
		detail := host + ": Docker cleanup unconfirmed; waiting for attempt " + r.Job + fmt.Sprintf("/%d", r.Attempt)
		if r.EscalatedAt != nil {
			choose(scopedHold("docker_cleanup_operator_action", host, detail+"; operator action required (myce resolve-docker)", true), 5)
			continue
		}
		choose(scopedHold("docker_cleanup_waiting", host, detail, false), 2)
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

// Finalize release-only state after this transaction's decisions. This never
// chooses a target or preempts more work.
func (q *Queue) finalizeHolds() {
	enabled := false
	for _, h := range q.s.Hosts {
		enabled = enabled || h.Active && h.Policy.hostWide()
	}
	if !enabled {
		return
	}
	demand := map[string]bool{}
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next > 0 {
			continue
		}
		if dependencyReady(j) && j.Spec.Priority == "production" && (j.State == "queued" || j.State == "running" || j.State == "stopping") && !cancelled(j) && !(j.State == "queued" && expired(j.Spec.GiveUpAt, now)) {
			demand[j.ProductionHost] = true
		}
	}
	for _, c := range q.calls {
		if c.Priority == "production" && c.Error == "" {
			demand[c.ProductionHost] = true
		}
	}
	for host, h := range q.s.Hosts {
		if !h.Active || !h.Policy.hostWide() {
			continue
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
