package myce

import (
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Policies are keyed by physical host_id (worker name when host_id is unset).
// No machine is ever reserved (MY-50): a production hold is device-scoped. It
// keeps exact release custody for the host's attempts and leaves exclusivity to
// ordinary per-resource priority preemption, so production claims only the
// devices and memory it needs while sibling cards and the CPU keep serving.
type HostPolicy struct {
	ProductionHold      bool   `json:"production_hold" yaml:"production_hold"`
	ProductionHoldScope string `json:"production_hold_scope,omitempty" yaml:"production_hold_scope,omitempty"`
}

type HostState struct {
	Policy HostPolicy `json:"policy"`
}

// Release obligations survive requeue/loss; only an execution acknowledgement or
// a fresh worker's completed cleanup can settle them.
type HostRelease struct {
	// Omitted older records retain conservative GPU-slot exclusion.
	CPUOnly                     bool    `json:"cpu_only,omitempty"`
	CPUCores                    int     `json:"cpu_cores,omitempty"`
	HostChargeGB                float64 `json:"host_charge_gb,omitempty"`
	CleanupFailed               bool    `json:"cleanup_failed,omitempty"`
	Device                      string  `json:"device,omitempty"`
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

// Install preflight checks a startup file the way queue start reads it.
func validateHostPolicies(p map[string]HostPolicy) error {
	_, err := normalizeHostPolicies(p, true)
	return err
}

// The whole-machine scope (an omitted or "host" scope with production_hold)
// was removed (MY-50). The policy API refuses it. A startup file or saved
// state from the previous release converts to device scope with a warning, so
// an upgrade never fails to start; drop the conversion after one release (MY-42).
func normalizeHostPolicies(p map[string]HostPolicy, convert bool) (map[string]HostPolicy, error) {
	out := map[string]HostPolicy{}
	for host, v := range p {
		if strings.TrimSpace(host) == "" {
			return nil, errors.New("host name is required")
		}
		switch v.ProductionHoldScope {
		case "", "host":
			v.ProductionHoldScope = ""
			if v.ProductionHold {
				if !convert {
					return nil, fmt.Errorf("host %s: %s; set production_hold_scope: device, or production_hold: false", host, wholeMachineHoldRemoved)
				}
				log.Printf("event=host_policy_converted host=%s from=whole_machine to=device reason=%q", host, wholeMachineHoldRemoved)
				v.ProductionHoldScope = "device"
			}
		case "device":
			if !v.ProductionHold {
				return nil, fmt.Errorf("host %s: production_hold_scope requires production_hold: true", host)
			}
		default:
			return nil, fmt.Errorf("host %s: production_hold_scope must be device", host)
		}
		out[host] = v
	}
	return out, nil
}

const wholeMachineHoldRemoved = "the whole-machine production hold was removed; production priority claims only the GPU devices and memory it needs"

// Saved state from the previous release may still hold a whole-machine policy.
func (q *Queue) retireWholeMachineHolds() {
	for host, h := range q.s.Hosts {
		if h != nil && h.Policy.ProductionHold && h.Policy.ProductionHoldScope != "device" {
			converted, _ := normalizeHostPolicies(map[string]HostPolicy{host: h.Policy}, true)
			h.Policy = converted[host]
		}
	}
	if len(q.s.HostConfig) > 0 {
		q.s.HostConfig, _ = normalizeHostPolicies(q.s.HostConfig, true)
	}
}

func (q *Queue) hostPolicies() map[string]HostPolicy {
	out := map[string]HostPolicy{}
	for h, s := range q.s.Hosts {
		out[h] = s.Policy
	}
	return out
}

// Release custody is keyed by attempt, so it survives a policy change.
func (q *Queue) replaceHostPolicies(p map[string]HostPolicy) {
	next := map[string]*HostState{}
	for h, v := range p {
		next[h] = &HostState{Policy: v}
	}
	q.s.Hosts = next
}

// The startup file seeds host policy once; afterwards `myce policy set` owns it.
// Restarting with the last accepted file keeps the live policy. A different
// file is still refused, so an edit can never silently relax a hold.
func (q *Queue) ConfigureHosts(p map[string]HostPolicy) error {
	normalized, err := normalizeHostPolicies(p, true)
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
	if j.Spec.Needs.CPUCores == 0 && (m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs == 0) && !q.maintenanceBlocked(hostID(m)) && hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) == 0 && j.Spec.StartBy == nil && m.HostMemoryBudgetGB == 0 && m.EffectiveHostMemoryBudgetGB == 0 && q.machineJobLimit(m) == 0 && (policy == nil || !policy.Policy.ProductionHold) {
		return
	}
	if q.s.HostReleases == nil {
		q.s.HostReleases = map[string]HostRelease{}
	}
	r := HostRelease{CPUOnly: j.Spec.Needs.cpuOnly(), CPUCores: jobCPUCharge(j, m), HostChargeGB: hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), Device: m.Device, MemoryGB: deviceShareGB(j), HostMemoryGB: j.Spec.Needs.HostMemoryGB, Host: hostID(m), Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}
	if len(j.AllocatedDevices) > 1 {
		r.Device, r.Devices = j.AllocatedDevices[0], slices.Clone(j.AllocatedDevices)
	}
	if j.StopAt != nil {
		r.Until = *j.StopAt
	}
	old := q.s.HostReleases[releaseKey(r)]
	r.HostChargeGB = max(r.HostChargeGB, q.hostJobCharge(j), retainedHostCharge(j), old.HostChargeGB)
	r.CleanupFailed = old.CleanupFailed
	q.s.HostReleases[releaseKey(r)] = r
}
func (q *Queue) settleRelease(r Result, exact bool) bool {
	if j := q.retainedJob(r.ID); j != nil && j.Spec.ephemeral() && !cleanupComplete(r.WorkerDataCleanup) {
		return false
	}
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
		if j := q.retainedJob(r.Job); j != nil && j.Spec.ephemeral() {
			continue
		}
		if r.Machine != c.Machine.ID {
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
	if (j.StartedAt == nil || j.State == "queued") && (!q.admissionHold(j, m, reasonHold("shared_host_backoff", q.sharedHostBackoffReason(j, m)), predicateSharedHostBackoff) || !q.admissionHold(j, m, reasonHold("storage_root", q.storageRootReason(j, m)), predicateStorageRoot) || !q.admissionHold(j, m, reasonHold(stagingHostRetryHold, q.stagingHostRetryReason(j, m)), predicateStagingHostRetry)) {
		return false
	}
	if !q.admissionPass(j, m, dependencyReady(j), predicateDependency) || !q.admissionPass(j, m, !m.Recovering, predicateRecovering) || !q.admissionPass(j, m, !cancelled(j), predicateCancelled) || !q.admissionPass(j, m, !(q.maintenanceBlocked(hostID(m))), predicateMaintenance) || !q.admissionPass(j, m, logPolicyFits(j.Spec.Logs, m), predicateLogPolicy) {
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

	if !q.admissionPass(j, m, !availabilityBlocked(m), predicateAvailability) || !q.admissionPass(j, m, !(q.machineCapacityReason(m) != "" && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping"))), predicateMachineCapacity) || !q.admissionHold(j, m, heldBy(q.startupReleaseHold(hostID(m))), predicateStartupRelease) || !q.admissionPass(j, m, q.jobAllowsMachine(j, m.ID), predicateAllowedMachine) {
		return false
	}
	if !q.admissionHold(j, m, q.checkpointTargetAdmission(j, m), predicateCheckpointTarget) {
		return false
	}
	return q.admissionHold(j, m, q.productionAdmissionHold(j, m), predicateProductionAdmission)
}

// Production and disk fences that hold this job.
func (q *Queue) holdWait(j *Job) *Hold {
	for _, m := range q.plannerMachines() {
		if q.jobAllowsMachine(j, m.ID) && fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			if h := q.productionAdmissionHold(j, m); h != nil {
				return h
			}
		}
	}
	if h := q.diskWait(j, ""); h != nil {
		return h
	}
	if j.Spec.Priority == "production" {
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
	return nil
}

// Host maintenance settles a preparation release only once no background
// build or non-production model copy remains on the worker.
func (q *Queue) backgroundPreparing(m *Machine) bool {
	for _, r := range m.EnvironmentReports {
		if r.State == "building" {
			return true
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
