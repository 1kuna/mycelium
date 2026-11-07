package myce

import "slices"

// FleetCheck classifies accepted queue observations, not external probe output.
// Waiting/blocked checks never grant custody or replace installer preflight.
type FleetCheck struct {
	Check      string `json:"check"`
	State      string `json:"state"`
	Code       string `json:"code,omitempty"`
	Capability string `json:"capability,omitempty"`
}

// The legacy NAS probe flag is only advisory when it predates this worker's
// cutover and no live NAS checkpoint work or model custody needs that host.
// Positive root failures and new degradation remain blocking. The helper still
// runs the installer's independent storage preflight before touching bytes.
func (q *Queue) fleetCheckpointAdvisory(h *fleetUpgradeHost, m *Machine) bool {
	if h.Before == nil || !h.Before.CheckpointUnavailable || h.CheckpointProbeRecovered || !m.CheckpointUnavailable || len(m.UnavailableRoots) != 0 {
		return false
	}
	for _, j := range q.liveJobs() {
		if !terminal(j.State) && !cancelled(j) && !j.FileTransport && needsCheckpoint(j) && q.jobAllowsMachine(j, m.ID) && executionTargetMatches(j.Spec, m) {
			return false
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == m.ID && slot.Attempt != "" {
			return false
		}
	}
	return true
}

func (q *Queue) fleetHostChecks(h *fleetUpgradeHost) []FleetCheck {
	checks := []FleetCheck{}
	add := func(name, state, code, capability string) {
		checks = append(checks, FleetCheck{Check: name, State: state, Code: code, Capability: capability})
	}
	if q.fleetHostQuiet(h) {
		add("custody", "ready", "", "")
	} else {
		add("custody", "waiting", "exact_release_pending", "")
	}
	if h.Queue && !h.QueueDone {
		if q.fleetQueue == nil || !q.fleetProofFresh(q.fleetQueue.Proof) {
			add("queue_runtime", "waiting", "fresh_runtime_required", "queue")
		} else {
			add("queue_runtime", "ready", "", "queue")
		}
	}
	if m := q.s.Machines[h.Machine]; m != nil {
		if q.fleetProofFresh(m.Fleet) {
			add("worker_runtime", "ready", "", "worker")
		} else {
			add("worker_runtime", "waiting", "fresh_runtime_required", "worker")
		}
		if len(m.UnavailableRoots) > 0 {
			add("storage_roots", "blocked", "storage_root_unavailable", "storage")
		} else {
			add("storage_roots", "ready", "", "storage")
		}
		if m.CheckpointUnavailable {
			if q.fleetCheckpointAdvisory(h, m) {
				add("checkpoint_probe", "advisory", "preexisting_checkpoint_probe_no_dependent_work", "nas_checkpoint")
			} else {
				add("checkpoint_probe", "blocked", "checkpoint_probe_unavailable", "nas_checkpoint")
			}
		} else {
			add("checkpoint_probe", "ready", "", "nas_checkpoint")
		}
		if m.HostMemoryPolicyMismatch || m.HostMemoryError != "" || m.MemoryError != "" || m.CPUCapacityReason != "" || m.InventoryReason != "" || m.DockerCleanup != "" || slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return d.Reason != "" }) {
			add("worker_components", "blocked", "worker_component_unavailable", "worker")
		} else if m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs <= 0 || m.CapabilityProfile.SystemMemoryBytes == 0 || !m.HostMemoryMeasured || m.HostAvailableMemoryGB <= 0 {
			add("worker_components", "waiting", "worker_reports_missing", "worker")
		} else {
			add("worker_components", "ready", "", "worker")
		}
	}
	if h.Failure != "" {
		add("installation", "blocked", h.FailureCode, "")
	}
	return checks
}
