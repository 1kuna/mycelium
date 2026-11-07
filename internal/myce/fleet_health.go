package myce

// FleetCheck classifies accepted queue observations, not external probe output.
// Waiting/blocked checks never grant custody or replace installer preflight.
type FleetCheck struct {
	Check      string `json:"check"`
	State      string `json:"state"`
	Code       string `json:"code,omitempty"`
	Capability string `json:"capability,omitempty"`
}

// Accepted exact worker reports own recovery evidence, even while the operation
// is stopped, the queue is updating, or fleet runtime proof is unavailable.
func (q *Queue) observeFleetCheckpointReport(m *Machine) {
	if m.CheckpointUnavailable || q.s.FleetUpgrade == nil {
		return
	}
	for _, h := range q.s.FleetUpgrade.Hosts {
		if h.Machine == m.ID && h.Host == hostID(m) && h.Before != nil && h.Before.CheckpointUnavailable {
			h.CheckpointProbeRecovered = true
		}
	}
}

func (q *Queue) fleetCheckpointDependent(j *Job, m *Machine) bool {
	if terminal(j.State) || cancelled(j) || j.FileTransport || !needsCheckpoint(j) {
		return false
	}
	if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") {
		return true // Existing custody still needs its checkpoint despite policy edits.
	}
	dependent := false
	// Share qualification's permanent-capacity/device/variant evaluation. It
	// ignores temporary occupancy, checkpoint flags and maintenance restrictions.
	// Placement attaches transient claims to scalar targets. Own that view here
	// because status can classify health while holding only the queue read lock.
	view := *m
	q.evaluateTargetsOnMachines(j, false, map[string]*Machine{m.ID: &view}, func(fit targetFit, _ *Job, _ *Machine) bool {
		dependent = fit.failures == 0
		return !dependent
	})
	return dependent
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
		if q.fleetCheckpointDependent(j, m) {
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
	if h.Queue {
		if h.Preservation == nil {
			add("queue_preservation", "waiting", "supported_handoff_not_observed", "queue_records")
		} else if h.Preservation.State == "verified" {
			add("queue_preservation", "ready", "", "queue_records")
		} else {
			add("queue_preservation", "waiting", "committed_record_verification_pending", "queue_records")
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
		state, code := q.fleetComponentCheck(h, m)
		add("worker_components", state, code, "worker")
	}
	if h.Failure != "" {
		add("installation", "blocked", h.FailureCode, "")
	}
	return checks
}

func (q *Queue) fleetComponentCheck(h *fleetUpgradeHost, m *Machine) (string, string) {
	if h.Before == nil || h.Before.CapabilityProfile == nil {
		return "waiting", "worker_baseline_missing"
	}
	profile := h.Before.CapabilityProfile
	receipt := EnrollmentReceipt{Devices: h.Before.Devices, LogicalCPUs: profile.LogicalCPUs, SystemMemoryBytes: profile.SystemMemoryBytes}
	observed := *m
	if q.fleetCheckpointAdvisory(h, m) {
		observed.CheckpointUnavailable = false
	}
	code, _ := enrollmentComponentIssue(receipt, EnrollmentObservation{Machine: observed, Policy: q.s.HostMaintenance[h.Host]})
	if code != "" {
		return "blocked", code
	}
	return "ready", ""
}
