package myce

// Phases retain the existing distinctions between new admission, observed
// Started work and recovery. None of these decisions releases execution custody.
type admissionPhase uint8

const (
	admissionPlacement admissionPhase = iota
	admissionRecovery
	admissionQualificationDemand // Started checks retain demand across a new reservation.
	admissionPreparationOffer
	admissionPreparationAuthorize
	admissionQualificationOffer
	admissionLaunch
	admissionStarted
	admissionHost
	admissionTakeover // Owner demand sources/reservations before an explicit preemption action.
	admissionHolds    // Source-only demand/status scans must not recurse into production forecasts.
)

type admissionRequest struct {
	Job       *Job // Physical work, including its exact attempt/session when assigned.
	Target    *Machine
	Phase     admissionPhase
	Resident  bool
	Launch    *HostLaunch // Exact preparation custody; checked again by authorization.
	Execution *Job        // Existing Started qualification may drain across a new admission hold.
	Session   string
}
type admissionDecision struct {
	Allowed bool
	Code    string
	Detail  HoldDetail
}

func deniedAdmission(m *Machine, code, text string) admissionDecision {
	d := admissionDecision{Code: code, Detail: HoldDetail{Kind: code, Text: text}}
	if m != nil {
		d.Detail.Host = hostID(m)
	}
	return d
}

func (q *Queue) admissionDemand(j *Job) *Job {
	if j != nil && automaticExtensionCheck(j) != nil && j.Spec.ExtensionCheck.Demand != "" {
		return q.s.Jobs[j.Spec.ExtensionCheck.Demand]
	}
	return j
}

// evaluateAdmission is the common policy intersection. Each source still owns
// its activation, revision and stop/preemption actions; this function only reads.
// Callers serialize decisions with queue mutation (or use a detached observation).
func (q *Queue) evaluateAdmission(r admissionRequest) admissionDecision {
	j, m := r.Job, r.Target
	deny := func(code, text string) admissionDecision { return deniedAdmission(m, code, text) }
	if m == nil {
		return deny("machine_unavailable", "machine unavailable")
	}
	hostPhase := r.Phase == admissionHost || r.Phase == admissionPreparationOffer || r.Phase == admissionPreparationAuthorize || r.Phase == admissionQualificationOffer
	if hostPhase {
		session := r.Session
		if session == "" && r.Phase != admissionHost {
			session = m.Session
		}
		if m.Recovering || m.Session != session || m.Gone || m.Leaving || q.unreported[m.ID] || q.maintenanceBlocked(hostID(m)) || availabilityBlocked(m) || m.HostMemoryPolicyMismatch {
			return deny("host_launch", "host unavailable for launch")
		}
		if r.Phase == admissionHost {
			return admissionDecision{Allowed: true}
		}
	}
	demand := q.admissionDemand(j)
	if j != nil && automaticExtensionCheck(j) != nil && j.Spec.ExtensionCheck.Demand != "" && demand == nil {
		return deny("useful_demand", "qualification demand no longer exists")
	}
	if r.Launch != nil && r.Launch.Started {
		// Reauthorization cannot revoke an already accepted preparation's custody.
		return admissionDecision{Allowed: true}
	}
	if r.Launch != nil && r.Launch.Demand != "" {
		demand = q.s.Jobs[r.Launch.Demand]
		if demand == nil || demand.State != "queued" || cancelled(demand) || expired(demand.Spec.GiveUpAt, q.clock.Now()) {
			return deny("useful_demand", "preparation demand no longer eligible")
		}
	}
	draining := j != nil && automaticExtensionCheck(j) != nil && j.StartedAt != nil && j.State != "queued" || r.Execution != nil && r.Execution.StartedAt != nil
	if reason := q.jobHoldReason(demand); !draining && reason != "" {
		return deny("job_hold", reason)
	}
	// A check uses its beneficiary's identity but its own Started state: starting
	// useful work elsewhere cannot grant a new check an exemption from a calendar.
	calendarJob := demand
	if draining {
		calendarJob = nil
	}
	if !draining && demand != nil && j != nil && demand != j {
		copy := *demand
		copy.State, copy.StartedAt = j.State, j.StartedAt
		calendarJob = &copy
	}
	if reason := q.scheduledHoldReason(calendarJob, m); !draining && reason != "" {
		return deny("scheduled_hold", reason)
	}
	if r.Phase == admissionHolds {
		return admissionDecision{Allowed: true}
	}
	preparation := r.Phase == admissionPreparationAuthorize
	if r.Phase != admissionQualificationDemand && !q.reservationAllows(demand, m) {
		return deny("reservation", "target reserved")
	}
	if r.Phase == admissionTakeover {
		return admissionDecision{Allowed: true}
	}
	if preparation {
		if demand != nil {
			if reason := q.productionAdmissionReason(demand, m); reason != "" {
				return deny("production_admission", reason)
			}
		}
		if (r.Launch == nil || r.Launch.Kind != "copy" && r.Launch.Device != "") && q.deviceAdmissionBlocked(m) {
			return deny("device_disabled", "device disabled")
		}
		return admissionDecision{Allowed: true}
	}
	if j == nil {
		j = demand
	}
	if j == nil {
		if (r.Launch == nil || r.Launch.Kind != "copy") && q.deviceAdmissionBlocked(m) {
			return deny("device_disabled", "device disabled")
		}
		return admissionDecision{Allowed: true}
	}
	// Internal check payloads retain their immutable execution spec. Admission
	// uses the accepted beneficiary's project/class and production authority.
	if automaticExtensionCheck(j) != nil && demand != nil && demand != j {
		copy := *j
		copy.Spec.Project, copy.Spec.Class, copy.Spec.Priority = demand.Spec.Project, demand.Spec.Class, demand.Spec.Priority
		j = &copy
	}
	start := r.Phase == admissionLaunch || r.Phase == admissionStarted
	if start {
		if r.Phase == admissionLaunch {
			if reason := q.protectedStorageLaunchReason(j); reason != "" {
				return deny(reason, reason)
			}
			if j.StartedAt == nil && !j.Spec.Needs.cpuOnly() && q.deviceAdmissionBlocked(m) {
				return deny("device_disabled", "device disabled")
			}
			if q.startDiskFloorReason(j, q.s.Machines[j.Machine]) != "" {
				return deny("disk_floor", "disk floor")
			}
		}
		if q.preparationStopping(m) {
			return deny("preparation_stopping", "preparation stopping")
		}
		if j.Spec.Run.Docker && dockerStartingOrRunning(m.DockerCleanup) {
			owner := q.s.Machines[j.Machine]
			previous := owner.DockerCleanup
			m.DockerCleanup, owner.DockerCleanup = "", ""
			defer func() { m.DockerCleanup, owner.DockerCleanup = previous, previous }()
		}
		if j.StartedAt == nil && !sharedHostJobFits(j, m) {
			return deny("shared_host_profile", "shared host profile")
		}
		if !executionTargetMatches(j.Spec, m) {
			return deny("execution_target", "execution target")
		}
		if !cpuFits(j, m) {
			return deny("cpu_fit", "CPU fit")
		}
	}
	if r.Phase == admissionPlacement || r.Phase == admissionPreparationOffer || r.Phase == admissionQualificationOffer {
		if !j.Spec.Needs.cpuOnly() && q.deviceAdmissionBlocked(m) {
			return deny("device_disabled", "device disabled")
		}
		if reason := q.diskHeadroomReason(j, m); reason != "" {
			return deny("disk_headroom", reason)
		}
	}
	if j.State == "queued" && (j.Spec.Priority == "production" || j.Spec.Class == "production") && q.fleetPausesProduction() {
		return deny("fleet_upgrade", "production paused for fleet upgrade")
	}
	if !dependencyReady(j) {
		return deny("dependency", "dependency not ready")
	}
	if m.Recovering {
		return deny("recovering", "worker recovering")
	}
	if cancelled(j) {
		return deny("cancelled", "job cancelled")
	}
	if !dockerRuntimeFits(j, m) {
		return deny("docker_runtime", "Docker runtime unavailable")
	}
	if q.maintenanceBlocked(hostID(m)) {
		return deny("maintenance", "host in planned maintenance")
	}
	if m.HostMemoryPolicyMismatch {
		return deny("host_memory_policy", "host memory policy mismatch")
	}
	if !logPolicyFits(j.Spec.Logs, m) {
		return deny("log_policy", "log policy")
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		if r.Resident {
			model = q.residentModel(model, m)
		}
		if !logPolicyFits(model.Logs, m) || !modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID) {
			return deny("model_policy", "model policy")
		}
	}
	if availabilityBlocked(m) {
		return deny("availability", "worker unavailable")
	}
	if q.machineLimitReason(m) != "" && !(j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping")) {
		return deny("machine_limit", "machine job limit")
	}
	if text := q.startupReleaseBlocker(hostID(m)); text != "" {
		return deny("startup_release", text)
	}
	if text := q.dockerBlocker(hostID(m)); text != "" {
		return deny("docker_cleanup", text)
	}
	if !q.jobAllowsMachine(j, m.ID) {
		return deny("machine_policy", "job not allowed on machine")
	}
	if j.ProductionHost != "" && j.ProductionHost != hostID(m) {
		return deny("production_host", "production bound to another host")
	}
	if text := q.productionAdmissionReason(j, m); text != "" {
		return deny("production_admission", text)
	}
	if q.held(hostID(m)) {
		if j.Spec.Priority != "production" || q.hostBlockers(hostID(m)) != "" {
			d := deny("production_hold", q.holdReason(hostID(m)))
			d.Detail = q.productionHoldDetail(hostID(m))
			return d
		}
	}
	return admissionDecision{Allowed: true}
}
