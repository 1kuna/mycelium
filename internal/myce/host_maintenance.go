package myce

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

const maintenanceStopCause = "planned_host_maintenance"

// Queue-owned policy, keyed by physical host identity, never by worker session.
// A nil budget is possible before the first worker report; zero is uncapped.
// Existing hosts bootstrap their allowance once, then worker reports cannot edit it.
type HostMaintenance struct {
	SharedHost     *SharedHostProfile `json:"shared_host,omitempty"`
	LastResort     bool               `json:"last_resort"`
	DrainOwner     string             `json:"drain_owner,omitempty"`
	CPUBudgetCores *int               `json:"cpu_budget_cores,omitempty"`
	StopAt         *time.Time         `json:"stop_at,omitempty"`
	Mode           string             `json:"mode"`
	MemoryBudget   *HostMemoryBudget  `json:"memory_budget,omitempty"`
	Role           string             `json:"role,omitempty"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

type HostMaintenanceRequest struct {
	SharedHost           *SharedHostProfile `json:"shared_host,omitempty"`
	LastResort           *bool              `json:"last_resort,omitempty"`
	DrainOwner           string             `json:"drain_owner,omitempty"`
	IfDrainOwner         string             `json:"if_drain_owner,omitempty"`
	IfMode               string             `json:"if_mode,omitempty"`
	IfUpdatedAt          *time.Time         `json:"if_updated_at,omitempty"`
	CPUBudgetCores       *int               `json:"cpu_budget_cores,omitempty"`
	Action               string             `json:"action"`
	Stop                 bool               `json:"stop,omitempty"`
	MemoryBudgetGB       *float64           `json:"memory_budget_gb,omitempty"`
	MemoryBudgetMeasured *bool              `json:"memory_budget_measured,omitempty"`
	Role                 *string            `json:"role,omitempty"`
}

type MaintenanceWorker struct {
	Machine Machine `json:"machine"`
	Fresh   bool    `json:"fresh"`
}

type MaintenanceAttempt struct {
	Execution
	Machine   string `json:"machine"`
	Session   string `json:"session"`
	State     string `json:"state"`
	Started   bool   `json:"started"`
	StopCause string `json:"stop_cause,omitempty"`
}

type HostMaintenanceStatus struct {
	DeviceUse         []DeviceUse         `json:"device_use"`
	DeviceMaintenance []DeviceMaintenance `json:"device_maintenance"`

	ExternalMemory *ExternalMemoryHistory `json:"external_memory,omitempty"`
	Host           string                 `json:"host"`
	Policy         HostMaintenance        `json:"policy"`

	Workers         []MaintenanceWorker  `json:"workers"`
	Attempts        []MaintenanceAttempt `json:"attempts"`
	Calls           int                  `json:"calls"`
	Preparations    int                  `json:"preparations"`
	Sidecars        int                  `json:"sidecars"`
	PendingReleases []HostRelease        `json:"pending_releases"`
	MemoryInUseGB   float64              `json:"memory_in_use_gb"`
	MemoryDebtGB    float64              `json:"memory_debt_gb"`
	// ProductionBlocker names open production work a planned drain of this
	// enabled host would strand; drain is refused while it is set.
	ProductionBlocker string `json:"production_blocker,omitempty"`
	// Healthy retains the legacy all-workers/all-devices health contract.
	Healthy   bool   `json:"healthy"`
	State     string `json:"state"`
	Quiescent bool   `json:"quiescent"`
}

func (q *Queue) maintenanceKnown(host string) bool {
	_, ok := q.s.HostMaintenance[host]
	return ok
}
func (q *Queue) maintenanceBlocked(host string) bool {
	mode := q.s.HostMaintenance[host].Mode
	return mode == "drained" || mode == "disabled"
}
func plannedMaintenanceStop(j *Job) bool {
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
			return c.StopCause == maintenanceStopCause || c.StopCause == deviceMaintenanceStopCause
		}
	}
	return false
}
func (q *Queue) registerMaintenanceHost(m *Machine) error {
	if q.s.HostMaintenance == nil {
		q.s.HostMaintenance = map[string]HostMaintenance{}
	}
	policy := q.s.HostMaintenance[hostID(m)]
	if policy.Mode == "" {
		policy.Mode = "enabled"
	}
	if policy.MemoryBudget == nil {
		budget, err := hostMemoryAllowance(m.HostMemoryBudgetGB)
		if err != nil {
			return err
		}
		// Preserve the reported admission precision; Bytes uses the existing
		// floor conversion for kernel enforcement and status.
		budget.GiB = m.HostMemoryBudgetGB
		budget.Measured = m.HostMemoryBudgetMeasured
		if budget.Measured && budget.GiB <= 0 {
			return errors.New("measured host allowance requires a positive authored fallback")
		}
		policy.MemoryBudget = &budget
		policy.UpdatedAt = q.clock.Now()
	}
	if policy.CPUBudgetCores == nil && m.CapabilityProfile != nil && m.CapabilityProfile.LogicalCPUs > 0 {
		value := m.CPUBudgetCores
		policy.CPUBudgetCores = &value
	}
	q.s.HostMaintenance[hostID(m)] = policy
	return nil
}

// ChangeHostMaintenance serializes admission, Started, cancellation and policy edits
// with the same SQLite transaction. Acceptance alone never settles an attempt.
func (q *Queue) ChangeHostMaintenance(host string, request HostMaintenanceRequest, requestContexts ...context.Context) (HostMaintenanceStatus, error) {
	requestCtx := requestContext(requestContexts)

	var out HostMaintenanceStatus
	if strings.TrimSpace(host) == "" {
		return out, errors.New("physical host is required")
	}
	err := q.updateRequest(requestCtx, func() error {
		if err := q.changeHostMaintenance(host, request); err != nil {
			return err
		}
		q.maintain()
		out = q.hostMaintenanceStatus(host)
		return nil
	}, nil)
	return out, err
}

func (q *Queue) HostMaintenanceStatus(host string, requestContexts ...context.Context) (HostMaintenanceStatus, error) {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	defer requestUnlock1()
	// A closed/failed queue cannot certify health using its in-memory snapshot.
	if !q.simulation {
		if err := q.db.Ping(); err != nil {
			return HostMaintenanceStatus{}, err
		}
	}
	q.refreshHostMemory()
	out := q.hostMaintenanceStatus(host)
	if out.Policy.Mode == "enabled" {
		out.ProductionBlocker = q.productionDrainBlocker(host)
	}
	return out, nil
}
func (q *Queue) hostMaintenanceStatus(host string) HostMaintenanceStatus {
	out := HostMaintenanceStatus{DeviceUse: q.deviceUseStatus(host), Host: host, DeviceMaintenance: q.deviceMaintenanceStatus(host), Policy: q.s.HostMaintenance[host], Workers: []MaintenanceWorker{}, Attempts: []MaintenanceAttempt{}, PendingReleases: []HostRelease{}}
	if out.Policy.Mode == "" {
		out.Policy.Mode = "enabled"
	}
	out.Policy.SharedHost = cloneSharedHost(out.Policy.SharedHost)
	if out.Policy.StopAt != nil {
		deadline := *out.Policy.StopAt
		out.Policy.StopAt = &deadline
	}
	if out.Policy.CPUBudgetCores != nil {
		value := *out.Policy.CPUBudgetCores
		out.Policy.CPUBudgetCores = &value
	}
	if out.Policy.MemoryBudget != nil {
		budget := *out.Policy.MemoryBudget
		out.Policy.MemoryBudget = &budget
	}
	out.ExternalMemory = q.externalMemoryStatus(host)
	out.Healthy = true
	operational := true
	freshEvidence := true
	now := q.clock.Now()
	for _, m := range q.s.Machines {
		if hostID(m) != host || m.Replacement != nil {
			continue
		}
		fresh := !q.unreported[m.ID] && q.workerFresh(m, now)
		copy := cloneMachine(m)
		copy.Suspect = q.workerSuspect(m, now)
		out.Workers = append(out.Workers, MaintenanceWorker{Machine: copy, Fresh: fresh})
		freshEvidence = freshEvidence && fresh
		operational = operational && fresh && !m.HostMemoryPolicyMismatch && m.MemoryError == "" && m.HostMemoryError == "" && m.InventoryReason == "" && (out.Policy.SharedHost == nil || q.sharedPressureFresh(m))
		out.Healthy = out.Healthy && operational && len(m.UnavailableRoots) == 0
		out.Healthy = out.Healthy && q.hostHygieneReasonAt(m, q.clock.Now()) == ""
		out.Healthy = out.Healthy && !slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return d.Reason != "" })
		out.MemoryInUseGB = max(out.MemoryInUseGB, m.HostMemoryInUseGB)
		out.MemoryDebtGB = max(out.MemoryDebtGB, m.hostReservationDebtGB)
	}
	obligations := q.hostObligations(host)
	for _, obligation := range obligations {
		if obligation.Call != nil {
			out.Calls++
			continue
		}
		if obligation.Engine != nil {
			out.Sidecars++
			continue
		}
		if obligation.Launch != nil {
			out.Preparations++
			continue
		}
		if obligation.Preparation {
			if m := q.s.Machines[obligation.Result.Machine]; m != nil && m.Session == obligation.Result.Session {
				out.Preparations += q.untrackedPreparations(m)
			}
			continue
		}
		r := obligation.Result
		attempt := MaintenanceAttempt{Execution: r.Execution, Machine: r.Machine, Session: r.Session, State: "release_pending"}
		if j := q.s.Jobs[r.ID]; j != nil {
			out.Sidecars += nativeSidecarCount(j, q.s.Machines[r.Machine])
			if j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
				attempt.State = j.State
				attempt.Started = j.StartedAt != nil
			}
			for _, copy := range j.Copies {
				if copy.Attempt == r.Attempt && copy.Machine == r.Machine && copy.Session == r.Session {
					attempt.StopCause = copy.StopCause
				}
			}
		}
		out.Attempts = append(out.Attempts, attempt)
	}
	for _, r := range q.s.HostReleases {
		if r.Host == host {
			out.PendingReleases = append(out.PendingReleases, r)
		}
	}
	sort.Slice(out.Workers, func(i, j int) bool { return out.Workers[i].Machine.ID < out.Workers[j].Machine.ID })
	sort.Slice(out.PendingReleases, func(i, j int) bool { return releaseKey(out.PendingReleases[i]) < releaseKey(out.PendingReleases[j]) })
	operational = operational && len(out.Workers) > 0 && out.Policy.MemoryBudget != nil
	out.Healthy = out.Healthy && operational
	out.State = "unhealthy"
	if operational {
		out.State = "degraded"
		if out.Healthy {
			out.State = "healthy"
		}
	}
	// Fresh custody evidence is required; inventory and memory health are separate.
	out.Quiescent = freshEvidence && len(out.Workers) > 0 && q.maintenanceBlocked(host) && len(obligations) == 0
	return out
}
func (a API) registerHostMaintenance(mux *requestMux) {
	mux.HandleFunc("GET /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		status, err := a.Queue.HostMaintenanceStatus(r.PathValue("id"), r.Context())
		respondOperation(w, status, err, "host_control_invalid")
	})
	mux.HandleFunc("POST /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		var request HostMaintenanceRequest
		if !decode(w, r, &request) {
			return
		}
		status, err := a.Queue.ChangeHostMaintenance(r.PathValue("id"), request, r.Context())
		respondOperation(w, status, err, "host_control_invalid")
	})
}

// Admission, stop and wait share hostObligations. Each adapter below uses the
// work kind's existing stop channel; a deadline never discharges its custody.
func (q *Queue) maintainHostMaintenance() {
	for host, policy := range q.s.HostMaintenance {
		if !q.maintenanceBlocked(host) {
			continue
		}
		for _, p := range q.hostObligations(host) {
			q.maintainHostObligation(p, policy.StopAt)
		}
	}
}

func boundStop(at **time.Time, deadline time.Time) {
	if *at == nil || deadline.Before(**at) {
		copy := deadline
		*at = &copy
	}
}

func (q *Queue) maintainHostObligation(p HostObligation, deadline *time.Time) {
	q.maintainHostObligationGrace(p, deadline, false)
}

// Normal upgrades promise the full planned-leave checkpoint grace, including
// GPU work. Other maintenance and critical upgrades retain their existing grace.
func (q *Queue) maintainHostObligationGrace(p HostObligation, deadline *time.Time, plannedLeave bool) {
	switch {
	case p.Launch != nil && !p.Launch.Started:
		delete(q.s.PreparationPlans, p.Launch.Attempt)
	case p.Call != nil:
		c := q.obligationCall(p.Call)
		if c == nil || deadline == nil {
			return
		}
		boundStop(&c.StopAt, *deadline)
		if expired(c.StopAt, q.clock.Now()) && c.Error == "" {
			c.Error = maintenanceStopCause
		}
	case p.Engine != nil:
		e := p.Engine
		s := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]
		if s == nil || s.Session != e.Session || s.Attempt != e.Attempt {
			return
		}
		if s.State != "failed" && s.State != "unloading" && s.State != "unload_failed" && (s.State != "loading" || deadline != nil) {
			s.State = "draining"
		}
		if deadline != nil {
			boundStop(&s.StopAt, *deadline)
			if expired(s.StopAt, q.clock.Now()) && s.State == "draining" {
				s.State = "unloading"
			}
		}
	case p.Preparation:
		if deadline == nil {
			return
		}
		r := HostRelease{Host: q.executionHost(p.Result.Machine), Job: p.Result.ID, Machine: p.Result.Machine, Session: p.Result.Session, Preparation: true, Until: *deadline}
		if q.s.HostReleases == nil {
			q.s.HostReleases = map[string]HostRelease{}
		}
		if old, ok := q.s.HostReleases[releaseKey(r)]; !ok || old.Until.IsZero() || deadline.Before(old.Until) {
			q.s.HostReleases[releaseKey(r)] = r
		}
	default:
		r := p.Result
		j := q.retainedJob(r.ID)
		q.touchJob(r.ID)
		if j == nil || j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || j.State != "running" && j.State != "stopping" {
			return
		}
		if !cancelled(j) && (deadline != nil || j.StartedAt == nil) {
			for i := range j.Copies {
				c := &j.Copies[i]
				if c.Attempt == r.Attempt && c.Machine == r.Machine && c.Session == r.Session && c.StopCause == "" && j.State == "running" {
					c.StopCause = maintenanceStopCause
				}
			}
			q.stop(j, maintenanceStopCause)
			if !plannedLeave && gracefulGPUStop(j) {
				boundStop(&j.StopAt, q.clock.Now().Add(q.gpuStopGrace()))
			}
			if deadline != nil {
				boundStop(&j.StopAt, *deadline)
			}
		}
		q.retainRelease(j)
	}
}

// changeHostMaintenance runs inside the existing queue transaction.
func (q *Queue) changeHostMaintenance(host string, request HostMaintenanceRequest) error {
	if err := q.validateMachineNames([]string{host}); err != nil {
		return err
	}
	if q.s.HostMaintenance == nil {
		q.s.HostMaintenance = map[string]HostMaintenance{}
	}
	policy := q.s.HostMaintenance[host]
	if request.Action == "resume" && q.s.ComponentExclusions["host="+host].Active {
		return errors.New("remove the host exclusion before resuming this host")
	}
	if request.IfUpdatedAt != nil && !policy.UpdatedAt.Equal(*request.IfUpdatedAt) || request.IfMode != "" && policy.Mode != request.IfMode || request.IfDrainOwner != "" && policy.DrainOwner != request.IfDrainOwner {
		return ErrStale
	}
	if policy.Mode == "" {
		policy.Mode = "enabled"
	}
	// Placement and allowance edits do not transfer drain authority, so an
	// owner's conditional resume still works after them. Other maintenance
	// edits, including a re-drain at the same timestamp, revoke it.
	preservesDrain := request.Action == "placement" || request.Action == "budget" || request.Action == "role"
	if !preservesDrain {
		policy.DrainOwner = ""
	}
	switch request.Action {
	case "drain":
		// A planned operator drain never strands open production work; disable
		// remains the explicit out-of-service action. A fleet upgrade (drain
		// owner) applies the same predicate itself, waiting unless critical.
		if policy.Mode == "enabled" && request.DrainOwner == "" {
			if reason := q.productionDrainBlocker(host); reason != "" {
				return &HTTPError{Status: http.StatusConflict, Code: "production_work_open", Message: "drain would strand open production work: " + reason + "; wait for it to close or expire, or use host disable to take the host out of service now"}
			}
		}
		policy.Mode = "drained"
		policy.DrainOwner = request.DrainOwner
	case "disable":
		policy.Mode = "disabled"
	case "resume":
		policy.Mode = "enabled"
	case "budget":
	case "profile":
		if request.SharedHost == nil || request.LastResort != nil || request.MemoryBudgetGB != nil || request.MemoryBudgetMeasured != nil || request.CPUBudgetCores != nil {
			return errors.New("profile requires shared_host and no other policy edits")
		}
		if policy.Mode == "enabled" {
			return errors.New("drain or disable the host before changing its profile")
		}
		if !validMemoryValue(request.SharedHost.InteractiveReserveGB) {
			return errors.New("interactive reserve must be finite and nonnegative")
		}
		policy.SharedHost = nil
		if request.SharedHost.InteractiveReserveGB > 0 {
			if policy.MemoryBudget == nil || policy.MemoryBudget.GiB <= 0 || policy.CPUBudgetCores == nil || *policy.CPUBudgetCores <= 0 {
				return errors.New("shared host requires positive CPU and RAM allowances")
			}
			if !q.hostMaintenanceStatus(host).Quiescent {
				return errors.New("shared host profile requires fresh quiescent custody; drain --wait first")
			}
			profile := *request.SharedHost
			policy.SharedHost = &profile
			policy.LastResort = true
		}
		// The profile implies the interactive role; clearing it returns the
		// host to the default on-demand node role.
		policy.Role = ""
		if policy.SharedHost != nil {
			policy.Role = hostRoleInteractive
		}
	case "placement":
		if request.LastResort == nil {
			return errors.New("placement requires last_resort")
		}
	case "role":
		// Capacity re-derives from the role on the next refresh; workers
		// acknowledge the resulting allowance before new starts, so no drain.
		if request.Role == nil || request.SharedHost != nil || request.LastResort != nil || request.MemoryBudgetGB != nil || request.MemoryBudgetMeasured != nil || request.CPUBudgetCores != nil {
			return errors.New("role requires role and no other policy edits")
		}
		if err := validHostRole(*request.Role); err != nil {
			return err
		}
		// The interactive role is exactly the hosts carrying the measured
		// shared-interactive profile (#389), whose owner-yield logic keys off
		// that profile; on-demand and services hosts never carry it.
		if (*request.Role == hostRoleInteractive) != (policy.SharedHost != nil) {
			return errors.New("the interactive role requires the shared host profile, and node or services require none; set or clear it with profile first")
		}
		policy.Role = *request.Role
	default:
		return errors.New("expected drain, disable, resume, budget, placement, profile or role")
	}
	if request.Role != nil && request.Action != "role" {
		return errors.New("role requires the role action")
	}
	if request.SharedHost != nil && request.Action != "profile" {
		return errors.New("shared_host requires profile")
	}
	if policy.SharedHost != nil && request.LastResort != nil && !*request.LastResort {
		return errors.New("shared hosts must remain last-resort")
	}
	if request.LastResort != nil {
		if request.Action != "placement" {
			return errors.New("last_resort requires placement")
		}
		policy.LastResort = *request.LastResort
	}
	if request.Stop && request.Action != "disable" {
		return errors.New("--stop requires disable")
	}
	if request.Action == "budget" && request.MemoryBudgetGB == nil && request.MemoryBudgetMeasured == nil && request.CPUBudgetCores == nil {
		return errors.New("budget requires RAM GiB or CPU cores")
	}
	if request.CPUBudgetCores != nil {
		if policy.Mode == "enabled" {
			return errors.New("drain or disable the host before changing its CPU allowance")
		}
		if policy.SharedHost != nil && *request.CPUBudgetCores == 0 {
			return errors.New("shared host requires a positive CPU allowance")
		}
		if *request.CPUBudgetCores < 0 {
			return errors.New("CPU allowance must be nonnegative (zero uses detected capacity minus reserve)")
		}
		value := *request.CPUBudgetCores
		policy.CPUBudgetCores = &value
	}
	if request.MemoryBudgetGB != nil {
		if policy.Mode == "enabled" {
			return errors.New("drain or disable the host before changing its memory allowance")
		}
		budget, err := hostMemoryAllowance(*request.MemoryBudgetGB)
		if err != nil || budget.Bytes <= 0 {
			return errors.New("host allowance must be positive finite GiB")
		}
		policy.MemoryBudget = &budget
	}
	if request.MemoryBudgetMeasured != nil {
		if request.Action != "budget" || policy.Mode == "enabled" {
			return errors.New("measured memory mode requires a drained or disabled host budget action")
		}
		if policy.MemoryBudget == nil || policy.MemoryBudget.GiB <= 0 {
			return errors.New("measured memory mode requires a positive authored fallback")
		}
		budget := *policy.MemoryBudget
		budget.Measured = *request.MemoryBudgetMeasured
		policy.MemoryBudget = &budget
	}
	if request.Action == "resume" || request.Action == "drain" {
		policy.StopAt = nil
	}
	if request.Stop && policy.StopAt == nil {
		deadline := q.clock.Now().Add(StopGrace)
		policy.StopAt = &deadline
	}
	// Preserve the drain receipt's timestamp as well as its owner so its
	// conditional resume remains valid after a placement or allowance edit.
	if !preservesDrain {
		policy.UpdatedAt = q.clock.Now()
	}
	q.s.HostMaintenance[host] = policy
	return nil
}

// DegradedReasons explains a degraded host (#1187): fresh, consistent workers
// whose devices, roots or host hygiene withhold capacity. Each entry names the
// worker, the component, its reason and what clears it.
func (s HostMaintenanceStatus) DegradedReasons() []string {
	var out []string
	for _, w := range s.Workers {
		m := w.Machine
		for _, d := range m.Devices {
			if d.Reason == "" {
				continue
			}
			clear := "clears when the worker's next device probe reports it healthy"
			if deviceFaultReason(d.Reason) || d.RepeatedTeardownFault {
				clear = "quarantined; after repair clear it on the host with `myce device clear-quarantine --state-dir DIR --device " + d.ID + "`, or reboot (a new boot clears it)"
			}
			out = append(out, "worker "+m.ID+" fresh; device "+d.ID+" ("+d.Platform+" "+d.Identity+"): "+d.Reason+"; "+clear)
		}
		for _, r := range m.UnavailableRoots {
			out = append(out, "worker "+m.ID+" fresh; root "+r.Root+" unavailable: "+r.Reason+"; clears when the worker's next root probe succeeds")
		}
		if m.HostHealth != nil && m.HostHealth.DrainReason != "" {
			out = append(out, "worker "+m.ID+" fresh; host hygiene: "+m.HostHealth.DrainReason+"; clears when the host health probe recovers")
		}
	}
	if len(out) == 0 {
		out = append(out, "workers fresh; host health probe evidence unknown or expired; clears with the next valid host health report")
	}
	return out
}
