package myce

import (
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

const maintenanceStopCause = "planned_host_maintenance"

// Queue-owned policy, keyed by physical host identity, never by worker session.
// A nil budget is possible before the first worker report; zero is uncapped.
// Existing hosts bootstrap their allowance once, then worker reports cannot edit it.
type HostMaintenance struct {
	StopAt       *time.Time        `json:"stop_at,omitempty"`
	Mode         string            `json:"mode"`
	MemoryBudget *HostMemoryBudget `json:"memory_budget,omitempty"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

type HostMaintenanceRequest struct {
	Action         string   `json:"action"`
	Stop           bool     `json:"stop,omitempty"`
	MemoryBudgetGB *float64 `json:"memory_budget_gb,omitempty"`
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
	Host   string          `json:"host"`
	Policy HostMaintenance `json:"policy"`

	Workers         []MaintenanceWorker  `json:"workers"`
	Attempts        []MaintenanceAttempt `json:"attempts"`
	Calls           int                  `json:"calls"`
	Preparations    int                  `json:"preparations"`
	Sidecars        int                  `json:"sidecars"`
	PendingReleases []HostRelease        `json:"pending_releases"`
	PendingCleanup  []string             `json:"pending_cleanup"`
	MemoryInUseGB   float64              `json:"memory_in_use_gb"`
	MemoryDebtGB    float64              `json:"memory_debt_gb"`
	Healthy         bool                 `json:"healthy"`
	Quiescent       bool                 `json:"quiescent"`
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
			return c.StopCause == maintenanceStopCause
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
		policy.MemoryBudget = &budget
		policy.UpdatedAt = q.clock.Now()
	}
	q.s.HostMaintenance[hostID(m)] = policy
	return nil
}

// ChangeHostMaintenance serializes admission, Started, cancellation and policy edits
// with the same SQLite transaction. Acceptance alone never settles an attempt.
func (q *Queue) ChangeHostMaintenance(host string, request HostMaintenanceRequest) (HostMaintenanceStatus, error) {
	var out HostMaintenanceStatus
	if strings.TrimSpace(host) == "" {
		return out, errors.New("physical host is required")
	}
	err := q.update(func() error {
		if q.s.HostMaintenance == nil {
			q.s.HostMaintenance = map[string]HostMaintenance{}
		}
		policy := q.s.HostMaintenance[host]
		if policy.Mode == "" {
			policy.Mode = "enabled"
		}
		switch request.Action {
		case "drain":
			policy.Mode = "drained"
		case "disable":
			policy.Mode = "disabled"
		case "resume":
			policy.Mode = "enabled"
		case "budget":
		default:
			return errors.New("expected drain, disable, resume or budget")
		}
		if request.Stop && request.Action != "disable" {
			return errors.New("--stop requires disable")
		}
		if request.Action == "budget" && request.MemoryBudgetGB == nil {
			return errors.New("budget requires positive GiB")
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
		if request.Action == "resume" || request.Action == "drain" {
			policy.StopAt = nil
		}
		if request.Stop && policy.StopAt == nil {
			deadline := q.clock.Now().Add(StopGrace)
			policy.StopAt = &deadline
		}
		policy.UpdatedAt = q.clock.Now()
		q.s.HostMaintenance[host] = policy
		q.maintain()
		out = q.hostMaintenanceStatus(host)
		return nil
	})
	return out, err
}

func (q *Queue) HostMaintenanceStatus(host string) (HostMaintenanceStatus, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// A closed/failed queue cannot certify health using its in-memory snapshot.
	if !q.simulation {
		if err := q.db.Ping(); err != nil {
			return HostMaintenanceStatus{}, err
		}
	}
	q.refreshHostMemory()
	return q.hostMaintenanceStatus(host), nil
}
func (q *Queue) hostMaintenanceStatus(host string) HostMaintenanceStatus {
	out := HostMaintenanceStatus{Host: host, Policy: q.s.HostMaintenance[host], Workers: []MaintenanceWorker{}, Attempts: []MaintenanceAttempt{}, PendingReleases: []HostRelease{}, PendingCleanup: []string{}}
	if out.Policy.Mode == "" {
		out.Policy.Mode = "enabled"
	}
	if out.Policy.StopAt != nil {
		deadline := *out.Policy.StopAt
		out.Policy.StopAt = &deadline
	}
	if out.Policy.MemoryBudget != nil {
		budget := *out.Policy.MemoryBudget
		out.Policy.MemoryBudget = &budget
	}
	out.Healthy = true
	now := q.clock.Now()
	for _, m := range q.s.Machines {
		if hostID(m) != host {
			continue
		}
		fresh := !m.Gone && !q.unreported[m.ID] && !m.LastSeen.After(now) && now.Sub(m.LastSeen) < min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2]))
		out.Workers = append(out.Workers, MaintenanceWorker{Machine: cloneMachine(m), Fresh: fresh})
		out.Healthy = out.Healthy && fresh && !m.HostMemoryPolicyMismatch && m.MemoryError == "" && m.HostMemoryError == ""
		out.MemoryInUseGB = max(out.MemoryInUseGB, m.HostMemoryInUseGB)
		out.MemoryDebtGB = max(out.MemoryDebtGB, m.hostReservationDebtGB)
		if m.DockerCleanup != "" {
			out.PendingCleanup = append(out.PendingCleanup, m.ID+": "+m.DockerCleanup)
		}
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
			out.Sidecars += len(j.Spec.Run.DockerSidecars) + nativeSidecarCount(j, q.s.Machines[r.Machine])
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
	sort.Strings(out.PendingCleanup)
	out.Healthy = out.Healthy && len(out.Workers) > 0 && out.Policy.MemoryBudget != nil
	out.Quiescent = out.Healthy && q.maintenanceBlocked(host) && len(obligations) == 0 && len(out.PendingCleanup) == 0
	return out
}
func (a API) registerHostMaintenance(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		status, err := a.Queue.HostMaintenanceStatus(r.PathValue("id"))
		respondOperation(w, status, err, "host_control_invalid")
	})
	mux.HandleFunc("POST /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		var request HostMaintenanceRequest
		if !decode(w, r, &request) {
			return
		}
		status, err := a.Queue.ChangeHostMaintenance(r.PathValue("id"), request)
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
		s := q.s.ModelSlots[slotKey(e.Machine, e.Model)]
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
			if deadline != nil {
				boundStop(&j.StopAt, *deadline)
			}
		}
		q.retainRelease(j)
	}
}
