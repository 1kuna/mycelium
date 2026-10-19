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
	Host            string               `json:"host"`
	Policy          HostMaintenance      `json:"policy"`
	LocalFence      string               `json:"local_fence,omitempty"`
	Workers         []MaintenanceWorker  `json:"workers"`
	Attempts        []MaintenanceAttempt `json:"attempts"`
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
		policy.UpdatedAt = q.clock.Now()
		q.s.HostMaintenance[host] = policy
		for _, j := range q.s.Jobs {
			if q.executionHost(j.Machine) != host || j.State != "running" && j.State != "stopping" {
				continue
			}
			// Drain leaves authorized work alone. Unstarted assignments must release
			// their preparation/create before being retried elsewhere.
			if request.Stop || policy.Mode != "enabled" && j.StartedAt == nil {
				if !cancelled(j) {
					for i := range j.Copies {
						c := &j.Copies[i]
						if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.StopCause == "" && j.State == "running" {
							c.StopCause = maintenanceStopCause
						}
					}
					q.stop(j, maintenanceStopCause)
				}
			}
			q.retainRelease(j)
		}
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
	out := HostMaintenanceStatus{Host: host, Policy: q.s.HostMaintenance[host], LocalFence: q.hostClosedReason(host), Workers: []MaintenanceWorker{}, Attempts: []MaintenanceAttempt{}, PendingReleases: []HostRelease{}, PendingCleanup: []string{}}
	if out.Policy.Mode == "" {
		out.Policy.Mode = "enabled"
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
		for _, r := range m.EnvironmentReports {
			if r.State == "building" {
				out.Preparations++
			}
		}
		for _, r := range m.CopyReports {
			if r.State == "copying" {
				out.Preparations++
			}
		}
	}
	for _, obligation := range q.hostObligations(host) {
		if obligation.Engine != nil {
			out.Sidecars++
			continue
		}
		if obligation.Preparation {
			continue
		} // counted separately with release custody
		r := obligation.Result
		attempt := MaintenanceAttempt{Execution: r.Execution, Machine: r.Machine, Session: r.Session, State: "release_pending"}
		if j := q.s.Jobs[r.ID]; j != nil {
			out.Sidecars += len(j.Spec.Run.DockerSidecars)
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
	out.Quiescent = out.Healthy && q.maintenanceBlocked(host) && len(out.Attempts) == 0 && out.Preparations == 0 && out.Sidecars == 0 && len(out.PendingReleases) == 0 && len(out.PendingCleanup) == 0
	return out
}
func (a API) registerHostMaintenance(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		status, err := a.Queue.HostMaintenanceStatus(r.PathValue("id"))
		respond(w, status, err)
	})
	mux.HandleFunc("POST /api/hosts/{id}/maintenance", func(w http.ResponseWriter, r *http.Request) {
		var request HostMaintenanceRequest
		if !decode(w, r, &request) {
			return
		}
		status, err := a.Queue.ChangeHostMaintenance(r.PathValue("id"), request)
		respond(w, status, err)
	})
}
