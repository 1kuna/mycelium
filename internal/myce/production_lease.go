package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

// ProductionLease is a project's own declaration of open production work,
// including pipeline stages it has not submitted yet. Planned drains (operator
// `host drain` and fleet upgrades) keep at least one enabled host that could
// start each declared stage until the project closes the lease or it expires.
// It grants no placement priority and reserves no capacity. A lease with no
// stages protects any host the project may use.
type ProductionLease struct {
	Project   string            `json:"project"`
	Key       string            `json:"key"`
	Stages    []ProductionStage `json:"stages,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// ProductionStage carries only the placement fields that decide which hosts
// could start the stage; it is the same eligibility a submitted job uses.
type ProductionStage struct {
	Needs            ResourceNeeds `json:"needs"`
	AllowedMachines  []string      `json:"allowed_machines,omitempty"`
	ExcludedMachines []string      `json:"excluded_machines,omitempty"`
}

// ProductionLeaseRequest opens or renews (same project and key) a lease.
// TTL is a Go duration chosen by the project; renewal replaces its expiry.
type ProductionLeaseRequest struct {
	Project string            `json:"project"`
	Key     string            `json:"key"`
	Stages  []ProductionStage `json:"stages,omitempty"`
	TTL     string            `json:"ttl"`
}

func productionLeaseID(project, key string) string { return project + "/" + key }

func (r ProductionLeaseRequest) validate() (time.Duration, error) {
	if strings.TrimSpace(r.Project) == "" || validateProjectIdentifier(r.Project) != nil {
		return 0, reject("production_lease_invalid", errors.New("project is required and cannot contain slash"))
	}
	if strings.TrimSpace(r.Key) == "" || strings.Contains(r.Key, "/") {
		return 0, reject("production_lease_invalid", errors.New("key is required and cannot contain slash"))
	}
	ttl, err := time.ParseDuration(r.TTL)
	if err != nil || ttl <= 0 {
		// The lease must end on its own; the project picks how long its
		// pipeline may still submit stages.
		return 0, reject("production_lease_invalid", errors.New("ttl must be a positive duration such as 2h"))
	}
	for _, s := range r.Stages {
		if !validMemoryValue(s.Needs.MemoryGB) || !validMemoryValue(s.Needs.HostMemoryGB) || s.Needs.CPUCores < 0 {
			return 0, reject("production_lease_invalid", errors.New("stage needs must be finite and nonnegative"))
		}
	}
	return ttl, nil
}

func (q *Queue) OpenProductionLease(request ProductionLeaseRequest, requestContexts ...context.Context) (ProductionLease, error) {
	ttl, err := request.validate()
	if err != nil {
		return ProductionLease{}, err
	}
	var out ProductionLease
	err = q.updateRequest(requestContext(requestContexts), func() error {
		for _, s := range request.Stages {
			if err := q.validateMachineNames(append(append([]string(nil), s.AllowedMachines...), s.ExcludedMachines...)); err != nil {
				return err
			}
		}
		now := q.clock.Now()
		out = ProductionLease{Project: request.Project, Key: request.Key, Stages: append([]ProductionStage(nil), request.Stages...), ExpiresAt: now.Add(ttl), UpdatedAt: now}
		if q.s.ProductionLeases == nil {
			q.s.ProductionLeases = map[string]ProductionLease{}
		}
		q.s.ProductionLeases[productionLeaseID(out.Project, out.Key)] = out
		return nil
	}, nil)
	return out, err
}

// CloseProductionLease is idempotent: closing an absent or expired lease succeeds.
func (q *Queue) CloseProductionLease(project, key string, requestContexts ...context.Context) error {
	return q.updateRequest(requestContext(requestContexts), func() error {
		delete(q.s.ProductionLeases, productionLeaseID(project, key))
		return nil
	}, nil)
}

func (q *Queue) ProductionLeases(requestContexts ...context.Context) []ProductionLease {
	unlock := requestReadLock(requestContext(requestContexts), &q.mu)
	defer unlock()
	return q.openProductionLeases()
}

func (q *Queue) openProductionLeases() []ProductionLease {
	now := q.clock.Now()
	out := []ProductionLease{}
	for _, l := range q.s.ProductionLeases {
		if now.Before(l.ExpiresAt) {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return productionLeaseID(out[i].Project, out[i].Key) < productionLeaseID(out[j].Project, out[j].Key)
	})
	return out
}

// Expired leases have no effect; dropping them only keeps state small.
func (q *Queue) pruneProductionLeases() {
	now := q.clock.Now()
	for id, l := range q.s.ProductionLeases {
		if !now.Before(l.ExpiresAt) {
			delete(q.s.ProductionLeases, id)
		}
	}
}

// productionDemand is one thing that would fail if every host able to start it
// were drained: an open lease's stage, a queued production job whose start
// deadline makes an all-drained host set a failure ("no eligible host is
// enabled"), or a production stage declared with after: that will get that
// deadline when its upstream releases it (#1374).
type productionDemand struct {
	job   *Job
	label string
}

func (q *Queue) productionDemands() []productionDemand {
	var out []productionDemand
	for _, l := range q.openProductionLeases() {
		label := fmt.Sprintf("production lease %s (project %s, expires %s)", l.Key, l.Project, l.ExpiresAt.UTC().Format(time.RFC3339))
		for _, j := range l.stageJobs() {
			out = append(out, productionDemand{job: j, label: label})
		}
	}
	for _, j := range q.liveJobs() {
		// Dependency readiness is not required: a pipeline submitted ahead of
		// time fails at its first ready stage if its hosts were drained
		// meanwhile. A blocked stage has no start deadline yet, but its
		// placement fields are known, so it counts like a lease stage. Upstream
		// failure or cancellation settles it to failed or cancelled in the same
		// transaction, which ends the protection.
		if j.State == "queued" && j.Attempt == 0 && j.Spec.Priority == "production" && (j.Spec.StartBy != nil || j.Spec.After != nil) && !cancelled(j) {
			label := fmt.Sprintf("queued production job %s (project %s)", j.ID, j.Spec.Project)
			if j.Spec.After != nil && !dependencyReady(j) {
				label = fmt.Sprintf("production stage %s (project %s, waiting on after: %s)", j.ID, j.Spec.Project, j.Spec.After.Job)
			}
			out = append(out, productionDemand{job: j, label: label})
		}
	}
	return out
}

// stageJobs is each declared stage as the job placement would evaluate; a
// lease without stages stands for any work the project may run.
func (l ProductionLease) stageJobs() []*Job {
	stages := l.Stages
	if len(stages) == 0 {
		stages = []ProductionStage{{Needs: ResourceNeeds{Platform: Platform{Any: true}}}}
	}
	var out []*Job
	for _, s := range stages {
		out = append(out, &Job{Spec: Spec{Project: l.Project, Priority: "production", Needs: s.Needs, AllowedMachines: s.AllowedMachines, ExcludedMachines: s.ExcludedMachines}})
	}
	return out
}

// productionLeaseHosts lists the hosts able to start any stage of the lease:
// planned drains keep at least one of them enabled while it is open.
func (q *Queue) productionLeaseHosts(l ProductionLease) []string {
	var hosts []string
	for _, j := range l.stageJobs() {
		for _, h := range q.startEligibleHosts(j) {
			if !slices.Contains(hosts, h) {
				hosts = append(hosts, h)
			}
		}
	}
	slices.Sort(hosts)
	return hosts
}

// productionDrainBlocker names open production work that draining host would
// leave without any enabled host able to start it, or returns "". A drain owned
// by the running fleet upgrade counts as enabled: that work waits for the host
// to return instead of failing (see allStartHostsDisabled).
func (q *Queue) productionDrainBlocker(host string) string {
	for _, d := range q.productionDemands() {
		eligible := q.startEligibleHosts(d.job)
		if !slices.Contains(eligible, host) {
			continue
		}
		other := false
		for _, h := range eligible {
			if h != host && (!q.maintenanceBlocked(h) || q.fleetDrainTemporary(h)) {
				other = true
				break
			}
		}
		if !other {
			return fmt.Sprintf("%s has no other enabled host that can start it", d.label)
		}
	}
	return ""
}

func (a API) registerProductionLeases(mux *requestMux) {
	mux.HandleFunc("GET /api/production-leases", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		respond(w, struct {
			Leases []ProductionLease `json:"leases"`
		}{a.Queue.ProductionLeases(r.Context())}, nil)
	})
	mux.HandleFunc("POST /api/production-leases", func(w http.ResponseWriter, r *http.Request) {
		var request ProductionLeaseRequest
		if !decode(w, r, &request) {
			return
		}
		out, err := a.Queue.OpenProductionLease(request, r.Context())
		respondOperation(w, out, err, "production_lease_invalid")
	})
	mux.HandleFunc("DELETE /api/production-leases/{project}/{key}", func(w http.ResponseWriter, r *http.Request) {
		err := a.Queue.CloseProductionLease(r.PathValue("project"), r.PathValue("key"), r.Context())
		respondOperation(w, struct {
			Closed bool `json:"closed"`
		}{err == nil}, err, "production_lease_invalid")
	})
}
