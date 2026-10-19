package myce

import (
	"slices"
	"sort"
	"time"
)

// HostRestVerdict is present as a group only when the caller requests a verdict.
// Null contact/allowance evidence remains null in the public result.
type HostRestVerdict struct {
	Expectation     string   `json:"expectation"`
	Verdict         string   `json:"verdict"`
	Reasons         []string `json:"reasons"`
	QueueReachable  *bool    `json:"queue_reachable"`
	MemoryAllowance *int64   `json:"memory_allowance_bytes"`
}

// Typed observation facts survive the dropped-child boundary. Diagnostics and
// human hold messages are not inputs to the rest decision.
type HostRestEvidence struct {
	QueueReachable   *bool      `json:"queue_reachable"`
	MemoryAllowance  *int64     `json:"memory_allowance_bytes"`
	QueueAt          *time.Time `json:"queue_observed_at"`
	ReadyViolation   bool       `json:"required_worker_unhealthy"`
	OpenAdmission    bool       `json:"queue_admission_open"`
	ClosedViolations []string   `json:"closed_violations"`
	UnfencedQueued   *int       `json:"unfenced_queued_jobs"`
}

type HostPlacementObservation struct {
	Queued     int `json:"queued"`
	Fenced     int `json:"fenced"`
	Unresolved int `json:"unresolved"`
	Unfenced   int `json:"unfenced"`
}

// Called under q.mu: only bounded in-memory projections, no status maintenance,
// filesystem, network, manifest work or interpretation of diagnostic text.
func (q *Queue) hostPlacementObservation(host string) *HostPlacementObservation {
	out := &HostPlacementObservation{}
	visit := func(j *Job) {
		// Copies retain custody even after terminal/removal and policy changes.
		for _, c := range j.Copies {
			if !c.ReleaseConfirmed && q.s.Machines[c.Machine] == nil {
				out.Unresolved++
			}
		}
		if terminal(j.State) {
			return
		}
		if j.State != "queued" {
			if j.Machine != "" && q.s.Machines[j.Machine] == nil {
				out.Unresolved++
			}
			return
		}
		projected := *j
		q.statusEligibility(&projected)
		local := projected.EligibleUnrestricted || projected.EligibleHosts != nil && slices.Contains(*projected.EligibleHosts, host)
		unknown := len(projected.EligibleUnknownMachines) > 0
		if unknown {
			out.Unresolved++
		}
		if !local && !unknown {
			return
		} // positively foreign-only
		out.Queued++
		if local {
			if q.s.HostIntents[host].Desired == "closed" {
				out.Fenced++
			} else {
				out.Unfenced++
			}
		}
	}
	for _, j := range q.s.Jobs {
		visit(j)
	}
	for _, r := range q.s.Submissions {
		if r.Job != nil && q.s.Jobs[r.JobID] == nil {
			visit(r.Job)
		}
	}
	// Calls use the same eligibility projection as generic status. Current
	// in-flight calls are already covered by host engine custody.
	for _, c := range q.calls {
		if c.Machine == "" {
			visit(&Job{State: "queued", Spec: Spec{Project: c.Project}})
		} else if q.s.Machines[c.Machine] == nil {
			out.Unresolved++
		}
	}
	return out
}

// ApplyRestVerdict is a pure product decision over one fenced observation. It
// never collects, mutates host state, or substitutes elapsed time for release.
func (e *HostEnvelope) ApplyRestVerdict(expect string, now time.Time) {
	r := &HostRestVerdict{Expectation: expect, Verdict: "PASS", Reasons: []string{}, QueueReachable: e.RestEvidence.QueueReachable, MemoryAllowance: e.RestEvidence.MemoryAllowance}
	add := func(severity, code string) {
		if !slices.Contains(r.Reasons, code) {
			r.Reasons = append(r.Reasons, code)
		}
		if severity == "FAIL" || severity == "UNKNOWN" && r.Verdict != "FAIL" {
			r.Verdict = severity
		}
	}
	source := func(name string) bool { at := e.Sources[name]; return at != nil && freshHostSource(*at, now) }
	if !freshHostSource(e.At, now) {
		add("UNKNOWN", "observation_stale")
	}
	if e.Host == nil || e.Operation == nil || e.Boot == nil || e.Desired.Revision == nil || !source("local_intent") {
		add("UNKNOWN", "host_identity_unknown")
	}
	wanted := "closed"
	if expect == "ready" {
		wanted = "open"
	}
	if e.Desired.State == nil {
		add("UNKNOWN", "desired_state_unknown")
	} else if *e.Desired.State != wanted {
		add("FAIL", "desired_state_mismatch")
	}
	if e.Observed.State == nil || *e.Observed.State == "unknown" || e.Observed.Revision == nil {
		add("UNKNOWN", "observed_state_unknown")
	} else if *e.Observed.State != wanted || e.Desired.Revision != nil && *e.Observed.Revision != *e.Desired.Revision {
		add("FAIL", "observed_state_mismatch")
	}
	if !source("queue") || e.QueueRevision == nil {
		add("UNKNOWN", "queue_acknowledgement_unknown")
	} else if e.Desired.Revision == nil || *e.QueueRevision != *e.Desired.Revision {
		add("FAIL", "queue_revision_mismatch")
	}
	if r.QueueReachable == nil || !*r.QueueReachable {
		add("UNKNOWN", "queue_unreachable")
	}
	checkCount := func(n *int, code string) {
		if n == nil || !source("queue") {
			add("UNKNOWN", code+"_unknown")
		} else if *n > 0 {
			if code == "release_pending" {
				add("UNKNOWN", code)
			} else {
				add("FAIL", code)
			}
		}
	}
	partialFresh := e.RestEvidence.QueueAt != nil && freshHostSource(*e.RestEvidence.QueueAt, now)
	if expect == "ready" {
		if partialFresh && slices.Contains(e.RestEvidence.ClosedViolations, "release_pending") {
			add("UNKNOWN", "release_pending")
		}
		if partialFresh && e.RestEvidence.ReadyViolation {
			add("FAIL", "required_worker_unhealthy")
		}
		if e.Ready == nil || !*e.Ready {
			add("UNKNOWN", "required_workers_not_ready")
		}
		checkCount(e.Counts.Unsettled, "release_pending")
	} else {
		if partialFresh && e.RestEvidence.OpenAdmission {
			add("FAIL", "queue_admission_open")
		}
		switch e.LocalExclusion {
		case "FAIL":
			add("FAIL", "local_exclusion_failed")
		case "PASS":
			for _, name := range []string{"cgroups", "docker", "gpu_holders"} {
				if !source(name) {
					add("UNKNOWN", "local_evidence_unknown")
				}
			}
		default:
			add("UNKNOWN", "local_exclusion_unknown")
		}
		// WP3 owns the complete local exclusion predicate. Counters are
		// diagnostics here: an unresolved daemon operation is not proof of
		// live execution, and must not promote its UNKNOWN verdict to FAIL.
		for _, err := range e.Errors {
			if err.Contrary {
				add("FAIL", err.Component+"/"+err.Code)
			}
		}
		if e.LastClose == nil || e.Desired.Revision == nil || e.Operation == nil || e.LastClose.Revision != *e.Desired.Revision || e.LastClose.Operation != *e.Operation || e.LastClose.Settlement != "confirmed" || e.Settlement != "confirmed" {
			add("UNKNOWN", "close_settlement_pending")
		}
		checkCount(e.Counts.Active, "active_attempts")
		checkCount(e.Counts.Preparing, "preparing_attempts")
		checkCount(e.Counts.Stopping, "stopping_attempts")
		checkCount(e.Counts.Unsettled, "release_pending")
		if partialFresh {
			for _, code := range e.RestEvidence.ClosedViolations {
				if code == "release_pending" {
					add("UNKNOWN", code)
				} else {
					add("FAIL", code)
				}
			}
		}
		if e.Counts.Queued == nil || e.Counts.FencedQueued == nil || e.RestEvidence.UnfencedQueued == nil || !source("queue") {
			add("UNKNOWN", "placement_unknown")
		} else {
			if *e.RestEvidence.UnfencedQueued > 0 {
				add("FAIL", "eligible_unfenced_work")
			}
			if *e.Counts.FencedQueued > 0 {
				add("PASS", "queued_work_fenced")
			}
		}
		if e.Counts.UnresolvedPlacement == nil || !source("queue") || *e.Counts.UnresolvedPlacement > 0 {
			add("UNKNOWN", "placement_unknown")
		}
	}
	for _, err := range e.Errors {
		if err.Component == "command" && err.Code == "deadline" {
			add("UNKNOWN", "timeout")
		}
		if err.Component == "host_intent" && err.Code == "revision_mismatch" {
			add("UNKNOWN", "collection_fence_changed")
		}
	}
	sort.Strings(r.Reasons)
	e.HostRestVerdict = r
}
