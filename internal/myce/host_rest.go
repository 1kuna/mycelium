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
	QueueReachable   *bool                              `json:"queue_reachable"`
	MemoryAllowance  *int64                             `json:"memory_allowance_bytes"`
	QueueAt          *time.Time                         `json:"queue_observed_at"`
	WorkerHealth     []HostRestWorkerHealth             `json:"worker_health,omitempty"`
	QueueFacts       []HostRestItem[HostRestQueueFacts] `json:"queue_facts,omitempty"`
	CollectionFailed bool                               `json:"collection_failed,omitempty"`
	UnfencedQueued   *int                               `json:"unfenced_queued_jobs"`
}

// HostRestItem binds a fact to its own observation time and freshness bound.
// Positive and negative facts use the same representation and normalization.
type HostRestItem[T any] struct {
	At     *time.Time    `json:"observed_at"`
	MaxAge time.Duration `json:"max_age_ns"`
	Value  *T            `json:"value"`
}

type HostRestWorkerHealth = HostRestItem[bool]

type HostRestQueueFacts struct {
	OpenAdmission    bool     `json:"admission_open"`
	ClosedViolations []string `json:"closed_violations"`
}

// normalizedHostRestEvidence is deliberately private: the verdict evaluator can
// only receive evidence that has passed the single normalization boundary.
type normalizedHostRestEvidence struct {
	ObservationFresh           bool
	Host, Operation, Boot      *string
	Desired, Observed          HostEnvelopeState
	QueueRevision              *uint64
	Counts                     HostEnvelopeCounts
	Ready                      *bool
	LocalExclusion, Settlement string
	LastClose                  *normalizedRestClose
	Errors                     []ExclusionError
	QueueReachable             *bool
	MemoryAllowance            *int64
	UnfencedQueued             *int
	CollectionFailed           bool
	workerHealth               []*bool
	queueFacts                 []*HostRestQueueFacts
}

type normalizedRestClose struct {
	Revision              uint64
	Operation, Settlement string
}

func normalizeRestItem[T any](item HostRestItem[T], now time.Time) *T {
	if item.At == nil || item.At.IsZero() || item.At.After(now) || item.MaxAge <= 0 || now.Sub(*item.At) > item.MaxAge {
		return nil
	}
	return item.Value
}

// normalizeHostRestEvidence maps ALL inputs at one instant, before any verdict
// rule. Expired (including future-dated) facts are absent regardless of sign.
// The input and its wire timestamps are never mutated by verdict evaluation.
func normalizeHostRestEvidence(input HostEnvelope, now time.Time) (HostEnvelope, normalizedHostRestEvidence) {
	e := input
	e.Sources = make(map[string]*time.Time, len(input.Sources))
	n := normalizedHostRestEvidence{}
	sources := map[string]bool{}
	for name, at := range input.Sources {
		value := normalizeRestItem(HostRestItem[bool]{at, HostObservationWindow, hostPtr(true)}, now)
		sources[name] = value != nil
		if value != nil {
			e.Sources[name] = at
		} else {
			e.Sources[name] = nil
		}
	}
	// observed_at remains the public completion time. Preserve the original
	// observation time separately so repeated projections never renew evidence.
	if _, recorded := input.Sources["observation"]; !recorded {
		at := input.At
		sources["observation"] = normalizeRestItem(HostRestItem[bool]{&at, HostObservationWindow, hostPtr(true)}, now) != nil
		if sources["observation"] {
			e.Sources["observation"] = &at
		} else {
			e.Sources["observation"] = nil
		}
	}
	for _, item := range input.RestEvidence.QueueFacts {
		facts := normalizeRestItem(item, now)
		n.queueFacts = append(n.queueFacts, facts)
		if facts == nil {
			sources["queue"] = false
			e.Sources["queue"] = nil
		}
	}
	if !sources["local_intent"] {
		e.Host, e.Operation, e.Boot = nil, nil, nil
		e.Desired = HostEnvelopeState{}
		e.LastClose = nil
		e.Settlement = "unknown"
		e.RestEvidence.MemoryAllowance = nil
	}
	if !sources["observed_state"] {
		e.Observed = HostEnvelopeState{State: hostPtr("unknown")}
	}
	if !sources["queue"] {
		e.QueueRevision = nil
		e.Ready = nil
		e.Counts.Active, e.Counts.Preparing, e.Counts.Stopping, e.Counts.Unsettled = nil, nil, nil, nil
		e.Counts.Queued, e.Counts.FencedQueued, e.Counts.UnresolvedPlacement = nil, nil, nil
		e.RestEvidence.UnfencedQueued = nil
		e.Attempts = nil
	}
	if !sources["required_workers"] || !sources["local_intent"] {
		e.Ready = nil
	}
	for _, health := range input.RestEvidence.WorkerHealth {
		value := normalizeRestItem(health, now)
		n.workerHealth = append(n.workerHealth, value)
		if value == nil {
			e.Ready = nil
		}
	}
	// Partial queue facts have their own timestamp, independent of a complete
	// acknowledgement. They may prove a violation when another queue is absent.
	if normalizeRestItem(HostRestItem[bool]{input.RestEvidence.QueueAt, HostObservationWindow, hostPtr(true)}, now) == nil {
		e.RestEvidence.QueueReachable = nil
	}
	reportAt, hasReport := input.Sources["exclusion_report"]
	reportFresh := !hasReport || reportAt != nil && sources["exclusion_report"]
	localComplete := reportFresh
	for _, name := range []string{"local_intent", "cgroups", "docker", "gpu_holders"} {
		localComplete = localComplete && sources[name]
	}
	if !localComplete {
		e.LocalExclusion = "UNKNOWN"
		e.RestartSafe = nil
	}
	if !sources["cgroups"] || !reportFresh {
		e.Units = nil
	}
	if !sources["docker"] || !reportFresh {
		e.Counts.Containers, e.Counts.PendingDaemon = nil, nil
	}
	if !sources["gpu_holders"] || !reportFresh {
		e.Counts.GPUHolders = nil
	}
	e.Errors = []ExclusionError{}
	for _, failure := range input.Errors {
		// Contrary facts were observed at report time, even when their source
		// inventory is incomplete. Completeness is required only to prove PASS.
		if !failure.Contrary || reportFresh {
			e.Errors = append(e.Errors, failure)
		}
	}
	n.ObservationFresh = sources["observation"]
	n.Host, n.Operation, n.Boot = e.Host, e.Operation, e.Boot
	n.Desired, n.Observed = e.Desired, e.Observed
	n.QueueRevision, n.Counts, n.Ready = e.QueueRevision, e.Counts, e.Ready
	n.LocalExclusion, n.Settlement, n.Errors = e.LocalExclusion, e.Settlement, e.Errors
	n.QueueReachable, n.MemoryAllowance = e.RestEvidence.QueueReachable, e.RestEvidence.MemoryAllowance
	n.UnfencedQueued, n.CollectionFailed = e.RestEvidence.UnfencedQueued, e.RestEvidence.CollectionFailed
	if e.LastClose != nil {
		n.LastClose = &normalizedRestClose{e.LastClose.Revision, e.LastClose.Operation, e.LastClose.Settlement}
	}
	e.At = now.UTC()
	return e, n
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

// ApplyRestVerdict is the normalization/evaluation adapter. All rules live in
// restVerdict, which cannot access a clock or perform collection.
func (e *HostEnvelope) ApplyRestVerdict(expect string, now time.Time) {
	projection, n := normalizeHostRestEvidence(*e, now)
	*e = projection
	e.HostRestVerdict = restVerdict(n, expect)
}

// restVerdict is a pure function of normalized evidence and expectation.
func restVerdict(n normalizedHostRestEvidence, expect string) *HostRestVerdict {
	e := &n
	partial := []string{}
	openAdmission := false
	for _, facts := range n.queueFacts {
		if facts != nil {
			openAdmission = openAdmission || facts.OpenAdmission
			partial = append(partial, facts.ClosedViolations...)
		}
	}
	r := &HostRestVerdict{Expectation: expect, Verdict: "PASS", Reasons: []string{}, QueueReachable: e.QueueReachable, MemoryAllowance: e.MemoryAllowance}
	add := func(severity, code string) {
		if !slices.Contains(r.Reasons, code) {
			r.Reasons = append(r.Reasons, code)
		}
		if severity == "FAIL" || severity == "UNKNOWN" && r.Verdict != "FAIL" {
			r.Verdict = severity
		}
	}
	if !n.ObservationFresh {
		add("UNKNOWN", "observation_stale")
	}
	if e.Host == nil || e.Operation == nil || e.Boot == nil || e.Desired.Revision == nil {
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
	if e.QueueRevision == nil || e.Desired.Revision == nil {
		add("UNKNOWN", "queue_acknowledgement_unknown")
	} else if *e.QueueRevision != *e.Desired.Revision {
		add("FAIL", "queue_revision_mismatch")
	}
	if r.QueueReachable == nil || !*r.QueueReachable {
		add("UNKNOWN", "queue_unreachable")
	}
	checkCount := func(n *int, code string) {
		if n == nil {
			add("UNKNOWN", code+"_unknown")
		} else if *n > 0 {
			if code == "release_pending" {
				add("UNKNOWN", code)
			} else {
				add("FAIL", code)
			}
		}
	}

	if expect == "ready" {
		if slices.Contains(partial, "release_pending") {
			add("UNKNOWN", "release_pending")
		}
		for _, health := range n.workerHealth {
			if health != nil && !*health {
				add("FAIL", "required_worker_unhealthy")
			}
		}
		if e.Ready == nil || !*e.Ready {
			add("UNKNOWN", "required_workers_not_ready")
		}
		checkCount(e.Counts.Unsettled, "release_pending")
	} else {
		if openAdmission {
			add("FAIL", "queue_admission_open")
		}
		switch e.LocalExclusion {
		case "FAIL":
			add("FAIL", "local_exclusion_failed")
		case "PASS":
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
		for _, code := range partial {
			if code == "release_pending" {
				add("UNKNOWN", code)
			} else {
				add("FAIL", code)
			}
		}
		if e.Counts.Queued == nil || e.Counts.FencedQueued == nil || e.UnfencedQueued == nil {
			add("UNKNOWN", "placement_unknown")
		} else {
			if *e.UnfencedQueued > 0 {
				add("FAIL", "eligible_unfenced_work")
			}
			if *e.Counts.FencedQueued > 0 {
				add("PASS", "queued_work_fenced")
			}
		}
		if e.Counts.UnresolvedPlacement == nil || *e.Counts.UnresolvedPlacement > 0 {
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
	if e.CollectionFailed {
		add("UNKNOWN", "collection_failed")
		r.Verdict = "UNKNOWN"
	}
	sort.Strings(r.Reasons)
	return r
}
