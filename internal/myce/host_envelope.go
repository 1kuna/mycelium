package myce

import (
	"context"
	"errors"
	"net/url"
	"os"
	"slices"
	"time"
)

const HostObservationWindow = 10 * time.Second

// HostEnvelope is the shared wire result. Pointers preserve unknown evidence.
// Expectation verdicts and placement accounting are supplied by the rest layer.
type HostEnvelope struct {
	*HostRestVerdict
	RestEvidence   HostRestEvidence      `json:"rest_evidence"`
	Schema         int                   `json:"schema_version"`
	Command        string                `json:"command"`
	Host           *string               `json:"host_id"`
	Operation      *string               `json:"operation_id"`
	Boot           *string               `json:"boot_id"`
	Accepted       *uint64               `json:"accepted_revision"`
	Desired        HostEnvelopeState     `json:"desired"`
	Observed       HostEnvelopeState     `json:"observed"`
	LocalExclusion string                `json:"local_exclusion"`
	Settlement     string                `json:"host_settlement"`
	LastClose      *HostClose            `json:"last_close"`
	QueueRevision  *uint64               `json:"queue_observed_revision"`
	Counts         HostEnvelopeCounts    `json:"counts"`
	Units          []ExclusionUnit       `json:"units"`
	Attempts       []HostEnvelopeAttempt `json:"attempts"`
	RestartSafe    *bool                 `json:"restart_safe"`
	At             time.Time             `json:"observed_at"`
	Sources        map[string]*time.Time `json:"source_observed_at"`
	Errors         []ExclusionError      `json:"errors"`
	Ready          *bool                 `json:"ready"`
}
type HostEnvelopeState struct {
	State    *string `json:"state"`
	Revision *uint64 `json:"revision"`
}
type HostEnvelopeCounts struct {
	Active              *int `json:"active_attempts"`
	Preparing           *int `json:"preparing_attempts"`
	Stopping            *int `json:"stopping_attempts"`
	PendingDaemon       *int `json:"pending_daemon_operations"`
	Containers          *int `json:"running_containers"`
	GPUHolders          *int `json:"owned_gpu_holders"`
	Unsettled           *int `json:"unsettled_attempts"`
	Queued              *int `json:"queued_jobs"`
	FencedQueued        *int `json:"fenced_queued_jobs"`
	UnresolvedPlacement *int `json:"unresolved_placement"`
}
type HostEnvelopeAttempt struct {
	Job     string `json:"job_id"`
	Machine string `json:"machine_id"`
	Session string `json:"session_id"`
	Attempt int    `json:"attempt"`
	Release string `json:"release_state"`
}

func hostPtr[T any](v T) *T { return &v }
func UnknownHostEnvelope(command string, clock Clock) HostEnvelope {
	return HostEnvelope{Schema: 1, Command: command, Observed: HostEnvelopeState{State: hostPtr("unknown")}, LocalExclusion: "UNKNOWN", Settlement: "unknown", At: clock.Now().UTC(), Sources: map[string]*time.Time{"local_intent": nil, "observed_state": nil, "cgroups": nil, "docker": nil, "gpu_holders": nil, "queue": nil}, Errors: []ExclusionError{}}
}

// ReadHostObservation copies only control evidence under the queue mutex. No
// update/snapshot/maintenance, local file reads, reconciliation or persistence.
type HostQueueObservation struct {
	Placement *HostPlacementObservation `json:"placement"`
	Host      string                    `json:"host_id"`
	HostRevision
	Boot      string            `json:"boot_id"`
	Desired   string            `json:"desired"`
	At        time.Time         `json:"observed_at"`
	Workers   []HostReadyWorker `json:"workers"`
	Pending   []HostObligation  `json:"pending"`
	Active    int               `json:"active"`
	Preparing int               `json:"preparing"`
	Stopping  int               `json:"stopping"`
}
type HostReadyWorker struct {
	Name string `json:"name"`
	HostRevision
	Boot    string    `json:"boot_id"`
	At      time.Time `json:"observed_at"`
	Healthy bool      `json:"healthy"`
}

func (q *Queue) ReadHostObservation(ctx context.Context, host string) (HostQueueObservation, error) {
	// Mutex.TryLock keeps a stalled writer from exceeding this read's deadline.
	for !q.mu.TryLock() {
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return HostQueueObservation{}, ctx.Err()
		case <-timer.C:
		}
	}
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return HostQueueObservation{}, err
	}
	s := q.s.HostIntents[host]
	v := HostQueueObservation{Host: host, HostRevision: s.HostRevision, Boot: s.Boot, Desired: s.Desired, At: q.clock.Now().UTC(), Workers: []HostReadyWorker{}, Pending: q.hostObligations(host)}
	v.Placement = q.hostPlacementObservation(host)
	// Open readiness permits current work. Only release obligations, rather than
	// all active custody that a future close would discharge, withhold ready.
	if s.Desired == "open" {
		pending := v.Pending[:0]
		for _, p := range v.Pending {
			if q.hostObservationReleasePending(p, s.HostRevision) {
				pending = append(pending, p)
			}
		}
		v.Pending = pending
	}
	for _, m := range q.s.Machines {
		if hostID(m) == host {
			healthy := !m.Gone && !m.Leaving && m.Disabled == "" && m.MemoryError == "" && m.HostMemoryError == "" && m.DockerCleanup == "" && m.AdmissionReason == "" && m.FileTransferError == ""
			v.Workers = append(v.Workers, HostReadyWorker{m.ID, m.HostRevision, m.HostBoot, m.LastSeen, healthy})
		}
	}
	for _, j := range q.s.Jobs {
		if q.executionHost(j.Machine) == host {
			switch j.State {
			case "running":
				v.Active++
			case "stopping":
				v.Stopping++
			}
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Host == host && r.Preparation {
			v.Preparing++
		}
	}
	return v, nil
}

func (q *Queue) hostObservationReleasePending(p HostObligation, revision HostRevision) bool {
	machine, session := p.Result.Machine, p.Result.Session
	if p.Engine != nil {
		machine, session = p.Engine.Machine, p.Engine.Session
	}
	m := q.s.Machines[machine]
	if m == nil || m.Session != session || m.Gone || m.Leaving || m.HostRevision != revision {
		return true
	}
	if p.Engine != nil {
		for _, slot := range q.s.ModelSlots {
			if slot.Machine == machine && slot.Session == session && slot.Model == p.Engine.Model && slot.Attempt == p.Engine.Attempt {
				return slot.State != "loading" && slot.State != "ready"
			}
		}
		return true
	}
	if p.Preparation {
		return true
	}
	for _, r := range q.s.HostReleases {
		if r.Job == p.Result.ID && r.Machine == machine && r.Session == session && r.Attempt == p.Result.Attempt {
			return true
		}
	}
	j := q.s.Jobs[p.Result.ID]
	return j == nil || cancelled(j) || j.State != "running" || j.Machine != machine || j.Session != session || j.Attempt != p.Result.Attempt
}

// HostSnapshot is intentionally separate from mutation execution. All worker
// files and authenticated queue reads stay in the worker identity boundary.
func HostSnapshot(ctx context.Context, v HostInvocation, clock Clock) (HostEnvelope, error) {
	e := UnknownHostEnvelope("status", clock)
	if v.Privileged {
		return e, ErrHostIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, HostObservationWindow)
	defer cancel()
	s, err := ReadHostIntent(v.StateDir)
	if err != nil {
		return e, err
	}
	boot, err := HostBootID()
	if err != nil {
		return e, ErrHostState
	}
	e.Host, e.Operation, e.Boot = hostPtr(s.Host), hostPtr(s.Operation), hostPtr(boot)
	e.Desired = HostEnvelopeState{hostPtr(s.Desired), hostPtr(s.Revision)}
	e.LastClose, e.Settlement = s.LastClose, s.HostSettlement()
	e.RestEvidence.MemoryAllowance = hostPtr(s.MemoryAllowanceBytes)
	captured := clock.Now().UTC()
	e.Sources["local_intent"] = &captured
	if o := s.Observation(boot); o != nil {
		e.Observed = HostEnvelopeState{hostPtr(o.State), hostPtr(o.Revision)}
		e.Sources["observed_state"] = hostPtr(o.At.UTC())
	}
	// Worker-owned saved reports are settlement history. An unprivileged read
	// cannot certify current real GPU-holder evidence from those bytes.
	e.Errors = append(e.Errors, ExclusionError{Component: "gpu_holders", Code: "permission_denied", Message: "Privileged observation is required"})

	// Stored acknowledgements are history, never current queue observations.
	queues, err := (HostControl{Dir: v.StateDir}).Queues()
	if err != nil {
		return e, errors.Join(ErrHostState, err)
	}
	// Installed queue membership exists before the first worker session. Never
	// infer queue release from an empty runtime registry: query this endpoint too.
	if v.Config.Queue != "" && !slices.Contains(queues, v.Config.Queue) {
		queues = append(queues, v.Config.Queue)
	}
	for _, p := range s.Pending {
		if !slices.Contains(queues, p.Queue) {
			queues = append(queues, p.Queue)
		}
	}
	required := v.Config.RequiredWorkers
	if len(required) == 0 && v.Config.Name != "" {
		required = []string{v.Config.Name}
	}
	// Saved Pending is custody from reconciliation, including legitimate current
	// work. The fresh queue observation below distinguishes unresolved release
	// from active custody; a cached inventory must not withhold open readiness.
	ready := s.Desired == "open" && len(required) > 0
	active, preparing, stopping, unsettled := 0, 0, 0, 0
	attempts := []HostEnvelopeAttempt{}
	queueTime := clock.Now().UTC()
	queueOK := len(queues) > 0
	foundWorkers := map[string]HostReadyWorker{}
	placement := HostPlacementObservation{}
	placementOK := true
	if len(queues) > 0 {
		e.RestEvidence.QueueReachable = hostPtr(true)
	}
	client := HostQueueClient([]HostWorker{{Config: v.ConfigPath, Queue: v.Config.Queue}}, os.Getenv("MYCE_TOKEN"))
	for _, endpoint := range queues {
		c, ce := client(endpoint)
		var q HostQueueObservation
		if ce == nil {
			ce = c.Call(ctx, "GET", "/api/hosts/"+url.PathEscape(s.Host)+"/observation", nil, &q)
		}
		if ce != nil || q.Host != s.Host {
			if ce != nil {
				e.RestEvidence.QueueReachable = hostPtr(false)
				e.RestEvidence.CollectionFailed = true
			}
			queueOK = false
			ready = false
			continue
		}
		if q.At.Before(queueTime) {
			queueTime = q.At
		}
		if e.RestEvidence.QueueAt == nil || q.At.Before(*e.RestEvidence.QueueAt) {
			e.RestEvidence.QueueAt = hostPtr(q.At)
		}
		facts := HostRestQueueFacts{OpenAdmission: s.Desired == "closed" && q.Desired == "open"}
		// Keep each queue's partial facts separately: absent evidence cannot
		// erase a fresh violation from another queue.
		e.RestEvidence.QueueFacts = append(e.RestEvidence.QueueFacts, HostRestItem[HostRestQueueFacts]{hostPtr(q.At), HostObservationWindow, &facts})
		if q.HostRevision != s.HostRevision || q.Boot != boot || q.Desired != s.Desired {
			queueOK = false
			ready = false
			continue
		}
		active += q.Active
		for _, fact := range []struct {
			n    int
			code string
		}{{q.Active, "active_attempts"}, {q.Preparing, "preparing_attempts"}, {q.Stopping, "stopping_attempts"}, {len(q.Pending), "release_pending"}} {
			if fact.n > 0 {
				facts.ClosedViolations = append(facts.ClosedViolations, fact.code)
			}
		}
		if q.Placement != nil && q.Placement.Unfenced > 0 {
			facts.ClosedViolations = append(facts.ClosedViolations, "eligible_unfenced_work")
		}
		preparing += q.Preparing
		stopping += q.Stopping
		unsettled += len(q.Pending)
		if q.Placement == nil {
			placementOK = false
		} else {
			placement.Queued += q.Placement.Queued
			placement.Fenced += q.Placement.Fenced
			placement.Unresolved += q.Placement.Unresolved
			placement.Unfenced += q.Placement.Unfenced
		}
		for _, p := range q.Pending {
			if p.Engine == nil && !p.Preparation && p.Result.Attempt > 0 {
				attempts = append(attempts, HostEnvelopeAttempt{p.Result.ID, p.Result.Machine, p.Result.Session, p.Result.Attempt, "pending"})
			}
		}
		for _, name := range required {
			for _, w := range q.Workers {
				if w.Name == name && w.HostRevision == s.HostRevision && w.Boot == boot {
					previous, found := foundWorkers[name]
					if !found || w.At.After(previous.At) || w.At.Equal(previous.At) && !w.Healthy {
						foundWorkers[name] = w
					}
				}
			}
		}
	}
	// Choose the latest fact for each worker regardless of health; preserve its
	// timestamp across the child boundary. Equal-time conflicts use unhealthy.
	for _, name := range required {
		w, found := foundWorkers[name]
		ready = ready && found && w.Healthy
		if found {
			e.RestEvidence.WorkerHealth = append(e.RestEvidence.WorkerHealth, HostRestWorkerHealth{hostPtr(w.At.UTC()), HostObservationWindow, hostPtr(w.Healthy)})
			if e.Sources["required_workers"] == nil || w.At.Before(*e.Sources["required_workers"]) {
				e.Sources["required_workers"] = hostPtr(w.At.UTC())
			}
		}
	}
	if queueOK {
		if placementOK {
			e.Counts.Queued = &placement.Queued
			e.Counts.FencedQueued = &placement.Fenced
			e.Counts.UnresolvedPlacement = &placement.Unresolved
			e.RestEvidence.UnfencedQueued = &placement.Unfenced
		}
		e.QueueRevision = hostPtr(s.Revision)
		e.Sources["queue"] = hostPtr(queueTime.UTC())
		e.Counts.Active = &active
		e.Counts.Preparing = &preparing
		e.Counts.Stopping = &stopping
		e.Counts.Unsettled = &unsettled
		e.Attempts = attempts
		e.Ready = hostPtr(ready && unsettled == 0)
		if *e.Ready {
			e.Observed = HostEnvelopeState{hostPtr("open"), hostPtr(s.Revision)}
			observedAt := queueTime
			if at := e.Sources["required_workers"]; at != nil && at.Before(observedAt) {
				observedAt = *at
			}
			e.Sources["observed_state"] = hostPtr(observedAt)
		}
	}
	// Re-read without taking any mutation lock; a collection crossing an operation
	// or boot fence cannot certify either operation.
	after, err := ReadHostIntent(v.StateDir)
	currentBoot, be := HostBootID()
	e.FinishObservation(clock.Now())
	if err != nil || be != nil || after.HostRevision != s.HostRevision || currentBoot != boot {
		return UnknownHostEnvelope("status", clock), ErrHostRevision
	}
	return e, nil
}

// ClearExclusion removes worker-supplied evidence before privileged collection.
func (e *HostEnvelope) ClearExclusion() {
	e.LocalExclusion = "UNKNOWN"
	e.Sources["exclusion_report"] = nil
	e.RestartSafe = nil
	e.Units = nil
	e.Counts.PendingDaemon = nil
	e.Counts.Containers = nil
	e.Counts.GPUHolders = nil
	for _, name := range []string{"cgroups", "docker", "gpu_holders"} {
		e.Sources[name] = nil
	}
	errors := e.Errors[:0]
	for _, err := range e.Errors {
		if !err.Contrary && err.Component != "gpu_holders" && err.Component != "kernel" {
			errors = append(errors, err)
		}
	}
	e.Errors = errors
}

func (e *HostEnvelope) ApplyExclusion(r *HostExclusionReport, now time.Time) {
	if r == nil || e.Boot == nil || r.Boot != *e.Boot || e.Desired.Revision == nil || r.Revision != *e.Desired.Revision || e.Operation == nil || r.Operation != *e.Operation {
		return
	}
	e.LocalExclusion = r.Verdict
	if e.LocalExclusion == "PASS" && !r.completePass() {
		e.LocalExclusion = "UNKNOWN"
	}
	e.Units = r.Units
	e.Counts.PendingDaemon, e.Counts.Containers, e.Counts.GPUHolders = r.PendingOperations, r.RunningContainers, r.GPUHolders
	e.RestartSafe = r.RestartSafe
	for name, at := range r.Sources {
		if name != "queue" && (name != "local_intent" || at != nil) {
			e.Sources[name] = at
		}
	}
	e.Sources["exclusion_report"] = hostPtr(r.At)
	e.Errors = append(e.Errors, r.Errors...)
	e.FinishObservation(now)
}

var ErrHostReleaseUnconfirmed = errors.New("close accepted, release not confirmed")

func HostEnvelopeError(err error) ExclusionError {
	code, component := "observation_unavailable", "local_intent"
	switch {
	case errors.Is(err, ErrHostReleaseUnconfirmed):
		return ExclusionError{Component: "command", Code: "release_not_confirmed", Message: ErrHostReleaseUnconfirmed.Error()}
	case errors.Is(err, ErrLegacyState):
		code = "legacy_state_present"
	case errors.Is(err, ErrHostStateUninitialized):
		code = "state_uninitialized"
	case errors.Is(err, ErrHostStateCorrupt):
		code = "state_corrupt"
	case errors.Is(err, ErrHostRevision):
		component, code = "host_intent", "revision_mismatch"
	case errors.Is(err, ErrPreviousCloseUnsettled):
		component, code = "host_intent", "previous_close_unsettled"
	case errors.Is(err, ErrHostDeadline) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		component, code = "command", "deadline"
	default:
		var invocation *HostInvocationError
		if errors.As(err, &invocation) {
			component, code = "invocation", invocation.Code
		}
	}
	message := code
	if errors.Is(err, ErrLegacyState) {
		message = err.Error()
	}
	return ExclusionError{Component: component, Code: code, Message: message}
}

// FinishObservation projects the same centralized normalization into the public
// diagnostics. Only the public completion time changes; source times are retained.
func (e *HostEnvelope) FinishObservation(now time.Time) {
	projection, _ := normalizeHostRestEvidence(*e, now)
	*e = projection
}
