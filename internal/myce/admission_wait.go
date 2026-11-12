package myce

import (
	"cmp"
	"context"
	"encoding/json"
	"log"
	"slices"
	"strings"
	"sync"
	"time"
)

// One build and one replaceable pending snapshot per queue. No timer or queue
// clock advance is needed: ordinary maintenance (including Tick) starts due work.
// A cooldown equal to the last build's measured elapsed cost prevents repeated
// backlog planning from occupying a core continuously. Its cost is observation
// latency: after a change, publication takes at most the remaining active build,
// one cooldown, the next maintenance interval, and one build/commit, absent
// control contention or a lifecycle fence. Under contention capture may yield; Latest.At and
// DeferredAt remain the evidence's age/coverage indicators, not a freshness SLA.
type admissionWaitBuilder struct {
	mu      sync.Mutex
	running bool
	pending *admissionWaitCapture
	next    time.Time
	done    chan struct{}
	latest  uint64
	// synchronous makes the committing caller publish its own capture before
	// returning, with no cooldown. Production keeps it false.
	synchronous bool
}

// synchronousAdmissionWaitDefault is the builder mode a queue opens with.
// Production leaves it false; this package's tests set it true so fixtures
// that read planning state never race a background publication.
var synchronousAdmissionWaitDefault = false

// WithSynchronousAdmissionWait selects the admission-wait evidence mode. When
// on, a committing transaction returns only after its post-commit capture,
// and any capture still pending from a concurrent commit, has been published
// or fenced off. Builds stay serialized and revision-ordered. Production uses
// the asynchronous, cooled-down builder; tests of that path opt into it here.
func WithSynchronousAdmissionWait(on bool) QueueOption {
	return func(q *Queue) { q.admissionWaitBuild.synchronous = on }
}

func (q *Queue) enqueueAdmissionWait(c *admissionWaitCapture) {
	if c.index == nil || q.stopping.Load() {
		return
	}
	b := &q.admissionWaitBuild
	b.mu.Lock()
	defer b.mu.Unlock()
	// Post-commit hooks can arrive out of order after releasing q.mu.
	if c.revision < b.latest {
		return
	}
	b.latest = c.revision
	if b.pending != nil {
		c = mergeAdmissionWaitCaptures(b.pending, c)
	}
	b.pending = c
	if b.synchronous {
		q.drainAdmissionWaitLocked(b)
		return
	}
	if b.running || c.queueClock.Now().Before(b.next) {
		return
	}
	c, b.pending = b.pending, nil
	b.running, b.done = true, make(chan struct{})
	go q.runAdmissionWait(b, c)
}

// runAdmissionWait performs one claimed build and records its cooldown.
func (q *Queue) runAdmissionWait(b *admissionWaitBuilder, c *admissionWaitCapture) {
	started := holdNow(c.clock)
	q.finishAdmissionWait(c)
	elapsed := max(0, holdNow(c.clock).Sub(started))
	b.mu.Lock()
	// Rate decisions use the injected queue Clock; cost measurement uses
	// the captured observational Clock, independently of scheduling time.
	b.next = c.queueClock.Now().Add(max(elapsed, c.service))
	b.running = false
	close(b.done)
	b.mu.Unlock()
}

// drainAdmissionWaitLocked runs builds inline until none is running or
// pending. Called with b.mu held; it releases b.mu while a build runs. The
// caller owns no q.mu here: the evidence commit takes it itself.
func (q *Queue) drainAdmissionWaitLocked(b *admissionWaitBuilder) {
	for {
		if b.running {
			done := b.done
			b.mu.Unlock()
			<-done
			b.mu.Lock()
			continue
		}
		if b.pending == nil {
			return
		}
		c := b.pending
		b.pending = nil
		b.running, b.done = true, make(chan struct{})
		b.mu.Unlock()
		q.runAdmissionWait(b, c)
		b.mu.Lock()
	}
}

// The commit already freezes these inputs for supported status reads. Capturing
// their pointers adds no backlog copy or diagnostic planner work under q.mu.
type admissionWaitCapture struct {
	index      *publishedReadIndex
	decisions  map[string]map[string]admissionWaitDecision
	admissions map[string]map[string]AdmissionDecision
	bytes      uint64
	service    time.Duration
	clock      Clock
	queueClock Clock
	revision   uint64
	refresh    time.Time
}

func (q *Queue) freezeAdmissionWait() *admissionWaitCapture {
	for _, j := range q.records.live {
		if j.State == "queued" && (j.Spec.Priority == "production" || j.AdmissionWait != nil || q.admissionTrace != nil && len(q.admissionTrace.jobs[j.ID]) > 0) {
			_, service := q.evidencePressure()
			var admissions map[string]map[string]AdmissionDecision
			if q.admissionTrace != nil {
				admissions = q.admissionTrace.jobs
			}
			return &admissionWaitCapture{admissions: admissions, index: q.publishedReads.Load(), decisions: q.admissionWaitDecisions, bytes: q.evidenceBytes, service: service, clock: q.holdClock, queueClock: q.clock, revision: q.fileReferenceRevision}
		}
	}
	q.admissionWaitLast.Store(nil)
	return nil
}

// Coalesce projections, not the latest real per-target scheduler fact. A Tick
// between check-ins must not erase a rejection that has not been published yet.
// Input maps remain immutable; copy only the compact decision maps, off q.mu.
func mergeAdmissionWaitCaptures(old, next *admissionWaitCapture) *admissionWaitCapture {
	if len(old.admissions) == 0 && len(old.decisions) == 0 {
		return next
	}
	copy := *next
	copy.admissions = make(map[string]map[string]AdmissionDecision, len(next.admissions))
	copy.decisions = make(map[string]map[string]admissionWaitDecision, len(next.decisions))
	for id, targets := range next.admissions {
		copy.admissions[id] = targets
	}
	for id, targets := range next.decisions {
		copy.decisions[id] = targets
	}
	same := func(id string) bool {
		a, b := readGet(old.index.root, "jobs/"+id), readGet(next.index.root, "jobs/"+id)
		if a == nil || b == nil {
			return false
		}
		x, xe := a.decode("jobs/" + id)
		y, ye := b.decode("jobs/" + id)
		return xe == nil && ye == nil && sameAdmissionWaitEpisode(x.(*Job), y.(*Job))
	}
	for id, targets := range old.admissions {
		if !same(id) {
			continue
		}
		merged := make(map[string]AdmissionDecision, len(targets))
		for key, d := range targets {
			merged[key] = d
		}
		for key, d := range next.admissions[id] {
			merged[key] = d
		}
		copy.admissions[id] = merged
	}
	for id, targets := range old.decisions {
		if !same(id) {
			continue
		}
		merged := make(map[string]admissionWaitDecision, len(targets))
		for key, d := range targets {
			merged[key] = d
		}
		for key, d := range next.decisions[id] {
			merged[key] = d
		}
		copy.decisions[id] = merged
	}
	return &copy
}

// Optional evidence construction runs after the scheduling commit unlocks.
// Publish only distinct transitions, fenced against each job's lifecycle.
// Telemetry may advance during a build: its snapshot timestamp describes that
// bounded lag. Never apply an old observation to a newer attempt or wait episode.
func (q *Queue) finishAdmissionWait(c *admissionWaitCapture) {
	if c.index == nil || q.stopping.Load() {
		return
	}
	if sameAdmissionWaitInputs(q.admissionWaitLast.Load(), c) {
		return
	}
	source, err := c.index.capture.observationQueue(context.Background(), "", c.index.capture.source.clock.Now())
	if err != nil {
		return
	}
	jobs := make(map[string]*Job, len(source.s.Jobs))
	latest := q.publishedReads.Load()
	for id, j := range source.s.Jobs {
		copy := *j
		// The pending snapshot may predate the preceding builder's evidence
		// commit. Carry its semantic endpoints and real decisions forward,
		// without taking any newer scheduling inputs from the live queue.
		if latest != nil {
			if record := readGet(latest.root, "jobs/"+id); record != nil {
				if value, err := record.decode("jobs/" + id); err == nil {
					current := value.(*Job)
					if sameAdmissionWaitEpisode(j, current) {
						copy.AdmissionWait = current.AdmissionWait
					}
				}
			}
		}
		jobs[id] = &copy
	}
	source.s.Jobs, source.records = jobs, nil
	source.evidenceBytes, source.holdClock = c.bytes, c.clock
	source.admissionWaitDecisions = c.decisions
	source.admissionTrace = &admissionTrace{jobs: c.admissions}
	source.memoryPlanning = newMemoryPlanningCache()
	refresh := func(at time.Time) {
		if at.After(source.clock.Now()) && (c.refresh.IsZero() || at.Before(c.refresh)) {
			c.refresh = at
		}
	}
	for _, j := range jobs {
		if j.PreemptionCount >= 2 && j.StartedAt != nil {
			refresh(j.StartedAt.Add(source.preemptionGrace()))
		}
		if j.State == "stopping" {
			refresh(source.batchCreditUntil(j))
		}
	}
	for _, slot := range source.s.ModelSlots {
		if slot.PreemptionCount >= 2 && slot.RunSince != nil {
			refresh(slot.RunSince.Add(source.preemptionGrace()))
		}
	}
	for _, samples := range source.hostMemorySamples {
		for _, sample := range samples {
			refresh(sample.at.Add(sample.freshFor()))
		}
	}
	source.buildAdmissionWaits(source.placementScan(), c.service)
	changes := map[string]*AdmissionWaitEvidence{}
	for id, j := range jobs {
		if j.State != "queued" || j.AdmissionWait == nil {
			continue
		}
		original := readGet(c.index.root, "jobs/"+id)
		if original == nil {
			continue
		}
		value, _ := original.decode("jobs/" + id)
		old := value.(*Job).AdmissionWait
		next := j.AdmissionWait
		if old == nil || old.Latest == nil || next.Latest == nil || old.CaptureGaps != next.CaptureGaps ||
			old.BudgetUnknown != next.BudgetUnknown || old.Dropped != next.Dropped ||
			old.DeferredAt != nil || !sameAdmissionWait(old.Latest, next.Latest) {
			changes[id] = next
		}
	}
	if len(changes) == 0 {
		q.admissionWaitLast.Store(c)
		return
	}
	// This ordinary small local-state transaction performs no scheduling or
	// projection. Recheck under q.mu before installing any off-lock result.
	if err := q.commitAdmissionWait(c, changes); err != nil {
		log.Print("event=admission_wait_capture_failed phase=persistence")
	}
}

func sameAdmissionWaitEpisode(a, b *Job) bool {
	return a.State == "queued" && b.State == "queued" && a.Attempt == b.Attempt &&
		a.PauseRevision == b.PauseRevision && a.Queued.Equal(b.Queued) &&
		((a.FanOut == nil && b.FanOut == nil) ||
			(a.FanOut != nil && b.FanOut != nil && a.FanOut.Next == b.FanOut.Next))
}

func (q *Queue) commitAdmissionWait(c *admissionWaitCapture, changes map[string]*AdmissionWaitEvidence) error {
	// Decode immutable episode identities before taking the writer mutex.
	originals := make(map[string]*Job, len(changes))
	for id := range changes {
		if record := readGet(c.index.root, "jobs/"+id); record != nil {
			value, err := record.decode("jobs/" + id)
			if err != nil {
				return err
			}
			originals[id] = value.(*Job)
		}
	}
	complete := true
	exact := false
	return q.updateTransactionMode(context.Background(), func() error {
		exact = q.publishedReads.Load() == c.index
		for id, next := range changes {
			j := q.s.Jobs[id]
			old := originals[id]
			if j == nil || old == nil {
				complete = false
				continue
			}
			if !sameAdmissionWaitEpisode(old, j) {
				complete = false
				evidence := AdmissionWaitEvidence{}
				if j.AdmissionWait != nil {
					evidence = *j.AdmissionWait
				}
				evidence.CaptureGaps++
				now := q.clock.Now()
				evidence.DeferredAt = &now
				q.touchJob(id).AdmissionWait = &evidence
				continue
			}
			q.touchJob(id).AdmissionWait = next
			complete = complete && next.DeferredAt == nil
		}
		return nil
	}, nil, false, func() {
		if complete {
			last := *c
			// Keep the source generation when telemetry advanced during the
			// build, so input reuse cannot hide those unobserved changes.
			if exact {
				last.index = q.publishedReads.Load()
			}
			q.admissionWaitLast.Store(&last)
		}
	}, false)
}

// Diagnostic observations only. These values never authorize admission, stops,
// release, retry, or learning. A missing sample is unknown, not measured zero.
type AdmissionWaitEvidence struct {
	First              *AdmissionWaitSnapshot   `json:"first,omitempty"`
	Latest             *AdmissionWaitSnapshot   `json:"latest,omitempty"`
	Transitions        []*AdmissionWaitSnapshot `json:"transitions,omitempty"`
	Dropped            uint64                   `json:"dropped,omitempty"`
	HistoryBudgetBytes uint64                   `json:"history_budget_bytes"`
	HistoryBytes       uint64                   `json:"history_bytes"`
	CaptureGaps        uint64                   `json:"capture_gaps,omitempty"`
	DeferredAt         *time.Time               `json:"deferred_at,omitempty"`
	BudgetUnknown      bool                     `json:"budget_unknown,omitempty"`
}

type AdmissionWaitSnapshot struct {
	At           time.Time             `json:"at"`
	Attempt      int                   `json:"attempt"`
	NextPiece    int                   `json:"next_piece,omitempty"`
	Predicate    string                `json:"predicate"`
	Targets      []AdmissionWaitTarget `json:"targets,omitempty"`
	ClosedAt     *time.Time            `json:"closed_at,omitempty"`
	ClosedReason string                `json:"closed_reason,omitempty"`
}

type AdmissionWaitTarget struct {
	AdmissionDecision   *AdmissionDecision    `json:"admission_decision,omitempty"`
	Host                string                `json:"host"`
	Machine             string                `json:"machine"`
	Session             string                `json:"session"`
	Device              string                `json:"device,omitempty"`
	Predicates          []string              `json:"predicates,omitempty"`
	DeviceFault         string                `json:"device_fault,omitempty"`
	Resolution          string                `json:"resolution"`
	Concurrency         int                   `json:"concurrency,omitempty"`
	RequiredCores       int                   `json:"required_cores"`
	AvailableCores      int                   `json:"available_cores"`
	RequiredHostGB      float64               `json:"required_host_gb"`
	AvailableHostGB     float64               `json:"available_host_gb"`
	HostMemoryKnown     bool                  `json:"host_memory_known"`
	HostDebtGB          float64               `json:"host_debt_gb"`
	RequiredDeviceGB    float64               `json:"required_device_gb"`
	AvailableDeviceGB   float64               `json:"available_device_gb"`
	DeviceMemoryKnown   bool                  `json:"device_memory_known"`
	Preemption          WaitPreemption        `json:"preemption"`
	ReleaseCommitment   bool                  `json:"release_commitment,omitempty"`
	LastPlannerDecision *AdmissionWaitPlan    `json:"last_planner_decision,omitempty"`
	Holders             []AdmissionWaitHolder `json:"holders,omitempty"`
}

// A historical real decision stays identifiable while subsequent maintenance
// waits for its releases. Its timestamp and vector are independent of the
// current diagnostic projection, even when optional transition history is shed.
type AdmissionWaitPlan struct {
	At               time.Time      `json:"at"`
	Preemption       WaitPreemption `json:"preemption"`
	Concurrency      int            `json:"concurrency,omitempty"`
	RequiredCores    int            `json:"required_cores"`
	RequiredHostGB   float64        `json:"required_host_gb"`
	RequiredDeviceGB float64        `json:"required_device_gb"`
}

// Considered means the actual scheduler evaluated a release plan on this
// target. A later diagnostic projection must never manufacture that event.
type WaitPreemption struct {
	Source     string `json:"source"`
	Considered bool   `json:"considered"`
	Planned    bool   `json:"planned"`
	Reason     string `json:"reason"`
}

// Only makeBatchRoom writes these: the exact target-specific vector evaluated
// there must survive the capacity changes caused by applying its release plan.
type admissionWaitDecision struct {
	preemption  WaitPreemption
	needs       ResourceNeeds
	concurrency int
	at          time.Time
}

type AdmissionWaitHolder struct {
	Key               string         `json:"key"`
	Kind              string         `json:"kind"`
	Job               string         `json:"job,omitempty"`
	Machine           string         `json:"machine"`
	Session           string         `json:"session"`
	Attempt           int            `json:"attempt,omitempty"`
	EngineAttempt     string         `json:"engine_attempt,omitempty"`
	Devices           []string       `json:"devices,omitempty"`
	Tier              *PriorityMatch `json:"tier,omitempty"`
	Cores             int            `json:"cores"`
	DeclaredHostGB    *float64       `json:"declared_host_gb,omitempty"`
	AdmissionHostGB   *float64       `json:"admission_host_gb,omitempty"`
	ChargedHostGB     *float64       `json:"charged_host_gb,omitempty"`
	RemainingGrowthGB *float64       `json:"remaining_growth_gb,omitempty"`
	ForecastHostGB    *float64       `json:"forecast_host_gb,omitempty"`
	SampleAgeSeconds  *float64       `json:"sample_age_seconds,omitempty"`
	SampleFresh       bool           `json:"sample_fresh"`
	EnforcedHostGB    *float64       `json:"enforced_host_gb,omitempty"`
	EnforcementBasis  string         `json:"enforcement_basis"`
	Preemptible       bool           `json:"preemptible"`
	PreemptionReason  string         `json:"preemption_reason"`
}

func waitNumber(n float64) *float64 { return &n }

func (q *Queue) noteWaitPreemption(j *Job, m *Machine, decision WaitPreemption) {
	if j.Spec.Priority != "production" {
		return
	}
	if q.admissionWaitDecisions == nil {
		q.admissionWaitDecisions = map[string]map[string]admissionWaitDecision{}
	}
	if q.admissionWaitDecisions[j.ID] == nil {
		q.admissionWaitDecisions[j.ID] = map[string]admissionWaitDecision{}
	}
	decision.Source = "planner"
	concurrency := 0
	if j.Batch != nil {
		concurrency = j.Batch.Concurrency
	}
	q.admissionWaitDecisions[j.ID][targetKey(m)] = admissionWaitDecision{decision, j.Spec.Needs, concurrency, q.clock.Now()}
}

func (q *Queue) closeAdmissionWait(j *Job, reason string) {
	if j == nil || j.AdmissionWait == nil || j.AdmissionWait.Latest == nil {
		return
	}
	now := q.clock.Now()
	evidence, latest := *j.AdmissionWait, *j.AdmissionWait.Latest
	if latest.ClosedAt != nil {
		if j.State != "queued" {
			return
		}
		// Resume/retry may close again before background capture runs. Record
		// that closure without reusing the preceding episode's blockers.
		latest = AdmissionWaitSnapshot{At: now, Attempt: j.Attempt, Predicate: "admission"}
		if j.FanOut != nil {
			latest.NextPiece = j.FanOut.Next
		}
		evidence.CaptureGaps++
	}
	latest.ClosedAt, latest.ClosedReason = &now, reason
	evidence.Latest = &latest
	q.touchJob(j.ID).AdmissionWait = &evidence
}

// Transactions that finish without another maintain (cancel, failure, pause,
// assignment) close their touched records before persistence. Never scan cold
// retained history, and never mutate a detached status projection.
func (q *Queue) closeChangedAdmissionWaits() {
	for id := range q.records.jobs {
		if j := q.s.Jobs[id]; j != nil && j.State != "queued" {
			q.closeAdmissionWait(j, j.State)
		}
	}
}

func (q *Queue) waitHolder(o HostObligation, demand *Job, now time.Time) AdmissionWaitHolder {
	h := AdmissionWaitHolder{Key: obligationKey(o), Kind: "release", Machine: o.Result.Machine,
		Session: o.Result.Session, Job: o.Result.ID, Attempt: o.Result.Attempt,
		EnforcementBasis: "unknown", PreemptionReason: "unconfirmed_release"}
	tier := func(s Spec) { match := q.priorityMatch(s); h.Tier = &match }
	switch {
	case o.Engine != nil:
		e := o.Engine
		h.Kind, h.Machine, h.Session, h.EngineAttempt, h.Devices = "engine", e.Machine, e.Session, e.Attempt, []string{e.Device}
		if s := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]; s != nil && s.Session == e.Session && s.Attempt == e.Attempt {
			tier(q.slotSpec(s))
			h.Cores = 1
			model, _ := q.model(s.Model)
			need := hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
			h.DeclaredHostGB, h.AdmissionHostGB, h.ChargedHostGB = waitNumber(model.HostMemoryGB), waitNumber(need), waitNumber(need)
			h.Preemptible = q.slotReclaimableFor(s, demand.Spec)
			h.PreemptionReason = "engine_protected"
		}
	case o.Call != nil:
		c := o.Call
		h.Kind, h.Machine, h.Session, h.Job, h.EngineAttempt = "call", c.Machine, c.Session, c.ID, c.Attempt
		// RAM and engine cores belong to the engine row, not each call.
		if live := q.obligationCall(c); live != nil {
			tier(callJob(live).Spec)
			h.Preemptible = q.comparePriority(callJob(live).Spec, demand.Spec) > 0
			h.PreemptionReason = "equal_or_higher_tier"
		}
	case o.Launch != nil:
		l := o.Launch
		h.Kind, h.Machine, h.Session, h.EngineAttempt, h.Job, h.Cores = "preparation", l.Machine, l.Session, l.Attempt, l.Demand, l.CPUCores
		if l.Spec != nil {
			tier(*l.Spec)
			h.Preemptible = q.comparePriority(*l.Spec, demand.Spec) > 0
			if owner := q.s.Jobs[l.Demand]; owner != nil && q.preemptionProtected(owner.PreemptionCount, l.StartedAt) {
				h.Preemptible = false
			}
		}
		h.PreemptionReason = "preparation_protected"
	default:
		j := q.s.Jobs[o.Result.ID]
		if j != nil {
			tier(j.Spec)
		}
		if j == nil || j.Attempt != o.Result.Attempt || j.Machine != o.Result.Machine || j.Session != o.Result.Session || !batchReservesCapacity(j) {
			// A newer attempt cannot supply an old execution's charge or sample.
			for _, r := range q.s.HostReleases {
				if r.Job == o.Result.ID && r.Attempt == o.Result.Attempt && r.Machine == o.Result.Machine && r.Session == o.Result.Session {
					h.Cores = r.CPUCores
					h.ChargedHostGB = waitNumber(r.HostChargeGB)
					h.RemainingGrowthGB = waitNumber(r.HostChargeGB)
					h.Devices = slices.Clone(r.Devices)
				}
			}
			return h
		}
		h.Kind, h.Cores, h.Devices = "batch", j.Spec.Needs.cores(), slices.Clone(j.AllocatedDevices)
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		if owner == nil {
			return h
		}
		n := hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		sample := q.hostMemorySamples[j.Machine][jobKey(*j)]
		charge, growth := memoryCharge(j, owner, n, sample, now)
		h.AdmissionHostGB, h.ChargedHostGB, h.RemainingGrowthGB = waitNumber(n), waitNumber(charge), waitNumber(growth)
		if j.Spec.Needs.Memory != "auto" {
			h.DeclaredHostGB = waitNumber(j.Spec.Needs.HostMemoryGB)
		}
		if !sample.at.IsZero() && !now.Before(sample.at) {
			h.SampleAgeSeconds = waitNumber(now.Sub(sample.at).Seconds())
		}
		if forecast, fresh := sample.identifiedHostForecast(now); fresh {
			h.ForecastHostGB, h.SampleFresh = waitNumber(forecast), true
		}
		// This is the Started worker contract, not a direct memory.max read.
		// Never describe sampled RSS or unstarted work as a kernel cap.
		if j.StartedAt != nil && owner.MemoryEnforcement == "memory ceilings enforced (cgroup v2)" {
			cap := max(n, adaptiveMemoryCeilingGB(j, owner))
			h.EnforcedHostGB, h.EnforcementBasis = waitNumber(cap), "started_worker_contract_not_direct_read"
		}
		h.Preemptible = q.batchPreemptible(j, demand.Spec)
		switch {
		case q.comparePriority(j.Spec, demand.Spec) <= 0:
			h.PreemptionReason = "equal_or_higher_tier"
		case q.preemptionProtected(j.PreemptionCount, j.StartedAt):
			h.PreemptionReason = "minimum_runtime"
		case j.State == "stopping" && !now.Before(q.batchCreditUntil(j)):
			h.PreemptionReason = "release_credit_expired"
		default:
			h.PreemptionReason = "not_preemptible"
		}
	}
	if h.Preemptible {
		h.PreemptionReason = "preemptible"
	}
	return h
}

// Runs on a private committed generation, never under the live writer mutex.
// No status/read path records evidence, and no OS, network or storage I/O runs.
func (q *Queue) buildAdmissionWaits(scan *placementScan, service time.Duration) {
	now := q.clock.Now()
	started := holdNow(q.holdClock)
	waiting := []*Job{}
	history := uint64(0)
	for _, j := range q.liveJobs() {
		if j.Spec.Priority != "production" && j.AdmissionWait == nil && (q.admissionTrace == nil || len(q.admissionTrace.jobs[j.ID]) == 0) {
			continue
		}
		if j.State != "queued" {
			q.closeAdmissionWait(j, j.State)
			continue
		}
		if j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next >= len(j.Spec.FanOut.Pieces) {
			q.closeAdmissionWait(j, "wave_admitted")
			continue
		}
		waiting = append(waiting, j)
		if j.AdmissionWait != nil {
			history = addDiskBytes(history, j.AdmissionWait.HistoryBytes)
		}
	}
	// Oldest evidence first, so pressure deferral cannot starve a fixed tail.
	slices.SortFunc(waiting, func(a, b *Job) int {
		at, bt := time.Time{}, time.Time{}
		if a.AdmissionWait != nil && a.AdmissionWait.Latest != nil {
			at = a.AdmissionWait.Latest.At
		}
		if b.AdmissionWait != nil && b.AdmissionWait.Latest != nil {
			bt = b.AdmissionWait.Latest.At
		}
		if c := at.Compare(bt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	budget, unknown := uint64(0), true
	if report := q.evidenceRetention; report != nil && !report.Unavailable {
		// Existing optional history is already charged. Redistribute it together
		// with measured unused capacity, rather than oscillating as it fills.
		used := q.evidenceBytes - min(q.evidenceBytes, history)
		budget = (report.BudgetBytes - min(report.BudgetBytes, used)) / uint64(max(1, len(waiting)))
		unknown = false
	}
	obligations := map[string][]HostObligation{}
	candidates := map[string]map[string]*Job{}
	for _, j := range waiting {
		// One ordinary measured control transaction is the observational work
		// quantum. Never let optional capture monopolize a pressured control path.
		// This can defer a snapshot, never scheduling or a cleanup acknowledgement.
		if service > 0 && holdNow(q.holdClock).Sub(started) >= service {
			evidence := AdmissionWaitEvidence{}
			if j.AdmissionWait != nil {
				evidence = *j.AdmissionWait
			}
			evidence.CaptureGaps++
			evidence.DeferredAt = &now
			q.touchJob(j.ID).AdmissionWait = &evidence
			continue
		}
		snapshot := &AdmissionWaitSnapshot{At: now, Attempt: j.Attempt, Predicate: "admission"}
		if j.FanOut != nil {
			snapshot.NextPiece = j.FanOut.Next
		}
		if !dependencyReady(j) {
			snapshot.Predicate = "dependency"
		} else {
			for _, m := range scan.machines {
				if !q.jobAllowsMachine(j, m.ID) {
					continue
				}
				key := targetKey(m)
				if candidates[key] == nil {
					candidates[key] = map[string]*Job{}
					for _, c := range q.candidates(m, scan).jobs {
						candidates[key][c.ID] = c
					}
				}
				candidate := candidates[key][j.ID]
				resolution := "candidate_projection"
				if candidate == nil {
					resolution = "serial_diagnostic_projection"
					template := j
					if j.Spec.FanOut != nil {
						template = q.fanOutTemplate(j)
					}
					for _, c := range q.memoryCandidates(template, m) {
						if memorySizingTargetFits(c, m) {
							candidate = c
							break
						}
					}
				}
				if candidate == nil {
					continue
				}
				n := candidate.Spec.Needs
				concurrency := 0
				if candidate.Batch != nil {
					concurrency = candidate.Batch.Concurrency
				}
				decision, recorded := q.admissionWaitDecisions[j.ID][key]
				if recorded {
					n, concurrency = decision.needs, decision.concurrency
					copy := *candidate
					copy.Spec.Needs = n
					candidate = &copy
					resolution = "planner_candidate"
				}
				target := AdmissionWaitTarget{Host: hostID(m), Machine: m.ID, Session: m.Session, Device: m.Device, Resolution: resolution, Concurrency: concurrency,
					RequiredCores: n.cores(), AvailableCores: m.FreeCPUCores, RequiredHostGB: hostNeed(m, n.MemoryGB, n.HostMemoryGB), AvailableHostGB: m.FreeHostMemoryGB,
					HostDebtGB: m.hostReservationDebtGB, RequiredDeviceGB: n.MemoryGB, AvailableDeviceGB: m.FreeMemoryGB,
					HostMemoryKnown:   m.HostMemoryMeasured && m.HostMemoryError == "",
					DeviceMemoryKnown: m.MemoryMeasured && m.MemoryError == "",
					Preemption:        WaitPreemption{Source: "diagnostic_projection", Reason: "not_scheduler_turn"}}
				var fit targetFit
				fitsResourcesChecked(candidate, m, m.FreeMemoryGB, &fit)
				for _, p := range fit.failed {
					target.Predicates = append(target.Predicates, strings.ReplaceAll(strings.ToLower(p), " ", "_"))
				}
				if !hostFits(m, n.MemoryGB, n.HostMemoryGB) {
					target.Predicates = append(target.Predicates, "host_memory")
				}
				if !q.placementAllowed(candidate, m) {
					target.Predicates = append(target.Predicates, "placement_policy")
				}
				if q.unreported[m.ID] {
					target.Predicates = append(target.Predicates, "worker_unreported")
				}
				if q.networkQuiet {
					target.Predicates = append(target.Predicates, "network_quiet")
				}
				if q.capReasonFor(candidate, m) != "" {
					target.Predicates = append(target.Predicates, "project_capacity")
				}
				if !q.checkpointReady(candidate, m) {
					target.Predicates = append(target.Predicates, "checkpoint")
				}
				slices.Sort(target.Predicates)
				target.Predicates = slices.Compact(target.Predicates)
				if deviceFaultReason(m.MemoryError) {
					target.DeviceFault = m.MemoryError
				}
				if recorded {
					target.Preemption = decision.preemption
					target.LastPlannerDecision = &AdmissionWaitPlan{At: decision.at, Preemption: decision.preemption, Concurrency: concurrency, RequiredCores: target.RequiredCores, RequiredHostGB: target.RequiredHostGB, RequiredDeviceGB: target.RequiredDeviceGB}
				} else if len(target.Predicates) > 0 {
					target.Preemption.Reason = "target_ineligible"
				}
				if !recorded && j.AdmissionWait != nil && j.AdmissionWait.Latest != nil {
					old := j.AdmissionWait.Latest
					if old.Attempt == snapshot.Attempt && old.NextPiece == snapshot.NextPiece {
						for _, previous := range old.Targets {
							if previous.Machine == target.Machine && previous.Session == target.Session && previous.Device == target.Device {
								target.LastPlannerDecision = previous.LastPlannerDecision
								break
							}
						}
					}
				}
				if placement := j.PreemptionPlacement; placement != nil && placement.Machine == m.ID && placement.Session == m.Session && placement.Device == m.Device && len(placement.Releases) > 0 {
					target.ReleaseCommitment = q.preemptionReleasesPending(j)
				}
				if _, ok := obligations[hostID(m)]; !ok {
					obligations[hostID(m)] = q.hostObligations(hostID(m))
				}
				for _, o := range obligations[hostID(m)] {
					target.Holders = append(target.Holders, q.waitHolder(o, j, now))
				}
				snapshot.Targets = append(snapshot.Targets, target)
			}
			if len(snapshot.Targets) == 0 {
				snapshot.Predicate = "no_qualified_target"
			}
		}
		q.attachAdmissionDecisions(j, snapshot, scan)
		q.touchJob(j.ID).AdmissionWait = retainAdmissionWait(j.AdmissionWait, snapshot, budget, unknown)
	}
}

// First and latest are semantic endpoints, not a sample-count tuning knob.
// Optional distinct transitions share measured evidence headroom. Dropped
// coverage is explicit; diagnostics cannot silently claim complete history.
func retainAdmissionWait(old *AdmissionWaitEvidence, next *AdmissionWaitSnapshot, budget uint64, unknown bool) *AdmissionWaitEvidence {
	out := &AdmissionWaitEvidence{First: next, Latest: next, HistoryBudgetBytes: budget, BudgetUnknown: unknown}
	if old == nil {
		return out
	}
	if old.First != nil {
		out.First = old.First
	}
	out.Dropped, out.CaptureGaps = old.Dropped, old.CaptureGaps
	out.Transitions = slices.Clone(old.Transitions)
	out.HistoryBytes = old.HistoryBytes
	if old.Latest != nil && (old.First == nil || !old.Latest.At.Equal(old.First.At)) && !sameAdmissionWait(old.Latest, next) {
		data, _ := json.Marshal(old.Latest)
		if len(out.Transitions) == 0 {
			out.HistoryBytes = 2
		} else {
			out.HistoryBytes++
		}
		out.HistoryBytes += uint64(len(data))
		out.Transitions = append(out.Transitions, old.Latest)
	}
	first := 0
	for first < len(out.Transitions) && (unknown || out.HistoryBytes > budget) {
		data, _ := json.Marshal(out.Transitions[first])
		out.HistoryBytes -= min(out.HistoryBytes, uint64(len(data)+1))
		first++
		out.Dropped++
	}
	if first > 0 {
		out.Transitions = slices.Clone(out.Transitions[first:])
	}
	if len(out.Transitions) == 0 {
		out.Transitions = nil
		out.HistoryBytes = 0
	}
	return out
}

// Age and scalar telemetry refresh Latest without creating a new event. Compare
// typed identity/policy/need fields directly: no JSON shape building per holder.
func sameAdmissionWait(a, b *AdmissionWaitSnapshot) bool {
	if a.Attempt != b.Attempt || a.NextPiece != b.NextPiece || a.Predicate != b.Predicate || a.ClosedReason != b.ClosedReason || len(a.Targets) != len(b.Targets) {
		return false
	}
	for i, x := range a.Targets {
		y := b.Targets[i]
		if x.Host != y.Host || x.Machine != y.Machine || x.Session != y.Session || x.Device != y.Device || x.DeviceFault != y.DeviceFault || x.Resolution != y.Resolution || x.Concurrency != y.Concurrency || x.Preemption != y.Preemption || x.ReleaseCommitment != y.ReleaseCommitment || !sameAdmissionDecision(x.AdmissionDecision, y.AdmissionDecision) || x.RequiredCores != y.RequiredCores || x.RequiredHostGB != y.RequiredHostGB || x.RequiredDeviceGB != y.RequiredDeviceGB || !slices.Equal(x.Predicates, y.Predicates) || len(x.Holders) != len(y.Holders) {
			return false
		}
		if (x.LastPlannerDecision == nil) != (y.LastPlannerDecision == nil) {
			return false
		}
		if x.LastPlannerDecision != nil {
			a, b := *x.LastPlannerDecision, *y.LastPlannerDecision
			a.At, b.At = time.Time{}, time.Time{}
			if a != b {
				return false
			}
		}
		for k, h := range x.Holders {
			other := y.Holders[k]
			if h.Key != other.Key || h.SampleFresh != other.SampleFresh || h.EnforcementBasis != other.EnforcementBasis || h.Preemptible != other.Preemptible || h.PreemptionReason != other.PreemptionReason || (h.Tier == nil) != (other.Tier == nil) {
				return false
			}
			if h.Tier != nil && *h.Tier != *other.Tier {
				return false
			}
		}
	}
	return true
}
