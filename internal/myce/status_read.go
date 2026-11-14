package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type jobSnapshotCache struct {
	hitCost       time.Duration
	mu            sync.Mutex // Completed response metadata only; never held during projection.
	refresh       sync.Mutex // Never acquired by a queue writer or a cache hit.
	data          string     // Immutable public JSON; each caller decodes its own copy.
	captured      time.Time  // When the committed inputs behind data were published.
	expires       time.Time
	dependsOnJobs bool // jobSnapshotsMu; queued guidance and descendant custody use other jobs.
}

// q.mu is held. Defer cache removal until the matching immutable capture is
// published, so a reader cannot refill a freshly invalidated entry from the
// previous generation in the gap between durable commit and publication.
func (q *Queue) invalidateJobSnapshot(id string) {
	if q.readInvalidations == nil {
		q.readInvalidations = map[string]bool{}
	}
	q.readInvalidations[id] = true
}

// Calls are ephemeral rather than persisted job records. Match their existing
// transaction rollback boundary, and invalidate only committed changes.
func (q *Queue) invalidateCallSnapshots(old map[string]*ModelCall) {
	changed := false
	for id, before := range old {
		if !reflect.DeepEqual(before, q.calls[id]) {
			q.invalidateJobSnapshot(id)
			changed = true
		}
	}
	for id := range q.calls {
		if old[id] == nil {
			q.invalidateJobSnapshot(id)
			changed = true
		}
	}
	if changed {
		q.invalidateDependentSnapshots()
	}
}

// q.mu is held. Both calls and batch jobs contribute to project caps and
// pending receipts; unchanged transactions leave their cached views intact.
func (q *Queue) invalidateDependentSnapshots() {
	q.readDependenciesInvalid = true
}

func (c Client) inspectionSnapshot(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	err := c.Call(ctx, "GET", "/api/inspection", nil, &out)
	return out, err
}

// Only HTTP observations use this cache. Durable/control operations always read
// the queue itself. Encoded JSON is immutable; callers that need a struct decode
// a private copy, so even an HTML renderer cannot mutate the cached generation.
type statusCache struct {
	hitCost          time.Duration
	mu               sync.Mutex
	refresh          sync.Mutex
	data             string
	compact          string // the default CompactStatus encoding of the same capture
	expires          time.Time
	capturedAt       time.Time
	generation       uint64
	terminalRevision atomic.Uint64
}

// Called only after a successful durable transaction, before discarding its
// undo records. Do not take the cache mutex here: refreshes acquire q.mu.
// Ordinary check-ins keep the one-second cache useful; terminal commits and
// their release acknowledgements must invalidate even an in-flight refresh.
func (q *Queue) invalidateTerminalStatus() {
	for id, old := range q.records.jobs {
		j := q.s.Jobs[id]
		if j != nil && terminal(j.State) && (old == nil || old.State != j.State || old.ReleaseConfirmed != j.ReleaseConfirmed) {
			q.statusCache.terminalRevision.Add(1)
			return
		}
	}
}

func (q *Queue) cachedPublicSnapshot(contexts ...context.Context) (Snapshot, error) {
	ctx, finishBudget, budgetErr := q.requestBudgets.projectionContext(requestContext(contexts))
	if budgetErr != nil {
		return Snapshot{}, budgetErr
	}
	defer finishBudget()
	contexts = []context.Context{ctx}
	data, err := q.cachedStatusJSON(contexts...)
	if err != nil {
		return Snapshot{}, err
	}
	allocation := projectionAllocation(ctx)
	cost, err := jsonAllocationCost([]byte(data), reflect.TypeFor[*Snapshot](), allocation.limit-allocation.held)
	if err != nil || !allocation.charge(cost) {
		return Snapshot{}, projectionRefusal(allocation.limit - allocation.held)
	}
	var out Snapshot
	err = json.Unmarshal([]byte(data), &out)
	return out, err
}

func (q *Queue) cachedStatusJSON(contexts ...context.Context) (string, error) {
	return q.cachedStatus(false, contexts...)
}

// cachedCompactStatusJSON serves the default GET /api/status: the CompactStatus
// projection of the same cached capture as view=snapshot.
func (q *Queue) cachedCompactStatusJSON(contexts ...context.Context) (string, error) {
	return q.cachedStatus(true, contexts...)
}

func (q *Queue) cachedStatus(compact bool, contexts ...context.Context) (string, error) {
	ctx, finishBudget, budgetErr := q.requestBudgets.projectionContext(requestContext(contexts))
	if budgetErr != nil {
		return "", budgetErr
	}
	defer finishBudget()
	c := &q.statusCache
	// The shared default contains all current work and no retained history
	// page. Response allocation/transport budgets still apply to its actual size.
	// An entry is valid for the terminal fence of the publication it was built
	// from; a later terminal commit publishes a new fence and misses.
	cached := func() (string, bool, error) {
		revision := q.statusRevision(ctx)
		c.mu.Lock()
		data, captured := c.data, c.capturedAt
		if compact {
			data = c.compact
		}
		hit := data != "" && c.generation == revision && q.clock.Now().Before(c.expires)
		c.mu.Unlock()
		if !hit {
			return "", false, nil
		}
		allocation := projectionAllocation(ctx)
		if int64(len(data)) > (allocation.limit-allocation.held)/6 || !allocation.charge(6*int64(len(data))) {
			return "", true, projectionRefusal(allocation.limit - allocation.held)
		}
		started := q.clock.Now()
		out, err := q.statusJSONFreshness(data, captured, true)
		c.mu.Lock()
		c.hitCost = q.clock.Now().Sub(started)
		c.mu.Unlock()
		return out, true, err
	}
	if data, hit, err := cached(); hit || err != nil {
		return data, err
	}
	// A build from the committed publication owns no live queue work, so
	// control pressure has nothing to protect from it. Only the pre-publication
	// fallback, which captures under q.mu, uses projection admission.
	if q.publishedReads.Load() == nil {
		if err := admitObservationProjection(ctx); err != nil {
			return "", err
		}
	}
	unlock, err := requestObservationLock(ctx, &c.refresh)
	if err != nil {
		return "", err
	}
	defer unlock()
	if data, hit, err := cached(); hit || err != nil {
		return data, err
	}
	allocation := projectionAllocation(ctx)
	mark := allocation.markProjection()
	var s Snapshot
	var revision uint64
	for {
		index := q.publishedReads.Load()
		pinned := pinRead(ctx, index)
		revision = q.statusRevision(pinned)
		s, err = q.publicStatusSnapshot(&statusOptions{jobs: "active"}, pinned)
		if err != nil {
			return "", err
		}
		// A terminal commit published during the projection must not be
		// answered or cached with the older generation.
		if q.statusRevision(ctx) == revision {
			break
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		allocation.rewindProjection(mark)
	}
	captured := s.Now
	if s.Freshness != nil {
		captured = s.Freshness.CapturedAt
	}
	s.Freshness = nil // Age/served time belong to each response, not the cache.
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	view, err := json.Marshal(NewCompactStatus(s))
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.data, c.compact, c.expires, c.generation = string(data), string(view), q.clock.Now().Add(q.observationCacheBudget.lease(q.clock.Now().Sub(s.Now), c.hitCost)), revision
	c.capturedAt = captured
	c.mu.Unlock()
	if compact {
		return q.statusJSONFreshness(string(view), captured, false)
	}
	return q.statusJSONFreshness(string(data), captured, false)
}

func (q *Queue) snapshotFreshness(captured time.Time, cached bool) *SnapshotFreshness {
	now := q.clock.Now()
	return &SnapshotFreshness{CapturedAt: captured, ServedAt: now,
		AgeSeconds: max(0, now.Sub(captured).Seconds()), Cached: cached}
}

func (q *Queue) statusJSONFreshness(data string, captured time.Time, cached bool) (string, error) {
	freshness, err := json.Marshal(q.snapshotFreshness(captured, cached))
	if err != nil {
		return "", err
	}
	// Splice one additive field into immutable JSON, without decoding/projecting
	// the cached generation or retaining an old age as if it were current.
	return data[:len(data)-1] + `,"freshness":` + string(freshness) + `}`, nil
}

type observationClock struct{ at time.Time }

func (c observationClock) Now() time.Time { return c.at }

type observationState struct {
	State        state
	Models       ModelConfig
	Environments EnvironmentCatalog
	Calls        map[string]*ModelCall
	Unreported   map[string]bool
}

// JSON does not carry the private scalar ledger used by diagnostic planning.
// Capture it by value alongside the public inputs, then restore only into the
// detached machines. Probe facts and runtime preparation/secrets are worker-only.
type observationMachineLedger struct {
	healthDelivery         *hostHealthDelivery
	toolInventory          *toolInventory
	cpuLedger              bool
	modelReservationDebtGB float64
	hostReservationDebtGB  float64
}

// A committed cold job changes only through the record journal. Keep its
// payload-free observation inputs detached at that boundary, rather than
// deep-copying retained executions under q.mu for every wide read. Each read
// copies these immutable values into its own generation after unlocking.
type statusHistoryEntry struct {
	source             *Job
	inputs, dependency *Job
}

func (q *Queue) indexStatusHistory(id string, j *Job) {
	if q.records.statusHistory == nil {
		q.records.statusHistory = map[string]statusHistoryEntry{}
	}
	inputs := statusJobInputs(j)
	inputs.Spec.SubmissionKey = j.Spec.SubmissionKey
	q.records.statusHistory[id] = statusHistoryEntry{
		source:     j,
		inputs:     copyObservationValue(reflect.ValueOf(&inputs)).Interface().(*Job),
		dependency: copyObservationValue(reflect.ValueOf(statusDependencyJob(j))).Interface().(*Job),
	}
}

// Copy while protected, then encode and project outside q.mu. No mutable
// state escapes the capture. Cold history needs only its public summary; active
// work retains the inputs needed by the existing diagnostic planner.
func (q *Queue) captureObservation(id string, contexts ...context.Context) (*Queue, fileStoreObservation, error) {
	return q.captureStatusObservation(id, nil, contexts...)
}

func (q *Queue) captureStatusObservation(id string, options *statusOptions, contexts ...context.Context) (*Queue, fileStoreObservation, error) {
	started := readWallStart(q.observeRead)
	// Serialize just the memory copy. Parallel read-locked captures can all
	// consume CPU while a waiting heartbeat writer must wait for every one.
	// Projection/encoding below remain concurrent outside the queue lock.
	unlockRequest := func() {}
	var lockErr error
	if !q.detachedRead {
		unlockRequest, lockErr = requestObservationLock(requestContext(contexts), &q.mu)
	}
	if lockErr != nil {
		return nil, fileStoreObservation{}, lockErr
	}
	locked := readWallStart(q.observeRead)
	var hold time.Time
	if !q.detachedRead {
		hold = q.statusHolds.start()
	}
	var files fileStoreObservation
	v := &Queue{clock: q.detachedClock(q.clock.Now()), nasAvailable: q.nasAvailable,
		evidenceRetention: q.evidenceRetention, checkpointExpiryStatus: q.checkpointExpiryStatus, simulation: q.simulation, productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts,
		productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil:                    q.restartUntil, networkQuiet: q.networkQuiet, root: q.root}
	q.freezeContactView(v)

	s := q.s
	if id == "" {
		v.efficiencyView = q.efficiencyView
	} else {
		// Job-only reads never return fleet diagnostics or their baselines.
		s.EfficiencyHosts, s.EfficiencyHealth, s.StrandedDemand = nil, nil, nil
	}
	healthEvents, externalMemory := s.Health.Events, maps.Clone(s.ExternalMemory)
	s.Health.Events, s.ExternalMemory = nil, nil
	s.Jobs, s.Submissions = map[string]*Job{}, map[string]*submissionRecord{}
	frozen := map[string]*Job{}
	var selected *Job
	var err error
	if options != nil {
		v.observationSelection, err = q.selectStatusJobs(*options)
	}
	if id != "" {
		// Resolve against committed queue state while holding q.mu, never the
		// aggregate status cache. Keep the selected job in this same capture.
		selected, err = q.findObservationJob(id)
		if err == nil {
			id = selected.ID
		}
	}
	// Once a batch execution has started, only policy and host identities are
	// needed for its eligibility summary. Queued/startup work and calls still
	// need the active diagnostic context, but never the fleet projection.
	minimal := selected != nil && q.calls[id] == nil && (terminal(selected.State) || selected.State != "queued" && selected.StartedAt != nil && !jobHasVariants(selected))
	if !minimal {
		// Shared-device diagnostics use the same fresh attempt evidence as
		// admission. Detach both map levels while the samples are protected;
		// their values contain only scalars, strings and timestamps.
		v.hostMemorySamples = make(map[string]map[attemptKey]hostMemorySample, len(q.hostMemorySamples))
		for machine, samples := range q.hostMemorySamples {
			v.hostMemorySamples[machine] = maps.Clone(samples)
		}
	}
	if err == nil {
		if minimal {
			copy := statusJobInputs(selected)
			copy.Spec.SubmissionKey = selected.Spec.SubmissionKey
			copy.Resources = jobResources(selected) // Before redaction drops disk and inputs.
			s = state{PriorityPolicy: q.s.PriorityPolicy, Jobs: map[string]*Job{id: &copy}}
			if !terminal(selected.State) {
				s.Policies = map[string]ProjectPolicy{selected.Spec.Project: q.s.Policies[selected.Spec.Project]}
				s.Machines = make(map[string]*Machine, len(q.s.Machines))
				for key, m := range q.s.Machines {
					s.Machines[key] = &Machine{ID: m.ID, HostID: m.HostID}
				}
			}
		} else {
			for key, j := range q.s.Jobs {
				if id == "" || mutableJob(j) || key == id {
					if !mutableJob(j) {
						if id == "" && q.records != nil {
							if entry, ok := q.records.statusHistory[key]; ok && entry.source == j {
								input := entry.inputs
								if v.observationSelection != nil && !v.observationSelection.ids[key] {
									input = entry.dependency
								}
								s.Jobs[key], frozen[key] = input, input
								continue
							}
						}
						if v.observationSelection != nil && !v.observationSelection.ids[key] {
							s.Jobs[key] = statusDependencyJob(j)
							continue
						}
						copy := statusJobInputs(j)
						copy.Spec.SubmissionKey = j.Spec.SubmissionKey
						s.Jobs[key] = &copy
					} else {
						s.Jobs[key] = j
					}
				}
			}
			for key, r := range q.s.Submissions {
				if r.Job != nil && q.s.Jobs[r.JobID] == nil && mutableJob(r.Job) {
					s.Submissions[key] = r
				}
			}
		}
		if id == "" {
			files = q.captureFileStoreStatus()
			// One allocation-free pass over every retained job; cheaper under
			// q.mu than copying each envelope into the detached view (#585).
			admissions := q.hostAdmissions()
			v.statusAdmissions = &admissions
		} else {
			// Receipt diagnostics do not need global usage/event history or
			// upload and enrollment bookkeeping.
			s.Preemptions, s.ProductionStarts, s.JobInputUploads, s.BundleUploads = nil, nil, nil, nil
			s.EnrollmentRelease, s.EnrollmentReleaseKey = nil, nil
		}
	}
	// Preserve declaration/custody inputs before receipt minimization. Build
	// the descendant index and project cancellation only after unlocking.
	cancellations := map[string]cancellationObservationInput{}
	cancellationSubmissions := map[string]cancellationSubmissionInput{}
	fullCancellation := false
	for key := range s.Jobs {
		if original := q.s.Jobs[key]; original != nil && (original.CancelledAt != nil || original.FanOut != nil) && (v.observationSelection == nil || v.observationSelection.ids[key]) {
			cancellations[key] = cancellationInputs(original)
			fullCancellation = fullCancellation || original.FanOut != nil
		}
	}
	if fullCancellation {
		for key, j := range q.s.Jobs {
			cancellations[key] = cancellationInputs(j)
		}
		for key, r := range q.s.Submissions {
			if r.Job != nil && q.s.Jobs[r.JobID] == nil {
				cancellationSubmissions[key] = cancellationSubmissionInput{Kind: r.Kind, JobID: r.JobID, Job: cancellationInputs(r.Job)}
			}
		}
	}
	for key, j := range s.Jobs {
		if len(j.LedgerEntries) > 0 {
			copy := *j
			copy.LedgerEntries = nil
			s.Jobs[key] = &copy
		}
	}
	for key, r := range s.Submissions {
		if r.Job != nil && len(r.Job.LedgerEntries) > 0 {
			copy := *r
			job := *r.Job
			job.LedgerEntries = nil
			copy.Job = &job
			s.Submissions[key] = &copy
		}
	}
	// These diagnostic generations are replaced, never mutated in place.
	// Their retained-fleet-sized copies do not belong under the queue mutex.
	efficiencyHealth, strandedDemand := s.EfficiencyHealth, s.StrandedDemand
	s.EfficiencyHealth, s.StrandedDemand = nil, nil
	inputs := observationState{State: s, Models: q.models, Environments: q.environments, Calls: q.calls, Unreported: q.unreported}
	if minimal {
		inputs.Models, inputs.Environments, inputs.Calls, inputs.Unreported = ModelConfig{}, EnvironmentCatalog{}, nil, nil
	}
	machineLedgers := make(map[string]observationMachineLedger, len(s.Machines))
	for key, m := range s.Machines {
		machineLedgers[key] = observationMachineLedger{healthDelivery: q.freezeHostHealthDelivery(m),
			toolInventory: m.toolInventory, cpuLedger: m.cpuLedger,
			modelReservationDebtGB: m.modelReservationDebtGB, hostReservationDebtGB: m.hostReservationDebtGB,
		}
	}
	// Simulator dependency barriers also affect the shared diagnostic planner.
	var gatherWait map[string]bool
	for key, j := range s.Jobs {
		if j.simGatherWait {
			if gatherWait == nil {
				gatherWait = make(map[string]bool)
			}
			gatherWait[key] = true
		}
	}
	cancellationReleases := maps.Clone(q.s.HostReleases)
	var detached observationState
	if err == nil {
		for key := range frozen {
			delete(inputs.State.Jobs, key)
		}
		err = chargeProjectionValue(requestContext(contexts), inputs)
		if err == nil {
			detached = copyObservation(inputs)
		}
	}
	duration := queueHoldDuration(hold)
	unlockRequest()
	if efficiencyHealth != nil {
		detached.State.EfficiencyHealth = copyObservationValue(reflect.ValueOf(efficiencyHealth)).Interface().(map[string]efficiencyBaseline)
	}
	if strandedDemand != nil {
		detached.State.StrandedDemand = copyObservationValue(reflect.ValueOf(strandedDemand)).Interface().([]StrandedDemand)
	}
	if v.efficiencyView != nil {
		v.efficiencyView = copyObservationValue(reflect.ValueOf(v.efficiencyView)).Interface().(*EfficiencyReport)
	}
	observeRead(q.observeRead, id, started, locked)
	if !q.detachedRead {
		logQueueHold(duration, "captureObservation", nil)
	}
	if err == nil {
		err = requestContext(contexts).Err()
	}
	if err != nil {
		return nil, files, err
	}
	detached.State.Health.Events = append([]HealthAlert(nil), healthEvents...)
	detached.State.ExternalMemory = make(map[string]*externalMemoryRing, len(externalMemory))
	for key, ring := range externalMemory {
		detached.State.ExternalMemory[key] = ring
	}
	for key, input := range frozen {
		// History outside the returned page contributes only its immutable
		// dependency summary (order and references); nothing projects or
		// mutates it. Share it rather than copying and charging all retained
		// work to every paged read, which refused pages with
		// projection_capacity_exceeded once history outgrew a request's share.
		if options != nil && options.stream || v.observationSelection != nil && !v.observationSelection.ids[key] {
			detached.State.Jobs[key] = input
			continue
		}
		if err = chargeProjectionValue(requestContext(contexts), input); err != nil {
			return nil, files, err
		}
		detached.State.Jobs[key] = copyObservationValue(reflect.ValueOf(input)).Interface().(*Job)
	}
	custody := &Queue{s: state{Jobs: map[string]*Job{}, Submissions: map[string]*submissionRecord{}, HostReleases: cancellationReleases}}
	for key, input := range cancellations {
		custody.s.Jobs[key] = input.job()
	}
	for key, input := range cancellationSubmissions {
		custody.s.Submissions[key] = &submissionRecord{Kind: input.Kind, JobID: input.JobID, Job: input.Job.job()}
	}
	if fullCancellation {
		custody = custody.cancellationObservation()
	}
	if options != nil && options.stream {
		v.observationSelection.custody = custody
	}
	for key, job := range detached.State.Jobs {
		if options != nil && options.stream {
			continue // Project cancellation into the private row, never borrowed history.
		}
		if original := custody.s.Jobs[key]; original != nil && (original.CancelledAt != nil || original.FanOut != nil) && (v.observationSelection == nil || v.observationSelection.ids[key]) {
			custody.projectCancellation(job, original)
		}
	}
	for key, m := range detached.State.Machines {
		ledger := machineLedgers[key]
		m.toolInventory = ledger.toolInventory
		if m.HostHealth != nil {
			m.HostHealth.delivery = ledger.healthDelivery
		}
		m.cpuLedger = ledger.cpuLedger
		m.modelReservationDebtGB, m.hostReservationDebtGB = ledger.modelReservationDebtGB, ledger.hostReservationDebtGB
	}
	for key := range gatherWait {
		detached.State.Jobs[key].simGatherWait = true
	}
	v.s, v.models, v.environments, v.calls, v.unreported = detached.State, detached.Models, detached.Environments, detached.Calls, detached.Unreported
	// Diagnostics need the same active/custody set as writer-side planning.
	// Keep full retained history in s.Jobs for evidence and public history rows;
	// scanning it as live work constructs a device view for each cold execution
	// on every exploratory-memory target comparison.
	v.observationLive = map[string]*Job{}
	for key, job := range v.s.Jobs {
		if mutableJob(job) {
			v.observationLive[key] = job
		}
	}
	return v, files, nil
}

// Exact IDs take the map lookup path. Prefix resolution examines only IDs and
// borrows pointers solely while the caller holds q.mu.
func (q *Queue) findObservationJob(id string) (*Job, error) {
	if j := q.s.Jobs[id]; j != nil {
		return j, nil
	}
	if c := q.calls[id]; c != nil {
		return callJob(c), nil
	}
	var matches []*Job
	if len(id) >= 8 {
		for key, j := range q.s.Jobs {
			if strings.HasPrefix(key, id) {
				matches = append(matches, j)
			}
		}
		for key, c := range q.calls {
			if strings.HasPrefix(key, id) {
				matches = append(matches, callJob(c))
			}
		}
	}
	return FindJob(matches, id)
}

func (q *Queue) jobSnapshot(id string, contexts ...context.Context) (*Job, error) {
	j, _, _, err := q.jobSnapshotReceipt(id, contexts...)
	return j, err
}

// jobSnapshotRead also reports the freshness of the committed inputs it read.
func (q *Queue) jobSnapshotRead(id string, contexts ...context.Context) (*Job, *SnapshotFreshness, error) {
	j, captured, hit, err := q.jobSnapshotReceipt(id, contexts...)
	if err != nil || captured.IsZero() {
		return j, nil, err
	}
	return j, q.snapshotFreshness(captured, hit), nil
}

func (q *Queue) jobSnapshotReceipt(id string, contexts ...context.Context) (*Job, time.Time, bool, error) {
	ctx := requestContext(contexts)
	resolved, err := q.resolveReadJobID(ctx, id)
	if err != nil {
		return nil, time.Time{}, false, err
	}
	id = resolved
	allocation := projectionAllocation(ctx)
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, false, err
	}
	q.jobSnapshotsMu.Lock()
	if q.jobSnapshots == nil {
		q.jobSnapshots = map[string]*jobSnapshotCache{}
	}
	entry := q.jobSnapshots[id]
	if entry == nil {
		entry = &jobSnapshotCache{dependsOnJobs: true}
		q.jobSnapshots[id] = entry
	}
	q.jobSnapshotsMu.Unlock()
	entry.mu.Lock()
	data, captured, hit := entry.data, entry.captured, entry.data != "" && q.clock.Now().Before(entry.expires)
	entry.mu.Unlock()
	if !hit {
		// Published job captures own no live queue work. Physical allocation
		// accounting still bounds both cold projection and response decoding.
		if published := q.publishedReads.Load(); published == nil || published.capture == nil {
			if err := admitObservationProjection(ctx); err != nil {
				return nil, time.Time{}, false, err
			}
		}
		unlock, lockErr := requestObservationLock(ctx, &entry.refresh)
		if lockErr != nil {
			return nil, time.Time{}, false, lockErr
		}
		entry.mu.Lock()
		data, captured, hit = entry.data, entry.captured, entry.data != "" && q.clock.Now().Before(entry.expires)
		entry.mu.Unlock()
		if !hit {
			now := q.clock.Now()
			out, at, captureErr := q.jobSnapshotCapture(id, contexts...)
			var encoded []byte
			if captureErr == nil {
				encoded, captureErr = json.Marshal(out)
			}
			q.jobSnapshotsMu.Lock()
			valid := q.jobSnapshots[id] == entry
			if captureErr == nil {
				data, captured = string(encoded), at
			}
			if valid && captureErr == nil {
				entry.dependsOnJobs = out.State == "queued" || out.FanOut != nil || out.CancelledAt != nil
				entry.mu.Lock()
				data = string(encoded)
				entry.data, entry.captured, entry.expires = data, captured, q.clock.Now().Add(q.observationCacheBudget.lease(q.clock.Now().Sub(now), entry.hitCost))
				entry.mu.Unlock()
			}
			q.jobSnapshotsMu.Unlock()
			unlock()
			if captureErr != nil {
				return nil, time.Time{}, false, captureErr
			}
		} else {
			unlock()
		}
	}
	if allocation != nil {
		// Charge only this response's decode/encoding, never another
		// request's completed projection or resident source generation.
		if int64(len(data)) > (allocation.limit-allocation.held)/6 || !allocation.charge(6*int64(len(data))) {
			return nil, time.Time{}, false, projectionRefusal(allocation.limit - allocation.held)
		}
		cost, costErr := jsonAllocationCost([]byte(data), reflect.TypeFor[*Job](), allocation.limit-allocation.held)
		if costErr != nil || !allocation.charge(cost) {
			return nil, time.Time{}, false, projectionRefusal(allocation.limit - allocation.held)
		}
	}
	started := q.clock.Now()
	var out Job
	err = json.Unmarshal([]byte(data), &out)
	// This response belongs to its captured committed generation. A later
	// commit invalidates future cache lookups, never restarts this request.
	if hit {
		entry.mu.Lock()
		entry.hitCost = q.clock.Now().Sub(started)
		entry.mu.Unlock()
	}
	return &out, captured, hit, err
}

func (q *Queue) jobSnapshotGeneration(id string, contexts ...context.Context) (*Job, error) {
	j, _, err := q.jobSnapshotCapture(id, contexts...)
	return j, err
}

// jobSnapshotCapture also reports when its committed inputs were published.
func (q *Queue) jobSnapshotCapture(id string, contexts ...context.Context) (*Job, time.Time, error) {
	v, _, err := q.captureReadObservation(id, nil, contexts...)
	if err != nil {
		return nil, time.Time{}, err
	}
	j, err := v.findObservationJob(id)
	if err != nil {
		return nil, time.Time{}, err
	}
	var reasons map[string]string
	var wait *Hold
	if j.State == "queued" {
		// The detached generation's observations are immutable. Reuse one
		// index across waiting, eligibility and deadline diagnostics rather
		// than copying every curve again for each target comparison.
		v.memoryPlanning = newMemoryPlanningCache()
		v.memoryObservationIndex = v.memoryObservationCatalog()
		v.observationScan = v.placementScan()
		v.observationScan.observationOwner = v
		v.observationScan.memory = v.memoryPlanning
		// A receipt explains this job; full fleet ordering belongs to status.
		wait = v.waitHold(j)
		reasons = map[string]string{j.ID: wait.Detail}
	}
	if c := v.calls[j.ID]; c != nil {
		j = v.projectCall(c, nil, reasons)
	} else {
		j = v.projectJob(j, nil, reasons, true)
		j.wait = wait
	}
	if j.Resources == nil {
		j.Resources = jobResources(j)
	}
	v.projectJobDiagnostics(j, true, true)
	j.DeadlineGuidance = v.deadlineGuidance(j)
	return j, readCommitted(v.clock), nil
}

func (q *Queue) statusSnapshot(contexts ...context.Context) (Snapshot, error) {
	return q.statusSnapshotOptions(nil, contexts...)
}

func (q *Queue) statusSnapshotOptions(options *statusOptions, contexts ...context.Context) (Snapshot, error) {
	ctx, finishBudget, budgetErr := q.requestBudgets.projectionContext(requestContext(contexts))
	if budgetErr != nil {
		return Snapshot{}, budgetErr
	}
	defer finishBudget()
	allocation := projectionAllocation(ctx)
	mark := allocation.markProjection()
	if _, pinned := ctx.Value(pinnedReadKey{}).(*publishedReadIndex); pinned {
		return q.statusSnapshotGeneration(options, ctx)
	}
	for {
		index := q.publishedReads.Load()
		revision := q.statusRevision(pinRead(ctx, index))
		out, err := q.statusSnapshotGeneration(options, pinRead(ctx, index))
		// Rebuild only when a newer publication carries a terminal commit
		// this response missed. Between a terminal fence and its publication
		// the commit is still in flight; the older generation answers.
		if err != nil || q.statusRevision(ctx) == revision {
			return out, err
		}
		if err := requestContext(contexts).Err(); err != nil {
			return Snapshot{}, err
		}
		// A terminal fence discarded this response. Its temporary copies are
		// no longer in flight; do not accumulate them as simultaneous work.
		allocation.rewindProjection(mark)
	}
}

func (q *Queue) statusSnapshotGeneration(options *statusOptions, contexts ...context.Context) (Snapshot, error) {
	ctx, finishBudget, budgetErr := q.requestBudgets.projectionContext(requestContext(contexts))
	if budgetErr != nil {
		return Snapshot{}, budgetErr
	}
	defer finishBudget()
	contexts = []context.Context{ctx}
	if options.compact() {
		v, summary, delivery, err := q.compactStatusObservation(options, ctx)
		if err != nil {
			return Snapshot{}, err
		}
		out := v.projectSnapshot(true)
		out.Machines = dropReplacedStatusMachines(out.Machines)
		out.Summary, out.HealthAlertDelivery = summary, delivery
		out.Freshness = q.snapshotFreshness(readCommitted(v.clock), false)
		return out, ctx.Err()
	}
	if options != nil {
		owned := *options
		limit, err := q.requestBudgets.pageLimit(requestContext(contexts), reflect.TypeFor[Job](), owned.limit)
		if err != nil {
			return Snapshot{}, err
		}
		if owned.limit == 0 {
			owned.limit = limit
		} else {
			owned.limit = min(owned.limit, limit)
		}
		options = &owned
	}
	v, files, err := q.captureReadObservation("", options, contexts...)
	if err != nil {
		return Snapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	// One detached generation is one immutable planning pass. Without this,
	// each queued job/target diagnostic rebuilds the retained-evidence catalog.
	v.memoryPlanning = newMemoryPlanningCache()
	v.observationScan = v.placementScan()
	v.observationScan.observationOwner = v
	v.observationScan.memory = v.memoryPlanning
	out := v.projectSnapshot(true, &q.requestBudgets)
	out.RequestLatencies = q.requestLatencies.snapshot()
	if options.includes("usage") {
		out.Usage, err = v.readUsage(q.usageReader, out.Now.Add(-7*24*time.Hour), ctx)
	}
	trimStatusSections(&out, options)
	if options != nil && !options.full {
		out.Machines = dropReplacedStatusMachines(out.Machines) // History only.
	}
	if err == nil {
		if options.includes("storage") {
			out.FileStore = files.status(ctx)
		}
		err = ctx.Err()
	}
	out.Freshness = q.snapshotFreshness(readCommitted(v.clock), false)
	return out, err
}

// A separate read-only WAL connection gives usage its own SQLite snapshot and
// never borrows the queue's single writer connection. Only observations use it.
func openUsageReader(path string) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	u.RawQuery = "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
