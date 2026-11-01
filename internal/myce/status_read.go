package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const statusCacheLifetime = time.Second

func (c Client) inspectionSnapshot(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	err := c.Call(ctx, "GET", "/api/inspection", nil, &out)
	return out, err
}

// Only HTTP observations use this cache. Durable/control operations always read
// the queue itself. Encoded JSON is immutable; callers that need a struct decode
// a private copy, so even an HTML renderer cannot mutate the cached generation.
type statusCache struct {
	mu               sync.Mutex
	data             string
	expires          time.Time
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
	data, err := q.cachedStatusJSON(contexts...)
	if err != nil {
		return Snapshot{}, err
	}
	var out Snapshot
	err = json.Unmarshal([]byte(data), &out)
	return out, err
}

func (q *Queue) cachedStatusJSON(contexts ...context.Context) (string, error) {
	c := &q.statusCache
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		revision := c.terminalRevision.Load()
		if c.data != "" && c.generation == revision && q.clock.Now().Before(c.expires) {
			return c.data, nil
		}
		s, err := q.publicStatusSnapshot(&statusOptions{jobs: "recent", limit: defaultStatusJobs}, contexts...)
		if err != nil {
			return "", err
		}
		data, err := json.Marshal(s)
		if err != nil {
			return "", err
		}
		if c.terminalRevision.Load() != revision {
			if err := requestContext(contexts).Err(); err != nil {
				return "", err
			}
			continue
		}
		c.data, c.expires, c.generation = string(data), s.Now.Add(statusCacheLifetime), revision
		return c.data, nil
	}
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
	cpuLedger              bool
	hostBudgetGB           float64
	modelReservationDebtGB float64
	hostReservationDebtGB  float64
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
	unlockRequest := requestLock(requestContext(contexts), &q.mu)
	locked := readWallStart(q.observeRead)
	hold := q.statusHolds.start()
	var files fileStoreObservation
	v := &Queue{clock: observationClock{q.clock.Now()}, nasAvailable: q.nasAvailable,
		simulation: q.simulation, productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts,
		productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil:                    q.restartUntil, networkQuiet: q.networkQuiet, root: q.root}
	s := q.s
	s.Jobs, s.Submissions = map[string]*Job{}, map[string]*submissionRecord{}
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
	minimal := selected != nil && q.calls[id] == nil && (terminal(selected.State) || selected.State != "queued" && selected.StartedAt != nil)
	if err == nil {
		if minimal {
			copy := statusJobInputs(selected)
			copy.Spec.SubmissionKey = selected.Spec.SubmissionKey
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
						copy := statusJobInputs(j)
						if v.observationSelection != nil && !v.observationSelection.ids[key] {
							s.Jobs[key] = statusDependencyJob(j)
							continue
						}
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
	inputs := observationState{State: s, Models: q.models, Environments: q.environments, Calls: q.calls, Unreported: q.unreported}
	if minimal {
		inputs.Models, inputs.Environments, inputs.Calls, inputs.Unreported = ModelConfig{}, EnvironmentCatalog{}, nil, nil
	}
	machineLedgers := make(map[string]observationMachineLedger, len(s.Machines))
	for key, m := range s.Machines {
		machineLedgers[key] = observationMachineLedger{
			cpuLedger: m.cpuLedger, hostBudgetGB: m.hostBudgetGB,
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
		detached = copyObservation(inputs)
	}
	duration := queueHoldDuration(hold)
	unlockRequest()
	observeRead(q.observeRead, id, started, locked)
	logQueueHold(duration, "captureObservation", nil)
	if err != nil {
		return nil, files, err
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
	for key, job := range detached.State.Jobs {
		if original := custody.s.Jobs[key]; original != nil && (original.CancelledAt != nil || original.FanOut != nil) && (v.observationSelection == nil || v.observationSelection.ids[key]) {
			custody.projectCancellation(job, original)
		}
	}
	for key, m := range detached.State.Machines {
		ledger := machineLedgers[key]
		m.cpuLedger, m.hostBudgetGB = ledger.cpuLedger, ledger.hostBudgetGB
		m.modelReservationDebtGB, m.hostReservationDebtGB = ledger.modelReservationDebtGB, ledger.hostReservationDebtGB
	}
	for key := range gatherWait {
		detached.State.Jobs[key].simGatherWait = true
	}
	v.s, v.models, v.environments, v.calls, v.unreported = detached.State, detached.Models, detached.Environments, detached.Calls, detached.Unreported
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
	for {
		revision := q.statusCache.terminalRevision.Load()
		out, err := q.jobSnapshotGeneration(id, contexts...)
		if err != nil || q.statusCache.terminalRevision.Load() == revision {
			return out, err
		}
		if err := requestContext(contexts).Err(); err != nil {
			return nil, err
		}
	}
}

func (q *Queue) jobSnapshotGeneration(id string, contexts ...context.Context) (*Job, error) {
	v, _, err := q.captureObservation(id, contexts...)
	if err != nil {
		return nil, err
	}
	j, err := v.findObservationJob(id)
	if err != nil {
		return nil, err
	}
	var reasons map[string]string
	if j.State == "queued" {
		// A receipt explains this job; full fleet ordering belongs to status.
		reasons = map[string]string{j.ID: v.waitReason(j)}
	}
	if c := v.calls[j.ID]; c != nil {
		j = v.projectCall(c, nil, reasons)
	} else {
		j = v.projectJob(j, nil, reasons, true)
	}
	v.projectJobDiagnostics(j, true, true)
	j.DeadlineGuidance = v.deadlineGuidance(j)
	return j, nil
}

func (q *Queue) statusSnapshot(contexts ...context.Context) (Snapshot, error) {
	return q.statusSnapshotOptions(nil, contexts...)
}

func (q *Queue) statusSnapshotOptions(options *statusOptions, contexts ...context.Context) (Snapshot, error) {
	for {
		revision := q.statusCache.terminalRevision.Load()
		out, err := q.statusSnapshotGeneration(options, contexts...)
		if err != nil || q.statusCache.terminalRevision.Load() == revision {
			return out, err
		}
		if err := requestContext(contexts).Err(); err != nil {
			return Snapshot{}, err
		}
	}
}

func (q *Queue) statusSnapshotGeneration(options *statusOptions, contexts ...context.Context) (Snapshot, error) {
	v, files, err := q.captureStatusObservation("", options, contexts...)
	if err != nil {
		return Snapshot{}, err
	}
	out := v.projectSnapshot(true)
	out.RequestLatencies = q.requestLatencies.snapshot()
	out.Usage, err = v.readUsage(q.usageReader, out.Now.Add(-7*24*time.Hour))
	if err == nil {
		out.FileStore = files.status(context.Background())
	}
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
