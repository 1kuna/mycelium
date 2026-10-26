package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
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
	mu      sync.Mutex
	data    string
	expires time.Time
}

func (q *Queue) cachedPublicSnapshot() (Snapshot, error) {
	data, err := q.cachedStatusJSON()
	if err != nil {
		return Snapshot{}, err
	}
	var out Snapshot
	err = json.Unmarshal([]byte(data), &out)
	return out, err
}

func (q *Queue) cachedStatusJSON() (string, error) {
	c := &q.statusCache
	c.mu.Lock()
	if c.data == "" || !q.clock.Now().Before(c.expires) {
		s, err := q.publicSnapshot()
		if err != nil {
			c.mu.Unlock()
			return "", err
		}
		data, err := json.Marshal(s)
		if err != nil {
			c.mu.Unlock()
			return "", err
		}
		c.data, c.expires = string(data), s.Now.Add(statusCacheLifetime)
	}
	data := c.data
	c.mu.Unlock()
	return data, nil
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

// Encode while protected, then decode and project outside q.mu. No mutable
// state escapes the capture. Cold history needs only its public summary; active
// work retains the inputs needed by the existing diagnostic planner.
func (q *Queue) captureObservation(id string) (*Queue, fileStoreObservation, error) {
	started := readWallStart(q.observeRead)
	q.mu.RLock()
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
	if id != "" {
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
			s.Preemptions, s.ProductionStarts, s.JobInputUploads = nil, nil, nil
			s.EnrollmentRelease, s.EnrollmentReleaseKey = nil, nil
		}
	}
	inputs := observationState{State: s, Models: q.models, Environments: q.environments, Calls: q.calls, Unreported: q.unreported}
	if minimal {
		inputs.Models, inputs.Environments, inputs.Calls, inputs.Unreported = ModelConfig{}, EnvironmentCatalog{}, nil, nil
	}
	var data []byte
	if err == nil {
		data, err = json.Marshal(inputs)
	}
	duration := queueHoldDuration(hold)
	q.mu.RUnlock()
	observeRead(q.observeRead, id, started, locked)
	logQueueHold(duration, "captureObservation", nil)
	if err != nil {
		return nil, files, err
	}
	var detached observationState
	if err = json.Unmarshal(data, &detached); err != nil {
		return nil, files, err
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

func (q *Queue) jobSnapshot(id string) (*Job, error) {
	v, _, err := q.captureObservation(id)
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
	return j, nil
}

func (q *Queue) statusSnapshot() (Snapshot, error) {
	v, files, err := q.captureObservation("")
	if err != nil {
		return Snapshot{}, err
	}
	out := v.projectSnapshot(true)
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
