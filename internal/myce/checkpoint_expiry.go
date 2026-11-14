package myce

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Settled checkpoint expiry (#894). The queue decides from its own terminal,
// delivery and release records which NAS checkpoint folders nothing can resume
// or still write; age alone never qualifies a folder. Consumers may still read
// a finished job's checkpoint for a while, so a settled folder also waits out
// the grace period after the job's recorded terminal time. Workers do the deletion
// off the queue lock and report each removal, which the queue records durably
// on the job. The queue performs no directory scan or NAS I/O for this.
const checkpointExpiryProtocol = "checkpoint-expiry-v1"

// DefaultCheckpointExpiryGrace keeps a settled job's checkpoint readable after
// it ends (maintainer-directed, #894). `myce queue --checkpoint-expiry-grace`.
const DefaultCheckpointExpiryGrace = 72 * time.Hour

// WithCheckpointExpiryGrace sets how long after a job's terminal time its
// settled checkpoint folder is kept. Zero keeps the default.
func WithCheckpointExpiryGrace(grace time.Duration) QueueOption {
	return func(q *Queue) {
		if grace > 0 {
			q.checkpointGrace = grace
		}
	}
}

type checkpointExpiryEntry struct {
	path string
	due  time.Time
}

// One worker holds at most this many folders, so a lost or wedged worker
// strands only these until its claim lapses or its session changes.
const checkpointExpiryBatch = 8

// Measured root creates and removes took up to 11 s each on macOS SMB (#894);
// a settled job folder with several attempts is tens of metadata operations.
// A claim outlives a slow batch rather than handing the same folder to a second
// worker. Its only cost is delay after a failed removal or a wedged worker.
const checkpointExpiryAllowance = 30 * time.Minute

// CheckpointExpiry asks a worker to remove one settled job's checkpoint folder.
// Path is relative to the worker's checkpoint root, slash-separated.
type CheckpointExpiry struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// CheckpointExpiryReport is a worker's outcome for one CheckpointExpiry.
type CheckpointExpiryReport struct {
	ID      string `json:"id"`
	Removed bool   `json:"removed,omitempty"`
	Error   string `json:"error,omitempty"`
}

type checkpointExpiryClaim struct {
	machine, session string
	until            time.Time
	failed           bool
}

// Only the job's own folder, flat or in its shard, can ever be named.
func validCheckpointExpiryPath(id, path string) bool {
	if id == "" || strings.ContainsAny(id, `/\:`) || strings.HasPrefix(id, ".") {
		return false
	}
	if path == id {
		return true
	}
	shard := checkpointShard(id)
	return shard != "" && path == shard+"/"+id
}

// checkpointExpirable reports the root-relative folder of a settled job.
// Terminal alone is not enough: undelivered outputs, unreleased attempts and
// parked storage custody keep the job mutable, and an unreleased attempt could
// still write its folder. Terminal jobs never resume (reconcileAttemptReport
// only recovers queued, running or stopping jobs), and retries, fan-out waves
// and resubmissions use new job IDs and folders.
func checkpointExpirable(root string, j *Job) (string, bool) {
	if j == nil || j.FileTransport && !j.Spec.localFirst() || j.CheckpointExpired != nil || j.Attempt == 0 || j.Checkpoint == "" || mutableJob(j) {
		return "", false
	}
	rel, err := filepath.Rel(root, j.Checkpoint)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	return rel, validCheckpointExpiryPath(j.ID, rel)
}

// checkpointEndTime is the time the grace period counts from. It is the
// recorded terminal time when present. Some historical terminal paths never
// stamped finished_at; for those it is the latest timestamp anywhere in the
// job's own record: attempt ends, stops and releases, cancellation and ledger
// events, then starts, assignment, waiting and submission. Taking the latest
// of all of them never places the end before any recorded activity. A record
// with no timestamp at all has no basis and is kept.
func checkpointEndTime(j *Job) (time.Time, bool) {
	if j.FinishedAt != nil {
		return *j.FinishedAt, true
	}
	var end time.Time
	at := func(t *time.Time) {
		if t != nil && t.After(end) {
			end = *t
		}
	}
	for i := range j.AttemptHistory {
		at(&j.AttemptHistory[i].EndedAt)
	}
	for i := range j.Copies {
		at(j.Copies[i].StoppedAt)
		at(j.Copies[i].ReleasedAt)
		at(j.Copies[i].StartedAt)
	}
	at(j.CancelledAt)
	if j.Cancellation != nil {
		at(j.Cancellation.AcknowledgedAt)
	}
	for i := range j.LedgerEntries {
		at(&j.LedgerEntries[i].At)
	}
	at(j.StartedAt)
	at(j.AssignedAt)
	at(&j.WaitingSince)
	at(&j.Queued)
	return end, !end.IsZero()
}

// CheckpointExpiryStatus counts settled checkpoint folders the queue still
// holds: Waiting have a basis and wait for their grace period or a worker;
// Undated have no timestamp at all in their record and are kept indefinitely.
type CheckpointExpiryStatus struct {
	Waiting int `json:"waiting"`
	Undated int `json:"undated"`
}

// indexCheckpointExpiry runs with the other record indexes at commit.
func (q *Queue) indexCheckpointExpiry(id string, j *Job) {
	if q.records.checkpointExpiry == nil {
		q.records.checkpointExpiry = map[string]checkpointExpiryEntry{}
		q.records.checkpointUndated = map[string]bool{}
	}
	delete(q.records.checkpointExpiry, id)
	delete(q.records.checkpointUndated, id)
	if rel, ok := checkpointExpirable(q.root, j); ok && !q.fileCheckpoints {
		if end, dated := checkpointEndTime(j); dated {
			q.records.checkpointExpiry[id] = checkpointExpiryEntry{path: rel, due: end.Add(q.expiryGrace())}
		} else {
			q.records.checkpointUndated[id] = true
		}
	}
	// Replaced, never mutated. Read captures freeze before commit indexes jobs,
	// so views share this atomic rather than copying a stale value.
	if q.checkpointExpiryStatus == nil {
		q.checkpointExpiryStatus = &atomic.Pointer[CheckpointExpiryStatus]{}
	}
	next := CheckpointExpiryStatus{Waiting: len(q.records.checkpointExpiry), Undated: len(q.records.checkpointUndated)}
	if old := q.checkpointExpiryStatus.Load(); old == nil || *old != next {
		q.checkpointExpiryStatus.Store(&next)
	}
}

func (q *Queue) checkpointExpiryCounts() *CheckpointExpiryStatus {
	if q.checkpointExpiryStatus == nil {
		return nil
	}
	return q.checkpointExpiryStatus.Load()
}

func (q *Queue) expiryGrace() time.Duration {
	if q.checkpointGrace > 0 {
		return q.checkpointGrace
	}
	return DefaultCheckpointExpiryGrace
}

// checkinCheckpointExpiry runs inside the check-in transaction under q.mu.
// It only reads in-memory records and mutates claims and touched jobs.
func (q *Queue) checkinCheckpointExpiry(c Checkin, m *Machine, reply *Reply) {
	now := q.clock.Now()
	if q.checkpointClaims == nil {
		q.checkpointClaims = map[string]checkpointExpiryClaim{}
	}
	for _, r := range c.CheckpointExpiryReports {
		if claim, ok := q.checkpointClaims[r.ID]; ok && claim.machine == m.ID {
			if r.Removed {
				delete(q.checkpointClaims, r.ID)
			} else {
				// Keep the claim until it lapses so a failing folder is not retried
				// on every heartbeat; the worker is free for another batch.
				claim.failed = true
				q.checkpointClaims[r.ID] = claim
			}
		}
		if !r.Removed {
			continue
		}
		// A removal is evidence only for a job that is still settled.
		if _, ok := checkpointExpirable(q.root, q.s.Jobs[r.ID]); ok {
			at := now
			q.touchJob(r.ID).CheckpointExpired = &at
		}
	}
	if c.CheckpointExpiryProtocol != checkpointExpiryProtocol || !q.shards.ready.Load() || q.records == nil || c.Recovering || m.Leaving || m.CheckpointUnavailable {
		return
	}
	busy := false
	for id, claim := range q.checkpointClaims {
		if _, pending := q.records.checkpointExpiry[id]; !now.Before(claim.until) || !pending {
			delete(q.checkpointClaims, id)
		} else if claim.machine == m.ID && claim.session == m.Session && !claim.failed {
			busy = true // Its current batch is still running.
		}
	}
	if busy {
		return
	}
	for id, entry := range q.records.checkpointExpiry {
		if len(reply.CheckpointExpiries) == checkpointExpiryBatch {
			break
		}
		if now.Before(entry.due) {
			continue
		}
		if claim, ok := q.checkpointClaims[id]; ok && (claim.failed || q.claimLive(claim)) {
			continue
		}
		q.checkpointClaims[id] = checkpointExpiryClaim{machine: m.ID, session: m.Session, until: now.Add(checkpointExpiryAllowance)}
		reply.CheckpointExpiries = append(reply.CheckpointExpiries, CheckpointExpiry{ID: id, Path: entry.path})
	}
}

// A restarted or departed worker cannot finish its old batch.
func (q *Queue) claimLive(claim checkpointExpiryClaim) bool {
	m := q.s.Machines[claim.machine]
	return m != nil && !m.Gone && m.Session == claim.session
}

type workerCheckpointExpiry struct {
	mu        sync.Mutex
	supported bool
	running   bool
	reports   []CheckpointExpiryReport // Finished, not yet acknowledged by a reply.
	remove    func(string) error
}

func newWorkerCheckpointExpiry() *workerCheckpointExpiry {
	return &workerCheckpointExpiry{remove: os.RemoveAll}
}

// checkpointExpiryCheckin returns the check-in and how many reports it carries.
// Strict predecessor queues refuse unknown fields, so nothing is sent until
// this queue advertises the protocol.
func (w Worker) checkpointExpiryCheckin(c Checkin) (Checkin, int) {
	e := w.checkpointExpiry
	if e == nil {
		return c, 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.supported {
		return c, 0
	}
	c.CheckpointExpiryProtocol = checkpointExpiryProtocol
	c.CheckpointExpiryReports = slices.Clone(e.reports)
	return c, len(e.reports)
}

func withoutCheckpointExpiryFields(c Checkin) Checkin {
	c.CheckpointExpiryProtocol, c.CheckpointExpiryReports = "", nil
	return c
}

// acceptCheckpointExpiryReply drops reports a reply acknowledged and records
// whether this queue accepts the protocol. Reports only ever append.
func (w Worker) acceptCheckpointExpiryReply(sent int, reply Reply) {
	e := w.checkpointExpiry
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reports = e.reports[min(sent, len(e.reports)):]
	e.supported = reply.CheckpointExpiryProtocol == checkpointExpiryProtocol
}

// expireCheckpoints starts one background batch; a batch already running
// ignores new tasks, which the queue reissues after their claims lapse.
func (w Worker) expireCheckpoints(tasks []CheckpointExpiry, root string) <-chan struct{} {
	e := w.checkpointExpiry
	if e == nil || len(tasks) == 0 || root == "" || w.fileCheckpoints {
		return nil
	}
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = true
	remove := e.remove
	e.mu.Unlock()
	tasks = slices.Clone(tasks)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, t := range tasks {
			r := expireCheckpoint(root, t, remove)
			e.mu.Lock()
			e.reports = append(e.reports, r)
			e.mu.Unlock()
		}
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()
	return done
}

func expireCheckpoint(root string, t CheckpointExpiry, remove func(string) error) CheckpointExpiryReport {
	r := CheckpointExpiryReport{ID: t.ID}
	if !validCheckpointExpiryPath(t.ID, t.Path) {
		r.Error = "invalid_path"
		return r
	}
	// The queue writes the marker on the share itself. An unmounted or wrong
	// root lacks it, so a folder this host cannot see is never reported removed.
	if _, err := os.Stat(filepath.Join(root, checkpointShardMarker)); err != nil {
		r.Error = "root_unverified"
		return r
	}
	path := filepath.Join(root, filepath.FromSlash(t.Path))
	// Never follow a symlinked shard or job folder out of the root.
	if parent := filepath.Dir(path); parent != root {
		if info, err := os.Lstat(parent); err != nil && !errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
			r.Error = "unexpected_parent"
			return r
		}
	}
	if info, err := os.Lstat(path); err == nil && !info.IsDir() {
		r.Error = "unexpected_entry"
		return r
	}
	if err := remove(path); err != nil {
		log.Printf("checkpoint expiry job=%s removal failed; the queue retries after its claim lapses", t.ID)
		r.Error = "remove_failed"
		return r
	}
	r.Removed = true
	return r
}
