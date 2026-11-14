package myce

import (
	"context"
	"errors"
	"log"
	"net/http"
	"slices"
	"strconv"
	"time"
)

// AttemptFilesRequest identifies an exact worker copy, including legacy folders
// whose cleared actor no longer records the session. Attempt numbers are unique
// within a job; neither a job prefix nor a status-page omission is authority.
type AttemptFilesRequest struct {
	Checkpoint bool `json:"checkpoint,omitempty"`
	Execution
	Machine string `json:"machine"`
}

type AttemptFilesDisposition struct {
	Reclaim bool       `json:"reclaim"`
	Reason  string     `json:"reason"`
	Until   *time.Time `json:"until,omitempty"`
}

func (a API) registerAttemptFiles(mux *requestMux) {
	mux.HandleFunc("POST /api/workers/attempt-files", func(w http.ResponseWriter, r *http.Request) {
		var request AttemptFilesRequest
		if !decode(w, r, &request) {
			return
		}
		if request.ID == "" || request.Attempt < 1 || request.Machine == "" {
			respondOperation(w, nil, errors.New("exact job, attempt and machine required"), "attempt_files_invalid")
			return
		}
		q := a.Queue
		unlock := requestLock(r.Context(), &q.mu)
		disposition := q.attemptFilesDisposition(request)
		unlock()
		respondOperation(w, disposition, nil, "attempt_files_invalid")
	})
	// One retention pass asks about all of its folders at once. Each answer
	// takes q.mu for one metadata lookup, exactly as the single-folder route
	// does, and releases it before the next, so a long list never holds the
	// mutex for the whole batch. Answers are read-only; the worker still waits
	// out its own marker before removing anything.
	mux.HandleFunc("POST /api/workers/attempt-files/batch", func(w http.ResponseWriter, r *http.Request) {
		var request AttemptFilesBatchRequest
		if !decode(w, r, &request) {
			return
		}
		if request.Machine == "" || slices.ContainsFunc(request.Attempts, func(e Execution) bool { return e.ID == "" || e.Attempt < 1 }) {
			respondOperation(w, nil, errors.New("exact jobs, attempts and machine required"), "attempt_files_invalid")
			return
		}
		q := a.Queue
		reply := AttemptFilesBatchReply{Dispositions: make([]AttemptFilesDisposition, 0, len(request.Attempts))}
		for _, e := range request.Attempts {
			if err := r.Context().Err(); err != nil {
				respondOperation(w, nil, err, "attempt_files_invalid")
				return
			}
			unlock := requestLock(r.Context(), &q.mu)
			reply.Dispositions = append(reply.Dispositions, q.attemptFilesDisposition(AttemptFilesRequest{Execution: Execution{ID: e.ID, Attempt: e.Attempt}, Machine: request.Machine}))
			unlock()
		}
		respondOperation(w, reply, nil, "attempt_files_invalid")
	})
}

// AttemptFilesBatchRequest asks about every listed attempt folder of one worker
// in one request. Dispositions come back in request order.
type AttemptFilesBatchRequest struct {
	Machine  string      `json:"machine"`
	Attempts []Execution `json:"attempts"`
}

type AttemptFilesBatchReply struct {
	Dispositions []AttemptFilesDisposition `json:"dispositions"`
}

// q.mu held; metadata only. A positive file policy is the existing operator's
// expiry authority. Zero and unknown history never grant deletion by age.
func (q *Queue) attemptFilesDisposition(r AttemptFilesRequest) AttemptFilesDisposition {
	keep := func(reason string) AttemptFilesDisposition { return AttemptFilesDisposition{Reason: reason} }
	j := q.retainedJob(r.ID)
	if j == nil {
		return keep("unknown_job")
	}
	released := false
	if j.Attempt == r.Attempt && j.Machine == r.Machine {
		released = j.ReleaseConfirmed
	}
	for _, copy := range j.Copies {
		if copy.Attempt == r.Attempt && copy.Machine == r.Machine {
			released = copy.ReleaseConfirmed || released
		}
	}
	if !released {
		return keep("unresolved_custody")
	}
	if r.Checkpoint {
		if !j.Spec.localFirst() || !terminal(j.State) || q.jobReleasePending(j) || checkpointMirrorsPending(j) || j.FinishedAt == nil {
			return keep("checkpoint_custody")
		}
		until := j.FinishedAt.Add(q.expiryGrace())
		return AttemptFilesDisposition{Reclaim: !q.clock.Now().Before(until), Reason: "checkpoint_retention", Until: &until}
	}
	if parkedStorageCustody(j) {
		return keep("parked_storage_custody")
	}
	// A released attempt the job has moved past (interrupted, yielded, preempted,
	// requeued) can never be accepted: results, delivery and partial-output
	// recovery belong to the current attempt. Checkpoints live in the checkpoint
	// root or queue file store, and diagnostics and pending uploads keep local
	// custody until the queue has them, so its scratch is disposable now rather
	// than after the terminal file-retention window. Repeated requeues otherwise
	// pile one scratch copy per attempt onto the worker disk.
	if r.Attempt < j.Attempt {
		for _, copy := range j.Copies {
			if copy.Attempt == r.Attempt && copy.Machine == r.Machine && copy.Recoverable {
				return keep("recoverable_attempt")
			}
		}
		return AttemptFilesDisposition{Reclaim: true, Reason: "superseded_attempt"}
	}
	if j.Attempt == r.Attempt && j.Machine == r.Machine && j.State == "succeeded" {
		return AttemptFilesDisposition{Reclaim: true, Reason: "accepted_result"}
	}
	if !terminal(j.State) || q.jobReleasePending(j) {
		return keep("unresolved_custody")
	}
	if deliveryPending(j) {
		return keep("pending_delivery")
	}
	if j.FilesExpired {
		return AttemptFilesDisposition{Reclaim: true, Reason: "files_released"}
	}
	retention := q.jobFileRetention(j)
	if retention == 0 || j.FinishedAt == nil {
		return keep("unique_unreleased")
	}
	until := j.FinishedAt.Add(retention)
	return AttemptFilesDisposition{Reclaim: !q.clock.Now().Before(until), Reason: "file_retention", Until: &until}
}

// Allocated bytes are measured off the queue mutex and heartbeat path. As with
// other cache reports, hardlinks can overstate the space a deletion will free.
type AttemptFilesCollection struct {
	RetainedBytes  int64            `json:"retained_bytes"`
	ReclaimedBytes int64            `json:"reclaimed_bytes"`
	Reasons        map[string]int64 `json:"retained_bytes_by_reason"`
}

type attemptFilesCollector struct {
	ctx     context.Context
	resolve func(string, int) (AttemptFilesDisposition, error)
	// batch answers a whole pass at once; prefetch fills known from it so
	// disposition needs no request per folder.
	batch  func([]Execution) ([]AttemptFilesDisposition, error)
	known  map[attemptFolder]AttemptFilesDisposition
	report AttemptFilesCollection
}

type attemptFolder struct {
	job     string
	attempt int
}

// prefetch asks the queue once about every folder that needs its authority.
// On failure every queried folder stays, reported once as queue_unavailable.
func (c *attemptFilesCollector) prefetch(queries []Execution) error {
	if c == nil || c.batch == nil || len(queries) == 0 {
		return nil
	}
	c.known = make(map[attemptFolder]AttemptFilesDisposition, len(queries))
	answers, err := c.batch(queries)
	if err == nil && len(answers) != len(queries) {
		err = errors.New("attempt files batch reply does not match the request")
	}
	for i, e := range queries {
		d := AttemptFilesDisposition{Reason: "queue_unavailable"}
		if err == nil {
			d = answers[i]
		}
		c.known[attemptFolder{e.ID, e.Attempt}] = d
	}
	return err
}

func (c *attemptFilesCollector) record(path, reason string) (int64, error) {
	if c == nil {
		return 0, nil
	}
	bytes, err := collectionAllocatedBytes(c.ctx, path)
	c.report.RetainedBytes += bytes
	c.report.Reasons[reason] += bytes
	return bytes, err
}

func (w Worker) retainWorkerFiles(ctx context.Context, dir string, now time.Time) error {
	c := &attemptFilesCollector{ctx: ctx, report: AttemptFilesCollection{Reasons: map[string]int64{}}}
	c.resolve = func(id string, attempt int) (AttemptFilesDisposition, error) {
		var d AttemptFilesDisposition
		err := w.Client.Call(ctx, "POST", "/api/workers/attempt-files", AttemptFilesRequest{Execution: Execution{ID: id, Attempt: attempt}, Machine: w.Machine.ID}, &d)
		return d, err
	}
	c.batch = func(attempts []Execution) ([]AttemptFilesDisposition, error) {
		var reply AttemptFilesBatchReply
		err := w.Client.Call(ctx, "POST", "/api/workers/attempt-files/batch", AttemptFilesBatchRequest{Machine: w.Machine.ID, Attempts: attempts}, &reply)
		if !previousReleaseRoute(err) {
			return reply.Dispositions, err
		}
		// The previous release (MY-42) has only the single-folder route.
		out := make([]AttemptFilesDisposition, len(attempts))
		for i, e := range attempts {
			if out[i], err = c.resolve(e.ID, e.Attempt); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	err := errors.Join(pruneWorkerLogs(dir, "", now, w.LogRetention), pruneAttemptFiles(dir, now, c), pruneAttemptState(dir, now, w.LogRetention))
	// Same confined collector, separate authority: checkpoint folders retain the
	// terminal 72-hour grace and never inherit early scratch/result disposal.
	c.batch, c.known = nil, nil
	c.resolve = func(id string, attempt int) (AttemptFilesDisposition, error) {
		var d AttemptFilesDisposition
		err := w.Client.Call(ctx, "POST", "/api/workers/attempt-files", AttemptFilesRequest{Execution: Execution{ID: id, Attempt: attempt}, Machine: w.Machine.ID, Checkpoint: true}, &d)
		return d, err
	}
	err = errors.Join(err, pruneAttemptFilesAt(dir, "local-checkpoints", now, c))
	w.hygieneCollector.recordScratch(c.report)
	log.Printf("worker scratch retention: retained_bytes=%d reclaimed_bytes=%d retained_bytes_by_reason=%v", c.report.RetainedBytes, c.report.ReclaimedBytes, c.report.Reasons)
	return err
}

func (c *attemptFilesCollector) disposition(job, name string) (AttemptFilesDisposition, error) {
	if c == nil {
		return AttemptFilesDisposition{Reason: "unique_unreleased"}, nil
	}
	attempt, ok := attemptFolderNumber(name)
	if !ok {
		return AttemptFilesDisposition{Reason: "unknown_folder"}, nil
	}
	if d, ok := c.known[attemptFolder{job, attempt}]; ok {
		return d, nil
	}
	if c.resolve == nil {
		return AttemptFilesDisposition{Reason: "queue_unavailable"}, nil
	}
	return c.resolve(job, attempt)
}

// attemptFolderNumber parses an exact attempt folder name.
func attemptFolderNumber(name string) (int, bool) {
	attempt, err := strconv.Atoi(name)
	return attempt, err == nil && attempt >= 1 && strconv.Itoa(attempt) == name
}

// previousReleaseRoute reports the plain 404 a previous-release queue returns
// for a worker route it does not have. Typed 404s are real answers.
func previousReleaseRoute(err error) bool {
	var refusal *HTTPError
	return errors.As(err, &refusal) && refusal.Status == http.StatusNotFound && refusal.Code == ""
}
