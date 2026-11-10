package myce

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"
)

// AttemptFilesRequest identifies an exact worker copy, including legacy folders
// whose cleared actor no longer records the session. Attempt numbers are unique
// within a job; neither a job prefix nor a status-page omission is authority.
type AttemptFilesRequest struct {
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
	if parkedStorageCustody(j) {
		return keep("parked_storage_custody")
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
	if q.fileRetention == 0 || j.FinishedAt == nil {
		return keep("unique_unreleased")
	}
	until := j.FinishedAt.Add(q.fileRetention)
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
	report  AttemptFilesCollection
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
	err := errors.Join(pruneWorkerLogs(dir, "", now, w.LogRetention), pruneAttemptFiles(dir, now, c), pruneAttemptState(dir, now, w.LogRetention))
	log.Printf("worker scratch retention: retained_bytes=%d reclaimed_bytes=%d retained_bytes_by_reason=%v", c.report.RetainedBytes, c.report.ReclaimedBytes, c.report.Reasons)
	return err
}

func (c *attemptFilesCollector) disposition(job, name string) (AttemptFilesDisposition, error) {
	if c == nil {
		return AttemptFilesDisposition{Reason: "unique_unreleased"}, nil
	}
	attempt, err := strconv.Atoi(name)
	if err != nil || attempt < 1 || strconv.Itoa(attempt) != name {
		return AttemptFilesDisposition{Reason: "unknown_folder"}, nil
	}
	return c.resolve(job, attempt)
}
