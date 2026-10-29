package myce

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const DefaultLogRetention = 7 * 24 * time.Hour

// ConfigureLogRetention bounds completed batch output. Active work and pending
// worker replay are protected; retention is logical deletion, not secure erasure.
func (q *Queue) ConfigureLogRetention(age time.Duration) error {
	if age <= 0 {
		return errors.New("log-retention must be positive")
	}
	q.mu.Lock()
	q.logRetention = age
	if q.records != nil {
		q.records.logsDone = map[string]bool{}
	}
	if q.records != nil {
		for id, j := range q.s.Jobs {
			q.indexJob(id, j)
		}
	}
	q.mu.Unlock()
	return q.pruneBatchLogs()
}

// Snapshot a bounded wave under q.mu; all log-table I/O runs outside it.
// Terminal attempts cannot append more logs (AppendLog requires current work).
func (q *Queue) pruneBatchLogs(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	if q.simulation {
		requestUnlock1()
		return nil
	}
	if q.records == nil {
		q.rebuildRecords()
	}
	now := q.clock.Now()
	ids := q.records.logs.due(now)
	// Keep retry ownership even if a delete fails or another Tick runs.
	for _, id := range ids {
		q.records.logs.set(id, now.Add(time.Minute))
	}
	requestUnlock1()
	var result error
	for _, id := range ids {
		complete := true
		for _, table := range []string{"logs", "log_offsets", "environment_log_offsets"} {
			r, err := q.db.Exec("DELETE FROM "+table+" WHERE rowid IN (SELECT rowid FROM "+table+" WHERE job=? LIMIT 512)", id)
			if err != nil {
				result = errors.Join(result, err)
				complete = false
				break
			}
			n, err := r.RowsAffected()
			if err != nil {
				result = errors.Join(result, err)
				complete = false
				break
			}
			if n == 512 {
				complete = false
			}
		}
		if complete {
			requestUnlock2 := requestLock(requestCtx, &q.mu)
			q.records.logs.set(id, time.Time{})
			if j := q.s.Jobs[id]; j != nil {
				q.records.logsDone[id] = true
				q.indexJob(id, j)
			}
			requestUnlock2()
		}
	}
	return result
}

func pruneWorkerLogs(dir, pending string, now time.Time, age time.Duration) error {
	protected := map[string]bool{pending: true}
	paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
	if err != nil {
		return err
	}
	paths = append(paths, filepath.Join(dir, "legacy-state.json"))
	for _, path := range paths {
		data, e := os.ReadFile(path)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		var st workerState
		if e = decodeWorkerState(data, &st); e != nil {
			return e
		}
		if st.Job != nil {
			protected[st.Log] = true
		}
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return err
	}
	for _, path := range files {
		name := filepath.Base(path)
		if protected[name] {
			continue
		}
		identity := strings.TrimSuffix(strings.TrimPrefix(name, "engine-"), ".log")
		if len(identity) != 32 {
			continue
		}
		if _, err := hex.DecodeString(identity); err != nil {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.ModTime().After(now.Add(-age)) {
			continue
		}
		if strings.HasPrefix(name, "engine-") {
			data, err := os.ReadFile(strings.TrimSuffix(path, ".log") + ".json")
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err == nil {
				var st workerState
				if err := decodeWorkerJSON(data, &st); err != nil {
					return err
				}
				if st.PID != 0 || st.LaunchPending {
					continue
				}
			}
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

// AttemptFilesGrace keeps an unclaimed attempt folder for diagnosis; failed
// and cancelled runs' work files may not be uploaded. It matches stopped-container retention.
const AttemptFilesGrace = 24 * time.Hour

// unclaimedMarker records, by its modification time, when retention first saw
// an attempt folder without custody.
const unclaimedMarker = ".myce-unclaimed"

// A completion POST can acknowledge release without accepting outputs (for
// example when cancellation races success). Confirm the exact terminal attempt
// before authorizing early scratch deletion; failed diagnostics keep their grace.
func (w Worker) publishedAttemptFiles(ctx context.Context, st workerState) (*Execution, error) {
	if st.Job == nil || st.Result == nil || st.Result.ReleasePending || st.Result.ReleaseOnly {
		return nil, nil
	}
	j, r := st.Job, st.Result
	if !j.Spec.extensionJob() && len(jobInputFiles(j))+len(jobInputPaths(j))+len(j.Spec.SharedInputs) == 0 {
		return nil, nil
	}
	var accepted Job
	if err := w.Client.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &accepted); err != nil {
		return nil, err
	}
	if accepted.ID != r.ID || accepted.Attempt != r.Attempt || accepted.Session != r.Session || accepted.Machine != r.Machine || !accepted.ReleaseConfirmed {
		return nil, nil
	}
	if accepted.State == "succeeded" && r.Success && !r.Interrupted && maps.Equal(accepted.OutputRefs, r.OutputRefs) {
		execution := r.Execution
		return &execution, nil
	}
	return nil, nil
}

// workerRetention is one pass of the worker's periodic local retention.
func workerRetention(dir string, now time.Time, logAge time.Duration) error {
	return errors.Join(pruneWorkerLogs(dir, "", now, logAge), pruneAttemptFiles(dir, now), pruneAttemptState(dir, now, logAge))
}

// pruneAttemptFiles removes job-files attempt folders that no saved attempt
// custody claims (#201). Confirmed published attempts go immediately; failed
// attempts and unknown orphans keep AttemptFilesGrace. Folders are listed before
// custody is read, and custody is saved before a folder is created, so a listed
// folder without custody can never belong to a starting attempt.
func pruneAttemptFiles(dir string, now time.Time) error {
	root := filepath.Join(dir, "job-files")
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("attempt files: %s is not a real directory; refusing to clean it", root)
	}
	jobs, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	listed := map[string][]string{}
	for _, job := range jobs {
		if !job.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, job.Name()))
		if err != nil {
			return err
		}
		listed[job.Name()] = []string{}
		for _, e := range entries {
			listed[job.Name()] = append(listed[job.Name()], e.Name())
		}
	}
	custody := map[string]map[string]bool{}
	published := map[string]map[string]bool{}
	paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
	if err != nil {
		return err
	}
	for _, path := range append(paths, filepath.Join(dir, "legacy-state.json"), filepath.Join(dir, "state.json")) {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var st workerState
		if err = decodeWorkerState(data, &st); err != nil {
			return err
		}
		if st.Job != nil {
			if custody[st.Job.ID] == nil {
				custody[st.Job.ID] = map[string]bool{}
			}
			custody[st.Job.ID][strconv.Itoa(st.Job.Attempt)] = true
		}
		if st.PublishedAttempt != nil && st.Job == nil && st.Result == nil && st.PID == 0 && !st.LaunchPending && st.Pending == 0 {
			e := st.PublishedAttempt
			if published[e.ID] == nil {
				published[e.ID] = map[string]bool{}
			}
			published[e.ID][strconv.Itoa(e.Attempt)] = true
		}
	}
	var result error
	for _, job := range jobs {
		names, ok := listed[job.Name()]
		if !ok {
			continue
		}
		for _, name := range names {
			// Staging folders of a held job may belong to its current attempt.
			if custody[job.Name()][name] || custody[job.Name()] != nil && strings.HasPrefix(name, ".") {
				continue
			}
			path := filepath.Join(root, job.Name(), name)
			if expired, err := attemptFilesExpired(path, now, published[job.Name()][name]); !expired {
				result = errors.Join(result, err)
				continue
			}
			if err := os.RemoveAll(path); err != nil {
				result = errors.Join(result, err)
				// A failed removal usually takes the marker with it. Re-mark
				// even published attempts so a stuck folder is retried daily.
				_, markErr := attemptFilesExpired(path, now, false)
				result = errors.Join(result, markErr)
			}
		}
		if custody[job.Name()] == nil {
			// Only an empty folder goes; a new attempt may have just created one.
			_ = os.Remove(filepath.Join(root, job.Name()))
		}
	}
	return result
}

// attemptFilesExpired reports whether an unclaimed attempt folder has been
// unclaimed, or unmodified for non-folders, for AttemptFilesGrace. The first
// sighting of a folder writes its marker unless publication permits immediate
// deletion. An existing marker also backs off failed published-folder removals.
func attemptFilesExpired(path string, now time.Time, published bool) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return published || !info.ModTime().After(now.Add(-AttemptFilesGrace)), nil
	}
	marker := filepath.Join(path, unclaimedMarker)
	if info, err = os.Lstat(marker); err == nil {
		return !info.ModTime().After(now.Add(-AttemptFilesGrace)), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if published {
		return true, nil
	}
	// O_EXCL never follows a planted link.
	f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	return false, os.Chtimes(marker, now, now)
}
