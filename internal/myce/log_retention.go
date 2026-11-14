package myce

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const DefaultLogRetention time.Duration = 0

// ConfigureLogRetention bounds completed batch output. Active work and pending
// worker replay are protected; retention is logical deletion, not secure erasure.
func (q *Queue) ConfigureLogRetention(age time.Duration) error {
	if age < 0 {
		return errors.New("log-retention must be nonnegative")
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
	if age == 0 {
		return nil
	}
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

// unclaimedMarker records, by its modification time, when retention first saw
// an attempt folder without custody. Age is diagnostic, never deletion authority.
const unclaimedMarker = ".myce-unclaimed"

// A completion POST can acknowledge release without accepting outputs (for
// example when cancellation races success). Confirm the exact terminal attempt
// before authorizing early scratch deletion; failed diagnostics keep their grace.
func (w Worker) publishedAttemptFiles(ctx context.Context, st workerState) (*Execution, error) {
	if st.Job == nil || st.Result == nil || st.Result.ReleasePending || st.Result.ReleaseOnly {
		return nil, nil
	}
	j, r := st.Job, st.Result
	if j.Spec.OutputDelivery == "" && !j.Spec.extensionJob() && len(jobInputFiles(j))+len(jobInputPaths(j)) == 0 && !sharedReaders(j.Spec) {
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
// custody claims (#201). Confirmed published attempts go immediately; other
// folders require the queue's exact-attempt release/retention authority. Listed
// unknown orphans remain protected regardless of marker age. Folders are listed before
// custody is read, and custody is saved before a folder is created, so a listed
// folder without custody can never belong to a starting attempt.
func pruneAttemptFiles(dir string, now time.Time, collectors ...*attemptFilesCollector) error {
	return pruneAttemptFilesAt(dir, "job-files", now, collectors...)
}
func pruneAttemptFilesAt(dir, folder string, now time.Time, collectors ...*attemptFilesCollector) error {
	var collector *attemptFilesCollector
	if len(collectors) > 0 {
		collector = collectors[0]
	}
	root := filepath.Join(dir, folder)
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
	files, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer files.Close()
	opened, err := files.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) {
		return errors.New("attempt files root changed during collection")
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
		if st.Result != nil {
			if custody[st.Result.ID] == nil {
				custody[st.Result.ID] = map[string]bool{}
			}
			custody[st.Result.ID][strconv.Itoa(st.Result.Attempt)] = true
		}
		if st.PublishedAttempt != nil && (st.PID != 0 || st.LaunchPending || st.Pending != 0 || st.InputWriterPending || st.Preparation != "") {
			e := st.PublishedAttempt
			if custody[e.ID] == nil {
				custody[e.ID] = map[string]bool{}
			}
			custody[e.ID][strconv.Itoa(e.Attempt)] = true
		}
		if folder == "job-files" && st.PublishedAttempt != nil && st.Job == nil && st.Result == nil && st.PID == 0 && !st.LaunchPending && st.Pending == 0 {
			e := st.PublishedAttempt
			if published[e.ID] == nil {
				published[e.ID] = map[string]bool{}
			}
			published[e.ID][strconv.Itoa(e.Attempt)] = true
		}
	}
	// Staging folders of a held job may belong to its current attempt.
	local := func(job, name string) bool {
		return custody[job][name] || custody[job] != nil && strings.HasPrefix(name, ".")
	}
	var queries []Execution
	for _, job := range jobs {
		for _, name := range listed[job.Name()] {
			if attempt, ok := attemptFolderNumber(name); ok && !local(job.Name(), name) && !published[job.Name()][name] {
				queries = append(queries, Execution{ID: job.Name(), Attempt: attempt})
			}
		}
	}
	result := collector.prefetch(queries)
	for _, job := range jobs {
		names, ok := listed[job.Name()]
		if !ok {
			continue
		}
		for _, name := range names {
			path := filepath.Join(root, job.Name(), name)
			if local(job.Name(), name) {
				_, err := collector.record(path, "local_custody")
				result = errors.Join(result, err)
				continue
			}
			d := AttemptFilesDisposition{Reclaim: published[job.Name()][name], Reason: "accepted_result"}
			if !d.Reclaim {
				var err error
				d, err = collector.disposition(job.Name(), name)
				if err != nil {
					d = AttemptFilesDisposition{Reason: "queue_unavailable"}
					result = errors.Join(result, err)
				}
			}
			reason := d.Reason
			if d.Reclaim {
				reason = "removal_pending"
			}
			bytes, err := collector.record(path, reason)
			result = errors.Join(result, err)
			if expired, err := attemptFilesExpired(path, now, d.Reclaim); !expired {
				result = errors.Join(result, err)
				continue
			}
			if err := removeAttemptFiles(files, filepath.Join(job.Name(), name)); err != nil {
				result = errors.Join(result, err)
				// A partial removal can take the marker; restore its diagnostic.
				_, markErr := attemptFilesExpired(path, now, false)
				result = errors.Join(result, markErr)
			} else if collector != nil {
				collector.report.RetainedBytes -= bytes
				collector.report.Reasons[reason] -= bytes
				collector.report.ReclaimedBytes += bytes
			}
		}
		if custody[job.Name()] == nil {
			// Only an empty folder goes; a new attempt may have just created one.
			_ = os.Remove(filepath.Join(root, job.Name()))
		}
	}
	return result
}

// Called only after local custody and exact-attempt collection authority pass.
// Confine traversal to this tree and chmod open directory descriptors: staged
// inputs can be read-only, while symlink targets and hardlinked files stay intact.
func removeAttemptFiles(files *os.Root, name string) error {
	info, err := files.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return files.Remove(name)
	}
	tree, err := files.OpenRoot(name)
	if err != nil {
		return err
	}
	defer tree.Close()
	opened, err := tree.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) {
		return errors.New("attempt files changed during collection")
	}
	if err := fs.WalkDir(tree.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		dir, err := tree.Open(path)
		if err != nil {
			return err
		}
		info, err := dir.Stat()
		if err == nil && !info.IsDir() {
			err = errors.New("attempt directory changed during collection")
		}
		if err == nil && info.Mode().Perm()&0200 == 0 {
			err = dir.Chmod(info.Mode() | 0200)
		}
		return errors.Join(err, dir.Close())
	}); err != nil {
		return err
	}
	return files.RemoveAll(name)
}

// attemptFilesExpired records first observation, and collects only with exact
// authority. Existing markers, however old, cannot pin accepted/released data
// forever or grant authority to destroy unique data.
func attemptFilesExpired(path string, now time.Time, published bool) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return published, nil
	}
	marker := filepath.Join(path, unclaimedMarker)
	if info, err = os.Lstat(marker); err == nil {
		return published, nil
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
