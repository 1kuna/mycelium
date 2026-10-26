package myce

import (
	"encoding/hex"
	"errors"
	"fmt"
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
	q.nextLogCleanup = time.Time{}
	q.mu.Unlock()
	return q.pruneBatchLogs()
}

func (q *Queue) pruneBatchLogs() error {
	q.mu.Lock()
	hold := q.logCleanupHolds.start()
	defer func() {
		duration := queueHoldDuration(hold)
		q.mu.Unlock()
		logQueueHold(duration, "pruneBatchLogs", nil)
	}()
	if q.simulation {
		return nil
	}
	now := q.clock.Now()
	if now.Before(q.nextLogCleanup) {
		return nil
	}
	cutoff := now.Add(-q.logRetention)
	for id, j := range q.s.Jobs {
		if !terminal(j.State) {
			continue
		}
		ended := j.Queued
		if j.FinishedAt != nil {
			ended = *j.FinishedAt
		}
		if ended.After(cutoff) {
			continue
		}
		if _, err := q.db.Exec("DELETE FROM logs WHERE job=?", id); err != nil {
			return err
		}
		if _, err := q.db.Exec("DELETE FROM log_offsets WHERE job=?", id); err != nil {
			return err
		}
	}
	q.nextLogCleanup = now.Add(time.Minute)
	return nil
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
// runs' work files are never uploaded. It matches stopped-container retention.
const AttemptFilesGrace = 24 * time.Hour

// unclaimedMarker records, by its modification time, when retention first saw
// an attempt folder without custody.
const unclaimedMarker = ".myce-unclaimed"

// workerRetention is one pass of the worker's periodic local retention.
func workerRetention(dir string, now time.Time, logAge time.Duration) error {
	return errors.Join(pruneWorkerLogs(dir, "", now, logAge), pruneAttemptFiles(dir, now), pruneAttemptState(dir, now, logAge))
}

// pruneAttemptFiles removes job-files attempt folders that no saved attempt
// custody claims (#201) once AttemptFilesGrace has passed since retention first
// saw them unclaimed. Custody clears only after the queue accepts the result,
// and a crash leaves orphans without custody. Folders are listed before custody
// is read, and custody is saved before a folder is created, so a listed folder
// without custody can never belong to a starting attempt.
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
			if expired, err := attemptFilesExpired(path, now); !expired {
				result = errors.Join(result, err)
				continue
			}
			// A failed removal usually takes the marker with it, so the
			// folder gets a fresh grace and its error repeats daily, not per pass.
			result = errors.Join(result, os.RemoveAll(path))
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
// sighting of a folder writes its marker.
func attemptFilesExpired(path string, now time.Time) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return !info.ModTime().After(now.Add(-AttemptFilesGrace)), nil
	}
	marker := filepath.Join(path, unclaimedMarker)
	if info, err = os.Lstat(marker); err == nil {
		return !info.ModTime().After(now.Add(-AttemptFilesGrace)), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
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
