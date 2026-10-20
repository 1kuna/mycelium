package myce

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
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
	defer q.mu.Unlock()
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
	paths, err := filepath.Glob(filepath.Join(dir, "attempt-state-*.json"))
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
