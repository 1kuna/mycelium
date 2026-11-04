package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Attempt-local receipt is independent of custody snapshots written by runners.
// The deadline is in the worker clock domain, conservatively anchored before
// the Started request. Request/response transit can shorten, never renew it.
type workerWallTime struct {
	Execution
	Session   string    `json:"session"`
	ExpiresAt time.Time `json:"expires_at"`
	Expired   bool      `json:"expired,omitempty"`
}

func wallTimeReceiptName(j Job) string { return fmt.Sprintf("wall-time-%s-%d.json", j.ID, j.Attempt) }
func readWorkerWallTime(dir string, j Job) (*workerWallTime, error) {
	if j.Spec.WallTime == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, wallTimeReceiptName(j)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} // Main has not passed Started yet.
	if err != nil {
		return nil, err
	}
	var r workerWallTime
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.ID != j.ID || r.Attempt != j.Attempt || r.Session != j.Session || r.ExpiresAt.IsZero() {
		return nil, errors.New("wall-time receipt identity invalid; custody retained")
	}
	return &r, nil
}
func saveWorkerWallTime(dir string, j Job, requested time.Time, remaining float64) error {
	old, err := readWorkerWallTime(dir, j)
	if err != nil {
		return err
	}
	if old != nil {
		return nil
	} // Replayed authorization cannot extend the budget.
	r := workerWallTime{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, ExpiresAt: requested.Add(time.Duration(remaining * float64(time.Second)))}
	return saveWorkerJSON(dir, wallTimeReceiptName(j), r)
}
func enforceWorkerWallTime(dir string, j Job, p *process, now time.Time) error {
	r, err := readWorkerWallTime(dir, j)
	if err != nil || r == nil || p == nil || now.Before(r.ExpiresAt) {
		return err
	}
	if !r.Expired {
		r.Expired = true
		if err := saveWorkerJSON(dir, wallTimeReceiptName(j), r); err != nil {
			return err
		}
	}
	// Existing group/container shutdown records delivered-before-exit evidence,
	// preserves checkpoints, and escalates with the existing cleanup grace.
	p.stopService(now, StopGrace)
	return nil
}
func applyWorkerWallTime(dir string, j Job, r Result) (Result, error) {
	limit, err := readWorkerWallTime(dir, j)
	if err != nil {
		return r, err
	}
	// Intent alone cannot replace a successful or independently failed member.
	// Delivered shutdown before exit is established by the ordinary classifier.
	if limit != nil && limit.Expired && r.Interrupted && !r.OutOfMemory && !attemptDiskLimitFailure(r) {
		r.Success, r.Interrupted, r.PriorityStop, r.PoliteStop, r.OperatorStop = false, false, nil, false, false
		r.ExecutionCause, r.Error = wallTimeCause(), wallTimeLimit
	}
	return r, nil
}
