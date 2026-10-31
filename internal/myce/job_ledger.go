package myce

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"time"
)

// Ledger entries are immutable. The existing job record and its keyed replay
// answer own their lifetime; no independent store or cleanup authority exists.
type JobLedgerInfo struct {
	Complete  bool `json:"complete"`
	HighWater int  `json:"high_water"`
}

type JobLedgerEntry struct {
	Placement       *PlacementDecision  `json:"placement,omitempty"`
	WorkLearning    *WorkLearningChange `json:"work_learning,omitempty"`
	Sequence        int                 `json:"sequence"`
	At              time.Time           `json:"at"`
	Attempt         int                 `json:"attempt"`
	ExecutionID     string              `json:"execution_id"`
	ProcessID       string              `json:"process_id,omitempty"`
	SupervisorPID   int                 `json:"supervisor_pid,omitempty"`
	SupervisorBirth string              `json:"supervisor_birth,omitempty"`
	Machine         string              `json:"machine"`
	Session         string              `json:"session"`
	Kind            string              `json:"kind"`
	Reason          string              `json:"reason"`
	State           string              `json:"state,omitempty"`
	Release         string              `json:"release"`
	CustodyID       string              `json:"custody_id,omitempty"`
	CustodyStart    *time.Time          `json:"custody_start,omitempty"`
	CustodyEnd      *time.Time          `json:"custody_end,omitempty"`
}

type JobLedgerPage struct {
	JobID     string           `json:"job_id"`
	Complete  bool             `json:"complete"`
	HighWater int              `json:"high_water"`
	Next      int              `json:"next"`
	HasMore   bool             `json:"has_more"`
	Entries   []JobLedgerEntry `json:"entries"`
}

func (q *Queue) appendJobLedger(j *Job, e JobLedgerEntry) {
	if j.Ledger == nil {
		j.Ledger = &JobLedgerInfo{}
	} // Older history cannot be reconstructed.
	e.Sequence = len(j.LedgerEntries) + 1
	e.ExecutionID = fmt.Sprintf("%s/%d", j.ID, e.Attempt)
	e.At = q.clock.Now()
	if e.Release == "" {
		e.Release = "pending"
	}
	for _, c := range j.Copies {
		if c.Attempt == e.Attempt && c.Machine == e.Machine && c.Session == e.Session {
			if e.ProcessID == "" {
				e.ProcessID = c.ProcessID
			}
			if e.CustodyID == "" && c.ReleaseConfirmed {
				e.Release = "confirmed"
			}
		}
	}
	j.LedgerEntries = append(j.LedgerEntries, e)
	// Replace, never mutate: snapshots/undo may borrow this immutable metadata.
	j.Ledger = &JobLedgerInfo{Complete: j.Ledger.Complete, HighWater: e.Sequence}
}

func ledgerAttempt(j *Job, kind, reason string) JobLedgerEntry {
	return JobLedgerEntry{Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, Kind: kind, Reason: reason, State: j.State}
}

// JobLedger copies only a bounded page under the lock. Encoding/network I/O is
// outside it. highWater == -1 begins a snapshot (including an empty snapshot).
func (q *Queue) JobLedger(id string, after, highWater, limit int, requestContexts ...context.Context) (JobLedgerPage, error) {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	defer requestUnlock1()
	j := q.s.Jobs[id]
	if j == nil && q.records != nil {
		if r := q.s.Submissions[q.records.bindings[id]]; r != nil {
			j = r.Job
		}
	}
	if j == nil {
		return JobLedgerPage{}, &HTTPError{Status: 410, Code: "job_ledger_unavailable", Message: "job ledger is unknown or has expired under history retention"}
	}
	n := len(j.LedgerEntries)
	if highWater == -1 {
		highWater = n
	}

	if after < 0 || highWater < 0 || highWater > n || after > highWater || limit < 1 || limit > 256 {
		return JobLedgerPage{}, &HTTPError{Status: 400, Code: "job_ledger_cursor_invalid", Message: "require 0 <= after <= high_water and limit 1..256"}
	}
	end := min(after+limit, highWater)
	entries := make([]JobLedgerEntry, end-after)
	copy(entries, j.LedgerEntries[after:end])
	// Timestamp pointers belong to immutable entries and are copied for callers.
	for i := range entries {
		if entries[i].WorkLearning != nil {
			change := *entries[i].WorkLearning
			entries[i].WorkLearning = &change
		}
		if entries[i].Placement != nil {
			p := *entries[i].Placement
			p.Devices = slices.Clone(p.Devices)
			p.LimitedBy = slices.Clone(p.LimitedBy)
			entries[i].Placement = &p
		}
		if entries[i].CustodyStart != nil {
			at := *entries[i].CustodyStart
			entries[i].CustodyStart = &at
		}
		if entries[i].CustodyEnd != nil {
			at := *entries[i].CustodyEnd
			entries[i].CustodyEnd = &at
		}
	}
	return JobLedgerPage{JobID: id, Complete: j.Ledger != nil && j.Ledger.Complete, HighWater: highWater, Next: end, HasMore: end < highWater, Entries: entries}, nil
}

func (c Client) JobLedger(ctx context.Context, id string, after, highWater, limit int) (JobLedgerPage, error) {
	var out JobLedgerPage
	query := url.Values{"after": {fmt.Sprint(after)}, "limit": {fmt.Sprint(limit)}}
	if highWater >= 0 {
		query.Set("high_water", fmt.Sprint(highWater))
	}
	err := c.Call(ctx, "GET", "/api/jobs/"+url.PathEscape(id)+"/ledger?"+query.Encode(), nil, &out)
	return out, err
}

// Native custody reports reuse the worker's durable execution identity and exact
// attempt fencing. Only cleanup can release historical custody; new generations
// require the current active attempt and host launch authorization.
type NativeCustodyReport struct {
	Execution
	Machine         string `json:"machine"`
	Session         string `json:"session"`
	ProcessID       string `json:"process_id"`
	SupervisorPID   int    `json:"supervisor_pid,omitempty"`
	SupervisorBirth string `json:"supervisor_birth,omitempty"`
	CustodyID       string `json:"custody_id"`
	Phase           string `json:"phase"` // reserved, started, released
}

func (q *Queue) NativeCustody(r NativeCustodyReport, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	if r.CustodyID == "" || len(r.CustodyID) > 128 || r.ProcessID == "" || len(r.ProcessID) > 128 || !slices.Contains([]string{"reserved", "started", "released"}, r.Phase) {
		return &HTTPError{Status: 400, Code: "native_custody_invalid", Message: "invalid native custody identity or phase"}
	}
	return q.updateRequest(requestCtx, func() error {
		j, err := q.current(Result{Execution: r.Execution, Machine: r.Machine, Session: r.Session})
		if err != nil {
			return err
		}
		var old *JobLedgerEntry
		for i := len(j.LedgerEntries) - 1; i >= 0; i-- {
			e := &j.LedgerEntries[i]
			if e.CustodyID == r.CustodyID {
				old = e
				break
			}
		}
		if old != nil && (old.Attempt != r.Attempt || old.Machine != r.Machine || old.Session != r.Session || old.ProcessID != r.ProcessID) {
			return ErrStale
		}
		kind := "custody_" + r.Phase
		if old == nil && r.Phase != "reserved" || old != nil && old.Kind != kind && (r.Phase == "reserved" || old.Release == "confirmed") {
			return ErrStale
		}
		if r.Phase != "released" {
			q.maintainStartDeadlines()
			if j.State != "running" || cancelled(j) || !q.hostLaunchAllowed(q.s.Machines[r.Machine], r.Session) || !q.startPlacementAllowed(j) {
				return ErrStale
			}
		}
		if old != nil && old.Kind == kind {
			return nil
		} // Replay still passes the live launch fence above.
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.Attempt == r.Attempt && c.Machine == r.Machine && c.Session == r.Session {
				if c.ReleaseConfirmed || c.ProcessID != "" && c.ProcessID != r.ProcessID {
					return ErrStale
				}
				c.ProcessID = r.ProcessID
			}
		}
		e := ledgerAttempt(j, kind, "native_"+r.Phase)
		e.ProcessID, e.CustodyID = r.ProcessID, r.CustodyID
		e.SupervisorPID, e.SupervisorBirth = r.SupervisorPID, r.SupervisorBirth
		if old != nil {
			e.CustodyStart = old.CustodyStart
			if r.Phase == "released" {
				e.SupervisorPID, e.SupervisorBirth = old.SupervisorPID, old.SupervisorBirth
			}
		}
		at := q.clock.Now()
		if r.Phase == "started" {
			e.CustodyStart = &at
		}
		if r.Phase == "released" {
			e.Release = "confirmed"
			e.CustodyEnd = &at
			e.Reason = "native_group_verified_empty"
		}
		q.appendJobLedger(j, e)
		return nil
	}, nil)
}

func (w Worker) reportNativeCustody(j Job, session, execution, custody, phase string, supervisors ...int) error {
	ctx, cancel := context.WithTimeout(context.Background(), workerStartRequestTimeout)
	defer cancel()
	report := NativeCustodyReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: w.Machine.ID, Session: session, ProcessID: execution, CustodyID: custody, Phase: phase}
	if len(supervisors) > 0 {
		report.SupervisorPID = supervisors[0]
		birth, err := processBirth(supervisors[0])
		if err != nil {
			return err
		}
		report.SupervisorBirth = birth
	}
	var reply struct {
		OK        bool   `json:"ok"`
		StartGate string `json:"start_gate"`
	}
	err := w.Client.Call(ctx, "POST", "/api/workers/native-custody", report, &reply)
	if err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartAuthorization, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol {
		return errWorkerStartAuthorization
	}
	return nil
}

// Older workers cannot provide native generation coverage. A queue execution ID
// is still known for every assignment; kernel/process identity requires custody.
func (q *Queue) checkLedgerNativeCoverage(j *Job) {
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.ProcessID != "" {
			return
		}
	}
	if j.Ledger != nil {
		j.Ledger = &JobLedgerInfo{Complete: false, HighWater: len(j.LedgerEntries)}
	}
}
