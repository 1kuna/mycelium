package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

// Each immutable event carries the interval known at that transition. Missing
// times remain unknown, particularly for engines loaded before ledger coverage.
type ModelInterval struct {
	Model         string        `json:"model"`
	Attempt       string        `json:"attempt"`
	Machine       string        `json:"machine"`
	Host          string        `json:"host"`
	Session       string        `json:"session"`
	Device        string        `json:"device"`
	Identity      string        `json:"identity,omitempty"`
	Platform      string        `json:"platform"`
	Project       string        `json:"project"`
	Class         string        `json:"class"`
	Priority      string        `json:"priority"`
	Tier          PriorityMatch `json:"tier"`
	AssignedAt    *time.Time    `json:"assigned_at,omitempty"`
	LoadStartedAt *time.Time    `json:"load_started_at,omitempty"`
	StoppedAt     *time.Time    `json:"stopped_at,omitempty"`
	ReleasedAt    *time.Time    `json:"released_at,omitempty"`
}

type ModelIntervalEvent struct {
	Sequence int       `json:"sequence"`
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	ModelInterval
}

type ModelIntervalPage struct {
	CoverageSince time.Time            `json:"coverage_since"`
	HighWater     int                  `json:"high_water"`
	Next          int                  `json:"next"`
	HasMore       bool                 `json:"has_more"`
	Entries       []ModelIntervalEvent `json:"entries"`
}

func (q *Queue) recordModelInterval(s *ModelSlot, kind string) {
	if q.simulation {
		return
	}
	now := q.clock.Now()
	var interval ModelInterval
	if s.Interval != nil && s.Interval.Attempt == s.Attempt {
		interval = *s.Interval
	}
	if kind == "assigned" {
		if interval.AssignedAt != nil {
			return
		}
		interval = ModelInterval{AssignedAt: &now}
	}
	interval.Model, interval.Attempt, interval.Machine, interval.Session, interval.Device = s.Model, s.Attempt, s.Machine, s.Session, s.Device
	if m := q.s.Machines[s.Machine]; m != nil {
		interval.Host, interval.Platform = hostID(m), m.GPU
		for _, d := range m.Devices {
			if d.ID == s.Device {
				interval.Identity, interval.Platform = d.Identity, d.Platform
			}
		}
	}
	spec := q.slotSpec(s)
	interval.Project, interval.Class, interval.Priority = spec.Project, jobClass(spec), spec.Priority
	interval.Tier = q.priorityMatch(spec)
	switch kind {
	case "load_started":
		if interval.LoadStartedAt != nil {
			return
		}
		interval.LoadStartedAt = &now
	case "stopped":
		if interval.StoppedAt != nil {
			return
		}
		interval.StoppedAt = &now
	case "released":
		if interval.ReleasedAt != nil {
			return
		}
		if interval.StoppedAt == nil {
			interval.StoppedAt = &now
		}
		interval.ReleasedAt = &now
	}
	s.Interval = &interval // Replace: older events and transaction undo never alias mutable data.
	q.s.ModelIntervalHighWater++
	q.modelIntervalWrites = append(q.modelIntervalWrites, ModelIntervalEvent{Sequence: q.s.ModelIntervalHighWater, At: now, Kind: kind, ModelInterval: interval})
}

func (q *Queue) observeModelIntervals() {
	if q.simulation {
		return
	}
	if q.s.ModelIntervalSince == nil {
		now := q.clock.Now()
		q.s.ModelIntervalSince = &now
	}
	for _, s := range q.s.ModelSlots {
		if s.Interval == nil {
			q.recordModelInterval(s, "observed")
		}
		if s.State == "draining" || s.State == "unloading" || s.State == "unload_failed" {
			q.recordModelInterval(s, "stopped")
		}
		interval := s.Interval
		spec := q.slotSpec(s)
		match := q.priorityMatch(spec)
		if interval.Project != spec.Project || interval.Class != jobClass(spec) || interval.Priority != spec.Priority || interval.Tier != match {
			q.recordModelInterval(s, "class_changed")
		}
	}
}

func (q *Queue) persistModelIntervals(tx *sql.Tx) error {
	for _, event := range q.modelIntervalWrites {
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO model_intervals(seq,data) VALUES(?,?)", event.Sequence, data); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot only metadata under q.mu. The indexed, bounded SQLite read and JSON
// decoding run outside the queue lock; history is never copied into queue state.
func (q *Queue) ModelIntervals(ctx context.Context, after, highWater, limit int) (ModelIntervalPage, error) {
	if limit < 0 {
		return ModelIntervalPage{}, &HTTPError{Status: 400, Code: "model_interval_cursor_invalid", Message: "limit must be nonnegative"}
	}
	capacity, capacityErr := q.requestBudgets.pageLimit(ctx, reflect.TypeFor[ModelIntervalEvent]())
	if capacityErr != nil {
		return ModelIntervalPage{}, capacityErr
	}
	if limit == 0 {
		limit = capacity
	} else {
		limit = min(limit, capacity)
	}
	unlock := requestReadLock(ctx, &q.mu)
	maxSequence := q.s.ModelIntervalHighWater
	var since time.Time
	if q.s.ModelIntervalSince != nil {
		since = *q.s.ModelIntervalSince
	}
	unlock()
	if highWater == -1 {
		highWater = maxSequence
	}
	if after < 0 || highWater < 0 || highWater > maxSequence || after > highWater || limit < 1 {
		return ModelIntervalPage{}, &HTTPError{Status: 400, Code: "model_interval_cursor_invalid", Message: "require 0 <= after <= high_water and positive limit fitting queue capacity"}
	}
	page := ModelIntervalPage{CoverageSince: since, HighWater: highWater, Next: after, Entries: []ModelIntervalEvent{}}
	rows, err := q.db.QueryContext(ctx, "SELECT data FROM model_intervals WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?", after, highWater, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return page, err
		}
		var event ModelIntervalEvent
		if err = json.Unmarshal(data, &event); err != nil {
			return page, err
		}
		page.Entries = append(page.Entries, event)
		page.Next = event.Sequence
	}
	page.HasMore = page.Next < highWater
	return page, rows.Err()
}

func (a API) registerModelIntervals(mux *requestMux) {
	mux.HandleFunc("GET /api/models/intervals", func(w http.ResponseWriter, r *http.Request) {
		values := []int{0, -1, 0}
		for i, key := range []string{"after", "high_water", "limit"} {
			if r.URL.Query().Has(key) {
				v, err := strconv.Atoi(r.URL.Query().Get(key))
				if err != nil || v < 0 {
					respondOperation(w, nil, &HTTPError{Status: 400, Code: "model_interval_cursor_invalid", Message: "invalid interval pagination"}, "")
					return
				}
				values[i] = v
			}
		}
		page, err := a.Queue.ModelIntervals(r.Context(), values[0], values[1], values[2])
		respondOperation(w, page, err, "model_intervals_unavailable")
	})
}
