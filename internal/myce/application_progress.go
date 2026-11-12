package myce

import (
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// Counts need pointers on input so missing fields cannot masquerade as zero.
type ProgressReport struct {
	updatedAt time.Time
	// Worker file identity/order, never an application-authored timestamp.
	UpdatedAt time.Time `json:"updated_at,omitempty"`

	Completed      *float64 `json:"completed"`
	Total          *float64 `json:"total"`
	Unit           string   `json:"unit"`
	AgeSeconds     float64  `json:"age_seconds"`
	StageIndex     *int     `json:"stage_index,omitempty"`
	StageCount     *int     `json:"stage_count,omitempty"`
	TotalWorkKnown bool     `json:"total_work_known"`
}

type ApplicationProgress struct {
	CadenceSeconds float64   `json:"cadence_seconds,omitempty"`
	Completed      float64   `json:"completed"`
	Total          float64   `json:"total"`
	Unit           string    `json:"unit"`
	Attempt        int       `json:"attempt"`
	Machine        string    `json:"machine"`
	Session        string    `json:"session"`
	ReceivedAt     time.Time `json:"received_at"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
	AgeSeconds     float64   `json:"age_seconds"`
	Stale          bool      `json:"stale"`
	StageIndex     *int      `json:"stage_index,omitempty"`
	StageCount     *int      `json:"stage_count,omitempty"`
	TotalWorkKnown bool      `json:"total_work_known"`
}

func (p *ProgressReport) valid() bool {
	return p != nil && p.Completed != nil && p.Total != nil && validMemoryValue(*p.Completed) && validMemoryValue(*p.Total) && *p.Completed <= *p.Total && len(p.Unit) <= 64 && extensionName.MatchString(p.Unit) && !math.IsNaN(p.AgeSeconds) && !math.IsInf(p.AgeSeconds, 0) && p.AgeSeconds >= 0 &&
		(p.StageIndex == nil || *p.StageIndex > 0) &&
		(p.StageCount == nil || (*p.StageCount > 0 && p.StageIndex != nil && *p.StageIndex <= *p.StageCount))
}

// Reuse the attempt's existing local input-directory retention and custody.
// Session is in the file name so a replacement worker cannot read its predecessor.
func applicationProgressFile(spool string, j Job) string {
	dir := filepath.Join(spool, "job-files", j.ID, strconv.Itoa(j.Attempt))
	return filepath.Join(dir, ".myce-progress-"+identityHash(j.Session)+".json")
}

// Only the worker reads the small local file; no filesystem I/O occurs at q.mu.
// Atomic replacement by the application avoids partial JSON. Invalid/missing
// files retain the last good queue report, whose age continues to grow.
func readApplicationProgress(spool string, j Job, now time.Time) *ProgressReport {
	path := applicationProgressFile(spool, j)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	limit, releaseBudget := reserveDiagnosticBytes(spool, uint64(max(0, info.Size())))
	defer releaseBudget()
	if info.Size() > limit {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil
	}
	var p ProgressReport
	if json.Unmarshal(data, &p) != nil {
		return nil
	}
	// Application timestamps/ages are not trusted. Use local file modification
	// age and queue receipt time, avoiding worker/queue clock synchronization.
	p.updatedAt = opened.ModTime()
	p.UpdatedAt = p.updatedAt
	p.AgeSeconds = max(0, now.Sub(p.updatedAt).Seconds())
	if !p.valid() {
		return nil
	}
	return &p
}

// Age a detached actor report at heartbeat send time, even when the actor is
// blocked in result handoff. Repeating its cached snapshot must not renew it.
func (p *ProgressReport) aged(now time.Time) *ProgressReport {
	if p == nil {
		return nil
	}
	copy := *p
	copy.AgeSeconds = max(0, now.Sub(p.updatedAt).Seconds())
	return &copy
}

func (q *Queue) retainApplicationProgress(j *Job, report *AttemptReport, m *Machine) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	p := report.ApplicationProgress
	if report.Session != m.Session || j.Session != report.Session || !p.valid() {
		return
	}
	now := q.clock.Now()
	cadence := 0.0
	if old := j.ApplicationProgress; old != nil && old.Attempt == j.Attempt && old.Session == j.Session {
		// File times order evidence within this worker/session, independently of
		// transport delay. Equal times are repeats and must not renew freshness.
		if !old.UpdatedAt.IsZero() && (p.UpdatedAt.IsZero() || !p.UpdatedAt.After(old.UpdatedAt)) {
			return
		}
		// Legacy workers lack source ordering. Only those reports use relative
		// age; transport delay makes ages incomparable to timestamped evidence.
		cadence = old.CadenceSeconds
		if !old.UpdatedAt.IsZero() && !p.UpdatedAt.IsZero() {
			cadence = max(cadence, p.UpdatedAt.Sub(old.UpdatedAt).Seconds())
		}
		oldAge := old.AgeSeconds + max(0, now.Sub(old.ReceivedAt).Seconds())
		if (old.UpdatedAt.IsZero() && p.UpdatedAt.IsZero() && p.AgeSeconds > oldAge) || (old.StageIndex != nil && p.StageIndex != nil && *p.StageIndex < *old.StageIndex) {
			return
		}
	}
	// Own the optional integers just as we already own the count values.
	cloneInt := func(v *int) *int {
		if v == nil {
			return nil
		}
		n := *v
		return &n
	}
	j.ApplicationProgress = &ApplicationProgress{CadenceSeconds: cadence, Completed: *p.Completed, Total: *p.Total, Unit: p.Unit, Attempt: j.Attempt, Machine: m.ID, Session: m.Session, ReceivedAt: now, UpdatedAt: p.UpdatedAt, AgeSeconds: p.AgeSeconds, StageIndex: cloneInt(p.StageIndex), StageCount: cloneInt(p.StageCount), TotalWorkKnown: p.TotalWorkKnown}
}
