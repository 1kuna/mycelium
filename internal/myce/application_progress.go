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

// Thirty seconds allows several ordinary heartbeats while exposing a stopped
// application reporter. It changes presentation only, never execution/custody.
const applicationProgressMaxAge = 30 * time.Second
const applicationProgressMaxBytes = 4096

// Counts need pointers on input so missing fields cannot masquerade as zero.
type ProgressReport struct {
	updatedAt time.Time

	Completed  *float64 `json:"completed"`
	Total      *float64 `json:"total"`
	Unit       string   `json:"unit"`
	AgeSeconds float64  `json:"age_seconds"`
}

type ApplicationProgress struct {
	Completed  float64   `json:"completed"`
	Total      float64   `json:"total"`
	Unit       string    `json:"unit"`
	Attempt    int       `json:"attempt"`
	Machine    string    `json:"machine"`
	Session    string    `json:"session"`
	ReceivedAt time.Time `json:"received_at"`
	AgeSeconds float64   `json:"age_seconds"`
	Stale      bool      `json:"stale"`
}

func (p *ProgressReport) valid() bool {
	return p != nil && p.Completed != nil && p.Total != nil && validMemoryValue(*p.Completed) && validMemoryValue(*p.Total) && *p.Completed <= *p.Total && len(p.Unit) <= 64 && extensionName.MatchString(p.Unit) && !math.IsNaN(p.AgeSeconds) && !math.IsInf(p.AgeSeconds, 0) && p.AgeSeconds >= 0
}

// Reuse the attempt's existing local input-directory retention and custody.
// Session is in the file name so a replacement worker cannot read its predecessor.
func applicationProgressFile(spool string, j Job) string {
	return filepath.Join(spool, "job-files", j.ID, strconv.Itoa(j.Attempt), ".myce-progress-"+identityHash(j.Session)+".json")
}

// Only the worker reads the small local file; no filesystem I/O occurs at q.mu.
// Atomic replacement by the application avoids partial JSON. Invalid/missing
// files retain the last good queue report, whose age continues to grow.
func readApplicationProgress(spool string, j Job, now time.Time) *ProgressReport {
	path := applicationProgressFile(spool, j)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > applicationProgressMaxBytes {
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
	data, err := io.ReadAll(io.LimitReader(f, applicationProgressMaxBytes+1))
	if err != nil || len(data) > applicationProgressMaxBytes {
		return nil
	}
	var p ProgressReport
	if json.Unmarshal(data, &p) != nil {
		return nil
	}
	// Application timestamps/ages are not trusted. Use local file modification
	// age and queue receipt time, avoiding worker/queue clock synchronization.
	p.updatedAt = opened.ModTime()
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
	p := report.ApplicationProgress
	if report.Session != m.Session || j.Session != report.Session || !p.valid() {
		return
	}
	j.ApplicationProgress = &ApplicationProgress{Completed: *p.Completed, Total: *p.Total, Unit: p.Unit, Attempt: j.Attempt, Machine: m.ID, Session: m.Session, ReceivedAt: q.clock.Now(), AgeSeconds: p.AgeSeconds}
}
