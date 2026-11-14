package myce

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// One exact-attempt owner shares this with its asynchronous publication lane.
// Count only server-confirmed blob bytes, once per digest, including dedup hits.
// Never put local paths, blob digests, URLs or transport error text in reports.
type publicationProgress struct {
	timing    uploadTiming // Measured queue service shared by this result's files.
	mu        sync.Mutex
	sequence  uint64
	active    map[uint64]publicationOperation
	published map[string]int64
	bytes     int64
	failure   string
}

type publicationOperation struct {
	name  string
	since time.Time
}

// ResultPublication is observation, never execution-release authority. Byte
// counts are unique server-confirmed blobs observed by this worker process.
type ResultPublication struct {
	Execution
	PublishedBytes int64   `json:"published_bytes"`
	Blocking       string  `json:"blocking"`
	WaitingSeconds float64 `json:"waiting_seconds"`
	LastError      string  `json:"last_error,omitempty"`
}

func (r ResultPublication) String() string {
	return fmt.Sprintf("result publication %s attempt=%d: published_bytes=%d blocking=%s waiting=%.3fs last_error=%s", ShortID(r.ID), r.Attempt, r.PublishedBytes, r.Blocking, r.WaitingSeconds, r.LastError)
}

func safeResultPublications(reports []ResultPublication) []ResultPublication {
	out := make([]ResultPublication, 0, len(reports))
	for _, r := range reports {
		switch r.Blocking {
		case "retry", "filesystem/checkpoint_publication", "filesystem/output_spool", "filesystem/output_path", "filesystem/output_validation",
			"filesystem/storage_observation", "filesystem/failure_diagnostics",
			"POST /api/workers/log", "POST /api/workers/complete", "GET /api/jobs/:id", "POST /api/files/result", "POST /api/files/preflight", "POST /api/files/checkpoint",
			"HEAD /api/files/blobs/:id", "POST /api/files/uploads/", "HEAD /api/files/uploads/:id", "PATCH /api/files/uploads/:id":
		default:
			r.Blocking = "publication"
		}
		if finalizationErrors[r.LastError] == "" && r.LastError != "queue_stalled" {
			r.LastError = ""
		}
		out = append(out, r)
	}
	return out
}

func (p *publicationProgress) begin(clock Clock, name string) func() {
	if p == nil {
		return func() {}
	}
	p.mu.Lock()
	p.sequence++
	id := p.sequence
	if p.active == nil {
		p.active = map[uint64]publicationOperation{}
	}
	p.active[id] = publicationOperation{name, clock.Now()}
	p.mu.Unlock()
	return func() { p.mu.Lock(); delete(p.active, id); p.mu.Unlock() }
}

func (p *publicationProgress) request(clock Clock, method, path string) func() {
	for _, prefix := range []string{"/api/files/blobs/", "/api/files/uploads/", "/api/jobs/"} {
		if strings.HasPrefix(path, prefix) && len(path) > len(prefix) {
			path = prefix + ":id"
			break
		}
	}
	return p.begin(clock, method+" "+path)
}

func (p *publicationProgress) confirm(ref FileRef) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.published == nil {
		p.published = map[string]int64{}
	}
	p.bytes += ref.SizeBytes - p.published[ref.SHA256]
	p.published[ref.SHA256] = ref.SizeBytes
}

func (p *publicationProgress) failed(err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failure = ""
	if err != nil {
		var stalled *QueueStalledError
		if errors.As(err, &stalled) {
			p.failure = "queue_stalled"
			return
		}
		p.failure = runtimeDiagnostic(finalizationErrorFor("output_upload", "", err)).Finalization.Error
	}
}

func (p *publicationProgress) snapshot(j Job, now time.Time) *ResultPublication {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sequence == 0 {
		return nil
	}
	var latest uint64
	for id := range p.active {
		latest = max(latest, id)
	}
	r := &ResultPublication{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, PublishedBytes: p.bytes, Blocking: "retry", LastError: p.failure}
	if op, ok := p.active[latest]; ok {
		r.Blocking, r.WaitingSeconds = op.name, max(0, now.Sub(op.since).Seconds())
	}
	return r
}
