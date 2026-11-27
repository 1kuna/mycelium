package myce

import (
	"encoding/json"
	"time"
)

// PublicationTimings describes an exact attempt. Worker observations cover
// only the current supervisor lifetime. Queue observations use its own clock.
type PublicationTimings struct {
	Worker                 *workerPublicationTimings `json:"worker,omitempty"`
	VerificationSeconds    *float64                  `json:"verification_seconds,omitempty"`
	CompletionQueueSeconds float64                   `json:"completion_queue_seconds"`
	ReleaseToAcceptSeconds *float64                  `json:"release_to_accept_seconds,omitempty"`
}

type workerPublicationTimings struct {
	ResultReadyToReleaseAckSeconds *float64 `json:"result_ready_to_release_ack_seconds,omitempty"`
	ManifestSeconds                float64  `json:"manifest_seconds"`
	UploadCreateSeconds            float64  `json:"upload_create_seconds"`
	UploadPatchSeconds             float64  `json:"upload_patch_seconds"`
	UploadWindowSeconds            float64  `json:"upload_window_seconds"`
	UploadCreates                  int64    `json:"upload_creates"`
	UploadPatches                  int64    `json:"upload_patches"`
	PublishedBytes                 int64    `json:"published_bytes"`
}

// The previous queue's strict Result decoder ignores optional HTTP headers.
const publicationTimingsHeader = "X-Myce-Publication-Timings"

func (p *publicationProgress) resultReady(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready.IsZero() {
		p.ready = now
	}
}

func (p *publicationProgress) releaseAcknowledged(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ResultReadyToReleaseAckSeconds == nil && !p.ready.IsZero() {
		elapsed := max(0, now.Sub(p.ready).Seconds())
		p.ResultReadyToReleaseAckSeconds = &elapsed
	}
}

// Called with p.mu held. Sums of request durations can overlap. The window
// measures wall time from the first upload request to the last reply.
func (p *publicationProgress) recordOperation(op publicationOperation, now time.Time) {
	elapsed := max(0, now.Sub(op.since).Seconds())
	switch op.name {
	case "filesystem/output_manifest", "filesystem/output_spool":
		p.ManifestSeconds += elapsed
		return
	case "POST /api/files/uploads/":
		p.UploadCreates++
		p.UploadCreateSeconds += elapsed
	case "PATCH /api/files/uploads/:id":
		p.UploadPatches++
		p.UploadPatchSeconds += elapsed
	default:
		return
	}
	if p.uploadStart.IsZero() || op.since.Before(p.uploadStart) {
		p.uploadStart = op.since
	}
	if now.After(p.uploadEnd) {
		p.uploadEnd = now
	}
}

func (p *publicationProgress) timingHeader() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready.IsZero() {
		return ""
	}
	v := p.workerPublicationTimings
	v.PublishedBytes = p.bytes
	if !p.uploadStart.IsZero() {
		v.UploadWindowSeconds = max(0, p.uploadEnd.Sub(p.uploadStart).Seconds())
	}
	data, _ := json.Marshal(v)
	return string(data)
}

func parsePublicationTimings(header string) *workerPublicationTimings {
	if len(header) == 0 || len(header) > 512 { // Fixed schema with eight numeric fields.
		return nil
	}
	var v *workerPublicationTimings
	if json.Unmarshal([]byte(header), &v) != nil || v == nil || v.UploadCreates < 0 || v.UploadPatches < 0 || v.PublishedBytes < 0 {
		return nil
	}
	times := []float64{v.ManifestSeconds, v.UploadCreateSeconds, v.UploadPatchSeconds, v.UploadWindowSeconds}
	if v.ResultReadyToReleaseAckSeconds != nil {
		times = append(times, *v.ResultReadyToReleaseAckSeconds)
	}
	for _, elapsed := range times {
		if elapsed < 0 {
			return nil
		}
	}
	return v
}

func (q *Queue) finishPublicationTimings(j *Job, timings *PublicationTimings, started time.Time, r Result) {
	t := *timings
	now := q.clock.Now()
	t.CompletionQueueSeconds = max(0, now.Sub(started).Seconds())
	accepted := now
	if j.FinishedAt != nil && j.Attempt == r.Attempt {
		accepted = *j.FinishedAt
	}
	for _, copy := range j.Copies {
		if copy.Attempt == r.Attempt && copy.Machine == r.Machine && copy.Session == r.Session && copy.ReleasedAt != nil {
			elapsed := max(0, accepted.Sub(*copy.ReleasedAt).Seconds())
			t.ReleaseToAcceptSeconds = &elapsed
			break
		}
	}
	for i := range j.AttemptHistory {
		h := &j.AttemptHistory[i]
		if h.Attempt == r.Attempt && h.Machine == r.Machine && h.Session == r.Session {
			h.PublicationTimings = &t
			break
		}
	}
	if terminal(j.State) && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		j.PublicationTimings = &t
	}
}
