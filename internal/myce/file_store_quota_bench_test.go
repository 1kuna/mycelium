package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func benchDigest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// About 10k jobs across 6 projects: inputs shared within a project, unique results.
func quotaBenchQueue(b *testing.B, n int) *Queue {
	b.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { q.Close() })
	q.s.Submissions = map[string]*submissionRecord{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job-%05d", i)
		project := fmt.Sprintf("p%d", i%6)
		finished := clock.Now().Add(-time.Duration(i) * time.Minute)
		j := &Job{ID: id, State: "succeeded", Queued: finished.Add(-time.Minute), FinishedAt: &finished,
			Spec: Spec{Name: "bench", Project: project, Needs: ResourceNeeds{Platform: Platform{Any: true}},
				Files: FileManifest{"a": {SHA256: benchDigest(project + fmt.Sprint(i%50)), SizeBytes: 1 << 20}, "b": {SHA256: benchDigest(id + "in"), SizeBytes: 4096}}},
			OutputRefs: FileManifest{"result": {SHA256: benchDigest(id + "out"), SizeBytes: 1 << 16}}}
		if i%5 == 0 {
			j.FilesExpired = true
		}
		if i%10 == 1 {
			j.State, j.FinishedAt = "queued", nil
		}
		q.s.Jobs[id] = j
		q.s.Submissions[id] = &submissionRecord{Kind: submissionBinding, JobID: id, Job: j}
	}
	return q
}

// Status holds q.mu for this whole capture (#80, #527); file-store accounting
// must not add a second reference walk to it.
func BenchmarkStatusCaptureTenK(b *testing.B) {
	q := quotaBenchQueue(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := q.captureStatusObservation("", nil); err != nil {
			b.Fatal(err)
		}
	}
}

// A limited project's submit check walks only that project's jobs under q.mu.
func BenchmarkQuotaSubmitCheckTenK(b *testing.B) {
	q := quotaBenchQueue(b, 10000)
	q.s.Policies = map[string]ProjectPolicy{"p0": {Weight: 1, MaxRetainedFileBytes: 1 << 50}}
	s := Spec{Project: "p0", Class: "experiment"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		q.mu.Lock()
		if err := q.fileStoreQuotaRefusal(s, nil, 0); err != nil {
			b.Fatal(err)
		}
		q.mu.Unlock()
	}
}
