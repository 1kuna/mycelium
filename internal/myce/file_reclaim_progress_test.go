package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Busy workers' check-ins keep a control request waiting on q.mu longer than
// one measured service interval almost continuously. Register such a waiter
// that never acquires, so every pressure sample in the pass reads busy.
func constantQueuePressure(t *testing.T, q *Queue, clock *fakeClock) {
	t.Helper()
	p := q.mu.pressure.Load()
	if p == nil {
		t.Fatal("queue mutex has no pressure tracker")
	}
	p.acquired(nil)
	clock.Advance(time.Millisecond)
	p.release()
	wait := p.wait()
	clock.Advance(time.Millisecond + time.Nanosecond)
	t.Cleanup(func() { p.acquired(wait); p.release() })
	if busy, _ := q.evidencePressure(); !busy {
		t.Fatal("fixture did not establish control pressure")
	}
}

// Finished jobs whose inputs passed retention, plus their blob refs.
func expiredRetentionInputs(t *testing.T, q *Queue, clock *fakeClock, c Client, n int) ([]*Job, []FileRef) {
	t.Helper()
	jobs, refs := make([]*Job, 0, n), make([]FileRef, 0, n)
	for i := range n {
		_, ref := retentionUpload(t, c, fmt.Sprintf("expired input %03d", i))
		s := spec("experiment")
		s.Files = FileManifest{"input": ref}
		j := submit(t, q, s)
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		jobs, refs = append(jobs, j), append(refs, ref)
	}
	clock.Advance(testFileRetention + time.Second)
	// An ordinary expiry pass marks one due record (fileReclaimBatch).
	for range n {
		if err := q.updateRequest(context.Background(), func() error { q.maintainFileExpiry(); return nil }, nil); err != nil {
			t.Fatal(err)
		}
	}
	return jobs, refs
}

func blobExists(t *testing.T, q *Queue, ref FileRef) bool {
	t.Helper()
	_, err := os.Lstat(q.files.blob(ref))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

// Live r6.10: retention set, queue restarted, and 13 minutes of busy check-ins
// later nothing had been reclaimed. Every pass must make progress anyway.
// Live r6.11 also carried a publication horizon past the file retention (#1375).
func TestFileReclaimDrainsUnderConstantControlPressure(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	jobs, refs := expiredRetentionInputs(t, q, clock, c, 6)
	q.s.PublicationHorizon = 2 * testFileRetention
	constantQueuePressure(t, q, clock)

	if err := q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	gone := 0
	for _, ref := range refs {
		if !blobExists(t, q, ref) {
			gone++
		}
	}
	if gone == 0 {
		t.Fatal("a pass under constant control pressure reclaimed nothing")
	}
	for pass := 0; gone < len(refs) && pass < len(refs); pass++ {
		if err := q.reclaimJobFiles(); err != nil {
			t.Fatal(err)
		}
		gone = 0
		for _, ref := range refs {
			if !blobExists(t, q, ref) {
				gone++
			}
		}
	}
	if gone != len(refs) {
		t.Fatalf("pressure left %d of %d expired blobs", len(refs)-gone, len(refs))
	}
	for _, j := range jobs {
		got, err := c.GetJob(context.Background(), j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.FileReclamation != nil && len(got.FileReclamation.Pending) != 0 {
			t.Fatal("reclaimed blobs still pending on job", j.ID, got.FileReclamation.Pending)
		}
	}
	status := q.fileStoreStatus(context.Background())
	if status == nil || status.Collection == nil {
		t.Fatal("file store status has no collection counters")
	}
	got := *status.Collection
	if got.ReclaimedBlobs != uint64(len(refs)) || got.ReclaimedBytes == 0 || got.Passes == 0 || got.PendingCandidates != 0 || got.LastReclaimAt == nil {
		t.Fatalf("collection counters did not record the drain: %+v", got)
	}
}

// Contention changes how fast collection runs, never what it may remove.
func TestFileReclaimUnderPressureKeepsReferencedAndUnexpired(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, expired := expiredRetentionInputs(t, q, clock, c, 3)

	// Still referenced by a live job, sharing bytes with an expired one.
	shared := spec("experiment")
	shared.Files = FileManifest{"input": expired[0]}
	submit(t, q, shared)
	// Finished but not yet past retention.
	_, fresh := retentionUpload(t, c, "finished recently")
	recent := spec("experiment")
	recent.Files = FileManifest{"input": fresh}
	if err := q.Cancel(submit(t, q, recent).ID); err != nil {
		t.Fatal(err)
	}
	constantQueuePressure(t, q, clock)
	for range 4 {
		if err := q.reclaimJobFiles(); err != nil {
			t.Fatal(err)
		}
	}
	if !blobExists(t, q, expired[0]) {
		t.Fatal("blob still referenced by a live job was removed")
	}
	if !blobExists(t, q, fresh) {
		t.Fatal("blob inside its retention window was removed")
	}
	for _, ref := range expired[1:] {
		if blobExists(t, q, ref) {
			t.Fatal("unreferenced expired blob survived")
		}
	}
}

// A submission can re-reference an expired blob after the collector took its
// candidate snapshot. The batch recheck inside the fence must keep it.
func TestFileReclaimKeepsBlobReferencedAfterSnapshot(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, refs := expiredRetentionInputs(t, q, clock, c, 3)
	constantQueuePressure(t, q, clock)
	ctx := &retentionInterleaveContext{Context: context.Background(), once: func() {
		s := spec("experiment")
		s.Files = FileManifest{"input": refs[1]}
		submit(t, q, s)
	}}
	if err := q.reclaimPublishedFilesContext(ctx); err != nil {
		t.Fatal(err)
	}
	if !blobExists(t, q, refs[1]) {
		t.Fatal("blob referenced between snapshot and delete was removed")
	}
	for _, i := range []int{0, 2} {
		if blobExists(t, q, refs[i]) {
			t.Fatal("unreferenced expired blob survived", i)
		}
	}
}

// The interrupted-upload sweep had the same pre-work yield.
func TestInterruptedUploadSweepProgressesUnderControlPressure(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.fileRetention = time.Hour
	refs := []FileRef{
		capacityPartial(t, c, "interrupted input one", jobInputUploadPurpose),
		capacityPartial(t, c, "interrupted input two", jobInputUploadPurpose),
	}
	clock.Advance(max(q.fileRetention, filePublicationGrace) + time.Nanosecond)
	constantQueuePressure(t, q, clock)
	left := func() (n int) {
		for _, ref := range refs {
			if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256)); err == nil {
				n++
			}
		}
		return n
	}
	for pass := 0; pass < len(refs) && left() > 0; pass++ {
		before := left()
		if err := q.reclaimInterruptedUploads(context.Background()); err != nil {
			t.Fatal(err)
		}
		if left() >= before {
			t.Fatal("interrupted-upload pass under control pressure made no progress")
		}
	}
	if n := left(); n != 0 {
		t.Fatalf("pressure left %d interrupted uploads", n)
	}
}

// Live r6.11: a settled claim is reused by later submissions of the same bytes,
// and each re-settlement lifted the publication horizon to the claim's age.
func TestReusedInputClaimKeepsPublicationHorizon(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, ref := retentionUpload(t, c, "reused input")
	s := spec("experiment")
	s.Files = FileManifest{"input": ref}
	if _, err := q.claimFileInput(context.Background(), s, ref); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	submit(t, q, s)
	horizon := q.s.PublicationHorizon
	clock.Advance(4 * 24 * time.Hour)
	submit(t, q, s)
	if q.s.PublicationHorizon != horizon {
		t.Fatalf("re-settling a reused claim moved the horizon from %v to %v", horizon, q.s.PublicationHorizon)
	}
}
