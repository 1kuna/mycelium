package myce

import (
	"fmt"
	"strings"
	"testing"
)

// Incident #1052 review: a reader can start after persistence has recorded the
// final job invalidation but before publishReadRuntime publishes the capture.
func TestJobSnapshotCacheReadBetweenInvalidationAndPublication(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	persisted, resume := make(chan struct{}), make(chan struct{})
	q.observePersistence = func(queuePersistenceTiming) {
		close(persisted)
		<-resume
	}
	done := make(chan error, 1)
	go func() {
		done <- q.update(func() error { q.touchJob(j.ID).State = "paused"; return nil })
	}()
	<-persisted
	// No cache existed before invalidation. This read creates one using the
	// old committed capture while the writer is paused before publication.
	got, readErr := q.jobSnapshot(j.ID)
	close(resume)
	commitErr := <-done
	if readErr != nil || got == nil || got.State != "queued" {
		t.Fatal("publication-gap read lost its committed view", got, readErr)
	}
	if commitErr != nil {
		t.Fatal(commitErr)
	}
	if got, err := q.jobSnapshot(j.ID); err != nil || got.State != "paused" {
		t.Fatal("old capture repopulated cache after invalidation", got, err)
	}
}

func TestJobSnapshotCacheCallDependsOnBatchCommit(t *testing.T) {
	q, _, _ := modelFixture(t)
	call := addModelCall(t, q, "experiment")
	batch := submit(t, q, spec("experiment"))
	if err := q.update(func() error {
		q.s.Policies = map[string]ProjectPolicy{call.Project: {Weight: 1, MaxConcurrent: 1}}
		j := q.touchJob(batch.ID)
		j.Spec.Project = call.Project
		j.State = "running"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(call.ID)
	if err != nil || !strings.Contains(got.Reason, "concurrent work cap") {
		t.Fatal("missing batch blocker", got, err)
	}
	if err := q.update(func() error { q.touchJob(batch.ID).State = "succeeded"; return nil }); err != nil {
		t.Fatal(err)
	}
	got, err = q.jobSnapshot(call.ID)
	if err != nil || strings.Contains(got.Reason, "concurrent work cap") {
		t.Fatal("call retained committed batch blocker", got, err)
	}
}

func TestJobSnapshotCacheBounds(t *testing.T) {
	q, clock := fixture(t)
	// Half the cache's byte allowance leaves room for only one large receipt.
	large := strings.Repeat("x", jobSnapshotCacheBytes/2)
	count := jobSnapshotCacheEntries + 1
	if err := q.update(func() error {
		for i := range count + 2 {
			id := fmt.Sprint(i)
			reason := ""
			if i >= count {
				reason = large
			}
			q.putJob(id, &Job{ID: id, State: "succeeded", Spec: spec("experiment"), Queued: clock.Now(), Reason: reason})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range count + 2 {
		id := fmt.Sprint(i)
		if got, err := q.jobSnapshot(id); err != nil || got.ID != id {
			t.Fatalf("receipt %s failed: %v", id, err)
		}
		if i == count-1 && (len(q.jobSnapshots) != jobSnapshotCacheEntries || q.jobSnapshots["0"] != nil) {
			t.Fatal("cache did not evict the oldest entry at the count limit")
		}
	}
	if q.jobSnapshotBytes > jobSnapshotCacheBytes || len(q.jobSnapshots) != 1 || q.jobSnapshots[fmt.Sprint(count+1)] == nil {
		t.Fatal("cache did not evict older entries at the byte limit", q.jobSnapshotBytes, len(q.jobSnapshots))
	}
}

// Isolate the hot receipt path from the release contention benchmark's
// intentionally unbounded number of concurrent reader requests.
func BenchmarkJobSnapshotCacheWarm(b *testing.B) {
	q, _ := fixture(b)
	s := spec("experiment")
	s.Run.Command = []string{"echo", strings.Repeat("payload", 256)}
	j := submit(b, q, s)
	if _, err := q.jobSnapshot(j.ID); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := q.jobSnapshot(j.ID); err != nil {
			b.Fatal(err)
		}
	}
}
