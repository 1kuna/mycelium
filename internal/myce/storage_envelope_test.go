package myce

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func storageFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec, FileRef) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	c := deliveryClient(t, q)
	_, source := retentionUpload(t, c, "frozen source")
	_, out := retentionUpload(t, c, "1234")
	m := cpuMachine("storage-worker", "storage-host", "cpu", 8, 32)
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	s.Name, s.Project = "storage-profile", "synthetic"
	s.Files = FileManifest{".myce-source.tar": source}
	s.Outputs = map[string]string{"a": "results", "b": "results"}
	return q, clock, m, s, out
}
func storageResult(j *Job, ref FileRef, n uint64) Result {
	r := result(j, true)
	host := 1.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	r.StoragePeak = &AttemptStoragePeak{ScratchBytes: n, ManagedBytes: n, Files: 2, AllocationUnit: 1, Samples: 2, Complete: true, Source: managedStorageSource}
	r.OutputRefs = FileManifest{"a": ref, "b": ref}
	return r
}
func onlyStorage(t *testing.T, q *Queue) StorageObservation {
	t.Helper()
	if len(q.s.StorageObservations) != 1 {
		t.Fatalf("storage observations: %+v", q.s.StorageObservations)
	}
	for _, p := range q.s.StorageObservations {
		return p
	}
	panic("unreachable")
}

func TestStorageEnvelopeStatusAndUngatedWriter(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := storageResult(&first, out, 4)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	before := onlyStorage(t, q)
	// The same runtime validity marker protects the new accepted writer too.
	q.recordStorageNeeds(eligibleMemorySample{})
	q.recordStorageNeeds(eligibleMemorySample{job: q.s.Jobs[first.ID], result: r, storageKey: first.StorageEnvelope.Key})
	if !reflect.DeepEqual(before, onlyStorage(t, q)) {
		t.Fatal("ungated storage writer accepted evidence")
	}
	snap, err := q.snapshot(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.StorageObservations) != 1 || snap.StorageObservations[0].Managed.need(1) != 4 {
		t.Fatal("status omitted evidence", snap.StorageObservations)
	}
	var page bytes.Buffer
	if err := statusPage.Execute(&page, snap); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Storage evidence", "storage-profile", "shape", "hard limit unknown", "unenforced; observation only"} {
		if !strings.Contains(page.String(), text) {
			t.Errorf("status missing %q", text)
		}
	}
	// The API must retain the same fields through its privacy projection.
	var got Job
	if err := deliveryClient(t, q).Call(context.Background(), "GET", "/api/jobs/"+first.ID, nil, &got); err != nil {
		t.Fatal(err)
	}
	if got.StorageEnvelope == nil || len(got.StorageAttempts) != 1 || got.StorageAttempts[0].LearningReason != "eligible" {
		t.Fatal("job GET lost storage evidence", got.StorageEnvelope, got.StorageAttempts)
	}
}

func storageRestart(t *testing.T, q *Queue, clock Clock) *Queue {
	t.Helper()
	root, limit, checkpoints := q.files.root, q.files.limit, true
	reopened := memoryFactsRestart(t, q, clock)
	if err := reopened.ConfigureFileStore(root, limit, checkpoints); err != nil {
		t.Fatal(err)
	}
	return reopened
}
