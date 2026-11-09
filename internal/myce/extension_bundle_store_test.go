package myce

import (
	"bytes"
	"context"

	"errors"
	"fmt"

	"os"

	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Scaled shape from #181: retained jobs, durable keyed answers, and frozen
// extensions. The full 798/782/135-row, 286 MB diagnostic is recorded in #181.
// Bytes are synthetic and generated in memory, never checked in or retained.
func bundleHistory(t *testing.T, q *Queue, payload int) {
	t.Helper()
	extensions := make([]Extension, 13)
	q.s.Extensions = map[string]Extension{}
	q.s.Submissions = map[string]*submissionRecord{}
	for i := range extensions {
		e := fakeExtension(t)
		e.Version = fmt.Sprint(i + 1)
		e.Files["payload"] = bytes.Repeat([]byte{byte(i)}, payload)
		e = freezeComposition(t, e)
		extensions[i] = e
		q.s.Extensions[extensionKey(e)] = e
	}
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("retained-%03d", i)
		s := Spec{Name: "retained", Project: "fixture", SubmissionKey: id, Extension: extensionKey(extensions[i%len(extensions)])}
		j := &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: q.clock.Now(), Spec: s, Extensions: []Extension{extensions[i%len(extensions)]}}
		q.s.Jobs[id] = j
		if i < 78 {
			fingerprint, err := submissionFingerprint(s)
			if err != nil {
				t.Fatal(err)
			}
			at := q.clock.Now()
			q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] = &submissionRecord{Kind: submissionBinding, Project: s.Project, Key: s.SubmissionKey, Fingerprint: fingerprint, JobID: id, Job: j, Created: &at}
		}
	}
}

func mutationCost(t *testing.T, q *Queue, count int) (time.Duration, uint64) {
	t.Helper()
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now() // Observation only; all queue decisions use fakeClock.
	for i := 0; i < count; i++ {
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start) / time.Duration(count)
	runtime.ReadMemStats(&after)
	return elapsed, (after.TotalAlloc - before.TotalAlloc) / uint64(count)
}

func TestExtensionBundleStoreInterruptedMigrationAndRecipes(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, true)
	// Build hooks carry the full extension folder, including immutable scripts.
	v := &e.Variants[0]
	r := v.Environments["main"]
	r.Build = []string{"true"}
	v.Environments["main"] = r
	e = freezeComposition(t, e)
	want, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	q.s.Extensions = map[string]Extension{extensionKey(e): e}
	j := &Job{ID: "retained", State: "succeeded", Extensions: []Extension{e}, EnvironmentRecipes: map[string]EnvironmentRecipe{"main": e.Variants[0].Environments["main"]}, SidecarEnvironmentRecipes: map[string]map[string]EnvironmentRecipe{"helper": {"main": e.Variants[0].Environments["main"]}}}
	q.s.Jobs[j.ID] = j
	jobWant := cloneJob(j)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	// Simulate interruption after all blobs are durable, before metadata commit.
	if err := q.migrateBundleFiles(); err == nil {
		t.Fatal("migration unexpectedly committed")
	}
	dbPath := q.lock.Name()[:len(q.lock.Name())-len(".lock")]
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.extensionByDigest(e.Digest)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("frozen bundle/recipe roundtrip", err)
	}
	out := cloneJob(reopened.s.Jobs[j.ID])
	if err := reopened.files.hydrateJob(&out); err != nil || !reflect.DeepEqual(jobWant.EnvironmentRecipes, out.EnvironmentRecipes) || !reflect.DeepEqual(jobWant.SidecarEnvironmentRecipes, out.SidecarEnvironmentRecipes) {
		t.Fatal("job recipe roundtrip", err)
	}

	expected := cloneJob(reopened.s.Jobs[j.ID])
	sentinel := errors.New("abort")
	if err := reopened.update(func() error { reopened.deleteJob(j.ID); return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, cloneJob(reopened.s.Jobs[j.ID])) {
		t.Fatal("rollback lost bundle refs")
	}
	// Retained answers protect blobs even after registry and job removal, and
	// even if another owner has marked the same digest as an expired job input.
	ref := *reopened.s.Extensions[extensionKey(e)].FilesRef
	if err := reopened.update(func() error {
		reopened.s.Submissions = map[string]*submissionRecord{"retained": {Kind: submissionBinding, JobID: j.ID, Job: reopened.s.Jobs[j.ID]}}
		delete(reopened.s.Extensions, extensionKey(e))
		reopened.deleteJob(j.ID)
		reopened.s.JobInputUploads = map[string]time.Time{ref.SHA256: clock.Now()}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention + time.Nanosecond)
	if err := reopened.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.files.verify(context.Background(), ref); err != nil {
		t.Fatal("retained answer bundle collected", err)
	}
}

func TestExtensionBundleStoreRejectsCorruptMigration(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	e.Files["run.sh"] = []byte("changed after freezing")
	q.s.Extensions = map[string]Extension{extensionKey(e): e}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	dbPath := q.lock.Name()[:len(q.lock.Name())-len(".lock")]
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, q.root, clock)
	if err == nil {
		reopened.Close()
		t.Fatal("migration accepted a changed bundle under the old digest")
	}
	if !strings.Contains(err.Error(), "bundle digest mismatch") {
		t.Fatal(err)
	}

	t.Run("ExtensionBundleStoreRepairsCorruptBlobFromEmbeddedCopy", func(t *testing.T) {

		q, clock := fixture(t)
		e := fakeExtension(t)
		frozen := e
		if err := q.files.freezeExtension(&frozen); err != nil {
			t.Fatal(err)
		}
		ref := *frozen.FilesRef
		// Preserve a good embedded copy in durable state, as after an interrupted
		// migration, while the previously published blob has been corrupted.
		e.FilesRef = &ref
		q.s.Extensions = map[string]Extension{extensionKey(e): e}
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(q.files.blob(ref), []byte("corrupt"), 0600); err != nil {
			t.Fatal(err)
		}
		dbPath := strings.TrimSuffix(q.lock.Name(), ".lock")
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(dbPath, q.root, clock, WithCheckpointAvailability(func() bool { return true }))
		if err != nil {
			t.Fatal("startup rejected the good embedded copy", err)
		}
		defer reopened.Close()
		if err := reopened.files.verify(context.Background(), ref); err != nil {
			t.Fatal("stored blob was not repaired", err)
		}
		got, err := reopened.extensionByDigest(e.Digest)
		if err != nil || !reflect.DeepEqual(got.Files, e.Files) {
			t.Fatal("repaired bundle differs from embedded copy", err)
		}

	})
}
