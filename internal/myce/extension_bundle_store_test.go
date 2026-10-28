package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func TestExtensionBundleStoreRetainedHistory(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	open := func(path string) *Queue {
		q, err := Open(path, dir, clock, WithCheckpointAvailability(func() bool { return true }), WithFileStore(filepath.Join(dir, "files"), 1<<30, false))
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	seed := filepath.Join(dir, "original.db")
	q := open(seed)
	defer func() {
		if q != nil {
			q.Close()
		}
	}()
	bundleHistory(t, q, 32<<10)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	var beforeBytes int64
	if err := q.db.QueryRow("SELECT SUM(length(data)) FROM queue_state").Scan(&beforeBytes); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(dir, "copy.db")
	// SQLite's own verified snapshot path; migrate a copy, never a live queue.
	if _, err := q.db.Exec("VACUUM INTO ?", copyPath); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = nil
	runtime.GC()
	q = open(copyPath)
	afterTime, afterAlloc := mutationCost(t, q, 5)
	var afterBytes int64
	if err := q.db.QueryRow("SELECT SUM(length(data)) FROM queue_state").Scan(&afterBytes); err != nil {
		t.Fatal(err)
	}
	t.Logf("retained fragments: %d -> %d bytes; warmed no-op after migration: %s / %d B per mutation", beforeBytes, afterBytes, afterTime, afterAlloc)
	// The full diagnostic measured 7.4 MB/op after migration (27.8 MB under
	// race) versus 820 MB embedded (2.30 GB under race). This scaled fixture
	// measured 3.24 MB/op under race; 4 MiB allows allocator/metadata variation
	// while rejecting bundle copies. Wall timing is reported separately.
	if afterAlloc > 4<<20 {
		t.Errorf("mutation still copies bundle volume: %d B/op, embedded volume %d", afterAlloc, beforeBytes)
	}
	if afterBytes > beforeBytes/16 {
		t.Errorf("bundle bytes remain in mutable state: %d -> %d", beforeBytes, afterBytes)
	}
	first := q.s.Jobs["retained-000"]
	replay := submit(t, q, first.Spec)
	if replay.ID != first.ID || len(replay.Extensions[0].Files["payload"]) != 32<<10 {
		t.Fatal("keyed answer lost bundle after migration")
	}
	frozen, err := q.extensionByDigest(first.Extensions[0].Digest)
	if err != nil || !reflect.DeepEqual(frozen.Files, replay.Extensions[0].Files) {
		t.Fatal("frozen digest roundtrip", err)
	}
	persisted := q.persisted
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = nil
	q = open(copyPath)
	if !reflect.DeepEqual(persisted, q.persisted) {
		t.Fatal("second startup changed migrated fragments")
	}
	if len(q.bundleReferences()) != 13 {
		t.Fatalf("deduplication: %d bundles", len(q.bundleReferences()))
	}
	entries, err := os.ReadDir(filepath.Join(q.files.root, "blobs"))
	if err != nil || len(entries) != 13 {
		t.Fatalf("store has %d entries: %v", len(entries), err)
	}
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
	old := q.persisted
	if _, err := q.db.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	// Simulate interruption after all blobs are durable, before metadata commit.
	if err := q.migrateBundleFiles(); err == nil {
		t.Fatal("migration unexpectedly committed")
	}
	for key, want := range old {
		var got []byte
		if err := q.db.QueryRow("SELECT data FROM queue_state WHERE key=?", key).Scan(&got); err != nil || !bytes.Equal(want, got) {
			t.Fatal("failed migration changed durable state", err)
		}
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
	if stored := reopened.s.Extensions[extensionKey(e)]; len(stored.Files) != 0 || len(stored.Variants[0].Environments["main"].Files) != 0 {
		t.Fatal("hydration put bytes back into queue state")
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
	clock.Advance(DefaultFileRetention + time.Nanosecond)
	if err := reopened.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.files.verify(context.Background(), ref); err != nil {
		t.Fatal("retained answer bundle collected", err)
	}
}

func TestExtensionBundleStoreDevSidecarControlTraffic(t *testing.T) {
	e := compositionExtension(t, false)
	e.Version = "sidecar"
	e.Kinds = nil
	e.Variants[0].Needs = ResourceNeeds{Memory: "auto"}
	delete(e.Variants[0].Baselines, "kind:sample")
	root := t.TempDir()
	e.Variants[0].Execution.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sh", "-c", `printf helper > "$MYCE_SCRATCH_DIR/helper"; exec sleep 60`}}}
	e.Variants[0].Check = []string{"sh", "{extension}/check.sh"}
	e.Files["check.sh"] = []byte(`test -z "$MYCE_TOKEN" || exit 99
while test ! -f "$MYCE_SCRATCH_DIR/helper"; do sleep .01; done
printf main >> "$MYCE_CHECKPOINT_DIR/main"
cat "$MYCE_CHECK_INPUT" > "$MYCE_CHECK_OUTPUT"
`)
	e = freezeComposition(t, e)
	extensionRoot := t.TempDir()
	if err := InstallExtension(extensionRoot, e); err != nil {
		t.Fatal(err)
	}
	d, err := startDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: extensionRoot, HostID: "bundle-check-host", Token: "bundle-fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}, func(q *Queue) {
		bundleHistory(t, q, 32<<10)
		if err := q.migrateBundleFiles(); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	await(t, func() bool { snap, _ := d.Queue.Snapshot(); return len(snap.Machines) > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	trafficCtx, stopTraffic := context.WithCancel(ctx)
	type trafficResult struct {
		ticks, reads int
		errs         []error
	}
	done := make(chan trafficResult, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var result trafficResult
		for {
			select {
			case <-trafficCtx.Done():
				done <- result
				return
			case <-ticker.C:
				if err := d.Queue.Tick(); err != nil {
					result.errs = append(result.errs, err)
				}
				result.ticks++
				var snap Snapshot
				if err := d.Client().Call(trafficCtx, "GET", "/api/status", nil, &snap); err != nil && trafficCtx.Err() == nil {
					result.errs = append(result.errs, err)
				}
				result.reads++
			}
		}
	}()
	defer stopTraffic()
	cache := &EnvironmentCache{Root: filepath.Join(root, "check-cache"), Limit: 4 << 20}
	reports, checkErr := CheckExtension(ctx, extensionRoot, e, "fake", "0", "bundle-check-host", io.Discard, cache, d.Client())
	stopTraffic()
	traffic := <-done
	if checkErr != nil || len(reports) != 1 || !reports[0].Qualified {
		t.Errorf("runtime check: reports=%+v error=%v", reports, checkErr)
	}
	if len(traffic.errs) > 0 || traffic.ticks == 0 || traffic.reads == 0 {
		t.Errorf("control traffic: %+v", traffic)
	}
	snap, snapErr := d.Queue.Snapshot()
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	checks := 0
	for _, j := range snap.Jobs {
		if j.Spec.ExtensionCheck == nil {
			continue
		}
		checks++
		// Preview checks use the queue's local checkpoint assignment. Unlike
		// work/output snapshots, this ledger survives publication and replay.
		mainMarker := filepath.Join(attemptCheckpoint(*j), "main")
		ran, readErr := os.ReadFile(mainMarker)
		attempts, err := os.ReadDir(j.Checkpoint)
		readErr = errors.Join(readErr, err)
		for _, attempt := range attempts {
			dir := filepath.Join(j.Checkpoint, attempt.Name())
			if !attempt.IsDir() || !strings.HasPrefix(attempt.Name(), "attempt-") || dir == filepath.Dir(mainMarker) {
				continue
			}
			other, err := os.ReadFile(filepath.Join(dir, "main"))
			if os.IsNotExist(err) {
				continue // An attempt stopped before main may have no append.
			}
			readErr = errors.Join(readErr, err)
			ran = append(ran, other...)
		}
		if readErr != nil || string(ran) != "main" {
			t.Errorf("main did not execute exactly once: %q %v", ran, readErr)
		}
		if j.State != "succeeded" || j.Attempt != 1 || j.Starts != 1 || j.Losses != 0 || !j.ReleaseConfirmed {
			t.Errorf("check outcome: state=%s attempts=%d starts=%d losses=%d release=%v", j.State, j.Attempt, j.Starts, j.Losses, j.ReleaseConfirmed)
		}
		for _, copy := range j.Copies {
			if !copy.ReleaseConfirmed {
				t.Errorf("copy not released: %+v", copy)
			}
		}
	}
	if checks != 1 {
		t.Errorf("runtime checks=%d", checks)
	}
	t.Logf("real sidecar check: %d check, %d concurrent ticks, %d status reads; worker checkins every 10ms", checks, traffic.ticks, traffic.reads)
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
}

func TestExtensionBundleStoreRepairsCorruptBlobFromEmbeddedCopy(t *testing.T) {
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
	stored := reopened.s.Extensions[extensionKey(e)]
	if len(stored.Files) != 0 || stored.FilesRef == nil || *stored.FilesRef != ref {
		t.Fatal("startup did not commit the repaired reference")
	}
}
