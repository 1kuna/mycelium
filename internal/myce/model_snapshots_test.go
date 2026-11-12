package myce

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	tinyRevision  = "0123456789abcdef0123456789abcdef01234567"
	tinyWeights   = "synthetic weights"
	tinyConfig    = `{"layers": 1}`
	otherRevision = "fedcba9876543210fedcba9876543210fedcba98"
)

// hubRepo writes a small Hugging Face hub-cache repository: snapshot files are
// relative links into blobs/, named by SHA-256 (LFS) or Git blob SHA-1.
func hubRepo(t *testing.T, modelRoot, repo, revision string, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(modelRoot, "hub", "models--"+strings.ReplaceAll(repo, "/", "--"))
	snapshot := filepath.Join(dir, "snapshots", revision)
	for _, path := range []string{filepath.Join(dir, "blobs"), snapshot, filepath.Join(dir, "refs")} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range files {
		var blob string
		if strings.HasSuffix(name, ".json") {
			sum := sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(content), content)))
			blob = hex.EncodeToString(sum[:])
		} else {
			sum := sha256.Sum256([]byte(content))
			blob = hex.EncodeToString(sum[:])
		}
		if err := os.WriteFile(filepath.Join(dir, "blobs", blob), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", "..", "blobs", blob), filepath.Join(snapshot, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "refs", "main"), []byte(revision), 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func tinyModel(t *testing.T, modelRoot string) string {
	return hubRepo(t, modelRoot, "org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig})
}

func setModelPolicy(t *testing.T, ctx context.Context, c Client, models *ModelPolicy) {
	t.Helper()
	var record PriorityPolicyRecord
	if err := c.Call(ctx, "GET", "/api/policy", nil, &record); err != nil {
		t.Fatal(err)
	}
	record.Policy.Models = models
	request := struct {
		ExpectedRevision uint64         `json:"expected_revision"`
		Policy           PriorityPolicy `json:"policy"`
	}{record.Revision, record.Policy}
	if err := c.Call(ctx, "PUT", "/api/policy", request, &record); err != nil {
		t.Fatal(err)
	}
}

func awaitTerminal(t *testing.T, ctx context.Context, c Client, id string) Job {
	t.Helper()
	for {
		j, err := c.GetJob(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(j.State) {
			return *j
		}
		select {
		case <-ctx.Done():
			t.Fatal("job did not finish", id, j.State, j.Reason)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestModelSnapshotsPolicyDefaultsAndRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  *ModelPolicy
		allowed bool
	}{
		{"absent", nil, true},
		{"empty", &ModelPolicy{}, true},
		{"limits only", &ModelPolicy{MaxBytes: 1024, FreeFloorBytes: 512}, true},
		{"matching restriction", &ModelPolicy{Sources: []string{"org/*"}}, true},
		{"nonmatching restriction", &ModelPolicy{Sources: []string{"other/*"}}, false},
		{"deny match", &ModelPolicy{Sources: []string{"!org/*"}}, false},
		{"deny other", &ModelPolicy{Sources: []string{"!other/*"}}, true},
		{"deny overrides permit", &ModelPolicy{Sources: []string{"*/*", "!org/tiny"}}, false},
		{"deny order independent", &ModelPolicy{Sources: []string{"!org/tiny", "org/*"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			policy := q.PriorityPolicy()
			policy.Policy.Models = tc.policy
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.ModelSnapshots = snapshotJob("org/tiny").Spec.ModelSnapshots
			job, err := q.Submit(s)
			if !tc.allowed {
				if err == nil || !strings.Contains(err.Error(), "not permitted") {
					t.Fatalf("explicit restriction ignored: %v", err)
				}
				return
			}
			if err != nil || job.ModelLimits == nil {
				t.Fatalf("default permission not frozen: job=%+v err=%v", job, err)
			}
			want := ModelLimits{PolicyRevision: 1}
			if tc.policy != nil {
				want.MaxBytes, want.FreeFloorBytes = tc.policy.MaxBytes, tc.policy.FreeFloorBytes
			}
			if *job.ModelLimits != want {
				t.Fatalf("limits=%+v want=%+v", job.ModelLimits, want)
			}
			policy = q.PriorityPolicy()
			policy.Policy.Models = &ModelPolicy{Sources: []string{"!org/*"}, MaxBytes: 1, FreeFloorBytes: 1}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			saved, err := q.jobSnapshot(job.ID)
			if err != nil || saved.ModelLimits == nil || *saved.ModelLimits != want {
				t.Fatalf("accepted limits changed with policy: %+v %v", saved, err)
			}
		})
	}
	for _, p := range []ModelPolicy{
		{MaxBytes: -1}, {FreeFloorBytes: -1},
		{Sources: []string{"!org/["}}, {Sources: []string{"org"}},
		{Sources: []string{"!"}}, {Sources: []string{"/name"}},
	} {
		if err := p.validate(); err == nil {
			t.Fatalf("invalid policy accepted: %+v", p)
		}
	}
}

func TestModelSnapshotsPhysicalLimits(t *testing.T) {
	originalProbe := workerDiskProbe
	t.Cleanup(func() { workerDiskProbe = originalProbe })
	const total = uint64(100 << 30)
	for _, tc := range []struct {
		name   string
		limits ModelLimits
		free   uint64
		wantOK bool
	}{
		{"derived floor fits", ModelLimits{}, diskFreeFloor(total) + uint64(len(tinyWeights)+len(tinyConfig)), true},
		{"derived floor refuses", ModelLimits{}, diskFreeFloor(total), false},
		{"explicit floor refuses", ModelLimits{FreeFloorBytes: 2 << 30}, 2 << 30, false},
		{"size limit refuses", ModelLimits{MaxBytes: 1}, total, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workerDiskProbe = func(string) (DiskReading, error) {
				return DiskReading{TotalBytes: total, FreeBytes: tc.free}, nil
			}
			root := t.TempDir()
			tinyModel(t, root)
			w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
			job := snapshotJob("org/tiny")
			job.ModelLimits = &tc.limits
			_, releases, err := w.acquireModelSnapshots(context.Background(), job)
			for _, release := range releases {
				release()
			}
			if tc.wantOK {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(runtimeDiagnostic(err).String(), "model_snapshot_too_large") {
				t.Fatalf("physical protection ignored: %v", err)
			}
		})
	}
	t.Run("derived floor permits inactive cache eviction", func(t *testing.T) {
		workerDiskProbe = originalProbe
		root := t.TempDir()
		tinyModel(t, root)
		hubRepo(t, root, "org/other", tinyRevision, map[string]string{"model.safetensors": "other weights"})
		w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
		paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
		if err != nil {
			t.Fatal(err)
		}
		for _, release := range releases {
			release()
		}
		victim := paths["MYCE_MODEL_BASE"]
		workerDiskProbe = func(string) (DiskReading, error) {
			free := diskFreeFloor(total)
			if _, err := os.Stat(victim); os.IsNotExist(err) {
				free += 4096 // Removing the inactive snapshot frees enough for the new one.
			}
			return DiskReading{TotalBytes: total, FreeBytes: free}, nil
		}
		_, releases, err = w.acquireModelSnapshots(context.Background(), snapshotJob("org/other"))
		for _, release := range releases {
			release()
		}
		if err != nil {
			t.Fatal("derived floor refused before inactive cache eviction:", err)
		}
		if _, err := os.Stat(victim); !os.IsNotExist(err) {
			t.Fatalf("inactive snapshot was not evicted: %v", err)
		}
	})
}

func TestModelSnapshotsNativeDevJourney(t *testing.T) {
	state := t.TempDir()
	if err := PrepareStateRoots(state); err != nil {
		t.Fatal(err)
	}
	modelRoot := filepath.Join(state, "models")
	repo := tinyModel(t, modelRoot)
	// A second revision of the same repository exists; the declared one does not.
	hubRepo(t, modelRoot, "org/moved", otherRevision, map[string]string{"model.safetensors": tinyWeights})
	outside := filepath.Join(modelRoot, "private.txt")
	if err := os.WriteFile(outside, []byte("not a model blob"), 0600); err != nil {
		t.Fatal(err)
	}
	escape := hubRepo(t, modelRoot, "org/escape", tinyRevision, map[string]string{"config.json": tinyConfig})
	if err := os.Symlink(outside, filepath.Join(escape, "snapshots", tinyRevision, "extra.txt")); err != nil {
		t.Fatal(err)
	}
	changed := hubRepo(t, modelRoot, "org/changed", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
	blob, err := filepath.EvalSymlinks(filepath.Join(changed, "snapshots", tinyRevision, "model.safetensors"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("tampered weights!"), 0644); err != nil {
		t.Fatal(err)
	}
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), ModelRoot: modelRoot, GPU: "cpu", Workers: 1, Token: "test", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	t.Cleanup(func() {
		_ = removeSharedEntry(filepath.Join(state, "workers", "fake-1", "file-cache", "model-snapshots"))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := d.Client()
	declare := func(repo, revision, command string, files ...string) Spec {
		return Spec{Name: "model", Project: "test", Run: RunSpec{Command: []string{"sh", "-ec", command}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, ModelSnapshots: map[string]ModelSnapshot{"base": {Repo: repo, Revision: revision, Files: files}}, Outputs: map[string]string{"result": "results"}}
	}
	reads := `test "$(cat "$MYCE_MODEL_BASE/model.safetensors")" = "` + tinyWeights + `"
test "$(cat "$MYCE_MODEL_BASE/config.json")" = '` + tinyConfig + `'
if (printf changed > "$MYCE_MODEL_BASE/model.safetensors") 2>/dev/null; then exit 21; fi
if touch "$MYCE_MODEL_BASE/new" 2>/dev/null; then exit 22; fi
printf '%s' "$MYCE_MODEL_BASE" > result`

	var resolved ResolveReport
	if err := c.Call(ctx, "POST", "/api/resolve", ResolveRequest{Spec: declare("org/tiny", tinyRevision, reads)}, &resolved); err != nil {
		t.Fatal(err)
	}
	for _, finding := range resolved.Findings {
		if finding.Field == "model_snapshots" {
			t.Fatalf("resolve requires model approval by default: %+v", finding)
		}
	}
	if _, err := c.Submit(ctx, declare("org/tiny", "main", reads)); err == nil || !strings.Contains(err.Error(), "full 40-character lowercase commit") {
		t.Fatal("movable revision accepted", err)
	}

	t.Run("read-only snapshot", func(t *testing.T) {
		j, err := c.Submit(ctx, declare("org/tiny", tinyRevision, reads))
		if err != nil {
			t.Fatal(err)
		}
		if got := awaitTerminal(t, ctx, c, j.ID); got.State != "succeeded" {
			t.Fatal(got.State, got.Reason)
		}
		out := filepath.Join(t.TempDir(), "result")
		if err := c.DownloadResult(ctx, j.ID, "result", out); err != nil {
			t.Fatal(err)
		}
		path, _ := os.ReadFile(out)
		if strings.HasPrefix(string(path), repo) {
			t.Fatal("job received the shared hub instead of a verified copy", string(path))
		}
		if data, _ := os.ReadFile(filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")); string(data) != tinyWeights {
			t.Fatal("shared hub changed", string(data))
		}
	})
	t.Run("allow-listed files only", func(t *testing.T) {
		j, err := c.Submit(ctx, declare("org/tiny", tinyRevision, `test -f "$MYCE_MODEL_BASE/config.json"; test ! -e "$MYCE_MODEL_BASE/model.safetensors"; : > result`, "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		if got := awaitTerminal(t, ctx, c, j.ID); got.State != "succeeded" {
			t.Fatal(got.State, got.Reason)
		}
	})
	for _, refused := range []struct{ name, repo, revision, want string }{
		{"wrong revision", "org/moved", tinyRevision, "has this repository but not the declared revision"},
		{"missing repository", "org/absent", tinyRevision, "acquire it with a CPU download job"},
		{"symlink escape", "org/escape", tinyRevision, "links outside its repository blob store"},
		{"changed blob", "org/changed", tinyRevision, "failed size or hash verification"},
	} {
		t.Run(refused.name, func(t *testing.T) {
			j, err := c.Submit(ctx, declare(refused.repo, refused.revision, `: > launched; : > result`))
			if err != nil {
				t.Fatal(err)
			}
			got := awaitTerminal(t, ctx, c, j.ID)
			if got.State != "failed" || !strings.Contains(got.Reason, refused.want) || strings.Contains(got.Reason, modelRoot) || got.StartedAt != nil {
				t.Fatalf("state=%s reason=%q started=%v", got.State, got.Reason, got.StartedAt)
			}
		})
	}
	setModelPolicy(t, ctx, c, &ModelPolicy{Sources: []string{"org/*"}, MaxBytes: 1 << 20})
	if _, err := c.Submit(ctx, declare("elsewhere/tiny", tinyRevision, reads)); err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatal("explicitly restricted source accepted", err)
	}
	if err := c.Call(ctx, "POST", "/api/resolve", ResolveRequest{Spec: declare("elsewhere/tiny", tinyRevision, reads)}, &resolved); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, finding := range resolved.Findings {
		found = found || finding.Field == "model_snapshots"
	}
	if !found {
		t.Fatalf("resolve hid explicit model restriction: %+v", resolved.Findings)
	}

	if data, _ := os.ReadFile(outside); string(data) != "not a model blob" {
		t.Fatal("escape target changed")
	}
}

// snapshotWorker wires the worker's real file-transfer caches: a tiny file
// cache and the default model-sized snapshot budget.
func snapshotWorker(t *testing.T, modelRoot string, clock Clock) *Worker {
	t.Helper()
	w := &Worker{ModelRoot: modelRoot, FileCacheRoot: filepath.Join(t.TempDir(), "file-cache"), FileCacheLimit: 16, Clock: clock}
	if err := w.initFileTransfers(context.Background(), &workerStore{dir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeSharedEntry(w.modelSnapshots.cache.Root) })
	return w
}

func snapshotJob(repo string) Job {
	return Job{Spec: Spec{ModelSnapshots: map[string]ModelSnapshot{"base": {Repo: repo, Revision: tinyRevision}}}, ModelLimits: &ModelLimits{}}
}

func TestModelSnapshotBudgetExceedsFileCache(t *testing.T) {
	modelRoot := t.TempDir()
	tinyModel(t, modelRoot)
	w := snapshotWorker(t, modelRoot, &fakeClock{now: time.Unix(1000, 0)})
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	if err != nil {
		t.Fatal("snapshot larger than the 16-byte file cache was refused:", err)
	}
	for _, release := range releases {
		release()
	}
	if data, _ := os.ReadFile(filepath.Join(paths["MYCE_MODEL_BASE"], "model.safetensors")); string(data) != tinyWeights {
		t.Fatalf("cached view %q", data)
	}
}

func TestModelSnapshotSameVolumeClonesAndIgnoresSourceWrites(t *testing.T) {
	modelRoot := t.TempDir()
	weights := strings.Repeat("w", 64<<20)
	repo := hubRepo(t, modelRoot, "org/large", tinyRevision, map[string]string{"model.safetensors": weights})
	blob, err := filepath.EvalSymlinks(filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors"))
	if err != nil {
		t.Fatal(err)
	}
	w := snapshotWorker(t, modelRoot, &fakeClock{now: time.Unix(1000, 0)})
	probe, err := os.Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	clones := cloneSharedFile(probe, filepath.Join(t.TempDir(), "probe"))
	probe.Close()
	if err := PrepareStateRoots(w.modelSnapshots.cache.Root); err != nil {
		t.Fatal(err)
	}
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/large"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	cached := filepath.Join(paths["MYCE_MODEL_BASE"], "model.safetensors")
	if clones {
		// Inspect these files' shared extents instead of free space on a
		// volume used concurrently by unrelated tests and processes.
		assertSharedFileClone(t, blob, cached)
	} else {
		t.Log("volume cannot clone; the snapshot was copied")
	}
	// A download job rewriting the hub blob in place must not reach the view.
	f, err := os.OpenFile(blob, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("tampered"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if data, _ := os.ReadFile(cached); string(data) != weights {
		t.Fatal("source write changed the verified snapshot view")
	}
	again, more, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/large"))
	releases = append(releases, more...)
	if err != nil || again["MYCE_MODEL_BASE"] != paths["MYCE_MODEL_BASE"] {
		t.Fatal("verified view not reused", err)
	}
}

func TestModelSnapshotRestartSkipsUnchangedRehash(t *testing.T) {
	modelRoot := t.TempDir()
	tinyModel(t, modelRoot)
	clock := &fakeClock{now: time.Unix(1000, 0)}
	w := snapshotWorker(t, modelRoot, clock)
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	if err != nil {
		t.Fatal(err)
	}
	releases[0]()
	// A restarted worker has a fresh cache object over the same directory.
	restart := func() int {
		rehashes := 0
		w.modelSnapshots = &SharedInputCache{cache: EnvironmentCache{Root: w.modelSnapshots.cache.Root, Limit: w.modelSnapshots.cache.Limit, Clock: clock}, beforeVerify: func() { rehashes++ }}
		_, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
		if err != nil {
			t.Fatal(err)
		}
		releases[0]()
		return rehashes
	}
	if n := restart(); n != 0 {
		t.Fatalf("restart re-hashed unchanged snapshot %d times", n)
	}
	clock.Advance(time.Hour)
	if err := os.Chtimes(filepath.Join(paths["MYCE_MODEL_BASE"], "config.json"), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	if n := restart(); n != 1 {
		t.Fatalf("restart after a changed mtime re-hashed %d times, want 1", n)
	}
	if n := restart(); n != 0 {
		t.Fatalf("re-verified snapshot re-hashed %d times after restart", n)
	}
}

func TestModelSnapshotActiveCustodySurvivesRestart(t *testing.T) {
	if root := os.Getenv("MYCE_SNAPSHOT_CUSTODY_HELPER"); root != "" {
		clock := &fakeClock{now: time.Unix(1000, 0)}
		cache := &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 1 << 20, Clock: clock}}
		w := Worker{ModelRoot: filepath.Join(root, "models"), modelSnapshots: cache}
		owner, err := newAttemptStore(&workerStore{dir: filepath.Join(root, "attempt")}, Job{ID: "active", Attempt: 1, Session: "session"})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = w.acquireModelSnapshots(context.WithValue(context.Background(), sharedInputOwnerKey{}, owner), snapshotJob("org/tiny"))
		if err != nil {
			t.Fatal(err)
		}
		// Exit without releasing: the OS drops every flock, as on worker death.
		os.Exit(0)
	}
	root := t.TempDir()
	t.Cleanup(func() { _ = removeSharedEntry(root) })
	modelRoot := filepath.Join(root, "models")
	tinyModel(t, modelRoot)
	if err := os.Mkdir(filepath.Join(root, "attempt"), 0700); err != nil {
		t.Fatal(err)
	}
	helper := exec.Command(os.Args[0], "-test.run=^TestModelSnapshotActiveCustodySurvivesRestart$")
	helper.Env = append(os.Environ(), "MYCE_SNAPSHOT_CUSTODY_HELPER="+root)
	if b, err := helper.CombinedOutput(); err != nil {
		t.Fatalf("snapshot helper: %v: %s", err, b)
	}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	clock.Advance(time.Hour)
	rehashes := 0
	cache := &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 1, Clock: clock}, beforeVerify: func() { rehashes++ }}
	if err := cache.cache.init(); err != nil {
		t.Fatal(err)
	}
	input, repo, err := resolveModelSnapshot(context.Background(), modelRoot, snapshotJob("org/tiny").Spec.ModelSnapshots["base"])
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cache.cache.Root, input.key(), "data", "config.json")
	if err := cache.admit(context.Background(), "other", 10); err == nil {
		t.Fatal("restart evicted an active snapshot")
	}
	if err := os.Chtimes(path, clock.Now(), clock.Now()); err != nil {
		t.Fatal("active snapshot removed", err)
	}
	if _, release, err := cache.Acquire(context.Background(), input, repo); err == nil {
		release()
		t.Fatal("changed active snapshot accepted")
	}
	if rehashes != 0 {
		t.Fatal("active snapshot reverified after restart")
	}
	// Another attempt in the real shared spool cannot release the active owner.
	sibling, err := newAttemptStore(&workerStore{dir: filepath.Join(root, "attempt")}, Job{ID: "sibling", Attempt: 1, Session: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sibling.pinSharedInput(cache.cache.Root, input.key()); err != nil {
		t.Fatal(err)
	}
	// Recover the interrupted atomic publication used by older pin writers too.
	temporary := sharedInputPin(cache.cache.Root, input.key(), sibling.sharedInputOwnerID()) + ".tmp"
	if err := os.WriteFile(temporary, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := sibling.cleanupOrphan(clock); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temporary); !os.IsNotExist(err) {
		t.Fatal("interrupted owner pin leaked", err)
	}
	if pinned, err := sharedInputPinned(cache.cache.Root, input.key()); err != nil || !pinned {
		t.Fatal("sibling cleanup lost active pin", err)
	}
	states, err := filepath.Glob(filepath.Join(root, "attempt", "state-attempt-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	var activeName string
	for _, state := range states {
		b, err := os.ReadFile(state)
		if err != nil {
			t.Fatal(err)
		}
		var saved workerState
		if err := decodeWorkerState(b, &saved); err != nil {
			t.Fatal(err)
		}
		if saved.Job.ID == "active" {
			activeName = filepath.Base(state)
		}
	}
	if activeName == "" {
		t.Fatal("active attempt missing")
	}
	// Exact cleanup failure retains the pin; successful cleanup removes it.
	cgroup := filepath.Join(root, "attempt-snapshot")
	if err := (fakeMemoryFS{}).Mkdir(cgroup); err != nil {
		t.Fatal(err)
	}
	owner := &workerStore{dir: filepath.Join(root, "attempt"), stateName: activeName, state: workerState{Identity: "snapshot", Cgroup: cgroup, Job: &Job{}, PID: 1}}
	// Group custody avoids signalling any real process in this small fixture.
	if err := saveWorkerJSON(owner.dir, "attempt-snapshot.json", workerState{Identity: "snapshot", PID: 1, Cgroup: cgroup}); err != nil {
		t.Fatal(err)
	}
	if err := owner.cleanupOrphan(clock, sharedCleanupDeniedFS{}); err == nil {
		t.Fatal("cleanup failure ignored")
	}
	if pinned, err := sharedInputPinned(cache.cache.Root, input.key()); err != nil || !pinned {
		t.Fatal("cleanup failure lost pin", err)
	}
	if err := owner.cleanupOrphan(clock, fakeMemoryFS{}); err != nil {
		t.Fatal(err)
	}
	_ = cache.admit(context.Background(), "other", 10)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("cleaned snapshot not evicted", err)
	}
}

func TestModelSnapshotPinCleanupPrecedesAttemptRetirement(t *testing.T) {
	h := &machineHarness{}
	w := harnessWorker(t, h)
	w.control = &processControl{}
	w.Clock = &fakeClock{now: time.Unix(1000, 0)}
	j := harnessJob(t, "pin-cleanup", "", "true")
	root := &workerStore{dir: t.TempDir()}
	s, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	cacheRoot, key := t.TempDir(), strings.Repeat("a", 64)
	if _, err := s.pinSharedInput(cacheRoot, key); err != nil {
		t.Fatal(err)
	}
	pin := sharedInputPin(cacheRoot, key, s.sharedInputOwnerID())
	if err := os.Remove(pin); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(pin, 0700); err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(pin, "blocked")
	if err := os.WriteFile(block, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s.state.Result = &Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: true}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	a := &attemptSupervisor{store: s, job: j, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.run(ctx)
	await(t, func() bool {
		_, d, _, _ := a.snapshot()
		return strings.Contains(d.FileTransferError, "cache pin cleanup pending")
	})
	data, err := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err != nil {
		t.Fatal(err)
	}
	var saved workerState
	if err := decodeWorkerState(data, &saved); err != nil || saved.Job == nil {
		t.Fatal("owner retired before unpin", err)
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.done:
	case <-time.After(testWaitTimeout):
		t.Fatal("unpin did not retry")
	}
	data, err = os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err != nil {
		t.Fatal(err)
	}
	saved = workerState{}
	if err := decodeWorkerState(data, &saved); err != nil || saved.Job != nil {
		t.Fatal("owner not retired after unpin", err)
	}
}
