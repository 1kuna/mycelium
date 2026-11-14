package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func ephemeralSpec() Spec {
	s := spec("experiment")
	s.Resumable = false
	s.WorkerData = "ephemeral"
	s.Outputs = map[string]string{"out.txt": "results"}
	return s
}
func ephemeralMachine() Machine {
	m := machine("worker", "fake")
	m.Capabilities = append(m.Capabilities, ephemeralCapability, nativeCICapability, "native_isolation_namespace")
	return m
}
func ephemeralFixture(t *testing.T) (*Queue, *fakeClock, Worker, *workerStore) {
	t.Helper()
	q, c := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := ephemeralMachine()
	check(t, q, m, nil)
	j := submit(t, q, ephemeralSpec())
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("no ephemeral assignment", job(t, q, j.ID))
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(api.Close)
	w := Worker{StateDir: newTestStateRoot(t), Machine: m, Clock: c, Client: Client{URL: api.URL, Token: "test"}, ephemeralProbe: func() error { return nil }}
	root := &workerStore{dir: w.StateDir, state: workerState{Session: m.Session}}
	if err := root.save(); err != nil {
		t.Fatal(err)
	}
	store, err := newAttemptStore(root, *assigned)
	if err != nil {
		t.Fatal(err)
	}
	r := Result{Machine: assigned.Machine, Session: assigned.Session, Execution: Execution{ID: j.ID, Attempt: assigned.Attempt}, Success: true}
	store.state.Result = &r
	for _, p := range []string{"work/input", "work/.tmp/temp", "work/.home/private", "work/.scratch/scratch", "checkpoint/save", "shared/data", "work/out.txt"} {
		path := filepath.Join(ephemeralRoot(store.dir, *assigned), p)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("sentinel"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	store.state.Log = ephemeralLogName(*assigned)
	logPath := filepath.Join(store.dir, store.state.Log)
	if err := os.MkdirAll(filepath.Dir(logPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("sentinel log\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	return q, c, w, store
}

type faultWorkerDataFS struct {
	osWorkerDataFS
	fail         bool
	beforeRemove func(string)
}

func (f *faultWorkerDataFS) RemoveAll(p string) error {
	if f.beforeRemove != nil {
		f.beforeRemove(p)
	}
	if f.fail {
		return os.ErrPermission
	}
	return f.osWorkerDataFS.RemoveAll(p)
}

func TestEphemeralAcceptanceCleanupAndReceipt(t *testing.T) {
	q, c, w, s := ephemeralFixture(t)
	j := *s.state.Job
	fault := &faultWorkerDataFS{beforeRemove: func(path string) {
		got := job(t, q, j.ID)
		if !s.state.WorkerDataAccepted || got.State != "succeeded" || got.ReleaseConfirmed {
			t.Fatalf("unlink before durable acceptance or after release: %s %+v", path, got)
		}
	}}
	w.cleanupFS = fault
	done, err := w.finishEphemeralAttempt(context.Background(), s, true)
	if err != nil || !done {
		t.Fatal(done, err)
	}
	got := job(t, q, j.ID)
	if !got.ReleaseConfirmed || !cleanupComplete(got.WorkerDataCleanup) || got.WorkerDataCleanup.CheckedAt != c.Now() {
		t.Fatalf("missing verified receipt: %+v", got.WorkerDataCleanup)
	}
	if len(got.AttemptHistory) != 1 || !cleanupComplete(got.AttemptHistory[0].WorkerDataCleanup) {
		t.Fatal("missing attempt history receipt", got.AttemptHistory)
	}
	for _, p := range got.WorkerDataCleanup.Paths {
		if _, err := os.Lstat(filepath.Join(w.StateDir, p.Path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("payload survives release", p, err)
		}
		if p.Bytes <= 0 {
			t.Fatal("missing removal byte census", p)
		}
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) == 0 || !strings.Contains(logs[0].Text, "sentinel log") {
		t.Fatal("logs lost", logs, err)
	}
	dest := filepath.Join(t.TempDir(), "output")
	if err := w.Client.DownloadFile(context.Background(), got.OutputRefs["out.txt"], dest); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(dest); string(data) != "sentinel" {
		t.Fatal("durable output changed", string(data))
	}
}

func TestEphemeralTransferOutageLostAckAndUnlinkFailure(t *testing.T) {
	q, _, w, s := ephemeralFixture(t)
	j := *s.state.Job
	real := http.DefaultTransport
	failUpload, lostAck := true, true
	w.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if failUpload && strings.Contains(r.URL.Path, "/api/files/") && r.Method != "GET" {
			return nil, errors.New("transfer unavailable")
		}
		response, err := real.RoundTrip(r)
		if err == nil && lostAck && r.URL.Path == "/api/workers/complete" && response.StatusCode == 200 {
			lostAck = false
			response.Body.Close()
			return nil, errors.New("lost acceptance reply")
		}
		return response, err
	})}
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err == nil || done {
		t.Fatal("outage accepted", done, err)
	}
	for _, p := range []string{ephemeralRoot(s.dir, j), filepath.Join(s.dir, ephemeralLogName(j))} {
		if _, err := os.Lstat(p); err != nil {
			t.Fatal("bytes removed during outage", err)
		}
	}
	failUpload = false
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err == nil || done {
		t.Fatal("lost reply was ignored", done, err)
	}
	if _, err := os.Stat(filepath.Join(ephemeralRoot(s.dir, j), "work/out.txt")); err != nil {
		t.Fatal("lost ack destroyed sole local copy", err)
	}
	// Reopen saved state just as startup does, without the volatile acknowledgement.
	data, err := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err != nil {
		t.Fatal(err)
	}
	var saved workerState
	if err := decodeWorkerState(data, &saved); err != nil {
		t.Fatal(err)
	}
	s.state = saved
	denied := &faultWorkerDataFS{fail: true}
	w.cleanupFS = denied
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err == nil || done {
		t.Fatal("unlink failure hidden", done, err)
	}
	got := job(t, q, j.ID)
	if got.ReleaseConfirmed || got.WorkerDataCleanup == nil || got.WorkerDataCleanup.State != "failed" || !strings.Contains(JobReason(got), "cleanup: failed") {
		t.Fatal("cleanup failure not visible", got.WorkerDataCleanup, got.ReleaseConfirmed, JobReason(got))
	}
	denied.fail = false
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err != nil || !done {
		t.Fatal("retry failed", done, err)
	}
}

func TestEphemeralFailureCancellationRequeueAndStartupRecovery(t *testing.T) {
	for _, outcome := range []string{"failure", "cancel", "requeue", "restart"} {
		t.Run(outcome, func(t *testing.T) {
			q, _, w, s := ephemeralFixture(t)
			j := *s.state.Job
			s.state.Result.Success = false
			switch outcome {
			case "cancel":
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			case "requeue":
				s.state.Result.Interrupted = true
				s.state.Result.OperatorStop = true
			case "restart":
				// No exit receipt: recovery must kill/reap custody before removing input.
				s.state.Result = nil
				if err := s.save(); err != nil {
					t.Fatal(err)
				}
				if _, err := w.recoverAttempt(s); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(ephemeralRoot(s.dir, j)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("startup retained payload", err)
				}
				if _, err := os.Stat(filepath.Join(s.dir, ephemeralLogName(j))); err != nil {
					t.Fatal("startup discarded undelivered logs", err)
				}
			}
			if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err != nil || !done {
				t.Fatal(done, err)
			}
			got := job(t, q, j.ID)
			if !cleanupComplete(got.AttemptHistory[0].WorkerDataCleanup) {
				t.Fatal("missing failed-attempt receipt", got.AttemptHistory)
			}
			if _, err := os.Lstat(ephemeralRoot(s.dir, j)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("terminal payload remains", err)
			}
		})
	}
}

func TestEphemeralStoredSharedArchiveAndWarmCacheBypass(t *testing.T) {
	_, _, w, s := ephemeralFixture(t)
	j := *s.state.Job
	_, ref := retentionUpload(t, w.Client, "new private input")
	cacheRoot := filepath.Join(w.StateDir, "file-cache")
	if err := os.MkdirAll(cacheRoot, 0700); err != nil {
		t.Fatal(err)
	}
	warm := filepath.Join(cacheRoot, ref.SHA256)
	if err := os.WriteFile(warm, []byte("corrupt warm entry must not be read"), 0600); err != nil {
		t.Fatal(err)
	}
	w.fileCache = &FileCache{Root: cacheRoot, Limit: 1 << 20, Client: w.Client, Clock: w.Clock}
	dst := filepath.Join(ephemeralRoot(s.dir, j), "private-copy")
	if err := w.copyAttemptInput(context.Background(), j, ref, dst); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(dst); string(data) != "new private input" {
		t.Fatal("warm cache served", string(data))
	}
	if data, _ := os.ReadFile(warm); string(data) != "corrupt warm entry must not be read" {
		t.Fatal("warm cache mutated", string(data))
	}
	_, cold := retentionUpload(t, w.Client, "cold private input")
	if err := w.copyAttemptInput(context.Background(), j, cold, dst+"-cold"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(cacheRoot, cold.SHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("private input populated cache", err)
	}
	nas := t.TempDir()
	input := sharedArchive(t, nas)
	j.Spec.SharedInputs = map[string]SharedInput{"runtime": input}
	w.CheckpointRoot = nas
	// Keep an ordinary consumer and its durable pin alive during private staging.
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(cacheRoot, "shared-inputs"), Limit: 1 << 20, Clock: w.Clock}, Client: w.Client}
	ordinary := &workerStore{dir: w.StateDir, state: workerState{Identity: "ordinary-consumer"}}
	warmCtx := context.WithValue(context.Background(), sharedInputOwnerKey{}, ordinary)
	warmPath, ordinaryRelease, err := w.sharedInputs.Acquire(warmCtx, input, nas)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinaryRelease()
	t.Cleanup(func() { _ = removeSharedEntry(w.sharedInputs.cache.Root) })
	evidence := w.sharedInputs.evidence()
	ctx := context.WithValue(context.Background(), sharedInputOwnerKey{}, s)
	env, release, err := w.acquireSharedInputs(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, done, err := w.acquireSharedInputs(ctx, j); err != nil {
		t.Fatal("private staging retry", err)
	} else {
		done()
	}
	if data, err := os.ReadFile(filepath.Join(env["MYCE_INPUT_RUNTIME"], "bin/run")); err != nil || string(data) != "hello" {
		t.Fatal("private archive", string(data), err)
	}
	if w.sharedInputs.evidence() != evidence || env["MYCE_INPUT_RUNTIME"] == warmPath {
		t.Fatal("private input acquired the ordinary cache")
	}
	if err := w.removeEphemeralPaths(s, false); err != nil {
		t.Fatal("read-only extracted tree not removed", err)
	}
	if data, err := os.ReadFile(filepath.Join(warmPath, "bin/run")); err != nil || string(data) != "hello" {
		t.Fatal("ordinary concurrent input was removed", err)
	}
	if pinned, err := sharedInputPinned(w.sharedInputs.cache.Root, input.key()); err != nil || !pinned {
		t.Fatal("ordinary consumer lost its pin", pinned, err)
	}
}

// Fake filesystem coverage proves verification, not just calling RemoveAll.
type fakeWorkerDataFS struct {
	files  fstest.MapFS
	leave  bool
	synced []string
}

func (f *fakeWorkerDataFS) key(p string) string                 { return strings.TrimPrefix(filepath.ToSlash(p), "/") }
func (f *fakeWorkerDataFS) Lstat(p string) (fs.FileInfo, error) { return fs.Stat(f.files, f.key(p)) }
func (f *fakeWorkerDataFS) WalkDir(p string, fn fs.WalkDirFunc) error {
	return fs.WalkDir(f.files, f.key(p), func(p string, d fs.DirEntry, e error) error { return fn("/"+p, d, e) })
}
func (f *fakeWorkerDataFS) RemoveAll(p string) error {
	if !f.leave {
		key := f.key(p)
		for name := range f.files {
			if name == key || strings.HasPrefix(name, key+"/") {
				delete(f.files, name)
			}
		}
	}
	return nil
}
func (f *fakeWorkerDataFS) SyncDir(p string) error { f.synced = append(f.synced, p); return nil }
func TestEphemeralFakeFilesystemVerificationAndReleaseFence(t *testing.T) {
	q, c, w, s := ephemeralFixture(t)
	j := *s.state.Job
	fake := &fakeWorkerDataFS{files: fstest.MapFS{}, leave: true}
	for _, p := range []string{filepath.Join(ephemeralRoot(s.dir, j), "sentinel"), filepath.Join(s.dir, ephemeralLogName(j))} {
		fake.files[fake.key(p)] = &fstest.MapFile{Data: []byte("secret"), Mode: 0600}
	}
	// Include the spool parent: SyncDir verifies the namespace update there.
	fake.files[fake.key(s.dir)] = &fstest.MapFile{Mode: fs.ModeDir | 0700}
	w.cleanupFS = fake
	s.state.WorkerDataAccepted = true
	if err := w.removeEphemeralPaths(s, true); err == nil || s.state.WorkerDataCleanup.State != "failed" {
		t.Fatal("false successful unlink was accepted", err)
	}
	r := *s.state.Result
	r.ReleaseOnly = true
	r.WorkerDataCleanup = s.state.WorkerDataCleanup
	if err := q.Complete(r); err == nil {
		t.Fatal("released with failed removal receipt")
	}
	fake.leave = false
	if err := w.removeEphemeralPaths(s, true); err != nil {
		t.Fatal(err)
	}
	if !cleanupComplete(s.state.WorkerDataCleanup) || s.state.WorkerDataCleanup.CheckedAt != c.Now() || len(fake.synced) != 2 {
		t.Fatal("verification receipt", s.state.WorkerDataCleanup, fake.synced)
	}
}

func TestEphemeralValidationAndOlderWorkerFence(t *testing.T) {
	base := ephemeralSpec()
	base.WorkerData = ""
	base.Advanced = &Advanced{WorkerData: "ephemeral"}
	if _, err := base.FoldAdvanced(); err != nil || !base.ephemeral() || base.Validate() != nil || AdvancedStatus(&base).WorkerData != "ephemeral" {
		t.Fatal("advanced policy not normalized", err)
	}
	cases := []func(*Spec){func(s *Spec) { s.Resumable = true }, func(s *Spec) { s.Run.Workdir = "/tmp/custom" }, func(s *Spec) { s.Environment.Name = "runtime" }, func(s *Spec) { s.Run.Sidecars = []NativeSidecar{{Name: "helper"}} }, func(s *Spec) { s.WorkerData = "other" }, func(s *Spec) { s.Outputs = nil }, func(s *Spec) { s.Isolation = "unsandboxed"; s.Reason = "test" }, func(s *Spec) { s.Needs.Network = true }}
	for i, mutate := range cases {
		s := ephemeralSpec()
		mutate(&s)
		err := s.Validate()
		var rejection *rejectionError
		if !errors.As(err, &rejection) || rejection.code != "job_worker_data_unsupported" {
			t.Fatal("not a typed refusal", i, err)
		}
	}
	j := Job{Spec: ephemeralSpec()}
	m := machine("old", "fake")
	if nativeNetworkFits(&j, &m) {
		t.Fatal("older worker can execute ephemeral job")
	}
	m = ephemeralMachine()
	if !nativeNetworkFits(&j, &m) {
		t.Fatal("capable worker rejected")
	}
	q, _ := fixture(t)
	if _, err := q.Submit(ephemeralSpec()); err == nil {
		t.Fatal("submission without store/capability accepted")
	}
	// Ordinary behavior keeps using the cache, proving the new path is opt-in.
	_, _, w, s := ephemeralFixture(t)
	ordinary := *s.state.Job
	ordinary.Spec.WorkerData = ""
	_, ref := retentionUpload(t, w.Client, "ordinary")
	w.fileCache = &FileCache{Root: filepath.Join(w.StateDir, "ordinary-cache"), Limit: 1 << 20, Client: w.Client, Clock: w.Clock}
	if err := w.copyAttemptInput(context.Background(), ordinary, ref, filepath.Join(t.TempDir(), "copy")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(w.fileCache.Root, ref.SHA256)); err != nil {
		t.Fatal("ordinary cache behavior changed", err)
	}
}

func TestEphemeralFreshSessionCannotCertifyCleanup(t *testing.T) {
	q, _, _, s := ephemeralFixture(t)
	j := *s.state.Job
	q.mu.Lock()
	q.retainRelease(q.s.Jobs[j.ID])
	old := q.s.Machines[j.Machine]
	replacement := *old
	replacement.Session = "replacement"
	q.reconcileHostReleases(Checkin{Machine: replacement, Fresh: true, PreviousSession: j.Session})
	retained := len(q.s.HostReleases)
	q.mu.Unlock()
	if retained == 0 {
		t.Fatal("fresh session discharged unknown data custody")
	}
}

func TestEphemeralWorkerLifecycle(t *testing.T) {
	requireEphemeralTestBoundary(t)
	q, c, w, s := ephemeralFixture(t)
	j := *s.state.Job
	j.Spec.Run.Command = []string{"sh", "-ec", `printf payload > "$TMPDIR/temp"; printf private > "$HOME/private"; printf checkpoint > "$MYCE_CHECKPOINT_DIR/save"; printf output > out.txt; printf delivered-log`}
	s.state.Job, s.state.Result, s.state.Log = &j, nil, ""
	if err := os.Remove(filepath.Join(s.dir, ephemeralLogName(j))); err != nil {
		t.Fatal(err)
	}
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(c.Now(), 0, true)
	select {
	case finished := <-p.done:
		if finished.err != nil {
			t.Fatal(finished.err)
		}
		s.state.Result = &finished.result
	case <-time.After(testWaitTimeout):
		t.Fatal("command did not exit")
	}
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); err != nil || !done {
		t.Fatal(done, err)
	}
	got := job(t, q, j.ID)
	if got.State != "succeeded" {
		t.Fatal(got.State, JobReasonDetail(got))
	}
	paths := []string{}
	for _, p := range got.WorkerDataCleanup.Paths {
		paths = append(paths, p.Path)
		if _, err := os.Stat(filepath.Join(w.StateDir, p.Path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("released with payload", p, err)
		}
	}
	if !slices.ContainsFunc(paths, func(p string) bool { return strings.HasPrefix(p, "ephemeral/") }) {
		t.Fatal("wrong cleanup roots", paths)
	}
	data, err := json.Marshal(got)
	if err != nil || !strings.Contains(string(data), `"worker_data":"ephemeral"`) {
		t.Fatal("policy absent from status", err)
	}
}

func TestEphemeralAcceptedRestartAndLostReleaseReply(t *testing.T) {
	q, _, w, s := ephemeralFixture(t)
	j := *s.state.Job
	lostRelease := true
	w.Client.HTTP = &http.Client{Transport: fetchTransport(func(req *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(req)
		if err == nil && lostRelease && req.URL.Path == "/api/workers/complete" && s.state.WorkerDataAccepted {
			lostRelease = false
			response.Body.Close()
			return nil, errors.New("lost release reply")
		}
		return response, err
	})}
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); done || err == nil {
		t.Fatal("lost release reply not retained", done, err)
	}
	if !cleanupComplete(job(t, q, j.ID).WorkerDataCleanup) {
		t.Fatal("queue lost removal receipt")
	}
	data, err := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeWorkerState(data, &s.state); err != nil {
		t.Fatal(err)
	}
	if _, err := w.recoverAttempt(s); err != nil {
		t.Fatal(err)
	}
	if done, err := w.finishEphemeralAttempt(context.Background(), s, true); !done || err != nil {
		t.Fatal("startup could not reconcile already removed logs/output", done, err)
	}
	rows, _, _, err := q.waitIndex().read(WaitRequest{IDs: []string{j.ID}})
	if err != nil || !strings.Contains(rows[0].Reason, "cleanup: complete") {
		t.Fatal("wait omitted custody state", rows, err)
	}
}

func TestEphemeralCrashRecoveryReapsBeforeRemoval(t *testing.T) {
	requireEphemeralTestBoundary(t)
	_, c, w, s := ephemeralFixture(t)
	j := *s.state.Job
	j.Spec.Run.Command = []string{"sh", "-ec", `exec 9>"$TMPDIR/open-payload"; printf ready > ready; sleep 120`}
	s.state.Job, s.state.Result, s.state.Log = &j, nil, ""
	if err := os.Remove(filepath.Join(s.dir, ephemeralLogName(j))); err != nil {
		t.Fatal(err)
	}
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(c.Now(), 0, true)
	await(t, func() bool {
		_, err := os.Stat(filepath.Join(attemptStorageRoot(j, s.dir), "ready"))
		return err == nil
	})
	pid := s.state.PID
	w.cleanupFS = &faultWorkerDataFS{beforeRemove: func(string) {
		members, err := workerGroupMembers(pid)
		if err != nil || len(members) != 0 {
			t.Fatal("removal before process/file-handle custody ended", members, err)
		}
	}}
	if _, err := w.recoverAttempt(s); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(ephemeralRoot(s.dir, j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovered payload remains", err)
	}
}

func TestEphemeralManagedStagingAndDiskPlacement(t *testing.T) {
	for _, source := range []string{"stored", "nas", "source-archive"} {
		t.Run(source, func(t *testing.T) {
			_, _, w, s := ephemeralFixture(t)
			j := *s.state.Job
			work := attemptStorageRoot(j, s.dir)
			if err := os.RemoveAll(work); err != nil {
				t.Fatal(err)
			}
			nas := t.TempDir()
			switch source {
			case "stored":
				_, ref := retentionUpload(t, w.Client, "payload")
				j.Spec.Files = FileManifest{"input": ref}
			case "nas":
				sharedFixtureFile(t, nas, "original", "payload")
				j.Spec.Inputs = map[string]string{"input": "original"}
				w.JobFileRoot = nas
			case "source-archive":
				sharedArchive(t, nas)
				data, err := os.ReadFile(filepath.Join(nas, "runtime.tar"))
				if err != nil {
					t.Fatal(err)
				}
				_, ref := retentionUpload(t, w.Client, string(data))
				j.Spec.Files = FileManifest{".myce-source.tar": ref}
				j.Spec.PrepareSource = true
			}
			if err := w.prepareExtensionFilesContext(context.Background(), &j, s.dir); err != nil {
				t.Fatal(err)
			}
			file := "input"
			if source == "source-archive" {
				file = "bin/run"
			}
			if _, err := os.Stat(filepath.Join(work, file)); err != nil {
				t.Fatal("input not staged privately", err)
			}
			if err := w.removeEphemeralPaths(s, false); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(work); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("staged payload remains", err)
			}
		})
	}
	s := ephemeralSpec()
	s.SharedInputs = map[string]SharedInput{"asset": {SizeBytes: 10, UnpackedBytes: 20, Archive: "tar"}}
	j := Job{Spec: s}
	if reason := sharedInputCacheReason(s, nil, false); reason != "" {
		t.Fatal("private staging waits for persistent cache", reason)
	}
	need, _ := diskVolumeNeed(&j, DiskVolume{Roots: []string{"data"}})
	cache, _ := diskVolumeNeed(&j, DiskVolume{Roots: []string{"file-cache"}})
	if need != 30 || cache != 0 {
		t.Fatal("private data was charged to wrong disk", need, cache)
	}
	for _, inputs := range []map[string]string{{"../escaped": "source"}, {"input": "../source"}, {"out.txt": "source"}, {"input": "source", "input/nested": "source"}} {
		s.Inputs = inputs
		if err := s.Validate(); err == nil {
			t.Fatal("unsafe NAS input accepted", inputs)
		}
	}
}
