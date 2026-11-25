package myce

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEphemeralExtensionDependencySidecarLifecycle(t *testing.T) {
	requireEphemeralTestBoundary(t)
	for _, outcome := range []string{"success", "failure", "cancel", "preempt", "restart"} {
		t.Run(outcome, func(t *testing.T) {
			e := compositionExtension(t, true)
			v := &e.Variants[0]
			v.Execution.Main.Command = []string{"sh", "-e", "{extension}/private.sh"}
			v.Execution.Sidecars = []NativeSidecar{{Name: "helper", Environment: "main", Command: []string{"sh", "-e", "{extension}/side.sh"}, Env: map[string]string{"HOME": "{work}/wrong-home", "TMPDIR": "{work}/wrong-temp"}}}
			e.Files["private.sh"] = []byte(`
test -z "$MYCE_TOKEN"
while [ ! -f side-ready ]; do sleep .01; done
cat input > "$HOME/main"
cat input > "$TMPDIR/main"
cat input > "$MYCE_SCRATCH_DIR/main"
cat input > "$MYCE_CHECKPOINT_DIR/main"
cat input > "$MYCE_EXTENSION_DIR/derived"
cat input > out.txt
cat input
touch main-ready
case "$1" in failure) exit 7;; block) while :; do sleep .01; done;; esac
`)
			e.Files["side.sh"] = []byte(`
test -z "$MYCE_TOKEN"
test "$HOME" -ef .home
test -z "$MYCE_USAGE_FILE"
cat input > "$HOME/side"
cat input > "$TMPDIR/side"
cat input > "$MYCE_SCRATCH_DIR/side"
cat input > "$MYCE_CHECKPOINT_DIR/side"
mkdir side-work
cat input > side-work/data
cat input
touch side-ready
while :; do sleep .01; done
`)
			e = freezeComposition(t, e)
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "queue-files"), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			w := prepareExtensionTestWorker(t, q, e)
			// This fixture drives check-ins directly, without Worker.Run's host
			// sampler. Supply the RAM telemetry required by the extension's needs.
			w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 32
			w.HostMemorySource = func(context.Context) (float64, error) { return 32, nil }
			w.MemorySource = func(context.Context) (float64, error) { return 8, nil }
			w.ephemeralProbe = func() error { return nil }
			w.Machine.Capabilities = append(w.Machine.Capabilities, ephemeralCapability, nativeCICapability, "native_isolation_namespace", "native_sidecars")
			w.environmentCache.Clock = w.Clock
			// Only software installation is synthetic. All job processes, after
			// binding, file transfers, custody and removal below are real.
			w.environmentCache.Run = func(context.Context, []string, string, []string, io.Writer, io.Writer) error { return nil }
			recipe := e.Variants[0].Environments["main"]
			environment, release, err := w.environmentCache.Acquire(context.Background(), recipe)
			if err != nil {
				t.Fatal(err)
			}
			release()
			if err := InstallExtension(w.ExtensionRoot, e); err != nil {
				t.Fatal(err)
			}
			rpt := newExtensionReport(e, &w.Machine, "extension:"+e.Name)
			rpt.Setup, rpt.Qualified, rpt.CheckID, rpt.Reason = true, true, "fixture-check", ""
			if err := writeExtensionReports(w.ExtensionRoot, e, w.Machine.GPU, []ExtensionReport{rpt}); err != nil {
				t.Fatal(err)
			}
			w.Machine.ExtensionReports = []ExtensionReport{rpt}
			w.Machine.Environments = []string{recipe.Key}
			w.CacheRoot = filepath.Join(w.StateDir, "model-cache")
			model := filepath.Join(w.CacheRoot, "software-model")
			if err := os.MkdirAll(w.CacheRoot, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(model, []byte("shared software"), 0600); err != nil {
				t.Fatal(err)
			}
			root := &workerStore{dir: w.StateDir, state: workerState{Session: w.Machine.Session}}
			if err := root.save(); err != nil {
				t.Fatal(err)
			}
			if err := w.initFileTransfers(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			check(t, q, w.Machine, nil)
			const payload = "customer-stage-sensitive-sentinel"
			_, ref := retentionUpload(t, w.Client, payload)
			producerSpec := spec("experiment")
			producerSpec.Resumable = false
			producerSpec.Files = FileManifest{"source": ref}
			producerSpec.Outputs = map[string]string{"out": "results"}
			producer := submit(t, q, producerSpec)
			pr := check(t, q, w.Machine, nil).Job
			if pr == nil || pr.ID != producer.ID {
				t.Fatal("producer not assigned")
			}
			if err := q.Complete(Result{Machine: pr.Machine, Session: pr.Session, Execution: Execution{ID: pr.ID, Attempt: pr.Attempt}, Success: true, OutputRefs: FileManifest{"out": ref}}); err != nil {
				t.Fatal(err)
			}
			arg := outcome
			if outcome == "cancel" || outcome == "preempt" || outcome == "restart" {
				arg = "block"
			}
			spec := Spec{Name: "private-stage", Project: "test", Priority: "experiment", Extension: extensionKey(e), WorkerData: "ephemeral", Args: []string{arg}, Outputs: map[string]string{"out.txt": "results"}, After: &PieceDependency{Job: producer.ID, Outputs: map[string]string{"input": "out"}}}
			pieces := 1
			if outcome == "success" {
				pieces = 2
				spec.Args = nil
				spec.Workload = &Workload{InputShape: "tiny", Units: 1}
				spec.FanOut = &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "first", Args: []string{"success"}}, {Key: "second", Args: []string{"success"}}}}
			}
			stage := submit(t, q, spec)
			for range pieces {
				assigned := check(t, q, w.Machine, nil).Job
				if assigned == nil || assigned.Dependency == nil || assigned.Dependency.Files["input"] != ref || assigned.Spec.WorkerData != "ephemeral" {
					got := job(t, q, stage.ID)
					t.Fatalf("after input was not bound: assigned=%+v stage=%s reason=%s dependency=%+v qualified=%t", assigned, got.State, got.Reason, got.Dependency, qualifiedExtension(got, &w.Machine) != nil)
				}
				store, err := newAttemptStore(root, *assigned)
				if err != nil {
					t.Fatal(err)
				}
				p, err := startAfterFixtureInputs(t, w, store, *assigned)
				if err != nil {
					t.Fatal(err)
				}
				defer p.stop(w.Clock.Now(), 0, true)
				work := attemptStorageRoot(*assigned, w.StateDir)
				if arg == "block" {
					await(t, func() bool {
						select {
						case f := <-p.done:
							logs, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
							t.Fatalf("group exited before main-ready: %+v log: %s", f, logs)
						default:
						}
						_, err := os.Stat(filepath.Join(work, "main-ready"))
						return err == nil
					})
					if outcome == "cancel" {
						if err := q.Cancel(stage.ID); err != nil {
							t.Fatal(err)
						}
					}
					p.stopDirective(w.Clock.Now(), 0, true, outcome == "cancel")
				}
				select {
				case f := <-p.done:
					if f.err != nil || f.result.ReleasePending || f.result.Success != (outcome == "success") {
						logs, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
						t.Fatalf("process result: %+v log: %s", f, logs)
					}
					store.state.Result = &f.result
				case <-time.After(testWaitTimeout):
					t.Fatal("extension group did not exit")
				}
				// Census the actual payload copies before finalization, including
				// both group members' writes and their single combined log.
				for _, path := range []string{"input", ".myce-extension/derived", ".home/main", ".home/side", ".tmp/main", ".tmp/side", ".scratch/main", ".scratch/side", "side-work/data", "out.txt"} {
					if data, err := os.ReadFile(filepath.Join(work, path)); err != nil || string(data) != payload {
						t.Fatal("missing custody sentinel", path, err)
					}
				}
				for _, member := range []string{"main", "side"} {
					if data, err := os.ReadFile(filepath.Join(ephemeralRoot(store.dir, *assigned), "checkpoint", member)); err != nil || string(data) != payload {
						t.Fatal("checkpoint escaped private custody", member, err)
					}
				}
				if outcome == "restart" {
					store.state.Result = nil
					if err := store.save(); err != nil {
						t.Fatal(err)
					}
					if _, err := w.recoverAttempt(store); err != nil {
						t.Fatal(err)
					}
				}
				if done, err := w.finishEphemeralAttempt(context.Background(), store, true); err != nil || !done {
					t.Fatal("finish", done, err)
				}
				got := job(t, q, assigned.ID)
				if !cleanupComplete(got.AttemptHistory[0].WorkerDataCleanup) {
					t.Fatal("no cleanup proof", got.AttemptHistory)
				}
				if outcome == "success" {
					output := filepath.Join(t.TempDir(), "accepted")
					if err := w.Client.DownloadFile(context.Background(), got.OutputRefs["out.txt"], output); err != nil {
						t.Fatal(err)
					}
					if data, _ := os.ReadFile(output); string(data) != payload {
						t.Fatal("result transfer changed bytes")
					}
				}
				logs, err := q.Logs(assigned.ID, 0)
				var text strings.Builder
				for _, entry := range logs {
					text.WriteString(entry.Text)
				}
				if err != nil || strings.Count(text.String(), payload) < 2 {
					t.Fatal("main/sidecar logs were not delivered", err, text.String())
				}
				if outcome == "preempt" && got.State != "queued" {
					t.Fatal("interrupted attempt did not requeue", got.State)
				}
				for _, path := range []string{ephemeralRoot(store.dir, *assigned), attemptPublicationRoot(*assigned, store.dir), filepath.Join(store.dir, ephemeralLogName(*assigned)), filepath.Join(store.dir, "job-files"), filepath.Join(store.dir, "checkpoints"), filepath.Join(store.dir, "t")} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("job-derived root survives", path, err)
					}
				}
				files := []string{}
				for label, root := range map[string]string{"state": w.StateDir, "environments": w.environmentCache.Root, "extensions": w.ExtensionRoot} {
					if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
						if err != nil {
							return err
						}
						if entry.IsDir() {
							return nil
						}
						rel, _ := filepath.Rel(root, path)
						rel = label + "/" + rel
						files = append(files, rel)
						data, err := os.ReadFile(path)
						if err == nil && strings.Contains(string(data), payload) {
							t.Errorf("payload remains in %s", rel)
						}
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				slices.Sort(files)
				t.Log("worker files after transfer:", files)
				for _, path := range []string{environment, extensionFolder(w.ExtensionRoot, e), extensionCache(w.ExtensionRoot, e, w.Machine.GPU), model} {
					if _, err := os.Stat(path); err != nil {
						t.Fatal("shared software removed", path, err)
					}
				}
				if actual, err := ReadExtension(extensionFolder(w.ExtensionRoot, e)); err != nil || actual.Digest != e.Digest || !w.environmentCache.ready(recipe.Key) {
					t.Fatal("shared software changed", err)
				}
				if data, err := os.ReadFile(model); err != nil || string(data) != "shared software" {
					t.Fatal("model cache changed", err)
				}
			}
			if outcome == "success" {
				parent := job(t, q, stage.ID)
				if parent.State != "succeeded" || len(parent.OutputRefs) != pieces {
					t.Fatal("fan-out did not gather", parent.State, parent.OutputRefs)
				}
			}
		})
	}
}

func TestEphemeralExtensionFrozenScopeRefusals(t *testing.T) {
	for _, field := range []string{"workdir", "network", "unqualified worker"} {
		t.Run(field, func(t *testing.T) {
			e := compositionExtension(t, false)
			v := &e.Variants[0]
			switch field {
			case "workdir":
				v.Execution.Main.Workdir = "{cache}"
			case "network":
				v.Needs.Network = true
			}
			e = freezeComposition(t, e)
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			w := prepareExtensionTestWorker(t, q, e)
			check(t, q, w.Machine, nil)
			s := Spec{Name: "unsupported", Project: "test", Extension: extensionKey(e), WorkerData: "ephemeral", Outputs: map[string]string{"out": "results"}}
			if field == "unqualified worker" {
				check(t, q, ephemeralMachine(), nil)
				s.AllowedMachines = []string{w.Machine.ID}
			}
			_, err := w.Client.Submit(context.Background(), s)
			var refusal *HTTPError
			if !errors.As(err, &refusal) || refusal.Code != "job_worker_data_unsupported" {
				t.Fatal(err)
			}
			message := map[string]string{"workdir": "{work}", "network": "offline", "unqualified worker": ephemeralCapability}[field]
			if !strings.Contains(err.Error(), message) {
				t.Fatal("imprecise refusal", err)
			}
		})
	}
}

func TestEphemeralExtensionTransferFence(t *testing.T) {
	for _, superseded := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "superseded"}[superseded], func(t *testing.T) {
			ctx := context.Background()
			e := compositionExtension(t, false)
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			w := prepareExtensionTestWorker(t, q, e)
			m := w.Machine
			m.Capabilities = append(m.Capabilities, ephemeralCapability, nativeCICapability, "native_isolation_namespace")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
			report := newExtensionReport(e, &m, "extension:"+e.Name)
			report.Setup, report.Qualified, report.CheckID = true, true, "fixture-check"
			m.ExtensionReports = []ExtensionReport{report}
			machineCheck(t, q, m)

			const payload = "private stage data"
			_, input := retentionUpload(t, w.Client, payload)
			queued := submit(t, q, Spec{Name: "stage", Project: "test", Priority: "experiment", Extension: extensionKey(e),
				Advanced: &Advanced{WorkerData: "ephemeral"}, Files: FileManifest{"in.txt": input},
				Outputs: map[string]string{"a.txt": "results", "b.txt": "results"}})
			j := assignedJob(machineCheck(t, q, m))
			if j == nil || j.ID != queued.ID || j.Extension == nil {
				t.Fatal("qualified extension was not assigned", j)
			}
			unqualified := m
			unqualified.Capabilities = slices.DeleteFunc(slices.Clone(m.Capabilities), func(c string) bool { return c == ephemeralCapability })
			if offered := assignedJob(machineCheck(t, q, unqualified)); offered != nil {
				t.Fatal("unqualified worker received a replayed assignment", offered.ID)
			}
			if offered := assignedJob(machineCheck(t, q, m)); offered == nil || jobKey(*offered) != jobKey(*j) {
				t.Fatal("qualified worker could not recover the exact offer", offered)
			}

			root := &workerStore{dir: w.StateDir, state: workerState{Session: m.Session}}
			store, err := newAttemptStore(root, *j)
			if err != nil {
				t.Fatal(err)
			}
			staged := *j
			if err := w.prepareExtensionFilesContext(ctx, &staged, store.dir); err != nil {
				t.Fatal(err)
			}
			work := attemptStorageRoot(*j, store.dir)
			// Simulate completed members so transfer failure and stale outcomes
			// run on Linux guests without qualified kernel/core routing too.
			local := map[string]string{"in.txt": payload, "a.txt": payload + " A", "b.txt": payload + " B", ".scratch/step": payload, ".myce-extension/generated": payload}
			for name, data := range local {
				if name == "in.txt" {
					continue // Exercised through actual queue-backed input staging.
				}
				writeEphemeralFixtureFile(t, filepath.Join(work, name), data)
			}
			store.state.Log = ephemeralLogName(*j)
			logPath := filepath.Join(store.dir, store.state.Log)
			writeEphemeralFixtureFile(t, logPath, payload)
			result := Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Success: true}
			store.state.Result = &result

			var firstUploaded, secondBlocked atomic.Bool
			publisher := w
			publisher.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/files/uploads/" {
					return http.DefaultTransport.RoundTrip(r)
				}
				if firstUploaded.Load() {
					secondBlocked.Store(true)
					return nil, errors.New("second output transfer interrupted")
				}
				response, err := http.DefaultTransport.RoundTrip(r)
				if err == nil && response.StatusCode == http.StatusCreated && response.Header.Get("Upload-Offset") != "" {
					firstUploaded.Store(true)
				}
				return response, err
			})}
			if done, err := publisher.finishEphemeralAttempt(ctx, store, true); err == nil || done || !secondBlocked.Load() {
				t.Fatal("expected a failed transfer after one real upload", done, err)
			}
			assertPayloadPresent := func() {
				t.Helper()
				for name, want := range local {
					got, err := os.ReadFile(filepath.Join(work, name))
					if err != nil || string(got) != want {
						t.Fatal("touched private data before acceptance", name, err)
					}
				}
				if _, err := os.Stat(logPath); err != nil {
					t.Fatal("touched the local log before acceptance", err)
				}
			}
			assertUnreleased := func() {
				t.Helper()
				assertPayloadPresent()
				got := job(t, q, j.ID)
				if store.state.WorkerDataAccepted || got.ReleaseConfirmed || len(got.OutputRefs) != 0 || cleanupComplete(got.WorkerDataCleanup) {
					t.Fatal("partial transfer accepted or released", got.State, got.OutputRefs, got.WorkerDataCleanup)
				}
			}
			assertUnreleased()

			var replacement *Job
			var replacementPath string
			if superseded {
				if err := q.update(func() error {
					q.requeue(q.s.Jobs[j.ID], "fixture lost the previous worker", true)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				next := m
				next.ID, next.HostID, next.Session = "replacement", "replacement", ID()
				nextReport := report
				nextReport.Host = next.HostID
				next.ExtensionReports = []ExtensionReport{nextReport}
				replacement = assignedJob(machineCheck(t, q, next))
				if replacement == nil || replacement.ID != j.ID || replacement.Attempt <= j.Attempt {
					t.Fatal("replacement extension attempt was not assigned", replacement)
				}
				replacementPath = filepath.Join(attemptStorageRoot(*replacement, store.dir), "in.txt")
				writeEphemeralFixtureFile(t, replacementPath, payload)

				// A mismatched command result cannot authorize deletion even
				// locally. Restore the real old result before reconciling it.
				mismatched := result
				mismatched.Attempt = replacement.Attempt
				store.state.Result = &mismatched
				if done, err := w.finishEphemeralAttempt(ctx, store, true); err == nil || done {
					t.Fatal("mismatched result accepted", done, err)
				}
				assertUnreleased()
				store.state.Result = &result
			} else {
				// A failed local acceptance write must not leave a volatile
				// acknowledgement that a retry can use as authority to unlink.
				stateName := store.stateName
				store.stateName = filepath.Join("missing-state-directory", stateName)
				done, err := w.finishEphemeralAttempt(ctx, store, true)
				store.stateName = stateName
				got := job(t, q, j.ID)
				if err == nil || done || store.state.WorkerDataAccepted || got.State != "succeeded" || got.ReleaseConfirmed {
					t.Fatal("local acceptance write failure did not retain custody", done, err, got.State)
				}
				assertPayloadPresent()
			}

			if done, err := w.finishEphemeralAttempt(ctx, store, true); err != nil || !done {
				t.Fatal("could not reconcile exact attempt", done, err)
			}
			got := job(t, q, j.ID)
			receipt := got.AttemptHistory[0].WorkerDataCleanup
			if !cleanupComplete(receipt) || receipt.Execution != result.Execution || receipt.Machine != j.Machine || receipt.Session != j.Session {
				t.Fatal("missing exact cleanup receipt", receipt)
			}
			for _, name := range []string{ephemeralRoot(store.dir, *j), logPath} {
				if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("old payload still present", name, err)
				}
			}
			if superseded {
				data, err := os.ReadFile(replacementPath)
				if err != nil || string(data) != payload || got.Attempt != replacement.Attempt || got.ReleaseConfirmed || cleanupComplete(got.WorkerDataCleanup) || len(got.OutputRefs) != 0 {
					t.Fatal("stale result affected the replacement", got.State, got.OutputRefs, got.WorkerDataCleanup, err)
				}
				return
			}
			if !got.ReleaseConfirmed || got.State != "succeeded" || len(got.OutputRefs) != 2 {
				t.Fatal("result was not verified before release", got.State, got.OutputRefs, got.WorkerDataCleanup)
			}
			for name, ref := range got.OutputRefs {
				dest := filepath.Join(t.TempDir(), name)
				if err := w.Client.DownloadFile(ctx, ref, dest); err != nil {
					t.Fatal(err)
				}
				if data, err := os.ReadFile(dest); err != nil || string(data) != local[name] {
					t.Fatal("accepted output changed", name, err)
				}
			}
		})
	}
}

func writeEphemeralFixtureFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
