package myce

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
test "$HOME" = "$(pwd)/.home"
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
					t.Fatal("after input was not bound", assigned)
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
	for _, field := range []string{"workdir", "network"} {
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
			_, err := q.Submit(Spec{Name: "unsupported", Project: "test", Extension: extensionKey(e), WorkerData: "ephemeral", Outputs: map[string]string{"out": "results"}})
			var refusal *rejectionError
			if !errors.As(err, &refusal) || refusal.code != "job_worker_data_unsupported" {
				t.Fatal(err)
			}
			message := map[string]string{"workdir": "{work}", "network": "offline"}[field]
			if !strings.Contains(err.Error(), message) {
				t.Fatal("imprecise refusal", err)
			}
		})
	}
}
