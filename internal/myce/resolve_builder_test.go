package myce

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Offline builders stand in for Python/package downloads, not the worker,
// planner, source staging, environment activation or command runner. Both
// roots run the same unpinned command and immutable input; no native runtime
// or accelerator qualification is inferred from the synthetic executables.
func TestResolveOldInterpreterBuilderJourney(t *testing.T) {
	system := t.TempDir()
	old := "#!/bin/sh\necho 'Python 3.8.0'\nexit 38\n"
	if err := os.WriteFile(filepath.Join(system, "python3"), []byte(old), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", system+string(os.PathListSeparator)+os.Getenv("PATH"))
	if output, err := exec.Command("python3", "--version").CombinedOutput(); err == nil || !strings.Contains(string(output), "3.8.0") {
		t.Fatalf("old system fixture: %s %v", output, err)
	}
	for _, backend := range []string{"uv", "pixi"} {
		t.Run(backend, func(t *testing.T) {
			for _, rootName := range []string{"first-root", "second-root"} {
				t.Run(rootName, func(t *testing.T) {
					q, clock := fixture(t)
					if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
						t.Fatal(err)
					}
					lockRoot := t.TempDir()
					recipe := EnvironmentRecipe{Backend: backend, Python: "3.12", LockFile: filepath.Join(lockRoot, "requirements.txt")}
					locks := map[string]string{"requirements.txt": "# locked offline fixture\n"}
					if backend == "pixi" {
						recipe.LockFile = filepath.Join(lockRoot, "pixi.lock")
						locks = map[string]string{"pixi.lock": "locked offline fixture", "pixi.toml": "[workspace]\nchannels=[]\nplatforms=['linux-64','osx-arm64']\n[dependencies]\npython='3.12.*'\nffmpeg='7.*'\n"}
					}
					for name, data := range locks {
						if err := os.WriteFile(filepath.Join(lockRoot, name), []byte(data), 0600); err != nil {
							t.Fatal(err)
						}
					}
					recipe, err := snapshotEnvironment(recipe, "cpu")
					if err != nil {
						t.Fatal(err)
					}
					if err = q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": recipe}}}); err != nil {
						t.Fatal(err)
					}
					m := cpuMachine("builder", "builder", "cpu", 4, 8)
					m.LastSeen, m.EnvironmentBuilder = clock.Now(), true
					q.s.Machines[m.ID] = &m
					client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
					input := filepath.Join(t.TempDir(), "data")
					if err := os.WriteFile(input, []byte("portable input\n"), 0600); err != nil {
						t.Fatal(err)
					}
					ref, err := IdentifyFile(input)
					if err == nil {
						err = client.UploadJobInput(context.Background(), input, ref)
					}
					if err != nil {
						t.Fatal(err)
					}
					s := cpuResolveSpec()
					s.Run.Command = []string{"python3", "--version"}
					s.Environment = Environment{Name: "tools"}
					s.SharedInputs = map[string]SharedInput{"data": {SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
					m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
					before := string(wireJSON(t, q.s))
					var report ResolveReport
					if err = client.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
						t.Fatal(err)
					}
					if len(report.Targets) != 1 || !report.Targets[0].Eligible || report.Targets[0].Environment == nil || report.Targets[0].Environment.Key != recipe.Key || report.Targets[0].Environment.State != "prepare-needed" {
						t.Fatalf("cold builder resolution: %+v", report)
					}
					if after := string(wireJSON(t, q.s)); before != after {
						t.Fatal("resolve mutated queue")
					}
					submitted, err := client.Submit(context.Background(), s)
					if err != nil {
						t.Fatal(err)
					}
					var builds atomic.Int32
					devices := slices.Clone(m.Devices)
					w := Worker{StateDir: newTestStateRoot(t), Machine: m, Client: client, Interval: time.Millisecond,
						CPUIsolationProbe: func(context.Context) error { return nil },
						InventoryProbe:    func(context.Context) ([]MachineDevice, error) { return slices.Clone(devices), nil },
					}
					// Reuse the synthetic live session advertised for resolution. A fresh
					// state directory would otherwise impersonate a second live worker.
					if err := saveWorkerJSON(w.StateDir, "state.json", workerState{Session: m.Session, Fresh: true}); err != nil {
						t.Fatal(err)
					}
					w.environmentRun = func(_ context.Context, args []string, dir string, _ []string, out, _ io.Writer) error {
						if args[0] != backend {
							return fmt.Errorf("wrong builder %v", args)
						}
						for name, data := range locks {
							b, err := os.ReadFile(filepath.Join(dir, name))
							if err != nil || string(b) != data {
								return fmt.Errorf("lock not frozen: %s %v", name, err)
							}
						}
						prefix := filepath.Join(dir, "venv")
						if backend == "pixi" {
							prefix = filepath.Join(dir, ".pixi", "envs", "default")
						}
						if args[1] == "shell-hook" {
							_, err := fmt.Fprintf(out, `{"environment_variables":{"PATH":%q,"CONDA_PREFIX":%q},"activation_scripts":[]}`, filepath.Join(prefix, "bin")+":/usr/bin:/bin", prefix)
							return err
						}
						if backend == "uv" && args[1] == "venv" && !slices.Contains(args, "3.12") {
							return fmt.Errorf("builder omitted requested interpreter: %v", args)
						}
						if backend == "uv" && args[1] == "pip" {
							return nil
						}
						builds.Add(1)
						bin := filepath.Join(prefix, "bin")
						if err := os.MkdirAll(bin, 0700); err != nil {
							return err
						}
						// The fake Python also exercises the locked media tool and staged data.
						for name, script := range map[string]string{
							"python3": "#!/bin/sh\nset -eu\ntest \"$1\" = --version\ntest -z \"${MYCE_TOKEN:-}\"\necho 'Python 3.12.0'\nffmpeg -version\ncat \"$MYCE_INPUT_DATA\"\n",
							"ffmpeg":  "#!/bin/sh\ntest \"$1\" = -version\necho 'ffmpeg 7 fixture'\n",
						} {
							if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
								return err
							}
						}
						return nil
					}
					ctx, cancel := context.WithCancel(context.Background())
					done := make(chan error, 1)
					go func() { done <- w.Run(ctx) }()
					defer func() {
						cancel()
						if err := <-done; err != nil {
							t.Error(err)
						}
					}()
					wait, stop := context.WithTimeout(ctx, 10*time.Second)
					defer stop()
					for run := range 2 {
						got, err := client.Wait(wait, submitted.ID)
						if err != nil || got.State != "succeeded" || !got.ReleaseConfirmed {
							t.Fatalf("builder job: %+v %v", got, err)
						}
						logs, err := q.Logs(got.ID, 0)
						if err != nil {
							t.Fatal(err)
						}
						for _, want := range []string{"Python 3.12.0", "ffmpeg 7 fixture", "portable input"} {
							if !strings.Contains(fmt.Sprint(logs), want) {
								t.Fatalf("missing %q: %v", want, logs)
							}
						}
						if err = client.Call(ctx, "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
							t.Fatal(err)
						}
						if report.Targets[0].Environment.State != "ready" {
							t.Fatalf("prepared runtime not reusable: %+v", report)
						}
						// Queue the identical unpinned spec once more; it must reuse the build.
						if run == 0 {
							next, err := client.Submit(ctx, s)
							if err != nil {
								t.Fatal(err)
							}
							submitted = next
						}
					}
					if builds.Load() != 1 {
						t.Fatalf("warm reuse rebuilt %d times", builds.Load())
					}
				})
			}
		})
	}
}
