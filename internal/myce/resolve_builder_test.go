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
	"testing"
	"time"
)

// Offline builders stand in for Python/package downloads, not the worker,
// planner, source staging, environment activation or command runner. Both
// roots run the same unpinned command and immutable input; no native runtime
// or accelerator qualification is inferred from the synthetic executables.
func TestResolveOldInterpreterBuilderJourney(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
				for _, rootName := range []string{"first-root"} {
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
						m.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
						check(t, q, m, nil)
						var report ResolveReport
						if err = client.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
							t.Fatal(err)
						}
						if len(report.Targets) != 1 || !report.Targets[0].Eligible || report.Targets[0].Environment == nil || report.Targets[0].Environment.Key != recipe.Key || report.Targets[0].Environment.State != "prepare-needed" {
							t.Fatalf("cold builder resolution: %+v", report)
						}
						submitted, err := client.Submit(context.Background(), s)
						if err != nil {
							t.Fatal(err)
						}
						devices := slices.Clone(m.Devices)
						w := Worker{EnvironmentLimit: 1 << 30, StateDir: newTestStateRoot(t), Machine: m, Client: client, Interval: time.Millisecond,
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
					})
				}
			})
		}

	})

	t.Run("ResolveNativeAuthoredMetadataRemainsUnqualified", func(t *testing.T) {
		q, _, _, s := observationFixture(t)
		e := q.s.Extensions[s.Extension]
		// Even a caller-supplied registration digest cannot turn local metadata into
		// qualified evidence. The existing portability reader clears it.
		before := string(wireJSON(t, q.s))
		r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
		if err != nil {
			t.Fatal(err)
		}
		if r.Portability == nil || len(r.Targets) == 0 {
			t.Fatalf("native metadata not resolved: %+v", r)
		}
		for _, target := range r.Targets {
			if target.Eligible || !strings.Contains(target.Reason, "frozen native registration") {
				t.Fatalf("unfrozen metadata claimed eligible: %+v", target)
			}
		}
		if after := string(wireJSON(t, q.s)); before != after {
			t.Fatal("native resolution mutated queue")
		}

	})

	t.Run("ResolveRegisteredNativeEnvironmentBackend", func(t *testing.T) {
		for _, backend := range []string{"uv", "pixi"} {
			t.Run(backend, func(t *testing.T) {
				q, clock := fixture(t)
				e := compositionExtension(t, true)
				variant := &e.Variants[0]
				if backend == "pixi" {
					variant.Environments["main"] = EnvironmentRecipe{Backend: "pixi", LockFile: "locks/pixi.lock"}
					e.Files["locks/pixi.lock"] = []byte("locked offline fixture")
					e.Files["locks/pixi.toml"] = []byte("[workspace]\nchannels=[]\nplatforms=['linux-64','osx-arm64']\n")
				}
				e = freezeComposition(t, e)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				m := machine("native", "fake")
				m.LastSeen = clock.Now()
				m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
				m.ExtensionFingerprint = "synthetic-native-v1"
				recipe := e.Variants[0].Environments["main"]
				m.Environments = []string{recipe.Key}
				report := newExtensionReport(e, &m, "extension:"+e.Name)
				report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-native-check"
				m.ExtensionReports = []ExtensionReport{report}
				q.s.Machines[m.ID] = &m
				r, err := q.Resolve(ResolveRequest{Spec: Spec{Name: "native", Project: "fixture", Extension: extensionKey(e)}})
				if err != nil || len(r.Targets) != 1 || r.Targets[0].Environment == nil || r.Targets[0].Environment.Key != recipe.Key || r.Targets[0].Environment.Backend != backend {
					t.Fatalf("registered environment misidentified: %+v %v", r, err)
				}
			})
		}

	})

	t.Run("ResolveExplainsSharedHostRefusal", func(t *testing.T) {
		q, c := fixture(t)
		key := strings.Repeat("b", 64)
		q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Key: key, Backend: "uv", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("# pinned\n")}}}}}
		m := machine("shared", "cpu")
		m.LastSeen = c.Now()
		check(t, q, m, nil)
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
		m.EnvironmentBuilder = true
		q.s.Machines[m.ID] = &m
		s := cpuResolveSpec()
		s.Environment = Environment{Name: "tools"}
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Targets) != 1 || r.Targets[0].Eligible || !strings.Contains(r.Targets[0].Reason, "environment not prepared on this shared host") || !strings.Contains(r.Targets[0].Reason, "Run without a managed environment") {
			t.Fatalf("shared host refusal not explained: %+v", r.Targets)
		}
		s.RequireTarget = true
		_, err = q.Submit(s)
		if err == nil || !strings.Contains(err.Error(), "shared hosts never build environments") {
			t.Fatalf("submit refusal lacks the shared-host condition: %v", err)
		}
		s.Environment = Environment{}
		s.RequireTarget = false
		r, err = q.Resolve(ResolveRequest{Spec: s})
		if err != nil || !r.Targets[0].Eligible {
			t.Fatalf("the suggested fix does not resolve: %+v %v", r, err)
		}

	})

	t.Run("ResolveRegisteredExtensionJob", func(t *testing.T) {
		q, _, _, s := observationFixture(t)
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil || len(r.Targets) != 1 || !r.Targets[0].Eligible {
			t.Fatalf("qualified extension job not eligible: %+v %v", r, err)
		}
		s.RequireTarget = true
		if _, err := q.Submit(s); err != nil {
			t.Fatalf("submit disagrees with resolve: %v", err)
		}

	})
}
