package myce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEnvironmentCPURecipes(t *testing.T) {
	dir := t.TempDir()
	for name, data := range map[string]string{
		"uv.lock": "locked-uv", "pyproject.toml": "[project]\nname='example'\n",
		"pixi.lock": "locked-pixi", "pixi.toml": "[workspace]\nchannels=['conda-forge']\nplatforms=['linux-64','osx-arm64']\n[dependencies]\npython='>=3.12,<3.13'\nuv='*'\nffmpeg='*'\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0700); err != nil {
			t.Fatal(err)
		}
	}
	catalogFile := filepath.Join(dir, "catalog.yaml")
	for _, recipe := range []string{
		"python: '3.12', lock_file: uv.lock", "backend: pixi, lock_file: pixi.lock",
	} {
		if err := os.WriteFile(catalogFile, []byte("environments:\n  tools:\n    cpu: {"+recipe+"}\n"), 0600); err != nil {
			t.Fatal(err)
		}
		catalog, err := ReadEnvironments(catalogFile)
		if err != nil {
			t.Fatalf("CPU recipe %s: %v", recipe, err)
		}
		q, _ := fixture(t)
		if err := q.ConfigureEnvironments(catalog); err != nil {
			t.Fatal(err)
		}
		s := cpuSpec(1, 1)
		s.Environment.Name = "tools"
		j := submit(t, q, s)
		for _, hostOS := range []string{"linux", "darwin"} {
			m := cpuMachine("host", "host", "cpu", 4, 8)
			m.CapabilityProfile.OS = hostOS
			m.FreeHostMemoryGB = 8
			m.EnvironmentBuilder = true
			if !fits(j, &m, 0) || environmentKey(j, &m) != catalog.Environments["tools"]["cpu"].Key {
				t.Fatalf("CPU recipe unavailable on %s: %s", hostOS, recipe)
			}
		}
		// Any still filters by the exact recipe; GPU work cannot borrow cpu.
		anyJob := *j
		anyJob.Spec = spec("experiment")
		anyJob.Spec.Environment.Name = "tools"
		gpu := cpuMachine("gpu", "gpu", "fake", 4, 8)
		gpu.FreeHostMemoryGB = 8
		gpu.EnvironmentBuilder = true
		if fits(&anyJob, &gpu, 8) {
			t.Fatal("any borrowed a CPU recipe for GPU work")
		}
		cpu := cpuMachineView(&gpu)
		gpuOnly := *j
		gpuOnly.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": binaryRecipe(t, "fake")}
		if fits(&gpuOnly, cpu, 0) {
			t.Fatal("CPU work borrowed a GPU recipe")
		}
	}
	q, _ := fixture(t)
	for _, platforms := range [][]string{{"cpu"}, {"cpu", "fake"}, nil} {
		r, err := q.resolveEnvironment(Environment{LockFile: filepath.Join(dir, "uv.lock"), Python: "3.12", Platforms: platforms})
		if err != nil || r["cpu"].Key == "" || r["cpu"].Platform != "cpu" {
			t.Fatalf("inline CPU resolution (%v): %v, %v", platforms, r, err)
		}
		if len(platforms) > 0 && len(r) != len(platforms) {
			t.Fatalf("inline platforms widened: %v", r)
		}
		if other, ok := r["fake"]; ok && other.Key == r["cpu"].Key {
			t.Fatal("CPU and GPU cache identities alias")
		}
	}
}

func TestEnvironmentCPUWorkerBuildAndExecute(t *testing.T) {
	fakePixi(t) // Existing offline installer fixture, no package downloads.
	for _, kind := range []string{"inline-uv", "catalog-pixi"} {
		t.Run(kind, func(t *testing.T) {
			q, _ := fixture(t)
			s := cpuSpec(1, .1)
			s.Run.Command = []string{"env-check"}
			if kind == "catalog-pixi" {
				r := pixiRecipe(t, "cpu-lock")
				catalogFile := filepath.Join(filepath.Dir(r.LockFile), "catalog.yaml")
				if err := os.WriteFile(catalogFile, []byte("environments:\n  tools:\n    cpu: {backend: pixi, lock_file: pixi.lock}\n"), 0600); err != nil {
					t.Fatal(err)
				}
				catalog, err := ReadEnvironments(catalogFile)
				if err != nil {
					t.Fatal(err)
				}
				if err := q.ConfigureEnvironments(catalog); err != nil {
					t.Fatal(err)
				}
				s.Environment.Name = "tools"
				s.Run.Command = []string{"sh", "-c", `test "${PATH%%:*}" = "$CONDA_PREFIX/bin" && test "$MYCE_PLATFORM" = cpu && tool`}
			} else {
				lock := filepath.Join(t.TempDir(), "requirements.txt")
				if err := os.WriteFile(lock, []byte("example==1.0\n"), 0600); err != nil {
					t.Fatal(err)
				}
				s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"cpu"}}
			}
			j := submit(t, q, s)
			if j.EnvironmentRecipes["cpu"].Key == "" {
				t.Fatal("CPU job lost its environment recipe")
			}
			ctx, cancel := context.WithCancel(context.Background())
			m := m3Machine() // GPU host serving CPU work through its CPU target.
			w := Worker{StateDir: newTestStateRoot(t), Client: Client{URL: testServerForEnvironments(t, q), Token: "test"}, Machine: m, Interval: time.Millisecond,
				CPUIsolationProbe: func(context.Context) error { return nil },
				InventoryProbe:    func(context.Context) ([]MachineDevice, error) { return slices.Clone(m.Devices), nil },
			}
			if kind == "inline-uv" {
				w.environmentRun = func(_ context.Context, argv []string, dir string, _ []string, _, _ io.Writer) error {
					if argv[0] != "uv" || (argv[1] != "venv" && argv[1] != "pip") {
						return fmt.Errorf("unexpected builder: %v", argv)
					}
					bin := filepath.Join(dir, "venv", "bin")
					if err := os.MkdirAll(bin, 0700); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(bin, "env-check"), []byte("#!/bin/sh\nset -eu\ntest \"${PATH%%:*}\" = \"$VIRTUAL_ENV/bin\"\ntest \"$MYCE_PLATFORM\" = cpu\necho cpu-env-ok\n"), 0700)
				}
			}
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			waitCtx, stopWait := context.WithTimeout(ctx, 10*time.Second)
			defer stopWait()
			got, err := w.Client.Wait(waitCtx, j.ID)
			if err != nil || got.State != "succeeded" || !got.ReleaseConfirmed || len(got.AllocatedDevices) != 0 {
				t.Fatalf("CPU environment job: %+v, %v", got, err)
			}
			logs, err := q.Logs(j.ID, 0)
			want := "cpu-env-ok"
			if kind == "catalog-pixi" {
				want = "activated:yes"
			}
			if err != nil || !strings.Contains(fmt.Sprint(logs), want) {
				t.Fatalf("environment command output: %v, %v", logs, err)
			}
		})
	}
}
