package myce

import (
	"errors"
)

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

func TestEnvironmentCPUWorkerBuildAndExecute(t *testing.T) {
	fakePixi(t) // Existing offline installer fixture, no package downloads.
	t.Setenv("MYCE_TOKEN", "must-not-reach-builder")
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
			w := Worker{EnvironmentLimit: 1 << 30, StateDir: newTestStateRoot(t), Client: Client{URL: testServerForEnvironments(t, q), Token: "test"}, Machine: m, Interval: time.Millisecond,
				CPUIsolationProbe: func(context.Context) error { return nil },
				InventoryProbe:    func(context.Context) ([]MachineDevice, error) { return slices.Clone(m.Devices), nil },
			}
			if kind == "inline-uv" {
				w.environmentRun = func(_ context.Context, argv []string, dir string, _ []string, _, _ io.Writer) error {
					if data, err := os.ReadFile(filepath.Join(dir, "requirements.txt")); err != nil || string(data) != "example==1.0\n" {
						return fmt.Errorf("staged recipe bytes: %q, %v", data, err)
					}
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
			customRun := w.environmentRun
			w.environmentRun = func(ctx context.Context, argv []string, dir string, env []string, stdout, stderr io.Writer) error {
				vars := map[string]string{}
				for _, e := range env {
					k, v, _ := strings.Cut(e, "=")
					vars[k] = v
				}
				if vars["MYCE_TOKEN"] != "" {
					return errors.New("missing HTTP limits or leaked token")
				}
				if customRun != nil {
					return customRun(ctx, argv, dir, env, stdout, stderr)
				}
				c := &EnvironmentCache{}
				return c.runBuild(ctx, argv, dir, env, stdout, stderr)
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

	t.Run("shared-wheel-second-environment-offline", func(t *testing.T) {
		// #144: a distinct environment using an already-downloaded package can
		// prepare offline; path layout, HTTP defaults and download counts are not pinned.
		cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, DownloadLimit: 1 << 20}
		offline := false
		cache.Run = func(_ context.Context, args []string, _ string, env []string, _, _ io.Writer) error {
			if args[1] == "venv" {
				return nil
			}
			vars := map[string]string{}
			for _, value := range env {
				k, v, _ := strings.Cut(value, "=")
				vars[k] = v
			}
			wheel := filepath.Join(vars["UV_CACHE_DIR"], "shared-wheel")
			if _, err := os.Stat(wheel); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return err
			}
			if offline {
				return errors.New("package download unavailable while preparing second environment")
			}
			return os.WriteFile(wheel, make([]byte, 128<<10), 0600)
		}
		for _, key := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
			recipe := EnvironmentRecipe{Key: key, Backend: "uv", Python: "3.12", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("numpy==2.0.2\n")}}
			_, release, err := cache.Acquire(t.Context(), recipe)
			if err != nil {
				t.Fatal("shared package did not prepare environment", err)
			}
			release()
			offline = true
		}
	})
}
