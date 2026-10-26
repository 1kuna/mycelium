package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnvironmentSharedDownloadsAndSeparatePruning(t *testing.T) {
	for _, backend := range []string{"uv", "pixi"} {
		t.Run(backend, func(t *testing.T) {
			c := &EnvironmentCache{Root: t.TempDir(), Limit: 64 << 10, DownloadLimit: 1 << 20}
			downloads, prunes := 0, 0
			shared := ""
			c.Run = func(_ context.Context, args []string, dir string, env []string, out, errout io.Writer) error {
				vars := map[string]string{}
				for _, e := range env {
					k, v, _ := strings.Cut(e, "=")
					vars[k] = v
				}
				key := "UV_CACHE_DIR"
				if backend == "pixi" {
					key = "PIXI_CACHE_DIR"
				}
				cache := vars[key]
				if shared != "" && cache != shared {
					return fmt.Errorf("cache changed between environments")
				}
				shared = cache
				if dir != c.Root && strings.HasPrefix(cache, dir+string(os.PathSeparator)) {
					return errors.New("download cache is scoped to environment")
				}
				if vars["MYCE_TOKEN"] != "" || vars["UV_HTTP_TIMEOUT"] != "60" || vars["UV_HTTP_RETRIES"] != "3" {
					return errors.New("missing HTTP limits or leaked token")
				}
				if len(args) > 2 && (args[1] == "cache" || args[1] == "clean") {
					if strings.Join(args, " ") != map[string]string{"uv": "uv cache prune --ci", "pixi": "pixi clean cache --yes"}[backend] {
						return fmt.Errorf("nonstandard pruning: %v", args)
					}
					prunes++
					return os.Remove(filepath.Join(cache, "shared-wheel"))
				}
				if args[1] == "shell-hook" {
					_, err := fmt.Fprint(out, `{"environment_variables":{"PATH":"/usr/bin:/bin"}}`)
					return err
				}
				if args[1] == "venv" {
					return nil
				}
				wheel := filepath.Join(cache, "shared-wheel")
				if _, err := os.Stat(wheel); os.IsNotExist(err) {
					downloads++
					return os.WriteFile(wheel, make([]byte, 128<<10), 0600)
				}
				return nil
			}
			for _, key := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
				r := EnvironmentRecipe{Key: key, Backend: backend, Python: "3.12", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("numpy==2.0.2\n")}}
				if backend == "pixi" {
					r.LockFile = "pixi.lock"
					r.Files = map[string][]byte{"pixi.lock": []byte("fake"), "pixi.toml": []byte("fake")}
				}
				_, release, err := c.Acquire(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
				release()
			}
			if downloads != 1 {
				t.Fatalf("wheel downloads=%d, want 1", downloads)
			}
			// Download bytes exceed the env budget, but not its separate budget.
			c.DownloadLimit = 1
			env := []string{"UV_CACHE_DIR=" + c.downloadCache("uv"), "PIXI_CACHE_DIR=" + c.downloadCache("pixi"), "UV_HTTP_TIMEOUT=60", "UV_HTTP_RETRIES=3"}
			if err := c.pruneDownloads(context.Background(), c.Root, env, io.Discard, c.Run); err != nil {
				t.Fatal(err)
			}
			if prunes != 1 || !c.ready(strings.Repeat("a", 64)) || !c.ready(strings.Repeat("b", 64)) {
				t.Fatal("prune failed or removed installed environments", prunes)
			}
		})
	}
}

func TestEnvironmentStallWatchdogRetriesOnce(t *testing.T) {
	// Budget one installation, including directory blocks and ready.json.
	// APFS and Linux filesystems charge these tiny entries differently.
	footprint := t.TempDir()
	for _, dir := range []string{"bin", ".Trash"} {
		if err := os.Mkdir(filepath.Join(footprint, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(footprint, "partial-wheel-install"), make([]byte, 8<<10), 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(footprint, "ready.json", EnvironmentReport{Key: strings.Repeat("a", 64), State: "ready"}); err != nil {
		t.Fatal(err)
	}
	limit, err := environmentDiskUsage(footprint)
	if err != nil {
		t.Fatal(err)
	}
	for _, recover := range []bool{true, false} {
		t.Run(fmt.Sprint("recover=", recover), func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)}
			c := &EnvironmentCache{Root: t.TempDir(), Limit: limit, Clock: clock}
			calls := 0
			c.Run = func(ctx context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
				calls++
				if err := os.WriteFile(filepath.Join(dir, "partial-wheel-install"), make([]byte, 8<<10), 0600); err != nil {
					return err
				}
				if calls == 2 && recover {
					return nil
				}
				clock.Advance(5*time.Minute + time.Second)
				<-ctx.Done()
				return ctx.Err()
			}
			r := EnvironmentRecipe{Key: strings.Repeat("a", 64), Build: []string{"fake-stalled-installer"}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, release, err := c.Acquire(ctx, r)
			if release != nil {
				release()
			}
			if calls != 2 || (recover && err != nil) || (!recover && !errors.Is(err, errEnvironmentBuildStalled)) {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			log, readErr := os.ReadFile(filepath.Join(c.Root, r.Key+".build.log"))
			if readErr != nil || !strings.Contains(string(log), "no log or download-cache growth for 5 minutes") {
				t.Fatal("missing stall diagnostic", readErr)
			}
			t.Logf("fake clock: %d attempts; recovered=%v; bounded failure=%v", calls, recover, errors.Is(err, errEnvironmentBuildStalled))
		})
	}
}
