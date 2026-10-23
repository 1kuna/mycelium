package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Reconfigure the same binary after conversion, retaining the old caches for rollback.
func TestInstallMachineCacheMigration(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYCE_TOKEN", "")
	c, err := myce.PlanInstall("worker", home, "", home, "http://queue.invalid", "host", myce.Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	c.HostID, c.AllowDeviceless = "host", true
	source := c
	if err = source.WorkerInstance("gpu0"); err != nil {
		t.Fatal(err)
	}
	source.Device = "0"
	sourceData, _ := json.Marshal(source)
	if err = os.MkdirAll(filepath.Dir(source.Config), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(source.Config, sourceData, 0600); err != nil {
		t.Fatal(err)
	}
	previewProbe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("conversion preview ran a command")
		return nil, nil
	}}
	conversion := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe([]string{"install", "worker", "--config", c.Config, "--from-worker-config", source.Config, "--machine-cache-root", "--dry-run"}, previewProbe, home)
	})
	if !strings.Contains(conversion, filepath.Join(c.State, "model-cache")) || !strings.Contains(conversion, filepath.Join(c.State, "environment-cache")) {
		t.Fatal("conversion kept retired defaults")
	}
	c.ModelCache, c.EnvironmentCache = source.ModelCache, source.EnvironmentCache
	c.ModelCacheBytes, c.EnvironmentCacheBytes = 7<<30, 3<<30
	healthy, idle := true, true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/hosts/host/maintenance" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(myce.HostMaintenanceStatus{Host: "host", Policy: myce.HostMaintenance{Mode: "disabled"}, Healthy: healthy, Quiescent: idle})
	}))
	defer server.Close()
	c.Queue = server.URL
	manager := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	binary := filepath.Join(home, "source-binary")
	if err = os.WriteFile(binary, []byte("same binary"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = myce.Install(context.Background(), c, binary, "synthetic-cache-token", manager); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"Version": 1, "Config": c, "Sources": []myce.ServiceConfig{source}})
	if err = os.WriteFile(c.Config+".m4-rollback.json", data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{source.ModelCache, source.EnvironmentCache} {
		if err = myce.PrepareStateRoots(root); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "keep"), []byte("cached artifact"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	probe := myce.MachineProbe{OS: "linux", Arch: "amd64", Command: manager}
	args := []string{"install", "worker", "--config", c.Config, "--machine-cache-root"}
	preview := captureJoiningOutput(t, func() error { return setupCommandWithProbe(append(args, "--dry-run"), probe, home) })
	for _, want := range []string{filepath.Join(c.State, "model-cache"), filepath.Join(c.State, "environment-cache"), "retained", "allocated bytes", "available bytes", "prepared again"} {
		if !strings.Contains(preview, want) {
			t.Fatalf("preview missing %q: %s", want, preview)
		}
	}
	var got myce.ServiceConfig
	install := func(ctx context.Context, next myce.ServiceConfig, _, token string, run func(context.Context, string, ...string) ([]byte, error)) error {
		got = next
		return myce.Install(ctx, next, c.Binary, token, run)
	}
	run := func(extra ...string) error {
		return setupCommandWithInstallerTimeout(append(append([]string{}, args...), extra...), probe, home, myce.LookupWorkerAccount, install, myce.SetupTimeout)
	}
	// No installation may start until the queue reports idle and Healthy.
	idle = false
	if err = run(); err == nil || !strings.Contains(err.Error(), "cache_migration_not_ready") {
		t.Fatal("busy host accepted", err)
	}
	idle, healthy = true, false
	if err = run(); err == nil || !strings.Contains(err.Error(), "cache_migration_not_ready") {
		t.Fatal("unhealthy host accepted", err)
	}
	healthy = true
	injected := errors.New("injected restart failure")
	failed := false
	probe.Command = func(ctx context.Context, name string, a ...string) ([]byte, error) {
		if !failed && strings.Join(a, " ") == "--user restart myce-worker.service" {
			failed = true
			return nil, injected
		}
		return manager(ctx, name, a...)
	}
	if err = run(); !errors.Is(err, injected) {
		t.Fatal("rollback not exercised", err)
	}
	restored, err := myce.ReadServiceConfig(c.Config)
	if err != nil || restored.ModelCache != c.ModelCache || restored.EnvironmentCache != c.EnvironmentCache {
		t.Fatal("cache config not restored", err)
	}
	probe.Command = manager
	if err = run(); err != nil {
		t.Fatal(err)
	}
	want := c
	want.Version = 1
	want.ModelCache = filepath.Join(c.State, "model-cache")
	want.EnvironmentCache = filepath.Join(c.State, "environment-cache")
	if !reflect.DeepEqual(want, got) {
		t.Fatal("migration changed unrelated fields or limits")
	}
	// An ordinary upgrade preserves the explicit saved paths after migration.
	if err = setupCommandWithInstallerTimeout([]string{"install", "worker", "--config", c.Config}, probe, home, myce.LookupWorkerAccount, install, myce.SetupTimeout); err != nil {
		t.Fatal(err)
	}
	if got.ModelCache != want.ModelCache || got.EnvironmentCache != want.EnvironmentCache {
		t.Fatal("upgrade reverted migration")
	}
	// A custom cache is preserved; selecting a cache flag explicitly still wins.
	custom := filepath.Join(home, "custom-model-cache")
	got.ModelCache = custom
	data, _ = json.Marshal(got)
	if err = os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = run("--environment-cache", filepath.Join(home, "explicit-environments")); err != nil {
		t.Fatal(err)
	}
	if got.ModelCache != custom || got.EnvironmentCache != filepath.Join(home, "explicit-environments") {
		t.Fatal("custom override lost")
	}
	for _, root := range []string{source.ModelCache, source.EnvironmentCache} {
		if b, err := os.ReadFile(filepath.Join(root, "keep")); err != nil || string(b) != "cached artifact" {
			t.Fatal("old cache changed", err)
		}
	}
}
