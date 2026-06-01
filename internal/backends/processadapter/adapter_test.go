package processadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAdapterLaunchWaitReadyStop(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer ready.Close()
	addr := strings.TrimPrefix(ready.URL, "http://")
	adapter := New(Config{
		Name:       "test",
		BinaryPath: "/bin/sh",
		Args:       []string{"-c", "sleep 30 # {model} {host} {port}"},
		Clock:      mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
	})

	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("model.gguf")), addr)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if handle.PID == 0 || handle.Addr != addr || adapter.Name() != "test" {
		t.Fatalf("handle = %+v name=%s", handle, adapter.Name())
	}
	if err := adapter.WaitReady(context.Background(), addr); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("idempotent Stop: %v", err)
	}
}

func TestAdapterPersistsProcessRefsAndLaunchArgs(t *testing.T) {
	registry := &recordingRegistry{}
	adapter := New(Config{
		Name:            "test",
		BinaryPath:      "/bin/sh",
		Args:            []string{"-c", "sleep 30 # {model}"},
		ProcessRegistry: registry,
	})
	preset := fixtures.MakePreset(
		fixtures.WithModelRef("model.gguf"),
		fixtures.WithLaunchArgs("--ctx", "{preset}"),
	)
	handle, err := adapter.Launch(context.Background(), preset, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if len(registry.added) != 1 || registry.added[0].PID != handle.PID {
		t.Fatalf("added refs = %+v handle=%+v", registry.added, handle)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(registry.removed) != 1 || registry.removed[0].PID != handle.PID {
		t.Fatalf("removed refs = %+v handle=%+v", registry.removed, handle)
	}
}

func TestAdapterErrorPaths(t *testing.T) {
	adapter := New(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "binary path") {
		t.Fatalf("binary err = %v", err)
	}
	adapter = New(Config{BinaryPath: "/definitely/not/a/mycelium/backend"})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil {
		t.Fatal("expected process start error")
	}
	if _, err := renderArgs(nil, fixtures.MakePreset(), "bad"); err == nil || !strings.Contains(err.Error(), "host:port") {
		t.Fatalf("addr err = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter = New(Config{BinaryPath: "/bin/sh", Args: []string{"-c", "sleep 30"}, Clock: mocks.NewFakeClock(time.Now())})
	if _, err := adapter.Launch(ctx, fixtures.MakePreset(), "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("launch ctx err = %v", err)
	}
	if err := adapter.WaitReady(ctx, "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ready ctx err = %v", err)
	}
	if err := adapter.WaitReady(context.Background(), ""); err == nil {
		t.Fatal("expected empty addr error")
	}
	registry := &recordingRegistry{err: errors.New("store failed")}
	adapter = New(Config{BinaryPath: "/bin/sh", Args: []string{"-c", "sleep 30"}, ProcessRegistry: registry})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "store failed") {
		t.Fatalf("registry err = %v", err)
	}
}

func TestWaitReadyRetriesUntilHealthy(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	calls := 0
	firstCall := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			firstCall <- struct{}{}
			http.Error(w, "not yet", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	adapter := New(Config{Clock: clock, PollInterval: time.Second})
	done := make(chan error, 1)
	go func() {
		done <- adapter.WaitReady(context.Background(), strings.TrimPrefix(server.URL, "http://"))
	}()
	<-firstCall
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(time.Second)
	for {
		select {
		case <-ticker.C:
			clock.Advance(time.Second)
		case <-timeout:
			t.Fatal("WaitReady did not retry")
		case err := <-done:
			if err != nil {
				t.Fatalf("WaitReady: %v", err)
			}
			if calls < 2 {
				t.Fatalf("calls = %d", calls)
			}
			return
		}
	}
}

func TestStopHonorsCanceledContextAfterKill(t *testing.T) {
	adapter := New(Config{BinaryPath: "/bin/sh", Args: []string{"-c", "sleep 30"}})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = adapter.Stop(ctx, handle)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStopKillsProcessAfterGracePeriod(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	readyPath := filepath.Join(t.TempDir(), "ready")
	adapter := New(Config{
		BinaryPath:      "/bin/sh",
		Args:            []string{"-c", "trap '' TERM; touch " + readyPath + "; sleep 30"},
		Clock:           clock,
		StopGracePeriod: time.Second,
	})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	waitForFile(t, readyPath)

	done := make(chan error, 1)
	go func() { done <- adapter.Stop(context.Background(), handle) }()

	timeout := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Stop: %v", err)
			}
			return
		case <-ticker.C:
			clock.Advance(time.Second)
		case <-timeout:
			t.Fatal("Stop did not kill process after grace period")
		}
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", path)
		case <-ticker.C:
		}
	}
}

func TestAdapterSatisfiesBackendPort(t *testing.T) {
	var _ ports.BackendAdapter = New(Config{})
}

type recordingRegistry struct {
	mu      sync.Mutex
	added   []domain.ProcessRef
	removed []domain.ProcessRef
	err     error
}

func (r *recordingRegistry) Add(_ context.Context, ref domain.ProcessRef) error {
	if r.err != nil {
		return r.err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, ref)
	return nil
}

func (r *recordingRegistry) Remove(_ context.Context, ref domain.ProcessRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, ref)
	return nil
}
