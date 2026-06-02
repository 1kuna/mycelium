package processadapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func directHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
}

func TestAdapterLaunchWaitReadyStop(t *testing.T) {
	readyClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	addr := "ready.test:8080"
	process := newFakeProcess(101)
	process.exitOnSignal = true
	adapter := New(Config{
		Name:          "test",
		BinaryPath:    "backend",
		Args:          []string{"--model", "{model}", "--host", "{host}", "--port", "{port}"},
		Clock:         mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		HTTPClient:    readyClient,
		ProcessRunner: &fakeRunner{next: process},
	})

	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("model.gguf")), addr)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if handle.PID == 0 || handle.Addr != addr || adapter.Name() != "test" {
		t.Fatalf("handle = %+v name=%s", handle, adapter.Name())
	}
	if got := strings.Join(process.startedArgs, " "); got != "--model model.gguf --host ready.test --port 8080" {
		t.Fatalf("started args = %q", got)
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
	process := newFakeProcess(202)
	process.exitOnSignal = true
	adapter := New(Config{
		Name:            "test",
		BinaryPath:      "backend",
		Args:            []string{"--model", "{model}"},
		ProcessRegistry: registry,
		ProcessRunner:   &fakeRunner{next: process},
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
	if got := strings.Join(process.startedArgs, " "); got != "--model model.gguf --ctx preset_test" {
		t.Fatalf("started args = %q", got)
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
	startErr := errors.New("start failed")
	adapter = New(Config{BinaryPath: "backend", ProcessRunner: &fakeRunner{startErr: startErr}})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil {
		t.Fatal("expected process start error")
	}
	if _, err := renderArgs(nil, fixtures.MakePreset(), "bad"); err == nil || !strings.Contains(err.Error(), "host:port") {
		t.Fatalf("addr err = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter = New(Config{BinaryPath: "backend", ProcessRunner: &fakeRunner{next: newFakeProcess(303)}, Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
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
	failedProcess := newFakeProcess(404)
	adapter = New(Config{BinaryPath: "backend", ProcessRegistry: registry, ProcessRunner: &fakeRunner{next: failedProcess}})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "store failed") {
		t.Fatalf("registry err = %v", err)
	}
	if !failedProcess.killed {
		t.Fatal("registry failure did not kill launched process")
	}
}

func TestWaitReadyRetriesUntilHealthy(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	calls := 0
	firstCall := make(chan struct{}, 1)
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "not yet", http.StatusServiceUnavailable)
			firstCall <- struct{}{}
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	adapter := New(Config{Clock: clock, PollInterval: time.Second, HTTPClient: client})
	done := make(chan error, 1)
	go func() {
		done <- adapter.WaitReady(context.Background(), "ready.test:8080")
	}()
	<-firstCall
	waitForFakeTimer(t, clock)
	clock.Advance(time.Second)
	finished := false
	for i := 0; i < 1000; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("WaitReady: %v", err)
			}
			finished = true
		default:
			runtime.Gosched()
		}
		if finished {
			break
		}
	}
	if !finished {
		t.Fatal("WaitReady did not retry")
	}
	if calls < 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestWaitReadyRequestAndTransportErrors(t *testing.T) {
	adapter := New(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
	if err := adapter.WaitReady(context.Background(), "[%"); err == nil {
		t.Fatal("expected malformed ready URL error")
	}

	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	firstCall := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		firstCall <- struct{}{}
		return nil, errors.New("down")
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter = New(Config{Clock: clock, PollInterval: time.Second, HTTPClient: client})
	done := make(chan error, 1)
	go func() { done <- adapter.WaitReady(ctx, "ready.test:8080") }()
	<-firstCall
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady err = %v", err)
	}
}

func TestStopHonorsCanceledContextAfterKill(t *testing.T) {
	process := newFakeProcess(505)
	adapter := New(Config{BinaryPath: "backend", ProcessRunner: &fakeRunner{next: process}})
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
	if !process.killed {
		t.Fatal("canceled Stop did not kill tracked process")
	}
}

func TestStopKillsProcessAfterGracePeriod(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	process := newFakeProcess(606)
	adapter := New(Config{
		BinaryPath:      "/bin/sh",
		ProcessRunner:   &fakeRunner{next: process},
		Clock:           clock,
		StopGracePeriod: time.Second,
	})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- adapter.Stop(context.Background(), handle) }()

	<-process.signalCalled
	waitForFakeTimer(t, clock)
	clock.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !process.killed {
		t.Fatal("Stop did not kill process after grace period")
	}
}

func TestStopHandlesSignalFailure(t *testing.T) {
	process := newFakeProcess(707)
	process.signalErr = errors.New("signal failed")
	adapter := New(Config{BinaryPath: "backend", ProcessRunner: &fakeRunner{next: process}})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !process.killed {
		t.Fatal("signal failure did not kill process")
	}

	killErr := errors.New("kill failed")
	process = newFakeProcess(808)
	process.signalErr = errors.New("signal failed")
	process.killErr = killErr
	adapter = New(Config{BinaryPath: "backend", ProcessRunner: &fakeRunner{next: process}})
	handle, err = adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); !errors.Is(err, killErr) {
		t.Fatalf("Stop err = %v", err)
	}
	process.finish(nil)
}

func TestStopNoopsWithoutPIDOrTrackedProcess(t *testing.T) {
	registry := &recordingRegistry{}
	adapter := New(Config{ProcessRegistry: registry})
	if err := adapter.Stop(context.Background(), ports.Handle{}); err != nil {
		t.Fatalf("zero Stop: %v", err)
	}
	if err := adapter.Stop(context.Background(), ports.Handle{PID: 999, Kind: "process", Ref: "missing"}); err != nil {
		t.Fatalf("untracked Stop: %v", err)
	}
	if len(registry.removed) != 1 {
		t.Fatalf("removed refs = %+v", registry.removed)
	}
}

func TestExecProcessPID(t *testing.T) {
	cmd := &exec.Cmd{Process: &os.Process{Pid: 1234}}
	if got := (execProcess{cmd: cmd}).PID(); got != 1234 {
		t.Fatalf("PID = %d", got)
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

type fakeRunner struct {
	next     *fakeProcess
	startErr error
	starts   []fakeStart
}

type fakeStart struct {
	binary string
	args   []string
}

func (r *fakeRunner) Start(_ context.Context, binary string, args []string) (ProcessHandle, error) {
	r.starts = append(r.starts, fakeStart{binary: binary, args: append([]string(nil), args...)})
	if r.startErr != nil {
		return nil, r.startErr
	}
	if r.next == nil {
		r.next = newFakeProcess(999)
	}
	r.next.startedArgs = append([]string(nil), args...)
	return r.next, nil
}

type fakeProcess struct {
	mu           sync.Mutex
	pid          int
	waitCh       chan error
	done         bool
	killed       bool
	exitOnSignal bool
	startedArgs  []string
	signals      []os.Signal
	signalCalled chan struct{}
	signalErr    error
	killErr      error
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, waitCh: make(chan error, 1), signalCalled: make(chan struct{}, 1)}
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Signal(sig os.Signal) error {
	p.mu.Lock()
	p.signals = append(p.signals, sig)
	exit := p.exitOnSignal
	err := p.signalErr
	p.mu.Unlock()
	p.signalCalled <- struct{}{}
	if err != nil {
		return err
	}
	if exit {
		p.finish(nil)
	}
	return nil
}

func (p *fakeProcess) Kill() error {
	p.mu.Lock()
	err := p.killErr
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.killed = true
	p.mu.Unlock()
	p.finish(nil)
	return nil
}

func (p *fakeProcess) Wait() error {
	return <-p.waitCh
}

func (p *fakeProcess) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	p.waitCh <- err
}

func waitForFakeTimer(t *testing.T, clock *mocks.FakeClock) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if clock.TimerCount() > 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("timer was not registered")
}
