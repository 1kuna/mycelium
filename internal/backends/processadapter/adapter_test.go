package processadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
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
	clock := mocks.NewFakeClock(time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC))
	adapter := New(Config{
		Name:            "test",
		BinaryPath:      "backend",
		Args:            []string{"--model", "{model}"},
		Clock:           clock,
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
	if registry.added[0].Kind != "test" || registry.added[0].Binary != "backend" || strings.Join(registry.added[0].Args, " ") != "--model model.gguf --ctx preset_test" || !registry.added[0].StartedAt.Equal(clock.Now()) {
		t.Fatalf("process ref metadata = %+v", registry.added[0])
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

func TestStopSurfacesProcessRegistryRemoveError(t *testing.T) {
	removeErr := errors.New("remove failed")
	registry := &recordingRegistry{removeErr: removeErr}
	process := newFakeProcess(203)
	process.exitOnSignal = true
	adapter := New(Config{
		Name:            "test",
		BinaryPath:      "backend",
		ProcessRegistry: registry,
		ProcessRunner:   &fakeRunner{next: process},
	})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); !errors.Is(err, removeErr) {
		t.Fatalf("Stop err = %v", err)
	}
	if len(registry.removed) != 1 {
		t.Fatalf("removed refs = %+v", registry.removed)
	}
}

func TestAdapterStopsUntrackedStoredProcessGroup(t *testing.T) {
	handle, err := execProcessRunner{}.Start(context.Background(), "/bin/sleep", []string{"60"})
	if err != nil {
		t.Fatalf("Start sleep: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- handle.Wait() }()
	pid := handle.PID()
	pgid := processGroupID(pid)
	adapter := New(Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := adapter.Stop(ctx, ports.Handle{PID: pid, PGID: pgid, Kind: "process", Ref: fmt.Sprintf("%d", pid), Binary: "/bin/sleep", Args: []string{"60"}}); err != nil {
		t.Fatalf("Stop untracked: %v", err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("untracked process did not exit")
	}
}

func TestAdapterKillsUntrackedStoredProcessGroupOnCanceledCleanup(t *testing.T) {
	t.Setenv("MYCELIUM_BACKEND_IGNORE_SIGNALS_HELPER", "1")
	handle, err := execProcessRunner{}.Start(context.Background(), os.Args[0], []string{"-test.run=TestSignalIgnoringHelperProcess"})
	if err != nil {
		t.Fatalf("Start helper: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- handle.Wait() }()
	pid := handle.PID()
	adapter := New(Config{StopGracePeriod: 200 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = adapter.Stop(ctx, ports.Handle{PID: pid, Kind: "process", Ref: fmt.Sprintf("%d", pid), Binary: os.Args[0], Args: []string{"-test.run=TestSignalIgnoringHelperProcess"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop err = %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("untracked process group was not killed")
	}
}

func TestSignalIgnoringHelperProcess(t *testing.T) {
	if os.Getenv("MYCELIUM_BACKEND_IGNORE_SIGNALS_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
	select {}
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

func TestLaunchCleansUpWhenContextCanceledAfterStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	process := newFakeProcess(405)
	adapter := New(Config{
		BinaryPath:    "backend",
		ProcessRunner: cancelingRunner{next: process, cancel: cancel},
	})
	_, err := adapter.Launch(ctx, fixtures.MakePreset(), "127.0.0.1:1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Launch err = %v", err)
	}
	if !process.killed {
		t.Fatal("post-start cancellation did not kill process")
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

func TestStopProcessTreatsAlreadyExitedProcessAsStopped(t *testing.T) {
	adapter := New(Config{Clock: mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))})

	process := newFakeProcess(809)
	process.signalErr = os.ErrProcessDone
	process.finish(os.ErrProcessDone)
	stopped, err := adapter.stopProcess(context.Background(), process)
	if !stopped || err != nil {
		t.Fatalf("signal done stopped=%t err=%v", stopped, err)
	}

	process = newFakeProcess(810)
	process.signalErr = errors.New("signal failed")
	process.killErr = os.ErrProcessDone
	process.finish(os.ErrProcessDone)
	stopped, err = adapter.stopProcess(context.Background(), process)
	if !stopped || err != nil {
		t.Fatalf("kill done stopped=%t err=%v", stopped, err)
	}
}

func TestStopProcessReturnsWaitError(t *testing.T) {
	process := newFakeProcess(811)
	adapter := New(Config{Clock: mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))})
	done := make(chan error, 1)
	go func() {
		stopped, err := adapter.stopProcess(context.Background(), process)
		if !stopped {
			done <- errors.New("process was not stopped")
			return
		}
		done <- err
	}()
	<-process.signalCalled
	waitErr := errors.New("wait failed")
	process.finish(waitErr)
	if err := <-done; !errors.Is(err, waitErr) {
		t.Fatalf("wait err = %v", err)
	}
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

func TestExecProcessRunnerAndProcessWrappers(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal wrapper test uses POSIX shell")
	}
	handle, err := execProcessRunner{}.Start(context.Background(), "/bin/sh", []string{"-c", "exit 0"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if handle.PID() <= 0 {
		t.Fatalf("PID = %d", handle.PID())
	}
	if err := handle.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", "trap 'exit 0' TERM; sleep 10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start signal process: %v", err)
	}
	process := execProcess{cmd: cmd}
	if process.PID() <= 0 {
		t.Fatalf("exec process PID = %d", process.PID())
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		_ = process.Kill()
		t.Fatalf("Signal: %v", err)
	}
	_ = process.Wait()

	cmd = exec.Command("/bin/sh", "-c", "sleep 10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start kill process: %v", err)
	}
	process = execProcess{cmd: cmd}
	if err := process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	_ = process.Wait()

	found, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	osHandle := osProcessHandle{process: found}
	if osHandle.PID() != os.Getpid() {
		t.Fatalf("os handle PID = %d", osHandle.PID())
	}
	if err := osHandle.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("os handle signal 0: %v", err)
	}
}

func TestStoredProcessIdentityAndExternalWaitFailures(t *testing.T) {
	selfPID := os.Getpid()
	selfPGID := processGroupID(selfPID)
	if err := verifyProcessIdentity(ports.Handle{}); err == nil || !strings.Contains(err.Error(), "pid is required") {
		t.Fatalf("missing pid err = %v", err)
	}
	if err := verifyProcessIdentity(ports.Handle{PID: selfPID}); err != nil {
		t.Fatalf("zero pgid should skip pgid verification: %v", err)
	}

	wrongPGID := selfPGID + 1
	if wrongPGID == selfPGID {
		wrongPGID++
	}
	adapter := New(Config{})
	err := adapter.Stop(context.Background(), ports.Handle{PID: selfPID, PGID: wrongPGID, Kind: "process", Ref: "self"})
	if err == nil || !strings.Contains(err.Error(), "pgid changed") {
		t.Fatalf("identity mismatch err = %v", err)
	}

	selfProcess, err := os.FindProcess(selfPID)
	if err != nil {
		t.Fatalf("FindProcess self: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopped, err := adapter.waitForExternalExit(ctx, ports.Handle{PID: selfPID}, selfProcess)
	if stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait stopped=%t err=%v", stopped, err)
	}

	clock := mocks.NewFakeClock(time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC))
	adapter = New(Config{Clock: clock, StopGracePeriod: time.Nanosecond})
	done := make(chan error, 1)
	go func() {
		stopped, err := adapter.waitForExternalExit(context.Background(), ports.Handle{PID: selfPID}, selfProcess)
		if stopped {
			done <- errors.New("self process reported stopped")
			return
		}
		done <- err
	}()
	waitForFakeTimer(t, clock)
	clock.Advance(time.Nanosecond)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "did not exit after signal") {
		t.Fatalf("timeout err = %v", err)
	}
}

func TestProcessHandleHelpersForOwnedChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process helper test uses POSIX process signaling")
	}
	cmd := exec.Command("/bin/sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	process := cmd.Process
	if err := signalHandle(ports.Handle{PID: process.Pid}, process, syscall.Signal(0)); err != nil {
		_ = process.Kill()
		_, _ = process.Wait()
		t.Fatalf("signalHandle: %v", err)
	}
	if err := killHandle(ports.Handle{PID: process.Pid}, process); err != nil {
		_ = process.Kill()
		_, _ = process.Wait()
		t.Fatalf("killHandle: %v", err)
	}
	_, _ = process.Wait()

	cmd = exec.Command("/bin/sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start os handle child: %v", err)
	}
	osHandle := osProcessHandle{process: cmd.Process}
	if osHandle.PID() != cmd.Process.Pid {
		t.Fatalf("os handle pid = %d", osHandle.PID())
	}
	if err := osHandle.Kill(); err != nil {
		t.Fatalf("os handle kill: %v", err)
	}
	if err := osHandle.Wait(); err != nil {
		t.Fatalf("os handle wait: %v", err)
	}
}

func TestAdapterSatisfiesBackendPort(t *testing.T) {
	var _ ports.BackendAdapter = New(Config{})
}

type recordingRegistry struct {
	mu        sync.Mutex
	added     []domain.ProcessRef
	removed   []domain.ProcessRef
	err       error
	removeErr error
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
	return r.removeErr
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

type cancelingRunner struct {
	next   *fakeProcess
	cancel context.CancelFunc
}

func (r cancelingRunner) Start(_ context.Context, _ string, _ []string) (ProcessHandle, error) {
	if r.cancel != nil {
		r.cancel()
	}
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
