package llamacpp

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

func TestAdapterNameAndRenderArgs(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset"), fixtures.WithModelRef("/models/qwen.gguf"), fixtures.WithContextLength(4096))
	adapter := NewAdapter(Config{Args: []string{"--host={host}", "--port={port}", "--model={model}", "--preset={preset}", "--ctx={ctx}", "--addr={addr}"}})
	if adapter.Name() != "llamacpp" {
		t.Fatalf("name = %s", adapter.Name())
	}
	args := renderArgs(adapter.cfg.Args, preset, "127.0.0.1:8080")
	want := []string{"--host=127.0.0.1", "--port=8080", "--model=/models/qwen.gguf", "--preset=preset", "--ctx=4096", "--addr=127.0.0.1:8080"}
	if strings.Join(args, "\n") != strings.Join(want, "\n") {
		t.Fatalf("args = %+v", args)
	}
}

func TestDefaultConfigUsesSingleServerSlot(t *testing.T) {
	args := strings.Join(DefaultConfig().Args, " ")
	if !strings.Contains(args, "--parallel 1") {
		t.Fatalf("default args must keep llama.cpp context on one slot: %q", args)
	}
}

func TestRenderLaunchArgsIncludesProfileAndPresetTuning(t *testing.T) {
	preset := fixtures.MakePreset(
		fixtures.WithPresetID("preset"),
		fixtures.WithModelRef("/models/qwen.gguf"),
		fixtures.WithLaunchProfile("metal"),
		fixtures.WithLaunchArgs("--n-gpu-layers", "99", "--tensor-split", "1,1"),
	)
	adapter := NewAdapter(Config{
		Args:           []string{"-m", "{model}"},
		LaunchProfiles: map[string][]string{"metal": {"--flash-attn"}},
	})
	args, err := adapter.renderLaunchArgs(preset, "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("renderLaunchArgs: %v", err)
	}
	want := []string{"-m", "/models/qwen.gguf", "--flash-attn", "--n-gpu-layers", "99", "--tensor-split", "1,1"}
	if strings.Join(args, "\n") != strings.Join(want, "\n") {
		t.Fatalf("args = %+v", args)
	}
}

func TestUnknownLaunchProfileFailsLoud(t *testing.T) {
	adapter := NewAdapter(Config{LaunchProfiles: map[string][]string{}})
	_, err := adapter.renderLaunchArgs(fixtures.MakePreset(fixtures.WithLaunchProfile("missing")), "127.0.0.1:1")
	if err == nil || !strings.Contains(err.Error(), "unknown llama.cpp launch profile") {
		t.Fatalf("err = %v", err)
	}
}

func TestWaitReadyReturnsOnHealthyResponse(t *testing.T) {
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))

	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), HTTPClient: client})
	if err := adapter.WaitReady(context.Background(), "ready.test:8080"); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestWaitReadyRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
	if err := adapter.WaitReady(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected context error")
	}
}

func TestLaunchErrorsForProcessStartFailure(t *testing.T) {
	startErr := errors.New("start failed")
	adapter := NewAdapter(Config{BinaryPath: "llama-server", ProcessRunner: &fakeRunner{startErr: startErr}})
	_, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if !errors.Is(err, startErr) {
		t.Fatalf("Launch err = %v", err)
	}
}

func TestLaunchCleansUpWhenContextCanceledAfterStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	process := newFakeProcess(304)
	adapter := NewAdapter(Config{
		BinaryPath:    "llama-server",
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

func TestLaunchAndStopLocalProcess(t *testing.T) {
	process := newFakeProcess(101)
	process.exitOnSignal = true
	adapter := NewAdapter(Config{BinaryPath: "llama-server", Args: []string{"--model", "{model}"}, ProcessRunner: &fakeRunner{next: process}})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("/models/tiny.gguf")), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if got := strings.Join(process.startedArgs, " "); got != "--model /models/tiny.gguf" {
		t.Fatalf("started args = %q", got)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestStopSurfacesProcessRegistryRemoveError(t *testing.T) {
	removeErr := errors.New("remove failed")
	registry := &recordingProcessRegistry{removeErr: removeErr}
	process := newFakeProcess(102)
	process.exitOnSignal = true
	adapter := NewAdapter(Config{
		BinaryPath:      "llama-server",
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

func TestLaunchCleansUpWhenRegistryFails(t *testing.T) {
	registry := &recordingProcessRegistry{addErr: errors.New("registry")}
	process := newFakeProcess(202)
	adapter := NewAdapter(Config{BinaryPath: "llama-server", Args: []string{"60"}, ProcessRegistry: registry, ProcessRunner: &fakeRunner{next: process}})
	_, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if !errors.Is(err, registry.addErr) {
		t.Fatalf("Launch err = %v", err)
	}
	if len(adapter.processes) != 0 || len(registry.added) != 1 {
		t.Fatalf("processes=%+v registry=%+v", adapter.processes, registry)
	}
	if !process.killed {
		t.Fatal("registry failure did not kill process")
	}
}

func TestLaunchContextDoesNotOwnProcessLifetime(t *testing.T) {
	process := newFakeProcess(303)
	process.exitOnSignal = true
	adapter := NewAdapter(Config{
		BinaryPath:    "llama-server",
		ProcessRunner: &fakeRunner{next: process},
	})
	ctx, cancel := context.WithCancel(context.Background())
	handle, err := adapter.Launch(ctx, fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	cancel()
	if process.killed {
		t.Fatal("launch context killed backend process")
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStopSignalsUntrackedPID(t *testing.T) {
	adapter := NewAdapter(Config{})
	if err := adapter.Stop(context.Background(), ports.Handle{PID: 0, Kind: "process", Ref: "sleep"}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStopSignalsUntrackedStoredProcessGroup(t *testing.T) {
	handle, err := execProcessRunner{}.Start(context.Background(), "/bin/sleep", []string{"60"})
	if err != nil {
		t.Fatalf("Start sleep: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- handle.Wait() }()
	pid := handle.PID()
	pgid := processGroupID(pid)
	adapter := NewAdapter(Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := adapter.Stop(ctx, ports.Handle{PID: pid, PGID: pgid, Kind: "llamacpp", Ref: fmt.Sprintf("%d", pid), Binary: "/bin/sleep", Args: []string{"60"}}); err != nil {
		t.Fatalf("Stop untracked: %v", err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("untracked process did not exit")
	}
}

func TestStopKillsUntrackedStoredProcessGroupOnCanceledCleanup(t *testing.T) {
	t.Setenv("MYCELIUM_BACKEND_IGNORE_SIGNALS_HELPER", "1")
	handle, err := execProcessRunner{}.Start(context.Background(), os.Args[0], []string{"-test.run=TestSignalIgnoringHelperProcess"})
	if err != nil {
		t.Fatalf("Start helper: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- handle.Wait() }()
	pid := handle.PID()
	adapter := NewAdapter(Config{StopGracePeriod: 200 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = adapter.Stop(ctx, ports.Handle{PID: pid, Kind: "llamacpp", Ref: fmt.Sprintf("%d", pid), Binary: os.Args[0], Args: []string{"-test.run=TestSignalIgnoringHelperProcess"}})
	if err != nil && !errors.Is(err, context.Canceled) {
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

func TestStopCanceledContextKillsTrackedProcessAndRemovesRef(t *testing.T) {
	registry := &recordingProcessRegistry{}
	process := newFakeProcess(404)
	adapter := NewAdapter(Config{BinaryPath: "llama-server", Args: []string{"60"}, ProcessRegistry: registry, ProcessRunner: &fakeRunner{next: process}})
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
	if len(registry.removed) != 1 {
		t.Fatalf("removed refs = %+v", registry.removed)
	}
}

func TestStopProcessGracefulSignalExit(t *testing.T) {
	process := newFakeProcess(505)
	process.exitOnSignal = true
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
	stopped, err := adapter.stopProcess(context.Background(), process)
	if err != nil || !stopped {
		t.Fatalf("stopProcess = %t %v", stopped, err)
	}
	if process.killed || len(process.signals) != 1 || process.signals[0] != os.Interrupt {
		t.Fatalf("process killed=%t signals=%+v", process.killed, process.signals)
	}
}

func TestStopProcessKillsAfterGracePeriod(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	process := newFakeProcess(606)
	adapter := NewAdapter(Config{Clock: clock, StopGracePeriod: time.Second})
	done := make(chan error, 1)
	go func() {
		stopped, err := adapter.stopProcess(context.Background(), process)
		if !stopped {
			done <- errors.New("process was not stopped")
			return
		}
		done <- err
	}()
	waitForFakeTimer(t, clock)
	clock.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatalf("stopProcess: %v", err)
	}
	if !process.killed {
		t.Fatal("process was not killed after grace period")
	}
}

func TestStopProcessSignalFailureKills(t *testing.T) {
	process := newFakeProcess(707)
	process.signalErr = errors.New("signal failed")
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})
	stopped, err := adapter.stopProcess(context.Background(), process)
	if err != nil || !stopped || !process.killed {
		t.Fatalf("stopProcess stopped=%t err=%v killed=%t", stopped, err, process.killed)
	}
}

func TestStopProcessDoneAndKillErrorBranches(t *testing.T) {
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))})

	doneOnSignal := newFakeProcess(808)
	doneOnSignal.signalErr = os.ErrProcessDone
	doneOnSignal.finish(nil)
	if stopped, err := adapter.stopProcess(context.Background(), doneOnSignal); err != nil || !stopped {
		t.Fatalf("done signal stopped=%t err=%v", stopped, err)
	}

	doneOnKill := newFakeProcess(809)
	doneOnKill.signalErr = errors.New("signal failed")
	doneOnKill.killErr = os.ErrProcessDone
	doneOnKill.finish(nil)
	if stopped, err := adapter.stopProcess(context.Background(), doneOnKill); err != nil || !stopped {
		t.Fatalf("done kill stopped=%t err=%v", stopped, err)
	}

	killErr := errors.New("kill failed")
	killFails := newFakeProcess(810)
	killFails.signalErr = errors.New("signal failed")
	killFails.killErr = killErr
	if stopped, err := adapter.stopProcess(context.Background(), killFails); !errors.Is(err, killErr) || stopped {
		t.Fatalf("kill failure stopped=%t err=%v", stopped, err)
	}
	killFails.finish(nil)
}

func TestStopProcessReturnsWaitError(t *testing.T) {
	process := newFakeProcess(811)
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))})
	done := make(chan error, 1)
	go func() {
		stopped, err := adapter.stopProcess(context.Background(), process)
		if !stopped {
			done <- errors.New("process was not stopped")
			return
		}
		done <- err
	}()
	waitForFakeSignal(t, process)
	waitErr := errors.New("wait failed")
	process.finish(waitErr)
	if err := <-done; !errors.Is(err, waitErr) {
		t.Fatalf("wait err = %v", err)
	}
}

func TestSignalPIDNoopsForInvalidPID(t *testing.T) {
	if err := signalPID(0); err != nil {
		t.Fatalf("signalPID: %v", err)
	}
	_ = signalPID(99999999)
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
	adapter := NewAdapter(Config{})
	err := adapter.Stop(context.Background(), ports.Handle{PID: selfPID, PGID: wrongPGID, Kind: "llamacpp", Ref: "self"})
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
	adapter = NewAdapter(Config{Clock: clock, StopGracePeriod: time.Nanosecond})
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

func TestHealthURLAndSplitAddr(t *testing.T) {
	if got := healthURL("127.0.0.1:8080", "/health"); got != "http://127.0.0.1:8080/health" {
		t.Fatalf("healthURL = %s", got)
	}
	if got := healthURL("http://127.0.0.1:8080/", "/health"); got != "http://127.0.0.1:8080/health" {
		t.Fatalf("healthURL = %s", got)
	}
	host, port := splitAddr("localhost")
	if host != "localhost" || port != "" {
		t.Fatalf("split = %s/%s", host, port)
	}
}

func TestExecProcessRunnerAndOSHandleWrappers(t *testing.T) {
	handle, err := execProcessRunner{}.Start(context.Background(), "/bin/sleep", []string{"1"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if handle.PID() <= 0 {
		t.Fatalf("pid = %d", handle.PID())
	}
	_ = handle.Signal(os.Interrupt)
	_ = handle.Kill()
	_ = handle.Wait()

	current, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	osHandle := osProcessHandle{process: current}
	if osHandle.PID() != os.Getpid() {
		t.Fatalf("os handle pid = %d", osHandle.PID())
	}
	if err := osHandle.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("signal current process: %v", err)
	}

	handle, err = execProcessRunner{}.Start(context.Background(), "/bin/sleep", []string{"1"})
	if err != nil {
		t.Fatalf("Start for os kill: %v", err)
	}
	found, err := os.FindProcess(handle.PID())
	if err != nil {
		t.Fatalf("FindProcess child: %v", err)
	}
	if err := (osProcessHandle{process: found}).Kill(); err != nil {
		t.Fatalf("os kill: %v", err)
	}
	_ = handle.Wait()
}

func TestAdapterSatisfiesPort(t *testing.T) {
	var _ ports.BackendAdapter = NewAdapter(Config{})
	var _ = domain.BackendLlamaCpp
}

type recordingProcessRegistry struct {
	addErr    error
	removeErr error
	added     []domain.ProcessRef
	removed   []domain.ProcessRef
}

func (r *recordingProcessRegistry) Add(_ context.Context, ref domain.ProcessRef) error {
	r.added = append(r.added, ref)
	return r.addErr
}

func (r *recordingProcessRegistry) Remove(_ context.Context, ref domain.ProcessRef) error {
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
	signalErr    error
	killErr      error
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, waitCh: make(chan error, 1)}
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Signal(sig os.Signal) error {
	p.mu.Lock()
	err := p.signalErr
	p.signals = append(p.signals, sig)
	exit := p.exitOnSignal
	p.mu.Unlock()
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

func waitForFakeSignal(t *testing.T, process *fakeProcess) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		process.mu.Lock()
		signaled := len(process.signals) > 0
		process.mu.Unlock()
		if signaled {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("process was not signaled")
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
