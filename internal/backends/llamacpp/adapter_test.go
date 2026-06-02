package llamacpp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestSignalPIDNoopsForInvalidPID(t *testing.T) {
	if err := signalPID(0); err != nil {
		t.Fatalf("signalPID: %v", err)
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

func TestAdapterSatisfiesPort(t *testing.T) {
	var _ ports.BackendAdapter = NewAdapter(Config{})
	var _ = domain.BackendLlamaCpp
}

type recordingProcessRegistry struct {
	addErr  error
	added   []domain.ProcessRef
	removed []domain.ProcessRef
}

func (r *recordingProcessRegistry) Add(_ context.Context, ref domain.ProcessRef) error {
	r.added = append(r.added, ref)
	return r.addErr
}

func (r *recordingProcessRegistry) Remove(_ context.Context, ref domain.ProcessRef) error {
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
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, waitCh: make(chan error, 1)}
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Signal(sig os.Signal) error {
	p.mu.Lock()
	p.signals = append(p.signals, sig)
	exit := p.exitOnSignal
	p.mu.Unlock()
	if exit {
		p.finish(nil)
	}
	return nil
}

func (p *fakeProcess) Kill() error {
	p.mu.Lock()
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
