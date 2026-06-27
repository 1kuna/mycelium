package openvino

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/backends/processadapter"
	"mycelium/internal/domain"
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

func TestNewAdapterNamesOpenVINO(t *testing.T) {
	if got := NewAdapter("openvino-genai-openai").Name(); got != "openvino" {
		t.Fatalf("name = %s", got)
	}
	if got := NewAdapterWithConfig(Config{BinaryPath: "ovms"}).Name(); got != "openvino" {
		t.Fatalf("configured name = %s", got)
	}
}

func TestAdapterLaunchUsesOpenVINOOpenAIWrapperContract(t *testing.T) {
	process := &fakeProcess{pid: 7101, done: make(chan struct{})}
	runner := &recordingRunner{process: process}
	registry := &recordingRegistry{}
	clock := mocks.NewFakeClock(time.Date(2026, 6, 27, 9, 0, 0, 0, time.UTC))
	adapter := NewAdapterWithConfig(Config{
		BinaryPath:      "openvino-genai-openai",
		Args:            []string{"--device", "GPU"},
		ProcessRunner:   runner,
		ProcessRegistry: registry,
		Clock:           clock,
	})
	preset := fixtures.MakePreset(
		fixtures.WithPresetID("gemma-ov"),
		fixtures.WithModelRef("/models/gemma-openvino"),
		fixtures.WithLaunchArgs("--served-model-name", "{preset}"),
	)

	handle, err := adapter.Launch(context.Background(), preset, "127.0.0.1:55374")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	wantArgs := []string{
		"--model", "/models/gemma-openvino",
		"--host", "127.0.0.1",
		"--port", "55374",
		"--device", "GPU",
		"--served-model-name", "gemma-ov",
	}
	if runner.binary != "openvino-genai-openai" || !reflect.DeepEqual(runner.args, wantArgs) {
		t.Fatalf("binary=%q args=%q want %q", runner.binary, runner.args, wantArgs)
	}
	if handle.Kind != "openvino" || handle.Binary != "openvino-genai-openai" || !reflect.DeepEqual(handle.Args, wantArgs) {
		t.Fatalf("handle = %+v", handle)
	}
	if len(registry.added) != 1 || registry.added[0].Kind != "openvino" || registry.added[0].StartedAt != clock.Now().UTC() {
		t.Fatalf("registry added = %+v", registry.added)
	}

	process.exitOnSignal = true
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(registry.removed) != 1 || registry.removed[0].PID != handle.PID {
		t.Fatalf("registry removed = %+v", registry.removed)
	}
}

func TestAdapterWaitReadyUsesOpenVINOHealthPath(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 6, 27, 9, 30, 0, 0, time.UTC))
	var calls int
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/health" {
			t.Fatalf("health path = %s", r.URL.Path)
		}
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	adapter := NewAdapterWithConfig(Config{BinaryPath: "openvino-genai-openai", HTTPClient: client, Clock: clock, PollInterval: time.Second})

	done := make(chan error, 1)
	go func() {
		done <- adapter.WaitReady(context.Background(), "ready.test:8080")
	}()
	waitForFakeTimer(t, clock)
	clock.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if calls != 2 {
		t.Fatalf("health calls = %d", calls)
	}
}

func TestAdapterLaunchFailsLoudForMissingBinaryAndBadAddress(t *testing.T) {
	adapter := NewAdapterWithConfig(Config{ProcessRunner: &recordingRunner{process: &fakeProcess{pid: 7102, done: make(chan struct{})}}})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "openvino backend binary path is required") {
		t.Fatalf("missing binary err = %v", err)
	}

	adapter = NewAdapterWithConfig(Config{BinaryPath: "openvino-genai-openai", ProcessRunner: &recordingRunner{process: &fakeProcess{pid: 7103, done: make(chan struct{})}}})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "not-a-host-port"); err == nil || !strings.Contains(err.Error(), "backend addr must be host:port") {
		t.Fatalf("bad addr err = %v", err)
	}
}

func TestAdapterSurfacesProcessAndRegistryFailures(t *testing.T) {
	startErr := errors.New("start failed")
	adapter := NewAdapterWithConfig(Config{BinaryPath: "openvino-genai-openai", ProcessRunner: &recordingRunner{startErr: startErr}})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); !errors.Is(err, startErr) {
		t.Fatalf("start err = %v", err)
	}

	process := &fakeProcess{pid: 7104, done: make(chan struct{})}
	addErr := errors.New("registry add failed")
	registry := &recordingRegistry{addErr: addErr}
	adapter = NewAdapterWithConfig(Config{
		BinaryPath:      "openvino-genai-openai",
		ProcessRunner:   &recordingRunner{process: process},
		ProcessRegistry: registry,
	})
	if _, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1"); !errors.Is(err, addErr) {
		t.Fatalf("registry err = %v", err)
	}
	if !process.killed || len(registry.added) != 0 {
		t.Fatalf("process killed=%t registry added=%+v", process.killed, registry.added)
	}
}

func TestDefaultPollInterval(t *testing.T) {
	if got := defaultPollInterval(10 * time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("configured poll = %s", got)
	}
	if got := defaultPollInterval(0); got != 250*time.Millisecond {
		t.Fatalf("default poll = %s", got)
	}
}

type recordingRunner struct {
	binary   string
	args     []string
	process  *fakeProcess
	startErr error
}

func (r *recordingRunner) Start(_ context.Context, binary string, args []string) (processadapter.ProcessHandle, error) {
	if r.startErr != nil {
		return nil, r.startErr
	}
	r.binary = binary
	r.args = append([]string(nil), args...)
	return r.process, nil
}

type recordingRegistry struct {
	added   []domain.ProcessRef
	removed []domain.ProcessRef
	addErr  error
}

func (r *recordingRegistry) Add(_ context.Context, ref domain.ProcessRef) error {
	if r.addErr != nil {
		return r.addErr
	}
	r.added = append(r.added, ref)
	return nil
}

func (r *recordingRegistry) Remove(_ context.Context, ref domain.ProcessRef) error {
	r.removed = append(r.removed, ref)
	return nil
}

type fakeProcess struct {
	pid          int
	signaled     bool
	killed       bool
	exitOnSignal bool
	done         chan struct{}
	closed       bool
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Signal(os.Signal) error {
	p.signaled = true
	if p.exitOnSignal {
		p.finish()
	}
	return nil
}

func (p *fakeProcess) Kill() error {
	p.killed = true
	p.finish()
	return nil
}

func (p *fakeProcess) Wait() error {
	<-p.done
	return nil
}

func (p *fakeProcess) finish() {
	if p.closed {
		return
	}
	close(p.done)
	p.closed = true
}

func waitForFakeTimer(t *testing.T, clock *mocks.FakeClock) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if clock.TimerCount() > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fake timer was not created")
		}
		time.Sleep(time.Millisecond)
	}
}
