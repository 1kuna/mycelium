package vllm

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"mycelium/internal/backends/processadapter"
	"mycelium/internal/backends/vllmargs"
	"mycelium/internal/domain"
)

func TestNewAdapterNamesVLLM(t *testing.T) {
	adapter := NewAdapter("vllm")
	if adapter.Name() != "vllm" {
		t.Fatalf("name = %s", adapter.Name())
	}
	configured := NewAdapterWithConfig(Config{BinaryPath: "vllm"})
	if configured.Name() != "vllm" {
		t.Fatalf("configured name = %s", configured.Name())
	}
}

func TestAdapterLaunchRendersConfiguredArgs(t *testing.T) {
	process := &fakeProcess{pid: 2345, done: make(chan struct{})}
	runner := &recordingRunner{process: process}
	adapter := NewAdapterWithConfig(Config{
		BinaryPath:    "vllm",
		Args:          []string{"--gpu-memory-utilization", "0.85"},
		ProcessRunner: runner,
	})
	preset := domain.Preset{ID: "preset-a", ModelRef: "model-a", LaunchArgs: []string{"--served-model-name", "{preset}"}}
	handle, err := adapter.Launch(context.Background(), preset, "127.0.0.1:54321")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	if handle.PID != process.pid {
		t.Fatalf("handle = %+v", handle)
	}
	want := []string{
		"serve", "model-a", "--host", "127.0.0.1", "--port", "54321",
		"--served-model-name", "preset-a",
		"--gpu-memory-utilization", "0.85",
	}
	if !reflect.DeepEqual(runner.args, want) {
		t.Fatalf("args = %+v want %+v", runner.args, want)
	}
	process.exitOnSignal = true
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !process.signaled {
		t.Fatal("process was not signaled")
	}
}

func TestAdapterNormalizesGPUUtilizationToPresetValue(t *testing.T) {
	process := &fakeProcess{pid: 2346, done: make(chan struct{})}
	runner := &recordingRunner{process: process}
	adapter := NewAdapterWithConfig(Config{
		BinaryPath:    "vllm",
		Args:          []string{"--gpu-memory-utilization", "0.85", "--dtype", "auto"},
		ProcessRunner: runner,
	})
	preset := domain.Preset{
		ID:         "preset-a",
		ModelRef:   "model-a",
		LaunchArgs: []string{"--gpu-memory-utilization=0.25", "--served-model-name", "{preset}"},
	}
	handle, err := adapter.Launch(context.Background(), preset, "127.0.0.1:54321")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	process.exitOnSignal = true
	defer func() { _ = adapter.Stop(context.Background(), handle) }()

	want := []string{
		"serve", "model-a", "--host", "127.0.0.1", "--port", "54321",
		"--dtype", "auto",
		"--served-model-name", "preset-a",
		"--gpu-memory-utilization", "0.25",
	}
	if !reflect.DeepEqual(runner.args, want) {
		t.Fatalf("args = %+v want %+v", runner.args, want)
	}
	util, ok, err := vllmargs.GPUMemoryUtilization(preset.LaunchArgs)
	if err != nil || !ok || util != 0.25 {
		t.Fatalf("preset utilization = %f ok=%t err=%v", util, ok, err)
	}
}

func TestDefaultVLLMPollInterval(t *testing.T) {
	if got := defaultVLLMPollInterval(10 * time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("configured poll = %s", got)
	}
	if got := defaultVLLMPollInterval(0); got != 250*time.Millisecond {
		t.Fatalf("default poll = %s", got)
	}
}

type recordingRunner struct {
	args    []string
	process *fakeProcess
}

func (r *recordingRunner) Start(_ context.Context, _ string, args []string) (processadapter.ProcessHandle, error) {
	r.args = append([]string(nil), args...)
	return r.process, nil
}

type fakeProcess struct {
	pid          int
	signaled     bool
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
