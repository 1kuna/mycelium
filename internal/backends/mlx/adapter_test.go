package mlx

import (
	"context"
	"os"
	"reflect"
	"testing"

	"mycelium/internal/backends/processadapter"
	"mycelium/test/fixtures"
)

func TestNewAdapterNamesMLX(t *testing.T) {
	adapter := NewAdapter("mlx-lm")
	if adapter.Name() != "mlx" {
		t.Fatalf("name = %s", adapter.Name())
	}
	configured := NewAdapterWithConfig(Config{BinaryPath: "mlx_lm.server"})
	if configured.Name() != "mlx" {
		t.Fatalf("configured name = %s", configured.Name())
	}
}

func TestAdapterUsesMLXServerExecutableDirectly(t *testing.T) {
	runner := &recordingRunner{process: &fakeProcess{pid: 42}}

	adapter := NewAdapterWithConfig(Config{BinaryPath: "mlx_lm.server", ProcessRunner: runner})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("mlx-model")), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Stop(context.Background(), handle) })

	want := []string{"--model", "mlx-model", "--host", "127.0.0.1", "--port", "1"}
	if runner.binary != "mlx_lm.server" || !reflect.DeepEqual(runner.args, want) || handle.PID != 42 {
		t.Fatalf("binary=%q args=%q handle=%+v", runner.binary, runner.args, handle)
	}
}

type recordingRunner struct {
	binary  string
	args    []string
	process processadapter.ProcessHandle
}

func (r *recordingRunner) Start(_ context.Context, binary string, args []string) (processadapter.ProcessHandle, error) {
	r.binary = binary
	r.args = append([]string(nil), args...)
	return r.process, nil
}

type fakeProcess struct {
	pid int
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Signal(os.Signal) error {
	return nil
}

func (p *fakeProcess) Kill() error {
	return nil
}

func (p *fakeProcess) Wait() error {
	return nil
}
