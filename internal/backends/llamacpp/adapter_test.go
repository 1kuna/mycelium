package llamacpp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Now())})
	if err := adapter.WaitReady(context.Background(), server.URL); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestWaitReadyRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := NewAdapter(Config{Clock: mocks.NewFakeClock(time.Now())})
	if err := adapter.WaitReady(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected context error")
	}
}

func TestLaunchErrorsForMissingBinary(t *testing.T) {
	adapter := NewAdapter(Config{BinaryPath: "/missing/llama-server"})
	_, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err == nil {
		t.Fatal("expected missing binary error")
	}
}

func TestLaunchAndStopLocalProcess(t *testing.T) {
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep binary unavailable")
	}
	adapter := NewAdapter(Config{BinaryPath: "sleep", Args: []string{"60"}})
	handle, err := adapter.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := adapter.Stop(context.Background(), handle); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestStopSignalsUntrackedPID(t *testing.T) {
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep binary unavailable")
	}
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	adapter := NewAdapter(Config{})
	if err := adapter.Stop(context.Background(), ports.Handle{PID: cmd.Process.Pid, Kind: "process", Ref: "sleep"}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("process did not stop")
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
