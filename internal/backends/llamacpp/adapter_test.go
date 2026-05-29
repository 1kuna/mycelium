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
