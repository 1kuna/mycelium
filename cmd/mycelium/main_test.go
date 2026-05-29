package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/catalog"
)

func TestRunDispatchesKnownCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "server", args: []string{"server"}, want: "--node or --join-token is required"},
		{name: "myce", args: []string{"myce"}, want: "usage: myce <add-model>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(%v) err = %v", tt.args, err)
			}
		})
	}
}

func TestRunControlAddModel(t *testing.T) {
	store := t.TempDir()
	model := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(model, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	err := runControl(context.Background(), []string{"add-model", "--store", store, "--id", "tiny", "--model", "tiny-model", model})
	if err != nil {
		t.Fatalf("runControl add-model: %v", err)
	}
	preset, err := catalog.ReadPreset(store, "tiny")
	if err != nil {
		t.Fatalf("ReadPreset: %v", err)
	}
	if preset.ModelRef == model || !strings.Contains(preset.ModelRef, "tiny-tiny.gguf") {
		t.Fatalf("preset = %+v", preset)
	}
}

func TestBuildGatewayServerWithJoinToken(t *testing.T) {
	addr, handler, err := buildGatewayServer(context.Background(), []string{"--listen", "127.0.0.1:0", "--join-token", "secret", "--model", "tiny"})
	if err != nil {
		t.Fatalf("buildGatewayServer: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nodes", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "[]\n" {
		t.Fatalf("nodes status/body = %d %q", rec.Code, rec.Body.String())
	}
}

func TestRunNodeAndServerExitOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runNode(ctx, []string{"--listen", "127.0.0.1:0", "--backend-listen", "127.0.0.1:0"}); err != nil {
		t.Fatalf("runNode canceled: %v", err)
	}
	if err := runServer(ctx, []string{"--listen", "127.0.0.1:0", "--join-token", "secret", "--model", "tiny"}); err != nil {
		t.Fatalf("runServer canceled: %v", err)
	}
}

func TestBuildNodeServer(t *testing.T) {
	addr, handler, err := buildNodeServer([]string{"--listen", "127.0.0.1:0", "--id", "node-a", "--name", "Node A", "--llama-server", "/bin/echo"})
	if err != nil {
		t.Fatalf("buildNodeServer: %v", err)
	}
	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %s", addr)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}
}

func TestJoinedBackendAddrRewritesLoopbackToAdvertisedLANHost(t *testing.T) {
	got := joinedBackendAddr("127.0.0.1:51848", "192.0.2.63:51847")
	if got != "192.0.2.63:51848" {
		t.Fatalf("joinedBackendAddr = %s", got)
	}
	if got := joinedBackendAddr("10.0.0.5:6000", "192.0.2.63:51847"); got != "10.0.0.5:6000" {
		t.Fatalf("explicit backend changed to %s", got)
	}
}

func TestEffectiveAdvertiseAddrUsesActualPortForZeroListen(t *testing.T) {
	got := effectiveAdvertiseAddr("0.0.0.0:0", "127.0.0.1:60000")
	if got != "0.0.0.0:60000" {
		t.Fatalf("effectiveAdvertiseAddr = %s", got)
	}
}

func TestRunRejectsMissingAndUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, []string{"bogus"}} {
		err := run(context.Background(), args)
		if err == nil {
			t.Fatalf("run(%v) expected error", args)
		}
	}
}
