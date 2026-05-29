package main

import (
	"context"
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
		{name: "server", args: []string{"server"}, want: "--node is required"},
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

func TestRunRejectsMissingAndUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, []string{"bogus"}} {
		err := run(context.Background(), args)
		if err == nil {
			t.Fatalf("run(%v) expected error", args)
		}
	}
}
