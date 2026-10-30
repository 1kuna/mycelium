package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGPUTelemetryClientCommand(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "myce-gpu-client-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "broker.sock")
	server, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		c, err := server.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(time.Second))
		_, err = io.Copy(io.Discard, c)
		if err == nil {
			_, err = io.WriteString(c, `{"sampled_at":"2026-10-30T00:00:00Z","gtt_mm":{"0000:03:00.0":"use_type: 1\nuse_tt: 1\nusage: 28672\n"}}`)
		}
		done <- err
	}()
	var out bytes.Buffer
	if err := gpuTelemetryClientCommand(context.Background(), []string{"--socket", socket}, &out); err != nil || out.String() != `{"sampled_at":"2026-10-30T00:00:00Z","gtt_mm":{"0000:03:00.0":"use_type: 1\nuse_tt: 1\nusage: 28672\n"}}` {
		t.Fatalf("unprivileged client command: %q %v", out.String(), err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := gpuTelemetryClientCommand(context.Background(), []string{"unexpected"}, &out); err == nil {
		t.Fatal("client accepted a positional request payload")
	}
}
