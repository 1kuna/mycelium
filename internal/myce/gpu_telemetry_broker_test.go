package myce

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGPUTelemetryBrokerClient(t *testing.T) {
	for _, mode := range []string{"snapshot", "oversized", "partial timeout", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			// Darwin's Unix socket path limit is shorter than the isolated
			// test TMPDIR. Use the existing short /tmp fixture convention.
			dir, err := os.MkdirTemp("/tmp", "myce-gpu-broker-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "broker.sock")
			var out bytes.Buffer
			if mode == "unavailable" {
				if err := GPUTelemetryBrokerClient(context.Background(), socket, &out); err == nil || out.Len() != 0 {
					t.Fatal("missing broker produced successful telemetry")
				}
				return
			}
			server, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			const snapshot = `{"sampled_at":"2026-10-30T00:00:00Z","gtt_mm":{"0000:03:00.0":"use_type: 1\nuse_tt: 1\nusage: 28672\n"}}`
			done := make(chan error, 1)
			release := make(chan struct{})
			defer close(release)
			go func() {
				c, err := server.Accept()
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(time.Second))
				request, err := io.ReadAll(io.LimitReader(c, 1))
				if err != nil || len(request) != 0 {
					done <- io.ErrUnexpectedEOF
					return
				}
				reply := snapshot
				if mode == "oversized" {
					reply += strings.Repeat(" ", 64<<10)
				}
				_, err = io.WriteString(c, reply)
				done <- err
				if mode == "partial timeout" {
					// A complete-looking JSON prefix without broker EOF is
					// not a complete snapshot and must never reach stdout.
					<-release
				}
			}()
			start := time.Now()
			err = GPUTelemetryBrokerClient(context.Background(), socket, &out)
			if mode == "snapshot" {
				if err != nil || out.String() != snapshot {
					t.Fatalf("broker snapshot changed: %q %v", out.String(), err)
				}
			} else if err == nil || out.Len() != 0 {
				t.Fatalf("incomplete broker response escaped: %d bytes, %v", out.Len(), err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("broker stalled beyond the bounded collection window")
			}
			select {
			case err := <-done:
				if err != nil && mode == "snapshot" {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("broker did not receive the no-payload request")
			}
		})
	}
}
