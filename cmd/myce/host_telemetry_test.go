package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"strconv"
	"testing"
)

func TestHostTelemetryLocalCapture(t *testing.T) {
	// Only process birth metadata for this test and its parent is real. Every
	// RAM/process inventory is injected; no queue, credential or GPU is read.
	queue, worker := os.Getpid(), os.Getppid()
	probe := myce.MachineProbe{OS: "linux", ReadFile: func(path string) ([]byte, error) {
		if path != "/proc/meminfo" {
			t.Fatal("unexpected read", path)
		}
		return []byte("MemTotal: 33554432 kB\nMemAvailable: 25165824 kB\n"), nil
	}, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "ps" {
			t.Fatal("unexpected command", name)
		}
		return []byte(fmt.Sprintf("%d 1 1024\n%d 1 2048\n", queue, worker)), nil
	}}
	var out bytes.Buffer
	args := []string{"capture", "--run-id", "baseline", "--host-ref", "host-a", "--queue-pid", strconv.Itoa(queue), "--worker-pid", strconv.Itoa(worker), "--duration", "50ms", "--interval", "50ms"}
	if err := hostTelemetryCommand(context.Background(), args, probe, &out); err != nil {
		t.Fatal(err)
	}
	var r myce.HostTelemetrySample
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.Schema != myce.HostTelemetrySchema || r.Sequence != 0 || r.Queue.RSS.Value == nil || *r.Queue.RSS.Value != 1<<20 || r.ExternalMemory.Value != nil || r.ExternalMemory.Missing != "attribution_unknown" {
		t.Fatal("incorrect local receipt", r)
	}
	if r.CaptureWindow.End.Sub(r.CaptureWindow.Start).Milliseconds() != 50 {
		t.Fatal("duration not retained")
	}
	if err := hostTelemetryCommand(context.Background(), []string{"capture", "--duration", "0s"}, probe, &out); err == nil {
		t.Fatal("unbounded capture accepted")
	}
}
