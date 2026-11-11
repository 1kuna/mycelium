package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	generate "github.com/invopop/jsonschema"
)

func TestHostTelemetryCaptureLocalProbe(t *testing.T) {
	p := LocalMachineProbe()
	p.OS = "linux" // Exercise Linux initialization without reading this host.
	p.Files = fstest.MapFS{"proc/meminfo": {Data: []byte("MemTotal: 33554432 kB\nMemAvailable: 25165824 kB\n")}}
	p.Command = func(context.Context, string, ...string) ([]byte, error) { return []byte("10 1 1024\n20 1 2048\n"), nil }
	c := HostTelemetryCapture{Probe: p, RunID: "baseline", HostRef: "host-a", QueuePID: 10, WorkerPID: 20, Birth: func(int) (string, error) { return "birth", nil }}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	r, err := c.Sample(context.Background())
	if err != nil || r.HostUsed.Value == nil || *r.HostUsed.Value != 8<<30 {
		t.Fatalf("production filesystem adapter: %+v, %v", r.HostUsed, err)
	}
}

func TestHostTelemetryCaptureSensorDescendants(t *testing.T) {
	for _, mode := range []string{"cancel", "exited-parent"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := captureFixture(t)
			dir := t.TempDir()
			pidPath := filepath.Join(dir, "child.pid")
			// uv is an allowed fake-tool boundary in LocalMachineProbe. No hardware
			// command runs: only this owned shell and its sleeping descendant.
			helper := filepath.Join(dir, "uv")
			if err := os.WriteFile(helper, []byte("#!/bin/sh\nsleep 10 &\nprintf '%s' \"$!\" > \"$1\"\nif [ \"$2\" = exited-parent ]; then exit 0; fi\nwait\n"), 0700); err != nil {
				t.Fatal(err)
			}
			run, original := LocalMachineProbe().Command, c.Probe.Command
			ready := make(chan struct{})
			c.Probe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "xpu-smi" {
					return original(ctx, name, args...)
				}
				close(ready)
				return run(ctx, helper, pidPath, mode)
			}
			c.Probe.XeActivity = nil
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.Sample(ctx); done <- err }()
			<-ready
			var pid int
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if b, err := os.ReadFile(pidPath); err == nil {
					pid, _ = strconv.Atoi(string(b))
					if pid > 0 {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			if pid <= 0 {
				t.Fatal("scratch sensor did not start")
			}
			defer syscall.Kill(pid, syscall.SIGKILL) // Only our scratch descendant.
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if mode == "cancel" && !errors.Is(err, context.Canceled) || mode != "cancel" && err != nil {
					t.Errorf("canceled sample returned %v", err)
				}
			case <-time.After(testWaitTimeout):
				_ = syscall.Kill(pid, syscall.SIGKILL)
				<-done
				t.Fatal("sensor deadline left inherited pipes blocked")
			}
			state, err := processPS("-p", strconv.Itoa(pid), "-o", "stat=")
			// Linux can retain a dead orphan until init reaps it; zombies cannot run.
			if err == nil && strings.TrimSpace(string(state)) != "" && !strings.HasPrefix(strings.TrimSpace(string(state)), "Z") {
				t.Fatalf("owned sensor descendant still running: %s", state)
			}
		})
	}
}

func captureFixture(t *testing.T) (*HostTelemetryCapture, *fakeClock) {
	t.Helper()
	_, clock := fixture(t)
	c := &HostTelemetryCapture{Clock: clock, RunID: "baseline", HostRef: "host-a", CaptureID: "capture-a", QueuePID: 10, WorkerPID: 20,
		NativeWorkerOnly: true, Cards: []HostTelemetryCard{{Ref: "card-a", PCI: "0000:01:00.0"}}, Birth: func(int) (string, error) { return "birth-a", nil }}
	c.Probe = MachineProbe{OS: "linux", ReadFile: func(path string) ([]byte, error) {
		if path != "/proc/meminfo" {
			t.Fatal("unexpected read", path)
		}
		clock.Advance(time.Millisecond)
		return []byte("MemTotal: 33554432 kB\nMemAvailable: 25165824 kB\n"), nil
	}, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		clock.Advance(time.Millisecond)
		switch name {
		case "ps":
			return []byte("10 1 1048576\n20 1 524288\n21 20 1572864\n"), nil
		case "xpu-smi":
			if strings.Join(args, " ") != "--id=0000:01:00.0 --query-gpu=pci.bus_id,memory.total,memory.used,memory.free --format=csv,noheader,nounits" {
				t.Fatal("wrong supported query", args)
			}
			return []byte("0000:01:00.0, 8192, 2048, 6144\n"), nil
		}
		t.Fatal("unexpected command", name)
		return nil, errors.New("unexpected")
	}, XeActivity: func(context.Context, string) (float64, error) {
		clock.Advance(attemptMemorySamplePeriod)
		return 0, nil // A measured idle zero must survive serialization.
	}}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	return c, clock
}

func TestHostTelemetryCaptureAlignmentAndMissing(t *testing.T) {
	c, _ := captureFixture(t)
	r, err := c.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if *r.Queue.RSS.Value != 1<<30 || *r.Worker.RSS.Value != 1<<29 || *r.WorkerTreeRSS.Value != 2<<30 || *r.HostUsed.Value != 8<<30 || *r.ExternalMemory.Value != 6<<30 {
		t.Fatal("incorrect shared-inventory residual", r)
	}
	card := r.Cards[0]
	if *card.Used.Value != 2<<30 || *card.Free.Value != 6<<30 || *card.Activity.Value != 0 || card.Activity.Window.End.Sub(card.Activity.Window.Start) != attemptMemorySamplePeriod {
		t.Fatal("memory/activity coverage lost", card)
	}
	if !r.Queue.RSS.Window.End.Before(card.Activity.Window.Start) {
		t.Fatal("independent sensors claimed simultaneous windows")
	}
	c.Probe.XeActivity = func(context.Context, string) (float64, error) { return 0, errors.New("permission denied") }
	c.NativeWorkerOnly = false
	r, err = c.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Cards[0].Activity.Value != nil || r.Cards[0].Activity.SampledAt != nil || r.Cards[0].Activity.Missing == "" || r.ExternalMemory.Value != nil || r.ExternalMemory.Missing != "attribution_unknown" {
		t.Fatal("missing counters became zero or invented attribution", r)
	}
	command := c.Probe.Command
	c.Probe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "xpu-smi" {
			return []byte("0000:01:00.0, 8192, N/A, 6144\n"), nil
		}
		return command(ctx, name, args...)
	}
	r, err = c.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, reading := range []TelemetryReading{r.Cards[0].Total, r.Cards[0].Used, r.Cards[0].Free} {
		if reading.Value != nil || reading.SampledAt != nil || reading.Missing == "" {
			t.Fatal("incomplete card survey became a measured value", reading)
		}
	}
}

func TestHostTelemetryCapturePIDReuse(t *testing.T) {
	c, _ := captureFixture(t)
	c.Birth = func(pid int) (string, error) {
		if pid == c.QueuePID {
			return "reused", nil
		}
		return "birth-a", nil
	}
	r, err := c.Sample(context.Background())
	if err == nil || !strings.Contains(err.Error(), "process_identity_changed") || r.Queue.RSS.Value != nil || r.Queue.RSS.Missing != "process_identity_changed" {
		t.Fatal("PID reuse was attributed to the queue", r, err)
	}
}

func TestHostTelemetryCaptureSchema(t *testing.T) {
	r := generate.Reflector{Anonymous: true, AllowAdditionalProperties: true}
	b, err := json.Marshal(r.Reflect(&HostTelemetrySample{}))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal(b, &document); err != nil {
		t.Fatal(err)
	}
	document["title"] = "Mycelium benchmark host telemetry JSONL record v1"
	document["description"] = "One local host sample per line. A reading has either an observed value and sample time, or a missing reason. Windows bound sensor reads, not continuous coverage. Host/card readings are not per-attempt allocations; retained job facts carry exact attempt bindings."
	document["$defs"].(map[string]any)["HostTelemetrySample"].(map[string]any)["properties"].(map[string]any)["schema"] = map[string]any{"const": HostTelemetrySchema}
	reading := document["$defs"].(map[string]any)["TelemetryReading"].(map[string]any)
	reading["oneOf"] = []any{
		map[string]any{"required": []string{"value", "sampled_at"}, "not": map[string]any{"required": []string{"missing"}}},
		map[string]any{"required": []string{"missing"}, "not": map[string]any{"anyOf": []any{map[string]any{"required": []string{"value"}}, map[string]any{"required": []string{"sampled_at"}}}}},
	}
	props := reading["properties"].(map[string]any)
	props["value"].(map[string]any)["minimum"] = 0
	props["unit"].(map[string]any)["enum"] = []string{"bytes", "percent"}
	props["missing"].(map[string]any)["minLength"] = 1
	b, err = json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join(schemaRoot, "host-telemetry-v1.schema.json")
	if os.Getenv("MYCE_UPDATE_SCHEMAS") == "1" {
		if err = os.WriteFile(path, b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil || string(want) != string(b) {
		t.Fatal("host telemetry schema drift: regenerate with MYCE_UPDATE_SCHEMAS=1", err)
	}
	schema := compileWireSchema(t, "host-telemetry-v1.schema.json")
	c, _ := captureFixture(t)
	sample, err := c.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schema, sample)
	// An unavailable reading must never retain a numeric value, even zero.
	sample.Cards[0].Activity.Missing = "read_failed"
	b, err = json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	var invalid any
	if err = json.Unmarshal(b, &invalid); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(invalid); err == nil {
		t.Fatal("schema accepted a missing counter with a numeric value")
	}
	c.Probe.XeActivity = nil
	c.NativeWorkerOnly = false
	sample, err = c.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schema, sample)
}
