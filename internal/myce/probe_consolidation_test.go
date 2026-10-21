package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func consolidatedProbe(t *testing.T, devices ...pciFixture) MachineProbe {
	t.Helper()
	p := pciFixtureProbe(devices...)
	command, env, read := p.Command, p.CommandEnv, p.ReadFile
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "sycl-ls" || name == "xpu-smi" {
			return nil, exec.ErrNotFound
		}
		if name == "nvidia-smi" && strings.Contains(strings.Join(args, " "), "memory.free") {
			return []byte("24576"), nil
		}
		return command(ctx, name, args...)
	}
	p.CommandEnv = func(ctx context.Context, e map[string]string, name string, args ...string) ([]byte, error) {
		if name != "python3" {
			return p.Command(ctx, name, args...)
		}
		b, err := env(ctx, e, name, args...)
		if err != nil {
			return nil, err
		}
		var rows []map[string]any
		if err = json.Unmarshal(b, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			row["memory_bytes"] = uint64(32) << 30
			row["fingerprint"] = "driver:device without vendor or GPU words"
		}
		return json.Marshal(rows)
	}
	p.ReadFile = func(path string) ([]byte, error) {
		if path == "/proc/meminfo" {
			return []byte("MemAvailable: 104857600 kB\n"), nil
		}
		return read(path)
	}
	return p
}

func TestConsolidatedHostMatrix(t *testing.T) {
	cuda := pciFixture{"0000:01:00.0", "0x10de", "16", "generic", "", true}
	xpu := pciFixture{"0000:02:00.0", "0x8086", "1", "generic", "", true}
	xpu2 := xpu
	xpu2.address = "0000:03:00.0"
	bmc := pciFixture{"0000:04:00.0", "0x1a03", "1", "BMC VGA", "", false}
	driverless := xpu
	driverless.usable = false
	integrated := xpu
	integrated.link = "0"
	spark := cuda
	spark.link = "0"
	for _, tc := range []struct {
		name    string
		devices []pciFixture
		modes   []string
		code    string
	}{
		{"b70", []pciFixture{xpu, xpu2, bmc}, []string{"fixed", "fixed"}, ""},
		{"spark", []pciFixture{spark}, []string{"unified"}, ""},
		{"mixed_driverless", []pciFixture{cuda, driverless}, []string{"dedicated"}, "gpu_driver_missing"},
		{"integrated_plus_cuda", []pciFixture{integrated, cuda}, []string{"dedicated", "unified"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := consolidatedProbe(t, tc.devices...)
			command := p.Command
			p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if tc.name == "b70" && name == "nvidia-smi" {
					return nil, installTestExit(9)
				}
				if tc.name == "spark" && strings.Contains(strings.Join(args, " "), "memory.total") {
					return []byte("generic, N/A, 0000:01:00.0"), nil
				}
				return command(ctx, name, args...)
			}
			if tc.name == "spark" {
				glob, read := p.Glob, p.ReadFile
				p.Glob = func(pattern string) ([]string, error) {
					if strings.Contains(pattern, "renderD") {
						return []string{"/dev/dri/renderD128"}, nil
					}
					return glob(pattern)
				}
				p.ReadFile = func(path string) ([]byte, error) {
					if strings.Contains(path, "renderD128") && strings.HasSuffix(path, "/vendor") {
						return []byte("0x10de"), nil
					}
					return read(path)
				}
				p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
					t.Fatal("CUDA-only render node invoked Level Zero")
					return nil, nil
				}
			}
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if tc.code != "" {
				requirePreflightCode(t, err, tc.code)
			} else if err != nil {
				t.Fatal(err)
			}
			if len(got.Devices) != len(tc.modes) {
				t.Fatalf("partial preflight lost healthy components: %+v err=%v", got, err)
			}
			for i, mode := range tc.modes {
				if got.Devices[i].Detection.MemoryMode != mode {
					t.Fatalf("%+v want %s", got.Devices[i], mode)
				}
			}
			w := Worker{Machine: Machine{GPU: "cuda"}}
			observed, probeErr := w.localInventoryWithProbe(context.Background(), p)
			if len(observed) != len(tc.modes) {
				t.Fatalf("worker lost components: %+v %v", observed, probeErr)
			}
			for i, d := range observed {
				if d.Reason != "" || d.MemoryMode != tc.modes[i] || d.FreeMemoryGB <= 0 {
					t.Fatalf("worker withheld healthy device: %+v err=%v", d, probeErr)
				}
			}
			if tc.name == "spark" && (!observed[0].UnifiedMemory || !observed[0].MemoryMeasured || observed[0].FreeMemoryGB != 84 || observed[0].ReserveMemoryGB != 16 || probeErr != nil) {
				t.Fatalf("shared unified memory/reserve: %+v %v", observed, probeErr)
			}
			w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) { return w.localInventoryWithProbe(ctx, p) }
			w.Machine.Devices = observed
			for _, d := range observed {
				if _, err = w.forAttempt(Job{AllocatedDevices: []string{d.ID}}); err != nil {
					t.Fatalf("healthy component launch: %v", err)
				}
			}
		})
	}
}

func TestConsolidatedAppleWithoutPCISysfs(t *testing.T) {
	p := onboardingProbe("metal")
	p.Glob = func(string) ([]string, error) { t.Fatal("Apple host accessed PCI"); return nil, nil }
	p.ReadFile = func(string) ([]byte, error) { t.Fatal("Apple host accessed PCI"); return nil, nil }
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err != nil || len(got.Devices) != 1 || got.MemoryMode != "unified" || got.ReserveGB != 4 {
		t.Fatal(got, err)
	}
}

func TestConsolidatedEndpointFailureIsolation(t *testing.T) {
	for _, attr := range []string{"class", "vendor", "current_link_width"} {
		t.Run(attr, func(t *testing.T) {
			p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x8086", "1", "healthy", "", true}, pciFixture{"0000:02:00.0", "0x8086", "1", "unreadable", "", true})
			read := p.ReadFile
			p.ReadFile = func(path string) ([]byte, error) {
				if strings.Contains(path, "0000:02:00.0") && strings.HasSuffix(path, "/"+attr) {
					return nil, os.ErrPermission
				}
				return read(path)
			}
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			if len(got.Devices) != 1 || got.Devices[0].Detection.MemoryMode != "fixed" || err == nil {
				t.Fatalf("unreadable endpoint hid healthy sibling: %+v %v", got, err)
			}
			w := Worker{Machine: Machine{GPU: "xpu"}}
			devices, probeErr := w.localInventoryWithProbe(context.Background(), p)
			if len(devices) != 2 || devices[0].Reason != "" || devices[1].Reason == "" {
				t.Fatalf("device-local refusal lost: %+v %v", devices, probeErr)
			}
			w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) { return w.localInventoryWithProbe(ctx, p) }
			w.Machine.Devices = devices
			if _, err = w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err != nil {
				t.Fatalf("healthy same-family launch: %v", err)
			}
			if _, err = w.forAttempt(Job{AllocatedDevices: []string{"1"}}); err == nil {
				t.Fatal("unreadable component launched")
			}
		})
	}
}

func TestConsolidatedDiscreteNAExplicitBudget(t *testing.T) {
	p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x10de", "16", "generic", "", true})
	command := p.Command
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "memory.total") {
			return []byte("generic, N/A"), nil
		}
		return command(ctx, name, args...)
	}
	c := ServiceConfig{Detection: Detection{Platform: "cuda", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2}}
	got, err := PreflightHardware(context.Background(), c, p)
	if err != nil || got.MemoryMode != "fixed" || got.MemoryGB != 24 || got.ReserveGB != 2 {
		t.Fatal(got, err)
	}
}

func TestConsolidatedMissingFamilyTyped(t *testing.T) {
	p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x10de", "16", "generic", "", false}, pciFixture{"0000:02:00.0", "0x8086", "1", "generic", "", true})
	devices, err := p.Accelerators(context.Background())
	var partial *acceleratorProbeError
	if !errors.As(err, &partial) || !partial.failed("cuda") || partial.failed("xpu") || len(devices) != 1 {
		t.Fatal(fmt.Sprint(devices), err)
	}
}

func TestConsolidatedMissingIntegratedRuntimeReported(t *testing.T) {
	p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x10de", "16", "generic", "", true}, pciFixture{"0000:02:00.0", "0x8086", "0", "generic", "", false})
	var notes []string
	p.Info = func(note string) { notes = append(notes, note) }
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err != nil || len(got.Devices) != 1 || got.Platform != "cuda" {
		t.Fatal(got, err)
	}
	if !strings.Contains(strings.Join(notes, "\n"), "needs driver/runtime") {
		t.Fatal("missing integrated runtime was not reported", notes)
	}
}

func TestConsolidatedAbsentCUDAInvalidRuntimeInformational(t *testing.T) {
	for _, output := range []string{"invalid runtime output", "GPU-same, driver\nGPU-same, driver"} {
		t.Run(output, func(t *testing.T) {
			p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x8086", "1", "generic", "", true})
			command := p.Command
			p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name == "nvidia-smi" {
					return []byte(output), nil
				}
				return command(ctx, name, args...)
			}
			var notes []string
			p.Info = func(note string) { notes = append(notes, note) }
			devices, err := (Worker{Machine: Machine{GPU: "xpu"}}).localInventoryWithProbe(context.Background(), p)
			if err != nil || len(devices) != 1 || devices[0].Platform != "xpu" || devices[0].Reason != "" {
				t.Fatal(devices, err)
			}
			if !strings.Contains(strings.Join(notes, "\n"), "without an NVIDIA PCI") {
				t.Fatal("absent CUDA runtime failure was not informational", notes)
			}
		})
	}
}

func TestConsolidatedSparkAbsentXPUHealthy(t *testing.T) {
	p := consolidatedProbe(t, pciFixture{"0000:01:00.0", "0x10de", "0", "GB10", "", true})
	var notes []string
	p.Info = func(line string) { notes = append(notes, line) }
	// An installed but failing Level Zero is irrelevant without an Intel PCI device.
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		return nil, errors.New("Level Zero unavailable")
	}
	glob := p.Glob
	p.Glob = func(pattern string) ([]string, error) {
		if strings.Contains(pattern, "renderD") {
			return []string{"/dev/dri/renderD128"}, nil
		}
		return glob(pattern)
	}
	command := p.Command
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" && strings.Contains(strings.Join(args, " "), "memory.") {
			return []byte("GB10, [N/A]"), nil
		}
		return command(ctx, name, args...)
	}
	q, _ := fixture(t)
	w := Worker{Machine: machine("spark-fixture", "cuda", "spark-fixture-host")}
	w.Machine.FreeMemoryGB, w.Machine.ReservedMemoryGB = 0, 16
	configuration := w
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) {
		return configuration.localInventoryWithProbe(ctx, p)
	}
	devices, err := w.InventoryProbe(context.Background())
	if err != nil || len(devices) != 1 || devices[0].Platform != "cuda" || devices[0].Reason != "" || !devices[0].UnifiedMemory {
		t.Fatal(devices, err)
	}
	if len(notes) == 0 || !strings.Contains(strings.Join(notes, "\n"), "no XPU devices") {
		t.Fatal("absent XPU informational note missing", notes)
	}
	runInventoryReviewWorker(t, q, w)
	awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
	status, err := q.HostMaintenanceStatus("spark-fixture-host")
	if err != nil || !status.Healthy || len(status.Workers) != 1 || len(status.Workers[0].Machine.Devices) != 1 || status.Workers[0].Machine.InventoryReason != "" {
		t.Fatalf("CUDA-only host unhealthy: %+v %v", status, err)
	}
}
