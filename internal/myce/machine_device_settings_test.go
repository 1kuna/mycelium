package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMachineDeviceSettingsFixedCapacity(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "cuda", FreeMemoryGB: 24, ReservedMemoryGB: 2}, DeviceSettings: []DeviceInstall{
		{Selector: "GPU-one", Detection: Detection{Platform: "cuda", MemoryMode: "fixed", MemoryGB: 24, ReserveGB: 2}},
		{Selector: "GPU-two", Detection: Detection{Platform: "cuda", MemoryMode: "fixed", MemoryGB: 31, ReserveGB: 3}},
	}}
	probe := MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" && args[0] == "--query-gpu=uuid,driver_version,pci.bus_id" {
			return []byte("GPU-one, driver, 0000:01:00.0\nGPU-two, driver, 0000:02:00.0\n"), nil
		}
		return nil, errors.New("fixed capacity must not require telemetry")
	}}
	probe = withCUDAFacts(probe, false, 2)
	devices, err := w.localInventoryWithProbe(context.Background(), probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 2 {
		t.Fatal(devices)
	}
	for i, want := range []float64{22, 28} {
		d := devices[i]
		if d.MemoryMode != "fixed" || d.BudgetGB != want || d.FreeMemoryGB != want || d.ReserveMemoryGB != float64(i+2) {
			t.Fatalf("device %d lost explicit fixed capacity/reserve: %+v", i, d)
		}
	}
}

func TestMachineDeviceSettingsMeasuredReserve(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "cuda"}, DeviceSettings: []DeviceInstall{{Selector: "GPU-one", Detection: Detection{Platform: "cuda", MemoryMode: "dedicated", MemoryGB: 24, ReserveGB: 5}}}}
	probe := MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" {
			switch args[0] {
			case "--query-gpu=uuid,driver_version,pci.bus_id":
				return []byte("GPU-one, driver\n"), nil
			case "--query-gpu=name,memory.total,pci.bus_id":
				return []byte("CUDA, 32768\n"), nil
			case "--query-gpu=memory.free":
				return []byte("32768\n"), nil
			}
		}
		return nil, errors.New("unexpected probe")
	}}
	probe = withCUDAFacts(probe, false, 1)
	d, err := w.localInventoryWithProbe(context.Background(), probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != 1 || d[0].ReserveMemoryGB != 5 || d[0].BudgetGB != 19 || d[0].MemoryMode != "dedicated" {
		t.Fatal("lost measured capacity mode/reserve", d)
	}
}

// A discrete XPU reports its allocatable memory through Level Zero. A fixed
// install-time budget is a fallback for cards that cannot report one, never a
// cap below (or above) the report; an auto-mode --memory-gb still caps.
func TestXPUDetectedMemoryOutranksStaleFixedBudget(t *testing.T) {
	xpu := pciFixture{"0000:02:00.0", "0x8086", "1", "generic", "", true}
	xpu2 := xpu
	xpu2.address = "0000:03:00.0"
	const detected, reserve = 32.0, 0.25
	inventory := func(t *testing.T, w Worker, reported bool) []MachineDevice {
		t.Helper()
		p := consolidatedProbe(t, xpu, xpu2)
		if !reported {
			env := p.CommandEnv
			p.CommandEnv = func(ctx context.Context, e map[string]string, name string, args ...string) ([]byte, error) {
				b, err := env(ctx, e, name, args...)
				if name == "python3" && err == nil {
					b = []byte(strings.ReplaceAll(string(b), `"memory_bytes":34359738368`, `"memory_bytes":0`))
				}
				return b, err
			}
		}
		devices, err := w.localInventoryWithProbe(context.Background(), p)
		if err != nil || len(devices) != 2 {
			t.Fatal(devices, err)
		}
		return devices
	}
	settings := func(mode string, memory float64) []DeviceInstall {
		return []DeviceInstall{
			{Selector: "0", Detection: Detection{Platform: "xpu", MemoryMode: mode, MemoryGB: memory, ReserveGB: reserve}},
			{Selector: "1", Detection: Detection{Platform: "xpu", MemoryMode: mode, MemoryGB: memory, ReserveGB: reserve}},
		}
	}
	for _, tc := range []struct {
		name     string
		w        Worker
		reported bool
		total    float64
		budget   float64
	}{
		{"fixed below detection", Worker{Machine: Machine{GPU: "xpu"}, DeviceSettings: settings("fixed", 31)}, true, detected, detected - reserve},
		{"fixed above detection", Worker{Machine: Machine{GPU: "xpu"}, DeviceSettings: settings("fixed", 40)}, true, detected, detected - reserve},
		{"legacy scalar fixed", Worker{Machine: Machine{GPU: "xpu", FreeMemoryGB: 30, ReservedMemoryGB: reserve}, FixedAcceleratorMemory: true}, true, detected, detected - reserve},
		{"auto cap", Worker{Machine: Machine{GPU: "xpu"}, DeviceSettings: settings("auto", 20)}, true, detected, 20 - reserve},
		{"fixed without detection", Worker{Machine: Machine{GPU: "xpu"}, DeviceSettings: settings("fixed", 31)}, false, 31, 31 - reserve},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, d := range inventory(t, tc.w, tc.reported) {
				if d.Reason != "" || d.MemoryMode != "fixed" || d.TotalMemoryGB != tc.total || d.ReserveMemoryGB != reserve || d.BudgetGB != tc.budget || d.FreeMemoryGB != tc.budget {
					t.Fatalf("want total %v budget %v reserve %v, got %+v", tc.total, tc.budget, reserve, d)
				}
			}
		})
	}
}
