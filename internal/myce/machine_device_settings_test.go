package myce

import (
	"context"
	"errors"
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
