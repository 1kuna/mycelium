package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"runtime"
)

func cpuFingerprint(_ context.Context, probe MachineProbe, _ string, data []byte, _ func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error) {
	var commands [][]string

	if runtime.GOOS == "darwin" {
		commands = [][]string{{"/usr/bin/sw_vers", "-productVersion"}, {"/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"}}
	} else if runtime.GOOS == "linux" {
		commands = [][]string{{"uname", "-r"}}
		facts, err := fs.ReadFile(probe.Files, "proc/cpuinfo")
		if err != nil {
			return nil, nil, errors.New("CPU identity unavailable")
		}
		data = append(data, []byte(cpuIdentity(facts))...)
	} else {
		return nil, nil, errors.New("CPU qualification requires Linux or macOS")
	}
	return data, commands, nil
}
func fakeFingerprint(_ context.Context, _ MachineProbe, _ string, data []byte, _ func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error) {
	return append(data, []byte("/fake-v1")...), nil, nil
}
func metalFingerprint(_ context.Context, _ MachineProbe, _ string, data []byte, _ func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error) {
	return data, [][]string{{"sw_vers", "-productVersion"}, {"system_profiler", "-json", "SPHardwareDataType", "SPDisplaysDataType"}}, nil
}
func cudaFingerprint(_ context.Context, _ MachineProbe, _ string, data []byte, _ func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error) {
	return data, [][]string{{"nvidia-smi", "--query-gpu=uuid,name,driver_version", "--format=csv,noheader"}, {"uname", "-r"}}, nil
}
func xpuFingerprint(ctx context.Context, probe MachineProbe, device string, _ []byte, inventory func(context.Context) ([]MachineDevice, error)) ([]byte, [][]string, error) {
	// Runtime ordinals select a component; they are not its identity. Reuse
	// the shared PCI/Level Zero probe instead of hashing incidental OpenCL
	// enumeration (which can differ between a shell and the worker).
	if inventory == nil {
		inventory = probe.Accelerators
	}
	devices, err := inventory(ctx)
	var selected *MachineDevice
	for i := range devices {
		d := &devices[i]
		if d.Platform == "xpu" && d.ID == device && d.Reason == "" && d.Fingerprint != "" {
			selected = d
			break
		}
	}
	if selected == nil {
		return nil, nil, errors.New("hardware/driver identity unavailable")
	}
	var partial *acceleratorProbeError
	if err != nil && (!errors.As(err, &partial) || partial.failed("xpu")) {
		return nil, nil, errors.New("hardware/driver identity unavailable")
	}
	identity, _ := json.Marshal(struct{ UUID, Driver string }{selected.Identity, selected.Fingerprint})
	data := append([]byte(runtime.GOOS+"/"+runtime.GOARCH+"/xpu/"), identity...)
	return data, [][]string{{"uname", "-r"}}, nil
}
