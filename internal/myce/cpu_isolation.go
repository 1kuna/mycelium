package myce

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"time"
)

const cpuIsolationUnavailable = "cpu_accelerator_isolation_unavailable"

func (w Worker) cpuRuntime() MachineProbe {
	if w.cpuRuntimeProbe != nil {
		return *w.cpuRuntimeProbe
	}
	return LocalMachineProbe()
}

// Readiness uses the same verification as launch. No command runs in the queue.
func (w Worker) cpuIsolationStatus() string {
	w.cpuIsolation = true
	if err := w.verifyDevice(); err != nil {
		return cpuIsolationUnavailable
	}
	p := w.cpuRuntime()
	if p.OS == "darwin" {
		if p.Arch == "arm64" {
			return "not-enforced"
		}
		for _, d := range w.Machine.Devices {
			if backendFor(d.Platform).cooperativeCPUIsolation {
				return "not-enforced"
			}
		}
	}
	return "select-none"
}

func cpuIsolationDiagnostic(value string) string {
	switch value {
	case "select-none", "not-enforced":
		return value
	}
	return cpuIsolationUnavailable
}

func (w Worker) verifyCPUIsolation() error {
	probe := w.cpuRuntime()
	devices := w.Machine.Devices
	simulated := len(devices) > 0
	for _, d := range devices {
		simulated = simulated && backendFor(d.Platform).simulated
	}
	if !simulated && len(devices) == 0 {
		// Metal has no select-none control. CPU work is cooperative on macOS;
		// qualification reports this explicitly rather than claiming isolation.
		for _, backend := range acceleratorBackends {
			if backend.isolationPresence == "" {
				continue
			}
			if nodes, _ := fs.Glob(probe.Files, backend.isolationPresence); len(nodes) > 0 {
				devices = append(devices, MachineDevice{Platform: backend.platform})
			}
		}
	}
	for _, d := range devices {
		verify := backendFor(d.Platform).verifyCPUIsolation
		if verify == nil {
			return reject(cpuIsolationUnavailable, errors.New("accelerator runtime has no qualified select-none control"))
		}
		if err := verify(probe); err != nil {
			return err
		}
	}
	return nil
}

func verifySimulatedSelectNone(MachineProbe) error { return nil }
func verifyMetalSelectNone(probe MachineProbe) error {
	if probe.OS != "darwin" {
		return reject(cpuIsolationUnavailable, errors.New("Metal CPU execution requires macOS"))
	}
	return nil
}
func verifyCUDASelectNone(probe MachineProbe) error {
	env, _ := WithModelDevice(Model{}, "cpu", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	b, err := probe.CommandEnv(ctx, env.DeviceEnv, "python3", "-c", `import ctypes as c; r=c.CDLL("libcudart.so"); n=c.c_int(); e=r.cudaGetDeviceCount(c.byref(n)); assert e in (0,100) and n.value==0; print("none")`)
	cancel()
	if err != nil || strings.TrimSpace(string(b)) != "none" {
		return reject("cpu_accelerator_isolation_unavailable", errors.New("CUDA select-none runtime enumeration failed"))
	}
	return nil
}
func verifyXPUSelectNone(probe MachineProbe) error {
	env, _ := WithModelDevice(Model{}, "cpu", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	b, err := probe.CommandEnv(ctx, env.DeviceEnv, "sycl-ls")
	cancel()
	if err != nil || strings.Contains(strings.ToLower(string(b)), "gpu") {
		return reject("cpu_accelerator_isolation_unavailable", errors.New("XPU select-none runtime enumeration failed"))
	}
	return nil
}
