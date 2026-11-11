package myce

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// Called only after the worker verified the physical PCI identity. These node
// readers are shared by backend descriptors, not queue transactions.
type acceleratorNodeProbe struct {
	readFile     func(string) ([]byte, error)
	glob         func(string) ([]string, error)
	evalSymlinks func(string) (string, error)
}

func cudaDeviceNodes(pci string, probe acceleratorNodeProbe) ([]string, error) {
	data, err := probe.readFile(filepath.Join("/proc/driver/nvidia/gpus", pci, "information"))
	if err != nil {
		return nil, errors.New("native_isolation_gpu_mapping_unavailable: NVIDIA kernel minor unavailable")
	}
	for _, line := range strings.Split(string(data), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "Device Minor" {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
			if err != nil {
				break
			}
			return []string{fmt.Sprintf("/dev/nvidia%d", n), "/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"}, nil
		}
	}
	return nil, errors.New("native_isolation_gpu_mapping_unavailable: selected device node not found")
}
func xpuDeviceNodes(pci string, probe acceleratorNodeProbe) ([]string, error) {
	paths, _ := probe.glob("/sys/class/drm/renderD*/device")
	for _, path := range paths {
		resolved, err := probe.evalSymlinks(path)
		if err == nil && normalizePCIAddress(filepath.Base(resolved)) == pci {
			return []string{filepath.Join("/dev/dri", filepath.Base(filepath.Dir(path)))}, nil
		}
	}
	return nil, errors.New("native_isolation_gpu_mapping_unavailable: selected device node not found")
}
