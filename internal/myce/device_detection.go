package myce

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// DetectDevice selects from the same physical/runtime observation used by the
// worker. A sibling failure never substitutes a different selected component.
func (p MachineProbe) DetectDevice(ctx context.Context, selector string) (Detection, error) {
	if err := singleDeviceMask(selector); err != nil {
		return Detection{}, err
	}
	devices, probeErr := p.Accelerators(ctx)
	for _, device := range devices {
		if selector != "" && selector != device.Selector && selector != device.ID && selector != device.Identity {
			continue
		}
		if selector == "" && len(devices) != 1 {
			return Detection{}, errors.New("automatic setup requires exactly one GPU")
		}
		return p.detectAccelerator(ctx, device)
	}
	if probeErr != nil {
		return Detection{}, probeErr
	}
	return Detection{}, ErrNoGPU
}

// Memory telemetry never supplies topology or runtime presence. All callers
// consume the runtime identity and PCI facts collected together.
func (p MachineProbe) detectAccelerator(ctx context.Context, device MachineDevice) (Detection, error) {
	d := Detection{OS: p.OS, Arch: p.Arch, Platform: device.Platform, GPU: device.Fingerprint}
	if device.Reason != "" {
		return d, preflightError(device.Reason, device.Selector, "component facts are unavailable or ambiguous; restore access and re-detect")
	}
	if device.Platform == "metal" {
		d.MemoryMode, d.ReserveGB = "unified", 4
		return d, nil
	}
	fact := device.pciFact
	if fact == nil || !fact.topologyKnown {
		return d, preflightError("gpu_memory_unavailable", device.Platform, "GPU topology unavailable; restore PCI/sysfs access or specify an explicit fixed device budget")
	}
	if fact.integrated {
		d.MemoryMode = "unified"
		if device.Platform == "cuda" {
			d.ReserveGB = 16
		} else {
			d.ReserveGB = 8
		}
		return d, nil
	}
	if device.Platform == "cuda" {
		b, err := p.Command(ctx, "nvidia-smi", "--query-gpu=name,memory.total,pci.bus_id", "--format=csv,noheader,nounits", "--id="+device.Selector)
		if err != nil {
			return d, preflightError("gpu_memory_unavailable", "cuda", "CUDA memory unavailable; repair telemetry or specify an explicit fixed device budget")
		}
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		fields := strings.Split(lines[0], ",")
		if len(lines) != 1 || len(fields) < 2 || len(fields) > 3 {
			return d, preflightError("gpu_memory_unavailable", "cuda", "invalid CUDA memory inventory")
		}
		if len(fields) == 3 && normalizePCIAddress(fields[2]) != normalizePCIAddress(fact.address) {
			return d, preflightError("gpu_inventory_invalid", "cuda", "CUDA memory response differs from selected PCI component")
		}
		n, err := parseCUDAFreeMemory([]byte(strings.TrimSpace(fields[1])))
		if errors.Is(err, errUnifiedMemory) {
			// A PCI transport can exist inside an integrated SoC (GB10 has
			// current_link_width=1). N/A is not topology evidence. Ask the
			// CUDA driver for the selected PCI device's integrated property.
			b, propertyErr := p.Command(ctx, "python3", "-c", cudaIntegratedAttribute, fact.address)
			if propertyErr == nil && strings.TrimSpace(string(b)) == "1" {
				fact.integrated, fact.topologyKnown = true, true
				d.MemoryMode, d.ReserveGB = "unified", 16
				return d, nil
			}
		}
		if err != nil || n <= 0 {
			return d, preflightError("gpu_memory_unavailable", "cuda", "discrete CUDA memory unavailable; specify an explicit fixed device budget")
		}
		d.GPU, d.MemoryMode, d.MemoryGB, d.ReserveGB = strings.TrimSpace(fields[0]), "dedicated", n, 2
		return d, nil
	}
	budget := device.TotalMemoryGB
	if budget <= 0 {
		// Compatibility memory telemetry only. Level Zero established identity.
		run := p.Command
		if p.CommandEnv != nil {
			bound, err := WithModelDevice(Model{}, "xpu", device.Selector)
			if err != nil {
				return d, err
			}
			run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				return p.CommandEnv(ctx, bound.DeviceEnv, name, args...)
			}
		}
		if run != nil {
			if b, err := run(ctx, "sycl-ls", "--verbose"); err == nil {
				for _, line := range strings.Split(string(b), "\n") {
					if !strings.Contains(line, "global_mem_size") && !strings.Contains(strings.ToLower(line), "global memory size") {
						continue
					}
					_, value, ok := strings.Cut(line, ":")
					if !ok {
						continue
					}
					fields := strings.Fields(value)
					if len(fields) == 0 {
						continue
					}
					bytes, err := strconv.ParseUint(fields[0], 10, 64)
					n := float64(bytes) / (1024 * 1024 * 1024)
					if err == nil && n > 0 && (budget == 0 || n < budget) {
						budget = n
					}
				}
			}
		}
	}
	if budget <= 0 {
		return d, preflightError("gpu_memory_unavailable", "xpu", "discrete Intel GPU memory unavailable: specify --gpu xpu --memory-mode fixed --memory-gb with an explicit device budget")
	}
	d.MemoryMode, d.MemoryGB, d.ReserveGB = "fixed", budget, min(2, budget/4)
	return d, nil
}

const cudaIntegratedAttribute = `import ctypes as c, sys
driver=c.CDLL("libcuda.so.1")
assert driver.cuInit(0)==0
device=c.c_int()
assert driver.cuDeviceGetByPCIBusId(c.byref(device),sys.argv[1].encode())==0
integrated=c.c_int()
assert driver.cuDeviceGetAttribute(c.byref(integrated),18,device)==0
assert integrated.value in (0,1)
print(integrated.value)
`
