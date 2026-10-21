package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// pciAccelerator contains only physical facts. Runtime names never determine
// topology, support or memory mode. Unknown topology stays unknown.
type pciAccelerator struct {
	address, vendor, device   string
	integrated, topologyKnown bool
}

func (d pciAccelerator) platform() string {
	switch d.vendor {
	case "0x10de":
		return "cuda"
	case "0x8086":
		return "xpu"
	}
	return ""
}
func (p MachineProbe) pciAccelerators() ([]pciAccelerator, error) {
	if p.OS != "linux" || p.Glob == nil {
		return nil, nil
	}
	paths, err := p.Glob("/sys/bus/pci/devices/*/class")
	if err != nil {
		return nil, err
	}
	out := []pciAccelerator{}
	for _, path := range paths {
		if p.ReadFile == nil {
			return nil, fmt.Errorf("PCI reader missing")
		}
		b, err := p.ReadFile(path)
		if err != nil {
			return nil, err
		}
		class, err := strconv.ParseUint(strings.TrimSpace(string(b)), 0, 32)
		if err != nil {
			return nil, err
		}
		// PCI base classes: display controller and processing accelerator.
		if class>>16 != 0x03 && class>>16 != 0x12 {
			continue
		}
		dir := filepath.Dir(path)
		vendor, err := p.ReadFile(filepath.Join(dir, "vendor"))
		if err != nil {
			return nil, err
		}
		device, _ := p.ReadFile(filepath.Join(dir, "device"))
		integrated, known, err := p.pciTopology(dir)
		if err != nil {
			return nil, err
		}
		out = append(out, pciAccelerator{filepath.Base(dir), strings.ToLower(strings.TrimSpace(string(vendor))), strings.TrimSpace(string(device)), integrated, known})
	}
	return out, nil
}

func (p MachineProbe) pciTopology(dir string) (integrated, known bool, err error) {
	if p.ReadFile == nil {
		return false, false, nil
	}
	// Local device memory is direct evidence; zero means system memory. Intel's
	// DRM ABI exposes this on devices which implement local_mem.
	if b, e := p.ReadFile(filepath.Join(dir, "local_mem")); e == nil {
		n, e := strconv.ParseUint(strings.TrimSpace(string(b)), 0, 64)
		if e != nil {
			return false, false, e
		}
		return n == 0, true, nil
	} else if !os.IsNotExist(e) {
		return false, false, e
	}
	// A PCIe link belongs to a discrete endpoint. Root-complex integrated
	// graphics report zero (or do not expose this attribute at all).
	if b, e := p.ReadFile(filepath.Join(dir, "current_link_width")); e == nil {
		n, e := strconv.Atoi(strings.TrimSpace(string(b)))
		if e != nil || n < 0 {
			return false, false, fmt.Errorf("invalid PCI link width")
		}
		return n == 0, true, nil
	} else if !os.IsNotExist(e) {
		return false, false, e
	}
	return false, false, nil
}

// Match physical/runtime identities before selectors or saved budgets are
// applied. A single-device legacy runtime can omit its PCI address; ambiguous
// multi-device correlation is refused instead of inferred from runtime order.
func reconcilePCI(pci []pciAccelerator, runtime []MachineDevice) (map[string]pciAccelerator, error) {
	matched := map[string]bool{}
	facts := map[string]pciAccelerator{}
	for _, r := range runtime {
		candidates := []pciAccelerator{}
		for _, d := range pci {
			if d.platform() == r.Platform && (r.pciAddress == "" || normalizePCIAddress(r.pciAddress) == normalizePCIAddress(d.address)) {
				candidates = append(candidates, d)
			}
		}
		if len(candidates) == 0 && len(pci) == 0 {
			continue
		} // Non-PCI platforms / injected runtime-only probes.
		if len(candidates) != 1 || matched[candidates[0].address] {
			return facts, preflightError("gpu_inventory_invalid", r.Platform, "runtime components cannot be matched uniquely to PCI hardware; restore complete inventory and re-detect")
		}
		d := candidates[0]
		matched[d.address] = true
		facts[r.Platform+":"+r.Selector] = d
	}
	var failures []error
	for _, d := range pci {
		if d.platform() == "" || matched[d.address] {
			continue
		}
		if d.platform() == "xpu" && d.topologyKnown && d.integrated {
			continue
		} // Optional absent iGPU runtime; existing CPU-only contract.
		failures = append(failures, preflightError("gpu_driver_missing", d.address, "GPU hardware has no supported runtime inventory; repair the driver and re-detect"))
	}
	return facts, errors.Join(failures...)
}
func normalizePCIAddress(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	parts := strings.Split(s, ":")
	if len(parts) == 3 {
		if domain, e := strconv.ParseUint(parts[0], 16, 32); e == nil {
			return fmt.Sprintf("%04x:%s:%s", domain, parts[1], parts[2])
		}
	}
	return s
}

func (p MachineProbe) intelPCIDevice(ctx context.Context, selector string) (*pciAccelerator, error) {
	if p.pciDevice != nil {
		return p.pciDevice, nil
	}
	pci, err := p.pciAccelerators()
	if err != nil {
		return nil, preflightError("gpu_inventory_unavailable", "xpu", "cannot inspect Intel PCI topology; restore access and re-detect")
	}
	intel := []pciAccelerator{}
	for _, d := range pci {
		if d.platform() == "xpu" {
			intel = append(intel, d)
		}
	}
	if len(intel) == 0 {
		return nil, nil
	}
	if len(intel) == 1 {
		return &intel[0], nil
	}
	runtime, err := p.Accelerators(ctx)
	if err != nil {
		return nil, err
	}
	facts, err := reconcilePCI(pci, runtime)
	if err != nil {
		return nil, err
	}
	if fact, ok := facts["xpu:"+selector]; ok {
		return &fact, nil
	}
	return nil, preflightError("gpu_inventory_invalid", "xpu", "select a PCI-correlated Intel device before memory detection")
}
