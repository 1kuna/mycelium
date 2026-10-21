package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// PreflightError contains a stable reason and a safe operator action. Probe
// output and command environments are deliberately never included.
type PreflightError struct{ Code, Component, Message string }

func (e *PreflightError) Error() string { return e.Code + ": " + e.Component + ": " + e.Message }
func preflightError(code, component, message string) error {
	return &PreflightError{code, component, message}
}
func probeAbsent(err error) bool {
	return errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrNoGPU)
}

func (p MachineProbe) gpuPresent() (bool, error) {
	if p.OS != "linux" {
		return false, nil
	}
	glob, read := p.Glob, p.ReadFile
	if glob == nil {
		glob = filepath.Glob
	}
	if read == nil {
		read = os.ReadFile
	}
	paths, err := glob("/sys/bus/pci/devices/*/class")
	if err != nil {
		return false, err
	}
	for _, path := range paths {
		b, e := read(path)
		if e != nil {
			return false, e
		}
		if strings.HasPrefix(strings.TrimSpace(string(b)), "0x03") {
			return true, nil
		}
	}
	return false, nil
}

// HardwarePreflight is also the nonsecret detect --config reply. The scalar
// fields retain compatibility; Devices is the complete selected install plan.
type HardwarePreflight struct {
	Detection
	Devices []DeviceInstall `json:"devices,omitempty"`
}

// PreflightHardware reuses M2 physical identities and per-component detection.
// A saved CPU choice is explicit; callers clear Platform for re-detection.
// It is pure with respect to saved configuration and probes afresh each call.
func PreflightHardware(ctx context.Context, c ServiceConfig, p MachineProbe) (HardwarePreflight, error) {
	out := HardwarePreflight{Detection: c.Detection}
	if p.OS != "linux" && p.OS != "darwin" || p.Arch != "amd64" && p.Arch != "arm64" {
		return out, preflightError("hardware_unsupported", "host", "supported Linux/macOS architecture required")
	}
	if c.Detection.Platform == "cpu" {
		out.Detection = Detection{OS: p.OS, Arch: p.Arch, Platform: "cpu", MemoryMode: "fixed"}
		return out, nil
	}
	if c.AllowDeviceless && c.Detection.Platform == "fake" {
		return out, nil
	}
	allowance, err := deviceAllowance(c.Device)
	if err != nil {
		return out, err
	}
	physical, err := p.Accelerators(ctx)
	if err != nil {
		return out, err
	}
	if len(physical) == 0 {
		// Absence of a runtime is not absence of hardware.
		if present, e := p.gpuPresent(); e != nil || present {
			return out, preflightError("gpu_driver_missing", "gpu", "GPU hardware has no supported inventory; repair the driver and re-detect")
		}
		if c.Detection.Platform != "" {
			return out, preflightError("gpu_inventory_unavailable", "gpu", "configured GPU is absent; repair it and re-detect")
		}
		if len(allowance) > 0 {
			return out, preflightError("device_selector_unmatched", "gpu", "explicit selectors are absent; restore the selected components and re-detect")
		}
		out.Detection = Detection{OS: p.OS, Arch: p.Arch, Platform: "cpu", MemoryMode: "fixed"}
		return out, nil
	}
	matched := map[string]bool{}
	for _, device := range physical {
		selected := len(allowance) == 0
		for _, s := range allowance {
			if s == device.ID || s == device.Selector || s == device.Identity {
				selected = true
				matched[s] = true
			}
		}
		if !selected {
			continue
		}
		probe := p
		if device.Platform == "xpu" {
			command := p.Command
			probe.Command = func(ctx context.Context, n string, args ...string) ([]byte, error) {
				if n == "nvidia-smi" {
					return nil, exec.ErrNotFound
				}
				return command(ctx, n, args...)
			}
		}
		d, e := probe.DetectDevice(ctx, device.Selector)
		var configured *Detection
		perDevice := false
		for _, saved := range c.MachineDevices {
			if saved.Selector == device.Selector || saved.Selector == device.Identity || saved.Selector == device.ID {
				v := saved.Detection
				configured = &v
				perDevice = true
				break
			}
		}
		if configured == nil && c.Detection.Platform != "" {
			v := c.Detection
			configured = &v
		}
		var typed *PreflightError
		fixed := configured != nil && configured.MemoryMode == "fixed" && configured.Platform == device.Platform && configured.MemoryGB > configured.ReserveGB
		if e != nil && !(fixed && device.Platform == "xpu" && errors.As(e, &typed) && typed.Code == "gpu_memory_unavailable") {
			return out, e
		}
		if configured != nil {
			if configured.Platform != device.Platform {
				return out, preflightError("gpu_inventory_invalid", device.ID, "configured platform differs from detected component")
			}
			if perDevice || configured.MemoryMode != "" && configured.MemoryMode != "auto" {
				d.MemoryMode, d.MemoryGB, d.ReserveGB = configured.MemoryMode, configured.MemoryGB, configured.ReserveGB
			} else {
				if configured.MemoryGB > 0 {
					d.MemoryGB = configured.MemoryGB
				}
				if configured.ReserveGB > 0 {
					d.ReserveGB = configured.ReserveGB
				}
			}
		}
		if d.Platform == "" {
			d.Platform = device.Platform
		}
		d.OS, d.Arch = p.OS, p.Arch
		out.Devices = append(out.Devices, DeviceInstall{Selector: device.Selector, Detection: d})
	}
	if len(matched) != len(allowance) {
		return out, preflightError("device_selector_unmatched", "gpu", "one or more explicit selectors are absent; selection was not broadened")
	}
	if len(out.Devices) == 0 {
		return out, preflightError("gpu_inventory_unavailable", "gpu", "no selected components detected")
	}
	if err := ValidateDeviceSettings(out.Devices); err != nil {
		return out, err
	}
	out.Detection = out.Devices[0].Detection
	return out, nil
}

// ApplyHardwarePreflight leaves identity, selector allowance and all host policy
// untouched. Existing per-device budgets win over scalar compatibility fields.
func ApplyHardwarePreflight(c ServiceConfig, p HardwarePreflight) ServiceConfig {
	c.Detection = p.Detection
	c.MachineDevices = slices.Clone(p.Devices)
	return c
}

// OverlayHardwareSettings applies explicit scalar memory flags to each offered
// component. An omitted flag never default-fills a saved per-device value.
func OverlayHardwareSettings(c ServiceConfig, explicit map[string]bool) ServiceConfig {
	c.MachineDevices = slices.Clone(c.MachineDevices)
	for i := range c.MachineDevices {
		d := &c.MachineDevices[i].Detection
		if explicit["memory-mode"] {
			d.MemoryMode = c.Detection.MemoryMode
		}
		if explicit["memory-gb"] {
			d.MemoryGB = c.Detection.MemoryGB
		}
		if explicit["reserve-memory-gb"] {
			d.ReserveGB = c.Detection.ReserveGB
		}
	}
	return c
}

func validateServiceHardware(c ServiceConfig, p HardwarePreflight) error {
	if p.Platform == "" || p.Platform != c.Detection.Platform {
		return fmt.Errorf("no device matching the configured GPU platform")
	}
	for _, expected := range c.MachineDevices {
		if !slices.ContainsFunc(p.Devices, func(d DeviceInstall) bool {
			return d.Selector == expected.Selector && d.Detection.Platform == expected.Detection.Platform && d.Detection.MemoryGB == expected.Detection.MemoryGB && d.Detection.ReserveGB == expected.Detection.ReserveGB && d.Detection.MemoryMode == expected.Detection.MemoryMode
		}) {
			return preflightError("service_inventory_incomplete", "gpu", "service cannot observe every configured component and budget")
		}
	}
	return nil
}
