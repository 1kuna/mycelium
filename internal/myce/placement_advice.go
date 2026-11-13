package myce

import "strings"

// PlacementAdvisories explains authored pins without changing frozen intent or
// claiming that a selector proves a hardware requirement. Never echo identifiers
// or free-form reasons: advice can be copied into public support reports.
func PlacementAdvisories(s Spec, reasons map[string]string, variants ...ExtensionVariant) []string {
	if s.ExtensionCheck != nil {
		return nil // Qualification intentionally selects exact hardware.
	}
	var advice []string
	if len(s.AllowedMachines) > 0 && strings.TrimSpace(reasons["host"]) == "" {
		advice = append(advice, "Host pin has no declared host requirement. If the software only needs a platform, accelerator, memory or environment, omit allowed_machines and declare needs, compatible targets and environment coverage; Mycelium chooses the host, even if only one host currently fits. Retain a pin for an explicit owner restriction, specific physical hardware or hardware qualification. The project's machine-kind production approval is already enforced and needs no matching job pin.")
	}
	devicePinned := s.Needs.GPU != nil && len(s.Needs.GPU.Devices) > 0
	for _, v := range variants {
		devicePinned = devicePinned || v.Needs.GPU != nil && len(v.Needs.GPU.Devices) > 0
	}
	if devicePinned && strings.TrimSpace(reasons["device"]) == "" {
		advice = append(advice, "Device pin has no declared device requirement. If compatible accelerators suffice, omit needs.gpu.devices and declare the required backend and memory; Mycelium chooses devices. Consume MYCE_DEVICE and assigned visibility. Retain an exact selector only when the work requires that physical device.")
	}
	return advice
}
