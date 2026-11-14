package myce

import (
	"context"
	"slices"
	"strings"
	"time"
)

const deviceTelemetryProtocol = "device-activity-v1"

// DeviceTelemetry is independent of the configured allowance. Nil counters
// mean unknown, including when a driver/tool prints N/A. At is the beginning
// of the observation, so slow probes cannot renew old evidence.
type DeviceTelemetry struct {
	At            time.Time `json:"at"`
	AgeSeconds    float64   `json:"age_seconds"`
	FreeGB        *float64  `json:"free_gb,omitempty"`
	ActivePercent *float64  `json:"active_percent,omitempty"`
	Source        string    `json:"source"`
}

func validDeviceTelemetry(s *DeviceTelemetry) bool {
	return s == nil || !s.At.IsZero() && validMemoryValue(s.AgeSeconds) && (s.FreeGB == nil || validMemoryValue(*s.FreeGB)) &&
		(s.ActivePercent == nil || validMemoryValue(*s.ActivePercent) && *s.ActivePercent <= 100)
}

func (w Worker) deviceTelemetryCheckin(c Checkin) Checkin {
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	if p := c.Machine.InventoryProbe; p != nil {
		copy := *safeInventoryObservation(p)
		if !copy.At.IsZero() {
			copy.AgeSeconds = clock.Now().Sub(copy.At).Seconds()
		}
		if !inventoryObservationFresh(&copy, clock.Now()) && (copy.State == "ready" || copy.State == "partial") {
			copy.State = "stale"
		}
		c.Machine.InventoryProbe = &copy
	}
	for i := range c.Machine.Devices {
		if s := c.Machine.Devices[i].Telemetry; s != nil {
			copy := *s
			copy.AgeSeconds = clock.Now().Sub(copy.At).Seconds()
			c.Machine.Devices[i].Telemetry = &copy
		}
	}
	return c
}

// Rebase sensor ages onto queue time, as with exact-attempt memory counters.
// Worker clock skew cannot refresh a stale inventory-cache observation.
func receivedDeviceTelemetry(c Checkin, now time.Time) Checkin {
	if p := c.Machine.InventoryProbe; p != nil {
		copy := *safeInventoryObservation(p)
		copy.ScanAt = copy.At // Stable token: receipt delay cannot turn a cached scan into new evidence.
		if validMemoryValue(copy.AgeSeconds) && copy.AgeSeconds < float64(time.Duration(1<<63-1))/float64(time.Second) {
			copy.At = now.Add(-time.Duration(copy.AgeSeconds * float64(time.Second)))
		} else {
			copy.At, copy.State = now, "stale"
		}
		c.Machine.InventoryProbe = &copy
	}
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	for i := range c.Machine.Devices {
		if s := c.Machine.Devices[i].Telemetry; s != nil && validDeviceTelemetry(s) {
			copy := *s
			copy.At = now.Add(-time.Duration(min(copy.AgeSeconds, deviceMemorySampleFreshFor.Seconds()) * float64(time.Second)))
			c.Machine.Devices[i].Telemetry = &copy
		}
	}
	return c
}

func deviceTelemetryReady(s *DeviceTelemetry, now time.Time) bool {
	return deviceTelemetryFresh(s, now) && s.FreeGB != nil && s.ActivePercent != nil
}

func deviceTelemetryFresh(s *DeviceTelemetry, now time.Time) bool {
	return validDeviceTelemetry(s) && s != nil && !now.Before(s.At) && now.Sub(s.At) < deviceMemorySampleFreshFor
}

// The optional broker exposes privileged kernel usage counters without Level
// Zero initialization. Old GTT-only brokers supply no VRAM evidence. Neither
// their host storage nor censored/missing counters can become device headroom.
func (w Worker) xpuFreeMemory(ctx context.Context) (map[string]float64, error) {
	free := map[string]float64{}
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	s, err := readGPUTelemetrySnapshot(ctx, w.Clock, w.GPUTelemetryHelper)
	if err != nil {
		return free, err
	}
	return xpuSnapshotFreeMemory(s), nil
}

func xpuSnapshotFreeMemory(s *gpuTelemetrySnapshot) map[string]float64 {
	free := map[string]float64{}
	seen := map[string]bool{}
	for identity, memory := range s.VRAM {
		pci := normalizePCIAddress(strings.TrimSpace(identity))
		if seen[pci] {
			delete(free, pci)
			continue
		}
		seen[pci] = true
		if hostTelemetryPCI.MatchString(pci) && memory.TotalBytes != nil && memory.UsedBytes != nil && *memory.TotalBytes > 0 && *memory.UsedBytes <= *memory.TotalBytes {
			free[pci] = float64(*memory.TotalBytes-*memory.UsedBytes) / (1 << 30)
		}
	}
	return free
}

func (w Worker) sampleXPUTelemetry(ctx context.Context, p MachineProbe, devices []MachineDevice) {
	if p.OS != "linux" {
		return
	}
	eligible := func(d MachineDevice) bool {
		return d.Platform == "xpu" && !d.UnifiedMemory && d.pciAddress != "" && d.Reason == ""
	}
	if !slices.ContainsFunc(devices, eligible) {
		return
	}
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	at := clock.Now()
	var freeByPCI map[string]float64
	if p.XPUFreeMemory != nil {
		freeByPCI, _ = p.XPUFreeMemory(ctx)
	} else {
		freeByPCI, _ = w.xpuFreeMemory(ctx)
	}
	for i := range devices {
		d := &devices[i]
		if !eligible(*d) {
			continue
		}
		s := &DeviceTelemetry{At: at, Source: "read-only Xe broker VRAM; Xe PMU busiest hardware engine"}
		if free, ok := freeByPCI[normalizePCIAddress(d.pciAddress)]; ok {
			free = max(0, free-d.ReserveMemoryGB)
			s.FreeGB = &free
		}
		if p.XeActivity != nil {
			if active, err := p.XeActivity(ctx, d.pciAddress); err == nil && validMemoryValue(active) && active <= 100 {
				s.ActivePercent = &active
			}
		}
		d.Telemetry = s
	}
}

func (w Worker) sampleDeviceTelemetry(ctx context.Context, p MachineProbe, devices []MachineDevice) {
	for _, backend := range acceleratorBackends {
		if backend.telemetry != nil {
			backend.telemetry(w, ctx, p, devices)
		}
	}
}
