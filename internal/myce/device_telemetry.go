package myce

import (
	"context"
	"encoding/csv"
	"slices"
	"strconv"
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

func legacyDeviceTelemetryCheckin(c Checkin) Checkin {
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	for i := range c.Machine.Devices {
		c.Machine.Devices[i].Telemetry = nil
	}
	return c
}

func hasDeviceTelemetry(c Checkin) bool {
	for _, d := range c.Machine.Devices {
		if d.Telemetry != nil {
			return true
		}
	}
	return false
}

func (w Worker) deviceTelemetryCheckin(c Checkin) Checkin {
	if w.unifiedMemoryWire == nil || !w.unifiedMemoryWire.telemetrySupported ||
		w.unifiedMemoryWire.machine != c.Machine.ID || w.unifiedMemoryWire.session != c.Machine.Session {
		return legacyDeviceTelemetryCheckin(c)
	}
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
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

// One host query returns PCI-keyed counters for every card. Missing, duplicate,
// malformed and unsupported counters remain unknown; ordinals are never joined.
func xpuFreeMemory(ctx context.Context, p MachineProbe) map[string]float64 {
	free := map[string]float64{}
	if p.Command == nil {
		return free
	}
	b, err := p.Command(ctx, "xpu-smi", "--query-gpu=pci.bus_id,memory.free", "--format=csv,noheader,nounits")
	if err != nil {
		return free
	}
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		return free
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if len(row) != 2 {
			return map[string]float64{}
		}
		pci := normalizePCIAddress(strings.TrimSpace(row[0]))
		if seen[pci] {
			delete(free, pci)
			continue
		}
		seen[pci] = true
		n, err := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		if err == nil && validMemoryValue(n) {
			free[pci] = n / 1024
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
	freeByPCI := xpuFreeMemory(ctx, p)
	for i := range devices {
		d := &devices[i]
		if !eligible(*d) {
			continue
		}
		s := &DeviceTelemetry{At: at, Source: "xpu-smi memory; Xe PMU busiest hardware engine"}
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
