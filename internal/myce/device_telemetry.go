package myce

import (
	"context"
	"encoding/csv"
	"errors"
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
	return validDeviceTelemetry(s) && s != nil && s.FreeGB != nil && s.ActivePercent != nil &&
		!now.Before(s.At) && now.Sub(s.At) < deviceMemorySampleFreshFor
}

// Use the installed CLI's supported single-shot query, with the PCI identity
// rather than a runtime ordinal. The old XPUM stats schema is not this tool's
// output. A missing/unsupported query leaves its telemetry counter unknown.
func xpuFreeMemory(ctx context.Context, p MachineProbe, pci string) (float64, error) {
	b, err := p.Command(ctx, "xpu-smi", "--id="+pci, "--query-gpu=pci.bus_id,memory.free", "--format=csv,noheader,nounits")
	if err != nil {
		return 0, err
	}
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil || len(rows) != 1 || len(rows[0]) != 2 || normalizePCIAddress(strings.TrimSpace(rows[0][0])) != normalizePCIAddress(pci) {
		return 0, errors.New("XPU free-memory identity unavailable")
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(rows[0][1]), 64)
	if err != nil || !validMemoryValue(n) {
		return 0, errors.New("XPU free-memory counter unavailable")
	}
	return n / 1024, nil
}

func (w Worker) sampleXPUTelemetry(ctx context.Context, p MachineProbe, d *MachineDevice) {
	if p.OS != "linux" || d.Platform != "xpu" || d.UnifiedMemory || d.pciAddress == "" {
		return
	}
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	s := &DeviceTelemetry{At: clock.Now(), Source: "xpu-smi memory; Xe PMU busiest hardware engine"}
	if free, err := xpuFreeMemory(ctx, p, d.pciAddress); err == nil {
		free = max(0, free-d.ReserveMemoryGB)
		s.FreeGB = &free
		d.FreeMemoryGB = min(d.FreeMemoryGB, free)
	}
	if p.XeActivity != nil {
		if active, err := p.XeActivity(ctx, d.pciAddress); err == nil && validMemoryValue(active) && active <= 100 {
			s.ActivePercent = &active
		}
	}
	d.Telemetry = s
}
