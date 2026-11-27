package myce

import (
	"context"
	"math"
	"runtime"
	"time"
)

// A record is added only after probe 0b-2 passes on this platform and pool.
// Detection includes the slowest footprint/free counter and its read time.
type memoryStopProbe struct {
	fastestGBPerSecond    float64
	detection, killToFree time.Duration
}

func (p memoryStopProbe) keepGB() float64 {
	if !validRate(p.fastestGBPerSecond) || p.detection <= 0 || p.killToFree <= 0 {
		return 0
	}
	return p.fastestGBPerSecond * (p.detection + p.killToFree).Seconds()
}

var memoryStopProbes = map[string]memoryStopProbe{}

func qualifiedStopKeep(pool string, cadence time.Duration) float64 {
	p := memoryStopProbes[pool]
	// Probe with the shipping sampler. A faster probe sampler cannot qualify a
	// slower production sampler.
	if p.detection < cadence {
		return 0
	}
	return p.keepGB()
}

func ramStopKeep(m *Machine, goos string) float64 {
	if goos == "darwin" {
		return qualifiedStopKeep(shadowDarwin+"/ram", processMemorySamplePeriod)
	}
	if goos != "linux" {
		return 0
	}
	if hostHasUnifiedCUDA(m) {
		if unifiedStopAttributionQualified {
			return qualifiedStopKeep(shadowLinuxUnified+"/ram", deviceMemorySamplePeriod+attemptMemorySamplePeriod)
		}
		return 0
	}
	keep := 0.0
	for _, d := range m.Devices {
		if !d.UnifiedMemory {
			keep = max(keep, qualifiedStopKeep(shadowLinuxDiscrete+"/"+d.Platform+"/ram", deviceMemorySamplePeriod+attemptMemorySamplePeriod))
		}
	}
	if m.Devices == nil && m.GPU != "cpu" && !m.UnifiedMemory {
		keep = qualifiedStopKeep(shadowLinuxDiscrete+"/"+m.GPU+"/ram", deviceMemorySamplePeriod+attemptMemorySamplePeriod)
	}
	return keep
}

func cardStopKeep(platform string) float64 {
	return qualifiedStopKeep(shadowLinuxDiscrete+"/"+platform+"/card", deviceMemorySamplePeriod+attemptMemorySamplePeriod)
}

// An attempt's own sampler checks only its fresh footprint and fresh physical
// free memory. The result uses the same durable receipt and exact process
// identity checks as the explicit kill limit.
type immediateMemoryStop struct {
	pool           string
	keep, expected float64
	free           MemorySource
}

func (s immediateMemoryStop) sample(ctx context.Context, clock Clock, attempt *process, reading shadowMemoryReading) bool {
	if s.free == nil || !validRate(s.expected) || !reading.fresh(clock.Now(), s.samplePeriod()) || reading.GB <= s.expected {
		return false
	}
	roots := attempt.rssRoots()
	if len(roots) == 0 {
		return false
	}
	free, err := s.free(ctx)
	if err != nil || !validMemoryValue(free) || free >= s.keep || ctx.Err() != nil ||
		!reading.fresh(clock.Now(), s.samplePeriod()) {
		return false
	}
	const gib = 1 << 30
	if reading.GB >= float64(math.MaxInt64)/gib {
		return false
	}
	e := &RSSLimitEvidence{ObservedBytes: uint64(reading.GB * gib), LimitBytes: int64(s.expected * gib), Quality: s.pool + poolFootprintLimitSuffix}
	return attempt.stopRSS(clock.Now(), roots, e)
}

func (s immediateMemoryStop) samplePeriod() time.Duration {
	if s.pool == "card" {
		return deviceMemorySamplePeriod
	}
	if runtime.GOOS == "darwin" {
		return processMemorySamplePeriod
	}
	return attemptMemorySamplePeriod
}

func (w Worker) immediateRAMStop(j Job) immediateMemoryStop {
	if runtime.GOOS == "linux" && j.Spec.Needs.cpuOnly() {
		return immediateMemoryStop{}
	}
	term := ramStopKeep(&w.Machine, runtime.GOOS)
	if term == 0 || !w.supportsMemoryStops() {
		return immediateMemoryStop{}
	}
	keep := w.HostMemoryReserveGB
	if w.Machine.SharedHost != nil {
		keep = max(keep, w.Machine.SharedHost.InteractiveReserveGB)
	}
	keep = max(keep+term, memoryPools(&j).KeepGB)
	return immediateMemoryStop{pool: "ram", keep: keep, expected: stopExpectedRAM(j, &w.Machine), free: w.hostRawMemorySource}
}

func (w Worker) cardStopReserve(d MachineDevice) float64 {
	if !d.UnifiedMemory && w.supportsMemoryStops() {
		return cardStopKeep(d.Platform)
	}
	return 0
}

func (w Worker) immediateCardStop(j Job) immediateMemoryStop {
	d := w.selectedDevice
	if d == nil || d.UnifiedMemory || len(w.selectedDevices) > 1 || !validRate(d.TotalMemoryGB) {
		return immediateMemoryStop{}
	}
	term := cardStopKeep(d.Platform)
	if term == 0 || !w.supportsMemoryStops() {
		return immediateMemoryStop{}
	}
	free, err := NewDeviceMemorySource(d.Platform, d.Selector, 0, 0, "dedicated")
	if d.Platform == "xpu" && d.pciAddress != "" {
		xe := workerMachineProbe().XeFreeMemory
		if xe == nil {
			return immediateMemoryStop{}
		}
		free, err = func(ctx context.Context) (float64, error) {
			return xe(ctx, d.pciAddress)
		}, nil
	}
	if err != nil {
		return immediateMemoryStop{}
	}
	return immediateMemoryStop{pool: "card", keep: d.ReserveMemoryGB + term, expected: stopExpectedCard(j, d.TotalMemoryGB), free: free}
}

func (w Worker) supportsMemoryStops() bool {
	return w.custodianMemoryStop || w.Client.memoryStopWire != nil && w.Client.memoryStopWire.Load()
}
