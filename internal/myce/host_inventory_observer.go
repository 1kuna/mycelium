package myce

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

func (w Worker) inventorySource() func(context.Context) ([]MachineDevice, error) {
	probeInventory := w.InventoryProbe
	// Pending OS probes may outlive worker shutdown. Keep this worker's source
	// while still probing fresh identity on each scan, without rereading defaults.
	identity := extensionIdentityProbe
	return func(ctx context.Context) ([]MachineDevice, error) {
		noteInventoryOperation(ctx, "", nil)
		devices, err := probeInventory(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		devices = slices.Clone(devices)
		probe := identity()
		// Qualification hashes retain their existing bytes, while common OS and
		// driver commands run once for this immutable inventory pass.
		type result struct {
			data []byte
			err  error
		}
		cache := map[string]result{}
		command := probe.Command
		probe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			key := name + "\x00" + strings.Join(args, "\x00")
			if r, ok := cache[key]; ok {
				return r.data, r.err
			}
			data, err := command(ctx, name, args...)
			cache[key] = result{data, err}
			return data, err
		}
		for i := range devices {
			d := &devices[i]
			d.ExtensionFingerprint, _ = extensionFingerprintWithInventory(ctx, d.Platform, d.ID, func() MachineProbe { return probe }, func(context.Context) ([]MachineDevice, error) { return devices, err })
		}
		return devices, err
	}
}

// Discovery responds to identity, topology, configuration and health changes,
// not wall time or workload activity. In particular, free unified RAM also
// changes its uncapped budget; neither is a hardware change.
func sameInventory(a, b []MachineDevice) bool {
	semantic := func(devices []MachineDevice) []MachineDevice {
		out := slices.Clone(devices)
		for i := range out {
			out[i].Telemetry, out[i].FreeMemoryGB = nil, 0
			if out[i].UnifiedMemory {
				out[i].BudgetGB = 0
			}
		}
		return out
	}
	return reflect.DeepEqual(semantic(a), semantic(b))
}

// Repeated unknown/partial results do not make unchanged hardware new. Recovery
// or a changed failed-family classification does; validity remains independent.
func sameInventoryObservation(a []MachineDevice, aerr error, b []MachineDevice, berr error) bool {
	if (aerr == nil) != (berr == nil) || !sameInventory(a, b) {
		return false
	}
	var ap, bp *acceleratorProbeError
	aPartial, bPartial := errors.As(aerr, &ap), errors.As(berr, &bp)
	return aPartial == bPartial && (!aPartial || maps.Equal(ap.families, bp.families))
}

// Inventory is an observation lane, not an attempt sampler. No per-host driver
// latency default kills a progressing scan; cancellation owns its lifetime.
// A pending scan retains identity but never indefinitely retains live capacity.
func sampleHostInventory(ctx context.Context, lane *hostProbeLane[[]MachineDevice], source func(context.Context) ([]MachineDevice, error)) ([]MachineDevice, error) {
	lane.equal = sameInventoryObservation
	lane.step(ctx, source)
	return hostInventorySnapshot(lane)
}

func hostInventorySnapshot(lane *hostProbeLane[[]MachineDevice]) ([]MachineDevice, error) {
	if lane.fresh(lane.clock.Now()) {
		return slices.Clone(lane.value), nil
	}
	var partial *acceleratorProbeError
	if errors.As(lane.err, &partial) && lane.observation.valid() && !lane.clock.Now().Before(lane.at) && lane.clock.Now().Sub(lane.at) < lane.observation.freshness() {
		return slices.Clone(lane.value), lane.err
	}
	return nil, context.DeadlineExceeded
}

// Inventory progresses independently of check-in duration and control ticks.
type hostInventoryObserver struct {
	progress        inventoryProgress
	mu              sync.Mutex
	lane            hostProbeLane[[]MachineDevice]
	telemetry       hostProbeLane[[]MachineDevice]
	sampleTelemetry func(context.Context, []MachineDevice) ([]MachineDevice, error)
	telemetryDemand bool
	wake            chan struct{}
}

// Only sharing consumes physical XPU headroom. CPU and exclusive-device work
// use their existing budgets/attempt counters; heartbeat is not sensor demand.
func (m *hostInventoryObserver) requestTelemetry(demand bool) {
	m.mu.Lock()
	if m.telemetryDemand == demand {
		m.mu.Unlock()
		return
	}
	m.telemetryDemand = demand
	if m.wake == nil {
		m.wake = make(chan struct{}, 1)
	}
	wake := m.wake
	m.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (m *hostInventoryObserver) stepTelemetry(ctx context.Context) {
	if m.sampleTelemetry == nil {
		return
	}
	// A fresh sample retains its original age and serves every decision in
	// that window. Pending work must still be drained after demand disappears.
	if m.telemetry.pending == nil && (!m.telemetryDemand || len(m.lane.value) == 0 || m.telemetry.fresh(m.lane.clock.Now()) && slices.ContainsFunc(m.telemetry.value, func(d MachineDevice) bool {
		return deviceTelemetryFresh(d.Telemetry, m.lane.clock.Now())
	})) {
		return
	}
	devices, _ := hostInventorySnapshot(&m.lane)
	m.telemetry.stepWhen(ctx, func(ctx context.Context) ([]MachineDevice, error) { return m.sampleTelemetry(ctx, devices) }, m.telemetryDemand)
}

func (m *hostInventoryObserver) snapshot() ([]MachineDevice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *hostInventoryObserver) snapshotWithObservation() ([]MachineDevice, error, *InventoryObservation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lane.timing.noteReport(m.lane.clock.Now())
	devices, err := m.snapshotLocked()
	return devices, err, m.observationLocked()
}

func (m *hostInventoryObserver) snapshotLocked() ([]MachineDevice, error) {
	devices, err := hostInventorySnapshot(&m.lane)
	if m.telemetry.fresh(m.lane.clock.Now()) {
		for i := range devices {
			for _, sampled := range m.telemetry.value {
				if devices[i].ID == sampled.ID && devices[i].Platform == sampled.Platform && devices[i].Identity == sampled.Identity && devices[i].Selector == sampled.Selector && devices[i].Fingerprint == sampled.Fingerprint && devices[i].pciAddress == sampled.pciAddress && devices[i].ReserveMemoryGB == sampled.ReserveMemoryGB && devices[i].UnifiedMemory == sampled.UnifiedMemory && devices[i].Reason == "" {
					devices[i].Telemetry = sampled.Telemetry
					if deviceTelemetryFresh(sampled.Telemetry, m.lane.clock.Now()) && sampled.Telemetry.FreeGB != nil {
						devices[i].FreeMemoryGB = min(devices[i].FreeMemoryGB, *sampled.Telemetry.FreeGB)
					}
				}
			}
		}
	}
	return devices, err
}
func (m *hostInventoryObserver) run(ctx context.Context, source func(context.Context) ([]MachineDevice, error)) {
	m.progress.clock = m.lane.clock
	ctx = context.WithValue(ctx, inventoryProgressKey{}, &m.progress)
	m.lane.equal = sameInventoryObservation
	m.telemetry.clock = m.lane.clock
	// Reuse the wire contract's freshness window. The measured lane may slow
	// expensive/unchanging observations; it cannot renew old sharing evidence.
	m.telemetry.timing = probeTiming{base: max(deviceMemorySampleFreshFor, m.lane.timing.controlPeriod()), cores: m.lane.timing.cores}
	for {
		m.mu.Lock()
		m.lane.step(ctx, source)
		wait, done := m.lane.waitDuration(), m.lane.done
		m.stepTelemetry(ctx)
		if m.telemetry.pending != nil {
			wait = min(wait, m.telemetry.waitDuration())
		}
		telemetryDone := m.telemetry.done
		wake := m.wake
		m.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-done:
		case <-telemetryDone:
		case <-wake:
		}
		timer.Stop()
	}
}
