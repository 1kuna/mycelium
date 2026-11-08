package myce

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

func (w Worker) inventorySource() func(context.Context) ([]MachineDevice, error) {
	probeInventory := w.InventoryProbe
	// Pending OS probes may outlive worker shutdown. Keep this worker's source
	// while still probing fresh identity on each scan, without rereading defaults.
	identity := extensionIdentityProbe
	return func(ctx context.Context) ([]MachineDevice, error) {
		devices, err := probeInventory(ctx)
		for i := range devices {
			d := &devices[i]
			d.ExtensionFingerprint, _ = extensionFingerprintFrom(ctx, d.Platform, d.ID, identity)
		}
		return devices, err
	}
}

// Inventory is an observation lane, not an attempt sampler. No per-host driver
// latency default kills a progressing scan; cancellation owns its lifetime.
// A pending scan retains identity but never indefinitely retains live capacity.
func sampleHostInventory(ctx context.Context, lane *hostProbeLane[[]MachineDevice], source func(context.Context) ([]MachineDevice, error)) ([]MachineDevice, error) {
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
	mu   sync.Mutex
	lane hostProbeLane[[]MachineDevice]
}

func (m *hostInventoryObserver) snapshot() ([]MachineDevice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return hostInventorySnapshot(&m.lane)
}
func (m *hostInventoryObserver) run(ctx context.Context, source func(context.Context) ([]MachineDevice, error)) {
	for {
		m.mu.Lock()
		m.lane.step(ctx, source)
		wait, done := m.lane.waitDuration(), m.lane.done
		m.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-done:
		}
		timer.Stop()
	}
}
