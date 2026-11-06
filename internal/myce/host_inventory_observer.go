package myce

import (
	"context"
	"errors"
)

// Inventory is an observation lane, not an attempt sampler. No per-host driver
// latency default kills a progressing scan; cancellation owns its lifetime.
// A pending scan retains identity but never indefinitely retains live capacity.
func sampleHostInventory(ctx context.Context, lane *hostProbeLane[[]MachineDevice], source func(context.Context) ([]MachineDevice, error)) ([]MachineDevice, error) {
	lane.step(ctx, source)
	if lane.fresh(lane.clock.Now()) {
		return lane.value, nil
	}
	var partial *acceleratorProbeError
	if errors.As(lane.err, &partial) && lane.observation.valid() && !lane.clock.Now().Before(lane.at) && lane.clock.Now().Sub(lane.at) < lane.observation.freshness() {
		return lane.value, lane.err
	}
	return nil, context.DeadlineExceeded
}
