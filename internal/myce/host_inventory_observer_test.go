package myce

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostInventoryObserverSlowPartialAndStale(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	lane := hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: time.Second, cores: 1}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int32
	release := make(chan struct{})
	source := func(context.Context) ([]MachineDevice, error) {
		if calls.Add(1) > 1 {
			<-release
		}
		return []MachineDevice{{ID: "0", Platform: "cuda", Identity: "gpu-a", Selector: "0"}}, &acceleratorProbeError{xpu: true}
	}
	sampleHostInventory(ctx, &lane, source)
	await(t, func() bool { return len(lane.pending) == 1 })
	devices, err := sampleHostInventory(ctx, &lane, source)
	var partial *acceleratorProbeError
	if len(devices) != 1 || !errors.As(err, &partial) || !partial.failed("xpu") {
		t.Fatal("lost healthy sibling", devices, err)
	}
	clock.Advance(time.Second)
	sampleHostInventory(ctx, &lane, source)
	await(t, func() bool { return calls.Load() == 2 })
	clock.Advance(time.Minute)
	for i := 0; i < 20; i++ {
		devices, err = sampleHostInventory(ctx, &lane, source)
	}
	if len(devices) > 0 || !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 2 {
		t.Fatal("stalled scan retained capacity or multiplied calls", calls.Load(), devices, err)
	}
	close(release)
}
