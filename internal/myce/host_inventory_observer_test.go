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
		return []MachineDevice{{ID: "0", Platform: "cuda", Identity: "gpu-a", Selector: "0"}}, &acceleratorProbeError{families: map[string]bool{"xpu": true}}
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

func TestInventoryObservationProbeDiagnosticsAndCancellation(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	lane := hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: time.Second}}
	observer := hostInventoryObserver{lane: lane}
	observer.progress = inventoryProgress{clock: clock}
	ctx := context.WithValue(t.Context(), inventoryProgressKey{}, &observer.progress)
	noteInventoryOperation(ctx, "nvidia-smi", []string{"--query-gpu=uuid,driver_version,pci.bus_id"})
	observer.lane.pending = make(chan hostProbeResult[[]MachineDevice])
	clock.Advance(16 * time.Minute)
	p := observer.observation()
	if p.State != "pending" || p.Operation != "nvidia_identity" || p.PendingSeconds != 960 {
		t.Fatal("lost precise pending operation", p)
	}
	observer.lane.pending = nil
	observer.lane.at = clock.Now()
	observer.lane.err = errors.Join(errors.New("private path and secret"), context.DeadlineExceeded)
	p = observer.observation()
	if p.State != "failed" || p.ErrorClass != "deadline" {
		t.Fatal("lost failed probe classification", p)
	}

	w := Worker{Machine: Machine{Devices: inventoryReviewDevices()[:1]}}
	ctx, cancel := context.WithCancel(t.Context())
	w.launchCtx = ctx
	w.InventoryProbe = func(got context.Context) ([]MachineDevice, error) {
		if _, ok := got.Deadline(); ok {
			t.Error("fixed launch probe deadline still installed")
		}
		cancel()
		<-got.Done()
		return nil, got.Err()
	}
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err == nil {
		t.Fatal("cancelled probe authorized launch")
	}
}
