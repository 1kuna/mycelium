package myce

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

func TestWorkerInventoryRetainsIdentitySourceAfterDefaultsReset(t *testing.T) {
	previous := extensionIdentityProbe
	defer func() { extensionIdentityProbe = previous }()
	identity := func(label string) func() MachineProbe {
		return func() MachineProbe {
			return MachineProbe{
				Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : " + label + "\n")}},
				Command: func(context.Context, string, ...string) ([]byte, error) {
					return []byte(label), nil
				},
			}
		}
	}
	extensionIdentityProbe = identity("original inventory source")
	want, err := ExtensionFingerprint(t.Context(), "cpu", "")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	w := Worker{InventoryProbe: func(context.Context) ([]MachineDevice, error) {
		close(entered)
		<-release // A pending OS probe can outlive worker cancellation.
		return []MachineDevice{{Platform: "cpu"}}, nil
	}}
	source := w.inventorySource()
	type result struct {
		devices []MachineDevice
		err     error
	}
	done := make(chan result, 1)
	go func() {
		devices, err := source(t.Context())
		done <- result{devices, err}
	}()
	<-entered
	// Match teardown restoring defaults while the old scan is still pending.
	extensionIdentityProbe = identity("replacement inventory source")
	close(release)
	got := <-done
	if got.err != nil || len(got.devices) != 1 {
		t.Fatal("inventory scan failed", got.err, len(got.devices))
	}
	if got.devices[0].ExtensionFingerprint != want {
		t.Fatal("pending inventory scan used replacement identity defaults")
	}
}

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
