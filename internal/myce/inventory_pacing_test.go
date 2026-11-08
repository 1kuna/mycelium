package myce

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func finishInventoryProbe(t *testing.T, lane *hostProbeLane[[]MachineDevice], source func(context.Context) ([]MachineDevice, error)) {
	t.Helper()
	lane.step(t.Context(), source)
	if lane.done != nil {
		<-lane.done
	}
	lane.step(t.Context(), source)
}

func TestInventorySemanticChangesAndBackoff(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	free, active := 4.0, 1.0
	original := []MachineDevice{{ID: "0", Platform: "xpu", Identity: "card", Selector: "0", Fingerprint: "driver", FreeMemoryGB: free, BudgetGB: 8, Telemetry: &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}}}
	changed := slices.Clone(original)
	telemetry := *original[0].Telemetry
	changed[0].Telemetry = &telemetry
	telemetry.At = telemetry.At.Add(time.Second)
	if !sameInventory(original, changed) {
		t.Fatal("timestamp changed hardware inventory")
	}
	active, free = 80, 2
	changed[0].FreeMemoryGB = free
	if !sameInventory(original, changed) {
		t.Fatal("workload counters changed hardware inventory")
	}
	for _, platform := range []string{"xpu", "cuda", "metal", "fake"} {
		t.Run(platform, func(t *testing.T) {
			lane := hostProbeLane[[]MachineDevice]{clock: clock, equal: sameInventory, timing: probeTiming{base: time.Second, cores: 2}}
			source := func(context.Context) ([]MachineDevice, error) {
				d := slices.Clone(changed)
				d[0].Platform = platform
				s := *d[0].Telemetry
				s.At = clock.Now()
				d[0].Telemetry = &s
				d[0].FreeMemoryGB = float64(clock.Now().Unix() % 8)
				clock.Advance(250 * time.Millisecond)
				return d, nil
			}
			finishInventoryProbe(t, &lane, source)
			first := lane.observation.PeriodSeconds
			clock.Advance(time.Hour)
			finishInventoryProbe(t, &lane, source)
			if lane.observation.PeriodSeconds <= first {
				t.Fatal("unchanged hardware did not back off", first, lane.observation)
			}
		})
	}
	changed[0].Identity = "replacement"
	if sameInventory(original, changed) {
		t.Fatal("replacement card ignored")
	}
	changed = slices.Clone(original)
	changed[0].Reason = "device_probe_unavailable"
	if sameInventory(original, changed) {
		t.Fatal("health transition ignored")
	}
}

func countedInventoryProbe(t *testing.T, enumerations, queries *int) MachineProbe {
	p := familyProbe(t, "")
	p.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "nvidia-smi":
			return nil, exec.ErrNotFound
		case "uname":
			return []byte("fixture kernel"), nil
		case "xpu-smi":
			*queries++
			return []byte("0000:01:00.0, 4096\n0000:02:00.0, 2048\n"), nil
		}
		return nil, errors.New("unexpected fixture command")
	}
	commandEnv := p.CommandEnv
	p.CommandEnv = func(ctx context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
		*enumerations++
		return commandEnv(ctx, env, name, args...)
	}
	p.XeActivity = func(context.Context, string) (float64, error) { return 10, nil }
	return p
}

func TestInventoryFingerprintsReuseEnumeration(t *testing.T) {
	previous := extensionIdentityProbe
	defer func() { extensionIdentityProbe = previous }()
	enumerations, queries := 0, 0
	p := countedInventoryProbe(t, &enumerations, &queries)
	extensionIdentityProbe = func() MachineProbe { return p }
	w := Worker{Machine: Machine{GPU: "xpu", FreeMemoryGB: 8}, FixedAcceleratorMemory: true}
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) { return w.localInventoryWithProbe(ctx, p) }
	devices, err := w.inventorySource()(t.Context())
	if err != nil || len(devices) != 2 || enumerations != 1 || queries != 0 {
		t.Fatal("discovery multiplied helpers", len(devices), enumerations, queries, err)
	}
	for _, d := range devices {
		expected, err := extensionFingerprintFrom(t.Context(), d.Platform, d.ID, func() MachineProbe { return p })
		if err != nil || expected != d.ExtensionFingerprint || expected == "" {
			t.Fatal("qualification fingerprint changed", err)
		}
	}
}

func TestXPUTelemetryHostIntervalAndUnknown(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	enumerations, queries := 0, 0
	p := countedInventoryProbe(t, &enumerations, &queries)
	devices, _ := p.Accelerators(t.Context())
	w := Worker{Clock: clock}
	lane := hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: deviceMemorySamplePeriod, cores: 2}}
	source := func(ctx context.Context) ([]MachineDevice, error) {
		d := slices.Clone(devices)
		w.sampleXPUTelemetry(ctx, p, d)
		return d, nil
	}
	finishInventoryProbe(t, &lane, source)
	for i := 0; i < 49; i++ {
		clock.Advance(100 * time.Millisecond)
		lane.step(t.Context(), source)
	}
	if queries != 1 || !deviceTelemetryReady(lane.value[0].Telemetry, clock.Now()) || *lane.value[1].Telemetry.FreeGB != 2 {
		t.Fatal("host telemetry duplicated or cross-wired", queries, lane.value)
	}
	clock.Advance(100 * time.Millisecond)
	finishInventoryProbe(t, &lane, source)
	if queries != 2 {
		t.Fatal("consumer interval not sampled", queries)
	}
	// A failed new query must clear a previously successful counter.
	p.Command = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("unavailable") }
	clock.Advance(deviceMemorySamplePeriod)
	finishInventoryProbe(t, &lane, source)
	if lane.value[0].Telemetry.FreeGB != nil || deviceTelemetryReady(lane.value[0].Telemetry, clock.Now()) {
		t.Fatal("missing counter reused")
	}
	p.Command = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("0000:01:00.0,4096\n0000:01:00.0,4096\n0000:02:00.0,N/A\n"), nil
	}
	w.sampleXPUTelemetry(t.Context(), p, devices)
	for _, d := range devices {
		if d.Telemetry.FreeGB != nil {
			t.Fatal("ambiguous or N/A counter granted capacity")
		}
	}
}

func TestInventoryHelperLaunchMeasurement(t *testing.T) {
	previous := extensionIdentityProbe
	defer func() { extensionIdentityProbe = previous }()
	type counts struct{ enumerations, queries int }
	measure := func(legacy bool) counts {
		var c counts
		clock := &workerFakeClock{now: time.Unix(1000, 0)}
		p := countedInventoryProbe(t, &c.enumerations, &c.queries)
		extensionIdentityProbe = func() MachineProbe { return p }
		w := Worker{Clock: clock, Machine: Machine{GPU: "xpu", FreeMemoryGB: 8}, FixedAcceleratorMemory: true}
		w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) { return w.localInventoryWithProbe(ctx, p) }
		current := w.inventorySource()
		source := func(ctx context.Context) ([]MachineDevice, error) {
			var d []MachineDevice
			var err error
			if legacy {
				d, err = w.InventoryProbe(ctx)
				for i := range d {
					// The former per-card query plus full-host fingerprint scan.
					p.Command(ctx, "xpu-smi", "--id="+d[i].pciAddress)
					d[i].ExtensionFingerprint, _ = extensionFingerprintFrom(ctx, d[i].Platform, d[i].ID, func() MachineProbe { return p })
				}
			} else {
				d, err = current(ctx)
			}
			clock.Advance(250 * time.Millisecond)
			return d, err
		}
		inventory := hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: time.Second, cores: 2}}
		if !legacy {
			inventory.equal = sameInventory
		}
		telemetry := hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: deviceMemorySamplePeriod, cores: 2}}
		sensor := func(ctx context.Context) ([]MachineDevice, error) {
			d := slices.Clone(inventory.value)
			w.sampleXPUTelemetry(ctx, p, d)
			return d, nil
		}
		end := clock.Now().Add(time.Minute)
		for clock.Now().Before(end) {
			finishInventoryProbe(t, &inventory, source)
			if !legacy {
				finishInventoryProbe(t, &telemetry, sensor)
			}
			clock.Advance(100 * time.Millisecond)
		}
		return c
	}
	before, after := measure(true), measure(false)
	t.Logf("fake two-card inventory, 1s heartbeat, 250ms scan, 60s: before enumeration=%d memory=%d; after enumeration=%d memory=%d helper launches/minute", before.enumerations, before.queries, after.enumerations, after.queries)
	if after.enumerations >= before.enumerations/3 || after.queries > 12 || after.queries >= before.queries {
		t.Fatal("helper launch storm retained", before, after)
	}
}

func TestHostInventoryTelemetryIdentityAndExpiry(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	free, active := 3.0, 10.0
	card := MachineDevice{ID: "0", Platform: "xpu", Identity: "card", Selector: "0", Fingerprint: "driver", pciAddress: "0000:01:00.0", FreeMemoryGB: 8}
	observer := hostInventoryObserver{
		lane:      hostProbeLane[[]MachineDevice]{clock: clock, known: true, at: clock.Now(), value: []MachineDevice{card}, observation: &ProbeObservation{PeriodSeconds: 60, CostSeconds: 1, ValidSeconds: 61}},
		telemetry: hostProbeLane[[]MachineDevice]{clock: clock, known: true, at: clock.Now(), observation: &ProbeObservation{PeriodSeconds: 5, CostSeconds: 1, ValidSeconds: 6}},
	}
	sampled := card
	sampled.Telemetry = &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}
	observer.telemetry.value = []MachineDevice{sampled}
	got, err := observer.snapshot()
	if err != nil || got[0].FreeMemoryGB != 3 || !deviceTelemetryReady(got[0].Telemetry, clock.Now()) {
		t.Fatal("fresh telemetry not merged", got, err)
	}
	sampled.Telemetry.ActivePercent = nil
	got, _ = observer.snapshot()
	if got[0].FreeMemoryGB != 3 || deviceTelemetryReady(got[0].Telemetry, clock.Now()) {
		t.Fatal("missing activity lost measured memory or authorized sharing")
	}
	observer.lane.value[0].Identity = "replacement"
	got, _ = observer.snapshot()
	if got[0].Telemetry != nil {
		t.Fatal("old device telemetry transferred to replacement")
	}
	observer.lane.value[0] = card
	clock.Advance(deviceMemorySampleFreshFor)
	got, _ = observer.snapshot()
	if got[0].Telemetry != nil || deviceTelemetryReady(sampled.Telemetry, clock.Now()) {
		t.Fatal("stale telemetry refreshed by inventory snapshot")
	}
	if observer.lane.value[0].FreeMemoryGB != 8 {
		t.Fatal("snapshot mutated discovery evidence")
	}
}

func TestWorkerTelemetryUsesInjectedHostSource(t *testing.T) {
	q, _ := fixture(t)
	var queries atomic.Int32
	previous := extensionIdentityProbe
	extensionIdentityProbe = func() MachineProbe {
		return MachineProbe{OS: "linux", Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name == "xpu-smi" {
				queries.Add(1)
				return []byte("0000:01:00.0,4096\n"), nil
			}
			return []byte("fixture kernel"), nil
		}, XeActivity: func(context.Context, string) (float64, error) { return 10, nil }}
	}
	t.Cleanup(func() { extensionIdentityProbe = previous })
	card := MachineDevice{ID: "0", Platform: "xpu", Identity: "card", Selector: "0", Fingerprint: "driver", pciAddress: "0000:01:00.0", MemoryMode: "dedicated", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}
	runInventoryReviewWorker(t, q, Worker{Machine: machine("telemetry", "xpu"), InventoryProbe: func(context.Context) ([]MachineDevice, error) {
		return []MachineDevice{card}, nil
	}})
	awaitInventoryReview(t, func() bool {
		s, err := q.publicSnapshot()
		return err == nil && len(s.Machines) == 1 && len(s.Machines[0].Devices) == 1 && deviceTelemetryReady(s.Machines[0].Devices[0].Telemetry, q.clock.Now())
	})
	if queries.Load() == 0 {
		t.Fatal("worker bypassed injected telemetry")
	}
}

func TestHostInventoryDrainsTelemetryAfterDeviceRemoval(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	observer := hostInventoryObserver{lane: hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: time.Millisecond, cores: 2}}}
	var scans atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	observer.sampleTelemetry = func(context.Context, []MachineDevice) ([]MachineDevice, error) {
		close(entered)
		<-release
		return nil, nil
	}
	source := func(context.Context) ([]MachineDevice, error) {
		if scans.Add(1) > 1 {
			return []MachineDevice{}, nil
		}
		return []MachineDevice{{ID: "0", Platform: "fake", Identity: "card", Selector: "0"}}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); observer.run(ctx, source) }()
	defer func() { cancel(); <-done }()
	<-entered
	clock.Advance(time.Second)
	await(t, func() bool {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		return scans.Load() > 1 && len(observer.lane.value) == 0
	})
	close(release)
	await(t, func() bool { observer.mu.Lock(); defer observer.mu.Unlock(); return observer.telemetry.pending == nil })
}
