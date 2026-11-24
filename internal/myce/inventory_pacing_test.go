package myce

import (
	"context"
	"errors"
	"os/exec"
	"slices"
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
	p.XPUFreeMemory = func(context.Context) (map[string]float64, error) {
		*queries++
		return map[string]float64{"0000:01:00.0": 4, "0000:02:00.0": 2}, nil
	}
	return p
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
			inventory.equal = sameInventoryObservation
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

// Exercise the observer's real demand/freshness call site, with the same
// heartbeat frequency that formerly drove repeated vendor initialization.
func TestXPUTelemetryDemandInvocationCount(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	reads, vendors := 0, 0
	p := MachineProbe{OS: "linux", XPUFreeMemory: func(context.Context) (map[string]float64, error) {
		reads++
		return map[string]float64{"0000:01:00.0": 4}, nil
	}, Command: func(context.Context, string, ...string) ([]byte, error) {
		vendors++
		return nil, errors.New("vendor initialization forbidden")
	}, XeActivity: func(context.Context, string) (float64, error) { return 20, nil }}
	w := Worker{Clock: clock}
	card := MachineDevice{ID: "0", Platform: "xpu", Identity: "card", Selector: "0", pciAddress: "0000:01:00.0"}
	observer := hostInventoryObserver{
		lane:      hostProbeLane[[]MachineDevice]{clock: clock, known: true, at: clock.Now(), value: []MachineDevice{card}, observation: &ProbeObservation{PeriodSeconds: 3600, CostSeconds: 1, ValidSeconds: 3601}},
		telemetry: hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: deviceMemorySampleFreshFor}},
	}
	observer.sampleTelemetry = func(ctx context.Context, devices []MachineDevice) ([]MachineDevice, error) {
		w.sampleXPUTelemetry(ctx, p, devices)
		return devices, nil
	}
	step := func() {
		observer.stepTelemetry(t.Context())
		if observer.telemetry.done != nil {
			<-observer.telemetry.done
		}
		observer.stepTelemetry(t.Context())
	}
	for range 600 {
		step()
		clock.Advance(100 * time.Millisecond)
	}
	if reads != 0 {
		t.Fatal("idle/CPU heartbeat polled memory", reads)
	}
	observer.requestTelemetry(true)
	start := clock.Now()
	for range 600 {
		step()
		clock.Advance(100 * time.Millisecond)
	}
	if reads != 6 || vendors != 0 {
		t.Fatal("sharing did not reuse the freshness window", reads, vendors)
	}
	got, _ := observer.snapshot()
	if len(got) != 1 || got[0].Telemetry == nil || !got[0].Telemetry.At.Equal(start.Add(50*time.Second)) {
		t.Fatal("snapshot renewed or lost the original sample time", got)
	}
	observer.requestTelemetry(false)
	for range 600 {
		step()
		clock.Advance(100 * time.Millisecond)
	}
	if reads != 6 || vendors != 0 {
		t.Fatal("withdrawn demand continued polling", reads, vendors)
	}
	got, _ = observer.snapshot()
	if got[0].Telemetry != nil {
		t.Fatal("withdrawn demand renewed stale sharing evidence")
	}
}
