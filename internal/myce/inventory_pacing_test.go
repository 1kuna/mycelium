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
	if !deviceTelemetryReady(lane.value[0].Telemetry, clock.Now()) || *lane.value[1].Telemetry.FreeGB != 2 {
		t.Fatal("host telemetry duplicated or cross-wired", queries, lane.value)
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

	t.Run("pending-sample-after-device-removal", func(t *testing.T) {

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

	})

	t.Run("changing-telemetry-discovery-backoff", func(t *testing.T) {
		clock := &workerFakeClock{now: time.Unix(1000, 0)}
		observer := hostInventoryObserver{lane: hostProbeLane[[]MachineDevice]{clock: clock, timing: probeTiming{base: time.Second, cores: 2}}}
		source := func(context.Context) ([]MachineDevice, error) {
			free, active := float64(clock.Now().Unix()%8), 80.0
			d := MachineDevice{ID: "0", Platform: "xpu", Identity: "card", Selector: "0", Fingerprint: "driver", FreeMemoryGB: free, BudgetGB: 8, Telemetry: &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}}
			clock.Advance(250 * time.Millisecond)
			return []MachineDevice{d}, nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); observer.run(ctx, source) }()
		defer func() { cancel(); <-done }()
		await(t, func() bool { observer.mu.Lock(); defer observer.mu.Unlock(); return observer.lane.observation.valid() })
		observer.mu.Lock()
		first := observer.lane.observation.PeriodSeconds
		observer.mu.Unlock()
		clock.Advance(time.Hour + time.Second)
		await(t, func() bool {
			observer.mu.Lock()
			defer observer.mu.Unlock()
			return observer.lane.observation.PeriodSeconds > first
		})
		observer.mu.Lock()
		defer observer.mu.Unlock()
		if observer.lane.observation.PeriodSeconds <= first {
			t.Fatal("unchanged hardware did not back off", first, observer.lane.observation)
		}
	})
}

func TestWorkerTelemetryUsesInjectedHostSource(t *testing.T) {
	q, _ := fixture(t)
	previous := extensionIdentityProbe
	extensionIdentityProbe = func() MachineProbe {
		return MachineProbe{OS: "linux", Files: syntheticIdentityProbe().Files, Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name == "xpu-smi" {
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

}
