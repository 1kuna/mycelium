package myce

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// #1565: the queue has accepted Started while the worker still owns the
// original assignment. Replay the retained envelope/credit and comparison;
// the sensor sequence before that comparison was not retained.

// A positive preflight refusal must reach the real queue immediately, without
// waiting for an experiment's start window or a simulated hour to elapse.
func TestAssignChurnPreflightRefusalReturnsPromptly(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 16)
	next := submit(t, q, cpuSpec(1, 4.125))
	offer := assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != next.ID {
		t.Fatal("missing initial assignment")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := recoveryWorker(t)
	w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
	probe := syntheticProfileProbe().Machine
	w.cpuRuntimeProbe, w.control = &probe, &processControl{clock: clock}
	w.HostMemorySource = func(context.Context) (float64, error) { return 3.2438201904296875, nil }
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	root.state.Session = offer.Session
	// Match the worker loop's normalization before creating its attempt actor.
	offer.Checkpoint = filepath.Join(t.TempDir(), offer.Checkpoint)
	store, err := newAttemptStore(root, *offer)
	if err != nil {
		t.Fatal(err)
	}
	a := &attemptSupervisor{store: store, job: *offer, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go a.run(ctx)
	select {
	case <-a.done:
	case <-time.After(testWaitTimeout):
		cancel()
		<-a.done
		t.Fatal("refused assignment was retained")
	}
	got := job(t, q, next.ID)
	if got.State != "queued" || got.Losses != 0 || !got.Copies[0].ReleaseConfirmed || got.Copies[0].ReleasedAt == nil || !clock.Now().Equal(*got.Copies[0].ReleasedAt) {
		t.Fatalf("refusal did not promptly release and requeue: %+v", got)
	}
	if len(got.LaunchCapacityWait) == 0 || got.Attempt != 1 {
		t.Fatal("refusal lost its typed wait or changed attempt", got.Attempt)
	}
}

// The retained incident has the worker comparison but not its matching queue
// snapshot. Replay both numbers without attributing the difference to a sensor,
// transport delay or changing reservations that the evidence cannot distinguish.

func assignChurnWaveFixture(t *testing.T, ram float64) (*Queue, *workerFakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, wave := fanOutFixture(t, 1)
	m.Devices = []MachineDevice{}
	m.CapabilityProfile.SystemMemoryBytes = 32 << 30
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	check(t, q, m, nil)
	wave.Priority, wave.SizeHint.HostMemoryGB = "production", ram
	return q, clock, m, wave
}

func TestAssignChurnProductionAfterRunningWave(t *testing.T) {
	q, clock, m, wave := assignChurnWaveFixture(t, 16)
	parent := submit(t, q, wave)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.Batch == nil || running.Batch.Parent != parent.ID {
		t.Fatal("missing running wave", running)
	}
	production := cpuSpec(1, 32)
	production.Priority = "production"
	demand := submit(t, q, production)
	clock.Advance(time.Second)
	lower := submit(t, q, cpuSpec(1, 4))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatal("running wave's eventual release was hidden from production admission", got.ID)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != lower.ID {
		t.Fatal("withdrawn demand still withheld capacity", got)
	}
}
