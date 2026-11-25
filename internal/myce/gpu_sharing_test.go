package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startGPUSharingJob(t *testing.T, q *Queue, m Machine, s Spec, active ...*Job) *Job {
	t.Helper()
	want := submit(t, q, s)
	got := assignedJob(machineCheck(t, q, m, active...))
	if got == nil || got.ID != want.ID {
		t.Fatalf("expected assignment of %s, got %v; %s", want.ID, got, job(t, q, want.ID).Reason)
	}
	if err := q.Started(result(got, false)); err != nil {
		t.Fatal(err)
	}
	return got
}

func completeGPUSharingJob(t *testing.T, q *Queue, j *Job, measured bool) {
	t.Helper()
	r := result(j, true)
	if measured {
		host, device := 1.0, deviceShareGB(j)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}

// Refresh exact holder contact while advancing long synthetic runtimes.
func advanceGPUSharingRuns(t *testing.T, q *Queue, c *fakeClock, m Machine, elapsed time.Duration, active ...*Job) {
	t.Helper()
	for elapsed > 0 {
		step := min(elapsed, 4*time.Second)
		c.Advance(step)
		machineCheck(t, q, m, active...)
		elapsed -= step
	}
}

func measureGPUSharingPair(t *testing.T, q *Queue, c *fakeClock, m Machine, s Spec, first, last time.Duration, measured bool) {
	t.Helper()
	a := startGPUSharingJob(t, q, m, s)
	b := startGPUSharingJob(t, q, m, s, a)
	advanceGPUSharingRuns(t, q, c, m, first, a, b)
	completeGPUSharingJob(t, q, a, measured)
	advanceGPUSharingRuns(t, q, c, m, last-first, b)
	completeGPUSharingJob(t, q, b, true)
}

func TestUnifiedGPUSharingLearnsThroughputAndKeepsOtherWorkMoving(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	heavy := sharedSpec("experiment", 14)
	light := sharedSpec("experiment", 2)
	initial := startGPUSharingJob(t, q, m, heavy)
	advanceGPUSharingRuns(t, q, clock, m, 162*time.Second, initial)
	completeGPUSharingJob(t, q, initial, true)
	// A solo sample alone still allows sharing. A pair with a missing peak
	// cannot change the decision. The next clean pair measures the loss.
	measureGPUSharingPair(t, q, clock, m, heavy, 480*time.Second, 570*time.Second, false)
	measureGPUSharingPair(t, q, clock, m, heavy, 480*time.Second, 570*time.Second, true)

	run := startGPUSharingJob(t, q, m, heavy)
	blocked := submit(t, q, heavy)
	if got := assignedJob(machineCheck(t, q, m, run)); got != nil {
		t.Fatalf("slow-sharing family received another slot: %s", got.ID)
	}
	if why := job(t, q, blocked.ID).Reason; !strings.Contains(why, "measured same-family throughput") {
		t.Fatalf("missing measured slot wait: %q", why)
	}
	lightRun := startGPUSharingJob(t, q, m, light, run)
	lightPeer := startGPUSharingJob(t, q, m, light, run, lightRun)
	stale := result(run, true)
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatalf("stale release returned %v", err)
	}
	if got := assignedJob(machineCheck(t, q, m, run, lightRun, lightPeer)); got != nil {
		t.Fatal("stale result opened a slot")
	}
	completeGPUSharingJob(t, q, run, false)
	if got := assignedJob(machineCheck(t, q, m, lightRun, lightPeer)); got == nil || got.ID != blocked.ID {
		t.Fatalf("exact release did not open a slot with other work still running: %v", got)
	}
}

func TestUnifiedGPUSharingUpdatesWithNewSoloObservations(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	s := sharedSpec("experiment", 14)
	// A pair without a solo benchmark still follows the sharing default.
	measureGPUSharingPair(t, q, clock, m, s, 15*time.Second, 17*time.Second, true)
	measureGPUSharingPair(t, q, clock, m, s, 15*time.Second, 17*time.Second, true)
	a := startGPUSharingJob(t, q, m, s)
	advanceGPUSharingRuns(t, q, clock, m, 5*time.Second, a)
	completeGPUSharingJob(t, q, a, true)
	b := startGPUSharingJob(t, q, m, s)
	pending := submit(t, q, s)
	if got := assignedJob(machineCheck(t, q, m, b)); got != nil {
		t.Fatal("solo measurement did not activate the restriction")
	}
	advanceGPUSharingRuns(t, q, clock, m, 14*time.Second, b)
	completeGPUSharingJob(t, q, b, true)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != pending.ID {
		t.Fatal("pending peer failed to start after release")
	}
	startGPUSharingJob(t, q, m, s, first)
}

func TestUnifiedGPUSharingPreservesProductionPriority(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	// The fake backend permits mixed tiers in ordinary sharing. Here it is
	// the measured conflict that requires the release, rather than CUDA's
	// standing tier isolation.
	m.GPU, m.Devices[0].Platform, m.CapabilityProfile.Accelerators[0].Type = "fake", "fake", "fake"
	s := sharedSpec("experiment", 14)
	base := startGPUSharingJob(t, q, m, s)
	advanceGPUSharingRuns(t, q, clock, m, 5*time.Second, base)
	completeGPUSharingJob(t, q, base, true)
	measureGPUSharingPair(t, q, clock, m, s, 15*time.Second, 17*time.Second, true)
	lower := startGPUSharingJob(t, q, m, s)
	s.Priority = "production"
	prod := submit(t, q, s)
	reply := machineCheck(t, q, m, lower)
	if got := assignedJob(reply); got != nil {
		t.Fatalf("production bypassed stop/release custody: %s", got.ID)
	}
	requireState(t, q, lower.ID, "stopping")
	stale := result(lower, false)
	stale.Attempt++
	stale.ReleaseOnly = true
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m, lower)); got != nil {
		t.Fatalf("production started after an inexact release acknowledgement: %s", got.ID)
	}
	finishInterrupted(t, q, lower)
	production := assignedJob(machineCheck(t, q, m))
	if production == nil || production.ID != prod.ID {
		t.Fatal("production did not get the device after release")
	}
	s.Priority = "experiment"
	submit(t, q, s)
	if got := assignedJob(machineCheck(t, q, m, production)); got != nil {
		t.Fatalf("lower tier bypassed production: %s", got.ID)
	}
	requireState(t, q, production.ID, "running")
}

func TestUnifiedGPUSharingRestoresLearnedPolicy(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	m := sharedUnifiedMachine()
	s := sharedSpec("experiment", 14)
	base := startGPUSharingJob(t, q, m, s)
	advanceGPUSharingRuns(t, q, clock, m, 5*time.Second, base)
	completeGPUSharingJob(t, q, base, true)
	measureGPUSharingPair(t, q, clock, m, s, 15*time.Second, 17*time.Second, true)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	fakeCheckpointAvailability(t, restarted, true)
	run := startGPUSharingJob(t, restarted, m, s)
	submit(t, restarted, s)
	if got := assignedJob(machineCheck(t, restarted, m, run)); got != nil {
		t.Fatalf("restart forgot the measured constraint: %s", got.ID)
	}
}

func TestDevUnifiedGPUInventory(t *testing.T) {
	root := newTestStateRoot(t)
	d, err := startTestDev(t, DevConfig{
		Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		Token: "unified-dev-fixture", Workers: 1, MemoryGB: 8, FakeUnifiedMemory: true,
		EnvironmentCacheLimit: 1 << 30, Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	awaitTestDevDeviceFingerprints(t, d)
	s := sharedSpec("experiment", 1)
	s.Resumable = false
	queued, err := d.Client().Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	var completed Job
	probeWait(t, ctx, func() bool {
		completed = *job(t, d.Queue, queued.ID)
		return terminal(completed.State)
	})
	view, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != "succeeded" || len(completed.AllocatedDevices) != 1 ||
		len(view.Machines) != 1 || !view.Machines[0].Devices[0].UnifiedMemory {
		t.Fatalf("dev job state=%s device=%v reason=%s", completed.State, completed.AllocatedDevices, completed.Reason)
	}
}
