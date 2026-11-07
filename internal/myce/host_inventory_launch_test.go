package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHostInventoryExpiredObservationDoesNotOverrideFreshLaunch(t *testing.T) {
	q, initial := fixture(t)
	clock := &workerFakeClock{now: initial.Now()}
	q.clock = clock
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := recoveryWorker(t)
	w.Clock, w.Client = clock, Client{URL: server.URL, Token: "test"}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	live := inventoryReviewDevices()[:1]
	w.Machine.Devices = live
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return live, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", "exit 0"}
	j := submit(t, q, s)
	assigned := check(t, q, w.Machine, nil).Job
	if assigned == nil || assigned.ID != j.ID || assigned.Attempt != 1 {
		t.Fatal("missing first assignment")
	}
	// The observation lane can expire while this exact assignment prepares.
	// Keep admission conservative, but check current hardware at execution.
	lane := hostProbeLane[[]MachineDevice]{clock: clock, known: true, value: live, at: clock.Now(), timing: probeTiming{base: time.Second, cores: 2}}
	lane.observation = lane.timing.observe(clock.Now(), clock.Now(), false)
	clock.Advance(lane.observation.freshness())
	observed, probeErr := hostInventorySnapshot(&lane)
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatal("fixture did not expire its observation")
	}
	if err := w.refreshInventory(t.Context(), observed, probeErr, store); err != nil {
		t.Fatal(err)
	}
	if w.Machine.Devices[0].Reason != "device_probe_unavailable" || w.Machine.Devices[0].FreeMemoryGB != 0 {
		t.Fatal("expired observation still advertises capacity")
	}
	bound, err := w.forAttempt(*assigned)
	if err != nil {
		t.Fatal("fresh matching launch probe lost to cached observation expiry", err)
	}
	p, err := startAfterFixtureInputs(t, bound, store, *assigned)
	if err != nil {
		t.Fatal(err)
	}
	finished := d5Finish(t, p)
	if !finished.Success || finished.Attempt != 1 {
		t.Fatal("healthy first launch did not complete", finished)
	}
	if err := q.Complete(finished); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "succeeded" || got.Attempt != 1 || got.InfrastructureRetries != 0 {
		t.Fatal("healthy launch churned attempts", got)
	}
	if _, err := hostInventorySnapshot(&lane); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("launch verification renewed the old background observation")
	}
	// An actual current failure and a cached non-observation policy fence
	// remain authoritative; the repair is not a blanket refusal bypass.
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return nil, errors.New("driver unavailable") }
	if _, err := w.forAttempt(*assigned); err == nil {
		t.Fatal("current launch probe failure admitted")
	}
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return live, nil }
	w.Machine.Devices[0].Reason = "device_identity_mismatch"
	if _, err := w.forAttempt(*assigned); err == nil {
		t.Fatal("saved identity fence bypassed")
	}
}
