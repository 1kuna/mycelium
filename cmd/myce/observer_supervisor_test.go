package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObserverSupervisorRecovery(t *testing.T) {
	for _, mode := range []string{"death", "coordinator-stall", "reader-stall", "intentional-wait", "launch-failure", "ready-and-exited", "busy-log"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			clock := &waitClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
			birth := clock.Now()
			lock, err := profileLock(filepath.Join(root, "observer.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			entry := filepath.Join(root, "arm.json")
			if err = os.WriteFile(entry, []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			launches, kills, pauses := 0, 0, 0
			if mode == "busy-log" {
				logLock, err := profileLock(filepath.Join(root, "observer-log.lock"))
				if err != nil {
					t.Fatal(err)
				}
				defer logLock.Close()
			}
			failedBirth := false
			var child *supervisedObserverProcess
			var exit chan error
			var diagnostic observerDiagnostic
			write := func() {
				t.Helper()
				if err := saveAtomicProfileJSON(filepath.Join(root, "observer.status"), diagnostic, false); err != nil {
					t.Fatal(err)
				}
			}
			launch := func() (*supervisedObserverProcess, error) {
				launches++
				if mode == "launch-failure" && launches == 2 {
					failedBirth = true
					return nil, errors.New("sensitive startup detail")
				}
				exit = make(chan error, 1)
				ready := make(chan error, 1)
				ready <- nil
				child = &supervisedObserverProcess{pid: 100 + launches, ready: ready, exited: exit, kill: func() error {
					kills++
					select {
					case exit <- nil:
					default:
					}
					return nil
				}}
				diagnostic = observerDiagnostic{PID: child.pid, StartedAt: clock.Now(), CapturedAt: clock.Now(), Phase: "observing", Registrations: 1}
				if mode == "reader-stall" {
					diagnostic.Readers = map[uint64]observerReadDiagnostic{1: {Phase: "batch_read", Jobs: 1, StartedAt: clock.Now(), CapturedAt: clock.Now()}}
				}
				if mode == "intentional-wait" {
					diagnostic.NextWakeAt = clock.Now().Add(time.Hour)
					diagnostic.NextReadAt = diagnostic.NextWakeAt
				}
				write()
				if mode == "ready-and-exited" && launches == 1 {
					exit <- nil
				}
				return child, nil
			}
			pause := func(ctx context.Context, d time.Duration) error {
				pauses++
				clock.advance(d)
				if pauses > 10000 {
					return errors.New("supervisor failed to recover")
				}
				if launches == 1 {
					switch mode {
					case "death", "launch-failure", "busy-log":
						if pauses == 1 {
							exit <- nil
						}
					case "reader-stall":
						// A live peer keeps all global timestamps fresh. Only the original
						// reader boundary can diagnose this authority's silent half-open read.
						diagnostic.CapturedAt = clock.Now()
						diagnostic.LastReadAt = clock.Now()
						write()
					case "intentional-wait":
						if clock.Now().Before(birth.Add(time.Hour + observerNotifierBudget)) {
							return nil
						}
					}
				}
				recovered := launches > 1 && !(mode == "launch-failure" && launches == 2)
				if recovered {
					if err := os.Remove(entry); err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
					select {
					case exit <- nil:
					default:
					}
				}
				return nil
			}
			readyCalls := 0
			err = runObserverSupervisor(context.Background(), root, lock, clock, pause, func() error { readyCalls++; return nil }, launch)
			if err != nil {
				t.Fatal(err)
			}
			if readyCalls != 1 || launches < 2 {
				t.Fatalf("no automatic adoption: readiness=%d births=%d", readyCalls, launches)
			}
			if mode == "launch-failure" && !failedBirth {
				t.Fatal("recovery did not exercise failed replacement")
			}
			var saved observerSupervision
			if err = readProfileJSON(filepath.Join(root, "observer.supervision"), &saved); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, "observer.log"))
			if err != nil && mode != "busy-log" {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "sensitive") {
				t.Fatal("raw launch error leaked")
			}
			if mode == "reader-stall" || mode == "coordinator-stall" || mode == "intentional-wait" {
				if kills == 0 || !strings.Contains(string(data), `"reason":"stalled"`) {
					t.Fatal("silent owner was not retired")
				}
				if saved.Window <= 21*time.Second {
					t.Fatal("missed window did not expand recovery allowance")
				}
			}
			if mode == "reader-stall" && !strings.Contains(string(data), `"phase":"batch_read"`) {
				t.Fatal("replacement erased stalled reader evidence")
			}
			if saved.LastRecovery == nil || saved.Restarts < 1 {
				t.Fatal("missing retained recovery record")
			}
			// The supervisor must release lifetime ownership while it still owns
			// startup.lock on idle exit, so the next registration can start an owner.
			next, err := profileLock(filepath.Join(root, "observer.lock"))
			if err != nil {
				t.Fatal("idle owner retained lifetime lock", err)
			}
			next.Close()
		})
	}
}

// #919 on r6.10–r6.14: at every release handoff the old supervisor read the
// successor's status, called it a stall and SIGKILLed the handing-off child.
// The successor's readiness write then failed ("write ready: broken pipe"),
// it exited, and arms went unobserved until a keyed command restarted one.
func TestObserverSupervisorHandoffSuccessor(t *testing.T) {
	root := t.TempDir()
	clock := &waitClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = os.WriteFile(filepath.Join(root, "arm.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	exit := make(chan error, 1)
	launches, kills, pauses := 0, 0, 0
	launch := func() (*supervisedObserverProcess, error) {
		launches++
		ready := make(chan error, 1)
		ready <- nil
		status := observerDiagnostic{PID: 101, StartedAt: clock.Now(), CapturedAt: clock.Now(), Phase: "observing", Registrations: 1}
		if err := saveAtomicProfileJSON(filepath.Join(root, "observer.status"), status, false); err != nil {
			t.Fatal(err)
		}
		return &supervisedObserverProcess{pid: 101, ready: ready, exited: exit, kill: func() error {
			kills++
			select {
			case exit <- errors.New("signal: killed"):
			default:
			}
			return nil
		}}, nil
	}
	pause := func(ctx context.Context, d time.Duration) error {
		pauses++
		clock.advance(d)
		switch pauses {
		case 3:
			// Hours later, the child starts a newer image. The successor's first
			// status lands while the child waits for its readiness byte.
			clock.advance(6 * time.Hour)
			successor := observerDiagnostic{PID: 202, StartedAt: clock.Now(), CapturedAt: clock.Now(), Phase: "observing", Registrations: 1,
				Readers: map[uint64]observerReadDiagnostic{1: {Phase: "batch_read", Jobs: 1, StartedAt: clock.Now(), CapturedAt: clock.Now()}}}
			if err := saveAtomicProfileJSON(filepath.Join(root, "observer.status"), successor, false); err != nil {
				t.Fatal(err)
			}
		case 8:
			exit <- nil // Readiness acknowledged; the handing-off child exits.
		case 1000:
			return errors.New("supervisor never released a handed-off child")
		}
		return nil
	}
	if err = runObserverSupervisor(context.Background(), root, lock, clock, pause, func() error { return nil }, launch); err != nil {
		t.Fatal(err)
	}
	if kills != 0 || launches != 1 {
		t.Fatalf("handoff treated as a stall: kills=%d launches=%d", kills, launches)
	}
	if data, _ := os.ReadFile(filepath.Join(root, "observer.log")); strings.Contains(string(data), `"reason":"stalled"`) {
		t.Fatal("handoff recorded as a stalled recovery")
	}
}

func TestObserverSupervisorAdaptiveWindow(t *testing.T) {
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	state := observerSupervision{ChildPID: 5, Window: 2 * time.Minute}
	d := observerDiagnostic{PID: 5, CapturedAt: now, Readers: map[uint64]observerReadDiagnostic{1: {CapturedAt: now}}}
	if state.stalled(now.Add(time.Minute), now, d) {
		t.Fatal("ignored measured service allowance")
	}
	if !state.stalled(now.Add(2*time.Minute), now, d) {
		t.Fatal("unbounded silent read")
	}
	d.Readers = nil
	d.NextReadAt = now.Add(time.Hour)
	d.NextWakeAt = d.NextReadAt
	if state.stalled(now.Add(time.Hour), now, d) {
		t.Fatal("Retry-After treated as a stall")
	}
	if !state.stalled(now.Add(time.Hour+2*time.Minute), now, d) {
		t.Fatal("missed scheduled wake not detected")
	}
}

func TestObserverSupervisorPendingDelivery(t *testing.T) {
	root := t.TempDir()
	clock := &waitClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	dir, entry, lock := sharedWatchFixture(t, root, "http://unused.invalid", clock)
	var reg sharedProfileRegistration
	if err := readProfileJSON(entry, &reg); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(root, "delivered")
	hook := filepath.Join(root, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > "+shellArg(events)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	reg.Observer.Notify = hook
	if err := saveProfileJSON(entry, reg); err != nil {
		t.Fatal(err)
	}
	var r profileReceipt
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil {
		t.Fatal(err)
	}
	// Simulate death after delivery intent was synced but before its local ack.
	// This exact payload, including the original delivery ID, must be replayed.
	pending := profileNotification{JobID: r.JobID, Key: r.Key, Status: "succeeded", DeliveryID: "original-delivery"}
	r.Observation = "not_armed"
	r.Status = "succeeded"
	r.DeliveryStatus = "pending"
	r.PendingNotification = &pending
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		t.Fatal(err)
	}
	if err := runSharedProfileLoop(context.Background(), root, lock, clock, func(context.Context, time.Duration) error {
		if _, err := os.Stat(events); err != nil {
			t.Fatal("pending delivery was not resumed", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var got profileNotification
	if err := readProfileJSON(events, &got); err != nil || got.DeliveryID != pending.DeliveryID {
		t.Fatal("pending delivery changed identity", err)
	}
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil || r.PendingNotification != nil || r.DeliveryStatus != "delivered" {
		t.Fatal("delivery acknowledgement not retained", err)
	}
	if _, err := os.Stat(entry); !os.IsNotExist(err) {
		t.Fatal("settled registration retained", err)
	}
}
