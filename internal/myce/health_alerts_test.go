package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func alertsOf(q *Queue, kind string) []HealthAlert {
	var events []HealthAlert
	for _, e := range q.s.Health.Events {
		if e.Kind == kind {
			events = append(events, e)
		}
	}
	return events
}
func TestHealthAlertDiskGuard(t *testing.T) {
	q, c := fixture(t)
	m := machine("disk-host", "cuda")
	m.DiskVolumes = []DiskVolume{{ID: "volume", Roots: []string{"data"}, TotalBytes: 100 << 30, FreeBytes: 2 << 30, SampledAt: c.Now()}}
	check(t, q, m, nil)
	if len(alertsOf(q, "disk_below_guard")) != 0 {
		t.Fatal("healthy disk alerted")
	}
	c.Advance(time.Second)
	m.DiskVolumes[0].SampledAt = c.Now()
	m.DiskVolumes[0].FreeBytes = 100 << 20
	check(t, q, m, nil)
	check(t, q, m, nil)
	if len(alertsOf(q, "disk_below_guard")) != 1 {
		t.Fatal("disk guard did not deduplicate")
	}
	snapshot, err := q.publicSnapshot()
	if err != nil || len(snapshot.HealthAlerts) != 1 {
		t.Fatalf("public event missing: %+v %v", snapshot.HealthAlerts, err)
	}
	c.Advance(healthAlertCooldown)
	m.DiskVolumes[0].SampledAt = c.Now()
	check(t, q, m, nil)
	if len(alertsOf(q, "disk_below_guard")) != 1 {
		t.Fatal("continuous incident repeated")
	}
	m.DiskVolumes[0].FreeBytes = 2 << 30
	check(t, q, m, nil)
	m.DiskVolumes[0].FreeBytes = 100 << 20
	check(t, q, m, nil)
	if len(alertsOf(q, "disk_below_guard")) != 2 {
		t.Fatal("recovered disk did not rearm")
	}
}
func TestHealthAlertHostStaleAndUnhealthy(t *testing.T) {
	q, c := fixture(t)
	m := machine("host", "cuda")
	check(t, q, m, nil)
	if len(alertsOf(q, "host_unhealthy_or_stale")) != 0 {
		t.Fatal("healthy host alerted")
	}
	c.Advance(q.machineLossTimeout(m.ID))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "host_unhealthy_or_stale")) != 1 {
		t.Fatal("entire quiet fleet must alert even when loss decisions are paused")
	}
	check(t, q, m, nil)
	c.Advance(healthAlertCooldown)
	m.InventoryReason = "private worker error"
	check(t, q, m, nil)
	if len(alertsOf(q, "host_unhealthy_or_stale")) != 2 {
		t.Fatal("unhealthy report did not alert")
	}
}
func TestHealthAlertStorageDelay(t *testing.T) {
	for _, scope := range []string{"queue", "worker", "file-store"} {
		t.Run(scope, func(t *testing.T) {
			q, c := fixture(t)
			m := machine("host", "cuda")
			check(t, q, m, nil)
			if len(alertsOf(q, "checkpoint_store_unavailable")) != 0 {
				t.Fatal("healthy storage alerted")
			}
			if scope == "queue" {
				fakeCheckpointAvailability(t, q, false)
			} else if scope == "worker" {
				m.CheckpointUnavailable = true
			} else {
				// Simulate the queue file-store mount disappearing; no bulk I/O is involved.
				q.files.root = filepath.Join(t.TempDir(), "missing")
			}
			check(t, q, m, nil)
			c.Advance(DefaultHealthStorageDelay - time.Second)
			check(t, q, m, nil)
			if len(alertsOf(q, "checkpoint_store_unavailable")) != 0 {
				t.Fatal("short outage alerted")
			}
			c.Advance(time.Second)
			check(t, q, m, nil)
			check(t, q, m, nil)
			if len(alertsOf(q, "checkpoint_store_unavailable")) != 1 {
				t.Fatal("sustained outage missing or duplicated")
			}
		})
	}
}
func TestHealthAlertProductionAttemptLoss(t *testing.T) {
	for _, operation := range []string{"quiet", "deadline", "start_authorization", "inspect", "kill"} {
		t.Run(operation, func(t *testing.T) {
			q, c := fixture(t)
			j := submit(t, q, spec("production"))
			m := machine("worker", "cuda")
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("no attempt")
			}
			c.Advance(time.Second)
			if operation == "quiet" {
				// A second reporting worker permits the existing machine-loss decision.
				other := machine("other", "cuda")
				check(t, q, other, nil)
				c.Advance(q.machineLossTimeout(m.ID))
				check(t, q, other, nil)
			} else if operation == "deadline" {
				c.Advance(ProductionStartTimeout)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
			} else {
				r := result(assigned, false)
				r.Interrupted = true
				cause := &RuntimeCause{Operation: operation, Class: "timeout"}
				if operation == "kill" {
					r.CleanupCause = cause
				} else {
					r.ExecutionCause = cause
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			events := alertsOf(q, "production_attempt_lost")
			if len(events) != 1 || events[0].JobID != j.ID || events[0].Attempt != assigned.Attempt {
				t.Fatalf("lost attempt: %+v", events)
			}
		})
	}
}
func TestHealthAlertNotifierPersistenceAndPrivacy(t *testing.T) {
	q, c := fixture(t)
	root := t.TempDir()
	hook := filepath.Join(root, "notify")
	out := filepath.Join(root, "event")
	script := "#!/bin/sh\n[ \"$1\" = --check ] && exit 0\n[ -z \"$MYCE_TOKEN\" ] || exit 2\ncat > '" + out + "'\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "private-token")
	if err := q.ConfigureHealthNotifier(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	m := machine("host", "cuda")
	m.InventoryReason = "private token and path"
	c.Advance(time.Second)
	check(t, q, m, nil)
	events := alertsOf(q, "host_unhealthy_or_stale")
	if len(events) != 1 {
		t.Fatal("missing event")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.runHealthNotifier(ctx) }()
	defer func() { cancel(); <-done }()
	await(t, func() bool { q.mu.RLock(); defer q.mu.RUnlock(); return q.s.Health.Events[0].Delivered })
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var got HealthAlert
	if err = json.Unmarshal(data, &got); err != nil || got.DeliveryID != events[0].DeliveryID {
		t.Fatalf("hook payload %s: %v", data, err)
	}
	// Reload the actual persisted record rather than trusting the in-memory flag.
	var persisted state
	if err := json.Unmarshal(q.persisted["state"], &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Health.Events) != 1 || !persisted.Health.Events[0].Delivered {
		t.Fatal("delivery acknowledgement not durable")
	}
}

func TestHealthAlertHealthyAndPlannedProduction(t *testing.T) {
	for _, planned := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "operator stop"}[planned], func(t *testing.T) {
			q, c := fixture(t)
			submit(t, q, spec("production"))
			m := machine("worker", "cuda")
			assigned := check(t, q, m, nil).Job
			c.Advance(time.Second)
			r := result(assigned, !planned)
			if planned {
				r.Interrupted = true
				r.OperatorStop = true
				r.ExecutionCause = &RuntimeCause{Operation: "inspect", Class: "timeout"}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if len(alertsOf(q, "production_attempt_lost")) != 0 {
				t.Fatal("healthy/planned production alerted")
			}
		})
	}
}
