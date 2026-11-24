package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Synthetic history span retained for old replay fixtures, not an alert limit.
const healthAlertCooldown = 15 * time.Minute

func alertsOf(q *Queue, kind string) []HealthAlert {
	var events []HealthAlert
	for _, e := range q.s.Health.Events {
		if e.Kind == kind {
			events = append(events, e)
		}
	}
	return events
}

// firings counts alert episodes: each kind-and-subject record carries its count.
func firings(q *Queue, kind string) int {
	n := 0
	for _, e := range alertsOf(q, kind) {
		n += e.Count
	}
	return n
}
func TestHealthAlertHostStaleAndUnhealthy(t *testing.T) {
	q, c := fixture(t)
	m := machine("host", "cuda")
	check(t, q, m, nil)
	if firings(q, "host_unhealthy_or_stale") != 0 {
		t.Fatal("healthy host alerted")
	}
	c.Advance(q.machineLossTimeout(m.ID))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if firings(q, "host_unhealthy_or_stale") != 0 {
		t.Fatal("stale host alerted before its evidence window")
	}
	c.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if firings(q, "host_unhealthy_or_stale") != 1 {
		t.Fatal("entire quiet fleet must alert even when loss decisions are paused")
	}
	check(t, q, m, nil)
	c.Advance(healthAlertCooldown)
	m.InventoryReason = "private worker error"
	check(t, q, m, nil)
	c.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
	check(t, q, m, nil)
	if firings(q, "host_unhealthy_or_stale") != 2 {
		t.Fatal("unhealthy report did not alert")
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
	c.Advance(hostDegradedSustain(q.s.Machines[m.ID]))
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
	data, err = readQueueState(q.db, normalizedQueueMarker)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Health.Events) != 1 || !persisted.Health.Events[0].Delivered {
		t.Fatal("delivery acknowledgement not durable")
	}

	// Finish notification before the independent retained-attempt incident.
	cancel()
	<-done
	{
		s := spec("production")
		s.AllowedMachines = []string{"retained"}
		submit(t, q, s)
		m := machine("retained", "cuda")
		assigned := check(t, q, m, nil).Job
		if assigned == nil {
			t.Fatal("no attempt")
		}
		cause := &RuntimeCause{Operation: "kill", Class: "timeout"}
		r := result(assigned, false)
		r.Interrupted, r.ReleasePending = true, true
		r.CleanupCause = cause
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if firings(q, "production_attempt_lost") != 1 {
			t.Fatal("missing initial alert")
		}
		// A burst preserves exact incident custody and must not duplicate an old
		// attempt alert. Delivered history has no count-based eviction.
		if err := q.update(func() error {
			for i := 0; i < 257; i++ {
				q.appendHealthAlert(HealthAlert{Kind: "disk_below_guard"})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if firings(q, "production_attempt_lost") != 1 {
			t.Fatal("burst evicted unique incident history")
		}
		c.Advance(time.Second)
		// A replayed report of the same attempt may be refused as stale; either
		// way it must not alert again.
		if err := q.Complete(r); err != nil && !errors.Is(err, ErrStale) {
			t.Fatal(err)
		}
		if firings(q, "production_attempt_lost") != 1 || q.s.Jobs[assigned.ID].LastHealthAlertAttempt != assigned.Attempt {
			t.Fatal("same retained attempt alerted after event pruning")
		}
		data, err := json.Marshal(q.s.Jobs[assigned.ID])
		if err != nil {
			t.Fatal(err)
		}
		var stored Job
		if err := json.Unmarshal(data, &stored); err != nil || stored.LastHealthAlertAttempt != assigned.Attempt {
			t.Fatal("attempt alert fact not serialized", err)
		}
	}
}
