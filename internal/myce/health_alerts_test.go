package myce

import (
	"context"
	"encoding/json"
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
}

func TestHealthAlertDockerPendingTimeout(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "observation heartbeat", true: "cleanup result"}[pending], func(t *testing.T) {
			q, c := fixture(t)
			s := spec("production")
			s.Run.Command = nil
			s.Run.Docker = true
			s.Run.Command = dockerJob().Spec.Run.Command
			submit(t, q, s)
			m := dockerMachine("docker", "cuda")
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("no attempt")
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			c.Advance(time.Second)
			cause := &RuntimeCause{Operation: "inspect", Class: "timeout"}
			if pending {
				r := result(assigned, false)
				r.ReleasePending = true
				r.CleanupCause = cause
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := 0; i < 2; i++ {
					c.Advance(time.Second)
					m.DockerCleanup = "observation pending"
					m.DockerCause = cause
					report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: assigned.Session, Started: true, DockerCause: cause}
					if _, err := q.Checkin(Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m, Attempts: []AttemptReport{report}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			events := alertsOf(q, "production_attempt_lost")
			if len(events) != 1 || events[0].Attempt != assigned.Attempt {
				t.Fatalf("pending timeout: %+v", events)
			}
			if q.s.Jobs[assigned.ID].ReleaseConfirmed {
				t.Fatal("alert released custody")
			}
		})
	}
}

func TestHealthAlertAttemptDedupOutlivesEventWindow(t *testing.T) {
	q, c := fixture(t)
	s := dockerJob().Spec
	s.Priority = "production"
	submit(t, q, s)
	m := dockerMachine("retained", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("no attempt")
	}
	cause := &RuntimeCause{Operation: "inspect", Class: "timeout"}
	r := result(assigned, false)
	r.ReleasePending = true
	r.CleanupCause = cause
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "production_attempt_lost")) != 1 {
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
	if len(alertsOf(q, "production_attempt_lost")) != 1 {
		t.Fatal("burst evicted unique incident history")
	}
	c.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "production_attempt_lost")) != 1 || q.s.Jobs[assigned.ID].LastHealthAlertAttempt != assigned.Attempt {
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
