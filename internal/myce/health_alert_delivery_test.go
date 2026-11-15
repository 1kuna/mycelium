package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHealthAlertDeliveryStatusWithoutNotifier(t *testing.T) {
	q, clock := fixture(t)
	if err := q.update(func() error {
		q.appendHealthAlert(HealthAlert{Kind: "disk_below_guard", Host: "h"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(8 * 24 * time.Hour)
	q.runHealthNotifier(context.Background())
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Delivery *struct {
			Configured bool `json:"configured"`
			Pending    int  `json:"pending"`
		} `json:"health_alert_delivery"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Delivery == nil || reply.Delivery.Configured || reply.Delivery.Pending != 0 {
		t.Fatalf("status does not explain unconfigured delivery: %+v", reply.Delivery)
	}
	if len(snapshot.HealthAlerts) != 1 || snapshot.HealthAlerts[0].Delivered {
		t.Fatal("unconfigured delivery must preserve unacknowledged history")
	}
	var page bytes.Buffer
	if err := statusPage.Execute(&page, snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(page.String(), "health notifier is not configured") {
		t.Fatal("status page does not explain undelivered diagnostic history")
	}
}

func TestHealthAlertDeliveryConfigurationClearsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if q != nil {
			q.Close()
		}
	}()
	hook := filepath.Join(dir, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHealthNotifier(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		for i := 0; i < 257; i++ {
			q.appendHealthAlert(HealthAlert{Kind: "disk_below_guard", Host: "h"})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Health.Events) != 1 || q.s.Health.Events[0].Count != 257 {
		t.Fatal("configured pending record lost firings", q.s.Health.Events)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHealthNotifier(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if q.s.Health.DeliveryConfigured {
		t.Fatal("historical hook configuration survived startup without a notifier")
	}
	if len(q.s.Health.Events) != 1 || q.s.Health.Events[0].Count != 257 {
		t.Fatal("clearing notifier configuration discarded alert history")
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HealthAlertDelivery == nil || snapshot.HealthAlertDelivery.Configured || snapshot.HealthAlertDelivery.Pending != 0 {
		t.Fatalf("restarted status: %+v", snapshot.HealthAlertDelivery)
	}
}

func TestHealthAlertDeliveryFailedHookRemainsUnacknowledged(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	hook, called := filepath.Join(dir, "notify"), filepath.Join(dir, "called")
	script := "#!/bin/sh\n[ \"$1\" = --check ] && exit 0\ncat >/dev/null\n: > '" + called + "'\nexit 1\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHealthNotifier(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.observeHealth("test/h", true, 0, HealthAlert{Kind: "disk_below_guard", Host: "h"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.runHealthNotifier(ctx) }()
	defer func() { cancel(); <-done }()
	await(t, func() bool { _, err := os.Stat(called); return err == nil })
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HealthAlertDelivery == nil || !snapshot.HealthAlertDelivery.Configured || snapshot.HealthAlertDelivery.Pending != 1 || snapshot.HealthAlerts[0].Delivered {
		t.Fatalf("failed-hook status: %+v", snapshot.HealthAlertDelivery)
	}
}

// Activation skips closed history and delivers only a still-open problem.
func TestHealthAlertDeliverySkipsClosedBacklog(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	hook, out := filepath.Join(dir, "notify"), filepath.Join(dir, "payloads")
	script := "#!/bin/sh\n[ \"$1\" = --check ] && exit 0\ncat >> '" + out + "'\necho >> '" + out + "'\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		for range 3 {
			q.appendHealthAlert(HealthAlert{Kind: "host_degraded", Reason: "host_probe_unknown", Host: "a"})
			clock.Advance(time.Minute)
		}
		q.observeHealth("test/b", true, 0, HealthAlert{Kind: "host_degraded", Reason: "host_probe_unknown", Host: "b"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHealthNotifier(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.runHealthNotifier(ctx) }()
	defer func() { cancel(); <-done }()
	await(t, func() bool {
		q.mu.RLock()
		defer q.mu.RUnlock()
		return len(q.pendingHealthDeliveries()) == 0
	})
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var got []HealthAlert
	for _, line := range strings.Fields(string(data)) {
		var e HealthAlert
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if len(got) != 1 || got[0].Host != "b" || got[0].State != "open" || strings.HasSuffix(got[0].DeliveryID, "/closed") {
		t.Fatalf("record deliveries: %s", data)
	}
	if len(q.s.Health.Events) != 2 {
		t.Fatal("records", len(q.s.Health.Events))
	}
}

// Exercise the actual executable hook: one opening, one closing, and no
// delivery of efficiency/recipe findings sharing the same host.
func TestHealthAlertDeliveryHostOpeningAndClosing(t *testing.T) {
	q, clock := fixture(t)
	dir := t.TempDir()
	hook, out := filepath.Join(dir, "notify"), filepath.Join(dir, "payloads")
	script := "#!/bin/sh\n[ \"$1\" = --check ] && exit 0\ncat >> '" + out + "'\necho >> '" + out + "'\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHealthNotifier(context.Background(), hook); err != nil {
		t.Fatal(err)
	}
	host := HealthAlert{Kind: "host_degraded", Host: "h", Reason: "host_memory_pressure"}
	flapHealth(t, q, clock, "test/host", host, true)
	for _, kind := range []string{"efficiency_regression", "environment_recipe_failed"} {
		flapHealth(t, q, clock, "test/"+kind, HealthAlert{Kind: kind, Host: "h"}, true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.runHealthNotifier(ctx) }()
	defer func() { cancel(); <-done }()
	delivered := func() bool {
		q.mu.RLock()
		defer q.mu.RUnlock()
		return len(q.pendingHealthDeliveries()) == 0
	}
	await(t, delivered)
	// Recoveries shorter than the measured sustain rule keep one inbox item.
	window := q.healthRecoverySustain(host)
	for range 20 {
		for _, bad := range []bool{false, true} {
			if err := q.update(func() error { q.observeHealth("test/host", bad, 0, host); return nil }); err != nil {
				t.Fatal(err)
			}
			clock.Advance(window / 4)
		}
	}
	if err := q.update(func() error { q.observeHealth("test/host", false, 0, host); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(window)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		q.mu.RLock()
		defer q.mu.RUnlock()
		return alertsOf(q, host.Kind)[0].ClosedDelivered
	})
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(data))
	if len(lines) != 2 {
		t.Fatalf("wanted opening and closing only: %s", data)
	}
	var opening, closing HealthAlert
	if err := json.Unmarshal([]byte(lines[0]), &opening); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &closing); err != nil {
		t.Fatal(err)
	}
	if opening.Kind != host.Kind || opening.State != "open" || closing.State != "closed" || closing.DeliveryID != opening.DeliveryID+"/closed" || !closing.Delivered {
		t.Fatalf("episode delivery: %s", data)
	}
}

// A snapshot taken before an outage must not replay recovered problems when
// the receiver resumes. Ambiguous deliveries, unlike untouched backlog, retain
// custody across recovery and a queue restart.
func TestHealthAlertDeliveryGapAndAmbiguousReceipt(t *testing.T) {
	q, clock := fixture(t)
	e := HealthAlert{Kind: "disk_below_guard", Host: "h"}
	observe := func(bad bool) {
		t.Helper()
		if err := q.update(func() error { q.observeHealth("test/h", bad, 0, e); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	observe(true)
	stale := q.pendingHealthDeliveries()[0]
	observe(false)
	clock.Advance(q.healthRecoverySustain(e))
	if _, ok, err := q.beginHealthDelivery(t.Context(), stale); err != nil || ok {
		t.Fatalf("replayed recovered snapshot: %v %v", ok, err)
	}
	if len(q.pendingHealthDeliveries()) != 0 {
		t.Fatal("closed backlog pending")
	}
	observe(true)
	opening, ok, err := q.beginHealthDelivery(t.Context(), q.pendingHealthDeliveries()[0])
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Simulate receiver acceptance followed by a failed exit / missing queue ack.
	observe(false)
	clock.Advance(q.healthRecoverySustain(e))
	q = reopenQueue(t, q, clock)
	pending := q.pendingHealthDeliveries()
	if len(pending) != 1 || pending[0].closing || pending[0].alert.State != "open" || pending[0].episode != opening.episode {
		t.Fatalf("opening custody lost: %+v", pending)
	}
	if err := q.ackHealthDelivery(t.Context(), pending[0]); err != nil {
		t.Fatal(err)
	}
	pending = q.pendingHealthDeliveries()
	if len(pending) != 1 || !pending[0].closing || pending[0].alert.DeliveryID != opening.episode+"/closed" {
		t.Fatalf("closing missing: %+v", pending)
	}
	closing, ok, err := q.beginHealthDelivery(t.Context(), pending[0])
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	observe(true) // reopen while the closing's receiver result is ambiguous
	q = reopenQueue(t, q, clock)
	pending = q.pendingHealthDeliveries()
	if len(pending) != 1 || !pending[0].closing || pending[0].alert.ClosedAt == nil || pending[0].alert.DeliveryID != closing.alert.DeliveryID {
		t.Fatalf("closing custody lost: %+v", pending)
	}
	if err := q.ackHealthDelivery(t.Context(), closing); err != nil {
		t.Fatal(err)
	}
	pending = q.pendingHealthDeliveries()
	if len(pending) != 1 || pending[0].closing || pending[0].episode == opening.episode {
		t.Fatalf("reopened episode missing: %+v", pending)
	}
	if err := q.ackHealthDelivery(t.Context(), pending[0]); err != nil {
		t.Fatal(err)
	}
	// Late duplicate acknowledgements cannot touch the replacement episode.
	if err := q.ackHealthDelivery(t.Context(), opening); err != nil {
		t.Fatal(err)
	}
	if err := q.ackHealthDelivery(t.Context(), closing); err != nil {
		t.Fatal(err)
	}
	if len(q.pendingHealthDeliveries()) != 0 {
		t.Fatal("duplicate delivery after acknowledgement")
	}
}
