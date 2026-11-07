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
		q.appendHealthAlert(HealthAlert{Kind: "queue_overload"})
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
	if reply.Delivery == nil || reply.Delivery.Configured || reply.Delivery.Pending != 1 {
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
			q.appendHealthAlert(HealthAlert{Kind: "queue_overload"})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Health.Events) != 257 {
		t.Fatal("configured pending events were pruned")
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
	if len(q.s.Health.Events) != 257 {
		t.Fatal("clearing notifier configuration discarded unique alert history")
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HealthAlertDelivery == nil || snapshot.HealthAlertDelivery.Configured || snapshot.HealthAlertDelivery.Pending != 257 {
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
		q.appendHealthAlert(HealthAlert{Kind: "queue_overload"})
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
