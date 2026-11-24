package myce

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func flapHealth(t *testing.T, q *Queue, clock *fakeClock, key string, e HealthAlert, bad bool) {
	t.Helper()
	if err := q.update(func() error { q.observeHealth(key, bad, 0, e); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(30 * time.Second)
}

// Previous-release per-firing rows fold into records on the first transaction;
// the remaining rows stay dense HealthAlert JSON that the previous release reads.
func TestHealthAlertMigratesPerFiringRows(t *testing.T) {
	q, clock := fixture(t)
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	start := clock.Now()
	legacy := []HealthAlert{}
	for i := range 40 {
		host := "a"
		if i%4 == 0 {
			host = "b"
		}
		legacy = append(legacy, HealthAlert{Kind: "host_degraded", Host: host, Reason: "host_probe_unknown", DeliveryID: "d" + strconv.Itoa(i), At: start.Add(time.Duration(i) * time.Minute), Delivered: i < 30})
	}
	for i, e := range legacy {
		data, _ := json.Marshal(e)
		if _, err := q.db.Exec("INSERT INTO queue_state(key,data) VALUES(?,?)", "health_events/"+strconv.Itoa(i), data); err != nil {
			t.Fatal(err)
		}
	}
	// The previous release left this condition active without a record link.
	q.s.Health.Conditions = map[string]healthCondition{"test/a": {Active: true, Since: start}}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	clock.Advance(time.Hour)
	if err := q.update(func() error {
		q.observeHealth("test/a", true, 0, HealthAlert{Kind: "host_degraded", Host: "a", Reason: "host_probe_unknown"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	events := q.s.Health.Events
	if len(events) != 2 {
		t.Fatal("records", events)
	}
	a, b := events[0], events[1]
	if a.Host != "a" {
		a, b = b, a
	}
	if a.Host != "a" || a.Count != 31 || a.State != "open" || !a.FirstAt.Equal(start.Add(time.Minute)) || !a.LastAt.Equal(clock.Now()) || b.Host != "b" || b.Count != 10 || b.State != "closed" || !b.ClosedAt.Equal(start.Add(36*time.Minute)) || b.DeliveryID != "d36" || b.Delivered {
		t.Fatalf("migrated %+v %+v", a, b)
	}
	rows, err := q.db.Query("SELECT key, data FROM queue_state WHERE key LIKE 'health_events/%' ORDER BY key")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		var data []byte
		var old struct {
			DeliveryID string    `json:"delivery_id"`
			Kind       string    `json:"kind"`
			At         time.Time `json:"at"`
			Delivered  bool      `json:"delivered"`
		}
		if err := rows.Scan(&key, &data); err != nil || json.Unmarshal(data, &old) != nil || old.DeliveryID == "" || old.Kind != "host_degraded" {
			t.Fatal("row unreadable by the previous release", key, err)
		}
		keys = append(keys, key)
	}
	if strings.Join(keys, ",") != "health_events/0,health_events/1" {
		t.Fatal("rows not dense", keys)
	}
}

// MY-49: the notifier receives host-level problems only. Efficiency, recipe,
// job and queue findings stay in status, are not counted pending, and expire
// on retention even with a notifier configured.
func TestHealthAlertNotifierReceivesHostLevelProblemsOnly(t *testing.T) {
	q, clock := fixture(t)
	q.s.Health.DeliveryConfigured = true
	host := HealthAlert{Kind: "host_degraded", Host: "h", Reason: "host_memory_pressure"}
	for _, e := range []HealthAlert{host, {Kind: "efficiency_regression", Host: "h", Reason: "stranded_demand"}, {Kind: "environment_recipe_failed", Host: "h"}, {Kind: "queue_overload"}} {
		flapHealth(t, q, clock, "test/"+e.Kind, e, true)
		flapHealth(t, q, clock, "test/"+e.Kind, e, false)
	}
	pending := q.pendingHealthDeliveries()
	if len(pending) != 0 || q.healthAlertDelivery().Pending != 0 {
		t.Fatalf("notifier receives %+v", pending)
	}
	clock.Advance(HealthAlertRetention)
	flapHealth(t, q, clock, "test/none", HealthAlert{}, false)
	if len(q.s.Health.Events) != 0 {
		t.Fatalf("status-only records kept for a notifier that never receives them: %+v", q.s.Health.Events)
	}
}
