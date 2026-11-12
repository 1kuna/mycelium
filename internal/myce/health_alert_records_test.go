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

// Repeated firings of one kind and subject update one saved row.
func TestHealthAlertRepeatedFiringsUpdateOneRecord(t *testing.T) {
	q, clock := fixture(t)
	e := HealthAlert{Kind: "host_degraded", Host: "h", Reason: "host_probe_unknown"}
	first := clock.Now()
	for range 3 {
		flapHealth(t, q, clock, "test/h", e, true)
		flapHealth(t, q, clock, "test/h", e, false)
	}
	flapHealth(t, q, clock, "test/h", e, true)
	other := HealthAlert{Kind: "host_degraded", Host: "g", Reason: "host_probe_unknown"}
	flapHealth(t, q, clock, "test/g", other, true)
	events := q.s.Health.Events
	if len(events) != 2 {
		t.Fatal("records", len(events))
	}
	r := events[0]
	if r.Count != 4 || r.State != "open" || !r.FirstAt.Equal(first) || r.LastAt == nil || !r.LastAt.After(first) || r.ClosedAt != nil {
		t.Fatalf("record %+v", r)
	}
	var rows int
	if err := q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key LIKE 'health_events/%'").Scan(&rows); err != nil || rows != 2 {
		t.Fatal("saved rows", rows, err)
	}
	q = reopenQueue(t, q, clock)
	if len(q.s.Health.Events) != 2 || q.s.Health.Events[0].Count != 4 {
		t.Fatal("record lost on restart", q.s.Health.Events)
	}
}

// Closing and reopening: deliveries are openings and closings; a reopen whose
// closing was never delivered continues the receiver's episode; closed records
// expire after HealthAlertRetention.
func TestHealthAlertCloseReopenAndExpire(t *testing.T) {
	q, clock := fixture(t)
	q.s.Health.DeliveryConfigured = true
	e := HealthAlert{Kind: "disk_below_guard", Host: "h"}
	ack := func(want string, closing bool) {
		t.Helper()
		pending := pendingHealthDeliveries(q.s.Health.Events)
		if len(pending) != 1 || pending[0].closing != closing || pending[0].alert.DeliveryID != want {
			t.Fatalf("pending %+v, want %s closing=%v", pending, want, closing)
		}
		if err := q.ackHealthDelivery(t.Context(), pending[0]); err != nil {
			t.Fatal(err)
		}
		if len(pendingHealthDeliveries(q.s.Health.Events)) != 0 {
			t.Fatal("acknowledged delivery still pending")
		}
	}
	flapHealth(t, q, clock, "test/o", e, true)
	opening := q.s.Health.Events[0].DeliveryID
	ack(opening, false)
	flapHealth(t, q, clock, "test/o", e, false)
	if r := q.s.Health.Events[0]; r.State != "closed" || r.ClosedAt == nil {
		t.Fatalf("not closed %+v", r)
	}
	// Reopened before the closing was delivered: the receiver still sees it open.
	flapHealth(t, q, clock, "test/o", e, true)
	if r := q.s.Health.Events[0]; r.State != "open" || r.DeliveryID != opening || !r.Delivered || len(pendingHealthDeliveries(q.s.Health.Events)) != 0 {
		t.Fatalf("silent reopen %+v", r)
	}
	flapHealth(t, q, clock, "test/o", e, false)
	ack(opening+"/closed", true)
	// A real new episode after a delivered closing notifies again.
	flapHealth(t, q, clock, "test/o", e, true)
	reopened := q.s.Health.Events[0].DeliveryID
	if reopened == opening || q.s.Health.Events[0].Count != 3 {
		t.Fatalf("reopen %+v", q.s.Health.Events[0])
	}
	ack(reopened, false)
	flapHealth(t, q, clock, "test/o", e, false)
	ack(reopened+"/closed", true)
	// Retention starts at closing.
	clock.Advance(HealthAlertRetention - time.Minute)
	flapHealth(t, q, clock, "test/other", HealthAlert{Kind: "queue_overload", ClientKind: "x"}, false)
	if len(q.s.Health.Events) != 1 {
		t.Fatal("expired early")
	}
	clock.Advance(time.Minute)
	flapHealth(t, q, clock, "test/other", HealthAlert{}, false)
	if len(q.s.Health.Events) != 0 {
		t.Fatal("closed record kept past retention", q.s.Health.Events)
	}
	var rows int
	if err := q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key LIKE 'health_events/%'").Scan(&rows); err != nil || rows != 0 {
		t.Fatal("expired row kept", rows, err)
	}
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

// Status lists one line per problem with its count, and groups open ones.
func TestHealthAlertStatusShowsGroupedOpenRecords(t *testing.T) {
	q, clock := fixture(t)
	for i := range 4 {
		bad := i%2 == 0
		flapHealth(t, q, clock, "test/h1", HealthAlert{Kind: "host_degraded", Host: "h1"}, bad)
		flapHealth(t, q, clock, "test/h2", HealthAlert{Kind: "host_degraded", Host: "h2"}, true)
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.HealthAlerts) != 2 {
		t.Fatal("alert lines", len(snapshot.HealthAlerts))
	}
	open := 0
	for _, r := range snapshot.HealthAlerts {
		if r.State == "open" {
			open++
		}
	}
	if open != 1 || snapshot.HealthAlerts[0].Count != 2 || snapshot.HealthAlerts[1].Count != 1 {
		t.Fatalf("records %+v", snapshot.HealthAlerts)
	}
	groups := statusAlertGroups(q.s.Health.Conditions)
	if len(groups) != 1 || groups[0].Count != 1 {
		t.Fatalf("groups %+v", groups)
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
	pending := pendingHealthDeliveries(q.s.Health.Events)
	if len(pending) != 1 || pending[0].alert.Kind != host.Kind || q.healthAlertDelivery().Pending != 1 {
		t.Fatalf("notifier receives %+v", pending)
	}
	clock.Advance(HealthAlertRetention)
	flapHealth(t, q, clock, "test/none", HealthAlert{}, false)
	if len(q.s.Health.Events) != 1 || q.s.Health.Events[0].Kind != host.Kind {
		t.Fatalf("status-only records kept for a notifier that never receives them: %+v", q.s.Health.Events)
	}
}
