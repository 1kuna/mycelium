package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func intervalPage(t *testing.T, q *Queue, after, highWater, limit int) ModelIntervalPage {
	t.Helper()
	page, err := q.ModelIntervals(context.Background(), after, highWater, limit)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func TestModelIntervalsTransientLoadUnloadDurablePagination(t *testing.T) {
	q, clock, m := modelFixture(t)
	// Use real per-card placement rather than a legacy scalar device.
	m.Devices = m3Machine().Devices
	machineCheck(t, q, m)
	before := intervalPage(t, q, 0, -1, 1)
	if before.HighWater != 0 {
		t.Fatal(before)
	}
	assignedAt := clock.Now()
	c := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test", "0")]
	if s == nil {
		t.Fatal("missing model slot", q.s.ModelSlots)
	}
	attempt := s.Attempt
	clock.Advance(time.Second)
	startAt := clock.Now()
	launch := HostLaunch{Machine: m.ID, Session: m.Session, Device: s.Device, Key: s.Model, Attempt: s.Attempt, Kind: "engine"}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	} // Replay must preserve first authorization.
	_, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Device: s.Device, Attempt: attempt, State: "ready"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(q.models.Idle)
	stopAt := clock.Now()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	releaseAt := clock.Now()
	_, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Device: s.Device, Attempt: "stale-attempt", State: "unloaded"}}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Interval.ReleasedAt != nil {
		t.Fatal("stale report released interval")
	}
	_, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Device: s.Device, Attempt: attempt, State: "unloaded"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(q.s.ModelSlots) != 0 {
		t.Fatal("unloaded slot retained")
	}
	// No reads during the entire interval. Events survive removal and restart.
	first := intervalPage(t, q, 0, -1, 1)
	if !first.HasMore {
		t.Fatal("missing transition history", first)
	}
	highWater := first.HighWater
	// A new arrival must not enter the frozen page snapshot.
	addModelCall(t, q, "production")
	var events []ModelIntervalEvent
	events = append(events, first.Entries...)
	for first.HasMore {
		first = intervalPage(t, q, first.Next, highWater, 1)
		events = append(events, first.Entries...)
	}
	if len(events) != highWater || events[0].Kind != "assigned" || events[1].Kind != "load_started" || events[len(events)-2].Kind != "stopped" {
		t.Fatalf("missing assignment/load/stop/release: %+v", events)
	}
	last := events[len(events)-1]
	if last.Kind != "released" || last.Attempt != attempt || last.Device != "0" || last.Identity != "fake-0" || last.Tier.Tier != "experiment" || last.Class != "experiment" {
		t.Fatal(last)
	}
	for _, pair := range []struct {
		got  *time.Time
		want time.Time
	}{{last.AssignedAt, assignedAt}, {last.LoadStartedAt, startAt}, {last.StoppedAt, stopAt}, {last.ReleasedAt, releaseAt}} {
		if pair.got == nil || !pair.got.Equal(pair.want) {
			t.Fatal("wrong interval", last, pair.want)
		}
	}
	var sequence int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	page := intervalPage(t, restarted, 0, highWater, 256)
	if len(page.Entries) != highWater || !page.Entries[len(page.Entries)-1].ReleasedAt.Equal(releaseAt) || !page.CoverageSince.Equal(before.CoverageSince) {
		t.Fatal("lost durable intervals", page)
	}
	server := httptest.NewServer(API{Queue: restarted, Token: "interval-fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "interval-fixture"}
	var apiPage ModelIntervalPage
	if err := client.Call(t.Context(), "GET", "/api/models/intervals?limit=1", nil, &apiPage); err != nil || len(apiPage.Entries) != 1 || !apiPage.HasMore {
		t.Fatal(apiPage, err)
	}
	if err := client.Call(t.Context(), "GET", "/api/models/intervals?limit=257", nil, nil); err == nil {
		t.Fatal("unbounded page accepted")
	}
}

func TestModelIntervalsFailedLoadRetryAndClassChange(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	attempt := s.Attempt
	clock.Advance(time.Second)
	reportModel(t, q, m, "failed")
	failed := intervalPage(t, q, 0, -1, 256)
	last := failed.Entries[len(failed.Entries)-1]
	if last.Kind != "released" || last.LoadStartedAt != nil || last.ReleasedAt == nil || last.Attempt != attempt {
		t.Fatal(last)
	}
	q.finishCall(c.ID)
	clock.Advance(time.Second)
	addModelCall(t, q, "experiment")
	fresh := q.s.ModelSlots[slotKey(m.ID, "test")]
	if fresh.Attempt == attempt || fresh.Interval.ReleasedAt != nil {
		t.Fatal("retry reused interval", fresh)
	}
	clock.Advance(time.Second)
	addModelCall(t, q, "production")
	changes := intervalPage(t, q, failed.HighWater, -1, 256)
	last = changes.Entries[len(changes.Entries)-1]
	if last.Kind != "class_changed" || last.Tier.Tier != "production" || last.Attempt != fresh.Attempt || !last.At.Equal(clock.Now()) {
		t.Fatal("lost tier transition", changes)
	}
}

func TestModelIntervalsRollbackAndUnknownLegacyTimes(t *testing.T) {
	q, _, m := modelFixture(t)
	if err := q.update(func() error {
		q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Attempt: "legacy", Machine: m.ID, Session: m.Session, State: "ready", Priority: "experiment"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	page := intervalPage(t, q, 0, -1, 256)
	if len(page.Entries) != 1 || page.Entries[0].Kind != "observed" || page.Entries[0].AssignedAt != nil || page.Entries[0].LoadStartedAt != nil {
		t.Fatal("fabricated legacy times", page)
	}
	sentinel := errors.New("abort transition")
	err := q.update(func() error {
		q.recordModelInterval(q.s.ModelSlots[slotKey(m.ID, "test")], "released")
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	after := intervalPage(t, q, 0, -1, 256)
	if after.HighWater != page.HighWater || q.s.ModelSlots[slotKey(m.ID, "test")].Interval.ReleasedAt != nil {
		t.Fatal("aborted transition leaked", after)
	}
}
