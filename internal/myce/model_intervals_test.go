package myce

import (
	"context"

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

func TestModelIntervalsTrackEffectivePinnedTier(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	before := intervalPage(t, q, 0, -1, 256)
	clock.Advance(time.Second)
	pinAt := clock.Now()
	if err := q.Pin(ModelPin{Project: "pin-owner", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	promoted := intervalPage(t, q, before.HighWater, -1, 256)
	if len(promoted.Entries) != 1 {
		t.Fatal("missing pinned promotion", promoted)
	}
	event := promoted.Entries[0]
	if event.Kind != "class_changed" || event.Project != "pin-owner" || event.Class != "production" || event.Tier.Tier != "production" || !event.At.Equal(pinAt) || event.Attempt != s.Attempt {
		t.Fatal("wrong pinned identity", event)
	}
	clock.Advance(time.Second)
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	pinned := intervalPage(t, q, promoted.HighWater, -1, 256)
	if len(pinned.Entries) != 0 {
		t.Fatal("completed experiment demoted active pin", pinned)
	}
	clock.Advance(time.Second)
	if err := q.Unpin("pin-owner", "test"); err != nil {
		t.Fatal(err)
	}
	unpinned := intervalPage(t, q, pinned.HighWater, -1, 256)
	if len(unpinned.Entries) != 1 || unpinned.Entries[0].Tier.Tier != "experiment" || !unpinned.Entries[0].At.Equal(clock.Now()) {
		t.Fatal("missing unpin transition", unpinned)
	}
}

func TestModelIntervalsPinAttributionIsCardSpecific(t *testing.T) {
	q, _, m := modelFixture(t)
	m.Devices = m3Machine().Devices
	machineCheck(t, q, m)
	addModelCall(t, q, "experiment")
	sibling := q.s.ModelSlots[slotKey(m.ID, "test", "0")]
	if sibling == nil {
		t.Fatal("missing experiment slot")
	}
	before := intervalPage(t, q, 0, -1, 256)
	if err := q.Pin(ModelPin{Project: "pin-owner", Model: "test", Machine: m.ID, Device: "1"}); err != nil {
		t.Fatal(err)
	}
	pinned := q.s.ModelSlots[slotKey(m.ID, "test", "1")]
	if pinned == nil {
		t.Fatal("missing card 1 pin")
	}

	after := intervalPage(t, q, before.HighWater, -1, 256)
	for _, event := range after.Entries {
		if event.Attempt == sibling.Attempt {
			t.Fatal("pin changed sibling interval", event)
		}
		if event.Attempt == pinned.Attempt && (event.Device != "1" || event.Project != "pin-owner" || event.Tier.Tier != "production") {
			t.Fatal("incorrect pinned card attribution", event)
		}
	}
}

func TestModelIntervalsCleanReplacementReleaseFence(t *testing.T) {
	for _, verified := range []bool{true, false} {
		name := "verified"
		if !verified {
			name = "unverified"
		}
		t.Run(name, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			call := addModelCall(t, q, "experiment")
			reportModel(t, q, m, "ready")
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			if err := q.finishCall(call.ID); err != nil {
				t.Fatal(err)
			}
			before := intervalPage(t, q, 0, -1, 256)
			clock.Advance(time.Second)
			replacedAt := clock.Now()
			oldSession := m.Session
			m.Session = "replacement-session"
			previous := oldSession
			if !verified {
				previous = "unverified-session"
			}
			_, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous})
			after := intervalPage(t, q, before.HighWater, -1, 256)
			if !verified {
				if err == nil {
					t.Fatal("replacement accepted without previous-session cleanup")
				}
				if after.HighWater != before.HighWater || s.Interval.ReleasedAt != nil {
					t.Fatal("unverified cleanup released history", after)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(after.Entries) != 1 {
				t.Fatal("clean replacement missing release event", after)
			}
			event := after.Entries[0]
			if event.Kind != "released" || event.Attempt != s.Attempt || event.Session != oldSession || event.ReleasedAt == nil || !event.ReleasedAt.Equal(replacedAt) {
				t.Fatal("incorrect replacement release", event)
			}
		})
	}
}
