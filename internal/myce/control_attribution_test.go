package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Existing request-window capacity bounds live detail, independent of the
// waiting population. Cohort IDs preserve reader joins without N-by-N lists.
func TestControlAttributionBoundedFlightsAndDependencies(t *testing.T) {
	q, clock := fixture(t)
	readers := make([]*controlLock, controlDetailCapacity+20)
	for i := range readers {
		readers[i] = q.mu.named("reader_population", nil)
		readers[i].RLock()
	}
	snap := q.control.snapshot()
	if len(snap.InFlight) != controlDetailCapacity || snap.InFlightTotal != len(readers) || snap.FlightDetailsDropped != 20 {
		t.Fatal("unbounded or hidden live detail", len(snap.InFlight), snap.InFlightTotal, snap.FlightDetailsDropped)
	}
	first, second := q.mu.named("first_writer", nil), q.mu.named("second_writer", nil)
	done := make(chan struct{}, 2)
	go func() { first.Lock(); first.Unlock(); done <- struct{}{} }()
	await(t, func() bool { return q.control.controlWriters.Load() == 1 })
	go func() { second.Lock(); second.Unlock(); done <- struct{}{} }()
	await(t, func() bool { return q.control.controlWriters.Load() == 2 })
	clock.Advance(time.Second)
	for _, r := range readers {
		r.RUnlock()
	}
	<-done
	<-done
	if len(second.event.Blockers) != 1 || second.event.Blockers[0] != first.event.ID {
		t.Fatal("predecessor chain missing", second.event.Blockers)
	}
	for _, r := range readers {
		if !r.event.Contended || r.event.ReaderGroup < first.event.ReaderGroups[0] || r.event.ReaderGroup > first.event.ReaderGroups[1] {
			t.Fatal("reader cohort join lost")
		}
	}
	if got := q.control.snapshot(); got.InFlightTotal != 0 || len(got.InFlight) != 0 {
		t.Fatal("live detail leaked", got)
	}
}

type controlSignalClock struct {
	Clock
	called chan struct{}
}

func (c controlSignalClock) Now() time.Time {
	now := c.Clock.Now()
	select {
	case c.called <- struct{}{}:
	default:
	}
	return now
}
func TestControlAttributionReleaseIncludesRecorderWait(t *testing.T) {
	q, clock := fixture(t)
	l := q.mu.named("recorder_wait_holder", nil)
	l.Lock()
	called := make(chan struct{}, 1)
	q.control.clock = controlSignalClock{clock, called}
	q.control.mu.Lock()
	done := make(chan struct{})
	go func() { l.Unlock(); close(done) }()
	<-called
	clock.Advance(2 * time.Second)
	q.control.mu.Unlock()
	<-done
	if l.event.HoldNS != 2*time.Second || l.event.ReleaseRecorderWait != 2*time.Second || l.event.Finished != clock.Now() {
		t.Fatal("recorder wait omitted", l.event)
	}
}

func TestControlAttributionReadbackContexts(t *testing.T) {
	q, _ := fixture(t)
	ctx := context.WithValue(t.Context(), requestSpanKey{}, &requestSpan{id: 123, handler: 456, client: "anonymous-source"})
	q.EnvironmentEntries(ctx)
	q.ComponentExclusions(ctx)
	_, _ = q.ResolveComponentSelector("unknown", ctx)
	q.publishEfficiencyDiagnostics(&efficiencyDiagnostics{applied: true, report: &EfficiencyReport{}}, ctx)
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range q.control.snapshot().Recent {
		switch e.Operation {
		case "EnvironmentEntries", "ComponentExclusions", "ResolveComponentSelector", "publishEfficiencyDiagnostics":
			if e.Request != 123 || e.Handler != 456 || e.Client != "anonymous-source" {
				t.Fatal("context lost", e)
			}
			seen[e.Operation] = true
		}
	}
	if len(seen) != 4 {
		t.Fatal("readback acquisitions missing", seen)
	}
	handler := API{q, "test-only"}.Handler()
	for _, path := range []string{"/api/environments", "/api/environments/unknown", "/api/exclusions", "/api/exclusions/resolve?selector=unknown"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test-only")
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}
	q.control.mu.Lock()
	buffered := append([]ControlEvent(nil), q.control.pending...)
	q.control.mu.Unlock()
	httpReads := map[string]int{}
	for _, event := range buffered {
		switch event.Operation {
		case "EnvironmentEntries", "ComponentExclusions", "ResolveComponentSelector":
			if event.Request == 0 || event.Request == 123 || event.Handler == 0 || event.Client == "" {
				t.Fatal("HTTP caller dropped context", event)
			}
			httpReads[event.Operation]++
		}
	}
	if httpReads["EnvironmentEntries"] != 2 || httpReads["ComponentExclusions"] != 1 || httpReads["ResolveComponentSelector"] != 1 {
		t.Fatal("HTTP readback acquisitions missing", httpReads)
	}
}

func TestControlAttributionSecretRedaction(t *testing.T) {
	q, _ := fixture(t)
	secret := "attribution-secret-fixture"
	r := httptest.NewRequest("GET", "/api/efficiency?private="+secret, nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	r.Header.Set("User-Agent", "myce/"+secret)
	r.RemoteAddr = secret + ":1234"
	w := httptest.NewRecorder()
	API{q, secret}.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	m := machine(secret, "fake")
	m.Session = secret
	if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(page)
	if err != nil || strings.Contains(string(raw), secret) {
		t.Fatal("secret present in response", err)
	}
	rows, err := q.usageReader.Query("SELECT data FROM control_events UNION ALL SELECT data FROM control_totals")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), secret) {
			t.Fatal("secret persisted in diagnostics")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
