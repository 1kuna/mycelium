package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestControlTelemetryHolderWaiters(t *testing.T) {
	q, clock := fixture(t)
	_ = newObservationLimit(clock, q)
	holder := q.mu.named("synthetic_holder", nil)
	holder.Lock()
	holder.next(controlSerialize)
	acquired := make(chan struct{}, 3)
	for range 3 {
		go func() {
			waiter := q.mu.named("synthetic_waiter", nil)
			waiter.RLock()
			waiter.RUnlock()
			acquired <- struct{}{}
		}()
	}
	await(t, func() bool { return len(q.control.snapshot().InFlight) == 4 })
	clock.Advance(2 * time.Second)
	flight := q.control.snapshot()
	if flight.Pending == 0 || len(flight.InFlight) != 4 {
		t.Fatal("missing live acquisitions", flight)
	}
	holder.Unlock()
	for range 3 {
		<-acquired
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var waits int
	for _, e := range page.Entries {
		if e.Operation == "synthetic_holder" {
			if e.Phases[controlSerialize] != 2*time.Second || e.HoldNS != 2*time.Second {
				t.Fatal("wrong holder phases", e)
			}
		}
		if e.Operation == "synthetic_waiter" {
			waits++
			if e.Blocker != holder.event.ID || e.Phase != "serialization" || e.WaitNS != 2*time.Second || e.Enqueued.After(holder.event.Finished) {
				t.Fatal("lost holder/waiter join", e)
			}
		}
	}
	if waits != 3 {
		t.Fatalf("retained %d waiters", waits)
	}
	// The two simultaneous readers have independent acquisitions and finishes.
	first, second := q.mu.named("reader", nil), q.mu.named("reader", nil)
	first.RLock()
	second.RLock()
	clock.Advance(time.Millisecond)
	first.RUnlock()
	clock.Advance(time.Millisecond)
	second.RUnlock()
	if first.event.ID == second.event.ID || first.event.HoldNS != time.Millisecond || second.event.HoldNS != 2*time.Millisecond {
		t.Fatal("readers conflated")
	}
}

func TestControlTelemetryRejectionBurst(t *testing.T) {
	q, clock, l := adaptiveObservationFixture(t, 1, 1)
	active, _ := observationAdmit(l, "/api/jobs/selected-experiment?view=wait")
	defer active()
	release := observationPressure(l, clock, time.Millisecond)
	defer release()
	seen := map[string]string{}
	for _, test := range []struct{ path, peer, family string }{{"/api/status", "192.0.2.1:1234", "myce/development"}, {"/api/status", "192.0.2.1:9876", "myce/development"}, {"/api/jobs/selected-experiment", "192.0.2.2:2345", "mycelium-python/development"}} {
		r := httptest.NewRequest("GET", test.path, nil)
		r.RemoteAddr = test.peer
		r.Header.Set("User-Agent", test.family)
		span := &requestSpan{id: q.control.id(), handler: 77, client: q.control.client(r)}
		r = r.WithContext(context.WithValue(r.Context(), requestSpanKey{}, span))
		rw := httptest.NewRecorder()
		_, admitted := l.admit(rw, r)
		if admitted || rw.Code != 429 {
			t.Fatal("expected measured shedding", rw.Code)
		}
		key := strings.Split(test.peer, ":")[0]
		if before := seen[key]; before != "" && before != span.client {
			t.Fatal("ephemeral port changed source")
		}
		seen[key] = span.client
	}
	if seen["192.0.2.1"] == seen["192.0.2.2"] {
		t.Fatal("sources conflated")
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]int{}
	for _, e := range page.Entries {
		if e.Kind == "rejection" {
			routes[e.Operation]++
			if e.Request == 0 || e.Client == "" || e.Handler != 77 || e.PressureNS <= time.Duration(e.BaselineNS) || e.PressureSamples == 0 || e.Active != 1 || e.Frontier != "job_wait" {
				t.Fatal("missing decision facts", e)
			}
		}
	}
	if routes["status"] != 2 || routes["job_full"] != 1 {
		t.Fatal(routes)
	}
	data, _ := json.Marshal(page)
	if strings.Contains(string(data), "192.0.2.") {
		t.Fatal("peer leaked")
	}
}

type controlSteppingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *controlSteppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

func TestControlTelemetryPersistenceRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")
	root := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if q != nil {
			q.Close()
		}
	})
	// A second injected observation clock gives every real persistence boundary
	// a distinct timestamp without changing scheduling time or sleeping for disk.
	q.control.clock = &controlSteppingClock{now: clock.Now()}
	before := q.control.epoch
	source := q.control.anonymous("synthetic-source")
	if err = q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, total := range q.control.snapshot().Totals {
		if total.Kind == "lock" && strings.Contains(total.Operation, "TestControlTelemetryPersistenceRestart") {
			found = true
			for _, phase := range []int{controlSerialize, controlSQL, controlCommit} {
				if total.Phases[phase] <= 0 {
					t.Fatal("phase missing", controlPhaseNames[phase], total)
				}
			}
		}
	}
	if !found {
		t.Fatal("update acquisition missing")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q = nil
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	if q.control.epoch == before || q.control.anonymous("synthetic-source") != source {
		t.Fatal("restart identity provenance lost")
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) == 0 || page.Telemetry.DurableAt.IsZero() {
		t.Fatal("durable evidence missing")
	}
	if q.mu.TryLock() {
		q.mu.Unlock()
	} else {
		t.Fatal("inspection took queue lock")
	}
}

func TestControlTelemetryPressureProbeSpans(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	for _, failure := range []bool{false, true} {
		m := &sharedPressureMonitor{clock: clock, enabled: true, source: func(context.Context) (SharedHostPressure, error) {
			clock.Advance(time.Millisecond)
			if failure {
				return SharedHostPressure{}, errors.New("private error")
			}
			return SharedHostPressure{MemoryKnown: true, WakeKnown: true}, nil
		}}
		m.sample(t.Context())
		<-m.lane.done
		clock.Advance(2 * time.Second)
		// Avoid launching another source while asserting this completed probe.
		m.lane.timing.base = 10 * time.Second
		m.sample(t.Context())
		spans := m.probeSpans()
		if len(spans) != 1 {
			t.Fatal(spans)
		}
		p := spans[0]
		if p.Ready.Sub(p.Started) != time.Millisecond || p.Consumed.Sub(p.Ready) != 2*time.Second || (p.Failure != "") != failure {
			t.Fatal("ready/consume/failure conflated", p)
		}
		if failure && m.current != nil {
			t.Fatal("failed probe renewed evidence")
		}
		m.acknowledge(p.Sequence)
		if len(m.probeSpans()) != 0 {
			t.Fatal("ack did not release delivered history")
		}
	}
}

func TestControlTelemetryCheckinAndWire(t *testing.T) {
	q, clock := fixture(t)
	m := machine("synthetic-control-worker", "fake")
	span := PressureProbeSpan{Sequence: 1, Started: clock.Now(), Ready: clock.Now(), Consumed: clock.Now()}
	reply, err := q.Checkin(Checkin{Machine: m, PressureProbes: []PressureProbeSpan{span}})
	if err != nil || reply.ControlTelemetryProtocol != controlTelemetryProtocol {
		t.Fatal(reply, err)
	}
	if err = q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var checkin, probe bool
	for _, r := range page.Entries {
		if r.Kind == "checkin" && r.Accepted {
			checkin = r.Worker == q.control.anonymous(m.ID) && r.Session == q.control.anonymous(m.Session)
		}
		if r.Kind == "pressure_probe" && r.Probe.Sequence == 1 {
			probe = true
		}
	}
	if !checkin || !probe {
		t.Fatal("missing worker receipt", page)
	}
	wire := &workerHygieneWire{}
	w := Worker{hygieneWire: wire}
	c := Checkin{Machine: m, PressureProbes: []PressureProbeSpan{span}}
	if len(w.hygieneCheckin(c).PressureProbes) != 0 {
		t.Fatal("sent unnegotiated telemetry")
	}
	w.acceptHygieneReply(reply)
	if len(w.hygieneCheckin(c).PressureProbes) != 1 {
		t.Fatal("negotiation lost evidence")
	}
	rw := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/control", nil)
	r.Header.Set("Authorization", "Bearer test-only")
	budgetHost(q, 64<<20, 2)
	API{q, "test-only"}.Handler().ServeHTTP(rw, r)
	if rw.Code != 200 || !strings.Contains(rw.Body.String(), "handler_frontier_queue_pressure") {
		t.Fatal("operator endpoint", rw.Code, rw.Body.String())
	}
}

func TestControlTelemetryReaderDependencies(t *testing.T) {
	q, clock := fixture(t)
	// Teach the retention filter that an ordinary short reader is unexceptional.
	for range 2 {
		r := q.mu.named("blocking_reader", nil)
		r.RLock()
		r.RUnlock()
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	reader := q.mu.named("blocking_reader", nil)
	reader.RLock()
	writer := q.mu.named("queued_writer", nil)
	done := make(chan struct{})
	go func() { writer.Lock(); writer.Unlock(); close(done) }()
	await(t, func() bool { return q.control.controlWriters.Load() == 1 })
	clock.Advance(time.Second)
	reader.RUnlock()
	<-done
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var retainedReader, joined bool
	for _, e := range page.Entries {
		if e.ID == reader.event.ID && e.Epoch == q.control.epoch {
			retainedReader = e.Contended
		}
		if e.ID == writer.event.ID && e.Epoch == q.control.epoch {
			for _, id := range e.Blockers {
				joined = joined || id == reader.event.ID
			}
		}
	}
	if !retainedReader || !joined {
		t.Fatal("reader dependency was filtered away", retainedReader, joined)
	}
}

func TestControlTelemetryWriterPriority(t *testing.T) {
	q, _ := fixture(t)
	// Hold the sole connection to model slow diagnostic SQLite I/O. The control
	// writer must wait BEFORE q.mu, allowing independent reads to keep working.
	conn, err := q.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	q.control.append(ControlEvent{ID: q.control.id(), Kind: "rejection", Operation: "status"})
	flushed := make(chan error, 1)
	go func() { flushed <- q.control.flush(q.db) }()
	await(t, func() bool {
		if q.control.writeGate.TryLock() {
			q.control.writeGate.Unlock()
			return false
		}
		return true
	})
	done := make(chan struct{})
	go func() { l := q.mu.named("control_during_diagnostic", nil); l.Lock(); l.Unlock(); close(done) }()
	await(t, func() bool { return q.control.controlWriters.Load() == 1 })
	if !q.mu.TryRLock() {
		t.Fatal("diagnostic writer caused a q.mu hold")
	}
	q.mu.RUnlock()
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-flushed; err != nil {
		t.Fatal(err)
	}
	<-done
	if q.control.controlWriters.Load() != 0 {
		t.Fatal("writer announcement leaked")
	}
}

func TestControlTelemetryWriterReservationPressure(t *testing.T) {
	q, clock := fixture(t)
	p := q.mu.enablePressure(clock)
	l := q.mu.named("baseline", nil)
	l.Lock()
	clock.Advance(time.Millisecond)
	l.Unlock()
	q.control.writeGate.Lock()
	done := make(chan struct{})
	go func() {
		l := q.mu.named("waiting_reservation", nil)
		l.Lock()
		l.Unlock()
		close(done)
	}()
	await(t, func() bool { return q.control.controlWriters.Load() == 1 })
	clock.Advance(2 * time.Millisecond)
	pressured, delay := p.pressure()
	q.control.writeGate.Unlock()
	<-done
	if !pressured || delay != 2*time.Millisecond {
		t.Fatal("writer reservation disappeared from queue pressure", pressured, delay)
	}
}

func TestControlTelemetryNegotiationAcknowledgesSent(t *testing.T) {
	q, clock := fixture(t)
	budgetHost(q, 64<<20, 2)
	server := httptest.NewServer(API{q, "test-only"}.Handler())
	defer server.Close()
	m := machine("probe-wire-worker", "fake")
	span := PressureProbeSpan{Sequence: 9, Started: clock.Now(), Ready: clock.Now(), Consumed: clock.Now()}
	monitor := &sharedPressureMonitor{spans: []PressureProbeSpan{span}}
	w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test-only"}, hygieneWire: &workerHygieneWire{}}
	c := Checkin{Machine: m, PressureProbes: monitor.probeSpans()}
	first, err := w.checkin(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	monitor.acknowledge(first.PressureProbeSequence)
	if first.ControlTelemetryProtocol != controlTelemetryProtocol || first.PressureProbeSequence != 0 || len(monitor.probeSpans()) != 1 {
		t.Fatal("negotiation acknowledged unsent evidence", first)
	}
	second, err := w.checkin(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	monitor.acknowledge(second.PressureProbeSequence)
	if second.PressureProbeSequence != 9 || len(monitor.probeSpans()) != 0 {
		t.Fatal("accepted evidence was not acknowledged", second)
	}
}

func TestControlTelemetryStorageRequestIdentity(t *testing.T) {
	q, _ := fixture(t)
	span := &requestSpan{id: 123, handler: 456, client: "anonymous-source"}
	ctx := context.WithValue(t.Context(), requestSpanKey{}, span)
	_ = q.reservedOutputBytes(nil, ctx)
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range page.Entries {
		if e.Operation == "reservedOutputBytes" {
			if e.Request != 123 || e.Handler != 456 || e.Client != "anonymous-source" {
				t.Fatal("request identity dropped", e)
			}
			return
		}
	}
	t.Fatal("storage acquisition missing")
}

func TestControlTelemetryEfficiencyRequestIdentity(t *testing.T) {
	q, _ := fixture(t)
	r := httptest.NewRequest("GET", "/api/efficiency", nil)
	r.Header.Set("Authorization", "Bearer test-only")
	w := httptest.NewRecorder()
	API{q, "test-only"}.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	for _, e := range q.control.snapshot().Recent {
		if e.Kind == "lock" && e.Request != 0 && e.Handler != 0 && e.Client != "" {
			return
		}
	}
	t.Fatal("HTTP read acquisition lost request identity")
}
