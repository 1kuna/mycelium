package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// #223: an accepted request finishing after listeners close must not charge a
// running production attempt for the queue's own shutdown gap.
func TestQueueShutdownPreservesRunningAttempt(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	fakeCheckpointAvailability(t, q, true)
	runner, peer := machine("runner", "fake"), machine("peer", "fake")
	s := spec("production")
	s.AllowedMachines = []string{runner.ID}
	j := submit(t, q, s)
	assigned := check(t, q, runner, nil).Job
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	check(t, q, peer, nil)
	before := storedTestJob(t, q, j.ID)

	entered, release := make(chan struct{}), make(chan struct{})
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	request := make(chan error, 1)
	go func() {
		client := Client{URL: server.URL, Token: "test"}
		request <- client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: peer}, nil)
	}()
	<-entered
	q.BeginShutdown()
	closing := make(chan struct{})
	server.Config.RegisterOnShutdown(func() { close(closing) })
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Config.Shutdown(context.Background()) }()
	<-closing
	c.Advance(ProductionLossAfter + time.Second)
	q.mu.Lock()
	q.maintain() // A tick/handler that reaches maintenance during shutdown.
	q.mu.Unlock()
	close(release)
	var unavailable *HTTPError
	if err := <-request; !errors.As(err, &unavailable) || unavailable.Status != 503 || unavailable.Code != "queue_stopping" {
		t.Fatalf("in-flight mutation during shutdown: %v", err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	assertAttempt := func() {
		t.Helper()
		got := q.s.Jobs[j.ID]
		if got.State != "running" || got.Attempt != before.Attempt || got.Starts != before.Starts || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Machine != before.Machine || got.Session != before.Session || q.s.Machines[runner.ID].Gone {
			t.Fatalf("shutdown changed running attempt: %+v", got)
		}
	}
	assertAttempt()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	check(t, q, runner, assigned)
	assertAttempt()

	// The replacement still detects a genuinely quiet production worker at 5 s.
	c.Advance(ProductionLossAfter)
	check(t, q, peer, nil)
	got := q.s.Jobs[j.ID]
	if got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 || !q.s.Machines[runner.ID].Gone {
		t.Fatalf("normal worker loss detection changed: %+v", got)
	}
}

// #889: shutdown used to queue behind every observation/control mutex waiter
// before its HTTP deadline even began. No queue time or client timeout can be
// mistaken for permission to change the running attempt.
func TestQueueShutdownShedsBlockedObservations(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	joining := httptest.NewTLSServer(API{Queue: q, Token: "test"}.Handler())
	defer joining.Close()

	// One accepted observation owns a cache refresh, another waits for its
	// mutex, and a third waits for the read-only SQL connection. All are
	// independent of the transaction we hold while setting the shutdown fence.
	conn, err := q.usageReader.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var requests sync.WaitGroup
	start := func(client Client, path string) {
		requests.Add(1)
		go func() {
			defer requests.Done()
			_ = client.Call(context.Background(), "GET", path, nil, nil)
		}()
	}
	client := Client{URL: server.URL, Token: "test"}
	// Retained usage is opt-in; the compact default never waits for usage SQL.
	start(client, "/api/status?sections=usage")
	await(t, func() bool { return q.usageReader.Stats().InUse == 1 && q.usageReader.Stats().WaitCount > 0 })
	start(client, "/api/status?sections=usage")
	start(Client{URL: joining.URL, Token: "test", HTTP: joining.Client()}, "/api/inspection")

	// q.mu stays held until after the fence, so BeginShutdown can only finish
	// without the mutex; the long deadline is a deadlock backstop, not a bound.
	q.mu.Lock()
	fenced := make(chan struct{})
	go func() { q.BeginShutdown(); close(fenced) }()
	select {
	case <-fenced:
	case <-time.After(testWaitTimeout):
		q.mu.Unlock()
		<-fenced
		t.Fatal("shutdown waited for the queue mutex before cancelling requests")
	}
	clock.Advance(RestartGrace + LossAfter)
	// New mutations must refuse even while q.mu and the SQL reader are busy.
	if err := q.Tick(); err == nil {
		q.mu.Unlock()
		t.Fatal("shutdown admitted a maintenance transaction")
	}
	q.mu.Unlock()
	// The held SQL connection never frees the handlers, so only cancellation
	// can drain them; http.Server.Shutdown polls idle connections at up to
	// 500 ms, so a one-second deadline failed under host load (#1224).
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := q.ShutdownServers(ctx, server.Config, joining.Config); err != nil {
		t.Fatal(err)
	}
	requests.Wait()
	// The SQL connection is deliberately still held: cancellation, not release
	// of the bottleneck, must let every handler and both transports finish.
}

func TestQueueShutdownRollsBackUncommittedControl(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- q.update(func() error {
			q.touchJob(j.ID).State = "failed"
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	q.BeginShutdown()
	close(release)
	var unavailable *HTTPError
	if err := <-done; !errors.As(err, &unavailable) || unavailable.Code != "queue_stopping" {
		t.Fatalf("uncommitted request did not retry: %v", err)
	}
	if q.s.Jobs[j.ID].State != "queued" {
		t.Fatal("uncommitted control state survived shutdown")
	}
}

func TestQueueShutdownCancelsPortability(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			q, clock := fixture(t)
			handler := API{Queue: q, Token: "test"}.Handler()
			pressure := q.mu.enablePressure(clock)
			q.mu.Lock()
			request := httptest.NewRequest(method, "/api/portability", strings.NewReader(`{}`))
			request.Header.Set("Authorization", "Bearer test")
			done := make(chan struct{})
			go func() { defer close(done); handler.ServeHTTP(httptest.NewRecorder(), request) }()
			await(t, func() bool {
				pressure.mu.Lock()
				defer pressure.mu.Unlock()
				return len(pressure.waiting) > 0
			})
			q.BeginShutdown()
			select {
			case <-done:
				q.mu.Unlock()
			case <-time.After(testWaitTimeout):
				q.mu.Unlock()
				<-done
				t.Fatal("portability retained its queue lock wait after shutdown")
			}
		})
	}
}

// Reservation projection no longer takes q.mu. Its captured-state contract is
// covered by TestOutputDeliveryCapacityPreservesSpoolAndReservations.
func TestQueueShutdownCancelsFileStatus(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	view := q.captureFileStoreStatus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.fileLifecycle.Lock()
	done := make(chan struct{})
	go func() { defer close(done); _ = view.status(ctx) }()
	cancel()
	select {
	case <-done:
		q.fileLifecycle.Unlock()
	case <-time.After(testWaitTimeout):
		q.fileLifecycle.Unlock()
		<-done
		t.Fatal("file status retained its lifecycle lock wait after cancellation")
	}
}
