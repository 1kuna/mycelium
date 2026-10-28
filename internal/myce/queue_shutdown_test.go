package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	before := cloneJob(q.s.Jobs[j.ID])

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
		request <- client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: peer}, nil)
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
