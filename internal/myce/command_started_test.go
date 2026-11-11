package myce

import (
	"context"
	"net/http/httptest"
	"testing"
)

// command_started and cancel release are the two answers a project needs
// before resubmitting (#1341): never started, started, cancelled before start,
// and cancelled while running with release pending then confirmed.
func TestCommandStartedAndCancelConfirmation(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test-only"}
	ctx := context.Background()
	running := submit(t, q, spec("experiment"))
	started := func(id string) bool {
		t.Helper()
		j, err := client.GetJob(ctx, id)
		if err != nil || j.CommandStarted == nil {
			t.Fatalf("job reply: %+v %v", j, err)
		}
		return *j.CommandStarted
	}
	compactStarted := func(id string) bool {
		t.Helper()
		// A filtered read bypasses the shared default's short-lived cache.
		var s CompactStatus
		if err := client.Call(ctx, "GET", "/api/status?project="+running.Spec.Project, nil, &s); err != nil {
			t.Fatal(err)
		}
		for _, row := range s.Jobs {
			if row.ID == id {
				return row.CommandStarted
			}
		}
		t.Fatalf("job %s missing from compact status", id)
		return false
	}

	if started(running.ID) || compactStarted(running.ID) {
		t.Fatal("queued job reported started")
	}
	m := machine("one", "fake")
	m.MaxJobs = 1
	assigned := check(t, q, m, nil).Job
	if started(running.ID) {
		t.Fatal("assignment alone reported started")
	}
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	if !started(running.ID) || !compactStarted(running.ID) {
		t.Fatal("started job reported not started")
	}

	before := submit(t, q, spec("experiment"))
	a, err := client.Cancel(ctx, before.ID)
	if err != nil || ackRelease(a) != "not_applicable" || started(before.ID) {
		t.Fatalf("cancelled before start: %+v %v", a, err)
	}

	a, err = client.Cancel(ctx, running.ID)
	if err != nil || ackRelease(a) != "pending" || !started(running.ID) {
		t.Fatalf("cancelled while running: %+v %v", a, err)
	}
	finish(t, q, assigned, true)
	j, err := client.GetJob(ctx, running.ID)
	if err != nil || j.Cancellation == nil || ackRelease(*j.Cancellation) != "confirmed" || !*j.CommandStarted {
		t.Fatalf("released: %+v %v", j, err)
	}
}
