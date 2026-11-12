package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The marker is stop provenance, not permission to erase a saved stage failure.
func TestRQv2OperatorMarkerPreservesSavedStageFailure(t *testing.T) {
	q, _ := fixture(t)
	m := machine("operator", "fake", "d5-review-synthetic-host")
	j := submit(t, q, spec("production"))
	old := check(t, q, m, nil).Job
	if old == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(old, false)); err != nil {
		t.Fatal(err)
	}
	w := recoveryWorker(t)
	w.Machine = m
	w.CheckpointRoot = q.root
	server := workerTestServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	s.state.Session = m.Session
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	child, err := newAttemptStore(s, *old)
	if err != nil {
		t.Fatal(err)
	}
	child.lock, s.lock = s.lock, nil
	s = child
	s.state = workerState{Session: m.Session, Identity: "operator-stage-failure", Job: old, Log: "operator-stage-failure.log", CheckpointRoot: q.root}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(s.dir, "operator-stop.json", s.state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.dir, s.state.Log), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := result(old, false)
	r.Error = "exit status 7"
	if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
		t.Fatal(err)
	}
	s.close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not stop")
		}
	}()
	await(t, func() bool { got := job(t, q, j.ID); return got.InfrastructureRetries != 0 || terminal(got.State) })
	got := job(t, q, j.ID)
	t.Logf("saved nonzero + operator marker: state=%s retries=%d reason=%s", got.State, got.InfrastructureRetries, got.Reason)
	if got.State != "failed" || got.InfrastructureRetries != 0 {
		t.Fatalf("operator marker overwrote stage cause: state=%s retries=%d reason=%s", got.State, got.InfrastructureRetries, got.Reason)
	}
}
