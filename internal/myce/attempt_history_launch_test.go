package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #379: run the real native preparation path, never start a payload, then
// acknowledge all three exact refusals and reopen the durable queue.
func TestAttemptHistoryLaunchRefusal(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	marker := filepath.Join(t.TempDir(), "must-not-start")
	spec := spec("experiment")
	spec.Resumable = false
	spec.Run.Workdir = t.TempDir()
	spec.Run.Command = []string{"sh", "-c", "touch " + marker}
	spec.Run.Env = map[string]string{"PRIVATE_VALUE": "private-secret"}
	submitted := submit(t, q, spec)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	var first Result
	for attempt := 1; attempt <= 3; attempt++ {
		j := check(t, q, w.Machine, nil).Job
		if j == nil || j.Attempt != attempt {
			t.Fatalf("missing attempt %d: %+v", attempt, j)
		}
		p, launchErr := startAfterFixtureInputs(t, w, store, *j)
		if p != nil || launchErr == nil {
			t.Fatalf("expected refusal: %v %v", p, launchErr)
		}
		live := startFailureOutcome(result(j, false), launchErr)
		if live.ExecutionCause == nil || live.ExecutionCause.Predicate != "native_workdir_containment" {
			t.Fatalf("live refusal lost predicate: %+v", live)
		}
		// Recover using only durable facts, as after worker restart/acknowledgement.
		data, err := os.ReadFile(filepath.Join(store.dir, store.state.LaunchOutcome))
		if err != nil {
			t.Fatal(err)
		}
		var facts memberOutcome
		if err := json.Unmarshal(data, &facts); err != nil {
			t.Fatal(err)
		}
		r := classifyAttemptOutcome(result(j, false), attemptCause{memberOutcome: facts})
		if !r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != launchRefused || r.ExecutionCause.Predicate != "native_workdir_containment" {
			t.Fatalf("lost refusal: %+v", r)
		}
		if attempt == 1 {
			first = r
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(store.dir, store.state.LaunchOutcome)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Complete(first); !errors.Is(err, ErrStale) {
		t.Fatalf("accepted stale refusal: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("payload started: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "private-token"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "private-token"}
	got, err := client.GetJob(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.Losses != 3 || got.Starts != 3 || got.StartedAt != nil || len(got.AttemptHistory) != 3 {
		t.Fatalf("changed accounting/history: %+v", got)
	}
	for i, a := range got.AttemptHistory {
		if a.Attempt != i+1 || a.ExecutionCause == nil || a.ExecutionCause.Predicate != "native_workdir_containment" || !strings.Contains(a.ExecutionCause.Reason, "workdir beneath {work}") || !strings.Contains(a.Reason, "workdir beneath {work}") {
			t.Fatalf("lost attempt reason: %+v", a)
		}
		if !strings.Contains(logs.String(), fmt.Sprintf("attempt=%d predicate=native_workdir_containment", i+1)) {
			t.Fatal("missing queue log", logs.String())
		}
	}
	ledger, err := client.JobLedger(context.Background(), submitted.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := 0
	for _, e := range ledger.Entries {
		if e.Kind == "outcome" {
			outcomes++
			if !strings.Contains(e.Reason, "workdir beneath {work}") {
				t.Fatal("ledger lost reason", e)
			}
		}
	}
	if outcomes != 3 {
		t.Fatal("missing ledger outcomes", outcomes)
	}
	var status Snapshot
	if err := client.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(status)
	jobData, _ := json.Marshal(got)
	public := string(data) + string(jobData) + logs.String()
	for _, private := range []string{"private-secret", "private-token", spec.Run.Workdir, marker} {
		if strings.Contains(public, private) {
			t.Fatalf("disclosed %q", private)
		}
	}
	if !strings.Contains(string(data), "workdir beneath {work}") {
		t.Fatal("status lost refusal", string(data))
	}
}

func TestAttemptHistoryLaunchPredicatePrivacy(t *testing.T) {
	for _, tc := range []struct {
		cause           error
		kind, predicate string
	}{
		{reject("device_identity_mismatch", errors.New("private-secret")), launchRefused, "device_identity_mismatch"},
		{reject("device_probe_unavailable", errors.New("private-secret")), launchRefused, "device_probe_unavailable"},
		{fmt.Errorf("private-secret: %w", errWorkerMemory), launchRefused, "memory_capacity"},
		{errors.New("private-secret"), environmentFailed, "environment_prepare"},
	} {
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		cause := s.startFailure(Job{ID: ID(), Attempt: 1}, tc.kind, tc.cause)
		r := startFailureOutcome(Result{}, cause)
		s.close()
		if r.ExecutionCause == nil || r.ExecutionCause.Predicate != tc.predicate || strings.Contains(r.ExecutionCause.String(), "private-secret") {
			t.Fatalf("lost/unsafe predicate: %+v", r)
		}
	}
	for _, predicate := range []string{"native_workdir_containment", "private-secret"} {
		c := safeRuntimeCause(&RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: predicate, Reason: "private-secret"})
		data, _ := json.Marshal(c)
		if strings.Contains(string(data), "private-secret") {
			t.Fatal("worker supplied text leaked", string(data))
		}
	}
}
