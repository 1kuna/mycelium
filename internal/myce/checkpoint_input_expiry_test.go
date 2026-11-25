package myce

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func checkpointInputExpiryFixture(t *testing.T, q *Queue) (*Job, Spec, Machine) {
	t.Helper()
	fakeCheckpointAvailability(t, q, true)
	worker := cpuMachine("worker", "worker", "cpu", 4, 16)
	worker.Capabilities = append(worker.Capabilities, "shared_inputs_v1")
	worker.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
	submit(t, q, cpuSpec(1, 1))
	producer := check(t, q, worker, nil).Job
	if producer == nil {
		t.Fatal("the worker did not receive the producer")
	}
	dir := filepath.Join(producer.Checkpoint, "attempt-1", "nested")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "seed")
	if err := os.WriteFile(path, []byte("seed"), 0600); err != nil {
		t.Fatal(err)
	}
	finish(t, q, producer, true)
	rel, err := filepath.Rel(q.root, path)
	if err != nil {
		t.Fatal(err)
	}
	consumer := cpuSpec(1, 1)
	consumer.Name, consumer.Project = "consumer", "consumer"
	consumer.SharedInputs = map[string]SharedInput{
		"seed": {NAS: filepath.ToSlash(rel), SHA256: strings.Repeat("a", 64), SizeBytes: 4},
	}
	return producer, consumer, worker
}

func TestCheckpointInputExpiryStopsQueuedConsumerBeforeStaging(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
	root, db := t.TempDir(), filepath.Join(t.TempDir(), "queue.db")
	q := openShardQueue(t, db, root, clock)
	defer func() { _ = q.Close() }()
	producer, spec, worker := checkpointInputExpiryFixture(t, q)
	deadline := clock.Now().Add(DefaultCheckpointExpiryGrace)
	consumer := submit(t, q, spec)
	if notes := consumer.CheckpointInputExpiries; len(notes) != 1 || notes[0].Field != "shared_inputs.seed.nas" || !notes[0].Deadline.Equal(deadline) || !notes[0].ExpiredAt.IsZero() {
		t.Fatalf("missing scheduled expiry in admission: %+v", notes)
	}
	validateWireValue(t, compileWireSchema(t, "job-v1.schema.json"), consumer)
	q.mu.Lock()
	candidate := q.pick(q.s.Machines[worker.ID])
	q.mu.Unlock()
	if candidate == nil || candidate.ID != consumer.ID {
		t.Fatal("fixture cannot exercise the expiry placement fence")
	}

	clock.Advance(DefaultCheckpointExpiryGrace + time.Minute)
	request := Checkin{Machine: worker, period: time.Second, CheckpointExpiryProtocol: checkpointExpiryProtocol}
	reply, err := q.Checkin(request)
	if err != nil || len(reply.CheckpointExpiries) != 1 || reply.CheckpointExpiries[0].ID != producer.ID || reply.Job != nil {
		t.Fatalf("expected expiry work without a consumer assignment: tasks=%+v job=%v err=%v", reply.CheckpointExpiries, reply.Job != nil, err)
	}
	removal := expireCheckpoint(root, reply.CheckpointExpiries[0], os.Rename)
	if !removal.Removed {
		t.Fatalf("expiry did not remove the source: %+v", removal)
	}
	if got, err := q.jobSnapshot(consumer.ID); err != nil || got.State != "queued" || got.Hold == nil || got.Hold.Kind != checkpointInputExpiryDue {
		t.Fatalf("expected an expiry hold before the report: %+v err=%v", got, err)
	}
	if gap, err := q.Checkin(request); err != nil || gap.Job != nil {
		t.Fatalf("offered a consumer after deletion and before the removal report: job=%v err=%v", gap.Job != nil, err)
	}
	request.CheckpointExpiryReports = []CheckpointExpiryReport{removal}
	if settled, err := q.Checkin(request); err != nil || settled.Job != nil {
		t.Fatalf("offered a consumer alongside its removal report: job=%v err=%v", settled.Job != nil, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = openShardQueue(t, db, root, clock)
	got, err := q.jobSnapshot(consumer.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.Attempt != 0 || got.DiagnosticCode != checkpointInputExpired || got.Finding == nil ||
		got.Finding.Code != checkpointInputExpired || !strings.Contains(got.Finding.Action, "retained NAS path") {
		t.Fatalf("missing durable pre-staging failure: %s attempt=%d code=%q finding=%+v", got.State, got.Attempt, got.DiagnosticCode, got.Finding)
	}
	if notes := got.CheckpointInputExpiries; len(notes) != 1 || !notes[0].ExpiredAt.Equal(clock.Now()) {
		t.Fatalf("missing confirmed removal evidence: %+v", notes)
	}
}

func TestCheckpointInputExpiryRefusesAlreadyRemovedSource(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
	root, db := t.TempDir(), filepath.Join(t.TempDir(), "queue.db")
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	// Exercise a retained flat checkpoint on the current sharded queue.
	producer, spec, worker := checkpointInputExpiryFixture(t, q)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = openShardQueue(t, db, root, clock)
	fakeCheckpointAvailability(t, q, true)
	clock.Advance(DefaultCheckpointExpiryGrace + time.Minute)
	tasks := expiryCheckin(t, q, worker)
	if len(tasks) != 1 || tasks[0].ID != producer.ID {
		t.Fatalf("expected normal producer expiry: %+v", tasks)
	}
	removal := expireCheckpoint(root, tasks[0], os.Rename)
	if !removal.Removed {
		t.Fatalf("normal expiry did not remove the source: %+v", removal)
	}
	if _, err := os.Stat(producer.Checkpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("producer checkpoint was not removed: %v", err)
	}
	expiryCheckin(t, q, worker, removal)

	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	_, err = client.Submit(t.Context(), spec)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Status != http.StatusGone || refusal.Code != checkpointInputExpired ||
		!strings.Contains(refusal.Message, "shared_inputs.seed.nas") || !strings.Contains(refusal.Message, "retained NAS path") {
		t.Fatalf("expected a typed actionable submission refusal: %v", err)
	}
	if got := check(t, q, worker, nil).Job; got != nil {
		t.Fatalf("refused input still reached assignment: %s", got.ID)
	}
}
