package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func pieceConsumer(s Spec, parent, piece string) Spec {
	s.FanOut = nil
	s.Workload = nil
	s.Name = "consumer-" + piece
	s.Outputs = nil
	s.After = &PieceDependency{Job: parent, Piece: piece, Outputs: map[string]string{"input": "out"}}
	return s
}

func TestPieceDependencyAcceptanceAndReplay(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	c := pieceConsumer(s, parent.ID, "item-0")
	c.SubmissionKey = "consume-0"
	consumer := submit(t, q, c)
	if dependencyReady(consumer) || consumer.State != "queued" || !consumer.WaitingSince.IsZero() {
		t.Fatal("consumer runnable before acceptance")
	}
	wave := check(t, q, m, nil).Job
	if wave == nil || wave.Batch == nil {
		t.Fatal("blocked consumer displaced producer")
	}
	finish(t, q, wave, true)
	ready := job(t, q, consumer.ID)
	if !dependencyReady(ready) || ready.Dependency.Execution.ID != wave.ID || ready.Dependency.Execution.Attempt != wave.Attempt {
		t.Fatalf("missing exact binding: %+v", ready.Dependency)
	}
	if job(t, q, parent.ID).State != "queued" {
		t.Fatal("consumer waited for gather")
	}
	replay := submit(t, q, c)
	if replay.ID != consumer.ID || replay.Dependency.ReadyAt == nil || !replay.Dependency.ReadyAt.Equal(*ready.Dependency.ReadyAt) {
		t.Fatal("key replay changed binding")
	}
	c.After.Piece = "item-1"
	if _, err := q.Submit(c); err == nil {
		t.Fatal("conflicting key accepted")
	}
	// A late submit binds the same accepted attempt, including across reopen.
	reopened := reopenCancellationQueue(t, q, clock)
	late := submit(t, reopened, pieceConsumer(s, parent.ID, "item-0"))
	if !dependencyReady(late) || late.Dependency.Execution != ready.Dependency.Execution {
		t.Fatal("late/reopened binding changed")
	}
}

func TestPieceDependencyProductionWindowStartsAtReadiness(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&m): {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	parent := submit(t, q, s)
	c := pieceConsumer(s, parent.ID, "item-0")
	c.Priority = "production"
	consumer := submit(t, q, c)
	explicit := c
	explicit.Name = "explicit-window"
	cutoff := clock.Now().Add(time.Second)
	explicit.StartBy = &cutoff
	absolute := submit(t, q, explicit)
	clock.Advance(ProductionStartTimeout + time.Second)
	wave := check(t, q, m, nil).Job
	if got := job(t, q, consumer.ID); got.State != "queued" || got.Spec.StartBy != nil || got.ProductionHost != "" {
		t.Fatalf("blocked job earned start window/hold: %+v", got)
	}
	if got := job(t, q, absolute.ID); got.State != "failed" {
		t.Fatal("explicit blocked deadline was extended")
	}
	if wave == nil || wave.Batch == nil {
		t.Fatal("blocked production demand withheld producer")
	}
	finish(t, q, wave, true)
	ready := job(t, q, consumer.ID)
	if !dependencyReady(ready) || ready.Spec.StartBy == nil || !ready.Spec.StartBy.Equal(clock.Now().Add(ProductionStartTimeout)) || !ready.WaitingSince.Equal(clock.Now()) {
		t.Fatal("readiness did not initialize one production window", ready)
	}
}

func TestPieceDependencySiblingFailureAndParentAbort(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	ready := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	blocked := submit(t, q, pieceConsumer(s, parent.ID, "item-2"))
	wave := check(t, q, m, nil).Job
	finish(t, q, wave, true)
	// Fail the remaining producer wave directly, so downstream fairness needn't
	// determine which of the two now-runnable commands happens to be pulled first.
	q.mu.Lock()
	tail := q.fanOutCandidate(q.s.Jobs[parent.ID], q.s.Machines[m.ID])
	if tail == nil {
		q.mu.Unlock()
		t.Fatal("no tail")
	}
	tail = q.assignFanOut(tail, q.s.Machines[m.ID])
	assigned := q.assign(tail, q.s.Machines[m.ID])
	q.mu.Unlock()
	finish(t, q, &assigned, false)
	if got := job(t, q, blocked.ID); got.State != "failed" || got.DiagnosticCode != "upstream_failed" {
		t.Fatalf("unaccepted dependency did not fail: %+v", got)
	}
	if got := job(t, q, ready.ID); !dependencyReady(got) || got.State != "queued" {
		t.Fatal("sibling failure retracted accepted output")
	}
	late := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	if !dependencyReady(late) {
		t.Fatal("late accepted dependency lost")
	}
	consumer := check(t, q, m, nil).Job
	if consumer == nil || consumer.Spec.After == nil {
		t.Fatal("no consumer assignment")
	}
	cancelled, err := q.CancelJob(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if *cancelled.Release != "pending" || !cancelled.WillNotRun {
		t.Fatalf("parent omitted consumer release: %+v", cancelled)
	}
	if got := job(t, q, consumer.ID); !cancelledJobForTest(got) {
		t.Fatal("parent abort did not fence consumer")
	}
	if err := q.Started(result(consumer, true)); err == nil {
		t.Fatal("Started authorized after abort")
	}
	finish(t, q, consumer, true)
	ack, err := q.CancelJob(parent.ID)
	if err != nil || *ack.Release == "pending" {
		t.Fatalf("release did not settle: %+v %v", ack, err)
	}
	future := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	if future.State != "cancelled" || future.Dependency.ReadyAt != nil {
		t.Fatal("future consumer escaped abort")
	}
}
func cancelledJobForTest(j *Job) bool {
	return cancelled(j) && (j.State == "stopping" || j.State == "cancelled")
}

func TestPieceDependencyRetention(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	if err := q.ConfigureFileStore(q.files.root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	c := pieceConsumer(s, parent.ID, "item-0")
	c.SubmissionKey = "retention-consumer"
	consumer := submit(t, q, c)
	wave := check(t, q, m, nil).Job
	// Publish through the real artifact API; completion verifies bytes outside q.mu.
	ref := putPieceTestFile(t, q, "piece bytes")
	r := result(wave, true)
	r.OutputRefs = FileManifest{}
	for name := range wave.Spec.Outputs {
		r.OutputRefs[name] = ref
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	// A completed child expires while the parent and its predeclared consumer wait.
	clock.Advance(filePublicationGrace + time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("partial parent lost bytes", err)
	}
	// Fail the remaining sibling; the accepted consumer remains independent.
	if err := q.update(func() error {
		p := q.s.Jobs[parent.ID]
		p.State = "failed"
		now := clock.Now()
		p.FinishedAt = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("bound waiting consumer lost expired source bytes", err)
	}
	late := pieceConsumer(s, parent.ID, "item-0")
	if _, err := q.Submit(late); !isResultExpired(err) {
		t.Fatal("late source binding did not reject expiry", err)
	}
	replay := submit(t, q, c)
	if replay.ID != consumer.ID || !dependencyReady(replay) {
		t.Fatal("expiry changed keyed answer")
	}
	if err := q.Cancel(consumer.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("unneeded artifact not reclaimed", err)
	}
}
func putPieceTestFile(t *testing.T, q *Queue, content string) FileRef {
	t.Helper()
	api := httptest.NewServer(API{Queue: q, Token: "piece-test"}.Handler())
	defer api.Close()
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := (Client{URL: api.URL, Token: "piece-test"}).UploadFile(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestPieceDependencyCancelAcceptanceRace(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	consumer := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	wave := check(t, q, m, nil).Job
	if err := q.Started(result(wave, true)); err != nil {
		t.Fatal(err)
	}
	start, done := make(chan struct{}), make(chan error, 3)
	go func() { <-start; done <- q.Complete(result(wave, true)) }()
	go func() { <-start; done <- q.Cancel(parent.ID) }()
	go func() { <-start; _, err := q.Submit(pieceConsumer(s, parent.ID, "item-0")); done <- err }()
	close(start)
	for range 3 {
		if err := <-done; err != nil && err != ErrStale {
			t.Error(err)
		}
	}
	if got := job(t, q, consumer.ID); !cancelled(got) {
		t.Fatal("acceptance escaped committed abort")
	}
	for _, j := range q.s.Jobs {
		if j.Spec.After != nil && j.Spec.After.Job == parent.ID && !cancelled(j) {
			t.Fatal("concurrent registration escaped abort")
		}
	}
}

func wholeParentConsumer(s Spec, parent string, fanOut bool) Spec {
	if !fanOut {
		s.FanOut, s.Workload = nil, nil
	}
	s.Name = "whole-parent-consumer"
	s.After = &PieceDependency{Job: parent, Outputs: map[string]string{"item-0/input": "item-0/out"}}
	return s
}

func TestPieceDependencyWholeParentAdmissionAndRestart(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	c := wholeParentConsumer(s, parent.ID, true)
	c.SubmissionKey = "whole-consumer"
	// Bound inputs must not overwrite either a serial output or keyed wave output.
	invalid := c
	invalid.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"out": "item-0/out"}}
	if _, err := q.Submit(invalid); err == nil {
		t.Fatal("dependent input may overwrite serial-fallback output")
	}
	invalid.After.Outputs = map[string]string{"item-0/out": "item-0/out"}
	if _, err := q.Submit(invalid); err == nil {
		t.Fatal("dependent input may overwrite keyed wave output")
	}
	consumer := submit(t, q, c)
	gather := submit(t, q, wholeParentConsumer(s, consumer.ID, false))
	wave := check(t, q, m, nil).Job
	finish(t, q, wave, true)
	// Even the selected piece is accepted, but a whole-parent edge waits for all.
	if got := job(t, q, consumer.ID); dependencyReady(got) || got.FanOut.Next != 0 || got.Attempt != 0 {
		t.Fatal("partial producer released dependent fan-out", got)
	}
	q = reopenCancellationQueue(t, q, clock)
	clock.Advance(time.Second)
	tail := check(t, q, m, nil).Job
	if tail == nil || tail.Batch.Parent != parent.ID {
		t.Fatal("blocked dependent displaced producer tail", tail)
	}
	finish(t, q, tail, true)
	ready := job(t, q, consumer.ID)
	if !dependencyReady(ready) || !ready.Dependency.ReadyAt.Equal(clock.Now()) || ready.Dependency.Executions["item-0"].ID != wave.ID {
		t.Fatal("whole-parent binding missing accepted execution/readiness", ready.Dependency)
	}
	if ready.Dependency.Inputs["item-0/input"] != job(t, q, parent.ID).OutputFiles["item-0/out"] {
		t.Fatal("keyed output was not bound as local input", ready.Dependency)
	}
	child := check(t, q, m, nil).Job
	if child == nil || child.Batch.Parent != consumer.ID || len(jobInputPaths(child)) != 1 {
		t.Fatal("dependent wave lost bound inputs", child)
	}
	finish(t, q, child, true)
	// Restart with the downstream's fixed membership and accepted partial gather.
	q = reopenCancellationQueue(t, q, clock)
	replay := submit(t, q, c)
	if replay.ID != consumer.ID || len(replay.FanOut.Children) != 1 || replay.Dependency.Executions["item-0"].ID != wave.ID {
		t.Fatal("restart/replay changed binding or membership", replay)
	}
	for job(t, q, consumer.ID).State != "succeeded" {
		tail = check(t, q, m, nil).Job
		if tail == nil || tail.Batch == nil || tail.Batch.Parent != consumer.ID {
			t.Fatal("no dependent tail", tail)
		}
		finish(t, q, tail, true)
	}
	if got := job(t, q, gather.ID); !dependencyReady(got) || got.Dependency.Executions["item-0"].ID != child.ID {
		t.Fatalf("dependent gather not ready: state=%s reason=%s after=%+v binding=%+v parent=%+v", got.State, got.Reason, got.Spec.After, got.Dependency, job(t, q, consumer.ID).FanOut)
	}
}

func TestPieceDependencyWholeParentFailure(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	ordinary := submit(t, q, wholeParentConsumer(s, parent.ID, false))
	fanOut := submit(t, q, wholeParentConsumer(s, parent.ID, true))
	wave := check(t, q, m, nil).Job
	finish(t, q, wave, true)
	clock.Advance(time.Second)
	tail := check(t, q, m, nil).Job
	finish(t, q, tail, false)
	for _, id := range []string{ordinary.ID, fanOut.ID} {
		got := job(t, q, id)
		if got.State != "failed" || got.DiagnosticCode != "upstream_failed" || got.Attempt != 0 || got.StartedAt != nil || !got.FinishedAt.Equal(clock.Now()) || !strings.Contains(got.Reason, "parent did not succeed") {
			t.Error("upstream failure did not fail unattempted consumer clearly", got)
		}
	}
	late := submit(t, q, wholeParentConsumer(s, parent.ID, true))
	if late.State != "failed" || late.Attempt != 0 {
		t.Fatal("late whole-parent consumer accepted failed parent", late)
	}
}

func TestPieceDependencyWholeParentCancellation(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 1)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	consumer := submit(t, q, wholeParentConsumer(s, parent.ID, true))
	gather := submit(t, q, wholeParentConsumer(s, consumer.ID, false))
	finish(t, q, check(t, q, m, nil).Job, true)
	child := check(t, q, m, nil).Job
	if child == nil || child.Batch.Parent != consumer.ID {
		t.Fatal("no downstream wave", child)
	}
	clock.Advance(time.Second)
	ack, err := q.CancelJob(parent.ID)
	if err != nil || *ack.Release != "pending" {
		t.Fatal("upstream omitted downstream wave custody", ack, err)
	}
	for _, id := range []string{consumer.ID, gather.ID, child.ID} {
		if got := job(t, q, id); !cancelledJobForTest(got) {
			t.Error("transitive cancellation missed downstream", got)
		}
	}
	if err := q.Started(result(child, true)); err != ErrStale {
		t.Fatal("cancelled downstream wave was allowed to start", err)
	}
	finish(t, q, child, true)
	ack, err = q.CancelJob(parent.ID)
	if err != nil || *ack.Release == "pending" {
		t.Fatal("downstream exact release did not settle upstream", ack, err)
	}
	q = reopenCancellationQueue(t, q, clock)
	future := submit(t, q, wholeParentConsumer(s, consumer.ID, true))
	if future.State != "cancelled" || future.FanOut.Next != 0 {
		t.Fatal("future dependent escaped persisted cancellation", future)
	}
}
