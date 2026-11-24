package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #1650: ordinary producer outputs use the same accepted-attempt binding,
// readiness clock, retention and transitive cancellation as fan-out outputs.
func TestPieceDependencyOrdinaryProducer(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "cancel", "expiry"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 1)
			s.FanOut = nil
			s.Outputs = map[string]string{"out": "results"}
			parent := submit(t, q, s)
			c := pieceConsumer(s, parent.ID, "")
			c.SubmissionKey = "ordinary-consumer"
			consumer := submit(t, q, c)
			if dependencyReady(consumer) || !consumer.WaitingSince.IsZero() {
				t.Fatal("ordinary consumer ran before producer acceptance")
			}
			invalid := c
			invalid.SubmissionKey = "invalid"
			invalid.After = &PieceDependency{Job: parent.ID, Piece: "not-a-piece", Outputs: c.After.Outputs}
			if _, err := q.Submit(invalid); err == nil {
				t.Fatal("ordinary source accepted a piece selector")
			}
			invalid.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "undeclared"}}
			_, err := q.Submit(invalid)
			var refusal *HTTPError
			if !errors.As(err, &refusal) || refusal.Code != "job_files_invalid" || refusal.Message != "after output is not declared by the parent" {
				t.Fatal("dependency validation lost its typed field rule", err)
			}
			if _, err := q.Resolve(ResolveRequest{Spec: invalid}); !errors.As(err, &refusal) || refusal.Message != "after output is not declared by the parent" {
				t.Fatal("resolve did not catch the same dependency error", err)
			}
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != parent.ID {
				t.Fatal("blocked consumer displaced producer", run)
			}
			if outcome == "cancel" {
				if _, err := q.CancelJob(parent.ID); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, consumer.ID); got.State != "cancelled" || got.Attempt != 0 {
					t.Fatal("ordinary cancellation missed blocked dependent", got)
				}
				return
			}
			clock.Advance(time.Second)
			finish(t, q, run, outcome != "failure")
			ready := job(t, q, consumer.ID)
			if outcome == "failure" {
				if ready.State != "failed" || ready.DiagnosticCode != "upstream_failed" || ready.Attempt != 0 {
					t.Fatal("failed producer released consumer", ready)
				}
				return
			}
			if !dependencyReady(ready) || ready.Dependency.Execution != (Execution{ID: parent.ID, Attempt: run.Attempt}) || ready.Dependency.Inputs["input"] != job(t, q, parent.ID).OutputFiles["out"] || !ready.Dependency.ReadyAt.Equal(clock.Now()) {
				t.Fatal("ordinary output lacks exact accepted binding", ready.Dependency)
			}
			q = reopenCancellationQueue(t, q, clock)
			if replay := submit(t, q, c); replay.ID != consumer.ID || replay.Dependency.Execution != ready.Dependency.Execution || !replay.Dependency.ReadyAt.Equal(*ready.Dependency.ReadyAt) {
				t.Fatal("restart/replay changed ordinary binding", replay)
			}
			if outcome == "expiry" {
				if err := q.update(func() error { q.touchJob(parent.ID).FilesExpired = true; return nil }); err != nil {
					t.Fatal(err)
				}
				c.SubmissionKey = "expired"
				if _, err := q.Submit(c); !isResultExpired(err) {
					t.Fatal("new ordinary binding ignored expired outputs", err)
				}
				return
			}
			running := check(t, q, m, nil).Job
			if running == nil || running.ID != consumer.ID {
				t.Fatal("ready ordinary consumer was not placed", running)
			}
			// A successful producer's later abort still fences running descendants.
			ack, err := q.CancelJob(parent.ID)
			if err != nil || ack.Release == nil || *ack.Release != "pending" || !cancelled(job(t, q, consumer.ID)) {
				t.Fatal("ordinary abort lost downstream release custody", ack, err)
			}
			if err := q.Started(result(running, true)); err == nil {
				t.Fatal("aborted consumer retained launch authority")
			}
			finish(t, q, running, false)
			ack, err = q.CancelJob(parent.ID)
			if err != nil || ack.Release == nil || *ack.Release == "pending" {
				t.Fatal("ordinary abort did not settle exact release", ack, err)
			}
			c.SubmissionKey = "late"
			if late := submit(t, q, c); late.State != "queued" || !dependencyReady(late) {
				t.Fatal("later cancellation invalidated a succeeded producer's outputs", late)
			}
		})
	}
}

func TestReadyPipelineStageGetsProjectTurn(t *testing.T) {
	for _, tc := range []struct {
		name, rootProject string
		stageRAM          float64
	}{
		{"same project", "test", 4},
		{"another project", "another-project", 4},
		{"stage needs both releases", "test", 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			m := m3Machine()
			m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 8, 8
			gpu := bigSpec("experiment", 8)
			gpu.Needs.HostMemoryGB = 4
			producer := gpu
			producer.Outputs = map[string]string{"out": "results"}
			producer.Files = FileManifest{"input.txt": putPieceTestFile(t, q, "producer input")}
			parent := submit(t, q, producer)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || first.ID != parent.ID {
				t.Fatal("pipeline producer was not assigned")
			}
			second := submit(t, q, gpu)
			peer := assignedJob(machineCheck(t, q, m, first))
			if peer == nil || peer.ID != second.ID {
				t.Fatal("second GPU job was not assigned")
			}
			stageSpec := cpuSpec(1, tc.stageRAM)
			stageSpec.Project = parent.Spec.Project
			stageSpec.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "out"}}
			stage := submit(t, q, stageSpec)
			gpu.Project = tc.rootProject
			root := submit(t, q, gpu)
			if got := assignedJob(machineCheck(t, q, m, first, peer)); got != nil {
				t.Fatalf("offered new work before release: %s", got.ID)
			}
			clock.Advance(time.Second)
			r := result(first, true)
			r.OutputRefs = FileManifest{"out": putPieceTestFile(t, q, "accepted stage output")}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if !dependencyReady(job(t, q, stage.ID)) {
				t.Fatal("stage did not receive the producer's accepted output")
			}
			if tc.stageRAM > 4 {
				if got := assignedJob(machineCheck(t, q, m, peer)); got != nil {
					t.Fatalf("offered new work before the stage's reservation filled: %s", got.ID)
				}
				finish(t, q, peer, true)
				peer = nil
			}
			want := root.ID
			if tc.rootProject == stage.Spec.Project {
				want = stage.ID
			}
			var running []*Job
			if peer != nil {
				running = append(running, peer)
			}
			got := assignedJob(machineCheck(t, q, m, running...))
			if got == nil {
				t.Fatal("no job was offered after memory released")
			}
			if got.ID != want {
				t.Fatalf("expected %s to win the freed RAM, got %s", want, got.ID)
			}
		})
	}
}

func pieceConsumer(s Spec, parent, piece string) Spec {
	s.FanOut = nil
	s.Workload = nil
	s.Name = "consumer-" + piece
	s.Outputs = nil
	s.After = &PieceDependency{Job: parent, Piece: piece, Outputs: map[string]string{"input": "out"}}
	return s
}

func cancelledJobForTest(j *Job) bool {
	return cancelled(j) && (j.State == "stopping" || j.State == "cancelled")
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
	// Pieces are serial jobs (MY-49): drain the producer's remaining pieces.
	var tail *Job
	for job(t, q, parent.ID).State != "succeeded" {
		tail = check(t, q, m, nil).Job
		if tail == nil || tail.Batch == nil || tail.Batch.Parent != parent.ID {
			t.Fatal("blocked dependent displaced producer tail", tail)
		}
		finish(t, q, tail, true)
	}
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
	if got := job(t, q, consumer.ID); got.DiagnosticCode != "upstream_cancelled" || got.Reason != "upstream parent cancelled" {
		t.Fatal("upstream cancellation lost its reason", got)
	}
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

func TestPieceDependencySucceededParentLaterCancellation(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 1)
	if err := q.ConfigureFileStore(q.files.root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	wave := check(t, q, m, nil).Job
	ref := putPieceTestFile(t, q, "accepted output")
	r := result(wave, true)
	// A piece is an ordinary job reporting its own output names; the parent
	// keys them by piece (KEY/OUTPUT_NAME).
	r.OutputRefs = FileManifest{"out": ref}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	completed := job(t, q, parent.ID)
	if completed.State != "succeeded" {
		t.Fatal("parent did not succeed", completed)
	}
	clock.Advance(time.Second)
	if err := q.Cancel(parent.ID); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" || got.CancelledAt == nil || !got.CancelledAt.After(*completed.FinishedAt) {
		t.Fatal("missing later cancellation intent", got)
	}
	for _, c := range []Spec{pieceConsumer(s, parent.ID, "item-0"), wholeParentConsumer(s, parent.ID, false)} {
		got := submit(t, q, c)
		if got.State != "queued" || cancelled(got) || !dependencyReady(got) || !got.Dependency.ReadyAt.Equal(clock.Now()) {
			t.Fatal("succeeded parent cancellation prevented binding", got)
		}
		if c.After.Piece == "" {
			if got.Dependency.Executions["item-0"] != result(wave, true).Execution {
				t.Fatal("whole-parent binding lost accepted execution", got.Dependency)
			}
		} else if got.Dependency.Execution != result(wave, true).Execution {
			t.Fatal("piece binding lost accepted execution", got.Dependency)
		}
		if len(got.Dependency.Files) != 1 {
			t.Fatal("binding lost accepted outputs", got.Dependency)
		}
	}
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, parent.ID).FilesExpired {
		t.Fatal("parent outputs did not expire")
	}
	if _, err := q.Submit(wholeParentConsumer(s, parent.ID, false)); !isResultExpired(err) {
		t.Fatal("later cancellation masked expired outputs", err)
	}
}

func TestPieceDependencyCancelledParentReason(t *testing.T) {
	q, clock, _, s := fanOutFixture(t, 1)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	existing := submit(t, q, wholeParentConsumer(s, parent.ID, false))
	clock.Advance(time.Second)
	if err := q.Cancel(parent.ID); err != nil {
		t.Fatal(err)
	}
	late := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	for _, id := range []string{existing.ID, late.ID} {
		got := job(t, q, id)
		if got.State != "cancelled" || got.CancelledAt == nil || got.DiagnosticCode != "upstream_cancelled" || got.Reason != "upstream parent cancelled" || got.Dependency.ReadyAt != nil || !got.FinishedAt.Equal(clock.Now()) {
			t.Fatal("missing typed upstream cancellation", got)
		}
	}
}
