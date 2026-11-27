package myce

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func warmDependentMachine(id string) Machine {
	m := machine(id, "fake")
	m.Capabilities = append(m.Capabilities, warmPiecesCapability, warmPiecesDependenciesCapability)
	return m
}

// A warm consumer whose keys bind the named producer pieces, by key.
func warmDependentSpec(producer string, keys ...string) Spec {
	s := spec("experiment")
	s.WarmPieces = warmPiecesFixed
	s.Resumable = false
	s.SubmissionKey = "warm-dependent-" + producer
	s.Outputs = map[string]string{"labels": "results"}
	s.FanOut = &FanOut{}
	for _, key := range keys {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: key, After: &PieceDependency{Job: producer, Piece: key, Outputs: map[string]string{"segments.json": "out"}}})
	}
	return s
}

func warmSuccessOutput(t *testing.T, q *Queue, j *Job, d *WarmPieceDelivery, name string) FileRef {
	t.Helper()
	r := warmRequest(j, "success", d)
	r.OutputRefs = FileManifest{name: putPieceTestFile(t, q, d.Key+" "+name)}
	warmCall(t, q, r)
	return r.OutputRefs[name]
}

// Keys are offered in readiness order to one attempt, each with exactly its
// producer piece's reference. A failed or cancelled producer fails only the
// keys it binds, and the producer's cancellation scope names the consumer.
func TestWarmPieceDependenciesReadyOrderAndUpstreamFailure(t *testing.T) {
	q, _, m, s := warmFixture(t)
	s.Resumable = false
	producer := submit(t, q, s)
	other := s
	other.SubmissionKey = "warm-cancelled-producer"
	other.AllowedMachines = []string{"worker-c"}
	cancelledProducer := submit(t, q, other)
	consumerSpec := warmDependentSpec(producer.ID, "c", "a", "b")
	consumerSpec.FanOut.Pieces = append(consumerSpec.FanOut.Pieces, FanOutPiece{Key: "w", After: &PieceDependency{Job: cancelledProducer.ID, Outputs: map[string]string{"segments.json": "a/out"}}})
	consumer := submit(t, q, consumerSpec)
	if got := job(t, q, consumer.ID); dependencyReady(got) || got.Spec.StartBy != nil {
		t.Fatalf("consumer ready before any producer piece: %+v", got.FanOut.Warm)
	}

	run := warmStart(t, q, m, producer.ID)
	a := warmSuccessOutput(t, q, run, warmNext(t, q, run, "a"), "out")
	worker := warmDependentMachine("worker-b")
	consumerRun := warmStart(t, q, worker, consumer.ID)
	offer := warmCall(t, q, warmRequest(consumerRun, "next", nil))
	if offer.Delivery == nil || offer.Delivery.Key != "a" || offer.Delivery.Sequence != 2 || !maps.Equal(offer.Files, FileManifest{"segments.json": a}) {
		t.Fatalf("first ready key not offered with exactly its input: %+v", offer)
	}
	// The producer settles c before the consumer finishes a: c is next, not b.
	warmCall(t, q, warmRequest(run, "failure", warmNext(t, q, run, "b")))
	c := warmSuccessOutput(t, q, run, warmNext(t, q, run, "c"), "out")
	warmSuccessOutput(t, q, consumerRun, warmNext(t, q, consumerRun, "a"), "labels")
	if replay := warmCall(t, q, warmRequest(consumerRun, "next", nil)); replay.Delivery == nil || replay.Delivery.Key != "c" || !maps.Equal(replay.Files, FileManifest{"segments.json": c}) {
		t.Fatalf("second offer = %+v", replay)
	}
	warmSuccessOutput(t, q, consumerRun, warmNext(t, q, consumerRun, "c"), "labels")

	if err := q.Cancel(cancelledProducer.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if got := warmCall(t, q, warmRequest(consumerRun, "next", nil)); !got.End {
		t.Fatalf("consumer kept waiting after every remaining producer ended: %+v", got)
	}
	if err := q.Complete(result(consumerRun, true)); err != nil {
		t.Fatal(err)
	}
	final := job(t, q, consumer.ID)
	deps := final.FanOut.Warm.Dependencies
	if final.State != "failed" || final.Attempt != 1 || final.FanOut.Warm.Loads != 1 || !strings.Contains(final.Reason, "2/4 pieces failed: b, w") ||
		len(final.FanOut.Results) != 2 || deps["b"].State != "upstream_failed" || deps["w"].State != "upstream_cancelled" || cancelled(final) {
		t.Fatalf("wrong consumer outcome: %s %q cancelled=%v %+v", final.State, final.Reason, cancelled(final), final.FanOut.Warm)
	}

	var scope Job
	q.mu.Lock()
	q.projectCancellation(&scope, q.s.Jobs[producer.ID])
	q.mu.Unlock()
	for _, piece := range scope.CancellationScope.Pieces {
		if !slices.Equal(piece.Dependents, []string{consumer.ID}) {
			t.Fatalf("producer piece %s dependents = %v", piece.Key, piece.Dependents)
		}
	}
}

// With no ready key the attempt keeps its reservation for at most its own
// measured load time, then ends the segment as a planned requeue. It is placed
// again, and its production start window restarts, once a key is ready.
func TestWarmPieceDependenciesIdleBoundRequeuesWithoutPenalty(t *testing.T) {
	q, clock, m, s := warmFixture(t)
	s.Resumable = false
	producer := submit(t, q, s)
	consumerSpec := warmDependentSpec(producer.ID, "a", "b")
	consumerSpec.Priority = "production"
	consumer := submit(t, q, consumerSpec)
	run := warmStart(t, q, m, producer.ID)
	warmSuccessOutput(t, q, run, warmNext(t, q, run, "a"), "out")
	if got := job(t, q, consumer.ID); got.Spec.StartBy == nil {
		t.Fatal("start window did not start at the first ready key")
	}

	worker := warmDependentMachine("worker-b")
	consumerRun := warmStart(t, q, worker, consumer.ID)
	advance := func(d time.Duration) {
		clock.Advance(d)
		check(t, q, worker, consumerRun) // Production loss is detected after 5 s.
		check(t, q, m, run)
	}
	advance(4 * time.Second) // Measured load.
	warmSuccessOutput(t, q, consumerRun, warmNext(t, q, consumerRun, "a"), "labels")
	idle := warmRequest(consumerRun, "next", nil)
	if got := warmCall(t, q, idle); !got.Wait || got.End || got.Delivery != nil {
		t.Fatalf("no ready key did not wait: %+v", got)
	}
	advance(3 * time.Second)
	if got := warmCall(t, q, idle); !got.Wait {
		t.Fatalf("attempt gave up before its measured load time: %+v", got)
	}
	advance(2 * time.Second)
	if got := warmCall(t, q, idle); !got.End {
		t.Fatalf("attempt kept idling past its measured load time: %+v", got)
	}
	if got := job(t, q, consumer.ID); got.State != "stopping" || got.Reason != warmIdleYield {
		t.Fatalf("idle bound did not end the segment: %s %q", got.State, got.Reason)
	}
	if err := q.Complete(result(consumerRun, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour) // Longer than any production start window.
	q.mu.Lock()
	q.maintainStartDeadlines()
	q.mu.Unlock()
	got := job(t, q, consumer.ID)
	if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || dependencyReady(got) || got.Spec.StartBy != nil {
		t.Fatalf("idle requeue was not a planned, unready requeue: %s %q losses=%d infra=%d startBy=%v", got.State, got.Reason, got.Losses, got.InfrastructureRetries, got.Spec.StartBy)
	}
	if placed := check(t, q, worker, nil).Job; placed != nil {
		t.Fatalf("unready parent placed: %s", placed.ID)
	}

	warmSuccessOutput(t, q, run, warmNext(t, q, run, "b"), "out")
	next := warmStart(t, q, worker, consumer.ID)
	if next.Spec.StartBy == nil || next.Spec.StartBy.Before(clock.Now()) {
		t.Fatalf("start window did not restart at the next ready key: %v", next.Spec.StartBy)
	}
	warmSuccessOutput(t, q, next, warmNext(t, q, next, "b"), "labels")
	if err := q.Complete(result(next, true)); err != nil {
		t.Fatal(err)
	}
	if final := job(t, q, consumer.ID); final.State != "succeeded" || final.FanOut.Warm.Loads != 2 || final.Losses != 0 || final.InfrastructureRetries != 0 {
		t.Fatalf("consumer after idle requeue: %s %q %+v", final.State, final.Reason, final.FanOut.Warm)
	}
}

func TestWarmPieceDependenciesRefusals(t *testing.T) {
	q, _, _, s := warmFixture(t)
	producer := submit(t, q, s)
	plain := cpuSpec(1, 1)
	plain.Project = s.Project
	plain.Outputs = map[string]string{"out": "results"}
	plain.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "a", After: &PieceDependency{Job: producer.ID, Piece: "a", Outputs: map[string]string{"in": "out"}}}}}
	overlap := warmDependentSpec(producer.ID, "a")
	overlap.Files = FileManifest{PlainPiecePrefix + "a/segments.json": putPieceTestFile(t, q, "own")}
	unknown := warmDependentSpec(producer.ID, "a")
	unknown.FanOut.Pieces[0].After.Piece = "missing"
	for name, tc := range map[string]struct {
		spec   Spec
		reason string
	}{
		"ordinary pieces":        {plain, "per-piece after requires advanced.warm_pieces"},
		"overlapping item input": {overlap, "inputs must not overlap"},
		"undeclared producer":    {unknown, "after.piece is not declared"},
	} {
		if _, err := q.Submit(tc.spec); err == nil || !strings.Contains(err.Error(), tc.reason) {
			t.Fatalf("%s: wanted %q, got %v", name, tc.reason, err)
		}
	}
	old := machine("old-worker", "fake")
	old.Capabilities = append(old.Capabilities, warmPiecesCapability)
	run := warmStart(t, q, warmDependentMachine("producer-worker"), producer.ID)
	warmSuccessOutput(t, q, run, warmNext(t, q, run, "a"), "out")
	dependent := submit(t, q, warmDependentSpec(producer.ID, "a"))
	if got := check(t, q, old, nil).Job; got != nil {
		t.Fatalf("worker without dependency support received %s", got.ID)
	}
	if got := job(t, q, dependent.ID); got.State != "queued" || !dependencyReady(got) {
		t.Fatalf("dependent %s ready=%v", got.State, dependencyReady(got))
	}
}

// A queued parent whose every key failed upstream is placed once to finalize
// with an empty list, as after an interrupted finalization.
func TestWarmPieceDependenciesAllUpstreamFailedFinalizes(t *testing.T) {
	q, _, m, s := warmFixture(t)
	s.Resumable = false
	producer := submit(t, q, s)
	consumer := submit(t, q, warmDependentSpec(producer.ID, "a"))
	run := warmStart(t, q, m, producer.ID)
	warmCall(t, q, warmRequest(run, "failure", warmNext(t, q, run, "a")))
	if err := q.Complete(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, consumer.ID); got.State != "queued" || got.FanOut.Next != 1 || !dependencyReady(got) {
		t.Fatalf("settled parent %s next=%d ready=%v", got.State, got.FanOut.Next, dependencyReady(got))
	}
	finalize := warmStart(t, q, warmDependentMachine("worker-b"), consumer.ID)
	if err := q.Complete(result(finalize, true)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, consumer.ID); got.State != "failed" || got.FanOut.Warm.Dependencies["a"].State != "upstream_failed" || got.FanOut.Warm.Loads != 0 {
		t.Fatalf("finalized parent %s %q %+v", got.State, got.Reason, got.FanOut.Warm)
	}
}
