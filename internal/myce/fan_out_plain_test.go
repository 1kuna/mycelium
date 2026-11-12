package myce

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func plainPiecesFixture(t *testing.T, keys ...string) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	s.Run.Command = []string{"score"}
	s.Files = FileManifest{"model.bin": putPieceTestFile(t, q, "common")}
	s.Outputs = map[string]string{"out": "results"}
	s.FanOut = &FanOut{}
	for _, key := range keys {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: key, Args: []string{"--slot", key}})
		s.Files[PlainPiecePrefix+key+"/slot.yaml"] = putPieceTestFile(t, q, "slot "+key)
	}
	return q, clock, m, s
}

func completeWithOutput(t *testing.T, q *Queue, j *Job, content string) FileRef {
	t.Helper()
	ref := putPieceTestFile(t, q, content)
	r := result(j, true)
	r.OutputRefs = FileManifest{"out": ref}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestPlainPiecesRunAsOrdinaryJobsAndGatherByKey(t *testing.T) {
	q, clock, m, s := plainPiecesFixture(t, "a", "b", "c")
	parent := submit(t, q, s)
	if parent.FanOut == nil || parent.FanOut.Next != 3 || len(parent.FanOut.Children) != 3 {
		t.Fatalf("pieces not materialized at submit: %+v", parent.FanOut)
	}
	refs := map[string]FileRef{}
	for range 3 {
		run := check(t, q, m, nil).Job
		if run == nil || run.Batch == nil || run.Batch.Parent != parent.ID || run.Spec.FanOut != nil {
			t.Fatalf("piece is not an ordinary child job: %+v", run)
		}
		key := run.Batch.Pieces[0].Key
		if !slices.Equal(run.Spec.Run.Command, []string{"score", "--slot", key}) {
			t.Fatalf("piece args not appended: %v", run.Spec.Run.Command)
		}
		if len(run.Spec.Files) != 2 || run.Spec.Files["slot.yaml"] != s.Files[PlainPiecePrefix+key+"/slot.yaml"] || run.Spec.Files["model.bin"] != s.Files["model.bin"] {
			t.Fatalf("piece %s received wrong files: %v", key, run.Spec.Files)
		}
		if got := job(t, q, parent.ID); got.State != "queued" {
			t.Fatalf("parent left queued before every piece settled: %s", got.State)
		}
		refs[key] = completeWithOutput(t, q, run, "result "+key)
	}
	got := job(t, q, parent.ID)
	if got.State != "succeeded" || len(got.FanOut.Results) != 3 {
		t.Fatalf("parent did not gather: state=%s reason=%s", got.State, got.Reason)
	}
	for key, ref := range refs {
		if got.FanOut.Results[key].OutputRefs["out"] != ref || got.OutputRefs[key+"/out"] != ref {
			t.Fatalf("result for %s not gathered by key: %+v", key, got.FanOut.Results[key])
		}
	}
	reopened := reopenCancellationQueue(t, q, clock)
	if got := job(t, reopened, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 3 {
		t.Fatalf("reopen lost gathered results: %+v", got.FanOut)
	}
}

func TestPlainPieceFailureLetsSiblingsSettle(t *testing.T) {
	q, _, m, s := plainPiecesFixture(t, "a", "b", "c")
	s.Resumable = false
	parent := submit(t, q, s)
	first := check(t, q, m, nil).Job
	finish(t, q, first, false)
	if got := job(t, q, first.ID); got.State != "failed" {
		t.Fatalf("piece did not fail: %s", got.State)
	}
	if got := job(t, q, parent.ID); got.State != "queued" {
		t.Fatalf("one failed piece settled the parent early: %s", got.State)
	}
	for range 2 {
		run := check(t, q, m, nil).Job
		if run == nil {
			t.Fatal("sibling piece was cancelled after a failure")
		}
		completeWithOutput(t, q, run, "ok "+run.Batch.Pieces[0].Key)
	}
	got := job(t, q, parent.ID)
	failedKey := first.Batch.Pieces[0].Key
	if got.State != "failed" || got.Reason != fmt.Sprintf("1/3 pieces failed: %s", failedKey) {
		t.Fatalf("state=%s reason=%q", got.State, got.Reason)
	}
	if _, ok := got.FanOut.Results[failedKey]; ok || len(got.FanOut.Results) != 2 {
		t.Fatalf("succeeded pieces not kept by key: %+v", got.FanOut.Results)
	}
}

func TestPlainPiecesCancelParentCancelsEveryPiece(t *testing.T) {
	q, _, m, s := plainPiecesFixture(t, "a", "b", "c")
	parent := submit(t, q, s)
	running := check(t, q, m, nil).Job
	if err := q.Started(result(running, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(parent.ID); err != nil {
		t.Fatal(err)
	}
	for _, id := range job(t, q, parent.ID).FanOut.Children {
		if child := job(t, q, id); !cancelled(child) {
			t.Fatalf("piece %s not cancelled: %s", id, child.State)
		}
	}
	got := job(t, q, parent.ID)
	if got.CancellationScope == nil || len(got.CancellationScope.Pieces) != 3 {
		t.Fatalf("cancel scope does not list every piece: %+v", got.CancellationScope)
	}
	if run := check(t, q, m, running).Job; run != nil && run.ID != running.ID {
		t.Fatalf("cancelled piece was assigned: %+v", run)
	}
}

func TestPlainPieceStaleAttemptRejected(t *testing.T) {
	q, _, m, s := plainPiecesFixture(t, "a")
	parent := submit(t, q, s)
	first := check(t, q, m, nil).Job
	if err := q.update(func() error { q.requeue(q.s.Jobs[first.ID], "retry", false); return nil }); err != nil {
		t.Fatal(err)
	}
	other := machine("other", "fake")
	retry := check(t, q, other, nil).Job
	if retry == nil || retry.ID != first.ID || retry.Attempt == first.Attempt {
		t.Fatalf("missing retry: %+v", retry)
	}
	stale := result(first, true)
	stale.OutputRefs = FileManifest{"out": putPieceTestFile(t, q, "stale")}
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatalf("stale attempt accepted: %v", err)
	}
	if got := job(t, q, parent.ID); got.State != "queued" || len(got.FanOut.Results) != 0 {
		t.Fatalf("stale attempt reached the parent: %+v", got.FanOut)
	}
	ref := completeWithOutput(t, q, retry, "current")
	got := job(t, q, parent.ID)
	if r := got.FanOut.Results["a"]; got.State != "succeeded" || r.Execution.Attempt != retry.Attempt || r.OutputRefs["out"] != ref {
		t.Fatalf("parent did not bind the current attempt: %+v", got.FanOut.Results)
	}
}

func TestPlainPiecesValidation(t *testing.T) {
	q, _, _, base := plainPiecesFixture(t, "a", "b")
	for name, edit := range map[string]func(*Spec){
		"concurrency cap": func(s *Spec) { s.FanOut.MaxConcurrency = 2 },
		"duplicate key":   func(s *Spec) { s.FanOut.Pieces[1].Key = "a" },
		"unclean key":     func(s *Spec) { s.FanOut.Pieces[1].Key = "../b" },
		"undeclared file": func(s *Spec) { s.Files[PlainPiecePrefix+"z/slot.yaml"] = s.Files["model.bin"] },
		"overlaps common": func(s *Spec) { s.Files[PlainPiecePrefix+"a/model.bin"] = s.Files["model.bin"] },
		"overlaps output": func(s *Spec) { s.Files[PlainPiecePrefix+"a/out"] = s.Files["model.bin"] },
		"after":           func(s *Spec) { s.After = &PieceDependency{Job: "x", Outputs: map[string]string{"in": "out"}} },
		"too many": func(s *Spec) {
			for i := range 255 {
				s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: fmt.Sprintf("k%d", i)})
			}
		},
	} {
		s := cloneJob(&Job{Spec: base}).Spec
		edit(&s)
		// after is refused by its own source check before piece validation.
		if _, err := q.Submit(s); err == nil || name != "after" && !strings.Contains(err.Error(), "piece") {
			t.Fatalf("%s: accepted or wrong error: %v", name, err)
		}
	}
}
