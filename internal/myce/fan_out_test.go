package myce

import (
	"fmt"
	"testing"
	"time"
)

// Pieces run as ordinary serial jobs (MY-49): no measured waves.
func fanOutFixture(t *testing.T, n int) (*Queue, *workerFakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, e, s := curveFixture(t)
	s.Extension = extensionKey(e)
	s.Priority = "experiment"
	s.FanOut = &FanOut{MaxConcurrency: 8}
	for i := range n {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: fmt.Sprintf("item-%d", i)})
	}
	return q, clock, m, s
}

func TestFanOutShortDailyAndGather(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	// Owner policy is an admission constraint.
	busy := cloneMachine(&m)
	busy.CapabilityProfile.Availability = &Availability{Available: false, Mode: "daily", Reason: "owner active"}
	if run := q.fanOutCandidate(q.s.Jobs[parent.ID], &busy); run != nil {
		t.Fatal("busy daily host contributed capacity", run)
	}
	for range 3 {
		run := check(t, q, m, nil).Job
		if run == nil || run.Batch == nil || run.Batch.Concurrency != 1 || len(run.Batch.Pieces) != 1 || run.Spec.Needs.CPUCores != 1 {
			t.Fatalf("wrong piece: batch=%+v needs=%+v", run.Batch, run.Spec.Needs)
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		finish(t, q, run, true)
	}
	got := job(t, q, parent.ID)
	if got.State != "succeeded" || len(got.FanOut.Results) != 3 || len(got.OutputFiles) != 3 {
		t.Fatalf("incomplete gather: %+v", got)
	}
	// Accepted results and membership survive ordinary SQLite reopen.
	reopened := reopenCancellationQueue(t, q, clock)
	if got := job(t, reopened, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 3 {
		t.Fatalf("state=%s results=%d children=%v", got.State, len(got.FanOut.Results), got.FanOut.Children)
	}
}

func TestFanOutGiveUpAfterDispatchStillGathers(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Priority = "production"
	s.Outputs = map[string]string{"out": "results"}
	deadline := clock.Now().Add(time.Second)
	s.GiveUpAt = &deadline
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&m): {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	parent := submit(t, q, s)
	for range 3 {
		run := check(t, q, m, nil).Job
		if run == nil || run.Batch == nil || run.Batch.Concurrency != 1 {
			t.Fatalf("missing piece: %+v", run)
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(2 * time.Second)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		if got := job(t, q, parent.ID); got.State != "queued" || job(t, q, run.ID).State != "running" {
			t.Fatalf("start cutoff cancelled dispatched work: parent=%s wave=%s", got.State, job(t, q, run.ID).State)
		}
		finish(t, q, run, true)
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 3 || len(got.OutputFiles) != 3 {
		t.Fatalf("incomplete gather: %+v", got)
	}
}

func TestFanOutLossRetriesOnlyAffectedPiece(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 2)
	s.Outputs = map[string]string{"out": "results"}
	other := cloneMachine(&m)
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	e := q.s.Extensions[s.Extension]
	r := newExtensionReport(e, &other, s.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "other-check"
	other.ExtensionReports = []ExtensionReport{r}
	check(t, q, other, nil)
	parent := submit(t, q, s)
	consumer := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("no first piece")
	}
	b := check(t, q, other, nil).Job
	if b == nil {
		t.Fatal("no second piece")
	}
	finish(t, q, b, true)
	advanceReportingPeer(t, q, clock, other, LossAfter+time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if job(t, q, b.ID).State != "succeeded" || job(t, q, b.ID).Attempt != 1 {
		t.Fatal("healthy pieces retried")
	}
	retry := check(t, q, other, nil).Job
	if retry == nil || retry.ID != a.ID || retry.Attempt != 2 || retry.Batch.Pieces[0].Key != a.Batch.Pieces[0].Key {
		t.Fatalf("wrong retry: %+v", retry)
	}
	if retry.Batch.Concurrency != 1 || len(retry.Batch.Pieces) != 1 {
		t.Fatal("retry changed its piece membership")
	}
	if err := q.Complete(result(a, true)); err != nil && err != ErrStale {
		t.Fatal(err)
	}
	if job(t, q, parent.ID).State == "succeeded" {
		t.Fatal("stale piece unlocked gather")
	}
	if dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("stale attempt released consumer")
	}
	finish(t, q, retry, true)
	if got := job(t, q, consumer.ID); !dependencyReady(got) || got.Dependency.Execution.Attempt != 2 {
		t.Fatal("retry did not bind exact accepted attempt")
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 2 {
		t.Fatalf("state=%s results=%d children=%v", got.State, len(got.FanOut.Results), got.FanOut.Children)
	}
}
