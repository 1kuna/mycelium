package myce

import (
	"fmt"
	"slices"
	"strings"
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

// max_concurrency caps a parent's unfinished pieces; finishing one frees a slot.
func TestFanOutMaxConcurrencyCapsRunningPieces(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 5)
	s.FanOut.MaxConcurrency = 2
	workers := []Machine{m}
	e := q.s.Extensions[s.Extension]
	for i := range 2 {
		w := cloneMachine(&m)
		w.ID, w.HostID, w.Session = fmt.Sprintf("w%d", i), fmt.Sprintf("host-%d", i), fmt.Sprintf("session-%d", i)
		r := newExtensionReport(e, &w, s.workType())
		r.Setup, r.Qualified, r.CheckID = true, true, fmt.Sprintf("check-%d", i)
		w.ExtensionReports = []ExtensionReport{r}
		check(t, q, w, nil)
		workers = append(workers, w)
	}
	parent := submit(t, q, s)
	on := map[string]*Job{} // worker ID -> running piece
	start := func(w Machine) *Job {
		run := check(t, q, w, nil).Job
		if run != nil {
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			on[w.ID] = run
		}
		return run
	}
	for done := 0; done < 5; done++ {
		for _, w := range workers {
			if on[w.ID] == nil {
				start(w)
			}
		}
		if len(on) > 2 {
			t.Fatalf("%d pieces running beyond max_concurrency 2", len(on))
		}
		if want := min(2, 5-done); len(on) != want {
			t.Fatalf("done=%d: %d pieces running, want %d", done, len(on), want)
		}
		if h := q.waitHold(q.s.Jobs[parent.ID]); done < 3 && (h == nil || h.Kind != "fan_out_cap" || !strings.Contains(h.Detail, "max_concurrency 2")) {
			t.Fatalf("hold does not name the cap: %+v", h)
		}
		for id, run := range on {
			clock.Advance(time.Second)
			w := workers[slices.IndexFunc(workers, func(w Machine) bool { return w.ID == id })]
			check(t, q, w, run)
			finish(t, q, run, true)
			delete(on, id)
			break
		}
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 5 {
		t.Fatalf("incomplete gather: state=%s results=%d", got.State, len(got.FanOut.Results))
	}
}
