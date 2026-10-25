package myce

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fanOutFixture(t *testing.T, n int) (*Queue, *workerFakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, e, s := curveFixture(t)
	e.Version = "4"
	sweep := e.Variants[0].Sweeps[s.workType()]
	sweep.Run = []string{"sh", "{extension}/batch.sh"}
	e.Variants[0].Sweeps[s.workType()] = sweep
	e.Files["batch.sh"] = []byte(`test -z "$MYCE_TOKEN" || exit 99
exec python3 -c 'import json,os,pathlib; m=json.load(open(os.environ["MYCE_BATCH_MANIFEST"])); [(pathlib.Path(p["key"])/"out").write_text(p["key"]) for p in m["pieces"]]'
`)
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s.Extension = extensionKey(e)
	s.Priority = "experiment"
	s.FanOut = &FanOut{MaxConcurrency: 8}
	for i := range n {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: fmt.Sprintf("item-%d", i)})
	}
	m.ExtensionReports = nil
	r := newExtensionReport(e, &m, s.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "fan-out-check"
	m.ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	key, why := curveKey(e, q.s.Machines[m.ID], s.workType())
	if key == nil {
		t.Fatal(why)
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	for _, c := range []int{1, 2, 4, 8} {
		k := *key
		k.Concurrency = c
		q.s.WorkObservations[identityHash(k)] = WorkObservation{Key: k, Rate: float64(c) * 10, HostMemoryGB: float64(c), Samples: 1, RecordedAt: clock.Now()}
	}
	return q, clock, m, s
}

func TestFanOutWorkerCommandAndArtifactGather(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 2)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.Batch == nil || !run.Batch.Run {
		t.Fatal("no measured wave")
	}
	root, shared, state := t.TempDir(), t.TempDir(), t.TempDir()
	e := q.s.Extensions[s.Extension]
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if err := writeExtensionReports(root, e, "cpu", m.ExtensionReports); err != nil {
		t.Fatal(err)
	}
	w := Worker{Machine: m, Clock: clock, ExtensionRoot: root, JobFileRoot: shared, extensionFingerprint: m.ExtensionFingerprint}
	env, release, err := w.prepareExtensionExecution(run)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = w.prepareExtensionFiles(run, state); err != nil {
		t.Fatal(err)
	}
	if err = runExtensionCommand(context.Background(), run.Spec.Run.Command, run.Spec.Run.Workdir, env, io.Discard, commandScope{Clock: clock, StateDir: state}); err != nil {
		t.Fatal(err)
	}
	if err = w.publishExtensionFiles(*run); err != nil {
		t.Fatal(err)
	}
	finish(t, q, run, true)
	got := job(t, q, parent.ID)
	if got.State != "succeeded" {
		t.Fatal(got.State)
	}
	for key, r := range got.FanOut.Results {
		data, err := os.ReadFile(filepath.Join(shared, r.OutputFiles["out"]))
		if err != nil || string(data) != key {
			t.Fatalf("keyed output mismatch for %s: %v", key, err)
		}
	}
}

func TestFanOutShortDailyAndGather(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	parent := submit(t, q, s)
	// Owner policy is an admission constraint, regardless of a fast curve.
	busy := cloneMachine(&m)
	busy.CapabilityProfile.Availability = &Availability{Available: false, Mode: "daily", Reason: "owner active"}
	if run := q.fanOutCandidate(q.s.Jobs[parent.ID], &busy); run != nil {
		t.Fatal("busy daily host contributed capacity", run)
	}
	for _, want := range []int{2, 1} {
		run := check(t, q, m, nil).Job
		if run == nil || run.Batch == nil || run.Batch.Concurrency != want || run.Spec.Needs.CPUCores != want {
			t.Fatalf("wrong wave: batch=%+v needs=%+v", run.Batch, run.Spec.Needs)
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
	for _, want := range []int{2, 1} {
		run := check(t, q, m, nil).Job
		if run == nil || run.Batch == nil || run.Batch.Concurrency != want {
			t.Fatalf("missing wave of %d pieces: %+v", want, run)
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

func TestFanOutLossRetriesOnlyAffectedWave(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 4)
	s.Outputs = map[string]string{"out": "results"}
	s.FanOut.MaxConcurrency = 2
	other := cloneMachine(&m)
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	e := q.s.Extensions[s.Extension]
	r := newExtensionReport(e, &other, s.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "other-check"
	other.ExtensionReports = []ExtensionReport{r}
	check(t, q, other, nil)
	key, _ := curveKey(e, q.s.Machines[other.ID], s.workType())
	for _, c := range []int{1, 2} {
		k := *key
		k.Concurrency = c
		q.s.WorkObservations[identityHash(k)] = WorkObservation{Key: k, Rate: float64(c) * 10, HostMemoryGB: float64(c), Samples: 1, RecordedAt: clock.Now()}
	}
	parent := submit(t, q, s)
	consumer := submit(t, q, pieceConsumer(s, parent.ID, "item-0"))
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("no first wave")
	}
	b := check(t, q, other, nil).Job
	if b == nil {
		t.Fatal("no second wave")
	}
	finish(t, q, b, true)
	clock.Advance(LossAfter + time.Second)
	check(t, q, other, nil) // Keeps one host alive so the lost one can be declared gone.
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
	if retry.Batch.Concurrency != 2 || retry.Spec.Needs.CPUCores != 2 || retry.Spec.Needs.HostMemoryGB < 2 {
		t.Fatal("retry lost its full wave reservation")
	}
	if err := q.Complete(result(a, true)); err != nil && err != ErrStale {
		t.Fatal(err)
	}
	if job(t, q, parent.ID).State == "succeeded" {
		t.Fatal("stale wave unlocked gather")
	}
	if dependencyReady(job(t, q, consumer.ID)) {
		t.Fatal("stale attempt released consumer")
	}
	finish(t, q, retry, true)
	if got := job(t, q, consumer.ID); !dependencyReady(got) || got.Dependency.Execution.Attempt != 2 {
		t.Fatal("retry did not bind exact accepted attempt")
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" || len(got.FanOut.Results) != 4 {
		t.Fatalf("state=%s results=%d children=%v", got.State, len(got.FanOut.Results), got.FanOut.Children)
	}
}

func TestFanOutOOMForbidsOnlyMeasuredWave(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 2)
	parent := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.Batch == nil || run.Batch.Concurrency != 2 {
		t.Fatal("missing measured wave")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	failure := result(run, false)
	failure.OutOfMemory = true
	if err := q.Complete(failure); err != nil {
		t.Fatal(err)
	}
	e := q.s.Extensions[s.Extension]
	key, _ := curveKey(e, q.s.Machines[m.ID], s.workType())
	if q.s.WorkObservations[identityHash(key)].Forbidden {
		t.Fatal("serial point was forbidden by a concurrent wave")
	}
	key.Concurrency = 2
	if !q.s.WorkObservations[identityHash(key)].Forbidden || job(t, q, parent.ID).State != "failed" {
		t.Fatal("OOM wave was reusable or gather did not fail")
	}
}
