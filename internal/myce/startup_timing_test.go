package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartupTimingActualDetachedCompletion(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}
	recorder := &startupTimings{clock: clock}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		finish := recorder.begin("input_staging", 0)
		close(entered)
		<-release
		finish()
	}()
	<-entered
	clock.Advance(9 * time.Second)
	if got := recorder.snapshot(); len(got) != 0 {
		t.Fatal("unfinished I/O was reported complete", got)
	}
	close(release)
	<-done
	clock.Advance(20 * time.Second) // Later consumption must not timestamp I/O.
	got := recorder.snapshot()
	if len(got) != 1 || got[0].Seconds != 9 || !got[0].FinishedAt.Equal(clock.Now().Add(-20*time.Second)) {
		t.Fatal("lost actual completion", got)
	}
	finish := recorder.begin("input_staging", 0)
	clock.Advance(time.Second)
	finish()
	if got := recorder.snapshot(); got[0].Calls != 2 || got[0].Seconds != 10 {
		t.Fatal("same-attempt retries must retain their cost", got)
	}
}

func TestStartupTimingFencesAndRecovery(t *testing.T) {
	q, clock := fixture(t)
	m := machine("startup", "fake")
	submitted := submit(t, q, spec("experiment"))
	assigned := check(t, q, m, nil).Job
	phase := StartupPhase{Phase: "input_staging", Calls: 1, Seconds: 3, StartedAt: clock.Now(), FinishedAt: clock.Now().Add(3 * time.Second)}
	report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt + 1}, Session: assigned.Session, StartupTiming: []StartupPhase{phase}}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if len(job(t, q, submitted.ID).StartupTiming) != 0 {
		t.Fatal("stale attempt supplied timings")
	}
	report.Attempt = assigned.Attempt
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	got := job(t, q, submitted.ID)
	if len(got.StartupTiming) != 1 || got.StartupTiming[0].Seconds != 3 {
		t.Fatal("heartbeat lost timing", got.StartupTiming)
	}
	// Worker state carries completed observations across recovery without
	// manufacturing time for an interrupted/incomplete interval.
	dir := t.TempDir()
	store := &workerStore{dir: dir, state: workerState{Job: got, Session: got.Session}}
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recovered workerState
	if err := json.Unmarshal(data, &recovered); err != nil || len(recovered.Job.StartupTiming) != 1 {
		t.Fatal("recovery lost timing", err)
	}
	r := result(assigned, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.State != "queued" || len(got.StartupTiming) != 0 {
		t.Fatal("retry retained previous execution timing", got.StartupTiming)
	}
}

func TestStartupTimingNativeSidecarToAcceptedJob(t *testing.T) {
	r := nativeStart(t, nativeSpec(t), "fake")
	nativeFile(t, r.dir, "main-ready")
	nativeTouch(t, r.dir, "finish")
	result := nativeFinish(t, r)
	if !result.Success {
		t.Fatal(result)
	}
	if err := r.q.Complete(result); err != nil {
		t.Fatal(err)
	}
	got := job(t, r.q, r.j.ID)
	for _, wanted := range []struct {
		phase   string
		sidecar int
	}{{"input_staging", 0}, {"native_supervisor", 0}, {"process_start", 1}, {"sidecar_readiness", 1}, {"process_start", 0}, {"start_authorization", 0}} {
		found := false
		for _, phase := range got.StartupTiming {
			if phase.Phase == wanted.phase && phase.Sidecar == wanted.sidecar {
				found = phase.Calls > 0 && !phase.FinishedAt.Before(phase.StartedAt)
			}
		}
		if !found {
			t.Errorf("accepted result missing %s/%d: %+v", wanted.phase, wanted.sidecar, got.StartupTiming)
		}
	}
}

func TestStartupTimingSafeMonotonicIngress(t *testing.T) {
	_, clock := fixture(t)
	j := Job{Extension: &ExtensionSelection{Digest: "frozen"}, Extensions: []Extension{{Digest: "frozen", Variants: []ExtensionVariant{{Execution: ExecutionSpec{Sidecars: []NativeSidecar{{Name: "helper"}}}}}}}}
	phase := StartupPhase{Phase: "sidecar_readiness", Sidecar: 1, Calls: 2, Seconds: 8, StartedAt: clock.Now(), FinishedAt: clock.Now().Add(8 * time.Second)}
	retainStartupTiming(&j, []StartupPhase{phase})
	old := phase
	old.Calls, old.Seconds = 1, 2
	bad := phase
	bad.Phase = "/private/worker/path"
	foreign := phase
	foreign.Sidecar = 2
	retainStartupTiming(&j, []StartupPhase{old, bad, foreign, phase})
	if len(j.StartupTiming) != 1 || j.StartupTiming[0].Seconds != 8 {
		t.Fatal("unsafe or regressive timing", j.StartupTiming)
	}
}
