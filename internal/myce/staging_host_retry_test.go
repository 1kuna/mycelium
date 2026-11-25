package myce

import (
	"testing"
	"time"
)

func TestStagingStorageHoldReleasesGPUAndSurvivesRestart(t *testing.T) {
	q, clock := fixture(t)
	a, b := machine("a", "cuda"), machine("b", "cuda")
	s := spec("experiment")
	s.Resumable = false
	s.Needs.Platform = Platform{Required: []string{"cuda"}}
	j := submit(t, q, s)
	status := func() *Job {
		t.Helper()
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	refuse := func(a *Job, pending bool) Result {
		t.Helper()
		if a == nil || a.ID != j.ID {
			t.Fatalf("missing affected assignment: %+v", a)
		}
		r := result(a, false)
		r.Interrupted, r.ReleasePending = true, pending
		r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "storage_full"}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	first := refuse(check(t, q, a, nil).Job, true)
	if got := status(); got.ExecutionRelease != "pending" || got.Hold == nil || got.Hold.Kind != stagingStorageHold {
		t.Fatalf("missing storage hold or release fence: release=%s hold=%+v", got.ExecutionRelease, got.Hold)
	}
	s.AllowedMachines = []string{a.ID}
	peer := submit(t, q, s)
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatalf("assigned before exact release: %+v", got)
	}
	first.ReleaseOnly, first.ReleasePending = true, false
	if err := q.Complete(first); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, a, nil).Job
	if next == nil || next.ID != peer.ID {
		t.Fatalf("storage-blocked job took peer GPU: %+v", next)
	}
	finish(t, q, next, true)

	clock.Advance(time.Second)
	refuse(check(t, q, b, nil).Job, false)
	q = reopenCancellationQueue(t, q, clock)
	for range 3 {
		clock.Advance(time.Second)
		for _, m := range []Machine{a, b} {
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatalf("reassigned without operator retry: %+v", got)
			}
		}
	}
	got := status()
	if got.Attempt != 2 || got.ExecutionRelease != "confirmed" || got.Losses != 0 || got.InfrastructureRetries != 0 ||
		got.Hold == nil || got.Hold.Kind != stagingStorageHold || got.Finding == nil || got.Finding.Code != stagingStorageHold {
		t.Fatalf("storage hold lost after restart: %+v", got)
	}
	paused, err := q.PauseJob(j.ID, PauseJobRequest{Reason: "retry after storage repair"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.ResumeJob(j.ID, paused.PauseRevision); err != nil {
		t.Fatal(err)
	}
	next = check(t, q, a, nil).Job
	if next == nil || next.ID != j.ID || next.Attempt != 3 {
		t.Fatalf("explicit resume did not allow a fresh attempt: %+v", next)
	}
	refuse(next, false)
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatalf("resumed failure did not restore host hold: %+v", got)
	}
}
