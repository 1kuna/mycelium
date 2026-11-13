package myce

import (
	"testing"
	"time"
)

// A staging failure is host-local: all three retries once timed out on the same
// nearly full host. Retry on an untried eligible host first; once every eligible
// host has failed, the first one may try again.
func TestStagingFailureRetriesOnAnotherHost(t *testing.T) {
	q, clock := fixture(t)
	s := submitVerificationInput(t, q)
	ref := s.Files["input"]
	s.SharedInputs = map[string]SharedInput{"source": {SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	a, b := machine("staging-a", "fake"), machine("staging-b", "fake")
	a.Capabilities = append(a.Capabilities, "shared_inputs_v1")
	b.Capabilities = append(b.Capabilities, "shared_inputs_v1")
	check(t, q, a, nil)
	check(t, q, b, nil)
	j := submit(t, q, s)
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: a}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	fail := func(m Machine, attempt int) {
		t.Helper()
		got := check(t, q, m, nil).Job
		if got == nil || got.ID != j.ID || got.Attempt != attempt {
			t.Fatalf("attempt %d not assigned to %s: %+v", attempt, m.ID, got)
		}
		clock.Advance(time.Second)
		store.state.Job, store.state.Session = got, got.Session
		r := startFailureOutcome(result(got, false), w.stagingFailure(store, *got, withStagingCause("shared_inputs/source", ref, "shared_input", errCheckpointIOTimeout)))
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	fail(a, 1)
	if got := check(t, q, a, nil).Job; got != nil {
		t.Fatal("retry returned to the host where staging failed while another host fits", got.Attempt)
	}
	if got := job(t, q, j.ID); got.wait != nil && got.wait.Kind == stagingHostRetryHold {
		t.Fatal("job-level wait blames the avoided host", got.wait)
	}
	fail(b, 2)
	if got := check(t, q, a, nil).Job; got == nil || got.Attempt != 3 {
		t.Fatal("no untried host remains; the first host must retry", got)
	}
}
