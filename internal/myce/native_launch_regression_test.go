package myce

import (
	"errors"
	"net"
	"net/http"
	"testing"
)

func TestNativeReservationSurvivesAvailabilityRetry(t *testing.T) {
	for _, group := range []bool{false, true} {
		name := "simple"
		if group {
			name = "group"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			s := spec("experiment")
			s.Resumable = false
			s.Needs.MemoryGB, s.Needs.CPUCores, s.Needs.HostMemoryGB = 0, 1, 2
			if group {
				s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"/bin/sleep", "60"}, MemoryGB: 1}}
			}
			submit(t, q, s)
			a := check(t, q, m, nil).Job
			if a == nil {
				t.Fatal("no assignment")
			}
			w := recoveryWorker(t)
			w.Clock, w.Machine, w.compactNativeLaunch = clock, m, true
			w.availability = newAvailabilityMonitor(clock, false, false, false)
			defer w.availability.cancel()
			setAvailable := func(available bool) {
				w.availability.mu.Lock()
				defer w.availability.mu.Unlock()
				w.availability.current.Available = available
			}
			api := (API{Queue: q, Token: "test"}).Handler()
			withdrawn := false
			w.Client = nativeFixtureClient(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				api.ServeHTTP(rw, r)
				if r.URL.Path == "/api/workers/launch" && !withdrawn {
					withdrawn = true
					setAvailable(false)
				}
			}))
			w.Client.Token = "test"
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = a.Session
			p, err := startAfterFixtureInputs(t, w, store, *a)
			if p != nil || !errors.Is(err, errWorkerUnavailable) {
				t.Fatalf("first start = %v, %v", p, err)
			}
			first := job(t, q, a.ID)
			reserved := first.Copies[0].ProcessID
			if reserved == "" || first.StartedAt != nil || store.state.PID != 0 {
				t.Fatalf("expected a durable reservation before fork, got process %q, Started %v, PID %d", reserved, first.StartedAt, store.state.PID)
			}
			setAvailable(true)
			p, err = startAfterFixtureInputs(t, w, store, *a)
			if p == nil || err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			f := <-p.done
			if f.err != nil || !f.result.Success || f.result.ReleasePending {
				t.Fatalf("retry completion = %+v, %v", f.result, f.err)
			}
			if err := q.Complete(f.result); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, a.ID)
			counts := nativeLedgerCounts(got.LedgerEntries)
			if got.Attempt != a.Attempt || got.State != "succeeded" || got.Losses != 0 || got.InfrastructureRetries != 0 ||
				got.Copies[0].ProcessID != reserved || !got.Copies[0].ReleaseConfirmed || !got.Ledger.Complete ||
				counts["custody_reserved"] != 1 || counts["custody_started"] != 1 || counts["custody_released"] != 1 {
				t.Fatalf("retry changed custody or accounting: copy=%+v losses=%d retries=%d ledger=%+v counts=%v", got.Copies[0], got.Losses, got.InfrastructureRetries, got.Ledger, counts)
			}
		})
	}
}

func TestNativeReservationRefusalDiagnostic(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	s := spec("experiment")
	s.Resumable = false
	submit(t, q, s)
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("no assignment")
	}
	if err := q.update(func() error { q.touchMachine(m.ID).Leaving = true; return nil }); err != nil {
		t.Fatal(err)
	}
	w := recoveryWorker(t)
	w.Clock, w.Machine, w.compactNativeLaunch = clock, m, true
	w.Client = nativeFixtureClient((API{Queue: q, Token: "test"}).Handler())
	w.Client.Token = "test"
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = a.Session
	p, err := startAfterFixtureInputs(t, w, store, *a)
	if p != nil || !stale(err) {
		t.Fatalf("start = %v, %v", p, err)
	}
	r := startFailureOutcome(result(a, false), err)
	if r.ExecutionCause == nil || r.ExecutionCause.LaunchRefusal != queueLaunchRefused {
		t.Fatalf("missing refusal boundary: %+v", r.ExecutionCause)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, a.ID)
	if len(got.AttemptHistory) != 1 || got.AttemptHistory[0].ExecutionCause == nil ||
		got.AttemptHistory[0].ExecutionCause.LaunchRefusal != queueLaunchRefused ||
		got.Losses != 0 || got.InfrastructureRetries != 0 || !got.Copies[0].ReleaseConfirmed {
		t.Fatalf("wrong refusal history or accounting: %+v", got.AttemptHistory)
	}
}

func TestNativeLedgerCoverageBeforeFirstMemberGate(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name := "legacy"
		if compact {
			name = "combined"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			port := listener.Addr().(*net.TCPAddr).Port
			q, clock := fixture(t)
			m := machine("worker", "fake")
			s := spec("experiment")
			s.Resumable = false
			s.Needs.MemoryGB, s.Needs.CPUCores, s.Needs.HostMemoryGB = 0, 1, 2
			s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"/bin/sleep", "60"}, Port: &port, MemoryGB: 1}}
			submit(t, q, s)
			a := check(t, q, m, nil).Job
			if a == nil {
				t.Fatal("no assignment")
			}
			w := recoveryWorker(t)
			w.Clock, w.Machine, w.compactNativeLaunch = clock, m, compact
			w.Client = nativeFixtureClient((API{Queue: q, Token: "test"}).Handler())
			w.Client.Token = "test"
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = a.Session
			p, err := startAfterFixtureInputs(t, w, store, *a)
			if p == nil || err != nil {
				t.Fatalf("start = %v, %v", p, err)
			}
			f := <-p.done
			e := f.result.ExecutionEvidence
			if f.err != nil || f.result.Success || f.result.ReleasePending || e == nil ||
				e.Main != executionNotStarted || e.Sidecars != executionNotStarted {
				t.Fatalf("expected pre-gate failure and verified cleanup: %+v, %v", f.result, f.err)
			}
			// Replacement releases the first supervisor before the attempt result.
			before, err := q.JobLedger(a.ID, 0, -1, 256)
			if err != nil {
				t.Fatal(err)
			}
			if err := q.Complete(f.result); err != nil {
				t.Fatal(err)
			}
			after, err := q.JobLedger(a.ID, 0, -1, 256)
			if err != nil {
				t.Fatal(err)
			}
			wantStarted := nativeBindAttempts
			if compact {
				wantStarted = 0
			}
			counts := nativeLedgerCounts(after.Entries)
			if before.Complete == compact || after.Complete == compact || counts["custody_reserved"] != nativeBindAttempts ||
				counts["custody_started"] != wantStarted || counts["custody_released"] != nativeBindAttempts || counts["started"] != 0 {
				t.Fatalf("pre-gate ledger coverage before result=%t after result=%t counts=%v", before.Complete, after.Complete, counts)
			}
		})
	}
}

func nativeLedgerCounts(entries []JobLedgerEntry) map[string]int {
	counts := map[string]int{}
	for _, entry := range entries {
		counts[entry.Kind]++
	}
	return counts
}
