package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrelaunchRefusalBoundaries(t *testing.T) {
	for _, want := range []string{assignmentWithdrawn, queueLaunchRefused, launchRPCStale} {
		t.Run(want, func(t *testing.T) {
			q, clock := fixture(t)
			var dbPath string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&dbPath); err != nil {
				t.Fatal(err)
			}
			m := machine("worker", "fake")
			submitted := submit(t, q, spec("experiment"))
			j := check(t, q, m, nil).Job
			server := httptest.NewServer(API{Queue: q, Token: "private-token"}.Handler())
			defer server.Close()
			w := recoveryWorker(t)
			w.Machine, w.Clock, w.Client = m, clock, Client{URL: server.URL, Token: "private-token"}
			w.control = &processControl{}
			root, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer root.close()
			store, err := newAttemptStore(root, *j)
			if err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			oldLog := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(oldLog)
			clock.Advance(time.Second)
			owner := result(j, false)
			switch want {
			case assignmentWithdrawn:
				a := &attemptSupervisor{store: store, job: *j, worker: w, connected: true, directive: AttemptDirective{Stop: true}, done: make(chan struct{})}
				ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
				defer cancel()
				a.run(ctx)
				if a.err != nil || !a.finished || !strings.Contains(logs.String(), "boundary="+want+" class=launch_refused") {
					t.Fatalf("worker did not release and log withdrawal: %v, %s", a.err, logs.String())
				}
			case queueLaunchRefused, launchRPCStale:
				if want == queueLaunchRefused {
					if err := q.update(func() error { q.touchMachine(m.ID).Leaving = true; return nil }); err != nil {
						t.Fatal(err)
					}
				} else {
					// Supersede the actual launch requester, preserving its original local custody.
					previous := owner
					previous.Interrupted = true
					previous.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "launch_authorization", PrelaunchRefused: true}
					if err := q.Complete(previous); err != nil {
						t.Fatal(err)
					}
					if replacement := check(t, q, m, nil).Job; replacement == nil || replacement.Attempt != j.Attempt+1 {
						t.Fatal("no newer attempt", replacement)
					}
				}
				launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)}
				rpcErr := w.authorizeLaunchContext(t.Context(), launch)
				if !stale(rpcErr) {
					t.Fatal("launch RPC accepted or changed response status", rpcErr)
				}
				// Raw errors can contain sensitive remote text. Only the typed boundary
				// and the existing allowlisted transport class reach history.
				err := store.startFailureAt(*j, launchRefused, fmt.Errorf("private-diagnostic: %w", rpcErr), clock.Now())
				owner.Error = err.Error()
				owner = startFailureOutcome(owner, &executionEvidenceError{err, unstartedExecution(j.Attempt)})
				if c := owner.ExecutionCause; c == nil || c.LaunchRefusal != want || c.Class != "admission_changed" || strings.Contains(c.String(), "private-diagnostic") {
					t.Fatalf("wrong or unsafe RPC cause: %+v", c)
				}
				if err := q.Complete(owner); (want == launchRPCStale && !errors.Is(err, ErrStale)) || (want == queueLaunchRefused && err != nil) {
					t.Fatal("wrong completion authority", err)
				}
			}
			owner.ReleaseOnly = true
			if err := q.Complete(owner); err != nil {
				t.Fatal("release replay", err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dbPath, q.root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got := job(t, reopened, submitted.ID)
			public, err := json.Marshal(statusJobCopy(got))
			if err != nil || strings.Contains(string(public)+logs.String(), "private-diagnostic") || strings.Contains(string(public)+logs.String(), "private-token") {
				t.Fatal("private refusal detail reached status or journal", err)
			}
			if got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 1 || !got.Copies[0].ReleaseConfirmed {
				t.Fatalf("changed accounting, history, or exact release: %+v", got)
			}
			a := got.AttemptHistory[0]
			if want == launchRPCStale {
				if got.Attempt != j.Attempt+1 || got.ReleaseConfirmed || got.State != "running" || a.ExecutionCause.LaunchRefusal != "" {
					t.Fatalf("stale result affected accepted work: %+v", got)
				}
			} else if a.ExecutionCause == nil || a.ExecutionCause.LaunchRefusal != want || a.ExecutionEvidence == nil || a.ExecutionEvidence.Main != executionNotStarted || !strings.Contains(a.ExecutionCause.String(), want) {
				t.Fatalf("lost history: %+v", a)
			}
		})
	}
}

// #379: run the real native preparation path, never start a payload, then
// acknowledge all three exact refusals and reopen the durable queue.
func TestAttemptHistoryLaunchRefusal(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	marker := filepath.Join(t.TempDir(), "must-not-start")
	spec := spec("experiment")
	spec.Resumable = false
	spec.Run.Workdir = t.TempDir()
	spec.Run.Command = []string{"sh", "-c", "touch " + marker}
	spec.Run.Env = map[string]string{"PRIVATE_VALUE": "private-secret"}
	submitted := submit(t, q, spec)
	var logs testLogBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	var first Result
	for attempt := 1; attempt <= 3; attempt++ {
		j := check(t, q, w.Machine, nil).Job
		if j == nil || j.Attempt != attempt {
			t.Fatalf("missing attempt %d: %+v", attempt, j)
		}
		p, launchErr := startAfterFixtureInputs(t, w, store, *j)
		if p != nil || launchErr == nil {
			t.Fatalf("expected refusal: %v %v", p, launchErr)
		}
		live := startFailureOutcome(result(j, false), launchErr)
		if live.ExecutionCause == nil || live.ExecutionCause.Predicate != "native_workdir_containment" {
			t.Fatalf("live refusal lost predicate: %+v", live)
		}
		// Recover using only durable facts, as after worker restart/acknowledgement.
		data, err := os.ReadFile(filepath.Join(store.dir, store.state.LaunchOutcome))
		if err != nil {
			t.Fatal(err)
		}
		var facts memberOutcome
		if err := json.Unmarshal(data, &facts); err != nil {
			t.Fatal(err)
		}
		r := classifyAttemptOutcome(result(j, false), attemptCause{memberOutcome: facts})
		if !r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != launchRefused || r.ExecutionCause.Predicate != "native_workdir_containment" {
			t.Fatalf("lost refusal: %+v", r)
		}
		if attempt == 1 {
			first = r
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(store.dir, store.state.LaunchOutcome)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Complete(first); !errors.Is(err, ErrStale) {
		t.Fatalf("accepted stale refusal: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("payload started: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "private-token"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "private-token"}
	got, err := client.GetJob(context.Background(), submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "queued" || got.Losses != 0 || got.Starts != 3 || got.StartedAt != nil || len(got.AttemptHistory) != 3 {
		t.Fatalf("changed accounting/history: %+v", got)
	}
	for i, a := range got.AttemptHistory {
		if a.Attempt != i+1 || a.ExecutionCause == nil || a.ExecutionCause.Predicate != "native_workdir_containment" || !strings.Contains(a.ExecutionCause.Reason, "workdir beneath {work}") || !strings.Contains(a.Reason, "workdir beneath {work}") {
			t.Fatalf("lost attempt reason: %+v", a)
		}
		if !strings.Contains(logs.String(), fmt.Sprintf("attempt=%d predicate=native_workdir_containment", i+1)) {
			t.Fatal("missing queue log", logs.String())
		}
	}
	ledger, err := client.JobLedger(context.Background(), submitted.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := 0
	for _, e := range ledger.Entries {
		if e.Kind == "outcome" {
			outcomes++
			if !strings.Contains(e.Reason, "workdir beneath {work}") {
				t.Fatal("ledger lost reason", e)
			}
		}
	}
	if outcomes != 3 {
		t.Fatal("missing ledger outcomes", outcomes)
	}
	var status Snapshot
	if err := client.Call(context.Background(), "GET", "/api/status?jobs=recent&sections=jobs", nil, &status); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(status)
	jobData, _ := json.Marshal(got)
	public := string(data) + string(jobData) + logs.String()
	for _, private := range []string{"private-secret", "private-token", spec.Run.Workdir, marker} {
		if strings.Contains(public, private) {
			t.Fatalf("disclosed %q", private)
		}
	}
	if !strings.Contains(string(data), "workdir beneath {work}") {
		t.Fatal("status lost refusal", string(data))
	}
}

// #807: storage recovers before a worker delivers its prelaunch refusal. The
// result describes the refused attempt, not the host's current health.
func TestLaunchRefusalAfterStorageRecovery(t *testing.T) {
	for _, prelaunchProof := range []bool{false, true} {
		t.Run(fmt.Sprintf("prelaunch_proof=%t", prelaunchProof), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			submitted := submit(t, q, spec("experiment"))
			var first Result
			for attempt := 1; attempt <= 3; attempt++ {
				a := check(t, q, m, nil).Job
				if a == nil || a.Attempt != attempt || a.StartedAt != nil {
					t.Fatalf("missing unstarted attempt %d: %+v", attempt, a)
				}
				m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "storage temporarily unavailable"}}
				check(t, q, m, a)
				if reason := q.storageRootReason(q.s.Jobs[a.ID], q.s.Machines[m.ID]); reason == "" {
					t.Fatal("storage fault did not withdraw admission")
				}
				r := result(a, false)
				r.Interrupted = true
				r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "launch_authorization", PrelaunchRefused: prelaunchProof}
				clock.Advance(time.Second)
				m.UnavailableRoots = nil
				check(t, q, m, a)
				if reason := q.storageRootReason(q.s.Jobs[a.ID], q.s.Machines[m.ID]); reason != "" {
					t.Fatal("storage did not recover", reason)
				}
				if attempt == 1 {
					first = r
				} else if err := q.Complete(first); !errors.Is(err, ErrStale) {
					t.Fatal("stale refusal touched the current attempt", err)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, submitted.ID)
				if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatalf("refusal %d charged a loss: state=%s losses=%d retries=%d", attempt, got.State, got.Losses, got.InfrastructureRetries)
				}
			}
			// A later execution really starts; an interruption must still count.
			a := check(t, q, m, nil).Job
			if a == nil || a.Attempt != 4 {
				t.Fatal("no fresh retry after three refusals", a)
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "launch_authorization"}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, submitted.ID); got.State != "queued" || got.Losses != 1 {
				t.Fatalf("post-Started interruption not charged: state=%s losses=%d", got.State, got.Losses)
			}
		})
	}
}

func TestAttemptHistoryLaunchPredicatePrivacy(t *testing.T) {
	for _, tc := range []struct {
		cause           error
		kind, predicate string
	}{
		{reject("device_identity_mismatch", errors.New("private-secret")), launchRefused, "device_identity_mismatch"},
		{reject("device_probe_unavailable", errors.New("private-secret")), launchRefused, "device_probe_unavailable"},
		{fmt.Errorf("private-secret: %w", errWorkerMemory), launchRefused, "memory_capacity"},
		{errors.New("private-secret"), environmentFailed, "environment_prepare"},
	} {
		w := recoveryWorker(t)
		s, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		cause := s.startFailure(Job{ID: ID(), Attempt: 1}, tc.kind, tc.cause)
		r := startFailureOutcome(Result{}, cause)
		s.close()
		if r.ExecutionCause == nil || r.ExecutionCause.Predicate != tc.predicate || strings.Contains(r.ExecutionCause.String(), "private-secret") {
			t.Fatalf("lost/unsafe predicate: %+v", r)
		}
	}
	for _, predicate := range []string{"native_workdir_containment", "private-secret"} {
		c := safeRuntimeCause(&RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: predicate, Reason: "private-secret"})
		data, _ := json.Marshal(c)
		if strings.Contains(string(data), "private-secret") {
			t.Fatal("worker supplied text leaked", string(data))
		}
	}
}
