package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
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
