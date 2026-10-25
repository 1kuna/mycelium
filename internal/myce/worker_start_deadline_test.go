package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkerStartHandoffTimeoutHistory(t *testing.T) {
	for _, tc := range []struct{ path, operation string }{
		{"/api/workers/launch", "start_authorization"},
		{"/api/workers/started", "start_handoff"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			submit(t, q, spec("production"))
			a := check(t, q, m, nil).Job
			w := Worker{Machine: m, Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != tc.path {
					t.Errorf("wrong gate: got %s, want %s", r.URL.Path, tc.path)
				}
				return nil, context.DeadlineExceeded
			})}}}
			var err error
			if tc.path == "/api/workers/started" {
				err = w.authorizeStart(*a, m.Session)
			} else {
				err = w.authorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"})
			}
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errWorkerStartAuthorization) {
				t.Fatal("authorization family or transport cause lost", err)
			}
			facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: launchRefused, At: clock.Now()}}}
			// Both early and running-group adapters must retain the exact boundary.
			early := startFailureOutcome(result(a, false), &recordedStartError{cause: err, facts: facts})
			cause := runtimeDiagnostic(runtimeErrorFor("command", err))
			if early.ExecutionCause == nil || early.ExecutionCause.Operation != tc.operation || cause.Operation != tc.operation || cause.Class != "timeout" {
				t.Errorf("gates indistinguishable: early=%+v group=%+v", early.ExecutionCause, cause)
			}
			r := classifyAttemptOutcome(result(a, false), attemptCause{memberOutcome: facts})
			r.ExecutionCause = cause
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 || len(got.AttemptHistory) != 1 {
				t.Fatal("retry policy changed", got)
			}
			h := got.AttemptHistory[0]
			if h.ExecutionCause == nil || h.ExecutionCause.Operation != tc.operation || h.ExecutionCause.Class != "timeout" || h.Reason != fmt.Sprintf("execution interrupted: %s: timeout", tc.operation) {
				t.Errorf("wrong gate in public history: %+v", h)
			}
		})
	}
}
