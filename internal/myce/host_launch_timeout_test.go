package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// #181: a control-plane timeout is an infrastructure interruption, not evidence
// of a worker restart. Preserve its boundary and the existing retry charges.
func TestHostLaunchTimeoutAttemptDiagnostic(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	w := Worker{Machine: m, Client: Client{URL: "http://private.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("private transport detail: %w", context.DeadlineExceeded)
	})}}}
	err := w.authorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errWorkerStartAuthorization) {
		t.Fatal("authorization error lost identity", err)
	}
	facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: launchRefused, At: clock.Now()}}}
	early := startFailureOutcome(result(a, false), &recordedStartError{cause: err, facts: facts})
	if early.ExecutionCause == nil || early.ExecutionCause.Operation != "start_authorization" || early.ExecutionCause.Class != "timeout" {
		t.Errorf("pre-process launch refusal lost its cause: %+v", early)
	}
	r := classifyAttemptOutcome(result(a, false), attemptCause{memberOutcome: facts})
	r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("command", err))
	r.Error = err.Error()
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
	if h.Reason != "execution interrupted: start_authorization: timeout" || h.ExecutionCause == nil || h.ExecutionCause.Operation != "start_authorization" || h.ExecutionCause.Class != "timeout" {
		t.Errorf("control-plane cause mislabeled: %+v", h)
	}
	data, _ := json.Marshal(got)
	if strings.Contains(string(data), "private") || strings.Contains(string(data), "restarted") {
		t.Error("private text or invented restart in job diagnostics", string(data))
	}
	// The same worker can succeed next; no fresh-session check-in is involved.
	b := check(t, q, m, nil).Job
	if b == nil || b.Attempt != 2 || b.Session != a.Session {
		t.Fatal("same-session retry missing", b)
	}
	finish(t, q, b, true)
	if got = job(t, q, a.ID); got.State != "succeeded" || got.Losses != 1 || got.InfrastructureRetries != 1 {
		t.Fatal("retry did not complete", got)
	}
}

func TestHostLaunchAuthorizationControlBudget(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Millisecond, 2 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				heartbeat := max(interval, time.Second) // Same normal period as Worker.Run.
				w := Worker{Interval: interval, Client: Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					// A serialized queue burst lasting two heartbeats must fit.
					select {
					case <-time.After(2*heartbeat + heartbeat/2):
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"start_gate":%q}`, startGateProtocol)))}, nil
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				})}}}
				if err := w.authorizeLaunch(HostLaunch{}); err != nil {
					t.Fatal("authorization did not allow a short heartbeat burst", err)
				}
				w.Client.HTTP.Transport = clientTestTransport(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				})
				started := time.Now()
				err := w.authorizeLaunch(HostLaunch{})
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 10*time.Second {
					t.Fatal("hung authorization escaped its control-request budget", time.Since(started), err)
				}
			})
		})
	}
}
