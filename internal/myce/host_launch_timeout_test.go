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

// #1028 supersedes #181's charged transport-timeout expectation: no refusal or
// worker restart is established. Reconcile on the same session and attempt.
func TestHostLaunchAmbiguousTimeoutReconciles(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		m := machine("one", "fake")
		submit(t, q, spec("production"))
		a := check(t, q, m, nil).Job
		handler := (API{Queue: q, Token: "test"}).Handler()
		calls := 0
		identity := HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: "1"}
		w := Worker{Machine: m, Clock: clock, Client: Client{URL: "http://private.invalid", Token: "test", Pause: func(_ context.Context, wait time.Duration) error {
			clock.Advance(wait)
			return nil
		}, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/workers/launch" {
				calls++
				var got HostLaunch
				body, err := io.ReadAll(r.Body)
				if err != nil || json.Unmarshal(body, &got) != nil || got.Machine != identity.Machine || got.Session != identity.Session || got.Key != identity.Key || got.Attempt != identity.Attempt {
					t.Fatal("launch retry changed exact identity", got, err)
				}
				r.Body = io.NopCloser(strings.NewReader(string(body)))
				if calls == 1 {
					return nil, fmt.Errorf("private transport detail: %w", context.DeadlineExceeded)
				}
			}
			rw := httptest.NewRecorder()
			handler.ServeHTTP(rw, r)
			return rw.Result(), nil
		})}}}
		if err := w.authorizeLaunch(identity); err != nil {
			t.Fatal("ambiguous launch timeout was treated as refusal", err)
		}
		if err := w.authorizeStart(*a, a.Session); err != nil {
			t.Fatal(err)
		}
		finish(t, q, a, true)
		got := job(t, q, a.ID)
		if got.State != "succeeded" || got.Attempt != a.Attempt || got.Session != a.Session || got.Starts != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 1 {
			t.Fatal("control retry changed execution or charged loss", got)
		}
		data, _ := json.Marshal(got)
		if strings.Contains(string(data), "private") || strings.Contains(string(data), "restarted") {
			t.Error("private text or invented restart in job diagnostics", string(data))
		}

	})

	t.Run("HostLaunchAuthorizationControlBudget", func(t *testing.T) {
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
					ctx, cancel := context.WithDeadline(context.Background(), started.Add(45*time.Second))
					defer cancel()
					w.launchCtx = ctx
					err := w.authorizeLaunch(HostLaunch{})
					if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 45*time.Second {
						t.Fatal("hung authorization did not retain the owner's deadline", time.Since(started), err)
					}
				})
			})
		}

	})
}
