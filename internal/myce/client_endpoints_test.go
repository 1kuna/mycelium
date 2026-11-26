package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultiEndpointClientReservesDialTimeForFallback(t *testing.T) {
	const primary, alternate = "https://primary.invalid", "https://alternate.invalid"
	client, err := NewMultiEndpointClient(primary+","+alternate, proofToken)
	if err != nil {
		t.Fatal(err)
	}
	const timeout = 4 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var calls int
	client.HTTP.Transport = proofRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host == "primary.invalid" {
			budget, ok := r.Context().Value(queueEndpointDialBudget{}).(time.Duration)
			if !ok || budget <= 0 || budget > timeout/2 {
				t.Fatalf("first origin dial budget = %v", budget)
			}
			return nil, &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
		}
		if r.URL.Host != "alternate.invalid" {
			t.Fatalf("unexpected origin %q", r.URL.Host)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	// This write can fail over only because the error happened during dial.
	if err := client.Call(ctx, http.MethodPost, "/api/jobs/example/cancel", nil, nil); err != nil || calls != 2 {
		t.Fatalf("calls=%d, err=%v", calls, err)
	}
}

func TestMultiEndpointClientReplayAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		status             int
		body               any
		replayed           bool
	}{
		{"read", http.MethodGet, "/api/jobs/example", 503, nil, true},
		{"wait", http.MethodPost, "/api/wait", 503, WaitRequest{}, true},
		{"keyed submit", http.MethodPost, "/api/jobs", 503, Spec{SubmissionKey: "same-intent"}, true},
		{"unkeyed submit", http.MethodPost, "/api/jobs", 503, Spec{}, false},
		{"unconfirmed cancel", http.MethodPost, "/api/jobs/example/cancel", 503, nil, false},
		{"credential refused", http.MethodGet, "/api/jobs/example", 401, nil, false},
		{"redirect refused", http.MethodGet, "/api/jobs/example", 302, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != tc.path || r.Method != tc.method || r.Header.Get("Authorization") != "Bearer "+proofToken {
					t.Error("fallback did not preserve the method, path and fleet credential")
				}
				_, _ = fmt.Fprint(w, "{}")
			}))
			defer alternate.Close()
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", alternate.URL+tc.path)
				w.WriteHeader(tc.status)
			}))
			defer primary.Close()
			client, err := NewMultiEndpointClient(primary.URL+","+alternate.URL, proofToken)
			if err != nil {
				t.Fatal(err)
			}
			defer client.HTTP.CloseIdleConnections()
			err = client.WithoutRetries().Call(t.Context(), tc.method, tc.path, tc.body, nil)
			if tc.replayed {
				if err != nil || calls.Load() != 1 {
					t.Fatalf("fallback calls=%d, err=%v", calls.Load(), err)
				}
				return
			}
			var refused *HTTPError
			if !errors.As(err, &refused) || refused.Status != tc.status || calls.Load() != 0 {
				t.Fatalf("fallback calls=%d, err=%v", calls.Load(), err)
			}
		})
	}
}
