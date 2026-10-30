package myce

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoiningProbeIndependentOfLargeStatus(t *testing.T) {
	q, clock := fixture(t)
	// A retained status generation the size observed in the incident.
	payload := `{"padding":"` + strings.Repeat("x", 15<<20) + `"}`
	q.statusCache.data = payload
	q.statusCache.expires = clock.Now().Add(time.Hour)
	api := API{Queue: q, Token: "throwaway-test-only"}.HandlerWithJoiningIdentity("test-pin")
	var statusCalls atomic.Int32
	server, invitation := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			statusCalls.Add(1)
		}
		api.ServeHTTP(w, r)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := ResolveJoining(ctx, invitation, "", &fakeDiscovery{})
	if err != nil || client.URL != server.URL {
		t.Fatalf("joining new queue: %v", err)
	}
	defer client.HTTP.CloseIdleConnections()
	if statusCalls.Load() != 0 {
		t.Fatal("joining fetched the large status snapshot")
	}

	// An unchanged worker's original probe still works against the new queue.
	if err := client.Call(context.Background(), http.MethodGet, "/api/status", nil, nil); err != nil {
		t.Fatal(err)
	}
	if statusCalls.Load() != 1 {
		t.Fatal("old worker status route unavailable")
	}
	for _, token := range []string{"", "wrong", invitation.Token} {
		r := httptest.NewRequest(http.MethodGet, "/api/ping", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		api.ServeHTTP(w, r)
		if token != invitation.Token {
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated ping returned %d", w.Code)
			}
			continue
		}
		var ping struct {
			Version     int    `json:"version"`
			Fingerprint string `json:"fingerprint"`
		}
		if w.Code != http.StatusOK || w.Body.Len() > 256 || json.Unmarshal(w.Body.Bytes(), &ping) != nil || ping.Version != 1 || ping.Fingerprint != "test-pin" {
			t.Fatalf("ping: status=%d body=%s", w.Code, w.Body)
		}
	}
}

func TestJoiningProbeOldQueueNeverDrainsLargeStatus(t *testing.T) {
	payload := `{"padding":"` + strings.Repeat("x", 15<<20) + `"}`
	var pings, statuses atomic.Int32
	server, invitation := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer throwaway-test-only" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/api/ping" {
			pings.Add(1)
			http.NotFound(w, r)
			return
		}
		statuses.Add(1)
		// A healthy old queue may spend more than the former one-second
		// deadline generating headers. This is transport time, not queue time.
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, payload)
		w.(http.Flusher).Flush()
		// Withhold EOF: draining this body cannot succeed within any budget.
		<-r.Context().Done()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, err := ResolveJoining(ctx, invitation, "", &fakeDiscovery{})
	if err != nil || client.URL != server.URL {
		t.Fatalf("joining old queue: %v", err)
	}
	defer client.HTTP.CloseIdleConnections()
	if pings.Load() != 1 || statuses.Load() != 1 {
		t.Fatalf("ping=%d status=%d", pings.Load(), statuses.Load())
	}
}
