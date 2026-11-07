package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestWaitActionableBlockerThenQuietSuccess(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := &myce.Blocker{Code: "waiting_for_builder", Dependency: "environment tools", Owner: "queue", Action: "wait for the offered builder", Evidence: "no builder report"}
	var done atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		j := myce.Job{ID: "cpu-job", State: "queued", Blocker: b}
		if done.Load() {
			j.State, j.Blocker = "succeeded", nil
			j.DeadlineGuidance = &myce.DeadlineGuidance{Example: "deadline tutorial"}
		}
		json.NewEncoder(w).Encode(j)
	}))
	defer server.Close()
	var out, diagnostics bytes.Buffer
	err := waitJobs(context.Background(), myce.Client{URL: server.URL}, []string{"cpu-job"}, time.Minute, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); done.Store(true); return nil })
	if err != nil || out.String() != "cpu-job succeeded\n" || diagnostics.String() != "cpu-job queued\n" || strings.Contains(diagnostics.String(), "tutorial") || strings.Contains(diagnostics.String(), "Blockers: .") {
		t.Fatalf("%v stdout=%q stderr=%q", err, &out, &diagnostics)
	}
}
