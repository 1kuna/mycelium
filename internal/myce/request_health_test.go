package myce

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Replaces the #768 fleet-count fixtures: #803 explicitly invalidates their
// fixed-rate expected outcomes. Healthy high volume must now remain healthy;
// cold history is unknown, not proof of overload. Retain lifecycle/HTTP proof.
func TestRequestHealthAdaptiveReplay(t *testing.T) {
	for _, hosts := range []int{2, 200} {
		for _, mode := range []string{"slow", "lock-wait", "long-poll"} {
			t.Run(fmt.Sprintf("%d/%s", hosts, mode), func(t *testing.T) {
				q, c := fixture(t)
				route := "POST /api/workers/{id}/checkin"
				if mode == "long-poll" {
					route = "GET /api/workers/{id}/events"
				}
				step := func(count int, cost, wait time.Duration) {
					for range count {
						q.requestHealth.recordRequest(c.Now(), route, cost, wait)
					}
					for range count {
						q.requestHealth.recordClient(c.Now(), "cli")
					}
					c.Advance(time.Duration(hosts) * time.Millisecond)
					q.applyRequestHealth(q.requestHealth.snapshot(c.Now()))
				}
				baseline := time.Duration(hosts) * time.Microsecond
				for range alertWarmup {
					step(hosts, baseline, 0)
				}
				if firings(q, "queue_overload") != 0 {
					t.Fatal("healthy baseline alerted")
				}
				cost, wait := baseline*10, time.Duration(0)
				if mode != "slow" {
					wait = baseline * 10
				}
				if mode == "long-poll" {
					cost = time.Hour
				}
				step(hosts*4, cost, wait)
				step(hosts, baseline, 0)
				if firings(q, "queue_overload") != 0 {
					t.Fatal("spike alerted")
				}
				for range alertConfirmation {
					step(hosts*4, cost, wait)
				}
				requireIncident(t, q, "queue_overload", "queue-overload", 1, true)
				requireIncident(t, q, "client_request_flood", "client-flood/cli", 1, true)
				for range alertHistorySize {
					step(hosts*4, cost, wait)
				}
				requireIncident(t, q, "queue_overload", "queue-overload", 1, true)
				for range alertConfirmation {
					step(hosts, baseline, 0)
				}
				requireIncident(t, q, "queue_overload", "queue-overload", 1, false)
				c.Advance(healthAlertCooldown)
				for range alertConfirmation {
					step(hosts*4, cost, wait)
				}
				requireIncident(t, q, "queue_overload", "queue-overload", 2, true)
				// Silence closes the episode without another HTTP request.
				c.Advance(time.Second)
				q.applyRequestHealth(q.requestHealth.snapshot(c.Now()))
				requireIncident(t, q, "queue_overload", "queue-overload", 2, false)
			})
		}
	}
}
func TestRequestHealthHTTPWiring(t *testing.T) {
	q, c := fixture(t)
	handler := (API{Queue: q, Token: "fixture"}).handler("", c)
	for range alertWarmup {
		r := httptest.NewRequest("GET", "/api/jobs/missing?view=wait", nil)
		r.Header.Set("Authorization", "Bearer fixture")
		r.Header.Set("User-Agent", "myce/test")
		handler.ServeHTTP(httptest.NewRecorder(), r)
		c.Advance(time.Second)
		q.requestHealth.snapshot(c.Now())
	}
	if len(q.requestHealth.rates[0]) != alertWarmup {
		t.Fatal("HTTP client attribution absent")
	}
	// Synthetic timing through real middleware; timing is observational wall
	// time, while interval/lifecycle decisions use the injected queue clock.
	measured := q.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "POST /api/workers/{id}/checkin"
		s := spanFrom(r.Context())
		s.started = s.started.Add(-time.Second)
		w.WriteHeader(http.StatusOK)
	}), map[string]struct{}{"POST /api/workers/{id}/checkin": {}})
	// First teach this route's independent healthy cost.
	for range alertWarmup {
		q.requestHealth.recordRequest(c.Now(), "POST /api/workers/{id}/checkin", time.Millisecond, 0)
		c.Advance(time.Second)
		q.requestHealth.snapshot(c.Now())
	}
	for range alertConfirmation {
		measured.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/workers/test/checkin", nil))
		c.Advance(time.Second)
		q.applyRequestHealth(q.requestHealth.snapshot(c.Now()))
	}
	requireIncident(t, q, "queue_overload", "queue-overload", 1, true)
}
