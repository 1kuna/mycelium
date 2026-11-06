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
				if len(alertsOf(q, "queue_overload")) != 0 {
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
				if len(alertsOf(q, "queue_overload")) != 0 {
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
func TestRequestHealthHealthyHistory(t *testing.T) {
	for _, count := range []int{2, 2000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var h requestHealth
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			for n := range alertHistorySize * 2 {
				// Baseline shifts smoothly with service cost and request volume. There
				// is no absolute latency/rate ceiling; long-poll duration never counts.
				for range count + n {
					h.recordRequest(now, "POST /api/workers/{id}/checkin", time.Duration(100+n)*time.Millisecond, 0)
					h.recordRequest(now, "POST /api/wait", time.Hour, 0)
					h.recordClient(now, "cli")
				}
				now = now.Add(time.Second)
				s := h.snapshot(now)
				if s.overload || s.clients[0] {
					t.Fatalf("healthy shifting baseline alerted at %d", n)
				}
			}
			if len(h.rates[0]) != alertHistorySize {
				t.Fatal("unbounded baseline")
			}
			_, hi := h.routes["POST /api/workers/{id}/checkin"].duration.band()
			if hi < float64(220*time.Millisecond) {
				t.Fatal("baseline failed to adapt")
			}
		})
	}
}
func TestRequestHealthClientHealthyExclusions(t *testing.T) {
	for _, balanced := range []bool{false, true} {
		t.Run(fmt.Sprint(balanced), func(t *testing.T) {
			var h requestHealth
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			step := func(count int, cost time.Duration) requestHealthSnapshot {
				for range count {
					h.recordRequest(now, "GET /api/jobs/{id}", cost, 0)
					h.recordClient(now, "cli")
					if balanced {
						h.recordClient(now, "python")
					}
				}
				now = now.Add(time.Second)
				return h.snapshot(now)
			}
			for range alertWarmup {
				step(10, time.Millisecond)
			}
			// Healthy high volume is not a flood even with sole-client dominance.
			for range alertHistorySize {
				if step(2000, time.Millisecond).clients[0] {
					t.Fatal("healthy volume alerted")
				}
			}
			for range alertConfirmation {
				s := step(8000, time.Second)
				if balanced && (s.clients[0] || s.clients[1]) {
					t.Fatal("proportional mix blamed a family")
				}
			}
		})
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
