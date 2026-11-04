package myce

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequestHealthOverloadReplay(t *testing.T) {
	for _, mode := range []string{"slow", "lock-wait"} {
		t.Run(mode, func(t *testing.T) {
			q, c := fixture(t)
			record := func() {
				for range 1900 {
					duration, wait := 2*time.Second, time.Duration(0)
					route := "GET /api/jobs/{id}"
					if mode == "lock-wait" {
						duration, wait, route = 15*time.Second, 15*time.Second, "GET /api/workers/{id}/events"
					}
					q.requestHealth.recordRequest(c.Now(), route, duration, wait)
				}
			}
			tick := func() {
				t.Helper()
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
			}
			// A full incident minute followed by a healthy minute is a brief spike.
			record()
			c.Advance(time.Minute)
			tick()
			c.Advance(time.Minute)
			tick()
			if len(alertsOf(q, "queue_overload")) != 0 {
				t.Fatal("brief spike alerted")
			}
			record()
			c.Advance(time.Minute)
			record()
			c.Advance(time.Minute)
			tick()
			if len(alertsOf(q, "queue_overload")) != 1 {
				t.Fatal("sustained overload did not alert")
			}
			tick()
			if len(alertsOf(q, "queue_overload")) != 1 {
				t.Fatal("duplicate alert")
			}
			// Continuous pressure stays one episode even beyond the cooldown.
			for range 16 {
				record()
				c.Advance(time.Minute)
				tick()
			}
			if len(alertsOf(q, "queue_overload")) != 1 {
				t.Fatal("continuous overload repeated")
			}
			c.Advance(time.Minute)
			tick()
			if q.s.Health.Conditions["queue-overload"].Active {
				t.Fatal("recovery did not clear")
			}
			record()
			c.Advance(time.Minute)
			record()
			c.Advance(time.Minute)
			tick()
			if len(alertsOf(q, "queue_overload")) != 2 {
				t.Fatal("recovery did not rearm")
			}
		})
	}
}

func TestRequestHealthHealthyHistory(t *testing.T) {
	q, c := fixture(t)
	for range 4 {
		for range 1900 {
			q.requestHealth.recordRequest(c.Now(), "GET /api/workers/{id}/events", 5*time.Second, 0)
		}
		// Healthy history was ~0.4 slow/min; even 99/min is below this detector.
		for range 99 {
			q.requestHealth.recordRequest(c.Now(), "POST /api/workers/{id}/checkin", 2*time.Second, 0)
		}
		for range 19 {
			q.requestHealth.recordRequest(c.Now(), "GET /api/jobs/{id}", time.Second, time.Second)
		}
		c.Advance(time.Minute)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if len(alertsOf(q, "queue_overload")) != 0 {
		t.Fatal("healthy history alerted")
	}
}

func TestRequestHealthClientFloodReplay(t *testing.T) {
	for _, agent := range []string{"myce/s072", "mycelium-python/0.1.0", "myce/private/path?secret=value"} {
		t.Run(jobGetClient(agent).Kind, func(t *testing.T) {
			q, c := fixture(t)
			v := newJobGetVolume(c)
			v.observe = q.requestHealth.recordClient
			kind := jobGetClient(agent).Kind
			key := "client-flood/" + kind
			record := func(n int) {
				for i := 0; i < n; i++ {
					v.record(agent, i%2 == 0)
				}
			}
			tick := func() {
				t.Helper()
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
			}
			record(3800)
			c.Advance(time.Minute)
			tick()
			c.Advance(time.Minute)
			tick()
			if len(alertsOf(q, "client_request_flood")) != 0 {
				t.Fatal("brief flood alerted")
			}
			for range 2 {
				record(3800)
				c.Advance(time.Minute)
			}
			tick()
			tick()
			events := alertsOf(q, "client_request_flood")
			if len(events) != 1 || events[0].ClientKind != kind {
				t.Fatalf("missing sanitized/deduplicated attribution: %+v", events)
			}
			for range 16 {
				record(3800)
				c.Advance(time.Minute)
				tick()
			}
			if len(alertsOf(q, "client_request_flood")) != 1 {
				t.Fatal("continuous flood repeated")
			}
			c.Advance(time.Minute)
			tick()
			if q.s.Health.Conditions[key].Active {
				t.Fatal("quiet recovery did not clear")
			}
			for range 2 {
				record(3800)
				c.Advance(time.Minute)
			}
			tick()
			if len(alertsOf(q, "client_request_flood")) != 2 {
				t.Fatal("flood did not rearm")
			}
		})
	}
}

func TestRequestHealthClientHealthyExclusions(t *testing.T) {
	for _, mode := range []string{"normal-volume", "balanced", "split-versions"} {
		t.Run(mode, func(t *testing.T) {
			q, c := fixture(t)
			v := newJobGetVolume(c)
			v.observe = q.requestHealth.recordClient
			for range 2 {
				n := 1702 // Recorded 28.36/s job GET baseline, even if all one kind.
				if mode != "normal-volume" {
					n = 3800
				}
				for i := 0; i < n; i++ {
					agent := "myce/s072"
					if mode == "balanced" && i%2 == 0 {
						agent = "mycelium-python/0.1.0"
					}
					if mode == "split-versions" && i%2 == 0 {
						agent = "myce/s071"
					}
					v.record(agent, true)
				}
				c.Advance(time.Minute)
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			got := len(alertsOf(q, "client_request_flood"))
			if mode == "split-versions" && got != 1 {
				t.Fatal("split versions hid dominant kind")
			}
			if mode != "split-versions" && got != 0 {
				t.Fatal("healthy volume/share alerted")
			}
		})
	}
}

func TestRequestHealthHTTPWiring(t *testing.T) {
	q, c := fixture(t)
	handler := (API{Queue: q, Token: "fixture"}).Handler()
	for range 2 {
		for range requestHealthClientPerMinute {
			r := httptest.NewRequest("GET", "/api/jobs/missing?view=wait", nil)
			r.Header.Set("Authorization", "Bearer fixture")
			r.Header.Set("User-Agent", "myce/s072")
			handler.ServeHTTP(httptest.NewRecorder(), r)
		}
		c.Advance(time.Minute)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "client_request_flood")) != 1 {
		t.Fatal("HTTP attribution was not observed")
	}
	// Drive the real timing middleware with a synthetic observed span; no sleeps.
	measured := q.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "GET /api/jobs/{id}"
		s := spanFrom(r.Context())
		s.started = s.started.Add(-2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}), map[string]struct{}{"GET /api/jobs/{id}": {}})
	for range 2 {
		for range requestHealthSlowPerMinute {
			measured.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/jobs/missing", nil))
		}
		c.Advance(time.Minute)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "queue_overload")) != 1 {
		t.Fatal("HTTP timings were not observed")
	}
}

func TestRequestHealthPartialMinuteExcluded(t *testing.T) {
	q, c := fixture(t)
	c.Advance(59 * time.Second)
	for range 2 {
		for range 1900 {
			q.requestHealth.recordRequest(c.Now(), "GET /api/jobs/{id}", 2*time.Second, 0)
		}
		c.Advance(time.Minute)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(alertsOf(q, "queue_overload")) != 0 {
		t.Fatal("initial partial minute counted as sustained")
	}
}
