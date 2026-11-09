package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMeasureSafetyNarrowReadIsFreshAndRetainsGuards(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		handler := API{Queue: q, Token: "test"}.Handler()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/measure/safety" {
				t.Errorf("wide or unexpected read: %s", r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			handler.ServeHTTP(w, r)
		}))
		defer server.Close()
		c := Client{URL: server.URL, Token: "test"}
		if err := c.measurementSafety(context.Background(), "local", "cuda"); err != nil {
			t.Fatal(err)
		}
		// A committed blocker after the first safe read must not use cached evidence.
		j := submit(t, q, spec("production"))
		if err := c.measurementSafety(context.Background(), "local", "cuda"); err == nil {
			t.Fatal("queued production was certified safe")
		}
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		if err := c.measurementSafety(context.Background(), "local", "cuda"); err != nil {
			t.Fatal(err)
		}

		for _, scenario := range []string{"release custody", "production call", "local call", "pin", "local model"} {
			t.Run(scenario, func(t *testing.T) {
				q.mu.Lock()
				switch scenario {
				case "release custody":
					job := q.s.Jobs[j.ID]
					job.Spec.Class = "production"
					job.Attempt = 1
					job.ReleaseConfirmed = false
					q.records.live[j.ID] = job
				case "production call":
					q.calls["call"] = &ModelCall{Class: "production", Machine: "other"}
				case "local call":
					q.calls["call"] = &ModelCall{Class: "experiment", Machine: "local"}
				case "pin":
					q.s.Pins = []ModelPin{{Platform: "cuda"}}
				case "local model":
					q.s.ModelSlots["local/model"] = &ModelSlot{Machine: "local", InFlight: 1}
				}
				q.mu.Unlock()
				if err := c.measurementSafety(context.Background(), "local", "cuda"); err == nil {
					t.Fatal("outstanding custody was certified safe")
				}
				q.mu.Lock()
				delete(q.records.live, j.ID)
				delete(q.calls, "call")
				q.s.Pins = nil
				delete(q.s.ModelSlots, "local/model")
				q.mu.Unlock()
			})
		}

	})

	t.Run("MeasureSafetyRemovedProductionCustodySurvivesReopen", func(t *testing.T) {
		q, clock := fixture(t)
		s := spec("production")
		s.SubmissionKey = "retained-production"
		j := submit(t, q, s)
		run := check(t, q, machine("worker", "fake"), nil).Job
		if run == nil {
			t.Fatal("production attempt not assigned")
		}
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		// Reproduce a terminal tombstone that still owes an exact copy release.
		// A removed nonterminal attempt must remain a blocker even after release.
		if err := q.update(func() error {
			q.finishCancellation(q.touchJob(j.ID))
			q.deleteJob(j.ID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		q = reopenCancellationQueue(t, q, clock)
		if q.s.Jobs[j.ID] != nil || q.records.removed[j.ID] == nil {
			t.Fatal("fixture did not reopen removed production custody")
		}
		assertSafety := func(safe bool) {
			t.Helper()
			got, err := q.measurementSafety(context.Background(), "local", "cuda")
			if err != nil || got.Safe != safe || !safe && got.Refusal == "" {
				t.Fatalf("safety=%+v err=%v, want safe=%t", got, err, safe)
			}
		}
		assertSafety(false)
		clock.Advance(RestartGrace + ProductionLossAfter)
		assertSafety(false) // Grace expiry cannot discharge exact copy custody.
		release := result(run, false)
		release.ReleaseOnly = true
		release.Session = "wrong-session"
		if err := q.Complete(release); err != nil {
			t.Fatal(err)
		}
		assertSafety(false)
		release.Session = run.Session
		if err := q.Complete(release); err != nil {
			t.Fatal(err)
		}
		assertSafety(true)
		q = reopenCancellationQueue(t, q, clock)
		assertSafety(true)

	})

	t.Run("MeasureSafetyTimeoutBackoffThenFreshAcknowledgement", func(t *testing.T) {
		o := measureTestOptions(t)
		clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		o.Worker.Clock = clock
		o.LoadTimeout = time.Minute
		reads := 0
		o.ProductionSafety = func(context.Context) error {
			reads++
			if reads < 3 {
				clock.Advance(2 * time.Second)
				return context.DeadlineExceeded
			}
			return nil
		}

		o.SafetyWait = func(_ context.Context, d time.Duration) error {

			clock.Advance(d)
			return nil
		}
		loaded := false
		o.NewEngine = func(Model, string, Clock) Engine {
			if reads != 3 {
				t.Fatal("model preparation started without fresh safety proof")
			}
			return &measurementEngine{load: func(context.Context, Model) error { loaded = true; return nil }}
		}
		if _, err := Measure(context.Background(), o); err != nil {
			t.Fatal(err)
		}
		if !loaded {
			t.Fatal("fresh safety proof did not allow model load")
		}
	})

	t.Run("MeasureSafetyNeverLoadsOnTimeoutRefusalOrCancellation", func(t *testing.T) {
		for _, scenario := range []string{"timeouts exhaust budget", "production after timeout", "cancel during backoff", "late safe reply"} {
			t.Run(scenario, func(t *testing.T) {
				o := measureTestOptions(t)
				clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
				o.Worker.Clock = clock
				o.LoadTimeout = 5 * time.Second
				started := clock.Now()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reads := 0
				o.ProductionSafety = func(context.Context) error {
					reads++
					if scenario == "late safe reply" {
						clock.Advance(o.LoadTimeout)
						return nil
					}
					if scenario == "production after timeout" && reads == 2 {
						return errors.New("production work outstanding; measurement refused")
					}
					clock.Advance(time.Second)
					return context.DeadlineExceeded
				}
				o.SafetyWait = func(ctx context.Context, d time.Duration) error {
					if scenario == "cancel during backoff" {
						cancel()
						return ctx.Err()
					}
					clock.Advance(d)
					return nil
				}
				o.ReadMemory = func(context.Context) (MemoryReading, error) {
					t.Fatal("sampled without safety proof")
					return MemoryReading{}, nil
				}
				o.Prepare = func(context.Context, Model, string) (Model, func(), error) {
					t.Fatal("prepared without safety proof")
					return Model{}, nil, nil
				}
				if report, err := Measure(ctx, o); err == nil || report.Fits {
					t.Fatalf("unsafe measurement accepted: %+v %v", report, err)
				}
				if clock.Now().Sub(started) > o.LoadTimeout {
					t.Fatal("retry extended the safety budget")
				}
				if scenario == "production after timeout" && reads != 2 || scenario == "cancel during backoff" && reads != 1 {
					t.Fatalf("refusal/cancellation retried: %d reads", reads)
				}
			})
		}

	})

	t.Run("MeasureSafetyTransportRefusalsNeverRetry", func(t *testing.T) {
		for _, body := range []string{"forbidden", `{}`, `{"safe":true,"refusal":"production"}`} {
			t.Run(body, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if body == "forbidden" {
						w.WriteHeader(http.StatusForbidden)
					} else if body == "old queue" {
						w.WriteHeader(http.StatusNotFound)
					}
					_, _ = w.Write([]byte(body))
				}))
				defer server.Close()
				o := measureTestOptions(t)
				o.ProductionSafety = nil
				o.Worker.Client = Client{URL: server.URL, Token: "test"}
				o.NewEngine = func(Model, string, Clock) Engine { t.Fatal("engine started after refused read"); return nil }
				o.SafetyWait = func(context.Context, time.Duration) error { t.Fatal("permanent failure retried"); return nil }
				if _, err := Measure(context.Background(), o); err == nil || !strings.Contains(err.Error(), "cannot establish production safety") {
					t.Fatalf("missing safety refusal: %v", err)
				}
			})
		}

	})

	t.Run("MeasureForceNeverBypassesJoinLock", func(t *testing.T) {
		o := measureTestOptions(t)
		s, e := o.Worker.openStore()
		if e != nil {
			t.Fatal(e)
		}
		defer s.close()
		o.Force = true
		if _, e = Measure(context.Background(), o); e == nil || !strings.Contains(e.Error(), "already in use") {
			t.Fatalf("lock error: %v", e)
		}

	})
}
