package myce

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestStartWindowDependentFanOut(t *testing.T) {
	for _, tc := range []struct {
		name, priority, declaration string
		window                      time.Duration
		absolute                    bool
	}{
		{"relative", "production", "600s", 600 * time.Second, false},
		{"default", "production", "", ProductionStartTimeout, false},
		{"absolute-wins", "production", "600s", 1000 * time.Second, true},
		{"experiment", "experiment", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 1)
			s.Outputs = map[string]string{"out": "results"}
			producer := submit(t, q, s)
			s.Priority, s.StartWindow = tc.priority, tc.declaration
			s.After = &PieceDependency{Job: producer.ID, Outputs: map[string]string{"input": "item-0/out"}}
			cutoff := clock.Now().Add(tc.window)
			if tc.absolute {
				s.StartBy = &cutoff
			}
			consumer := submit(t, q, s)
			// Time blocked on the producer must not consume a relative budget.
			clock.Advance(601 * time.Second)
			wave := check(t, q, m, nil).Job
			if wave == nil || wave.Batch == nil || wave.Batch.Parent != producer.ID {
				t.Fatal("blocked consumer displaced producer")
			}
			if got := job(t, q, consumer.ID); got.State != "queued" || !tc.absolute && got.Spec.StartBy != nil {
				t.Fatalf("blocked deadline: %+v", got)
			}
			finish(t, q, wave, true)
			ready := job(t, q, consumer.ID)
			want := clock.Now().Add(tc.window)
			if tc.absolute {
				want = cutoff
			}
			if !dependencyReady(ready) || ready.StartWindow != tc.window || tc.window > 0 && (ready.Spec.StartBy == nil || !ready.Spec.StartBy.Equal(want)) {
				t.Fatalf("readiness did not preserve window: %+v", ready)
			}
			q = reopenCancellationQueue(t, q, clock)
			clock.Advance(5 * time.Second)
			child := check(t, q, m, nil).Job
			if child == nil || child.Batch == nil || child.Batch.Parent != consumer.ID {
				t.Fatal("no consumer wave")
			}
			if !tc.absolute {
				want = clock.Now().Add(tc.window)
			}
			if child.Spec.StartWindow != tc.declaration || child.StartWindow != tc.window || tc.window > 0 && (child.Spec.StartBy == nil || !child.Spec.StartBy.Equal(want)) {
				t.Fatalf("assignment changed window: %+v", child)
			}
			if tc.window == 0 {
				if child.Spec.StartBy != nil {
					t.Fatal("experiment acquired a start deadline")
				}
				return
			}
			// Started losses renew exactly the original length, even after restart
			// and a second loss; an unstarted retry does not renew again.
			for range 2 {
				if err := q.Started(result(child, true)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(10 * time.Second)
				r := result(child, false)
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				recovery := job(t, q, child.ID)
				if recovery.State != "queued" || recovery.StartWindow != tc.window || !recovery.Spec.StartBy.Equal(clock.Now().Add(tc.window)) {
					t.Fatalf("loss changed original window: %+v", recovery)
				}
				q = reopenCancellationQueue(t, q, clock)
				child = check(t, q, m, nil).Job
				if child == nil || child.ID != recovery.ID {
					t.Fatal("recovery was not reassigned")
				}
			}
			clock.Advance(tc.window)
			if got := job(t, q, child.ID); got.State != "failed" {
				t.Fatalf("unstarted retry renewed deadline: %+v", got)
			}
		})
	}
}

func TestStartWindowSubmissionAndGET(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.StartWindow = "600s"
	j := submit(t, q, s)
	if !j.Spec.StartBy.Equal(clock.Now().Add(600 * time.Second)) {
		t.Fatal("relative submission cutoff", j.Spec.StartBy)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil)
	request.Header.Set("Authorization", "Bearer test")
	reply := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(reply, request)
	var got Job
	if err := json.Unmarshal(reply.Body.Bytes(), &got); err != nil || reply.Code != http.StatusOK {
		t.Fatalf("GET: %d %s: %v", reply.Code, reply.Body.String(), err)
	}
	if got.Spec.StartWindow != "600s" || got.StartWindow != 600*time.Second || !got.Spec.StartBy.Equal(*j.Spec.StartBy) {
		t.Fatalf("GET lost effective window: %+v", got)
	}
}

func TestStartWindowFanOutLaterWave(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(map[bool]string{false: "relative", true: "absolute"}[absolute], func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 2)
			s.Priority, s.StartWindow = "production", "600s"
			s.FanOut.MaxConcurrency = 1
			cutoff := clock.Now().Add(300 * time.Second)
			if absolute {
				s.StartBy = &cutoff
			}
			parent := submit(t, q, s)
			first := check(t, q, m, nil).Job
			if first == nil || first.Batch == nil || len(first.Batch.Pieces) != 1 {
				t.Fatal("expected first single-piece wave")
			}
			if err := q.Started(result(first, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(601 * time.Second)
			finish(t, q, first, true)
			next := check(t, q, m, nil).Job
			if absolute {
				if got := job(t, q, parent.ID); got.State != "failed" || next != nil {
					t.Fatalf("expired absolute tail dispatched: %+v %+v", got, next)
				}
			} else if next == nil || next.StartWindow != 600*time.Second || !next.Spec.StartBy.Equal(clock.Now().Add(600*time.Second)) {
				t.Fatalf("later wave lost its full relative budget: %+v", next)
			}
		})
	}
}

func TestStartWindowValidation(t *testing.T) {
	for _, value := range []string{"-1s", "0s", "1h1ns", "600", "forever", "999999999999999999999h"} {
		t.Run(value, func(t *testing.T) {
			s := spec("production")
			s.StartWindow = value
			if err := s.Validate(); err == nil {
				t.Fatal("invalid window accepted")
			}
		})
	}
	s := spec("experiment")
	s.StartWindow = "600s"
	if err := s.Validate(); err == nil {
		t.Fatal("experiment window accepted")
	}
	s.Priority = "production"
	for _, value := range []string{"1ns", "600s", "1h"} {
		if err := yaml.Unmarshal([]byte("start_window: "+value), &s); err != nil {
			t.Fatal(err)
		}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
