package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAttemptProtocolLostAssignmentStop(t *testing.T) {
	for _, planned := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "P3-planned-stop"}[planned], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("lost-assignment", "fake")
			if planned {
				if err := q.ConfigureHosts(map[string]HostPolicy{m.ID: {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
			}
			acknowledgements := make(chan Result, 8)
			api := API{q, "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/workers/complete" {
					var ack Result
					if err := json.NewDecoder(r.Body).Decode(&ack); err != nil {
						t.Error(err)
					}
					acknowledgements <- ack
					respondOperation(rw, nil, q.Complete(ack), "worker_result_invalid")
					return
				}
				api.ServeHTTP(rw, r)
			}))
			defer server.Close()
			w := Worker{Clock: clock, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
			victim := submit(t, q, spec("experiment"))
			// The queue commits the assignment, but the worker never receives it.
			lost := attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}).Attempts[0].Job
			if lost == nil || lost.ID != victim.ID {
				t.Fatal("missing initial assignment")
			}
			var next *Job
			if planned {
				demand := spec("production")
				deadline := clock.Now().Add(5 * time.Second)
				demand.StartBy = &deadline
				next = submit(t, q, demand)
			} else {
				if err := q.Cancel(victim.ID); err != nil {
					t.Fatal(err)
				}
				next = submit(t, q, spec("experiment"))
			}
			requireState(t, q, victim.ID, "stopping")
			// No grace/expiry advance: an unheld key can be released immediately.
			if reply, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil || reply.Stop {
				t.Fatalf("unheld stop affected worker: %+v %v", reply, err)
			}
			if len(acknowledgements) != 1 {
				t.Fatalf("unheld stop got %d exact release acknowledgements; want 1", len(acknowledgements))
			}
			ack := <-acknowledgements
			if !ack.ReleaseOnly || ack.ReleasePending || ack.Success || ack.ID != lost.ID || ack.Attempt != lost.Attempt || ack.Session != m.Session || ack.Machine != m.ID {
				t.Fatalf("not an exact release-only acknowledgement: %+v", ack)
			}
			want := "cancelled"
			if planned {
				want = "queued"
			}
			got := job(t, q, victim.ID)
			if got.State != want || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("unheld stop not promptly released: %+v", got)
			}
			reply, err := w.checkin(context.Background(), Checkin{Machine: m})
			if err != nil || reply.Job == nil || reply.Job.ID != next.ID {
				t.Fatalf("next job did not place promptly: %+v %v", reply, err)
			}
			if planned {
				if err := q.Started(result(reply.Job, false)); err != nil {
					t.Fatalf("production missed start deadline: %v", err)
				}
				if job(t, q, next.ID).StartedAt == nil {
					t.Fatal("production did not start")
				}
			}
		})
	}
}

func TestAttemptProtocolCheckinBadRequestPreserved(t *testing.T) {
	q, clock := fixture(t)
	m := machine("duplicate", "fake")
	check(t, q, m, nil)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	m.Session = "other-session"
	w := Worker{Clock: clock, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
	_, err := w.checkin(context.Background(), Checkin{Machine: m})
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusBadRequest || he.Code != "worker_checkin_invalid" {
		t.Fatalf("duplicate worker check-in misreported as queue incompatibility: %v", err)
	}
}

func TestAttemptProtocolUnheldStopFences(t *testing.T) {
	m := machine("fenced", "fake")
	owned := AttemptReport{Execution: Execution{ID: "owned", Attempt: 2}, Session: m.Session}
	stop := AttemptDirective{Execution: Execution{ID: "unheld", Attempt: 1}, Session: m.Session, Stop: true}
	for _, tc := range []struct {
		name       string
		directives []AttemptDirective
		wantAck    []Execution
		wantError  bool
	}{
		{name: "multiple-stops", directives: []AttemptDirective{stop, {Execution: owned.Execution, Session: m.Session, Stop: true}, {Execution: Execution{ID: "another", Attempt: 3}, Session: m.Session, Stop: true}}, wantAck: []Execution{stop.Execution, {ID: "another", Attempt: 3}}},
		{name: "foreign-session", directives: []AttemptDirective{{Execution: stop.Execution, Session: "foreign", Stop: true}}},
		{name: "unheld-hold", directives: []AttemptDirective{{Execution: stop.Execution, Session: m.Session, Hold: true}}},
		{name: "validate-before-ack", directives: []AttemptDirective{stop, {Execution: Execution{ID: "invalid"}, Session: m.Session, Stop: true}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acks := make(chan Result, 8)
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/workers/checkin" {
					_ = json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: tc.directives})
					return
				}
				if r.URL.Path != "/api/workers/complete" {
					t.Errorf("unexpected request: %s", r.URL.Path)
				}
				var ack Result
				if err := json.NewDecoder(r.Body).Decode(&ack); err != nil {
					t.Error(err)
				}
				acks <- ack
			}))
			defer server.Close()
			w := Worker{Machine: m, Client: Client{URL: server.URL, Token: "test"}}
			reply, err := w.checkin(context.Background(), Checkin{Machine: m, Running: &owned.Execution})
			if (err != nil) != tc.wantError || len(acks) != len(tc.wantAck) {
				t.Fatalf("acknowledgements=%d want=%d error=%v", len(acks), len(tc.wantAck), err)
			}
			for _, key := range tc.wantAck {
				ack := <-acks
				if ack.Execution != key || ack.Session != m.Session || ack.Machine != m.ID || !ack.ReleaseOnly || ack.ReleasePending {
					t.Fatalf("acknowledgement crossed custody fence: %+v", ack)
				}
			}
			if tc.name == "multiple-stops" && !reply.Stop {
				t.Fatal("owned stop was lost while acknowledging unheld stops")
			}
		})
	}
}
