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
	for _, tc := range []struct {
		name                         string
		planned, docker, otherReport bool
	}{
		{name: "cancel"},
		{name: "P3-planned-stop", planned: true},
		{name: "Docker-cancel", docker: true},
		{name: "Docker-P3-planned-stop", planned: true, docker: true},
		{name: "Docker-cancel-other-report", docker: true, otherReport: true},
		{name: "Docker-P3-other-report", planned: true, docker: true, otherReport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			planned := tc.planned
			q, clock := fixture(t)
			m := machine("lost-assignment", "fake")
			if tc.docker {
				m = dockerMachine(m.ID, m.GPU)
			}
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
			victimSpec := spec("experiment")
			if tc.docker {
				victimSpec.Run = dockerJob().Spec.Run
			}
			victim := submit(t, q, victimSpec)
			// The queue commits the assignment, but the worker never receives it.
			wire := attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol})
			if len(wire.Attempts) != 1 {
				t.Fatalf("missing initial assignment: %+v", wire)
			}
			lost := wire.Attempts[0].Job
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
			if (got.State != want && !tc.docker) || !got.ReleaseConfirmed || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("unheld stop not promptly released: %+v", got)
			}
			if tc.otherReport {
				attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{
					{Execution: Execution{ID: "unknown", Attempt: 1}, Session: m.Session},
				}})
				requireState(t, q, victim.ID, want)
			}
			reply, err := w.checkin(context.Background(), Checkin{Machine: m})
			if err != nil || reply.Job == nil || reply.Job.ID != next.ID {
				t.Fatalf("next job did not place promptly: %+v %v", reply, err)
			}
			got = job(t, q, victim.ID)
			if got.State != want || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("acknowledged absent attempt not released: %+v", got)
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

func TestAttemptProtocolDockerReportedRelease(t *testing.T) {
	q, _ := fixture(t)
	m := dockerMachine("docker-custody", "fake")
	submit(t, q, dockerJob().Spec)
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	reply, err := q.Checkin(c)
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].Job == nil {
		t.Fatalf("missing Docker assignment: %+v %v", reply, err)
	}
	j := reply.Attempts[0].Job
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	pending := result(j, false)
	pending.Interrupted, pending.ReleasePending = true, true
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	// Neither omission nor a cleanup-pending result proves container release.
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "stopping" || got.ReleaseConfirmed {
		t.Fatalf("pending Docker custody discharged: %+v", got)
	}
	ack := result(j, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	c.Attempts = []AttemptReport{{Execution: ack.Execution, Session: m.Session}}
	reply, err = q.Checkin(c)
	if err != nil || len(reply.Attempts) != 1 || !reply.Attempts[0].Stop {
		t.Fatalf("reported Docker stop lost: %+v %v", reply, err)
	}
	requireState(t, q, j.ID, "stopping")
	c.Attempts = nil
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "cancelled")
}
