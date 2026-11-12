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
		name                 string
		planned, otherReport bool
	}{
		{name: "cancel"},
		{name: "P3-planned-stop", planned: true},
		{name: "cancel-other-report", otherReport: true},
		{name: "P3-other-report", planned: true, otherReport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			planned := tc.planned
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
			wire := attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second})
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
			if reply, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m}); err != nil || reply.Stop {
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
			if got.State != want || !got.ReleaseConfirmed || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("unheld stop not promptly released: %+v", got)
			}
			if tc.otherReport {
				attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{
					{Execution: Execution{ID: "unknown", Attempt: 1}, Session: m.Session},
				}})
				requireState(t, q, victim.ID, want)
			}
			reply, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m})
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
	_, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m})
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
			reply, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m, Running: &owned.Execution})
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

// #1021: the queue commits an assignment whose reply the worker never receives
// (slow control plane), then the job fails before starting. Its custody must
// release on the worker's next same-session contact, only through an exact
// unheld acknowledgement, and the late release must not reopen the ledger.
func TestAttemptProtocolNeverStartedTerminalCustody(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "unheld-releases", true: "held-keeps-custody"}[held], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("never-started", "fake")
			acks := make(chan Result, 8)
			api := API{q, "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/workers/complete" {
					var ack Result
					if err := json.NewDecoder(r.Body).Decode(&ack); err != nil {
						t.Error(err)
					}
					acks <- ack
					respondOperation(rw, nil, q.Complete(ack), "worker_result_invalid")
					return
				}
				api.ServeHTTP(rw, r)
			}))
			defer server.Close()
			w := Worker{Clock: clock, Machine: m, Client: Client{URL: server.URL, Token: "test"}}
			victim := submit(t, q, spec("production"))
			wire := attemptWire(t, w.Client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, period: time.Second})
			if len(wire.Attempts) != 1 || wire.Attempts[0].Job == nil || wire.Attempts[0].Job.ID != victim.ID {
				t.Fatalf("missing initial assignment: %+v", wire)
			}
			lost := wire.Attempts[0].Job
			clock.Advance(ProductionStartTimeout)
			failed := job(t, q, victim.ID)
			if failed.State != "failed" || failed.ReleaseConfirmed || failed.StartedAt != nil || !q.jobReleasePending(failed) {
				t.Fatalf("precondition: want a failed never-started job holding custody: %+v", failed)
			}
			complete := failed.Ledger != nil && failed.Ledger.Complete
			if !complete {
				t.Fatalf("precondition: closed ledger already incomplete: %+v", failed.Ledger)
			}
			c := Checkin{Machine: m}
			if held {
				// A worker still holding the key gets an addressed stop, never a release.
				c.Attempts = []AttemptReport{{Execution: Execution{ID: lost.ID, Attempt: lost.Attempt}, Session: m.Session}}
			}
			reply, err := w.fixtureCheckin(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			got := job(t, q, victim.ID)
			if held {
				if len(acks) != 0 || got.ReleaseConfirmed || !q.jobReleasePending(got) || !reply.Stop || !reply.Kill {
					t.Fatalf("held attempt released without cleanup: acks=%d reply=%+v job=%+v", len(acks), reply, got)
				}
				return
			}
			if len(acks) != 1 {
				t.Fatalf("got %d release acknowledgements; want 1", len(acks))
			}
			ack := <-acks
			if !ack.ReleaseOnly || ack.ReleasePending || ack.ID != lost.ID || ack.Attempt != lost.Attempt || ack.Session != m.Session || ack.Machine != m.ID {
				t.Fatalf("not an exact release-only acknowledgement: %+v", ack)
			}
			if got.State != "failed" || !got.ReleaseConfirmed || q.jobReleasePending(got) || len(q.s.HostReleases) != 0 {
				t.Fatalf("never-started custody still pending: job=%+v releases=%+v", got, q.s.HostReleases)
			}
			if got.Ledger == nil || !got.Ledger.Complete {
				t.Fatalf("late release reopened a closed ledger: %+v", got.Ledger)
			}
			st, err := q.HostMaintenanceStatus(hostID(&m))
			if err != nil || len(st.Attempts) != 0 || len(st.PendingReleases) != 0 {
				t.Fatalf("host still holds custody: %+v %v", st, err)
			}
			// Confirmed custody receives no further controls.
			again, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m})
			if err != nil || len(acks) != 0 || again.Stop {
				t.Fatalf("released copy kept receiving stops: %+v %v acks=%d", again, err, len(acks))
			}
		})
	}
}
