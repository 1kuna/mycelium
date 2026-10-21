package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Exercise the additive wire field so these tests also compile on the base.
func recoveryCheckin(t *testing.T, m Machine, reports ...AttemptReport) Checkin {
	t.Helper()
	data, err := json.Marshal(struct {
		Checkin
		Recovering bool `json:"recovering"`
	}{Checkin: Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports}, Recovering: true})
	if err != nil {
		t.Fatal(err)
	}
	var c Checkin
	if err = json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func recoveringCheck(t *testing.T, q *Queue, m Machine, reports ...AttemptReport) Reply {
	t.Helper()
	r, err := q.Checkin(recoveryCheckin(t, m, reports...))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMachineRecoveryRejectsUnexpectedOffer(t *testing.T) {
	m := m3Machine()
	j := harnessJob(t, "unexpected", "1", "exit 0")
	j.Machine, j.Session = m.ID, m.Session
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/checkin" {
			t.Error("recovery refusal sent an execution result", r.URL.Path)
		}
		_ = json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptDirective{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: &j}}})
	}))
	defer server.Close()
	w := Worker{Machine: m, Client: Client{URL: server.URL, Token: "test"}}
	r, err := w.checkin(context.Background(), recoveryCheckin(t, m))
	var protocol *HTTPError
	if !errors.As(err, &protocol) || protocol.Status != 426 || protocol.Code != "worker_protocol_invalid" || r.Job != nil || len(r.Attempts) != 0 {
		t.Fatal("recovery offer accepted instead of preserving custody", r, err)
	}
}

func TestMachineRecoveryDoesNotSpendQueuedAttempts(t *testing.T) {
	for _, phase := range []string{"running", "grace", "kill"} {
		for _, priorRetry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prior-retry=%t", phase, priorRetry), func(t *testing.T) {
				q, clock := fixture(t)
				m := m3Machine()
				submit(t, q, m3Spec("experiment", "0", 8, 1))
				owned := assignedJob(machineCheck(t, q, m))
				if owned == nil {
					t.Fatal("missing owned attempt")
				}
				machineCheck(t, q, m, owned)
				s := m3Spec("production", "1", 8, 1)
				s.Resumable = false
				s.SubmissionKey = "recovery-bound"
				waiting := submit(t, q, s)
				if priorRetry {
					a := assignedJob(machineCheck(t, q, m, owned))
					if a == nil || a.ID != waiting.ID {
						t.Fatal("missing initial production attempt")
					}
					finishInterrupted(t, q, a)
				}
				if phase != "running" {
					if err := q.Cancel(owned.ID); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "kill" {
					clock.Advance(StopGrace + time.Second)
				}
				before := job(t, q, waiting.ID)
				report := AttemptReport{Execution: Execution{ID: owned.ID, Attempt: owned.Attempt}, Session: owned.Session, Started: true}
				offers := 0
				for i := 0; i < 100; i++ {
					r := recoveringCheck(t, q, m, report)
					for _, d := range r.Attempts {
						if d.ID == owned.ID && (d.Stop != (phase != "running") || d.Kill != (phase == "kill")) {
							t.Fatalf("recovery lost owned control: %+v", d)
						}
						if d.Job != nil {
							offers++
							// Reproduce the old worker's refusal, including its retry charge.
							finishInterrupted(t, q, d.Job)
						}
					}
					clock.Advance(time.Millisecond)
				}
				got := job(t, q, waiting.ID)
				if offers != 0 || got.State != "queued" || got.Attempt != before.Attempt || got.Starts != before.Starts || got.Losses != before.Losses || got.InfrastructureRetries != before.InfrastructureRetries || len(got.Copies) != len(before.Copies) {
					t.Fatalf("recovery churn: offers=%d state=%s attempt=%d->%d starts=%d->%d losses=%d->%d infra=%d->%d copies=%d->%d", offers, got.State, before.Attempt, got.Attempt, before.Starts, got.Starts, before.Losses, got.Losses, before.InfrastructureRetries, got.InfrastructureRetries, len(before.Copies), len(got.Copies))
				}
				finishInterrupted(t, q, owned)
				previous := m.Session
				m.Session = "after-drain"
				r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Fresh: true, PreviousSession: previous})
				if err != nil {
					t.Fatal(err)
				}
				// Recovery has not spent any of the remaining two-requeue budget.
				for n := before.InfrastructureRetries; n <= 2; n++ {
					a := assignedJob(r)
					if a == nil || a.ID != waiting.ID || a.Attempt != before.Attempt+1+n-before.InfrastructureRetries {
						t.Fatal("normal placement did not resume", a)
					}
					if err := q.Started(result(a, false)); err != nil {
						t.Fatal(err)
					}
					finishInterrupted(t, q, a)
					got = job(t, q, waiting.ID)
					want := "queued"
					if n == 2 {
						want = "failed"
					}
					if got.State != want || got.InfrastructureRetries != min(n+1, 2) || n == 2 && got.Reason != "infrastructure_retries_exhausted" {
						t.Fatal("production infrastructure bound changed", got)
					}
					r = machineCheck(t, q, m)
				}
			})
		}
	}
}

func TestMachineRecoveryDoesNotRedeliverOmittedAssignment(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, m3Spec("experiment", "0", 8, 1))
	owned := assignedJob(machineCheck(t, q, m))
	before := job(t, q, owned.ID)
	for i := 0; i < 100; i++ {
		if got := assignedJob(recoveringCheck(t, q, m)); got != nil {
			t.Fatal("recovery redelivered an unheld assignment", got.ID)
		}
	}
	got := job(t, q, owned.ID)
	if got.ReleaseConfirmed || got.State != before.State || got.Attempt != before.Attempt || len(got.Copies) != len(before.Copies) {
		t.Fatal("recovery omission changed custody", got)
	}
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != owned.ID || got.Attempt != before.Attempt {
		t.Fatal("ordinary lost-response redelivery changed", got)
	}
}

func TestMachineKillPhaseCapacitySparesNeighbor(t *testing.T) {
	for _, phase := range []string{"grace", "kill"} {
		t.Run(phase, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			submit(t, q, m3Spec("experiment", "0", 8, 1))
			a := assignedJob(machineCheck(t, q, m))
			submit(t, q, m3Spec("experiment", "1", 8, 1))
			b := assignedJob(machineCheck(t, q, m, a))
			machineCheck(t, q, m, a, b)
			if err := q.Cancel(a.ID); err != nil {
				t.Fatal(err)
			}
			if phase == "kill" {
				clock.Advance(StopGrace + time.Second)
			}
			prod := submit(t, q, spec("production"))
			for i := 0; i < 10; i++ {
				if next := assignedJob(machineCheck(t, q, m, a, b)); next != nil {
					t.Fatal("replacement admitted before exact cleanup", next)
				}
				if got := job(t, q, b.ID); got.State != "running" || got.PreemptionCount != 0 || priorityStop(got) != nil {
					t.Fatalf("%s capacity ignored: neighbor state=%s preemptions=%d", phase, got.State, got.PreemptionCount)
				}
				clock.Advance(time.Millisecond)
			}
			finishInterrupted(t, q, a)
			next := assignedJob(machineCheck(t, q, m, b))
			if next == nil || next.ID != prod.ID || len(next.AllocatedDevices) != 1 || next.AllocatedDevices[0] != "0" {
				t.Fatal("production did not take the released device", next)
			}
			if got := job(t, q, b.ID); got.State != "running" || got.Attempt != b.Attempt || got.PreemptionCount != 0 {
				t.Fatal("neighbor changed after placement", got)
			}
		})
	}
}

func TestMachineWorkerReportsRecoveryUntilBufferedResultDrains(t *testing.T) {
	h := &machineHarness{issued: true, controls: map[string]AttemptDirective{}}
	w := harnessWorker(t, h)
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	root.state.Session = "saved-session"
	if err = root.save(); err != nil {
		t.Fatal(err)
	}
	j := harnessJob(t, "saved", "0", "exit 0")
	j.Machine, j.Session = w.Machine.ID, root.state.Session
	s, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	s.state.Result = &Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: true}
	if err = s.save(); err != nil {
		t.Fatal(err)
	}
	root.close()
	observed := make(chan struct {
		Checkin
		Recovering bool `json:"recovering"`
	}, 1000)
	drain := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/checkin" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(rw, err.Error(), 500)
				return
			}
			var c struct {
				Checkin
				Recovering bool `json:"recovering"`
			}
			if err = json.Unmarshal(data, &c); err != nil {
				http.Error(rw, err.Error(), 400)
				return
			}
			observed <- c
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		if r.URL.Path == "/api/workers/complete" {
			select {
			case <-drain:
			case <-r.Context().Done():
				return
			}
		}
		h.serve(rw, r)
	}))
	t.Cleanup(server.Close)
	w.Client.URL = server.URL
	startHarness(t, w, h)
	// Registered last so a failure also releases the worker's result replay.
	released := false
	t.Cleanup(func() {
		if !released {
			close(drain)
		}
	})
	for i := 0; i < 20; i++ {
		select {
		case c := <-observed:
			if !c.Recovering || c.Machine.Session != j.Session || c.Fresh || len(c.Attempts) != 1 || c.Attempts[0].ID != j.ID {
				t.Fatalf("startup custody not marked recovering: recovering=%t session=%s fresh=%t attempts=%d", c.Recovering, c.Machine.Session, c.Fresh, len(c.Attempts))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("recovery blocked heartbeat")
		}
	}
	close(drain)
	released = true
	for {
		select {
		case c := <-observed:
			if c.Machine.Session != j.Session {
				if c.Recovering || !c.Fresh || c.PreviousSession != j.Session || len(c.Attempts) != 0 {
					t.Fatal("recovery did not finish at fresh-session handoff")
				}
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("result drain did not restore admission")
		}
	}
}
