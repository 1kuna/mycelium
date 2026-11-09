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
	"slices"
	"strings"
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

func TestMachineRecoveryDoesNotSpendQueuedAttempts(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, phase := range []string{"running", "kill"} {
			for _, priorRetry := range []bool{phase == "kill"} {
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
					r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Fresh: true, PreviousSession: previous, period: time.Second})
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

	})

	t.Run("M3SettledReportsDrainBeforeNewAssignment", func(t *testing.T) {
		q, _ := fixture(t)
		m := m3Machine()
		submit(t, q, spec("experiment"))
		a := assignedJob(machineCheck(t, q, m))
		if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Interrupted: true}); err != nil {
			t.Fatal(err)
		}
		if next := assignedJob(machineCheck(t, q, m, a)); next != nil {
			t.Fatal("new assignment prevented recovered custody from draining", next.Attempt)
		}
		if next := assignedJob(machineCheck(t, q, m)); next == nil || next.Attempt != a.Attempt+1 {
			t.Fatal("fresh admission did not resume after settled report drained", next)
		}

	})

	t.Run("MachineRecoveryDoesNotRedeliverOmittedAssignment", func(t *testing.T) {
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

	})
}

func TestMachineWorkerReportsRecoveryUntilBufferedResultDrains(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("MachineRecoveryStatusAndClear", func(t *testing.T) {
		q, clock := fixture(t)
		m := m3Machine()
		since := clock.Now()
		assertStatus := func(recovering, attention bool) {
			t.Helper()
			s, err := q.Snapshot()
			if err != nil || len(s.Machines) != 1 {
				t.Fatal("snapshot", err)
			}
			data, err := json.Marshal(s.Machines[0])
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Recovering bool       `json:"recovering"`
				Since      *time.Time `json:"recovering_since"`
				Attention  bool       `json:"recovery_needs_attention"`
			}
			if err = json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			if state.Recovering != recovering || state.Attention != attention || recovering && (state.Since == nil || !state.Since.Equal(since)) || !recovering && state.Since != nil {
				t.Fatalf("recovery status missing or wrong: %+v", state)
			}
			words := HyphaAvailability(s.Machines[0])
			if strings.Contains(words, "recovering") != recovering || strings.Contains(words, "needs attention") != attention {
				t.Fatal("status hides recovery", words)
			}
		}
		if r := recoveringCheck(t, q, m); r.AdmissionBlocked {
			t.Fatal("recovery must not hold saved-result replay")
		}
		assertStatus(true, false)
		for i := 0; i < 12; i++ {
			clock.Advance(10 * time.Second)
			recoveringCheck(t, q, m)
		}
		assertStatus(true, false)
		clock.Advance(time.Second)
		recoveringCheck(t, q, m)
		assertStatus(true, true)
		machineCheck(t, q, m)
		assertStatus(false, false)

	})

	t.Run("MachineWorkerRestartReplaysEveryBufferedResult", func(t *testing.T) {
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
		for i := 0; i < 2; i++ {
			j := harnessJob(t, fmt.Sprint("saved-", i), fmt.Sprint(i), "exit 0")
			j.Machine, j.Session = w.Machine.ID, root.state.Session
			s, err := newAttemptStore(root, j)
			if err != nil {
				t.Fatal(err)
			}
			r := Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: true}
			s.state.Identity = ID()
			if err = saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
				t.Fatal(err)
			}
			if err = s.save(); err != nil {
				t.Fatal(err)
			}
		}
		root.close()
		startHarness(t, w, h)
		await(t, func() bool { return h.completed("saved-0") && h.completed("saved-1") })
		for i := 0; i < 2; i++ {
			r := h.result(fmt.Sprint("saved-", i))
			if !r.Success || r.Session != "saved-session" || r.Attempt != 1 {
				t.Fatal("buffered result lost exact custody")
			}
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if !slices.ContainsFunc(h.reports, func(c Checkin) bool { return c.Machine.Session == "saved-session" && len(c.Attempts) == 2 }) {
			t.Fatal("restart omitted saved attempts")
		}

	})

	t.Run("MachineRecoveryRejectsUnexpectedOffer", func(t *testing.T) {
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

	})
}
