package myce

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCancellationRejoinBeforeCancel(t *testing.T) {
	q, c := fixture(t)
	one, two := machine("one", "fake"), machine("two", "fake")
	for _, m := range []*Machine{&one, &two} {
		m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 16, 16
	}
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	j := submit(t, q, s)
	first := check(t, q, one, nil).Job
	check(t, q, two, nil)
	check(t, q, machine("three", "fake"), nil)
	c.Advance(LossAfter)
	if err := q.update(func() error {
		q.s.Machines["two"].LastSeen, q.s.Machines["three"].LastSeen = c.Now(), c.Now()
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	second := check(t, q, two, nil).Job
	if second == nil || second.Attempt != 2 {
		t.Fatal("missing second attempt")
	}
	one.Session = "fresh-one"
	if _, err := q.Checkin(Checkin{Machine: one, Fresh: true, PreviousSession: first.Session}); err != nil {
		t.Fatal(err)
	}
	finish(t, q, second, true)
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
		t.Fatal("recreated discharged custody", a)
	}
	q.refreshHostMemory()
	if len(q.s.HostReleases) != 0 || q.s.Machines[one.ID].HostMemoryInUseGB != 0 {
		t.Fatal("cancel invented a historical charge")
	}
}

func TestCancellationFreshRejoinUntrackedCopies(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	s := spec("experiment")
	s.AllowedMachines = []string{m.ID}
	j := submit(t, q, s)
	j = check(t, q, m, nil).Job
	check(t, q, machine("two", "fake"), nil)
	check(t, q, machine("three", "fake"), nil)
	for attempt := 1; attempt <= 2; attempt++ {
		c.Advance(LossAfter)
		if err := q.update(func() error {
			q.s.Machines["two"].LastSeen, q.s.Machines["three"].LastSeen = c.Now(), c.Now()
			q.maintain()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			m.Session = "second-one"
			j = check(t, q, m, nil).Job
			if j == nil || j.Attempt != 2 {
				t.Fatal("missing second session attempt")
			}
		}
	}
	if len(q.s.HostReleases) != 0 {
		t.Fatal("fixture unexpectedly tracks custody")
	}
	q = reopenCancellationQueue(t, q, c)
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" || len(q.s.HostReleases) != 0 {
		t.Fatal("modern unknown copy was assumed released or charged", a)
	}
	m.Session = "fresh-one"
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: j.Session}); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal("fresh rejoin confirmed an older session", a)
	}
	copies := q.s.Jobs[j.ID].Copies
	if copies[0].ReleaseConfirmed || !copies[1].ReleaseConfirmed {
		t.Fatal("fresh cleanup must cover only the immediately previous session", copies)
	}
	r := Result{Execution: Execution{ID: j.ID, Attempt: 1}, Machine: m.ID, Session: "session-one", ReleaseOnly: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
		t.Fatal("exact older-session proof did not settle release", a)
	}
}

func TestCancellationFreshRejoinKeepsOlderProductionCustody(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 16, 16
	if err := q.ConfigureHosts(map[string]HostPolicy{m.ID: {ProductionHold: true, ProductionIdle: "0s"}}); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.AllowedMachines, s.Needs.HostMemoryGB = []string{m.ID}, 4
	j := submit(t, q, s)
	first := check(t, q, m, nil).Job
	if first == nil {
		t.Fatal("missing production assignment")
	}
	if err := q.Started(result(first, true)); err != nil {
		t.Fatal(err)
	}
	check(t, q, machine("two", "fake"), nil)
	check(t, q, machine("three", "fake"), nil)
	c.Advance(ProductionLossAfter)
	if err := q.update(func() error {
		q.s.Machines["two"].LastSeen, q.s.Machines["three"].LastSeen = c.Now(), c.Now()
		q.maintain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID].State != "queued" {
		t.Fatal("production loss did not requeue")
	}
	m.Session = "second-one"
	if reply := check(t, q, m, nil); reply.Job != nil {
		t.Fatal("old custody allowed production reassignment")
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
		t.Fatal(a)
	}
	m.Session = "fresh-one"
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: "second-one"}); err != nil {
		t.Fatal(err)
	}
	key := releaseKey(HostRelease{Job: j.ID, Machine: first.Machine, Session: first.Session, Attempt: first.Attempt})
	if _, retained := q.s.HostReleases[key]; !retained || !q.held(m.ID) || q.s.Machines[m.ID].HostMemoryInUseGB != 4 {
		t.Fatal("fresh S2 cleanup freed S1 production custody")
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" || q.s.Jobs[j.ID].Copies[0].ReleaseConfirmed {
		t.Fatal("older production session was confirmed", a)
	}
	r := result(first, false)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" || q.held(m.ID) {
		t.Fatal("exact S1 cleanup failed to settle custody", a)
	}
}

func TestCancellationLostAssignmentEmptyCheckin(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "redelivery", true: "held-host"}[held], func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("one", "fake")
			if held {
				if err := q.ConfigureHosts(map[string]HostPolicy{m.ID: {ProductionHold: true, ProductionIdle: "0s"}}); err != nil {
					t.Fatal(err)
				}
			}
			j := submit(t, q, spec("production"))
			assigned := check(t, q, m, nil).Job // Assignment reply is lost before launch.
			if assigned == nil {
				t.Fatal("missing assignment")
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if len(q.s.HostReleases) != 1 {
				t.Fatal("fixture must retain runtime custody before cancellation")
			}
			if a := cancelAck(t, q, j.ID); a.State != "stopping" || ackRelease(a) != "pending" {
				t.Fatal(a)
			}
			if reply := check(t, q, m, nil); reply.Job != nil {
				t.Fatal("cancelled assignment was redelivered")
			}
			if a := cancelAck(t, q, j.ID); a.State != "cancelled" || ackRelease(a) != "confirmed" {
				t.Fatal("same-session empty report did not prove exact release", a)
			}
			if !q.s.Jobs[j.ID].Copies[0].ReleaseConfirmed || len(q.s.HostReleases) != 0 {
				t.Fatal("exact attempt evidence or release record was not settled")
			}
			assertNoCancellationAttempt(t, q, assigned)
		})
	}
}

// Write the original wire shape: pre-S03 copies have no release evidence fields.
func legacyCancellationFixture(t *testing.T, retained bool) (*Queue, *fakeClock, *Job, Machine) {
	t.Helper()
	q, c := fixture(t)
	one, two := machine("one", "fake"), machine("two", "fake")
	one.HostMemoryMeasured, one.HostAvailableMemoryGB, one.HostMemoryBudgetGB = true, 16, 16
	check(t, q, one, nil)
	check(t, q, two, nil)
	s := spec("production")
	s.Needs.HostMemoryGB = 4
	j := submit(t, q, s)
	if err := q.update(func() error {
		live := q.s.Jobs[j.ID]
		live.State, live.Machine, live.Session, live.Attempt = "succeeded", two.ID, two.Session, 2
		live.ReleaseConfirmed = true
		now := c.Now()
		live.FinishedAt = &now
		live.Copies = []Copy{{Machine: one.ID, Session: "old-one", Attempt: 1}, {Machine: two.ID, Session: two.Session, Attempt: 2}}
		if retained {
			r := HostRelease{Job: j.ID, Host: one.ID, Machine: one.ID, Session: "old-one", Attempt: 1, HostChargeGB: 4}
			q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	return q, c, q.s.Jobs[j.ID], one
}

func TestCancellationLegacyCopies(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "retained"}[retained], func(t *testing.T) {
			q, _, j, one := legacyCancellationFixture(t, retained)
			want := "confirmed"
			if retained {
				want = "pending"
			}
			if a := cancelAck(t, q, j.ID); ackRelease(a) != want {
				t.Fatal("legacy release", a)
			}
			if retained {
				r := Result{Execution: Execution{ID: j.ID, Attempt: 1}, Machine: one.ID, Session: "old-one", ReleaseOnly: true}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
					t.Fatal(a)
				}
			} else {
				data, _ := json.Marshal(j.Copies[0])
				if !strings.Contains(string(data), "legacy_no_retained_custody") {
					t.Fatal("missing legacy diagnostic", string(data))
				}
				if blocker := q.startupReleaseBlocker(one.ID); blocker != "" {
					t.Fatal(blocker)
				}
				s := spec("production")
				s.AllowedMachines = []string{one.ID}
				s.Needs.HostMemoryGB = 4
				next := submit(t, q, s)
				if got := check(t, q, one, nil).Job; got == nil || got.ID != next.ID {
					t.Fatal("terminal cancellation blocked unrelated placement", got)
				}
			}
		})
	}
}

func TestCancellationAcknowledgementTime(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	first := cancelAck(t, q, j.ID)
	reserved, err := q.CancelSubmission("test", "reserved-time")
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Second)
	if a := cancelAck(t, q, j.ID); !a.AcknowledgedAt.Equal(*first.AcknowledgedAt) {
		t.Fatal("job acknowledgement changed")
	}
	a, err := q.CancelSubmission("test", "reserved-time")
	if err != nil || !a.AcknowledgedAt.Equal(*reserved.AcknowledgedAt) {
		t.Fatal("reservation acknowledgement changed", a, err)
	}
	q = reopenCancellationQueue(t, q, c)
	s := spec("experiment")
	s.SubmissionKey = "reserved-time"
	bound := submit(t, q, s)
	if a := cancelAck(t, q, bound.ID); !a.AcknowledgedAt.Equal(*reserved.AcknowledgedAt) {
		t.Fatal("bound reservation changed acknowledgement")
	}
}

func TestCancellationLegacyIntentPersistsOnOpen(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	if err := q.update(func() error { q.s.Jobs[j.ID].Reason = "cancel requested"; return nil }); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	first := *q.s.Jobs[j.ID].CancelledAt
	var saved []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&saved); err != nil {
		t.Fatal(err)
	}
	var persisted state
	if err := json.Unmarshal(saved, &persisted); err != nil || persisted.Jobs[j.ID].CancelledAt == nil {
		t.Fatal("Open did not persist cancellation import", err)
	}
	c.Advance(time.Second)
	q = reopenCancellationQueue(t, q, c)
	if !q.s.Jobs[j.ID].CancelledAt.Equal(first) {
		t.Fatal("legacy cancellation time moved on reopen")
	}
}

func TestCancellationWorkerPreparing(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, c := fixture(t)
			s := spec(priority)
			marker := filepath.Join(t.TempDir(), "launched")
			s.Run.Command = []string{"sh", "-c", "echo launched > " + marker}
			j := submit(t, q, s)
			m := machine("worker", "fake")
			assigned := check(t, q, m, nil).Job
			w := cancellationPreparingWorker(t, q, c, m, j.ID)
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = assigned.Session
			p, err := w.start(store, *assigned)
			if err != nil || p == nil {
				t.Fatalf("start: %v", err)
			}
			select {
			case done := <-p.done:
				_, statErr := os.Stat(marker)
				if priority == "production" {
					if done.result.Success || !os.IsNotExist(statErr) {
						t.Fatal("cancelled gated command launched", done)
					}
				} else if !done.result.Success || statErr != nil {
					t.Fatal("expected documented preassigned, ungated launch window", done, statErr)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "pending" {
					t.Fatal(a)
				}
				if err := q.Complete(done.result); err != nil {
					t.Fatal(err)
				}
				if a := cancelAck(t, q, j.ID); ackRelease(a) != "confirmed" {
					t.Fatal(a)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("worker preparation stalled")
			}
			assertNoCancellationAttempt(t, q, assigned)
		})
	}
}

func cancellationPreparingWorker(t *testing.T, q *Queue, c Clock, m Machine, id string) Worker {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	prepared := t.TempDir()
	return Worker{StateDir: t.TempDir(), Machine: m, Clock: c, Client: Client{URL: server.URL, Token: "test"}, checkpointPrepare: func(Job) (string, error) {
		if a := cancelAck(t, q, id); a.State != "stopping" || ackRelease(a) != "pending" {
			t.Fatal("preparing cancellation", a)
		}
		return prepared, nil
	}}
}

func TestCancellationPlacementPreemption(t *testing.T) {
	for _, kind := range []string{"job", "pin"} {
		t.Run(kind, func(t *testing.T) {
			q, _, m := modelFixture(t)
			j := submit(t, q, spec("experiment"))
			j = check(t, q, m, nil).Job
			if kind == "job" {
				p := spec("production")
				p.AllowedMachines = []string{m.ID}
				submit(t, q, p)
			} else if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			if !check(t, q, m, j).Stop || !strings.Contains(job(t, q, j.ID).Reason, "making room for production") {
				t.Fatal("real placement did not displace experiment")
			}
			cancelAck(t, q, j.ID)
			// Repeated real placement exercises stopFor's reason/grace update while
			// cancellation is already persisted, instead of calling retry directly.
			if !check(t, q, m, j).Stop || job(t, q, j.ID).Reason != "cancel requested" {
				t.Fatal("placement overwrote cancellation intent")
			}
			r := result(j, false)
			r.Interrupted = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			assertNoCancellationAttempt(t, q, j)
		})
	}
}

func TestCancellationExcludedFromHoldDemand(t *testing.T) {
	q, _, a, _, _ := holdFleet(t, true)
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	j := submit(t, q, p)
	j = check(t, q, a, nil).Job
	cancelAck(t, q, j.ID)
	// A release-only receipt proves resources gone while the cancelled job is
	// still stopping. It must not continue to count as production demand.
	r := result(j, false)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if q.s.Hosts[hostID(&a)].IdleSince == nil {
		t.Fatal("cancelled stopping job retained production demand")
	}
}

func TestSubmissionKeyLimits(t *testing.T) {
	q, _ := fixture(t)
	for _, key := range []string{"a", "AZaz09._:-", strings.Repeat("k", 128)} {
		s := spec("experiment")
		s.SubmissionKey = key
		j := submit(t, q, s)
		if a, err := q.CancelSubmission(s.Project, key); err != nil || *a.JobID != j.ID {
			t.Fatal("valid key rejected", err)
		}
	}
	for _, key := range []string{"a/b", "has space", "ümlaut", "line\nbreak", strings.Repeat("k", 129)} {
		s := spec("experiment")
		s.SubmissionKey = key
		_, err := q.Submit(s)
		submissionError(t, err, 400, "submission_key_invalid")
		_, err = q.CancelSubmission(s.Project, key)
		submissionError(t, err, 400, "submission_key_invalid")
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	schema := compileWireSchema(t, "error-v1.schema.json")
	for _, path := range []string{"/api/jobs", "/api/jobs/cancel"} {
		for _, key := range []any{"", "a/b", "space key", strings.Repeat("x", 129), nil, 42} {
			body := map[string]any{"project": "test", "submission_key": key}
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(wireJSON(t, body))))
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			var reply map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if w.Code != 400 || reply["code"] != "submission_key_invalid" {
				t.Fatal(path, key, w.Code, reply)
			}
			validateWireValue(t, schema, reply)
		}
	}
}

func TestWireUnknownJobHandler(t *testing.T) {
	q, _ := fixture(t)
	r := httptest.NewRequest(http.MethodPost, "/api/jobs/unknown/cancel", nil)
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatal(w.Code)
	}
	var reply map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, compileWireSchema(t, "error-v1.schema.json"), reply)
}
