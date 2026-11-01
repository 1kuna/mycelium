package myce

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func changeHold(t *testing.T, q *Queue, id string, revision uint64, pause bool, until *time.Time) JobHold {
	t.Helper()
	request := JobHoldRequest{ExpectedRevision: &revision, Until: until}
	if pause {
		request.Reason = "owner requested a hold"
	}
	h, err := q.ChangeJobHold(id, request, pause)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestJobHoldReopenExpiryReplayAndRevision(t *testing.T) {
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "retained-intent"
	original := submit(t, q, s)
	until := c.Now().Add(time.Minute)
	changeHold(t, q, original.ID, 0, true, &until)
	if got := check(t, q, machine("one", "fake"), nil).Job; got != nil {
		t.Fatal("held job assigned", got)
	}
	receipt, err := q.jobSnapshot(original.ID)
	if err != nil || receipt.Hold == nil || !receipt.Hold.Active || !strings.Contains(receipt.Reason, "job held") {
		t.Fatalf("receipt %+v %v", receipt, err)
	}
	q = reopenCancellationQueue(t, q, c)
	replay := submit(t, q, s)
	if replay.ID != original.ID || !reflect.DeepEqual(replay.Spec, original.Spec) || !replay.Queued.Equal(original.Queued) || replay.Attempt != 0 || replay.Losses != original.Losses {
		t.Fatalf("logical identity changed: %+v", replay)
	}
	if h := job(t, q, original.ID).Hold; h == nil || !h.Active || h.Revision != 1 || h.Reason == "" {
		t.Fatal("hold lost on reopen", h)
	}
	c.Advance(time.Minute - time.Nanosecond)
	if got := check(t, q, machine("one", "fake"), nil).Job; got != nil {
		t.Fatal("early expiry", got)
	}
	c.Advance(time.Nanosecond)
	// Expiry is Clock-derived even before another queue write.
	receipt, err = q.jobSnapshot(original.ID)
	if err != nil || receipt.Hold.Active {
		t.Fatal("expired hold still active", receipt, err)
	}
	changeHold(t, q, original.ID, 1, true, nil)
	stale := uint64(1)
	if _, err := q.ChangeJobHold(original.ID, JobHoldRequest{ExpectedRevision: &stale}, false); err == nil {
		t.Fatal("stale resume cleared newer hold")
	}
	c.Advance(time.Hour)
	if got := job(t, q, original.ID); !got.Hold.Active || got.Attempt != 0 {
		t.Fatal("indefinite hold expired", got)
	}
	changeHold(t, q, original.ID[:8], 2, false, nil)
	run := check(t, q, machine("one", "fake"), nil).Job
	if run == nil || run.ID != original.ID || run.Attempt != 1 {
		t.Fatal("resume did not admit original", run)
	}
	rev := uint64(3)
	_, err = q.ChangeJobHold(original.ID, JobHoldRequest{ExpectedRevision: &rev, Reason: "too late"}, true)
	var conflict *HTTPError
	if !errors.As(err, &conflict) || conflict.Code != "job_hold_state_conflict" {
		t.Fatal("assigned pause not rejected specifically", err)
	}
}

func TestJobHoldCancellationAndClocks(t *testing.T) {
	for _, kind := range []string{"cancel", "give-up", "start-by"} {
		t.Run(kind, func(t *testing.T) {
			q, c := fixture(t)
			s := spec("experiment")
			expiry := c.Now().Add(time.Minute)
			if kind == "give-up" {
				s.GiveUpAt = &expiry
			}
			if kind == "start-by" {
				s = spec("production")
				s.StartBy = &expiry
			}
			j := submit(t, q, s)
			changeHold(t, q, j.ID, 0, true, nil)
			if kind == "cancel" {
				cancelAck(t, q, j.ID)
			} else {
				c.Advance(time.Minute)
				check(t, q, machine("one", "fake"), nil)
			}
			got := job(t, q, j.ID)
			if !terminal(got.State) || got.Attempt != 0 || got.Hold.Active {
				t.Fatalf("hold froze lifecycle clock/cancellation: %+v", got)
			}
			rev := uint64(1)
			if _, err := q.ChangeJobHold(j.ID, JobHoldRequest{ExpectedRevision: &rev}, false); err == nil {
				t.Fatal("terminal job resumed")
			}
		})
	}
}

func TestJobHoldAssignmentRace(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	var wg sync.WaitGroup
	var holdErr, checkErr error
	var reply Reply
	wg.Add(2)
	go func() {
		defer wg.Done()
		n := uint64(0)
		_, holdErr = q.ChangeJobHold(j.ID, JobHoldRequest{ExpectedRevision: &n, Reason: "race"}, true)
	}()
	go func() { defer wg.Done(); reply, checkErr = q.Checkin(Checkin{Machine: machine("one", "fake")}) }()
	wg.Wait()
	if checkErr != nil {
		t.Fatal(checkErr)
	}
	if holdErr == nil {
		if reply.Job != nil || job(t, q, j.ID).Attempt != 0 {
			t.Fatal("pause succeeded alongside assignment", reply)
		}
	} else {
		var conflict *HTTPError
		if reply.Job == nil || !errors.As(holdErr, &conflict) || conflict.Code != "job_hold_state_conflict" {
			t.Fatal("unexpected race result", reply, holdErr)
		}
	}
}

func TestJobHoldPreparationAndParentStartedFence(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 3)
	parent := submit(t, q, s)
	wave := check(t, q, m, nil).Job
	if wave == nil {
		t.Fatal("no first wave")
	}
	// A parent remains queued while its waves are assigned. Hold between offer
	// and Started must deny launch, while a Started wave drains normally.
	changeHold(t, q, parent.ID, 0, true, nil)
	if err := q.Started(result(wave, true)); !errors.Is(err, ErrStale) {
		t.Fatal("held parent permitted unstarted child", err)
	}
	changeHold(t, q, parent.ID, 1, false, nil)
	if err := q.Started(result(wave, true)); err != nil {
		t.Fatal(err)
	}
	changeHold(t, q, parent.ID, 2, true, nil)
	finish(t, q, wave, true)
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("parent materialized while held", next)
	}
	changeHold(t, q, parent.ID, 3, false, nil)
	next := check(t, q, m, nil).Job
	if next == nil || next.Batch == nil || len(next.Batch.Pieces) != 1 || next.Hold != nil {
		t.Fatal("remaining child not independent after resume", next)
	}
	finish(t, q, next, true)
	if job(t, q, parent.ID).State != "succeeded" {
		t.Fatal("parent did not finish")
	}
}

func TestJobHoldPreparationAuthorization(t *testing.T) {
	q, _ := fixture(t)
	m := machine("one", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	j := submit(t, q, s)
	// Exercise a stale preparation offer issued before the hold was committed.
	l := HostLaunch{Machine: m.ID, Session: m.Session, Kind: "environment", Key: "synthetic-env", Attempt: "preparation-one", Demand: j.ID, Spec: &s}
	if err := q.update(func() error { q.s.PreparationPlans = map[string]HostLaunch{l.Attempt: l}; return nil }); err != nil {
		t.Fatal(err)
	}
	changeHold(t, q, j.ID, 0, true, nil)
	if err := q.AuthorizeLaunch(l); !errors.Is(err, ErrStale) {
		t.Fatal("stale preparation authorized", err)
	}
}

func TestJobHoldAPI(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	handler := API{Queue: q, Token: "test-owner"}.Handler()
	request := func(endpoint, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/jobs/"+j.ID+"/"+endpoint, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request("hold", `{"expected_revision":0,"reason":"review"}`, ""); w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	if w := request("hold", `{"reason":"review"}`, "test-owner"); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("hold", `{"expected_revision":0,"reason":"review"}`, "test-owner"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("resume", `{"expected_revision":0}`, "test-owner"); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	r := httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil)
	r.Header.Set("Authorization", "Bearer test-owner")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var receipt Job
	if err := json.Unmarshal(w.Body.Bytes(), &receipt); err != nil || receipt.Hold == nil || !receipt.Hold.Active {
		t.Fatal(w.Body.String(), err)
	}
	if w := request("resume", `{"expected_revision":1}`, "test-owner"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestJobHoldResumeRetainsScheduledPolicy(t *testing.T) {
	q, c := fixture(t)
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	changeHold(t, q, j.ID, 0, true, nil)
	changeHold(t, q, j.ID, 1, false, nil)
	if got := check(t, q, machine("one", "fake", "host-one"), nil).Job; got != nil {
		t.Fatal("resume cleared independent scheduled hold", got)
	}
	if q.PriorityPolicy().Revision != 1 || !strings.Contains(job(t, q, j.ID).Reason, "scheduled hold") {
		t.Fatal("scheduled policy not retained")
	}
}

func TestJobHoldProductionDoesNotReservePeerCapacity(t *testing.T) {
	q, _ := fixture(t)
	m := machine("one", "fake", "host-one")
	if err := q.ConfigureHosts(map[string]HostPolicy{"host-one": {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	production := submit(t, q, spec("production"))
	changeHold(t, q, production.ID, 0, true, nil)
	peer := submit(t, q, spec("experiment"))
	if run := check(t, q, m, nil).Job; run == nil || run.ID != peer.ID {
		t.Fatal("held production retained admission demand", run)
	}
}
