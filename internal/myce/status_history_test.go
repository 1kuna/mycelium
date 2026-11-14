package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStatusHistoryLarge(t *testing.T) {
	q, clock := fixture(t)
	for i := range 10000 {
		id := fmt.Sprintf("history-%05d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Attempt: 1,
			Queued: clock.Now().Add(-time.Duration(i) * time.Second), Spec: spec("experiment"),
			AttemptHistory: []AttemptOutcome{{Attempt: 1, State: "succeeded", Reason: strings.Repeat("x", 512)}}}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	q.readWarm.enable(&q.publishedReads)
	q.readWarm.idle.Wait()
	budgetHost(q, 48<<20, 3)
	baselineStarted := time.Now()
	_, baselineErr := q.publicStatusSnapshot(&statusOptions{jobs: "all", full: true})
	t.Logf("unstreamed baseline: elapsed=%s error=%v", time.Since(baselineStarted), baselineErr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := httptest.NewRequest("GET", "/api/status?detail=full", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer test")
	var peak int64
	w := &historyCommitWriter{ResponseRecorder: httptest.NewRecorder(), observe: func() {
		q.requestBudgets.mu.Lock()
		peak = max(peak, q.requestBudgets.held)
		q.requestBudgets.mu.Unlock()
	}}
	handler := (API{Queue: q, Token: "test"}).Handler()
	// Even an occupied writer cannot delay the observer.
	q.mu.Lock()
	defer q.mu.Unlock()
	started := time.Now()
	handler.ServeHTTP(w, r)
	t.Logf("10000 retained jobs: elapsed=%s status=%d bytes=%d", time.Since(started), w.Code, w.Body.Len())
	t.Logf("peak in-flight projection allowance=%d bytes", peak)
	if peak > 4<<20 {
		t.Fatalf("retained payload charged simultaneously: %d bytes", peak)
	}
	if w.Code != 200 {
		t.Fatalf("full history: %d %s", w.Code, w.Body.String())
	}
	var got Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Jobs) != 10000 || got.JobsPage.Returned != 10000 {
		t.Fatal("incomplete history", len(got.Jobs), got.JobsPage)
	}
	seen := map[string]bool{}
	for _, j := range got.Jobs {
		if seen[j.ID] || len(j.AttemptHistory) != 1 || len(j.AttemptHistory[0].Reason) != 512 {
			t.Fatal("lost/duplicated history", j.ID)
		}
		seen[j.ID] = true
	}
	r = httptest.NewRequest("GET", "/api/usage?since=168h", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer test")
	usage := httptest.NewRecorder()
	handler.ServeHTTP(usage, r)
	if usage.Code != 200 {
		t.Fatalf("usage: %d %s", usage.Code, usage.Body.String())
	}
}

func TestStatusHistoryEquivalentAndPinned(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	for i, state := range []string{"succeeded", "cancelled", "running", "paused", "queued"} {
		j := &Job{ID: fmt.Sprintf("job-%d", i), State: state, Spec: spec("experiment"), Attempt: 1, Queued: clock.Now().Add(-time.Duration(i) * time.Second), ReleaseConfirmed: state == "succeeded" || state == "cancelled"}
		if state == "cancelled" {
			now := clock.Now()
			j.CancelledAt = &now
		}
		q.s.Jobs[j.ID] = j
	}
	q.efficiencyView = &EfficiencyReport{Jobs: []EfficiencyWait{}}
	q.calls = map[string]*ModelCall{
		"call-running": {ID: "call-running", Class: "experiment", Machine: m.ID, Queued: clock.Now()},
		"call-failed":  {ID: "call-failed", Class: "experiment", Error: "failed", Queued: clock.Now()},
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	options := statusOptions{jobs: "all", full: true}
	want, err := q.publicStatusSnapshot(&options)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/api/status?detail=full", nil)
	r.Header.Set("Authorization", "Bearer test")
	w := &historyCommitWriter{ResponseRecorder: httptest.NewRecorder(), commit: func() {
		if err := q.update(func() error { q.touchJob("job-0").Reason = "new generation"; return nil }); err != nil {
			t.Error(err)
		}
	}}
	(API{Queue: q, Token: "test"}).Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var got Snapshot
	if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err, w.Body.String())
	}
	a, _ := json.Marshal(want)
	b, _ := json.Marshal(got)
	var av, bv any
	_ = json.Unmarshal(a, &av)
	_ = json.Unmarshal(b, &bv)
	if !reflect.DeepEqual(av, bv) {
		t.Fatalf("history changed:\n%s\n%s", a, b)
	}
	current, err := q.publicStatusSnapshot(&options)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range current.Jobs {
		if j.ID == "job-0" && j.Reason != "new generation" {
			t.Fatal("stream mutated published history")
		}
	}
}

type historyCommitWriter struct {
	*httptest.ResponseRecorder
	commit  func()
	observe func()
}

func (w *historyCommitWriter) Write(b []byte) (int, error) {
	if w.observe != nil {
		w.observe()
	}
	if w.commit != nil {
		f := w.commit
		w.commit = nil
		f()
	}
	return w.ResponseRecorder.Write(b)
}
