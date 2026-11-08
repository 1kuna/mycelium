package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

// The incident-size history preserves constrained execution provenance used by
// memory planning: 6,500 retained executions, 110 queued, 1,400 curves, 5 workers.
func submissionAdviceFixture(t testing.TB) *Queue {
	t.Helper()
	q, _ := candidateCostFixture(t)
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return q
}

func BenchmarkSubmissionAdvice(b *testing.B) {
	q := submissionAdviceFixture(b)
	var phases [4]time.Duration
	q.observeSubmissionAdvice = func(timing submissionAdviceTiming) {
		for i, d := range timing.phases {
			phases[i] += d
		}
	}
	var hold time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		j := cloneJob(q.s.Jobs["queued-081"])
		span := &requestSpan{}
		timing := newSubmissionAdviceTiming()
		unlock := requestLock(context.WithValue(context.Background(), requestSpanKey{}, span), &q.mu)
		snapshot := q.captureSubmissionAdvice()
		unlock()
		v, err := snapshot.planner(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		v.submissionDeadlineGuidance(&j, &timing)
		timing.finish(q.observeSubmissionAdvice)
		hold += span.hold
	}
	b.StopTimer()
	b.ReportMetric(float64(hold.Nanoseconds())/float64(b.N), "mutex-ns/op")
	for i, name := range []string{"capture-ns/op", "wait-reason-ns/op", "status-reason-ns/op", "deadline-ns/op"} {
		b.ReportMetric(float64(phases[i].Nanoseconds())/float64(b.N), name)
	}
}

// Freeze bulk decode after the submit transaction. A later control commit must
// finish, and the entire HTTP response must finish while this test owns q.mu.
// This also fences advice to the accepted commit when policy changes meanwhile.
func TestSubmissionAdviceDoesNotHoldQueueAfterCommit(t *testing.T) {
	q, clock := fixture(t)
	q.s.Jobs["retained"] = &Job{ID: "retained", State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	record := readGet(q.readRoot, "jobs/retained")
	decoding, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	go record.once.Do(func() { close(decoding); <-resume; record.value, record.err = decodeReadJob(record.data) })
	<-decoding
	committed := make(chan struct{})
	var observed sync.Once
	q.observeUpdate = func(_, _ time.Duration) { observed.Do(func() { close(committed) }) }
	s := spec("experiment")
	s.SubmissionKey = "advice-commit"
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	handler := API{Queue: q, Token: "test"}.Handler()
	go func() { defer close(done); handler.ServeHTTP(w, r) }()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("submission never committed")
	}
	control := make(chan error, 1)
	go func() {
		_, err := q.SetPriorityPolicy(0, PriorityPolicy{Version: 1, Tiers: []PriorityTier{{"shared", []string{"*/*"}}}})
		control <- err
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("advice blocked a later control commit")
	}
	q.mu.Lock()
	release.Do(func() { close(resume) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		q.mu.Unlock()
		t.Fatal("submission response reacquired q.mu after commit")
	}
	q.mu.Unlock()
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var got Job
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.DeadlineGuidance == nil || got.PriorityPolicy.Tier == "shared" || got.DeadlineGuidance.PrecedenceChange != "higher" {
		t.Fatal("advice lost its submission commit policy", got.PriorityPolicy, got.DeadlineGuidance)
	}
}

func TestSubmissionAdvicePreservesFields(t *testing.T) {
	q := submissionAdviceFixture(t)
	q.mu.Lock()
	snapshot := q.captureSubmissionAdvice()
	q.mu.Unlock()
	v, err := snapshot.planner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"queued-000", "queued-081", "running-0", "retained-000"} {
		before := cloneJob(q.s.Jobs[id])
		want := cloneJob(&before)
		copy := want
		if copy.State == "queued" {
			copy.Reason = q.waitReason(&copy)
			copy.Reason = q.statusWaitingReason(&copy)
		}
		want.PriorityPolicy, want.DeadlineGuidance = q.priorityMatch(want.Spec), q.deadlineGuidance(&copy)
		got := cloneJob(&before)
		timing := newSubmissionAdviceTiming()
		v.submissionDeadlineGuidance(&got, &timing)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s advice changed: got %+v want %+v", id, got.DeadlineGuidance, want.DeadlineGuidance)
		}
		if !reflect.DeepEqual(before, cloneJob(q.s.Jobs[id])) {
			t.Fatal("advice changed durable receipt")
		}
	}
}
