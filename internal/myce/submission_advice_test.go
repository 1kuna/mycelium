package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
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
	budgetHost(q, 8<<30, 1) // Synthetic headroom keeps the measured copy admitted.
	run := func() (submissionAdviceTiming, time.Duration) {
		j := cloneJob(q.s.Jobs["queued-081"])
		span := &requestSpan{}
		timing := newSubmissionAdviceTiming()
		unlock := requestLock(context.WithValue(context.Background(), requestSpanKey{}, span), &q.mu)
		snapshot := q.captureSubmissionAdvice()
		unlock()
		ctx, release, err := q.requestBudgets.projectionContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		defer release()
		v, err := snapshot.planner(ctx)
		if err != nil {
			b.Fatal(err)
		}
		v.submissionDeadlineGuidance(&j, &timing)
		timing.next(timing.active)
		return timing, span.hold
	}
	// Report the one cold generation decode separately. Unchanged retained
	// records decode only once across commits, so steady-state advice is warm.
	cold, _ := run()
	var phases [4]time.Duration
	var hold time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		timing, locked := run()
		hold += locked
		for i, d := range timing.phases {
			phases[i] += d
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(cold.last.Sub(cold.started).Nanoseconds()), "cold-total-ns")
	b.ReportMetric(float64(cold.phases[adviceCapture].Nanoseconds()), "cold-capture-ns")
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

// A cold retained record can take arbitrarily long to decode. Production's
// accepted receipt must finish while that decode is still blocked, including
// keyed replay, and must keep the committed policy and frozen spec.
func TestProductionSubmitReceiptDoesNotDecodeHistory(t *testing.T) {
	q, clock := fixture(t)
	budgetHost(q, 64<<20, 1)
	q.s.Jobs["retained"] = &Job{ID: "retained", State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	admissionWaitBenchmarkSettle(q)
	record := readGet(q.readRoot, "jobs/retained")
	decoding, resume, decoded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go record.once.Do(func() {
		close(decoding)
		<-resume
		record.value, record.err = decodeReadJob(record.data)
		close(decoded)
	})
	<-decoding
	defer func() { close(resume); <-decoded }()
	handler := API{Queue: q, Token: "test"}.Handler()
	s := spec("production")
	s.SubmissionKey = "production-receipt"
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	for range 2 {
		r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() { handler.ServeHTTP(w, r); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("production receipt waited for optional retained diagnostics")
		}
		var got Job
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		if got.PriorityPolicy.Tier != "production" || got.DeadlineGuidance == nil || !got.DeadlineGuidance.DetailsDeferred || got.Spec.SubmissionKey != s.SubmissionKey {
			t.Fatal("accepted receipt lost its policy/spec or implied complete diagnostics", got)
		}
		if id != "" && got.ID != id {
			t.Fatal("replay created duplicate work")
		}
		id = got.ID
	}
}

func TestSubmissionAdvicePreservesFields(t *testing.T) {
	q := submissionAdviceFixture(t)
	inputs, _, err := q.readGeneration.inputs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	immutableBefore, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		after, err := json.Marshal(inputs)
		if err != nil || !bytes.Equal(immutableBefore, after) {
			t.Error("advice mutated published immutable inputs", err)
		}
	}()
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

// Cold decode and warm-copy refusal must both preserve durable acceptance,
// including replay of the exact keyed job. No oversized copy may bypass charge.
func TestSubmissionAdviceCapacityPreservesAcceptance(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm=%t", warm), func(t *testing.T) {
			q, clock := fixture(t)
			retained := &Job{ID: "retained", State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
			retained.Spec.Run.Command = []string{"echo", strings.Repeat("payload", 20000)}
			q.s.Jobs[retained.ID] = retained
			for i := 1; i < 1024; i++ {
				j := *retained
				j.ID = fmt.Sprintf("retained-%04d", i)
				j.Spec = spec("experiment")
				q.s.Jobs[j.ID] = &j
			}
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			record := readGet(q.readRoot, "jobs/retained")
			if warm {
				if _, _, err := q.readGeneration.inputs(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			budgetHost(q, 128<<10, 1)
			handler := API{Queue: q, Token: "test"}.Handler()
			for _, keyed := range []bool{false, true} {
				s := spec("experiment")
				if keyed {
					s.SubmissionKey = "capacity-accepted"
				}
				var accepted string
				repeats := 1
				if keyed {
					repeats = 2
				}
				for i := 0; i < repeats; i++ {
					data, err := json.Marshal(s)
					if err != nil {
						t.Fatal(err)
					}
					r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
					r.Header.Set("Authorization", "Bearer test")
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code != 200 {
						t.Fatal("durable acceptance lost", w.Code, w.Body.String())
					}
					var got Job
					if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
						t.Fatal(err)
					}
					if got.ID == "" || q.s.Jobs[got.ID] == nil || got.DeadlineGuidance == nil || got.PriorityPolicy.Tier == "" || !strings.Contains(strings.Join(got.DeadlineGuidance.Blockers, " "), "details unavailable") {
						t.Fatal("accepted receipt or honest guidance missing", got.ID, got.DeadlineGuidance)
					}
					if i > 0 && (got.ID != accepted || !got.SubmissionReused) {
						t.Fatal("key replay duplicated accepted work")
					}
					accepted = got.ID
					if q.requestBudgets.held != 0 {
						t.Fatal("advice leaked request capacity")
					}
				}
			}
			if record.decoded.Load() != warm {
				t.Fatal("cold oversized history decoded without capacity")
			}
			if len(q.s.Jobs) != 1026 {
				t.Fatal("advice failure duplicated accepted submissions")
			}
		})
	}
}

// Warm curves with few jobs must not bypass the allowance via private indexes.
// The job/copy allowance fits; catalog construction is the independent expense.
func TestSubmissionAdviceWarmCurveCapacityPreservesAcceptance(t *testing.T) {
	q, _ := fixture(t)
	q.s.WorkObservations = map[string]WorkObservation{}
	for i := range 2048 {
		key := fmt.Sprint(i)
		q.s.WorkObservations[key] = WorkObservation{Key: ObservationKey{Work: key}, MemoryConfiguration: key}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.readGeneration.inputs(context.Background()); err != nil {
		t.Fatal(err)
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	s := spec("experiment")
	s.SubmissionKey = "curve-capacity"
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	request := func() Job {
		r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal("curve capacity lost committed acceptance", w.Code, w.Body.String())
		}
		var got Job
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.ID == "" || got.DeadlineGuidance == nil {
			t.Fatal("accepted receipt missing")
		}
		if q.requestBudgets.held != 0 {
			t.Fatal("curve capacity leaked")
		}
		return got
	}
	budgetHost(q, 256<<10, 1)
	accepted := request()
	if !strings.Contains(strings.Join(accepted.DeadlineGuidance.Blockers, " "), "details unavailable") {
		t.Fatal("warm curve catalog bypassed measured capacity")
	}
	budgetHost(q, 64<<20, 1)
	replay := request()
	if replay.ID != accepted.ID || !replay.SubmissionReused || len(q.s.Jobs) != 1 {
		t.Fatal("curve refusal duplicated accepted work")
	}
	if strings.Contains(strings.Join(replay.DeadlineGuidance.Blockers, " "), "details unavailable") {
		t.Fatal("advice did not recover with measured headroom")
	}
}
