package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func modelFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureModels(ModelConfig{Idle: 10 * time.Minute, Wait: time.Minute, Models: []Model{{Name: "test", Engine: "fake", Path: "fake", MemoryGB: 8, Platforms: []string{"fake"}}}}); err != nil {
		t.Fatal(err)
	}
	m := machine("one", "fake")
	m.ModelURL = "http://127.0.0.1:1234"
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	return q, clock, m
}
func addModelCall(t *testing.T, q *Queue, priority string) *ModelCall {
	t.Helper()
	c := &ModelCall{Model: "test", Project: "test", Priority: priority}
	if priority == "deadline" {
		d := q.clock.Now().Add(time.Hour)
		c.Deadline = &d
	}
	if err := q.addCall(c); err != nil {
		t.Fatal(err)
	}
	return c
}
func reportModel(t *testing.T, q *Queue, m Machine, state string) Reply {
	t.Helper()
	r, e := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", State: state}}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestModelLoadRouteIdleAndReload(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil || s.State != "loading" || c.Machine != "" {
		t.Fatal("first call must wait for load")
	}
	r := reportModel(t, q, m, "loading")
	if len(r.ModelPlan) != 1 {
		t.Fatal("missing load plan")
	}
	reportModel(t, q, m, "ready")
	if c.Machine != m.ID || s.InFlight != 1 {
		t.Fatal("call not routed to ready model")
	}
	if e := q.finishCall(c.ID); e != nil {
		t.Fatal(e)
	}
	clock.Advance(9 * time.Minute)
	q.Tick()
	if s.State != "ready" {
		t.Fatal("unloaded too early")
	}
	clock.Advance(time.Minute)
	q.Tick()
	if s.State != "draining" {
		t.Fatal("idle model not drained")
	}
	reportModel(t, q, m, "unloaded")
	if len(q.s.ModelSlots) != 0 {
		t.Fatal("unloaded memory not released")
	}
	c = addModelCall(t, q, "production")
	if q.s.ModelSlots[slotKey(m.ID, "test")].State != "loading" || c.Machine != "" {
		t.Fatal("second call must reload")
	}
}
func TestModelPriorityDrainingAndBatchMemory(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	j := submit(t, q, spec("production"))
	r := reportModel(t, q, m, "ready")
	if r.Job != nil {
		t.Fatal("batch started without enough memory")
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s.State != "draining" || s.InFlight != 1 {
		t.Fatal("priority must drain without unloading in-flight call")
	}
	next := addModelCall(t, q, "experiment")
	if next.Machine != "" {
		t.Fatal("new call routed to draining model")
	}
	if e := q.finishCall(c.ID); e != nil {
		t.Fatal(e)
	}
	r = reportModel(t, q, m, "unloaded")
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("production batch must get released memory first")
	}
	if len(q.s.ModelSlots) != 0 {
		t.Fatal("model loaded over batch reservation")
	}
}
func TestModelCallPreemptsResumableBatch(t *testing.T) {
	q, _, m := modelFixture(t)
	j := submit(t, q, spec("experiment"))
	r := check(t, q, m, nil)
	if r.Job == nil {
		t.Fatal("batch not started")
	}
	c := addModelCall(t, q, "production")
	if q.s.Jobs[j.ID].State != "stopping" || c.Machine != "" {
		t.Fatal("production call did not wait for lower priority batch to save")
	}
	finish(t, q, r.Job, true)
	check(t, q, m, nil)
	if q.s.ModelSlots[slotKey(m.ID, "test")] == nil {
		t.Fatal("model not loaded before retried lower priority batch")
	}
}
func TestModelCallStopsNonresumableBatch(t *testing.T) {
	q, _, m := modelFixture(t)
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, q, s)
	check(t, q, m, nil)
	addModelCall(t, q, "production")
	if q.s.Jobs[j.ID].State != "stopping" || len(q.s.ModelSlots) != 0 {
		t.Fatal("nonresumable batch was not preempted")
	}
}
func TestModelLossAndFreshSession(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	other := machine("two", "fake")
	other.ModelURL = "http://127.0.0.1:1235"
	other.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, other, nil)
	clock.Advance(LossAfter)
	check(t, q, other, nil)
	if !q.s.Machines[m.ID].Gone || len(q.s.ModelSlots) != 0 {
		t.Fatal("lost worker still has model")
	}
	q.finishCall(c.ID)
	m.Session = "new"
	if _, e := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: "session-one"}); e != nil {
		t.Fatal(e)
	}
	c = addModelCall(t, q, "production")
	if c.Machine != "" {
		t.Fatal("fresh worker incorrectly has old model")
	}
}
func TestModelCallSharesPriorityOrder(t *testing.T) {
	q, _, m := modelFixture(t)
	low := submit(t, q, spec("experiment"))
	c := addModelCall(t, q, "production")
	r := reportModel(t, q, m, "ready")
	if c.Machine != m.ID || r.Job != nil || q.s.Jobs[low.ID].State != "queued" {
		t.Fatal("batch bypassed production call")
	}
}
func TestModelLoadedAffinity(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	q.finishCall(c.ID)
	other := machine("aaa", "fake")
	other.ModelURL = "http://127.0.0.1:1235"
	other.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, other, nil)
	c = addModelCall(t, q, "production")
	if c.Machine != m.ID || len(q.s.ModelSlots) != 1 {
		t.Fatal("loaded model must win over idle worker")
	}
}
func TestModelConfigAndOpenAIValidation(t *testing.T) {
	q, _, _ := modelFixture(t)
	handler := API{Queue: q, Token: "test-token"}.Handler()
	for _, tc := range []struct {
		body, project, priority string
		status                  int
	}{{`{"model":"test"}`, "", "production", 400}, {`{"model":"unknown"}`, "test", "production", 400}, {`{"model":"test"}`, "test", "invalid", 400}, {`{"model":"test"}`, "test", "deadline", 400}} {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("X-Myce-Project", tc.project)
		req.Header.Set("X-Myce-Priority", tc.priority)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("got %d: %s", w.Code, w.Body.String())
		}
	}
}
func TestDevModelHTTPFormats(t *testing.T) {
	dir := t.TempDir()
	d, e := StartDev(DevConfig{Address: "127.0.0.1:0", Database: dir + "/db", Checkpoints: dir + "/checkpoints", Token: "test", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		req, _ := http.NewRequest("POST", d.URL+path, strings.NewReader(`{"model":"dev-model","messages":[{"role":"user","content":"hello"}],"prompt":"hello","input":"hello"}`))
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("X-Myce-Project", "test")
		req.Header.Set("X-Myce-Priority", "production")
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		data, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || !json.Valid(data) {
			t.Fatalf("%s: %d %s", path, res.StatusCode, data)
		}
	}
	req, _ := http.NewRequestWithContext(context.Background(), "POST", d.URL+"/v1/chat/completions", strings.NewReader(`{"model":"dev-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(string(data), "data: [DONE]") {
		t.Fatalf("stream: %d %s", res.StatusCode, data)
	}
}

func TestModelIdleStartsAfterLoad(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "production")
	clock.Advance(10 * time.Minute)
	reportModel(t, q, m, "ready")
	if c.Machine != m.ID || q.s.ModelSlots[slotKey(m.ID, "test")].State != "ready" {
		t.Fatal("load duration counted as idle time")
	}
}
func TestModelLeaveDrainsBeforeGone(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	if e := q.Leave(m.ID, false); e != nil {
		t.Fatal(e)
	}
	reportModel(t, q, m, "ready")
	if q.s.Machines[m.ID].Gone {
		t.Fatal("worker marked gone before call finished")
	}
	q.finishCall(c.ID)
	reportModel(t, q, m, "unloaded")
	if !q.s.Machines[m.ID].Gone {
		t.Fatal("worker not gone after drained unload")
	}
}

func TestModelRetryAndStreamFailure(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			q, _, m := modelFixture(t)
			var firstCalls, secondCalls atomic.Int32
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstCalls.Add(1)
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: first\n\n")
					w.(http.Flusher).Flush()
					panic(http.ErrAbortHandler)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"partial":`)
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				secondCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"answer":"retried"}`)
			}))
			defer second.Close()
			m.ModelURL = first.URL
			check(t, q, m, nil)
			other := machine("two", "fake")
			other.ModelURL = second.URL
			other.ModelConfigHash = modelConfigHash(q.models)
			check(t, q, other, nil)
			// Pretend both engines loaded, so this test focuses on transport retry.
			q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now()}
			q.s.ModelSlots[slotKey(other.ID, "test")] = &ModelSlot{Model: "test", Machine: other.ID, Session: other.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now()}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			req, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":"test","stream":%v,"messages":[{"role":"user","content":"hi"}]}`, stream)))
			req.Header.Set("Authorization", "Bearer test")
			req.Header.Set("X-Myce-Project", "test")
			req.Header.Set("X-Myce-Priority", "production")
			res, e := http.DefaultClient.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			// Server handler counts become visible when their HTTP response completes.
			if firstCalls.Load() != 1 {
				t.Fatal("first worker not called exactly once")
			}
			if stream {
				if secondCalls.Load() != 0 || !strings.Contains(string(data), "unsafe to retry") {
					t.Fatalf("stream replayed or unclear error: %s", data)
				}
			} else {
				if secondCalls.Load() != 1 || string(data) != `{"answer":"retried"}` {
					t.Fatalf("nonstream not retried intact: %s", data)
				}
			}
		})
	}
}

func TestModelCallUsesFreeWorkerBeforePreemption(t *testing.T) {
	q, _, m := modelFixture(t)
	j := submit(t, q, spec("experiment"))
	check(t, q, m, nil)
	other := machine("two", "fake")
	other.ModelURL = "http://127.0.0.1:1235"
	other.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, other, nil)
	addModelCall(t, q, "production")
	if q.s.Jobs[j.ID].State != "running" || q.s.ModelSlots[slotKey(other.ID, "test")] == nil {
		t.Fatal("preempted batch despite another worker having room")
	}
}

func TestModelWaitTimeoutUsesClock(t *testing.T) {
	q, clock, _ := modelFixture(t)
	clock.Advance(2 * time.Minute)
	// An advancing clock makes the waiting limit deterministic, with no sleep.
	q.clock = &advancingModelClock{now: clock.Now()}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, req)
	if w.Code != 504 || !strings.Contains(w.Body.String(), "maximum model wait exceeded") {
		t.Fatalf("wait timeout: %d %s", w.Code, w.Body.String())
	}
}

type advancingModelClock struct{ now time.Time }

func (c *advancingModelClock) Now() time.Time { c.now = c.now.Add(time.Minute); return c.now }
func TestModelConfigMismatchFailsClearly(t *testing.T) {
	q, _, m := modelFixture(t)
	m.ModelConfigHash = "wrong"
	if _, e := q.Checkin(Checkin{Machine: m}); e == nil || !strings.Contains(e.Error(), "config does not match") {
		t.Fatal("mismatched catalog accepted")
	}
}
func TestModelUnloadFailureKeepsMemory(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	q.finishCall(c.ID)
	submit(t, q, spec("production"))
	if _, e := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", State: "unload_failed", Error: "process survived"}}}); e != nil {
		t.Fatal(e)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil || s.Error != "process survived" || q.s.Machines[m.ID].FreeMemoryGB != 0 {
		t.Fatal("failed unload released memory or hid error")
	}
}

func TestModelLostWorkerRetriesInFlightCall(t *testing.T) {
	q, clock, m := modelFixture(t)
	q.models.Wait = 10 * time.Minute
	started := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer first.Close()
	var secondCalls atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		fmt.Fprint(w, `{"answer":"other worker"}`)
	}))
	defer second.Close()
	m.ModelURL = first.URL
	check(t, q, m, nil)
	other := machine("two", "fake")
	other.ModelURL = second.URL
	other.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, other, nil)
	for _, worker := range []Machine{m, other} {
		q.s.ModelSlots[slotKey(worker.ID, "test")] = &ModelSlot{Model: "test", Machine: worker.ID, Session: worker.Session, State: "ready", Priority: "experiment", LastUsed: clock.Now()}
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	result := make(chan string, 1)
	go func() {
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			result <- e.Error()
			return
		}
		data, _ := io.ReadAll(res.Body)
		res.Body.Close()
		result <- string(data)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first call did not start")
	}
	if e := q.update(func() error {
		clock.Advance(LossAfter)
		q.s.Machines[other.ID].LastSeen = clock.Now()
		q.maintain()
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	select {
	case data := <-result:
		if data != `{"answer":"other worker"}` || secondCalls.Load() != 1 {
			t.Fatalf("lost call not retried once: %s", data)
		}
	case <-ctx.Done():
		t.Fatal("lost request was not cancelled")
	}
	if !q.s.Machines[m.ID].Gone || q.s.ModelSlots[slotKey(m.ID, "test")] != nil {
		t.Fatal("lost inventory remained")
	}
}

func TestModelPriorityTracksRemainingCalls(t *testing.T) {
	q, _, m := modelFixture(t)
	high := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	low := addModelCall(t, q, "experiment")
	if err := q.finishCall(high.ID); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s.Priority != low.Priority || s.InFlight != 1 {
		t.Fatalf("finished production call still protects experiments: %+v", s)
	}
	submit(t, q, spec("deadline"))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if s.State != "draining" {
		t.Fatal("deadline batch cannot reclaim experiment model")
	}
}

func TestIdleModelCanBeReclaimedAfterProductionCall(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	submit(t, q, spec("production"))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.ModelSlots[slotKey(m.ID, "test")].State != "draining" {
		t.Fatal("idle model retains production priority and blocks production batch")
	}
}

func TestModelCallNotBlockedByBatchThatCannotReclaimMemory(t *testing.T) {
	q, _, m := modelFixture(t)
	addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	submit(t, q, spec("deadline"))
	c := addModelCall(t, q, "experiment")
	if c.Machine != m.ID {
		t.Fatal("batch which cannot reclaim production model blocks calls to that model")
	}
}

func TestModelReservationMeasuredMemory(t *testing.T) {
	q, _, m := modelFixture(t)
	for _, state := range []string{"loading", "ready", "draining", "unloading", "unload_failed"} {
		q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: state}
		for _, measured := range []bool{false, true} {
			m.MemoryMeasured = measured
			want := 8.0
			if measured && state != "loading" {
				want = 0
			}
			if got := q.modelReservation(&m, []ModelReport{{Name: "test", State: state}}); got != want {
				t.Fatalf("%s measured=%v: got %v want %v", state, measured, got, want)
			}
		}
	}
	m.MemoryMeasured = true
	q.s.ModelSlots[slotKey(m.ID, "test")].State = "loading"
	if q.modelReservation(&m, nil) != 8 {
		t.Fatal("unreported loading memory not reserved")
	}
	if q.modelReservation(&m, []ModelReport{{Name: "test", State: "ready"}}) != 0 {
		t.Fatal("readiness counted both measured usage and reservation")
	}
}

func TestCallCanUseDoesNotBlockBatchForImpossibleCall(t *testing.T) {
	q, _, m := modelFixture(t)
	q.models.Models[0].MemoryGB = 16
	c := &ModelCall{Model: "test", Priority: "production"}
	if q.callCanUse(q.s.Machines[m.ID], c) {
		t.Fatal("impossible model can block fitting batch")
	}
	q.models.Models[0].MemoryGB = 8
	if !q.callCanUse(q.s.Machines[m.ID], c) {
		t.Fatal("fitting model cannot precede batch")
	}
}

func TestEmptyStreamingResponsePreservesStatus(t *testing.T) {
	q, _, m := modelFixture(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer upstream.Close()
	m.ModelURL = upstream.URL
	check(t, q, m, nil)
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now()}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test","stream":true}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("empty upstream error changed: %d %+v", w.Code, w.Header())
	}
}

func TestFailedUnloadIsNotCountedAsReclaimable(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	s.State, s.Error = "unload_failed", "process survived"
	submit(t, q, spec("production"))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if s.State != "unload_failed" {
		t.Fatalf("scheduler hides failed unload: %+v", s)
	}
}

func TestCancelledModelCallDoesNotLoadRetryWorker(t *testing.T) {
	q, _, m := modelFixture(t)
	started := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer first.Close()
	m.ModelURL = first.URL
	check(t, q, m, nil)
	other := machine("two", "fake")
	other.ModelURL = "http://127.0.0.1:1235"
	other.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, other, nil)
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "experiment", LastUsed: q.clock.Now()}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	done := make(chan struct{})
	go func() {
		defer close(done)
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("call did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled request did not return")
	}
	if q.s.ModelSlots[slotKey(other.ID, "test")] != nil {
		t.Fatal("client cancellation starts unnecessary model on retry worker")
	}
	if len(q.calls) != 0 || q.s.ModelSlots[slotKey(m.ID, "test")].InFlight != 0 {
		t.Fatal("cancelled admission leaked")
	}
}

func TestRetryReleasesOldModelPriorityAndResetsIdle(t *testing.T) {
	q, clock, m := modelFixture(t)
	c := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	addModelCall(t, q, "experiment")
	clock.Advance(time.Minute)
	if err := q.retryCall(c.ID); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s.Priority != "experiment" || s.InFlight != 1 || !s.LastUsed.Equal(clock.Now()) {
		t.Fatalf("old attempt retained reservation priority/idle: %+v", s)
	}
}
