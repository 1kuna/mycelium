package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testLatencyBurst = 256
const testRouteBurst = 128

// A cache refresh encloses the whole off-lock read build. Its lifetime must
// not be reported as queue-mutex occupancy, including when a capture is nested.
func TestRequestTimingObservationRefresh(t *testing.T) {
	q, _ := fixture(t)
	span := &requestSpan{}
	ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
	var refresh sync.Mutex
	unlockRefresh, err := requestObservationLock(ctx, &refresh)
	if err != nil {
		t.Fatal(err)
	}
	unlockQueue, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		unlockRefresh()
		t.Fatal(err)
	}
	unlockQueue()
	beforeWait, beforeHold, beforeUnlock := span.wait, span.hold, span.lastUnlock
	if !span.measured || beforeHold <= 0 {
		t.Fatal("queue capture did not record occupancy")
	}
	unlockRefresh()
	if span.wait != beforeWait || span.hold != beforeHold || span.lastUnlock != beforeUnlock {
		t.Fatal("off-lock response build was attributed to the queue mutex")
	}
}

func TestRequestTimingSlowHandler(t *testing.T) {
	q, _ := fixture(t)
	var logs testLogBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	q.requestLatencies.record("POST /test/{id}", time.Millisecond)
	waiting := make(chan struct{})
	done := make(chan struct{})
	mux := newRequestMux()
	mux.HandleFunc("POST /test/{id}", func(w http.ResponseWriter, r *http.Request) {
		close(waiting)
		err := q.updateRequest(r.Context(), func() error {
			<-time.After(25 * time.Millisecond)
			return nil
		}, nil)
		if err != nil {
			t.Error(err)
		}
		// Simulate hydration/response work after the durable transaction unlocks.
		<-time.After(1100 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	})
	q.mu.Lock()
	go func() {
		defer close(done)
		r := httptest.NewRequest("POST", "/test/private-id?token=private-query", strings.NewReader("private-body"))
		r.Header.Set("Authorization", "Bearer private-token")
		q.measureRequests(mux, mux.patterns).ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-waiting
	<-time.After(25 * time.Millisecond)
	q.mu.Unlock()
	<-done
	line := logs.String()
	if strings.Count(line, "event=slow_request") != 1 {
		t.Fatal(line)
	}
	if !strings.Contains(line, `route="POST /test/{id}" status=202`) || !strings.Contains(line, "mutex_measured=true") {
		t.Fatal(line)
	}
	for _, secret := range []string{"private-id", "private-query", "private-body", "private-token"} {
		if strings.Contains(line, secret) {
			t.Fatalf("request data leaked: %s", line)
		}
	}
	var total, wait, hold, after float64
	line = testLogEvent(t, line, "slow_request")
	i := strings.Index(line, "duration_ms=")
	if _, err := fmt.Sscanf(line[i:], "duration_ms=%f mutex_measured=true mutex_wait_ms=%f mutex_hold_ms=%f after_unlock_ms=%f", &total, &wait, &hold, &after); err != nil {
		t.Fatal(err, line)
	}
	if wait < 20 || hold < 20 || after < 1000 || total < wait+hold+after-2 {
		t.Fatalf("incorrect split: %s", line)
	}
	summary := q.requestLatencies.snapshot()
	if len(summary) != 1 || summary[0].Route != "POST /test/{id}" || summary[0].Count != 2 || summary[0].P99MS < 1000 {
		t.Fatal(summary)
	}
}

func TestRequestTimingRollingStatusAndFastSilence(t *testing.T) {
	q, _ := fixture(t)
	var logs testLogBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	h := API{Queue: q, Token: "secret"}.Handler()
	for i := 0; i < testLatencyBurst+10; i++ {
		r := httptest.NewRequest("GET", fmt.Sprintf("/api/jobs/missing-%d?token=hidden", i), nil)
		r.Header.Set("Authorization", "Bearer secret")
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	if strings.Contains(logs.String(), "private") || strings.Contains(logs.String(), "hidden") {
		t.Fatal("private data in locally selected telemetry", logs.String())
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.RequestLatencies) != 1 {
		t.Fatal(snapshot.RequestLatencies)
	}
	s := snapshot.RequestLatencies[0]
	if s.Route != "GET /api/jobs/{id}" || s.Count != testLatencyBurst+10 || s.Samples != testLatencyBurst+10 {
		t.Fatal(s)
	}
	// Nearest rank percentiles describe only the retained window.
	var samples requestLatencies
	for i := 1; i <= testLatencyBurst*2; i++ {
		samples.record("route", time.Duration(i)*time.Millisecond)
	}
	got := samples.snapshot()[0]
	if got.P50MS < 256 || got.P50MS > 512 || got.P95MS < 487 || got.P99MS < 507 || got.P99MS > 512 {
		t.Fatal(got)
	}
	for i := 0; i < testRouteBurst+10; i++ {
		samples.record(fmt.Sprint(i), time.Second)
	}
	if len(samples.snapshot()) != testRouteBurst+11 {
		t.Fatal("trusted route summaries were dropped")
	}
	// Requests without queue work must not claim to have measured a mutex.
	if spanFrom(context.Background()) != nil {
		t.Fatal("unexpected span")
	}
}

func TestRequestTimingHydrationRecovery(t *testing.T) {
	q, _ := fixture(t)
	s := &requestSpan{started: time.Now()}
	ctx := context.WithValue(context.Background(), requestSpanKey{}, s)
	q.observeUpdate = func(time.Duration, time.Duration) { <-time.After(25 * time.Millisecond) }
	reply := Reply{EnvironmentPlan: []EnvironmentRecipe{{Key: "missing", FilesRef: &FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 1}, Launch: &HostLaunch{Attempt: "missing"}}}}
	started := make(chan struct{})
	done := make(chan error, 1)
	q.mu.Lock()
	go func() { close(started); done <- q.hydrateReply(&reply, ctx) }()
	<-started
	<-time.After(25 * time.Millisecond)
	q.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !s.measured || s.wait < 20*time.Millisecond || s.hold < 20*time.Millisecond {
		t.Fatalf("hydration capture/recovery missing: wait=%s hold=%s", s.wait, s.hold)
	}
	if len(reply.EnvironmentPlan) != 0 {
		t.Fatal("undeliverable plan retained")
	}
}

func TestRequestTimingEarlyRouteLabels(t *testing.T) {
	q, _ := fixture(t)
	h := API{Queue: q, Token: "secret"}.Handler()
	for _, tc := range []struct{ method, path, token, route string }{
		{"POST", "/api/jobs", "wrong", "POST /api/jobs"},
		{"POST", "/api/workers/started", "secret", "POST /api/workers/started"},
		{"POST", "/api/invites/redeem", "", "/api/invites/redeem"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		h.ServeHTTP(httptest.NewRecorder(), r)
		found := false
		for _, s := range q.requestLatencies.snapshot() {
			if s.Route == tc.route {
				found = true
			}
		}
		if !found {
			t.Fatalf("route %s lost: %+v", tc.route, q.requestLatencies.snapshot())
		}
	}
}

func TestRequestTimingControlTransactions(t *testing.T) {
	q, _ := fixture(t)
	for name, operation := range map[string]func(context.Context){
		"started": func(ctx context.Context) { _ = q.Started(Result{}, ctx) },
		"cancel":  func(ctx context.Context) { _, _ = q.CancelJob("missing", ctx) },
		"upload": func(ctx context.Context) {
			_ = q.recordFileUpload(strings.Repeat("b", 64), jobInputUploadPurpose, true, ctx)
		},
		"bundle": func(ctx context.Context) { _ = q.recordBundleUpload(strings.Repeat("c", 64), true, ctx) },
	} {
		t.Run(name, func(t *testing.T) {
			s := &requestSpan{started: time.Now()}
			operation(context.WithValue(context.Background(), requestSpanKey{}, s))
			if !s.measured || s.hold <= 0 {
				t.Fatal("queue transaction was not attributed")
			}
		})
	}
	t.Run("submit phases", func(t *testing.T) {
		var measured []submitRequestTiming
		q.observeSubmit = func(p submitRequestTiming) { measured = append(measured, p) }
		spec := cpuSpec(1, 1)
		spec.Name = "phase-probe"
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(string(data)))
		r.Header.Set("Authorization", "Bearer fixture")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "fixture"}.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusOK || len(measured) != 1 {
			t.Fatalf("status=%d observations=%+v body=%s", w.Code, measured, w.Body.String())
		}
		p := measured[0]
		if p.Name != spec.Name || p.Admission <= 0 || p.Decode <= 0 || p.Hold <= 0 || p.SQLiteCommit <= 0 || p.Response <= 0 {
			t.Fatalf("submit phases missing: %+v", p)
		}
		// Hold contains SQLite commit; it is diagnostic, not an extra phase.
		if p.Admission+p.Decode+p.Wait+p.SQLiteCommit+p.Response+p.EvidenceWait > p.Finished.Sub(p.Started) || p.SQLiteCommit > p.Hold {
			t.Fatalf("submit phases overlap: %+v", p)
		}
	})
}

type delayedRequestResponse struct{ *httptest.ResponseRecorder }

func (w delayedRequestResponse) WriteHeader(status int) {
	<-time.After(1100 * time.Millisecond)
	w.ResponseRecorder.WriteHeader(status)
}

func TestRequestTimingConnectRedirectPrivacy(t *testing.T) {
	q, _ := fixture(t)
	var logs testLogBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	h := API{Queue: q, Token: "secret"}.Handler()
	for i := 0; i < testRouteBurst+5; i++ {
		// Both early unauthenticated responses and authenticated mux dispatch can
		// obtain concrete redirect paths. Neither may enter the route table.
		for _, token := range []string{"wrong", "secret"} {
			r := httptest.NewRequest("CONNECT", fmt.Sprintf("http://queue/models/private-model-%d", i), nil)
			r.Header.Set("Authorization", "Bearer "+token)
			h.ServeHTTP(httptest.NewRecorder(), r)
		}
	}
	summary := q.requestLatencies.snapshot()
	if len(summary) != 1 || summary[0].Route != "unmatched" {
		t.Fatal(summary)
	}
	r := httptest.NewRequest("CONNECT", "http://queue/models/private-model-slow", nil)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(delayedRequestResponse{httptest.NewRecorder()}, r)
	if line := logs.String(); strings.Contains(line, "private-model") || !strings.Contains(line, `event=slow_request route="unmatched"`) {
		t.Fatal(line)
	}
	r = httptest.NewRequest("GET", "/api/jobs/missing", nil)
	r.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(httptest.NewRecorder(), r)
	summary = q.requestLatencies.snapshot()
	if len(summary) != 2 || summary[0].Route != "GET /api/jobs/{id}" {
		t.Fatal("legitimate route blocked by CONNECT traffic", summary)
	}
}
