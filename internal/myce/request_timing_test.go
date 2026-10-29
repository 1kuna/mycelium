package myce

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestTimingSlowHandler(t *testing.T) {
	q, _ := fixture(t)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	waiting := make(chan struct{})
	done := make(chan struct{})
	mux := http.NewServeMux()
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
		q.measureRequests(mux).ServeHTTP(httptest.NewRecorder(), r)
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
	i := strings.Index(line, "duration_ms=")
	if _, err := fmt.Sscanf(line[i:], "duration_ms=%f mutex_measured=true mutex_wait_ms=%f mutex_hold_ms=%f after_unlock_ms=%f", &total, &wait, &hold, &after); err != nil {
		t.Fatal(err, line)
	}
	if wait < 20 || hold < 20 || after < 1000 || total < wait+hold+after-2 {
		t.Fatalf("incorrect split: %s", line)
	}
	summary := q.requestLatencies.snapshot()
	if len(summary) != 1 || summary[0].Route != "POST /test/{id}" || summary[0].Count != 1 || summary[0].P99MS < 1000 {
		t.Fatal(summary)
	}
}

func TestRequestTimingRollingStatusAndFastSilence(t *testing.T) {
	q, _ := fixture(t)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	h := API{Queue: q, Token: "secret"}.Handler()
	for i := 0; i < requestLatencyWindow+10; i++ {
		r := httptest.NewRequest("GET", fmt.Sprintf("/api/jobs/missing-%d?token=hidden", i), nil)
		r.Header.Set("Authorization", "Bearer secret")
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	if strings.Contains(logs.String(), "event=slow_request") {
		t.Fatal(logs.String())
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.RequestLatencies) != 1 {
		t.Fatal(snapshot.RequestLatencies)
	}
	s := snapshot.RequestLatencies[0]
	if s.Route != "GET /api/jobs/{id}" || s.Count != requestLatencyWindow+10 || s.Samples != requestLatencyWindow {
		t.Fatal(s)
	}
	// Nearest rank percentiles describe only the retained window.
	var samples requestLatencies
	for i := 1; i <= requestLatencyWindow*2; i++ {
		samples.record("route", time.Duration(i)*time.Millisecond)
	}
	got := samples.snapshot()[0]
	if got.P50MS != 384 || got.P95MS != 500 || got.P99MS != 510 {
		t.Fatal(got)
	}
	for i := 0; i < requestLatencyRoutes+10; i++ {
		samples.record(fmt.Sprint(i), time.Second)
	}
	if len(samples.snapshot()) != requestLatencyRoutes {
		t.Fatal("route cap exceeded")
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
}
