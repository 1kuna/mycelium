package myce

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testLatencyBurst = 256
const testRouteBurst = 128

func TestRequestTimingSlowHandler(t *testing.T) {
	q, _ := fixture(t)
	var logs bytes.Buffer
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

type delayedRequestResponse struct{ *httptest.ResponseRecorder }

func (w delayedRequestResponse) WriteHeader(status int) {
	<-time.After(1100 * time.Millisecond)
	w.ResponseRecorder.WriteHeader(status)
}

func TestRequestTimingConnectRedirectPrivacy(t *testing.T) {
	q, _ := fixture(t)
	var logs bytes.Buffer
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
