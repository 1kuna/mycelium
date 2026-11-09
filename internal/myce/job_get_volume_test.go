package myce

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type heldVolumeSink struct {
	started chan struct{}
	release chan struct{}
	writes  atomic.Int32
}

func (s *heldVolumeSink) Write(p []byte) (int, error) {
	if s.writes.Add(1) == 1 {
		close(s.started)
	}
	<-s.release
	return len(p), nil
}

func TestJobGetVolumeRejectedObservations(t *testing.T) {
	q, clock := fixture(t)
	// Rejection summaries retain their original counter clock. The old fixed-
	// rate exhaustion is replaced by measured pressure and a real in-flight read.
	rateClock := &fakeClock{now: clock.Now()}
	handler := (API{Queue: q, Token: "fixture"}).handler("", rateClock)
	var output bytes.Buffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	request := func(method, path, agent, token string) int {
		r := httptest.NewRequest(method, path, nil)
		r.RemoteAddr = "192.0.2.123:4567"
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("User-Agent", agent)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	for range 100 {
		if code := request("GET", "/api/jobs/missing", "myce/s073", "fixture"); code != 404 {
			t.Fatal(code)
		}
	}
	busy := holdObservationHTTP(t, handler, "/api/jobs/missing?view=wait")
	defer busy()
	pressure := q.mu.pressure.Load()
	pressure.acquired(nil)
	rateClock.Advance(time.Millisecond)
	pressure.release()
	waiting := pressure.wait()
	rateClock.Advance(2 * time.Millisecond)
	defer func() { pressure.acquired(waiting); pressure.release() }()

	for _, tc := range []struct{ method, path, agent string }{
		{"GET", "/api/jobs/private-job?secret=private-query", "myce/s073"},
		{"HEAD", "/api/jobs/private-job?view=wait", "myce/s073"},
		{"GET", "/api/status", "mycelium-python/development"},
		{"GET", "/api/inspection", "myce/private/path"},
		{"GET", "/api/usage", "Go-http-client/1.1"},
		{"GET", "/api/portability", "myce/s073"},
		{"GET", "/", "myce/s073"},
	} {
		if code := request(tc.method, tc.path, tc.agent, "fixture"); code != 429 {
			t.Fatal(tc.path, code)
		}
	}
	if code := request("GET", "/api/status", "myce/s073", "wrong"); code != 401 {
		t.Fatal(code)
	}
	if strings.Contains(output.String(), "event=job_get_totals") {
		t.Fatal("summary emitted before one minute")
	}
	clock.Advance(time.Minute)
	if code := request("GET", "/api/status", "myce/s073", "fixture"); code != 429 {
		t.Fatal(code)
	}
	_, data, ok := strings.Cut(output.String(), "event=job_get_totals totals=")
	if !ok {
		t.Fatal("rejection did not trigger summary", output.String())
	}
	var totals []jobGetTotal
	if err := json.Unmarshal([]byte(strings.Split(strings.TrimSpace(data), "\n")[0]), &totals); err != nil {
		t.Fatal(err)
	}
	want := []jobGetTotal{
		{Kind: "cli", Version: "s073", Full: 100, Wait: 1, RateLimited: map[string]uint64{"job_full": 1, "job_wait": 1, "portability": 1, "page": 1, "status": 1}},
		{Kind: "python", Version: "development", RateLimited: map[string]uint64{"status": 1}},
		{Kind: "unknown", Version: "unknown", RateLimited: map[string]uint64{"inspection": 1, "usage": 1}},
	}
	gotJSON, _ := json.Marshal(totals)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("rejection attribution %s, want %s", gotJSON, wantJSON)
	}
	for _, private := range []string{"192.0.2.123", "private-job", "private-query", "private/path", "Bearer", "fixture"} {
		if strings.Contains(output.String(), private) {
			t.Fatal("private observation data logged", private)
		}
	}
}
