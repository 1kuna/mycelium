package myce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestJobGetVolumePaysExpensiveSinkWithoutConcurrentSummaries(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1, 0)}
	v := newJobGetVolume(clock)
	sink := &heldVolumeSink{started: make(chan struct{}), release: make(chan struct{})}
	old := log.Writer()
	log.SetOutput(sink)
	defer log.SetOutput(old)
	clock.Advance(time.Nanosecond)
	done := make(chan struct{})
	go func() { defer close(done); v.record("myce/test", false) }()
	<-sink.started
	for range 1023 {
		v.record("myce/test", false)
	}
	clock.Advance(5 * time.Second)
	close(sink.release)
	<-done
	if sink.writes.Load() != 1 || v.logCost < 5*time.Second {
		t.Fatal("summary concurrency or complete sink cost lost", sink.writes.Load(), v.logCost)
	}
	for range 1024 {
		clock.Advance(time.Nanosecond)
		v.record("myce/test", false)
	}
	if sink.writes.Load() > 2 || v.totals[jobGetIdentity{"cli", "test"}].Full != 2048 {
		t.Fatal("sustained traffic logged per request or lost counts", sink.writes.Load())
	}
}

func TestJobGetVolume(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)}
	v := newJobGetVolume(clock)
	var output testLogBuffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	v.record("myce/s072", false)
	v.record("myce/s072", true)
	v.record("mycelium-python/0.1.0", true)
	for _, ua := range []string{"", "Go-http-client/1.1", "Python-urllib/3.12", "myce/private/path", "myce/s072\nsecret"} {
		v.record(ua, false)
	}
	if output.Len() != 0 {
		t.Fatal("per-request logging", output.String())
	}
	clock.Advance(time.Minute)
	v.record("myce/s072", true)
	line := output.String()
	for _, want := range []string{`event=job_get_totals`, `"kind":"cli","version":"s072","full":1,"wait":2`, `"kind":"python","version":"0.1.0","full":0,"wait":1`, `"kind":"unknown","version":"unknown","full":5,"wait":0`} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %s in %s", want, line)
		}
	}
	output.Reset()
	v.record("myce/s072", false)
	if output.Len() != 0 {
		t.Fatal("logged before next minute")
	}
	clock.Advance(time.Minute)
	v.record("myce/s072", false)
	if output.Len() != 0 || v.totals[jobGetIdentity{"cli", "s072"}].Full != 3 {
		t.Fatal("magnitude unchanged or totals reset", output.String())
	}
	for range 5 {
		v.record("myce/s072", false)
	}
	if !strings.Contains(output.String(), `"full":8,"wait":2`) {
		t.Fatal("new traffic magnitude lost cumulative totals", output.String())
	}
}

func TestJobGetVolumeBoundedConcurrent(t *testing.T) {
	v := newJobGetVolume(&fakeClock{now: time.Unix(1, 0)})
	v.identityLimit = 4
	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Go(func() { v.record(fmt.Sprintf("myce/v%d", i), i%2 == 0) })
	}
	wg.Wait()
	v.record("", false)
	if len(v.totals) > v.identityLimit+3 {
		t.Fatal("unbounded identities", len(v.totals))
	}
	var full, wait uint64
	for _, total := range v.totals {
		full += total.Full
		wait += total.Wait
	}
	if full != 501 || wait != 500 {
		t.Fatal("lost counts", full, wait)
	}
	if v.totals[jobGetIdentity{"unknown", "unknown"}].Full != 1 {
		t.Fatal("missing unknown bucket")
	}
}

func TestJobGetVolumeHTTPRouting(t *testing.T) {
	q, clock := fixture(t)
	handler := (API{Queue: q, Token: "fixture"}).Handler()
	var output testLogBuffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	request := func(path, token, agent string) {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("User-Agent", agent)
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}
	request("/api/jobs/missing?view=wait", "fixture", "myce/s072")
	request("/api/jobs/missing?view=wait", "wrong", "myce/s072")
	request("/api/ping", "fixture", "myce/s072")
	clock.Advance(time.Minute)
	request("/api/jobs/missing", "fixture", "")
	line := output.String()
	if strings.Count(line, "event=job_get_totals") != 1 || !strings.Contains(line, `"kind":"cli","version":"s072","full":0,"wait":1`) || !strings.Contains(line, `"kind":"unknown","version":"unknown","full":1,"wait":0`) {
		t.Fatal(line)
	}
}

func TestJobGetVolumeRejectedObservations(t *testing.T) {
	q, clock := fixture(t)
	// Rejection summaries retain their original counter clock. The old fixed-
	// rate exhaustion is replaced by measured pressure and a real in-flight read.
	rateClock := &fakeClock{now: clock.Now()}
	handler := (API{Queue: q, Token: "fixture"}).handler("", rateClock)
	var output testLogBuffer
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

	// Job receipts, usage and full/default status project the committed publication
	// without q.mu, so control pressure does not shed them (#1052, #1592).
	if code := request("GET", "/api/jobs/private-job?secret=private-query", "myce/s073", "fixture"); code != 404 {
		t.Fatal("published job read", code)
	}
	if code := request("GET", "/api/status", "mycelium-python/development", "fixture"); code != 200 {
		t.Fatal("published status read", code)
	}
	if code := request("GET", "/api/status?detail=full", "mycelium-python/development", "fixture"); code != 200 {
		t.Fatal("published full status read", code)
	}
	if code := request("GET", "/api/usage?since=1h", "Go-http-client/1.1", "fixture"); code != 200 {
		t.Fatal("published usage read", code)
	}
	for _, tc := range []struct{ method, path, agent string }{
		{"HEAD", "/api/jobs/private-job?view=wait", "myce/s073"},
		{"GET", "/api/inspection", "myce/private/path"},
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
	if code := request("GET", "/api/inspection", "mycelium-python/development", "fixture"); code != 429 {
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
		{Kind: "cli", Version: "s073", Full: 101, Wait: 1, RateLimited: map[string]uint64{"job_wait": 1, "portability": 1, "page": 1}},
		{Kind: "python", Version: "development", RateLimited: map[string]uint64{"inspection": 1}},
		{Kind: "unknown", Version: "unknown", RateLimited: map[string]uint64{"inspection": 1}},
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

func TestJobGetVolumeRejectedConcurrent(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1, 0)}
	v := newJobGetVolume(clock)
	var output testLogBuffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	var wg sync.WaitGroup
	for i := range 500 {
		wg.Go(func() {
			r := httptest.NewRequest("GET", "/api/jobs/unretained?view=wait", nil)
			r.Header.Set("User-Agent", fmt.Sprintf("myce/v%d", i))
			v.recordRejection(r)
			clock.Advance(time.Second)
			v.recordRejection(r)
		})
	}
	wg.Wait()
	var rejected uint64
	for _, total := range v.totals {
		rejected += total.RateLimited["job_wait"]
		if total.Full != 0 || total.Wait != 0 {
			t.Fatal("rejection counted as admitted observation")
		}
	}
	if rejected != 1000 || len(v.totals) > v.identityLimit+3 {
		t.Fatal("lost or unbounded rejection attribution", rejected, len(v.totals))
	}
	if !strings.Contains(output.String(), `"rate_limited":{"job_wait":`) {
		t.Fatal("missing concurrent rejection summary")
	}
}
