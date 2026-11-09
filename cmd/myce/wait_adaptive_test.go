package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWaitAdaptiveEveryJobInitialDeadline(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	completed := false
	pressure := false
	older := false
	reads, gets, batches := 0, 0, 0
	var methods []string
	var sleeps []time.Duration
	status := 429
	runningReads := 0
	deadline := clock.Now().Add(10 * time.Second)
	initial := map[string]bool{}
	client := myce.Client{URL: "http://fixture.invalid", Jitter: func() float64 { return 0 }, HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
		if pressure {
			reads++
			methods = append(methods, r.Method)
			clock.advance(time.Millisecond)
			return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"7"}}, Body: io.NopCloser(strings.NewReader(`{"code":"client_rate_limited"}`))}, nil
		}
		if older {
			clock.advance(time.Millisecond)
			status, body := 200, ""
			if r.Method == "POST" {
				batches++
				body = `{"jobs":[{"id":"job","state":"running","revision":1}]}`
				if batches > 1 {
					status, body = 503, `{}`
				}
			} else {
				gets++
				body = fmt.Sprintf(`{"id":"job","state":"running","spec":{"deadline":%q}}`, deadline.Format(time.RFC3339))
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		if completed {
			clock.advance(time.Millisecond)
			if r.Method == "POST" {
				return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}
			body := `{"id":"done","state":"succeeded","spec":{"deadline":"2026-11-05T11:00:00Z"}}`
			if strings.Contains(r.URL.Path, "running") {
				runningReads++
				state := "running"
				if runningReads > 1 {
					state = "succeeded"
				}
				body = fmt.Sprintf(`{"id":"running","state":%q}`, state)
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		clock.advance(time.Millisecond)
		var body string
		if r.Method == "GET" {
			t.Fatal("bootstrap GET", r.URL)
		}
		var req myce.WaitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		reply := myce.WaitReply{}
		for _, id := range req.IDs {
			row := myce.WaitState{ID: id, State: "running", Revision: 1, Spec: &myce.WaitSpec{}}
			if id == "b" {
				row.Spec = &myce.WaitSpec{Deadline: &deadline}
			}
			initial[id] = true
			reply.Jobs = append(reply.Jobs, row)
		}
		data, _ := json.Marshal(reply)
		body = string(data)
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Myce-Wait": {"batch-v1"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	err := waitJobs(context.Background(), client, []string{"a", "b"}, time.Minute, io.Discard, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if exitCode(err) != 3 || len(initial) != 2 || !clock.Now().Equal(deadline) {
		t.Fatal("missing initial deadline", err, initial, clock.Now())
	}

	completed = true
	var out bytes.Buffer
	err = waitJobs(context.Background(), client, []string{"done", "running"}, time.Minute, &out, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if err != nil || strings.Count(out.String(), " succeeded\n") != 2 {
		t.Fatal(err, out.String())
	}

	pressure = true
	err = waitJobs(context.Background(), client, []string{"job"}, 10*time.Second, io.Discard, io.Discard, clock, func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		clock.advance(d)
		return nil
	})
	expectedReads := 2
	if strings.Join(methods, ",") != "POST,POST" {
		t.Fatal(methods)
	}
	if exitCode(err) != 3 || reads != expectedReads || len(sleeps) != 2 || sleeps[0] < 7*time.Second || sleeps[1] >= 7*time.Second {
		t.Fatal(status, err, reads, sleeps)
	}
	pressure = false
	older = true
	deadline = clock.Now().Add(time.Second)
	err = waitJobs(context.Background(), client, []string{"job"}, 0, io.Discard, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if exitCode(err) != 3 || gets != 1 || !clock.Now().Equal(deadline) {
		t.Fatal(err, gets, batches, clock.Now())
	}

}
