package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestWaitAdaptiveTinyLargeBatch(t *testing.T) {
	for _, count := range []int{1, 200} {
		for _, latency := range []time.Duration{time.Millisecond, 2 * time.Second} {
			clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			ids := make([]string, count)
			for i := range ids {
				ids[i] = fmt.Sprintf("job-%08d", i)
			}
			batches := 0
			var sleeps []time.Duration
			client := myce.Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
				clock.advance(latency)
				header := make(http.Header)
				header.Set("X-Myce-Wait", "batch-v1")
				var body string
				if r.Method == "GET" {
					body = fmt.Sprintf(`{"id":%q,"state":"queued"}`, strings.TrimPrefix(r.URL.Path, "/api/jobs/"))
				} else {
					batches++
					var req myce.WaitRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Fatal(err)
					}
					if len(req.IDs) != count {
						t.Fatal("batch split", len(req.IDs), count)
					}
					state := "queued"
					if batches == 6 {
						state = "succeeded"
					}
					reply := myce.WaitReply{}
					for _, id := range req.IDs {
						reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
					}
					data, _ := json.Marshal(reply)
					body = string(data)
				}
				return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			var out bytes.Buffer
			err := waitJobs(context.Background(), client, ids, 0, &out, io.Discard, clock, func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				clock.advance(d)
				return nil
			})
			if err != nil || strings.Count(out.String(), " succeeded\n") != count {
				t.Fatal(count, latency, err, out.String())
			}
			if len(sleeps) < 4 || sleeps[3] <= sleeps[1] {
				t.Fatal("unchanged jobs did not back off", count, latency, sleeps)
			}
		}
	}
}
func TestWaitAdaptiveRetryAfterOwnerTimeout(t *testing.T) {
	for _, status := range []int{429, 503} {
		clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
		reads := 0
		var sleeps []time.Duration
		client := myce.Client{URL: "http://fixture.invalid", Jitter: func() float64 { return 0 }, HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
			reads++
			clock.advance(time.Millisecond)
			return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"7"}}, Body: io.NopCloser(strings.NewReader(`{"code":"client_rate_limited"}`))}, nil
		})}}
		err := waitJobs(context.Background(), client, []string{"job"}, 10*time.Second, io.Discard, io.Discard, clock, func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			clock.advance(d)
			return nil
		})
		if exitCode(err) != 3 || reads != 2 || len(sleeps) != 2 || sleeps[0] < 7*time.Second || sleeps[1] >= 7*time.Second {
			t.Fatal(status, err, reads, sleeps)
		}
	}
}
