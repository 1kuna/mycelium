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

func TestWaitDefaultBatchUnchangedJobs(t *testing.T) {
	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprint(structured), func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
			ids := []string{"job-a", "job-b", "job-c"}
			gets, batches := 0, 0
			client := myce.Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
				clock.advance(time.Millisecond)
				state := "running"
				if batches >= 6 {
					state = "succeeded"
				}
				body := ""
				if r.Method == "POST" && r.URL.Path == "/api/wait" {
					var req myce.WaitRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Fatal(err)
					}
					if len(req.IDs) != len(ids) {
						t.Fatal("split wait", req.IDs)
					}
					if batches > 0 && len(req.After) != len(ids) {
						t.Fatal("missing revisions", req.After)
					}
					batches++
					if batches == 6 {
						state = "succeeded"
					}
					reply := myce.WaitReply{}
					for _, id := range req.IDs {
						reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
					}
					data, _ := json.Marshal(reply)
					body = string(data)
				} else if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/jobs/") {
					gets++
					if batches > 0 && batches < 6 {
						t.Fatal("per-job GET during unchanged batch", r.URL)
					}
					body = fmt.Sprintf(`{"id":%q,"state":%q,"spec":{"name":"full-detail"}}`, strings.TrimPrefix(r.URL.Path, "/api/jobs/"), state)
				} else {
					t.Fatal("unexpected request", r.Method, r.URL)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"X-Myce-Wait": {"batch-v1"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			var out bytes.Buffer
			err := waitJobsReporting(context.Background(), client, nil, ids, time.Minute, &out, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }, structured)
			expectedGets := len(ids)
			if structured {
				expectedGets *= 2
			}
			if err != nil || gets != expectedGets || batches != 6 {
				t.Fatal(err, gets, batches, out.String())
			}
			if structured && strings.Count(out.String(), `"name":"full-detail"`) != 6 {
				t.Fatal("lost full JSON diagnostics", out.String())
			}
		})
	}
}
