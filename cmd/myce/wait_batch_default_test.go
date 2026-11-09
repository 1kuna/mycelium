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
	for _, structured := range []bool{true} {
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
						reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1, Spec: &myce.WaitSpec{}})
					}
					data, _ := json.Marshal(reply)
					body = string(data)
				} else if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/jobs/") {
					gets++
					if batches > 1 && batches < 6 {
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
			expectedGets := 0
			if structured {
				expectedGets = 2 * len(ids)
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

// A full GET may be shed indefinitely while /api/wait remains available.
func TestWaitShedBootstrapStillStartsBatch(t *testing.T) {
	for _, structured := range []bool{true} {
		for _, alreadyDone := range []bool{false} {
			t.Run(fmt.Sprint(structured, alreadyDone), func(t *testing.T) {
				clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
				batches, gets := 0, 0
				client := myce.Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
					clock.advance(time.Millisecond)
					status, body := 200, ""
					if r.Method == "POST" && r.URL.Path == "/api/wait" {
						batches++
						state := "running"
						if alreadyDone || batches >= 4 {
							state = "succeeded"
						}
						body = fmt.Sprintf(`{"jobs":[{"id":"job","state":%q,"revision":1,"spec":{}}]}`, state)
					} else {
						gets++
						status, body = 429, `{"code":"client_rate_limited"}`
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}}
				var out bytes.Buffer
				err := waitJobsReporting(context.Background(), client, nil, []string{"job"}, time.Minute, &out, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }, structured)
				if err != nil || batches == 0 || (!structured && gets != 0) || (structured && gets > 2) || !strings.Contains(out.String(), "succeeded") {
					t.Fatal(err, batches, gets, out.String())
				}
			})
		}
	}
}

func TestWaitJSONSlowOrUnavailableDiagnosticsDoesNotHidePeers(t *testing.T) {
	previous := waitReadBudget
	waitReadBudget = time.Millisecond
	t.Cleanup(func() { waitReadBudget = previous })
	for _, status := range []int{0} {
		clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
		gets, batches := 0, 0
		client := myce.Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
			clock.advance(time.Millisecond)
			if r.Method == "POST" {
				batches++
				var req myce.WaitRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				reply := myce.WaitReply{}
				for _, id := range req.IDs {
					reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: "succeeded", Revision: 1, Spec: &myce.WaitSpec{}})
				}
				data, _ := json.Marshal(reply)
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data)))}, nil
			}
			gets++
			if status == 0 {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"status_unavailable"}`))}, nil
		})}}
		var out bytes.Buffer
		err := waitJobsReporting(context.Background(), client, nil, []string{"a", "b"}, 0, &out, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil }, true)
		if err != nil || batches != 1 || gets < 1 || gets > 2 || strings.Count(out.String(), `"state":"succeeded"`) != 2 {
			t.Fatal(status, err, batches, gets, out.String())
		}
	}
}

func TestWaitOlderBatchMetadataRetainsDeadlineDuringOutage(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	deadline := clock.Now().Add(time.Second)
	gets, batches := 0, 0
	client := myce.Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
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
	})}}
	err := waitJobs(context.Background(), client, []string{"job"}, 0, io.Discard, io.Discard, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if exitCode(err) != 3 || gets != 1 || !clock.Now().Equal(deadline) {
		t.Fatal(err, gets, batches, clock.Now())
	}
}
