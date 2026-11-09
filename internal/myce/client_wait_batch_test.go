package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// #810 retires the arbitrary 24-hour observer lifetime; the owner supplies it.
func TestWaitSubmittedOwnerDeadlineAndBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 48*time.Hour)
		defer cancel()
		started := time.Now()
		deadline, _ := ctx.Deadline()
		var previousReply time.Time
		retried := false
		c := Client{URL: "http://fixture.invalid", Jitter: func() float64 { return 0 }, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet {
				t.Fatal("waiting must not resubmit or cancel the job", r.Method)
			}
			if !time.Now().Before(deadline) {
				t.Fatal("request at or after owner deadline")
			}
			if !previousReply.IsZero() {
				retried = true
				// No server signal: #1099 uses the RFC 6298 recovery period,
				// independently of failure history. This fixture has zero jitter.
				if spacing := time.Since(previousReply); spacing != time.Second {
					t.Fatal("retry recovery spacing", spacing)
				}
			}
			time.Sleep(time.Millisecond)
			previousReply = time.Now()
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, nil
		})}}
		_, err := c.WaitSubmitted(ctx, &Job{ID: "job", State: "queued"})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 48*time.Hour {
			t.Fatal(err, time.Since(started))
		}
		if !retried {
			t.Fatal("transient failure was not retried")
		}
	})
}
func TestWaitBatchInvalidReply(t *testing.T) {
	for _, body := range []string{`{"jobs":[{"id":"job","state":"queued","revision":1},{"id":"job","state":"queued","revision":"bad"}]}`, `{}`, `{"jobs":[{"id":"job"}]}`, `{"jobs":[{"id":"other","state":"queued","revision":1}]}`} {
		c := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		reply, err := c.WaitBatch(context.Background(), []string{"job"}, nil, 0)
		if len(reply.Jobs) != 0 {
			t.Fatal("unvalidated partial rows escaped", reply)
		}
		var api *HTTPError
		if !errors.As(err, &api) || api.Code != "job_reply_invalid" {
			t.Fatal(body, err)
		}
	}
}

func TestClientTransientProbeRetainsLaterBatchDeadline(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	deadline := clock.Now().Add(2 * time.Second)
	gets, batches := 0, 0
	c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(_ context.Context, d time.Duration) error { clock.Advance(d); return nil }, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		clock.Advance(time.Millisecond)
		status, body := 503, `{"code":"queue_stopping"}`
		if r.Method == "GET" {
			gets++
			status, body = 429, `{"code":"client_rate_limited"}`
		} else {
			batches++
			if batches == 2 {
				status, body = 200, `{"jobs":[{"id":"job","state":"running","revision":1,"spec":{"deadline":"2026-11-08T12:00:02Z"}}]}`
			}
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	_, err := c.Wait(context.Background(), "job")
	if !errors.Is(err, context.DeadlineExceeded) || gets != 1 || batches < 3 || !clock.Now().Equal(deadline) {
		t.Fatal(err, gets, batches, clock.Now())
	}
}
