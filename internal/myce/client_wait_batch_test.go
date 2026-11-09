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
		reads := 0
		c := Client{URL: "http://fixture.invalid", Jitter: func() float64 { return 0 }, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			reads++
			time.Sleep(time.Millisecond)
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, nil
		})}}
		ctx, cancel := context.WithTimeout(context.Background(), 48*time.Hour)
		defer cancel()
		started := time.Now()
		_, err := c.WaitSubmitted(ctx, &Job{ID: "job", State: "queued"})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 48*time.Hour {
			t.Fatal(err, time.Since(started))
		}
		if reads > 32 {
			t.Fatal("no adaptive backoff", reads)
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
