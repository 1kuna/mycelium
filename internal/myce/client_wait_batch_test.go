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

func TestWaitSubmittedDefaultDeadlineAndBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := 0
		c := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			reads++
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, nil
		})}}
		start := time.Now()
		_, err := c.WaitSubmitted(context.Background(), &Job{ID: "job", State: "queued"})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != DefaultWaitTimeout {
			t.Fatal("unbounded submitted wait", err, time.Since(start))
		}
		if reads > int(DefaultWaitTimeout/(10*time.Second))+10 {
			t.Fatal("outage did not back off", reads)
		}
	})
}
func TestWaitBatchInvalidReply(t *testing.T) {
	for _, body := range []string{`{}`, `{"jobs":[{"id":"job"}]}`, `{"jobs":[{"id":"other","state":"queued","revision":1}]}`} {
		c := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		_, err := c.WaitBatch(context.Background(), []string{"job"}, nil, 0)
		var api *HTTPError
		if !errors.As(err, &api) || api.Code != "job_reply_invalid" {
			t.Fatal(body, err)
		}
	}
}
