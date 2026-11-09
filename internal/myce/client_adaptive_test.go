package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestClientAdaptiveWaitLegacyAndBatch(t *testing.T) {
	for _, latency := range []time.Duration{2 * time.Millisecond, 2 * time.Second} {
		for _, batch := range []bool{false, true} {
			clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			calls := 0
			var sleeps []time.Duration
			var holds []int
			c := Client{URL: "http://fixture.invalid", Clock: clock, Pause: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				clock.Advance(d)
				return nil
			}, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				clock.Advance(latency)
				state := "queued"
				if calls >= 7 {
					state = "succeeded"
				}
				headers := make(http.Header)
				if batch {
					headers.Set("X-Myce-Wait", "batch-v1")
				}
				body := `{"id":"job","state":"` + state + `"}`
				if r.URL.Path == "/api/wait" {
					var request WaitRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Fatal(err)
					}
					holds = append(holds, request.Seconds)
					body = `{"jobs":[{"id":"job","state":"` + state + `","revision":1}]}`
				}
				return &http.Response{StatusCode: 200, Header: headers, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			j, err := c.Wait(context.Background(), "job")
			if err != nil || j.State != "succeeded" {
				t.Fatal(j, err)
			}
			if !batch && (len(sleeps) < 4 || sleeps[3] <= sleeps[1]) {
				t.Fatal("unchanged reads did not back off", latency, batch, sleeps)
			}
			if batch && (len(holds) == 0 || holds[len(holds)-1] < holds[0]) {
				t.Fatal("batch hold did not adapt", holds)
			}
		}
	}
}

type adaptiveTransferConn struct {
	net.Conn
	clock    *fakeClock
	next     time.Duration
	deadline time.Time
	stalled  bool
}

func (c *adaptiveTransferConn) SetReadDeadline(end time.Time) error { c.deadline = end; return nil }
func (c *adaptiveTransferConn) Read(p []byte) (int, error) {
	if c.stalled || !c.deadline.IsZero() && c.clock.Now().Add(c.next).After(c.deadline) {
		if c.deadline.IsZero() {
			return 0, errors.New("missing learned stall deadline")
		}
		c.clock.now = c.deadline
		return 0, os.ErrDeadlineExceeded
	}
	c.clock.Advance(c.next)
	return len(p), nil
}

func TestClientAdaptiveTransientStopsPermanent(t *testing.T) {
	for _, failure := range []error{&HTTPError{Status: 401}, &HTTPError{Status: 503, Code: "file_blob_unavailable"}, &HTTPError{Status: 507, Code: "file_store_capacity"}, errors.New("download hash mismatch"), errJoiningIdentity} {
		calls := 0
		c := Client{Pause: func(context.Context, time.Duration) error { t.Fatal("permanent failure slept"); return nil }}
		err := c.RetryTransient(context.Background(), func(context.Context, Client) error { calls++; return failure }, func(error, time.Duration) { t.Fatal("permanent failure reported as retry") })
		if err != failure || calls != 1 {
			t.Fatal(err, calls)
		}
	}
}
