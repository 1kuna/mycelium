package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestClientAdaptiveExplicitHTTPTimeoutBoundsRetries(t *testing.T) {
	for _, submit := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			clock := RealClock{} // synctest advances the injected clock and timers.
			c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return 0 }, HTTP: &http.Client{
				Timeout: time.Second,
				Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
					calls++
					return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {"7"}}, Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, nil
				}),
			}}
			started := clock.Now()
			var err error
			if submit {
				_, err = c.Submit(context.Background(), Spec{SubmissionKey: "same-intent"})
			} else {
				err = c.Call(context.Background(), "GET", "/api/status", nil, nil)
			}
			if !errors.Is(err, context.DeadlineExceeded) || clock.Now().Sub(started) != time.Second || calls != 1 {
				t.Fatalf("owner timeout lost across overload retries: submit=%v elapsed=%s calls=%d err=%v", submit, clock.Now().Sub(started), calls, err)
			}
		})
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
func TestClientAdaptiveTransferProgressClock(t *testing.T) {
	for _, period := range []time.Duration{time.Millisecond, 2 * time.Minute} {
		for _, size := range []int{16, 1 << 20} {
			clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			source := &adaptiveTransferConn{clock: clock, next: period}
			conn := &fileTransferConn{Conn: source, clock: clock}
			data := make([]byte, size)
			start := clock.Now()
			for i := 0; i < 100; i++ {
				if n, err := conn.Read(data); err != nil || n != size {
					t.Fatal("progressing transfer killed", period, size, n, err)
				}
			}
			if clock.Now().Sub(start) != 100*period {
				t.Fatal("wrong observed progress")
			}
			source.next = 2 * period
			if _, err := conn.Read(data); err != nil {
				t.Fatal("slower progress killed", err)
			}
			source.stalled = true
			before := clock.Now()
			if _, err := conn.Read(data); !errors.Is(err, errFileTransferStalled) {
				t.Fatal("stall not detected", err)
			}
			if clock.Now().Sub(before) <= period || clock.Now().Sub(before) >= 100*period {
				t.Fatal("stall did not follow measured throughput", period, clock.Now().Sub(before))
			}
		}
	}
}

func TestClientAdaptiveTransferIncludesServicePeriod(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	source := &adaptiveTransferConn{clock: clock, next: time.Microsecond}
	conn := &fileTransferConn{Conn: source, clock: clock}
	data := make([]byte, 256)
	if _, err := conn.Read(data); err != nil {
		t.Fatal(err)
	}
	// The consumer drains buffered bytes and does local work before the next
	// readiness edge. A near-ready socket poll alone omits that service cost.
	clock.Advance(time.Second)
	if _, err := conn.Read(data); err != nil {
		t.Fatal(err)
	}
	source.next = 50 * time.Millisecond
	if _, err := conn.Read(data); err != nil {
		t.Fatal("brief readiness waits hid the measured service period", err)
	}
	source.stalled = true
	if _, err := conn.Read(data); !errors.Is(err, errFileTransferStalled) {
		t.Fatal("actual absence of progress did not stop", err)
	}
}

func TestClientAdaptiveLongPollCompletion(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	completion := clock.Now().Add(350 * time.Second)
	c := Client{URL: "http://fixture.invalid", Clock: clock, Pause: func(_ context.Context, d time.Duration) error {
		if clock.Now().After(completion.Add(time.Minute)) {
			return errors.New("completion hidden by sleep")
		}
		clock.Advance(d)
		return nil
	}, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		clock.Advance(10 * time.Millisecond)
		if r.Method == "POST" {
			var req WaitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			clock.Advance(min(time.Duration(req.Seconds)*time.Second, max(0, completion.Sub(clock.Now()))))
		}
		state := "running"
		if !clock.Now().Before(completion) {
			state = "succeeded"
		}
		body := `{"id":"job","state":"` + state + `"}`
		if r.Method == "POST" {
			body = `{"jobs":[{"id":"job","state":"` + state + `","revision":1}]}`
		}
		if clock.Now().After(completion.Add(time.Minute)) {
			return nil, errors.New("completion hidden by sleep")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Myce-Wait": {"batch-v1"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	job, err := c.Wait(context.Background(), "job")
	if err != nil || job.State != "succeeded" || clock.Now().After(completion.Add(21*time.Second)) {
		t.Fatal("long-poll completion hidden by sleep", err, clock.Now())
	}
}

func TestClientAdaptiveTransientFileRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, value := range []string{"7", now.Add(7 * time.Second).Format(http.TimeFormat)} {
		err := fileTransferError(&http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {value}}, Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, "transfer failed", now)
		var api *HTTPError
		if !errors.As(err, &api) || api.RetryAfter != 7*time.Second || !strings.Contains(ClientErrorCause(err), "retry_after=7s") {
			t.Fatal(value, err)
		}
	}
}

func TestClientAdaptiveStallTypedEvidence(t *testing.T) {
	for _, branch := range []string{"lock_no_progress", "probe_failed"} {
		t.Run(branch, func(t *testing.T) {
			var logs testLogBuffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
			clock := &fakeClock{now: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
			start := clock.Now()
			evidence := stalledRequest{RequestID: ID(), Job: ID(), Attempt: 2, Offset: 16385, Length: 262144}
			c := Client{URL: "http://private-endpoint.invalid", Token: "private-credential", Clock: clock, Pause: func(_ context.Context, d time.Duration) error { clock.Advance(d); return nil }, HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				clock.Advance(20 * time.Millisecond)
				if branch == "probe_failed" {
					return nil, &net.OpError{Op: "private-operation", Err: errors.New("private-transport-payload")}
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"queue_progress":{"releases":7,"waiting":2,"p99_ms":20}}`))}, nil
			})}}
			ctx := context.WithValue(context.Background(), stalledRequestKey{}, evidence)
			err := c.watchProgress(ctx, "POST", "/api/workers/log-batch")
			var stall *QueueStalledError
			if !errors.As(err, &stall) {
				t.Fatal(err)
			}
			output := logs.String()
			if strings.Count(output, "event=queue_stalled") != 1 {
				t.Fatal(output)
			}
			var record queueStallEvidence
			at := strings.Index(output, "evidence=")
			if at < 0 {
				t.Fatal(output)
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(output[at+len("evidence="):])), &record); err != nil {
				t.Fatal(err, output)
			}
			if record.Branch != branch || record.stalledRequest != evidence || record.Waited != stall.Waited || record.Window != stall.Window || record.Started != start || record.LastProgress.IsZero() || record.Ended.Sub(start) != stall.Waited || record.Route != "POST /api/workers/log-batch" {
				t.Fatalf("wrong evidence: %+v", record)
			}
			if len(record.Probes) < 4 {
				t.Fatal("missing probe history", record)
			}
			for _, p := range record.Probes {
				if p.RTT != 20*time.Millisecond || p.Ended.Sub(p.Started) != p.RTT {
					t.Fatal(p)
				}
				if branch == "lock_no_progress" && (p.Releases != 7 || p.Waiters != 2 || p.P99MS != 20 || p.Error != "none" || !p.Seen) {
					t.Fatal(p)
				}
				if branch == "probe_failed" && (p.Error != "network" || p.Seen) {
					t.Fatal(p)
				}
			}
			for _, private := range []string{"private-endpoint", "private-credential", "private-operation", "private-transport-payload"} {
				if strings.Contains(output+err.Error(), private) {
					t.Fatal("unsafe diagnostic", output)
				}
			}
			if got := stalledRoute("GET", "/api/jobs/private-payload?token=private"); got != "other" {
				t.Fatal(got)
			}
		})
	}
}
