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

func TestClientAdaptiveReadRetryAfter(t *testing.T) {
	for _, status := range []int{429, 503} {
		for _, signal := range []string{"7", "Thu, 05 Nov 2026 12:00:07 GMT"} {
			clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			calls := 0
			var sleeps []time.Duration
			c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return .5 }, Pause: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				clock.Advance(d)
				return nil
			}, HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
				calls++
				clock.Advance(time.Millisecond)
				code, body := 200, `{"id":"job","state":"succeeded"}`
				headers := make(http.Header)
				if calls <= 4 {
					code = status
					body = `{"code":"client_rate_limited"}`
					headers.Set("Retry-After", signal)
				}
				return &http.Response{StatusCode: code, Header: headers, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			if _, err := c.GetJob(context.Background(), "job"); err != nil {
				t.Fatal(err)
			}
			if calls != 5 || len(sleeps) != 4 || sleeps[0] < 7*time.Second {
				t.Fatal(status, signal, calls, sleeps)
			}
			if signal == "7" {
				for _, d := range sleeps {
					if d < 7*time.Second {
						t.Fatal("ignored Retry-After", sleeps)
					}
				}
			}
		}
	}
}

func TestClientAdaptiveBatchWait(t *testing.T) {
	for _, latency := range []time.Duration{2 * time.Millisecond, 2 * time.Second} {
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
		if len(holds) == 0 || holds[len(holds)-1] < holds[0] {
			t.Fatal("batch hold did not adapt", holds)
		}
	}
}

func TestClientAdaptiveNearExpectedEvent(t *testing.T) {
	var pace ClientPace
	for i := 0; i < 12; i++ {
		pace.Observe(time.Millisecond, i == 0)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	event := now.Add(time.Second)
	far := pace.Delay(now, event, nil, nil)
	near := pace.Delay(event.Add(-10*time.Millisecond), event, nil, nil)
	if near >= far {
		t.Fatal("did not speed up near event", far, near)
	}
	if got := pace.Delay(now, event, &HTTPError{Status: 429, RetryAfter: 7 * time.Second}, func() float64 { return 0 }); got < 7*time.Second {
		t.Fatal("event overrode Retry-After", got)
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

func TestClientAdaptiveBufferedSocketProgress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		peer, e := listener.Accept()
		if e != nil {
			return
		}
		defer peer.Close()
		for i := 0; i < 6; i++ {
			if _, e = peer.Write(make([]byte, 8192)); e != nil {
				return
			}
			select {
			case <-time.After(50 * time.Millisecond):
			case <-stop:
				return
			}
		}
		<-stop
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		close(stop)
		<-done
		t.Fatal(err)
	}
	defer func() { close(stop); conn.Close(); <-done }()
	guard := time.AfterFunc(5*time.Second, func() { conn.Close() })
	defer guard.Stop()
	progress := &fileTransferConn{Conn: conn, clock: RealClock{}}
	buffer := make([]byte, 256)
	received := 0
	for received < 6*8192 {
		n, e := progress.Read(buffer)
		if e != nil {
			t.Fatal("buffered copies taught a false network rate", received, e)
		}
		received += n
	}
	if _, err := progress.Read(buffer); !errors.Is(err, errFileTransferStalled) {
		t.Fatal("stopped stream was not detected", err)
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

func TestClientAdaptiveTransientOwnerSpan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls, reports := 0, 0
		c := Client{URL: "http://fixture.invalid", Jitter: func() float64 { return 0 }, HTTP: &http.Client{Timeout: 10 * time.Second, Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			if strings.HasSuffix(r.URL.Path, "preflight") {
				time.Sleep(3 * time.Second)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}
			return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": {"7"}}, Body: io.NopCloser(strings.NewReader(`{"code":"queue_stopping"}`))}, nil
		})}}
		started := time.Now()
		err := c.RetryTransient(context.Background(), func(ctx context.Context, one Client) error {
			if err := one.PreflightFileStore(ctx, nil); err != nil {
				return err
			}
			res, err := one.fileRequest(ctx, "POST", "/api/files/uploads/", nil, nil)
			if err != nil {
				return err
			}
			return fileResponse(res, 201)
		}, func(err error, d time.Duration) {
			reports++
			var api *HTTPError
			if !errors.As(err, &api) || api.RetryAfter != 7*time.Second || d < 7*time.Second {
				t.Fatal(err, d)
			}
		})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 10*time.Second || calls != 2 || reports != 1 {
			t.Fatal("owner span restarted or nested retries hid cause", err, time.Since(started), calls, reports)
		}
	})
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

func TestClientAdaptiveTransientGeneric503WithoutDeadline(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls, reports := 0, 0
	var latest error
	c := Client{Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(ctx context.Context, d time.Duration) error {
		clock.Advance(d)
		// The test owner interrupts after observing continued recovery. This
		// is not a retry budget and uses no wall-clock waiting.
		if reports == 8 {
			cancel()
			return ctx.Err()
		}
		return nil
	}}
	err := c.RetryTransient(ctx, func(context.Context, Client) error {
		calls++
		clock.Advance(time.Millisecond)
		latest = &HTTPError{Status: 503, Code: "request_failed", RetryAfter: time.Duration(calls) * time.Second}
		return latest
	}, func(cause error, d time.Duration) {
		reports++
		if cause != latest || d < time.Duration(calls)*time.Second || ClientErrorCause(cause) != ClientErrorCause(latest) {
			t.Fatal("latest cause or server pacing lost", cause, latest, d)
		}
	})
	if !errors.Is(err, context.Canceled) || calls != 8 || reports != 8 || ClientErrorCause(err) != ClientErrorCause(latest) {
		t.Fatal("unbounded owner lost recovery or latest cause", err, calls, reports)
	}
}

func TestClientAdaptiveTransientGeneric503OwnerDeadline(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	start := clock.Now()
	ctx, cancel := context.WithDeadline(context.Background(), start.Add(10*time.Second))
	defer cancel()
	calls, reports := 0, 0
	var latest error
	c := Client{Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(_ context.Context, d time.Duration) error { clock.Advance(d); return nil }}
	err := c.RetryTransient(ctx, func(context.Context, Client) error {
		calls++
		clock.Advance(time.Second)
		latest = &HTTPError{Status: 503, Code: "request_failed", RetryAfter: 7 * time.Second}
		return latest
	}, func(cause error, _ time.Duration) {
		reports++
		if cause != latest {
			t.Fatal("latest cause lost")
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) || clock.Now().Sub(start) != 10*time.Second || calls != 2 || reports != 2 || ClientErrorCause(err) != ClientErrorCause(latest) {
		t.Fatal("owner deadline not exact or cause lost", err, clock.Now().Sub(start), calls, reports)
	}
}

func TestClientAdaptiveTransientTypedPreparationRefusal(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	calls := 0
	failure := &HTTPError{Status: 503, Code: "file_store_unavailable"}
	c := Client{Clock: clock, Pause: func(context.Context, time.Duration) error { t.Fatal("permanent refusal paused"); return nil }}
	err := c.RetryTransient(context.Background(), func(context.Context, Client) error { calls++; return failure }, func(error, time.Duration) { t.Fatal("permanent refusal retried") })
	if err != failure || calls != 1 {
		t.Fatal("permanent storage refusal did not stop", err, calls)
	}
}
