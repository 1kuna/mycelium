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
			if len(sleeps) < 4 || sleeps[3] <= sleeps[1] {
				t.Fatal("unchanged reads did not back off", latency, batch, sleeps)
			}
			if batch && (len(holds) == 0 || holds[len(holds)-1] < holds[0]) {
				t.Fatal("batch hold did not adapt", holds)
			}
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
