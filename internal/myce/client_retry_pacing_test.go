package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"
)

func TestClientRetryPacingFastFailureRecoveryPeriod(t *testing.T) {
	for _, elapsed := range []time.Duration{0, time.Millisecond} {
		for _, failure := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, &HTTPError{Status: 503}} {
			t.Run(fmt.Sprintf("elapsed=%s/error=%v", elapsed, failure), func(t *testing.T) {
				clock := &fakeClock{now: time.Now()}
				end := clock.Now().Add(5 * time.Second)
				ctx, cancel := context.WithDeadline(context.Background(), end)
				defer cancel()
				calls := 0
				c := Client{Clock: clock, Jitter: func() float64 { return .5 }, Pause: func(_ context.Context, delay time.Duration) error {
					if want := min(1500*time.Millisecond, end.Sub(clock.Now())); delay != want {
						t.Fatalf("fast failure collapsed recovery period: delay=%s want=%s", delay, want)
					}
					clock.Advance(delay)
					return nil
				}}
				err := c.RetryTransient(ctx, func(context.Context, Client) error {
					calls++
					clock.Advance(elapsed)
					return failure
				}, nil)
				if !errors.Is(err, context.DeadlineExceeded) || !clock.Now().Equal(end) || calls != 4 {
					t.Fatal("recovery lost deadline or bounded request volume", err, clock.Now(), calls)
				}
				var pace ClientPace
				pace.Observe(elapsed, false)
				if got := pace.ServiceDelay(failure, func() float64 { return .5 }); got != 1500*time.Millisecond {
					t.Fatal("shared observer can hot-loop", got)
				}
				// An explicit sub-second signal overrides the no-signal floor.
				signal := &HTTPError{Status: 503, RetryAfter: 100 * time.Millisecond}
				if pace.Delay(clock.Now(), time.Time{}, signal, func() float64 { return 0 }) != signal.RetryAfter || pace.ServiceDelay(signal, func() float64 { return 0 }) != signal.RetryAfter {
					t.Fatal("server recovery signal lost authority")
				}
			})
		}
	}
}

func TestClientRetryPacingDiscardsUnchangedHistory(t *testing.T) {
	var pace ClientPace
	for range 100 {
		pace.Observe(8*time.Millisecond, false)
	}
	for _, signal := range []time.Duration{13 * time.Second, time.Second, 0} {
		want := signal
		if want == 0 {
			want = time.Second
		}
		if got := pace.Delay(time.Time{}, time.Time{}, &HTTPError{Status: 429, RetryAfter: signal}, func() float64 { return .5 }); got != want*3/2 {
			t.Fatalf("history dominated current signal: got %s want %s", got, want*3/2)
		}
	}
	pace.Observe(8*time.Second, false)
	if got := pace.Delay(time.Time{}, time.Time{}, io.EOF, func() float64 { return .5 }); got != 6006*time.Millisecond {
		t.Fatal("fallback lost measured service cost", got)
	}
}

// One failure after progress retries after one recovery period; consecutive
// failures double toward, never past, the failed work's measured cost, and a
// server's Retry-After stays authoritative (#2164).
func TestClientRetryBackoffFollowsFailureHistory(t *testing.T) {
	none := func() float64 { return 0 }
	var b retryBackoff
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := b.next(io.EOF, 5*time.Second, none); got != want {
			t.Fatalf("failure %d waited %s, want %s", i+1, got, want)
		}
	}
	if got := b.next(&HTTPError{Status: 503, RetryAfter: 13 * time.Second}, 5*time.Second, none); got != 13*time.Second {
		t.Fatal("Retry-After lost authority", got)
	}
	b.progress()
	if got := b.next(io.EOF, 9*time.Minute, none); got != time.Second {
		t.Fatal("a failure after progress paid for the whole pass", got)
	}
	if got := b.next(io.EOF, 0, func() float64 { return .5 }); got != 1500*time.Millisecond {
		t.Fatal("fast failures lost the recovery period or its jitter", got)
	}
}
