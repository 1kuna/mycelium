package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestClientRetryPacingCurrentPressure(t *testing.T) {
	for _, tc := range []struct{ transient, signaled, bounded bool }{{false, true, false}, {true, false, true}} {
		transient, signaled, bounded := tc.transient, tc.signaled, tc.bounded
		t.Run(fmt.Sprintf("transient=%v/signal=%v/deadline=%v", transient, signaled, bounded), func(t *testing.T) {
			clock := &fakeClock{now: time.Now()}
			start := clock.Now()
			ctx := context.Background()
			if bounded {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, start.Add(17*time.Second))
				defer cancel()
			}
			calls, pauses := 0, 0
			var expected time.Duration
			var notBefore time.Time
			c := Client{URL: "http://fixture.invalid", Clock: clock, Jitter: func() float64 { return .5 }, Pause: func(_ context.Context, d time.Duration) error {
				pauses++
				want := expected
				if end, ok := ctx.Deadline(); ok {
					want = min(want, max(0, end.Sub(clock.Now())))
				}
				if d != want {
					t.Fatalf("retry %d pause=%s, current evidence requires %s", pauses, d, want)
				}
				clock.Advance(d)
				return nil
			}, HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
				if clock.Now().Before(notBefore) || bounded && !clock.Now().Before(start.Add(17*time.Second)) {
					t.Fatal("retried before server signal or after owner deadline")
				}
				calls++
				clock.Advance(8 * time.Millisecond)
				header := make(http.Header)
				if calls <= 19 {
					base := time.Second // No signal: RFC 6298 transport recovery period.
					if signaled {
						base = time.Duration((calls-1)%13+1) * time.Second
						header.Set("Retry-After", fmt.Sprint((calls-1)%13+1))
						notBefore = clock.Now().Add(base)
					}
					expected = base * 3 / 2
					return &http.Response{StatusCode: 429, Header: header, Body: io.NopCloser(strings.NewReader(`{"code":"client_rate_limited"}`))}, nil
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"job","state":"succeeded"}`))}, nil
			})}}
			var job *Job
			var err error
			if transient {
				err = c.RetryTransient(ctx, func(ctx context.Context, one Client) error {
					job, err = one.GetJob(ctx, "job")
					return err
				}, nil)
			} else {
				job, err = c.GetJob(ctx, "job")
			}
			if bounded {
				if !errors.Is(err, context.DeadlineExceeded) || clock.Now().Sub(start) != 17*time.Second {
					t.Fatal("owner deadline lost", err, clock.Now().Sub(start))
				}
			} else if err != nil || calls != 20 || pauses != 19 || job.State != "succeeded" {
				t.Fatal("eventual success lost", err, calls, pauses, job)
			}
		})
	}

	// #1072/#1099: a zero-duration reset still observes the owner recovery period.
	t.Run("zero-duration-reset", func(t *testing.T) {
		elapsed := time.Duration(0)
		var failure error = syscall.ECONNRESET

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
		// An explicit sub-second signal overrides the no-signal floor.
		signal := &HTTPError{Status: 503, RetryAfter: 100 * time.Millisecond}
		if pace.Delay(clock.Now(), time.Time{}, signal, func() float64 { return 0 }) != signal.RetryAfter || pace.ServiceDelay(signal, func() float64 { return 0 }) != signal.RetryAfter {
			t.Fatal("server recovery signal lost authority")
		}

	})

}
