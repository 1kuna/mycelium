package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

// ClientPace learns the cost of an observation. Unchanged observations and
// failed attempts double the spacing, without an installation-specific ceiling.
// A change resets it to the measured service cost. Event times and owner
// deadlines can shorten healthy spacing; they never override Retry-After.
// The factors express exponential search and randomized contention avoidance,
// not a duration calibrated to one installation.
type ClientPace struct {
	cost, delay time.Duration
}

func growDuration(d time.Duration) time.Duration {
	if d > time.Duration(math.MaxInt64/2) {
		return time.Duration(math.MaxInt64)
	}
	return d * 2
}
func (p *ClientPace) Observe(elapsed time.Duration, changed bool) {
	elapsed = max(elapsed, time.Nanosecond) // Clock representation, not a polling floor.
	if p.cost == 0 {
		p.cost = elapsed
	} else {
		p.cost += (elapsed - p.cost) / 2
	}
	if changed || p.delay == 0 {
		p.delay = p.cost
	} else {
		p.delay = max(p.cost, growDuration(p.delay))
	}
}
func (p *ClientPace) Delay(now time.Time, event time.Time, failure error, jitter func() float64) time.Duration {
	delay := p.delay
	if jitter == nil {
		jitter = rand.Float64
	}
	// Positive jitter never retries before the server's not-before signal.
	if failure != nil {
		var api *HTTPError
		if errors.As(failure, &api) {
			delay = max(delay, api.RetryAfter)
		}
		delay = time.Duration(min(float64(math.MaxInt64), float64(delay)*(1+jitter())))
	} else if !event.IsZero() && event.After(now) {
		// Approach an expected event geometrically, down to the measured read cost.
		delay = min(delay, max(p.cost, event.Sub(now)/2))
	}
	return max(delay, time.Nanosecond)
}

// Seconds is the existing batch-v1 protocol's whole-second, at-most-20s hold.
// No extra poll sleep is needed when the transport already waited this long.
func (p *ClientPace) Seconds(now time.Time, event time.Time) int {
	return min(20, int(p.Delay(now, event, nil, nil)/time.Second))
}
func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseUint(value, 10, 63); err == nil {
		if seconds > uint64(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return max(0, when.Sub(now))
	}
	return 0
}
func (c Client) clock() Clock {
	if c.Clock != nil {
		return c.Clock
	}
	return RealClock{}
}
func (c Client) pause(ctx context.Context, d time.Duration) error {
	if c.Pause != nil {
		return c.Pause(ctx, d)
	}
	return waitStatusPause(ctx, d)
}

// An explicit HTTP timeout belongs to the API operation, including overload
// pauses and retries. Restarting it on every attempt would make it unbounded.
func (c Client) apiContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.HTTP != nil && c.HTTP.Timeout > 0 {
		return context.WithTimeout(ctx, c.HTTP.Timeout)
	}
	return context.WithCancel(ctx)
}

// WithoutRetries lets an observation loop own pacing and overall timeouts,
// avoiding nested retries that conceal overload or consume its whole deadline.
func (c Client) WithoutRetries() Client { c.singleAttempt = true; return c }

// RetryTransient runs a replay-safe operation under one owner deadline. The
// operation must reconcile uploads/accepted results rather than replaying a
// mutation blindly. No retry count or installation-specific timeout is imposed.
// report receives each transient cause before the learned, Retry-After-aware
// pause. The operation's client disables nested retry loops.
func (c Client) RetryTransient(ctx context.Context, operation func(context.Context, Client) error, report func(error, time.Duration)) error {
	ctx, cancel := c.apiContext(ctx)
	defer cancel()
	pace := ClientPace{}
	one := c.WithoutRetries()
	var last error
	ownerError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A pause timer and owner timer can become ready together. Do not
		// start another attempt before context cancellation is scheduled.
		if end, ok := ctx.Deadline(); ok && !c.clock().Now().Before(end) {
			return context.DeadlineExceeded
		}
		return nil
	}
	for {
		if err := ownerError(); err != nil {
			if last != nil {
				return &clientRetryStopped{last, err}
			}
			return err
		}
		started := c.clock().Now()
		err := operation(ctx, one)
		if err == nil || !retryTransient(err) && !errors.Is(err, ctx.Err()) {
			return err
		}
		if retryTransient(err) {
			last = err
		}
		if stopped := ownerError(); stopped != nil {
			if last != nil {
				return &clientRetryStopped{last, stopped}
			}
			return err
		}
		pace.Observe(c.clock().Now().Sub(started), false)
		delay := pace.Delay(c.clock().Now(), time.Time{}, err, c.Jitter)
		if end, ok := ctx.Deadline(); ok {
			// The owner may end recovery before the server permits a retry.
			// A shortened pause ends observation; it never starts an early retry.
			delay = min(delay, max(0, end.Sub(c.clock().Now())))
		}
		if report != nil {
			report(err, delay)
		}
		if stopped := c.pause(ctx, delay); stopped != nil {
			return &clientRetryStopped{err, stopped}
		}
	}
}

type clientRetryStopped struct{ last, stop error }

func (e *clientRetryStopped) Error() string {
	return fmt.Sprintf("%v; latest transient cause: %v", e.stop, e.last)
}
func (e *clientRetryStopped) Unwrap() []error { return []error{e.last, e.stop} }

func retryTransient(err error) bool {
	if err == nil || IsTLSIdentityError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var api *HTTPError
	if errors.As(err, &api) {
		// Typed storage, integrity and configuration refusals need owner repair.
		if api.Code != "" && api.Code != "request_failed" && api.Code != "queue_stopping" && api.Code != "client_rate_limited" && api.Code != "observation_shed_control_pressure" {
			return false
		}
		return api.Status == 408 || api.Status == 429 || api.Status == 500 || api.Status == 502 || api.Status == 503 || api.Status == 504
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && !dns.Timeout() && !dns.Temporary() {
		return false
	}
	var network net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errFileTransferStalled) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.ETIMEDOUT) || errors.As(err, &network) && (network.Timeout() || network.Temporary())
}

// ClientJobEvent is advisory. It carries metadata across scalar wait replies.
// A duration is meaningful only after execution starts; an overrun is unknown.
func ClientJobEvent(j *Job, now time.Time) time.Time {
	if j == nil {
		return time.Time{}
	}
	event := j.Spec.StartBy
	if j.StartedAt != nil && j.Spec.ExpectedMinutes > 0 {
		end := j.StartedAt.Add(time.Duration(j.Spec.ExpectedMinutes * float64(time.Minute)))
		if end.After(now) {
			return end
		}
	}
	if event != nil && event.After(now) {
		return *event
	}
	return time.Time{}
}

// transferProgress estimates a no-progress window from the slowest observed
// byte service time and gap. Four observed periods allow jitter/retransmission
// without treating total transfer duration as a failure. With no observations,
// there is no defensible throughput estimate: only the owner's context applies.
type transferProgress struct {
	gap     time.Duration
	perByte float64
	seen    bool
	period  time.Time
	bytes   int64
}

// A readiness wait ends a service period; buffered copies between waits belong
// to that same period. Timing only the final poll wait can turn a brief EAGAIN
// race into a microsecond rate, omitting producer/consumer scheduling and work.
func (p *transferProgress) record(now time.Time, elapsed time.Duration, n int, waited bool) {
	if n <= 0 {
		return
	}
	p.bytes += int64(n)
	if !waited {
		return
	}
	if !p.period.IsZero() {
		elapsed = now.Sub(p.period)
	}
	p.observe(elapsed, int(p.bytes))
	p.period, p.bytes = now, 0
}

func (p *transferProgress) observe(elapsed time.Duration, n int) {
	if n <= 0 {
		return
	}
	// The first readiness wait may just catch bytes already arriving at
	// connection startup. It establishes an observation, not a service period.
	if !p.seen {
		p.seen = true
		return
	}
	p.gap = max(p.gap, elapsed)
	p.perByte = max(p.perByte, float64(elapsed)/float64(n))
}
func (p *transferProgress) window(bytes int) time.Duration {
	estimate := max(float64(p.gap), p.perByte*float64(bytes))
	return time.Duration(min(float64(math.MaxInt64), estimate*4))
}
