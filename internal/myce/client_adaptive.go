package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

// ClientPace learns the cost of an observation. Unchanged observations double
// the spacing, without an installation-specific ceiling.
// A change resets it to the measured service cost. Event times and owner
// deadlines can shorten healthy spacing. Failures use the current Retry-After
// or measured service cost with the transport recovery floor, independently
// of unchanged-observation history.
// The factors express exponential search and randomized contention avoidance,
// not a duration calibrated to one installation.
type ClientPace struct {
	cost, delay time.Duration
}

// RFC 6298 sections 2.1 and 2.4 specify a one-second initial/minimum TCP
// retransmission timeout. A fast refusal is not evidence that the failed
// service can recover in that RTT. Without a server signal, allow at least
// one transport recovery period; the cost is up to one second before probing
// a fast recovery. Slower measured service still extends it, without a cap.
const clientRecoveryPeriod = time.Second

func (p *ClientPace) recoveryDelay(failure error) time.Duration {
	var api *HTTPError
	if errors.As(failure, &api) && api.RetryAfter > 0 {
		return api.RetryAfter // The current server signal remains authoritative.
	}
	return max(p.cost, clientRecoveryPeriod)
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
		delay = p.recoveryDelay(failure)
		delay = time.Duration(min(float64(math.MaxInt64), float64(delay)*(1+jitter())))
	} else if !event.IsZero() && event.After(now) {
		// Approach an expected event geometrically, down to the measured read cost.
		delay = min(delay, max(p.cost, event.Sub(now)/2))
	}
	return max(delay, time.Nanosecond)
}

// ServiceDelay bounds a shared observer's next read by measured service cost,
// the current server signal, or the no-signal transport recovery period.
// Unchanged long-polls can accumulate
// exponential search history for choosing a protocol hold, but that history is
// not evidence of pressure and must never become a retry or scalar-read timer.
func (p *ClientPace) ServiceDelay(failure error, jitter func() float64) time.Duration {
	cost := max(p.cost, time.Nanosecond)
	delay := cost
	if failure != nil {
		delay = p.recoveryDelay(failure)
		jitterPeriod := cost
		var api *HTTPError
		if !errors.As(failure, &api) || api.RetryAfter <= 0 {
			// No-signal retries also spread across the recovery period.
			jitterPeriod = max(cost, clientRecoveryPeriod)
		}
		if jitter == nil {
			jitter = rand.Float64
		}
		// Spread contenders over one measured service period, never over the
		// accumulated failure count. Integer headroom prevents jitter overflow.
		extra := time.Duration(float64(jitterPeriod) * min(1, max(0, jitter())))
		delay += min(max(0, extra), time.Duration(math.MaxInt64)-delay)
	}
	return delay
}

// retryBackoff paces one operation's retries after consecutive failures with
// no progress between them. Without a server signal the first retry waits
// RFC 6298's one-second recovery period, and each further failure doubles it
// (RFC 6298 section 5.5), never past the measured cost of the work that failed
// since the operation's last progress. A long healthy transfer is progress, so
// one lost reply late in it is retried within seconds instead of after a pause
// as long as the transfer (#2164); repeated failures of expensive work still
// spend about as long waiting as working. Retry-After stays authoritative.
// Jitter spreads contenders across the delay.
type retryBackoff struct{ failures int }

func (b *retryBackoff) progress() { b.failures = 0 }

func (b *retryBackoff) next(failure error, cost time.Duration, jitter func() float64) time.Duration {
	b.failures++
	var api *HTTPError
	if errors.As(failure, &api) && api.RetryAfter > 0 {
		return api.RetryAfter
	}
	limit := max(clientRecoveryPeriod, cost)
	delay := clientRecoveryPeriod
	for i := 1; i < b.failures && delay < limit; i++ {
		delay = growDuration(delay)
	}
	delay = min(delay, limit)
	if jitter == nil {
		jitter = rand.Float64
	}
	extra := time.Duration(float64(delay) * min(1, max(0, jitter())))
	return delay + min(max(0, extra), time.Duration(math.MaxInt64)-delay)
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
	if err := c.ownerError(ctx); err != nil {
		return err
	}
	if end, ok := ctx.Deadline(); ok {
		d = min(d, max(0, end.Sub(c.clock().Now())))
	}
	var err error
	if c.Pause != nil {
		err = c.Pause(ctx, d)
	} else {
		err = waitStatusPause(ctx, d)
	}
	if err != nil {
		return err
	}
	return c.ownerError(ctx)
}

func (c Client) ownerError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Pause and owner timers can become ready together. Consult the injected
	// clock as well, so a shortened pause stops instead of retrying early.
	if end, ok := ctx.Deadline(); ok && !c.clock().Now().Before(end) {
		return context.DeadlineExceeded
	}
	return nil
}

// An explicit HTTP timeout belongs to the API operation, including overload
// pauses and retries. Restarting it on every attempt would make it unbounded.
func (c Client) apiContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.HTTP != nil && c.HTTP.Timeout > 0 {
		return context.WithTimeout(ctx, c.HTTP.Timeout)
	}
	return context.WithCancel(ctx)
}

// WithProgressWatch bounds each call by queue progress instead of a fixed
// timeout. A call that is still waiting after one transport recovery period
// reads GET /api/ping; it keeps waiting while the queue releases its mutex or
// nobody is blocked on it, and fails with QueueStalledError once the release
// count has not moved, with callers waiting, for four of the queue's own p99
// request periods (never less than four transport recovery periods). A queue
// whose ping lacks progress evidence (an older release) leaves the call to the
// owner's deadline. Opt-in: workers pace their own check-ins.
func (c Client) WithProgressWatch() Client { c.watch = true; return c }

const logRequestIDHeader = "X-Myce-Request-ID"

type stalledRequestKey struct{}
type stalledRequest struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Attempt   int    `json:"attempt"`
	Offset    int64  `json:"offset"`
	Length    int    `json:"length"`
}

// IDs are opaque 128-bit queue identities. Never echo arbitrary header/body text.
func safeLogID(id string) string {
	if len(id) != 32 {
		return ""
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
	}
	return id
}

func logRequestEvidence(chunks []LogChunk) stalledRequest {
	var r stalledRequest
	if len(chunks) > 0 {
		r.Job, r.Attempt, r.Offset = safeLogID(chunks[0].ID), chunks[0].Attempt, chunks[0].Offset
		for _, c := range chunks {
			r.Length += len(c.Text)
		}
	}
	return r
}

// A bounded tail preserves per-probe timing and typed outcomes without retaining
// transport strings or growing with a long-running healthy request. At the usual
// four probes/window, eight entries include the preceding window as well.
type stalledProbe struct {
	Started          time.Time     `json:"started"`
	Ended            time.Time     `json:"ended"`
	RTT              time.Duration `json:"rtt_ns"`
	Error            string        `json:"error_class"`
	HTTPStatus       int           `json:"http_status,omitempty"`
	Seen             bool          `json:"seen"`
	PreviousReleases uint64        `json:"previous_releases"`
	Releases         uint64        `json:"releases"`
	Waiters          int64         `json:"waiters"`
	P99MS            float64       `json:"p99_ms"`
}

type queueStallEvidence struct {
	stalledRequest
	Branch        string         `json:"branch"`
	Route         string         `json:"route"`
	Started       time.Time      `json:"started"`
	Ended         time.Time      `json:"ended"`
	Waited        time.Duration  `json:"waited_ns"`
	Window        time.Duration  `json:"window_ns"`
	LastProgress  time.Time      `json:"last_progress"`
	Probes        []stalledProbe `json:"probes"`
	DroppedProbes int            `json:"dropped_probes"`
}

func typedProbeError(err error) (string, int) {
	var api *HTTPError
	var dns *net.DNSError
	var network net.Error
	switch {
	case err == nil:
		return "none", 0
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded", 0
	case errors.Is(err, context.Canceled):
		return "cancelled", 0
	case IsTLSIdentityError(err):
		return "tls_identity", 0
	case errors.As(err, &api):
		return "http", api.Status
	case errors.As(err, &dns):
		return "dns", 0
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset", 0
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused", 0
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof", 0
	case errors.Is(err, io.EOF):
		return "eof", 0
	case errors.As(err, &network):
		if network.Timeout() {
			return "timeout", 0
		}
		return "network", 0
	default:
		return "other", 0
	}
}

// Literal routes only. No endpoints, dynamic IDs, query strings or caller text.
func stalledRoute(method, path string) string {
	switch method + " " + path {
	case "POST /api/workers/log", "POST /api/workers/log-batch", "POST /api/workers/checkin", "POST /api/workers/complete", "POST /api/resolve", "GET /api/status":
		return method + " " + path
	default:
		return "other"
	}
}

// QueueStalledError reports a queue that stopped making progress while this
// call waited. The request may still complete on the queue.
type QueueStalledError struct {
	Method, Path string
	Waited       time.Duration
	Window       time.Duration
	Cause        error // the last probe failure, when the queue gave no answer
}

func (e *QueueStalledError) Error() string {
	cause := "the queue held its lock with callers waiting"
	if e.Cause != nil {
		class, status := typedProbeError(e.Cause)
		cause = fmt.Sprintf("the queue did not answer progress checks (class=%s status=%d)", class, status)
	}
	return fmt.Sprintf("queue_stalled: %s %s waited %s; %s for %s (four measured request periods); the request may still complete. Check the queue with `myce status` or its service log; read-only calls and keyed submissions are safe to repeat", e.Method, e.Path, e.Waited.Round(time.Millisecond), cause, e.Window.Round(time.Millisecond))
}
func (e *QueueStalledError) Unwrap() error { return e.Cause }

func (c Client) callWatched(ctx context.Context, method, path string, in, out any) error {
	var evidence stalledRequest
	switch v := in.(type) {
	case LogChunk:
		evidence = logRequestEvidence([]LogChunk{v})
	case logBatch:
		evidence = logRequestEvidence(v.Chunks)
	}
	evidence.RequestID = ID()
	ctx = context.WithValue(ctx, stalledRequestKey{}, evidence)
	// A batch wait is held on purpose and reads the wait index, never the lock.
	if !c.watch || path == "/api/ping" || path == "/api/wait" {
		return c.callOnce(ctx, method, path, in, out)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := c.watchProgress(ctx, method, path); err != nil {
			cancel(err)
		}
	}()
	err := c.callOnce(ctx, method, path, in, out)
	cancel(nil)
	<-stopped
	// A completed response is authoritative even if a parallel probe timed
	// out at the same instant. Never replace success or an actual refusal
	// with an inference about queue liveness.
	var refusal *HTTPError
	if err == nil || errors.As(err, &refusal) {
		return err
	}
	var stall *QueueStalledError
	if errors.As(context.Cause(ctx), &stall) {
		return stall
	}
	return err
}

// watchProgress returns nil when ctx ends or the queue gives no progress
// evidence, and a QueueStalledError when the queue stopped progressing.
func (c Client) watchProgress(ctx context.Context, method, path string) error {
	clock := c.clock()
	started := clock.Now()
	progressAt := started
	window := 4 * clientRecoveryPeriod
	wait := clientRecoveryPeriod // calls answered within one period never probe
	var releases uint64
	seen := false
	var cause error
	evidence, _ := ctx.Value(stalledRequestKey{}).(stalledRequest)
	record := queueStallEvidence{stalledRequest: evidence, Route: stalledRoute(method, path), Started: started}
	var last QueueProgress
	for {
		if c.pause(ctx, wait) != nil {
			return nil
		}
		probeStarted := clock.Now()
		probeCtx, cancel := context.WithTimeout(ctx, window)
		var ping struct {
			Progress *QueueProgress `json:"queue_progress"`
		}
		err := c.callOnce(probeCtx, "GET", "/api/ping", nil, &ping)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		now := clock.Now()
		probe := stalledProbe{Started: probeStarted, Ended: now, RTT: now.Sub(probeStarted), Seen: seen, PreviousReleases: releases, Releases: releases, Waiters: last.Waiting, P99MS: last.P99MS}
		probe.Error, probe.HTTPStatus = typedProbeError(err)
		var api *HTTPError
		var syntax *json.SyntaxError
		var shape *json.UnmarshalTypeError
		switch {
		case err == nil && ping.Progress == nil, errors.As(err, &api) && api.Status == http.StatusNotFound, errors.As(err, &syntax), errors.As(err, &shape):
			return nil // No evidence from this server: the owner's deadline applies.
		case err != nil:
			cause = err
		default:
			cause = nil
			p := ping.Progress
			rtt := now.Sub(probeStarted)
			window = max(4*clientRecoveryPeriod, time.Duration(min(float64(math.MaxInt64), 4*(p.P99MS*float64(time.Millisecond)+float64(rtt)))))
			if !seen || p.Releases != releases || p.Waiting == 0 {
				progressAt = now
			}
			seen, releases = true, p.Releases
			last = *p
			probe.Seen, probe.Releases, probe.Waiters, probe.P99MS = true, p.Releases, p.Waiting, p.P99MS
		}
		if len(record.Probes) == 8 {
			copy(record.Probes, record.Probes[1:])
			record.Probes = record.Probes[:7]
			record.DroppedProbes++
		}
		record.Probes = append(record.Probes, probe)
		quiet := now.Sub(progressAt)
		if quiet >= window {
			record.Branch = "lock_no_progress"
			if cause != nil {
				record.Branch = "probe_failed"
			}
			record.Ended, record.Waited, record.Window, record.LastProgress = now, now.Sub(started), window, progressAt
			data, _ := json.Marshal(record) // Only typed fields and allowlisted identities.
			log.Printf("event=queue_stalled evidence=%s", data)
			return &QueueStalledError{Method: method, Path: path, Waited: now.Sub(started), Window: window, Cause: cause}
		}
		// Probe about four times per window, and again just as it would close.
		wait = max(clientRecoveryPeriod, min(window/4, window-quiet))
	}
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
	for {
		if err := c.ownerError(ctx); err != nil {
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
		if stopped := c.ownerError(ctx); stopped != nil {
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
	var pending *uploadPendingError
	if errors.As(err, &pending) {
		return true
	}
	var api *HTTPError
	if errors.As(err, &api) {
		// Typed storage, integrity and configuration refusals need owner repair.
		switch api.Code {
		case "", "request_failed", "queue_stopping", "client_rate_limited", "observation_shed_control_pressure",
			"shared_queue_control_retry", "shared_file_store_unready":
		default:
			return false
		}
		return api.Status == 408 || api.Status == 429 || api.Status == 500 || api.Status == 502 || api.Status == 503 || api.Status == 504
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && !dns.Timeout() && !dns.Temporary() {
		return false
	}
	var network net.Error
	// Concurrent HTTP read/write shutdown can surface the closed socket's
	// deadline error instead of the timeout that caused it. Replay-safe callers
	// retry; upload callers first reconcile the server's accepted offset.
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, errFileTransferStalled) ||
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
