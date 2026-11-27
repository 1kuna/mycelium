package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"
)

var errWorkerStartAuthorization = errors.New("queue start authorization unavailable")
var errWorkerControlUnreachable = errors.New("queue control endpoint unreachable")

// Distinguish the Started boundary while retaining the authorization family
// used by adapters to keep control-plane failures out of stage-failure paths.
var errWorkerStartHandoff = fmt.Errorf("queue start hand-off unavailable: %w", errWorkerStartAuthorization)

// A failed dial only says that this request did not reach the queue. Give the
// original operation the existing restart window to reconnect. The queue's
// exact session, attempt and native custody checks still decide whether it can
// proceed. An outage that outlasts the window goes through normal cleanup.
type queueDialRecovery struct {
	until time.Time
	err   error
}

func (r *queueDialRecovery) observe(started time.Time, err error) {
	if r.until.IsZero() {
		r.until = started.Add(RestartGrace)
	}
	r.err = err
}

// Completed control-request RTTs share the estimator used by queue contact.
// Worker value copies share this state; sensor validity and report gaps are not
// request latency. No estimate is used as a refusal or release deadline.
type workerControlLatency struct {
	mu    sync.Mutex
	paths map[string]probeEstimate
}

func (s *workerControlLatency) observe(path string, elapsed time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paths == nil {
		s.paths = map[string]probeEstimate{}
	}
	e := s.paths[path]
	if elapsed > 0 {
		e.observe(elapsed.Seconds())
		s.paths[path] = e
	}
	seconds := e.upper()
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}

// A handshake owns one in-flight request until it completes or its owner stops.
// A lost reply is ambiguous: the queue may have committed before losing
// the reply. Replay only these idempotent exact-custody operations, with the
// same identity and the ordinary measured client pacing. Elapsed time never
// proves refusal or release and never creates a new execution.
func (w Worker) controlHandshake(path string, in, out any) (time.Time, error) {
	ctx := w.launchContext()
	c := w.Client
	if w.Clock != nil {
		c.Clock = w.Clock
	}
	latency := w.controlLatency
	if latency == nil {
		latency = &workerControlLatency{}
	}
	base := w.Interval
	if base <= 0 {
		base = defaultWorkerInterval
	}
	var outage queueDialRecovery
	for {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		started := c.clock().Now()
		if outage.err != nil && !started.Before(outage.until) {
			return time.Time{}, outage.err
		}
		var body json.RawMessage
		// In shared mode each retry probes a fresh turn. The mutation
		// remains exact-custody and idempotent if its prior reply was lost.
		err := c.Call(ctx, "POST", path, in, &body)
		measured := latency.observe(path, c.clock().Now().Sub(started))
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		if err == nil {
			// Decode only the final complete reply; a truncated reply cannot leave
			// authorization fields behind for a later incomplete response.
			return started, json.Unmarshal(body, out)
		}
		transient := retryTransient(err)
		var connect *net.OpError
		if errors.As(err, &connect) && connect.Op == "dial" {
			unreachable := fmt.Errorf("%w: %w", errWorkerControlUnreachable, err)
			if !transient && !errors.Is(err, context.DeadlineExceeded) {
				return time.Time{}, unreachable
			}
			outage.observe(started, unreachable)
		}
		var refusal *HTTPError
		if outage.err != nil && errors.As(err, &refusal) && refusal.Code == "shared_queue_control_retry" {
			outage = queueDialRecovery{} // The replacement answered; check-in still owns reconciliation.
		}
		// A per-request transport deadline is not the attempt owner's deadline.
		// Typed refusals (including stale custody) and protocol errors stop now.
		if !transient && !errors.Is(err, context.DeadlineExceeded) {
			return time.Time{}, err
		}
		now := c.clock().Now()
		pace := ClientPace{cost: max(base, measured)}
		delay := pace.Delay(now, time.Time{}, err, c.Jitter)
		if outage.err != nil {
			delay = min(delay, outage.until.Sub(now))
			if delay <= 0 {
				return time.Time{}, outage.err
			}
		}
		if err := c.pause(ctx, delay); err != nil {
			return time.Time{}, err
		}
	}
}

func (w Worker) launchContext() context.Context {
	if w.launchCtx != nil {
		return w.launchCtx
	}
	return context.Background()
}

// A positive reply means this exact attempt is durably started, before execution.
func (w Worker) authorizeStart(j Job, session string, receiptDirs ...string) error {
	return w.authorizeStartWithCustody(j, session, nil, receiptDirs...)
}

func (w Worker) authorizeStartWithCustody(j Job, session string, custody *nativeLaunchCustody, receiptDirs ...string) error {
	timing := w.launchTiming
	defer timing.within(launchStartChecks)()
	if err := w.checkAvailability(); err != nil {
		return err
	}
	if reason := w.storageRootReason(&j); reason != "" {
		return fmt.Errorf("%w: %s", errWorkerStartAuthorization, reason)
	}
	if j.Spec.WallTime != "" {
		if w.Clock == nil || len(receiptDirs) != 1 {
			return errors.New("wall-time enforcement requires owned receipt storage and Clock")
		}
	}
	var reply struct {
		WallTimeRemainingSeconds *float64 `json:"wall_time_remaining_seconds"`
		OK                       bool     `json:"ok"`
		StartGate                string   `json:"start_gate"`
		NativeLaunch             string   `json:"native_launch_protocol"`
	}
	done := timing.within(launchStartedRPC)
	result := Result{LaunchTimings: timing.snapshot(), InputsStagedAt: j.InputsStagedAt, StagingTimings: j.StagingTimings, Machine: w.Machine.ID, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	// The client negotiates optional timing fields on the legacy Result type.
	var request any = result
	if custody != nil {
		request = workerStartedRequest{Result: result, Custody: custody}
	}
	requested, err := w.controlHandshake("/api/workers/started", request, &reply)
	done()
	if err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartHandoff, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol || custody != nil && reply.NativeLaunch != nativeLaunchProtocol {
		return fmt.Errorf("queue start-gate protocol mismatch: upgrade queue and workers together")
	}
	if j.Spec.WallTime != "" {
		if reply.WallTimeRemainingSeconds == nil || *reply.WallTimeRemainingSeconds <= 0 {
			return errors.New("queue did not authorize a positive wall-time allowance")
		}
		if err := saveWorkerWallTime(receiptDirs[0], j, requested, *reply.WallTimeRemainingSeconds); err != nil {
			return err
		}
	}
	if err := w.checkLocalBatchAdmission(HostLaunch{Machine: w.Machine.ID, Session: session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)}); err != nil {
		return err
	}
	// A queue reply can predate a fault observed while the request was in
	// flight. Reuse the live dependency resolver at the payload hand-off.
	if reason := w.storageRootReason(&j); reason != "" {
		return fmt.Errorf("%w: %s", errWorkerStartAuthorization, reason)
	}
	if err := w.launchContext().Err(); err != nil {
		return err
	}
	return w.checkAvailability()
}
