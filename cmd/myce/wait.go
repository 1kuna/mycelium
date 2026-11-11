package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"mycelium/internal/myce"
)

// Preserve the other commands' exit behavior while giving wait a shell contract.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, myce.ErrMemoryCeilingsRequired) {
		return 78
	}
	var e *exitError
	if errors.As(err, &e) {
		return e.code
	}
	return 1
}
func waitError(code int, message string) error { return &exitError{code, errors.New(message)} }

func waitCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	f := flag.NewFlagSet("wait", flag.ContinueOnError)
	f.SetOutput(errOut)
	authority := installedClientFlags(f)
	observerStatus := f.Bool("observer-status", false, "show private local completion-observer diagnostics")
	observerDir := f.String("observer-dir", "", "private shared profile observer registry")
	detached := f.Bool("detached", false, "continue after parent exit (explicit managed use only)")
	receipt := f.String("receipt", "", "private profile receipt directory (managed detached observer)")
	jsonOutput := f.Bool("json", false, "print state/reason changes as JSON with full diagnostics")
	timeout := f.Duration("timeout", 0, "maximum wait duration; does not cancel jobs (default: unlimited)")
	f.Usage = func() {
		fmt.Fprintln(errOut, "Usage: myce wait <job-id> [<job-id>...] [--timeout DURATION]")
		fmt.Fprintln(errOut, "Wait for every job, retrying network/queue outages. Prints one short line per state change. Use --json or status for full reasons.")
		fmt.Fprintln(errOut, "Exit codes: 0 all succeeded (or help); 1 any failed/cancelled; 2 usage/auth/API error; 3 timeout (jobs continue); 4 interrupted.")
		f.PrintDefaults()
	}
	// flag stops at the first positional argument; accept flags on either side.
	var flags, ids []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			ids = append(ids, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			key := strings.TrimLeft(a, "-")
			if (key == "queue" || key == "token" || key == "config" || key == "joining-file" || key == "timeout" || key == "receipt" || key == "observer-dir") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		} else {
			ids = append(ids, a)
		}
	}
	if err := f.Parse(flags); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{2, err}
	}
	if *observerStatus {
		root := *observerDir
		if root == "" {
			var err error
			root, err = sharedProfileRoot()
			if err != nil {
				return err
			}
		}
		return observerStatusCommand(root, out)
	}
	if !*detached {
		var cancel context.CancelFunc
		ctx, cancel = waitParentContext(ctx, os.Getppid(), time.Second, os.Getppid)
		defer cancel()
	}
	if *observerDir != "" {
		return waitSharedProfiles(ctx, *observerDir)
	}
	if *receipt != "" {
		return waitProfileReceipt(ctx, *receipt, *timeout)
	}
	if len(ids) == 0 {
		return waitError(2, "usage: myce wait <job-id> [<job-id>...] [--timeout DURATION]")
	}
	explicitTimeout := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "timeout" {
			explicitTimeout = true
		}
	})
	if explicitTimeout && *timeout <= 0 {
		return waitError(2, "timeout must be positive")
	}
	client, err := authority.client()
	if err != nil {
		return &exitError{2, err}
	}
	if client.HTTP != nil {
		defer client.HTTP.CloseIdleConnections()
	}
	// Re-resolving installed authority lets a long wait follow a queue restart
	// or a worker upgrade that replaced the config mid-wait (#607).
	return waitJobsReporting(ctx, client, authority.client, ids, *timeout, out, errOut, myce.RealClock{}, waitPause, *jsonOutput)
}

// Observation uses the owner deadline. A failed read refreshes installed
// authority (#607); there is no fixed read or total-observer deadline.
var waitReadBudget time.Duration // Optional test override; owner deadlines bound reads.

func waitPause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Clock and pause are injected so deadlines, backoff, and recovery tests do not sleep.
func waitJobs(ctx context.Context, c myce.Client, ids []string, timeout time.Duration, out, errOut io.Writer, clock myce.Clock, pause func(context.Context, time.Duration) error) error {
	return waitJobsReconnecting(ctx, c, nil, ids, timeout, out, errOut, clock, pause)
}

func waitJobsReconnecting(ctx context.Context, c myce.Client, reconnect func() (myce.Client, error), ids []string, timeout time.Duration, out, errOut io.Writer, clock myce.Clock, pause func(context.Context, time.Duration) error) error {
	return waitJobsReporting(ctx, c, reconnect, ids, timeout, out, errOut, clock, pause, false)
}

func waitJobsReporting(ctx context.Context, c myce.Client, reconnect func() (myce.Client, error), ids []string, timeout time.Duration, out, errOut io.Writer, clock myce.Clock, pause func(context.Context, time.Duration) error, jsonOutput bool) error {
	pending := map[string]bool{}
	completed := map[string]bool{}
	lastState := map[string]string{}
	lastDetail := map[string]string{}
	observed := map[string]bool{}
	revisions := map[string]uint64{}
	for _, id := range ids {
		pending[id] = true
	}
	if len(pending) > 256 {
		return waitError(2, "wait accepts at most 256 job IDs; use one wait per batch")
	}
	var ownerDeadline time.Time
	if timeout > 0 {
		ownerDeadline = clock.Now().Add(timeout)
	}
	deadline := ownerDeadline
	pace := myce.ClientPace{}
	var diagnosticNotBefore time.Time
	c = c.WithoutRetries()
	c.Clock = clock
	failed, retrying := false, false
	var outageSince, nextRetryLog time.Time
	outageAttempts := 0
	last := map[string]string{}
	metadata := map[string]*myce.Job{}
	var event time.Time
	for len(pending) > 0 {
		if ctx.Err() != nil {
			return waitError(4, "wait interrupted; jobs continue")
		}
		deadline, event = waitPendingTimes(pending, metadata, ownerDeadline, clock.Now())
		// Iterate a fixed set: prefixes are replaced with exact IDs as they resolve.
		ids := make([]string, 0, len(pending))
		for id := range pending {
			ids = append(ids, id)
		}
		started := clock.Now()
		changed := false
		unavailable := false
		var readErr error
		var batchJobs map[string]*myce.Job
		batchMetadata := map[string]bool{}
		var diagnosticDeadline time.Time
		var batchErr error
		held := false
		for _, id := range ids {
			if !pending[id] {
				continue
			}
			deadline, event = waitPendingTimes(pending, metadata, ownerDeadline, clock.Now())
			remaining := deadline.Sub(clock.Now())
			if !deadline.IsZero() && remaining <= 0 {
				return waitError(3, "wait timed out; jobs continue")
			}
			budget := waitReadBudget
			if !deadline.IsZero() {
				if budget == 0 {
					budget = remaining
				} else {
					budget = min(budget, remaining)
				}
			}
			requestCtx, cancel := context.WithCancel(ctx)
			if budget > 0 {
				cancel()
				requestCtx, cancel = context.WithTimeout(ctx, budget)
			}
			var j *myce.Job
			var err error
			if batchJobs == nil {
				batchJobs = map[string]*myce.Job{}
				var reply myce.WaitReply
				seconds := pace.Seconds(clock.Now(), event)
				reply, batchErr = c.WaitBatch(requestCtx, ids, revisions, seconds)
				diagnosticDeadline = clock.Now().Add(myce.WaitBatchMaxSeconds * time.Second)
				if batchErr == nil {
					held = seconds > 0 && len(reply.Jobs) > 0 && reply.Jobs[0].Revision > 0
					for i, state := range reply.Jobs {
						revisions[state.ID] = state.Revision
						batchJobs[ids[i]] = state.Job()
						batchMetadata[ids[i]] = state.Spec != nil
						if state.Spec != nil {
							metadata[ids[i]] = state.Job()
						} else if known := metadata[ids[i]]; known != nil {
							known.State = state.State
						}
					}
				}
			}
			j, err = batchJobs[id], batchErr
			if err == nil && j != nil && !clock.Now().Before(diagnosticNotBefore) &&
				(jsonOutput && lastDetail[j.ID] != j.State+"\x00"+j.Reason || !observed[j.ID] && !batchMetadata[id] && j.State != "succeeded" && j.State != "failed" && j.State != "cancelled") {
				// Spend at most one protocol hold interval on optional diagnostics
				// across this whole batch, not one interval per job. This bounds the
				// extra observation delay without an installation-specific timeout.
				// A slow/shed read leaves details unknown; scalar progress continues.
				deadline, event = waitPendingTimes(pending, metadata, ownerDeadline, clock.Now())
				detailBudget := max(0, diagnosticDeadline.Sub(clock.Now()))
				if !deadline.IsZero() {
					detailBudget = min(detailBudget, max(0, deadline.Sub(clock.Now())))
				}
				if waitReadBudget > 0 {
					detailBudget = min(detailBudget, waitReadBudget)
				}
				detailCtx, detailCancel := context.WithTimeout(requestCtx, detailBudget)
				detail, detailErr := c.GetJob(detailCtx, j.ID)
				if detailCtx.Err() == context.DeadlineExceeded {
					diagnosticDeadline = clock.Now()
				}
				detailCancel()
				if detailErr == nil {
					j = detail
				} else if !myce.RetryStatusRead(detailErr) {
					err = detailErr
				} else {
					var api *myce.HTTPError
					if errors.As(detailErr, &api) {
						diagnosticNotBefore = clock.Now().Add(api.RetryAfter)
					}
				}
			}
			deadlineExpired := requestCtx.Err() == context.DeadlineExceeded
			cancel()
			if ctx.Err() != nil {
				return waitError(4, "wait interrupted; jobs continue")
			}
			if !deadline.IsZero() && (!clock.Now().Before(deadline) || deadlineExpired && budget == remaining) {
				return waitError(3, "wait timed out; jobs continue")
			}
			if err != nil {
				if myce.IsTLSIdentityError(err) {
					return waitError(2, "wait: tls_identity; check installed queue authority")
				}
				var httpErr *myce.HTTPError
				if errors.As(err, &httpErr) && !myce.RetryStatusRead(err) {
					// Do not echo a remote response body (it may contain credentials).
					return waitError(2, fmt.Sprintf("wait: queue HTTP %d; check job ID, authentication and queue address", httpErr.Status))
				}
				unavailable = true
				readErr = err
				if reconnect != nil {
					fmt.Fprintf(errOut, "wait: blocked reading job %s (cause=%s); reconnecting\n", id, waitReadCause(err))
					if c.HTTP != nil {
						c.HTTP.CloseIdleConnections()
					}
					if reconnect != nil {
						if fresh, e := reconnect(); e == nil {
							c = fresh.WithoutRetries()
							c.Clock = clock
						} else {
							fmt.Fprintln(errOut, "wait: reconnect could not resolve queue authority; retrying with the previous one")
						}
					}
				}
				break
			}
			if !observed[j.ID] {
				metadata[j.ID] = j
			}
			if known := metadata[j.ID]; known != nil && known.StartedAt == nil && j.State == "running" {
				now := clock.Now()
				known.StartedAt = &now
			}
			signature := j.State + "\x00" + j.Reason
			changed = changed || last[j.ID] != signature
			last[j.ID] = signature
			observed[j.ID] = true
			terminal := j.State == "succeeded" || j.State == "failed" || j.State == "cancelled"
			if jsonOutput {
				detail := j.State + "\x00" + j.Reason
				if detail != lastDetail[j.ID] {
					if err := json.NewEncoder(out).Encode(j); err != nil {
						return &exitError{2, err}
					}
					lastDetail[j.ID] = detail
				}
			} else if j.State != lastState[j.ID] {
				// Preserve stdout's terminal-state shell contract; progress goes
				// to stderr. Scheduler explanations remain in JSON and status.
				destination := errOut
				if terminal {
					destination = out
				}
				if _, err := fmt.Fprintf(destination, "%s %s\n", j.ID, j.State); err != nil {
					return &exitError{2, err}
				}
				if terminal {
					if err := printJobFinding(errOut, j); err != nil {
						return &exitError{2, err}
					}
				}
				lastState[j.ID] = j.State
			}
			delete(pending, id)
			if j.State != "succeeded" && j.State != "failed" && j.State != "cancelled" {
				pending[j.ID] = true
				continue
			}
			delete(pending, j.ID)
			if completed[j.ID] {
				continue
			}
			completed[j.ID] = true
			failed = failed || j.State != "succeeded"
		}
		if unavailable {
			if !retrying {
				outageSince = clock.Now()
				nextRetryLog = outageSince
				outageAttempts = 0
				retrying = true
			}
			outageAttempts++
			if !clock.Now().Before(nextRetryLog) {
				// Keep evidence of progress during long outages without exposing
				// request URLs, credentials or remote error bodies. At most one
				// periodic line per minute keeps unattended wait logs small.
				fmt.Fprintf(errOut, "wait: queue unavailable; retrying (cause=%s attempts=%d elapsed=%s)\n", waitReadCause(readErr), outageAttempts, clock.Now().Sub(outageSince).Truncate(time.Second))
				nextRetryLog = clock.Now().Add(time.Minute)
			}

		} else {
			if retrying {
				fmt.Fprintln(errOut, "wait: queue connection restored")
				retrying = false
			}

		}
		if len(pending) == 0 {
			break
		}
		deadline, event = waitPendingTimes(pending, metadata, ownerDeadline, clock.Now())
		pace.Observe(clock.Now().Sub(started), changed && !unavailable)
		delay := pace.Delay(clock.Now(), event, readErr, c.Jitter)
		if !unavailable && held {
			delay = 0
		}
		if !deadline.IsZero() {
			if !clock.Now().Before(deadline) {
				return waitError(3, "wait timed out; jobs continue")
			}
			delay = min(delay, deadline.Sub(clock.Now()))
		}
		if err := pause(ctx, delay); err != nil {
			return waitError(4, "wait interrupted; jobs continue")
		}
	}
	if failed {
		return waitError(1, "one or more jobs failed or were cancelled")
	}
	return nil
}

// Return only fixed categories and HTTP status numbers, never raw error text.
func waitReadCause(err error) string {
	var httpErr *myce.HTTPError
	if errors.As(err, &httpErr) {
		return fmt.Sprintf("http_%d", httpErr.Status)
	}
	var network net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network) && network.Timeout() {
		return "timeout"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "response_incomplete"
	}
	return "read_error"
}

// Only pending jobs contribute deadlines/events. Recompute after every reply:
// a completed job or a passed start event cannot constrain a later completion.
func waitPendingTimes(pending map[string]bool, metadata map[string]*myce.Job, owner time.Time, now time.Time) (time.Time, time.Time) {
	deadline := owner
	var event time.Time
	for id := range pending {
		j := metadata[id]
		if j == nil || j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
			continue
		}
		if end := j.Spec.Deadline; end != nil && (deadline.IsZero() || end.Before(deadline)) {
			deadline = *end
		}
		if next := myce.ClientJobEvent(j, now); !next.IsZero() && (event.IsZero() || next.Before(event)) {
			event = next
		}
	}
	return deadline, event
}
