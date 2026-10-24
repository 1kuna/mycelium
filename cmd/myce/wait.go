package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	receipt := f.String("receipt", "", "private profile receipt directory (managed detached observer)")
	timeout := f.Duration("timeout", 0, "maximum wait duration; does not cancel jobs (default: unlimited)")
	f.Usage = func() {
		fmt.Fprintln(errOut, "Usage: myce wait <job-id> [<job-id>...] [--timeout DURATION]")
		fmt.Fprintln(errOut, "Wait for every job, retrying network/queue outages. Prints each final state and reason once.")
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
			if (key == "queue" || key == "token" || key == "config" || key == "joining-file" || key == "timeout" || key == "receipt") && i+1 < len(args) {
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
	return waitJobs(ctx, client, ids, *timeout, out, errOut, myce.RealClock{}, waitPause)
}

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
	pending := map[string]bool{}
	for _, id := range ids {
		pending[id] = true
	}
	deadline := clock.Now().Add(timeout)
	delay := 3 * time.Second
	failed, retrying := false, false
	for len(pending) > 0 {
		if ctx.Err() != nil {
			return waitError(4, "wait interrupted; jobs continue")
		}
		remaining := deadline.Sub(clock.Now())
		if timeout > 0 && remaining <= 0 {
			return waitError(3, "wait timed out; jobs continue")
		}
		requestCtx := ctx
		cancel := func() {}
		if timeout > 0 {
			requestCtx, cancel = context.WithTimeout(ctx, remaining)
		}
		s, err := status(requestCtx, c)
		deadlineExpired := requestCtx.Err() == context.DeadlineExceeded
		cancel()
		if ctx.Err() != nil {
			return waitError(4, "wait interrupted; jobs continue")
		}
		if timeout > 0 && (!clock.Now().Before(deadline) || deadlineExpired) {
			return waitError(3, "wait timed out; jobs continue")
		}
		if err != nil {
			if myce.IsTLSIdentityError(err) {
				return waitError(2, "wait: tls_identity; check installed queue authority")
			}
			var httpErr *myce.HTTPError
			if errors.As(err, &httpErr) && !myce.RetryStatusRead(err) {
				// Do not echo a remote response body (it may contain credentials).
				return waitError(2, fmt.Sprintf("wait: queue HTTP %d; check authentication and queue address", httpErr.Status))
			}
			if !retrying {
				fmt.Fprintln(errOut, "wait: queue unavailable; retrying")
				retrying = true
			}
			delay = min(delay*2, 10*time.Second)
		} else {
			if retrying {
				fmt.Fprintln(errOut, "wait: queue connection restored")
				retrying = false
			}
			delay = 3 * time.Second
			for id := range pending {
				j, err := myce.FindJob(s.Jobs, id)
				if err != nil {
					return waitError(2, "wait: "+err.Error())
				}
				if j.ID != id {
					delete(pending, id)
					pending[j.ID] = true
				}
			}
			found := map[string]bool{}
			for _, j := range s.Jobs {
				if !pending[j.ID] {
					continue
				}
				found[j.ID] = true
				if j.State != "succeeded" && j.State != "failed" && j.State != "cancelled" {
					continue
				}
				// A reason can contain line breaks; keep one physical line per job.
				reason := strings.Join(strings.Fields(j.Reason), " ")
				if reason != "" {
					reason = " " + reason
				}
				if _, err := fmt.Fprintf(out, "%s %s%s\n", j.ID, j.State, reason); err != nil {
					return &exitError{2, err}
				}
				delete(pending, j.ID)
				failed = failed || j.State != "succeeded"
			}
			for id := range pending {
				if !found[id] {
					return waitError(2, "wait: unknown job ID "+id)
				}
			}
		}
		if len(pending) == 0 {
			break
		}
		if timeout > 0 {
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
