package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func printCancellationScope(w io.Writer, j *myce.Job) {
	if c := j.Cancellation; c != nil && c.Release != nil {
		fmt.Fprintf(w, "  cancellation will_not_run=%t release=%s\n", c.WillNotRun, *c.Release)
	}
	if scope := j.CancellationScope; scope != nil {
		for _, p := range scope.Pieces {
			fmt.Fprintf(w, "  piece %s materialized=%t accepted=%t wave=%s will_not_run=%t release=%s dependents=%s\n", p.Key, p.Materialized, p.Accepted, p.WaveID, p.WillNotRun, p.Release, strings.Join(p.Dependents, ","))
		}
		for _, m := range scope.Members {
			fmt.Fprintf(w, "  member %s state=%s will_not_run=%t release=%s\n", m.JobID, m.State, m.WillNotRun, m.Release)
		}
	}
}

// cancelUnconfirmedExit is myce cancel's exit code when the cancel is
// acknowledged (will_not_run) but release of every attempt is not yet proven.
// 75 is EX_TEMPFAIL: run again or pass --wait.
const cancelUnconfirmedExit = 75

// cancelPollInterval matches myce wait's status poll; release proof arrives on
// the worker's next control exchange, so faster polling only adds queue load.
const cancelPollInterval = 3 * time.Second

// cancelConfirmed is the one resubmission answer: the job will not run and no
// attempt still holds resources.
func cancelConfirmed(a myce.Cancellation) bool {
	return a.WillNotRun && a.Release != nil && (*a.Release == "confirmed" || *a.Release == "not_applicable")
}

// cancelAndConfirm sends one idempotent cancel. With wait it observes the
// stored acknowledgement (job GET, else the cancel retry) until release is
// confirmed. A null release (expired coverage) can never become confirmed.
func cancelAndConfirm(ctx context.Context, c myce.Client, send func(context.Context) (myce.Cancellation, error), wait bool, pause func(context.Context, time.Duration) error) (myce.Cancellation, error) {
	ack, err := send(ctx)
	for err == nil && wait && !cancelConfirmed(ack) && ack.Release != nil {
		if err = pause(ctx, cancelPollInterval); err != nil {
			break
		}
		// The read is preferred: a cancel retry is a queue write.
		if ack.JobID != nil {
			if j, getErr := c.GetJob(ctx, *ack.JobID); getErr == nil && j.Cancellation != nil {
				ack = *j.Cancellation
				continue
			}
		}
		var next myce.Cancellation
		if next, err = send(ctx); err == nil {
			ack = next
		}
	}
	if err != nil && !(wait && errors.Is(err, ctx.Err()) && ack.WillNotRun) {
		return ack, err
	}
	if !cancelConfirmed(ack) {
		release := "unknown"
		if ack.Release != nil {
			release = *ack.Release
		}
		return ack, &exitError{cancelUnconfirmedExit, fmt.Errorf("cancel acknowledged but not confirmed: release=%s; run myce cancel --wait", release)}
	}
	return ack, nil
}

func printCancelAck(w io.Writer, ack myce.Cancellation, err error) error {
	var exit *exitError
	if ack.WillNotRun && (err == nil || errors.As(err, &exit) && exit.code == cancelUnconfirmedExit) {
		if encodeErr := json.NewEncoder(w).Encode(ack); encodeErr != nil {
			return encodeErr
		}
	}
	return err
}
