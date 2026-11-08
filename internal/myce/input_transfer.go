package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const inputTransferRecoveringReason = "input transfer recovering; same preparation attempt retained"

var errInputTransferUnreachable = errors.New("input storage unreachable")

type inputTransferRecoveryKey struct{}

func preferTransferError(current, next string) string {
	// Actor maps have no iteration order. A pending read must never replace
	// another attempt's real refusal; choose real errors deterministically.
	switch {
	case next == "" || next == inputTransferRecoveringReason && current != "":
		return current
	case current == "" || current == inputTransferRecoveringReason || next < current:
		return next
	default:
		return current
	}
}

// A completed idle expiry says to resume the immutable read, not that an
// execution was lost. Retain the partial and the preparation owner's context;
// neither an API timeout nor an observation retry creates a new owner here.
func (c Client) recoverInputTransfer(ctx context.Context, ref FileRef, operation string, read func(Client) error) error {
	pending, _ := ctx.Value(inputTransferRecoveryKey{}).(*atomic.Bool)
	if pending != nil {
		defer pending.Store(false)
	}
	one := c.WithoutRetries()
	pace := ClientPace{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Timer delivery can race the recovery pause. Never open another read
		// once the owner's deadline has arrived, even before Done is scheduled.
		if end, ok := ctx.Deadline(); ok && !c.clock().Now().Before(end) {
			return context.DeadlineExceeded
		}
		started := c.clock().Now()
		err := read(one)
		if stopped := ctx.Err(); stopped != nil {
			return stopped
		}
		if err == nil {
			return nil
		}
		// Local writes and syncs never become advisory remote-read recovery,
		// even when their errno also occurs in a transient network failure.
		if errors.Is(err, errFileTransferLocal) {
			return err
		}
		// Do not turn a real queue outage into an endless preparation wait.
		// Unlike a reset/lost body, a completed dial failure has no live peer.
		var transport *net.OpError
		if errors.Is(err, errFileTransferDial) || errors.As(err, &transport) && transport.Op == "dial" {
			return fmt.Errorf("%w: %w", errInputTransferUnreachable, err)
		}
		if !retryTransient(err) {
			return err
		}
		pace.Observe(c.clock().Now().Sub(started), false)
		delay := pace.Delay(c.clock().Now(), time.Time{}, err, c.Jitter)
		if end, ok := ctx.Deadline(); ok {
			if !c.clock().Now().Before(end) {
				return context.DeadlineExceeded
			}
			delay = min(delay, end.Sub(c.clock().Now()))
		}
		if pending != nil {
			pending.Store(true)
		}
		log.Printf("input transfer recovering: %s; retry after %s", withStagingCause("", ref, operation, err), delay)
		if err := c.pause(ctx, delay); err != nil {
			return err
		}
	}
}
