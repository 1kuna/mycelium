package main

import (
	"context"
	"io"
	"mycelium/internal/myce"
	"os"
	"time"
)

// The coordinator owns receipts and their fences. Each authority has one
// independent, cancellable network operation; a silent server cannot withhold
// another authority's completion or an owner's deadline.
func runSharedProfileLoop(ctx context.Context, root string, lock *os.File, clock myce.Clock, pause func(context.Context, time.Duration) error, events ...<-chan struct{}) error {
	return runSharedProfileLoopStarted(ctx, root, lock, clock, pause, nil, events...)
}

// Clock and pause are injected so deadlines, backoff, and recovery tests do not sleep.
func waitJobs(ctx context.Context, c myce.Client, ids []string, timeout time.Duration, out, errOut io.Writer, clock myce.Clock, pause func(context.Context, time.Duration) error) error {
	return waitJobsReporting(ctx, c, nil, ids, timeout, out, errOut, clock, pause, false)
}
