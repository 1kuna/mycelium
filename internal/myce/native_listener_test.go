package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNativeListenerHungProbe(t *testing.T) {
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release); <-finished })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var err error
	// release closes only in cleanup, so returning proves the readiness bound.
	returnsWhileBlocked(t, "hung listener probe", func() {
		err = verifyNativeListenerBounded(ctx, func() error {
			close(started)
			defer close(finished)
			<-release // Model a kernel call that cannot be interrupted by cancellation.
			return nil
		})
	})
	<-started
	var typed *runtimeError
	if !errors.As(err, &typed) || typed.diagnostic.Class != "timeout" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected typed readiness timeout, got %v", err)
	}
}
