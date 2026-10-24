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
	start := time.Now()
	err := verifyNativeListenerBounded(ctx, func() error {
		close(started)
		defer close(finished)
		<-release // Model a kernel call that cannot be interrupted by cancellation.
		return nil
	})
	<-started
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("hung listener probe exceeded readiness bound: %s", elapsed)
	}
	var typed *runtimeError
	if !errors.As(err, &typed) || typed.diagnostic.Class != "timeout" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected typed readiness timeout, got %v", err)
	}
}
