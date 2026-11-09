package myce

import (
	"context"
	"errors"

	"testing"
	"time"
)

func TestAdaptiveReadinessDockerCommandProgress(t *testing.T) {
	d := &dockerAttempt{clock: RealClock{}, timeout: 20 * time.Millisecond, halt: make(chan struct{}), dir: t.TempDir()}
	d.command = func(ctx context.Context, _ ...string) ([]byte, error) {
		for i := 0; i < 5; i++ {
			if err := readinessPause(ctx, 5*time.Millisecond); err != nil {
				return nil, err
			}
			ctx.Value(ioProgressKey{}).(func())()
		}
		return []byte("done"), nil
	}
	if b, err := d.callWithTimeout(d.timeout, "start"); err != nil || string(b) != "done" {
		t.Fatalf("progressing command: %s %v", b, err)
	}
	d.command = func(ctx context.Context, _ ...string) ([]byte, error) { <-ctx.Done(); return nil, context.Cause(ctx) }
	if _, err := d.callWithTimeout(d.timeout, "start"); err == nil {
		t.Fatal("silent command never expired")
	}

	{
		// An unavailable observer can have a very large learned allowance. Halt must
		// cancel its active RPC, without clearing durable custody.
		d.observationBudget = map[string]time.Duration{"inspect": 24 * time.Hour}
		started := make(chan struct{})
		d.command = func(ctx context.Context, _ ...string) ([]byte, error) {
			close(started)
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		result := make(chan error, 1)
		go func() { _, err := d.call("inspect", "owned-id"); result <- err }()
		<-started
		close(d.halt)
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("halt did not cancel active observation")
		}
		if d.state.Released {
			t.Fatal("cancel released unresolved custody")
		}

	}
}
