package myce

import (
	"context"
	"testing"
	"time"
)

func TestCacheCopyLearnsSaturatedAndScalableVolumes(t *testing.T) {
	for _, scales := range []bool{false, true} {
		t.Run(map[bool]string{false: "slow", true: "fast"}[scales], func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			g := &copyGovernor{clock: clock}
			first, err := g.acquire(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer first.release()
			// Model a second waiting request, then report the first actual stream.
			g.mu.Lock()
			g.waiters++
			g.mu.Unlock()
			clock.Advance(time.Second)
			first.progress(1024)
			if g.capacity() != 2 {
				t.Fatal("no demand-driven probe", g.capacity())
			}
			second, err := g.acquire(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer second.release()
			g.mu.Lock()
			g.waiters--
			g.mu.Unlock()
			// Bursty callback ordering must not decide before both streams contribute.
			clock.Advance(time.Second / 2)
			burst := int64(512)
			if !scales {
				burst = 256
			}
			first.progress(burst)
			first.progress(burst)
			if g.capacity() != 2 {
				t.Fatal("partial epoch decided concurrency")
			}
			clock.Advance(time.Second / 2)
			n := int64(0)
			if scales {
				n = 512024
			} else {
				n = 512
			}
			second.progress(n)
			want := 2
			if !scales {
				want = 1
			}
			if g.capacity() != want {
				t.Fatal("wrong measured concurrency", g.capacity(), want)
			}
		})
	}
}
func TestCacheCopyCancellationAndExplicitCeiling(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 1<<30, 1<<29)
	ctx, release, err := b.copy(context.Background(), "files", clock, 1)
	if err != nil {
		t.Fatal(err)
	}
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = b.copy(stopped, "models", clock, 1); err != context.Canceled {
		t.Fatal(err)
	}
	release()
	_, release, err = b.copy(context.Background(), "shared", clock, 1)
	if err != nil {
		t.Fatal("cancelled waiter leaked slot", err)
	}
	release()
}

func TestCacheCopyAdmissionWaitIsNotSourceStall(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 1<<30, 1<<29)
	_, release, err := b.copy(context.Background(), "shared", clock, 1)
	if err != nil {
		t.Fatal(err)
	}
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock, budgets: b}}
	if err = c.cache.init(); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.stageInput(context.Background(), func(ctx context.Context, _ string) error {
			progress := ctx.Value(cacheAdmissionKey{}).(func(bool))
			ctx = context.WithValue(ctx, cacheAdmissionKey{}, func(wait bool) {
				progress(wait)
				if wait {
					close(waiting)
				}
			})
			_, unlock, err := b.copy(ctx, "shared", clock, 1)
			if err == nil {
				unlock()
			}
			return err
		}, func(string) error { return nil })
	}()
	<-waiting
	clock.Advance(2 * checkpointAvailabilityStallBudget)
	select {
	case err := <-done:
		t.Fatal("admission wait counted as source stall", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
