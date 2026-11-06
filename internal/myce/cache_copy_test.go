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
			release, err := g.acquire(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			g.progress(0)
			clock.Advance(time.Second)
			g.progress(1024)
			if g.capacity() != 2 {
				t.Fatal("no exploratory writer", g.capacity())
			}
			release2, err := g.acquire(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer release2()
			g.progress(0)
			clock.Advance(time.Second)
			bytes := int64(1024)
			if scales {
				bytes *= 2
			}
			g.progress(bytes)
			want := 1
			if scales {
				want = 2
			}
			if g.capacity() != want {
				t.Fatal("wrong measured concurrency", g.capacity(), want)
			}
		})
	}
}
func TestCacheCopyCancellationAndExplicitCeiling(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(clock, 1<<30, 1<<29)
	ctx, release, err := b.copy(context.Background(), "files", clock, 1)
	if err != nil {
		t.Fatal(err)
	}
	g := b.copies["local"]
	g.progress(0)
	clock.Advance(time.Second)
	g.progress(1024)
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
