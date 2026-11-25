package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A probe that hangs, then fails, then recovers yields one window per outage,
// and the watcher never stacks probes behind a hung one.
func TestMountWatcherRecordsHangsAndErrors(t *testing.T) {
	var calls, inFlight, maxInFlight atomic.Int32
	release := make(chan struct{})
	w := &mountWatcher{interval: 10 * time.Millisecond, timeout: 30 * time.Millisecond, probe: func() error {
		n := calls.Add(1)
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		switch {
		case n == 3:
			<-release // hung mount
		case n >= 6 && n <= 7:
			return errors.New("host is down")
		}
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan []mountWindow)
	go func() { done <- w.run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	close(release)
	for calls.Load() < 12 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	windows := <-done
	if len(windows) != 2 {
		t.Fatalf("windows %+v", windows)
	}
	if !strings.HasPrefix(windows[0].Reason, "no answer") || windows[0].Seconds < 0.15 {
		t.Fatalf("hang window %+v", windows[0])
	}
	if windows[1].Reason != "error: host is down" || windows[1].OpenAtEnd {
		t.Fatalf("error window %+v", windows[1])
	}
	if maxInFlight.Load() != 1 {
		t.Fatalf("probes stacked: %d in flight", maxInFlight.Load())
	}
}

func TestMountChecksNeedALongWindowAndHealthyCommits(t *testing.T) {
	t0 := time.Now()
	appends := []opSample{
		{At: t0.Add(2 * time.Second), Lat: 5 * time.Millisecond, Resp: 6 * time.Millisecond},
		{At: t0.Add(5 * time.Second), Lat: 7 * time.Millisecond, Resp: 8 * time.Millisecond},
		{At: t0.Add(30 * time.Second), Lat: 1 * time.Millisecond, Resp: 1 * time.Millisecond},
	}
	renewals := []opSample{{At: t0.Add(3 * time.Second), Lat: 4 * time.Millisecond}}
	short := []mountWindow{{Start: t0, End: t0.Add(3 * time.Second), Seconds: 3}}
	if c := mountChecks(correlateMount(short, appends, renewals, 5), 1000); c[0].Status != statusNotExercised {
		t.Fatalf("a 3 s window proves nothing: %+v", c)
	}
	long := []mountWindow{{Start: t0, End: t0.Add(12 * time.Second), Seconds: 12}}
	m := correlateMount(long, appends, renewals, 15)
	if m.Windows[0].Append.Count != 2 || m.Windows[0].RenewMaxMs != 4 || m.UnresponsiveSec != 12 {
		t.Fatalf("correlation %+v", m.Windows[0])
	}
	if c := mountChecks(m, 1000); c[0].Status != statusPass {
		t.Fatalf("%+v", c)
	}
	if c := mountChecks(m, 7); c[0].Status != statusFail {
		t.Fatalf("an 8 ms response over a 7 ms limit must fail: %+v", c)
	}
	none := correlateMount(long, appends[2:], nil, 15)
	if c := mountChecks(none, 1000); c[0].Status != statusFail {
		t.Fatalf("no commits during the window must fail: %+v", c)
	}
}
