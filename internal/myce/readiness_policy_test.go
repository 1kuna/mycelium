package myce

import (
	"fmt"
	"io"
	"testing"
	"time"
)

func TestAdaptiveReadinessProgressClock(t *testing.T) {
	for _, host := range []struct {
		name string
		gap  time.Duration
	}{
		{"tiny", 40 * time.Second},
		{"large", 2 * time.Millisecond},
	} {
		t.Run(host.name, func(t *testing.T) {
			clock := &manualWorkerClock{now: time.Unix(1, 0)}
			history := readinessHistory{}
			history.Load.observe(host.gap)
			p := newReadinessProgressClocks(clock, clock, history)
			output := &modelReadinessOutput{output: io.Discard, progress: p}
			for i := 1; i <= 100; i++ {
				clock.Advance(host.gap)
				fmt.Fprintf(output, "MYCE_LOAD_PROGRESS {\"stage\":1,\"completed\":%d,\"total\":100}\n", i)
				if _, stalled := p.state(); stalled {
					t.Fatal("advancing load killed by total elapsed time")
				}
			}
			// Replaying a percentage or ordinary logs is not a progress signal.
			budget, _ := p.state()
			clock.Advance(budget)
			fmt.Fprintln(output, "MYCE_LOAD_PROGRESS {\"stage\":1,\"completed\":100,\"total\":100}")
			fmt.Fprintln(output, "still alive, waiting")
			if _, stalled := p.state(); !stalled {
				t.Fatal("true stall hidden by repeated output")
			}
			p.record([]byte(`{"stage":2,"completed":0,"total":10}`))
			if _, stalled := p.state(); stalled {
				t.Fatal("new stage did not renew progress")
			}
		})
	}
}

func TestAdaptiveReadinessTransportClock(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	transport := &manualWorkerClock{now: time.Unix(1, 0)}
	history := readinessHistory{}
	history.Load.observe(2 * time.Millisecond)
	p := newReadinessProgressClocks(clock, transport, history)
	budget, _ := p.state()
	transport.Advance(budget)
	if _, stalled := p.state(); !stalled {
		t.Fatal("stationary primary clock hid transport silence")
	}
	p.record([]byte(`{"stage":1,"completed":1,"total":2}`))
	if _, stalled := p.state(); stalled {
		t.Fatal("progress did not renew transport silence")
	}
}
