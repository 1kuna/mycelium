package myce

import (
	"context"
	"io"
	"math"
	"net"
	"net/url"
	"runtime"
	"sync"
	"time"
)

// Match io.Copy's ordinary buffer: this is a bootstrap quantum, not a byte/sec
// limit. Later allowances come only from this worker's control observations.
const bulkIOQuantum = 32 * 1024

// One controller is shared by all copies in a worker. No queue lock, request,
// file I/O, or caller callback is made while holding mu. Heartbeats never wait
// on bulk writers. The clock controls every pacing decision.
type workerBulkIO struct {
	mu      sync.Mutex
	clock   Clock
	wake    chan struct{}
	started time.Time
	budget  float64
	used    int64
	waiters int
	active  int
	failed  bool
	samples uint64
	mean    float64
	m2      float64
	// Test seam for deterministic waiting; production uses a cancellable timer.
	wait func(context.Context, time.Duration, <-chan struct{}) error
}

func newWorkerBulkIO(clock Clock) *workerBulkIO {
	return &workerBulkIO{clock: clock, wake: make(chan struct{}), budget: bulkIOQuantum}
}

// Only Linux workers sharing the queue host participate. Resolving an endpoint
// does not establish a shared physical disk; pacing all this worker's owned bulk
// writes is conservative when the host has multiple disks. Remote workers and
// Darwin retain their existing behavior. No credential or service file is read.
func localWorkerBulkIO(ctx context.Context, clock Clock, endpoint string) *workerBulkIO {
	if runtime.GOOS != "linux" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" {
		return nil
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, u.Hostname())
	if err != nil {
		return nil
	}
	local, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, address := range addresses {
		if address.IP.IsLoopback() {
			return newWorkerBulkIO(clock)
		}
		for _, candidate := range local {
			if ip, _, err := net.ParseCIDR(candidate.String()); err == nil && ip.Equal(address.IP) {
				return newWorkerBulkIO(clock)
			}
		}
	}
	return nil
}

func (p *workerBulkIO) begin() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.started = p.clock.Now()
	p.mu.Unlock()
}

func (p *workerBulkIO) baseline() time.Duration {
	// The idle mean plus one observed standard deviation tolerates the host's
	// own ordinary jitter. Neither a deployment latency nor a fixed rate is used.
	variance := 0.0
	if p.samples > 1 {
		variance = p.m2 / float64(p.samples-1)
	}
	return time.Duration(p.mean + math.Sqrt(max(0, variance)))
}

func (p *workerBulkIO) end(err error) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := p.clock.Now().Sub(p.started)
	p.started = time.Time{}
	baseline := p.baseline()
	p.failed = err != nil
	if err == nil && elapsed > 0 {
		if p.samples == 0 || p.used == 0 && p.waiters == 0 && p.active == 0 {
			// Never teach the idle baseline from a window containing bulk writes
			// or writers waiting for their allowance: pressure must not normalize
			// itself into recovery. Constant-space Welford statistics.
			p.samples++
			delta := float64(elapsed) - p.mean
			p.mean += delta / float64(p.samples)
			p.m2 += delta * (float64(elapsed) - p.mean)
		} else if baseline > 0 {
			if elapsed > baseline {
				// Preserve forward progress in one ordinary copy quantum, while
				// reducing aggregate bytes in proportion to observed interference.
				p.budget = max(bulkIOQuantum, p.budget*float64(baseline)/float64(elapsed))
			} else if p.used > 0 {
				// Probe the available device instead of imposing a fixed ceiling.
				// Double only a consumed allowance supported by a healthy heartbeat.
				p.budget = min(float64(math.MaxInt64/2), p.budget+float64(p.used))
			}
		}
	}
	p.used = 0
	close(p.wake)
	p.wake = make(chan struct{})
}

type workerBulkIOKey struct{}

func (p *workerBulkIO) context(ctx context.Context) context.Context {
	if p == nil {
		return ctx
	}
	return context.WithValue(ctx, workerBulkIOKey{}, p)
}

// Retain a bulk operation through its final sync/verification and any cancelled
// staging tail. A blocked write/sync must not become an idle baseline sample
// merely because a heartbeat reset the byte counter in the meantime.
func trackBulkIO(ctx context.Context) func() {
	p, _ := ctx.Value(workerBulkIOKey{}).(*workerBulkIO)
	if p == nil {
		return func() {}
	}
	p.mu.Lock()
	p.active++
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}
}

func (p *workerBulkIO) take(ctx context.Context, bytes int) (int, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		p.mu.Lock()
		baseline := p.baseline()
		if baseline <= 0 {
			// Missing/zero-resolution observations cannot establish contention.
			// In particular, fake queue clocks need not advance during HTTP I/O.
			n := min(bytes, bulkIOQuantum)
			p.used += int64(n)
			p.mu.Unlock()
			return n, nil
		}
		slow := !p.started.IsZero() && baseline > 0 && p.clock.Now().Sub(p.started) > baseline
		available := int64(p.budget) - p.used
		if !p.failed && !slow && available > 0 {
			n := min(bytes, bulkIOQuantum, int(available))
			p.used += int64(n)
			p.mu.Unlock()
			return n, nil
		}
		wake := p.wake
		p.waiters++
		p.mu.Unlock()
		// A deliberate control-pressure wait is not a stalled source read or
		// local write. Renew only while waiting here, never during blocked I/O.
		if progress, ok := ctx.Value(ioProgressKey{}).(func()); ok {
			progress()
		}
		wait := p.wait
		if wait == nil {
			wait = waitWorkerBulkIO
		}
		// Once measured, renew at the observed control interval. Before the
		// first check-in, wake only on its completion (or cancellation).
		err := wait(ctx, baseline, wake)
		p.mu.Lock()
		p.waiters--
		p.mu.Unlock()
		if err != nil {
			return 0, err
		}
	}
}

func waitWorkerBulkIO(ctx context.Context, interval time.Duration, wake <-chan struct{}) error {
	var timer <-chan time.Time
	if interval > 0 {
		t := time.NewTimer(interval)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-timer:
		return nil
	}
}

type workerBulkWriter struct {
	ctx context.Context
	out io.Writer
	p   *workerBulkIO
}

func bulkWriter(ctx context.Context, out io.Writer) io.Writer {
	p, _ := ctx.Value(workerBulkIOKey{}).(*workerBulkIO)
	if p == nil {
		return out
	}
	return workerBulkWriter{ctx, out, p}
}

func (w workerBulkWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		n, err := w.p.take(w.ctx, len(data))
		if err != nil {
			return written, err
		}
		count, err := w.out.Write(data[:n])
		written += count
		data = data[count:]
		if err == nil && count != n {
			err = io.ErrShortWrite
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
