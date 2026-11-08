package myce

import (
	"math"
	"sync"
	"time"
)

// A cache trades projection CPU against stale observations between writes.
// Learn all three costs locally. Terminal/release commits still invalidate
// immediately, including a refresh in flight; this lease never overrides them.
type observationCacheBudget struct {
	mu       sync.Mutex
	last     time.Time
	interval float64
	samples  uint64
}

func (b *observationCacheBudget) mutation(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.last.IsZero() && now.After(b.last) {
		b.samples++
		b.interval += (float64(now.Sub(b.last)) - b.interval) / float64(b.samples)
	}
	b.last = now
}

func (b *observationCacheBudget) lease(capture, hit time.Duration) time.Duration {
	b.mu.Lock()
	interval := b.interval
	b.mu.Unlock()
	return observationCacheLease(capture, hit, time.Duration(interval))
}

func observationCacheLease(capture, hit, mutation time.Duration) time.Duration {
	// One nanosecond is time.Duration's representation quantum, allowing a
	// singleflight generation to be reused at an unchanged injected time. Cold
	// clocks/measurements do not invent a universal one- or two-second lease.
	capture = max(time.Nanosecond, capture)
	if mutation <= 0 {
		return capture
	}
	if hit <= 0 {
		hit = capture
	} // unknown savings cannot claim cheap reads
	// Balancing regeneration cost C/T with staleness cost T*H/(C*W)
	// gives T=C*sqrt(W/H). Do not promise faster refresh than one measured
	// capture, or delay beyond one observed write interval when capture fits.
	lease := float64(capture) * math.Sqrt(float64(mutation)/float64(hit))
	lease = min(float64(mutation), lease)
	if lease >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return max(capture, time.Duration(lease))
}
