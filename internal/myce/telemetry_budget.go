package myce

import (
	"math"
	"sync"
	"time"
)

// Telemetry learns the local service distribution and pays for a diagnostic
// line from observed service time. No universal latency or log-rate threshold
// decides which installation is slow. Cold observations teach without logging.
type telemetryBudget struct {
	mu           sync.Mutex
	count        uint64
	mean, change float64
	last         time.Duration
	credit, cost time.Duration
}

func (b *telemetryBudget) admit(d time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d < 0 {
		return false
	}
	abnormal := b.count > 0 && float64(d) > b.mean+b.change
	b.count++
	n := float64(b.count)
	b.mean += (float64(d) - b.mean) / n
	if b.count > 1 {
		b.change += (math.Abs(float64(d-b.last)) - b.change) / (n - 1)
	}
	b.last = d
	if d > time.Duration(math.MaxInt64)-b.credit {
		b.credit = time.Duration(math.MaxInt64)
	} else {
		b.credit += d
	}
	if abnormal && b.credit >= b.cost {
		b.credit = 0
		return true
	}
	return false
}
func (b *telemetryBudget) spent(d time.Duration) { b.mu.Lock(); b.cost = max(b.cost, d); b.mu.Unlock() }

// Call sites are fixed compiled operations, never request data or identities.
var queueHoldTelemetry sync.Map

func holdTelemetry(operation string) *telemetryBudget {
	b, _ := queueHoldTelemetry.LoadOrStore(operation, &telemetryBudget{})
	return b.(*telemetryBudget)
}
