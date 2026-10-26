package myce

import (
	"database/sql"
	"log"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Sample one in sixteen holds (including the first), independently of queue
// time. Fast, unsampled operations do no clock reads or stack inspection.
type queueHoldSampler struct{ count atomic.Uint64 }

func (s *queueHoldSampler) start() time.Time {
	if (s.count.Add(1)-1)%16 == 0 {
		return time.Now()
	}
	return time.Time{}
}

func queueHoldDuration(started time.Time) time.Duration {
	if started.IsZero() {
		return 0
	}
	return time.Since(started)
}

// Call only after unlocking. fn names a transaction's operation without logging
// job, host, path or other request data. Resolve its name only for slow holds.
func logQueueHold(duration time.Duration, operation string, fn func() error) {
	if duration < 250*time.Millisecond {
		return
	}
	if fn != nil {
		operation = runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	}
	log.Printf("event=slow_queue_mutex_hold operation=%s duration_ms=%.3f sample_every=16", operation, float64(duration)/float64(time.Millisecond))
}

// Monotonic wall time is observational only: queue decisions still use q.clock.
// Only launch requests opt into detailed request/commit timing; mutex holds
// are sampled independently above.
type queueUpdateTiming struct {
	started   time.Time
	lockedAt  time.Time
	mutexWait time.Duration
	sqlCommit time.Duration
}

func newQueueUpdateTiming() *queueUpdateTiming {
	return &queueUpdateTiming{started: time.Now()}
}

func (t *queueUpdateTiming) lock(mu *sync.RWMutex) {
	if t == nil {
		mu.Lock()
		return
	}
	started := time.Now()
	mu.Lock()
	t.mutexWait = time.Since(started)
	t.lockedAt = time.Now()
}

func (t *queueUpdateTiming) commit(tx *sql.Tx) error {
	if t == nil {
		return tx.Commit()
	}
	started := time.Now()
	err := tx.Commit()
	t.sqlCommit = time.Since(started)
	return err
}

func (t *queueUpdateTiming) logLaunch(ok bool) {
	duration := time.Since(t.started)
	// Half the default heartbeat period is material control-plane delay. Keep
	// fast requests silent; a slow one costs one small journal line after unlock.
	if duration >= defaultWorkerInterval/2 {
		log.Printf("event=slow_launch_authorization path=/api/workers/launch duration_ms=%.3f mutex_wait_ms=%.3f sqlite_commit_ms=%.3f ok=%t",
			float64(duration)/float64(time.Millisecond), float64(t.mutexWait)/float64(time.Millisecond), float64(t.sqlCommit)/float64(time.Millisecond), ok)
	}
}

// Optional internal observer used by the queue-history burst regression.
func (t *queueUpdateTiming) observe(fn func(time.Duration, time.Duration)) {
	if fn != nil {
		fn(t.mutexWait, time.Since(t.lockedAt))
	}
}
