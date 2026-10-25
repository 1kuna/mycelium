package myce

import (
	"database/sql"
	"log"
	"sync"
	"time"
)

// Monotonic wall time is observational only: queue decisions still use q.clock.
// Only launch requests opt in; other transactions pay no clock-read cost.
type queueUpdateTiming struct {
	started   time.Time
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
