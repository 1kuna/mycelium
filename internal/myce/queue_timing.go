package myce

import (
	"context"
	"database/sql"
	"fmt"
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

func readWallStart(observer func(string, time.Duration, time.Duration)) time.Time {
	if observer == nil {
		return time.Time{}
	}
	return time.Now()
}

func observeRead(observer func(string, time.Duration, time.Duration), id string, started, locked time.Time) {
	if observer == nil {
		return
	}
	kind := "status"
	if id != "" {
		kind = "job"
	}
	observer(kind, locked.Sub(started), time.Since(locked))
}

func (s *queueHoldSampler) start() time.Time { return s.startClock(nil) }

func (s *queueHoldSampler) startClock(clock Clock) time.Time {
	if (s.count.Add(1)-1)%16 == 0 {
		return holdNow(clock)
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
func logQueueHold(duration time.Duration, operation string, fn func() error, phases ...*queueHoldPhases) {
	if duration < 250*time.Millisecond {
		return
	}
	if fn != nil {
		operation = runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	}
	detail := ""
	if len(phases) != 0 {
		p := phases[0]
		detail = fmt.Sprintf(" records_snapshot_ms=%.3f fn_body_ms=%.3f persist_commit_ms=%.3f usage_accounting_ms=%.3f notify_ms=%.3f", holdMS(p.phases[holdRecords]), holdMS(p.phases[holdBody]), holdMS(p.phases[holdPersist]), holdMS(p.phases[holdUsage]), holdMS(p.phases[holdNotify]))
	}
	log.Printf("event=slow_queue_mutex_hold operation=%s duration_ms=%.3f sample_every=16%s", operation, holdMS(duration), detail)
}

// Timing is observational only: queue decisions still use q.clock.
// Launch requests and instruction polls opt into detailed request/commit
// timing; mutex holds are sampled independently above.
type queueUpdateTiming struct {
	control   *controlLock
	started   time.Time
	lockedAt  time.Time
	mutexWait time.Duration
	sqlCommit time.Duration
	// Instruction polls opt in to distinguish pool acquisition from work.
	measureWriter bool
	writerWait    time.Duration
}

func (t *queueUpdateTiming) writer(db *sql.DB, clock Clock) (*sql.Conn, error) {
	started := clock.Now()
	conn, err := db.Conn(context.Background())
	t.writerWait += clock.Now().Sub(started)
	return conn, err
}

func newQueueUpdateTiming() *queueUpdateTiming {
	return &queueUpdateTiming{started: time.Now()}
}

func (t *queueUpdateTiming) lock(mu sync.Locker) {
	if t == nil {
		mu.Lock()
		return
	}
	started := time.Now()
	mu.Lock()
	t.mutexWait = time.Since(started)
	t.lockedAt = time.Now()
}

// The detached cancellation waiter may touch only the mutex, never caller
// timing state: callers can log that state as soon as cancellation returns.
func (t *queueUpdateTiming) lockContext(ctx context.Context, mu sync.Locker) error {
	var started time.Time
	if t != nil {
		started = time.Now()
	}
	if err := lockUploadContext(ctx, mu.Lock, mu.Unlock); err != nil {
		return err
	}
	if t != nil {
		t.mutexWait = time.Since(started)
		t.lockedAt = time.Now()
	}
	return nil
}

func (t *queueUpdateTiming) commit(tx *sql.Tx) error {
	if t != nil && t.control != nil {
		t.control.next(controlCommit)
	}
	if t == nil {
		return tx.Commit()
	}
	started := time.Now()
	var controlStarted time.Time
	if t.control != nil && t.control.recorder != nil {
		controlStarted = t.control.recorder.clock.Now()
	}
	err := tx.Commit()
	t.sqlCommit = time.Since(started)
	if !controlStarted.IsZero() {
		finished := t.control.recorder.clock.Now()
		t.control.next(controlOther)
		if t.control.dirty {
			t.control.recorder.observeCommit(finished.Sub(controlStarted), false, t.control.event.Operation)
		}
	}
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

// Sampled timestamps only; queue decisions always use q.clock. The optional
// telemetry clock permits deterministic slow-update tests without sleeping.
type queueHoldPhases struct {
	control       *controlLock
	clock         Clock
	started, last time.Time
	active        int
	phases        [5]time.Duration
}

func holdNow(clock Clock) time.Time {
	if clock != nil {
		return clock.Now()
	}
	return time.Now()
}
func holdMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

const (
	holdRecords = iota
	holdUsage
	holdBody
	holdPersist
	holdNotify
)

func newQueueHoldPhases(started time.Time, clock Clock) queueHoldPhases {
	return queueHoldPhases{started: started, last: started, clock: clock, active: holdRecords}
}
func (p *queueHoldPhases) next(active int) {
	if p.control != nil {
		p.control.coarse(active)
		phase := controlOther
		switch active {
		case holdRecords:
			phase = controlRecords
		case holdUsage:
			phase = controlUsage
		case holdBody:
			phase = controlBody
		case holdPersist:
			phase = controlOther
		case holdNotify:
			phase = controlNotify
		}
		p.control.next(phase)
	}
	if p.started.IsZero() {
		return
	}
	now := holdNow(p.clock)
	p.phases[p.active] += now.Sub(p.last)
	p.last, p.active = now, active
}
func (p *queueHoldPhases) finish() time.Duration {
	if p.started.IsZero() {
		return 0
	}
	p.next(p.active)
	return p.last.Sub(p.started)
}
