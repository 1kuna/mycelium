package myce

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Measure each hold; the local service distribution controls diagnostic logging.
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
	s.count.Add(1)
	return holdNow(clock)
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
	if fn != nil {
		operation = runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	}
	budget := holdTelemetry(operation)
	if !budget.admit(duration) {
		return
	}
	started := time.Now()
	defer func() { budget.spent(time.Since(started)) }()
	detail := ""
	if len(phases) != 0 {
		p := phases[0]
		detail = fmt.Sprintf(" records_snapshot_ms=%.3f fn_body_ms=%.3f persist_commit_ms=%.3f usage_accounting_ms=%.3f notify_ms=%.3f", holdMS(p.phases[holdRecords]), holdMS(p.phases[holdBody]), holdMS(p.phases[holdPersist]), holdMS(p.phases[holdUsage]), holdMS(p.phases[holdNotify]))
		detail += fmt.Sprintf(" persist_marshal_ms=%.3f persist_sql_ms=%.3f persist_sync_commit_ms=%.3f persist_payload_bytes=%d persist_records=%d", holdMS(p.persistence.Marshal), holdMS(p.persistence.SQL), holdMS(p.persistence.Commit), p.persistence.Bytes, p.persistence.Records)
		if p.checkinBody {
			for i, name := range queueBodyPhaseNames {
				detail += fmt.Sprintf(" body_%s_ms=%.3f", name, holdMS(p.body[i]))
			}
		}
	}
	log.Printf("event=slow_queue_mutex_hold operation=%s duration_ms=%.3f sample_every=1%s", operation, holdMS(duration), detail)
}

// Timing is observational only: queue decisions still use q.clock.
// Launch requests and instruction polls opt into detailed request/commit
// timing; mutex holds are sampled independently above.
type queueUpdateTiming struct {
	started   time.Time
	lockedAt  time.Time
	mutexWait time.Duration
	sqlCommit time.Duration
	// Instruction polls opt in to distinguish pool acquisition from work.
	measureWriter bool
	writerWait    time.Duration
	persistence   queuePersistenceTiming
}

// Observational timings only. Commit includes SQLite's synchronous=FULL sync;
// it is not an isolated filesystem fsync measurement. Bytes counts changed
// queue-state payloads submitted to SQL, excluding usage rows and WAL overhead.
type queuePersistenceTiming struct {
	Marshal time.Duration
	SQL     time.Duration
	Commit  time.Duration
	Bytes   int64
	Records int64
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
	// Compare with locally observed authorization cost, after releasing control.
	budget := holdTelemetry("launch")
	if budget.admit(duration) {
		started := time.Now()
		defer func() { budget.spent(time.Since(started)) }()
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
	persistence   queuePersistenceTiming
	clock         Clock
	started, last time.Time
	active        int
	phases        [5]time.Duration
	checkinBody   bool
	body          [len(queueBodyPhaseNames)]time.Duration
}

// Fixed names only: no request data or per-job allocations. The checkin_*
// phases partition the callback, and transaction_tail covers shared work after
// it returns. The remaining helper timings are inclusive children; do not sum
// them with their parents (e.g. health is inside transaction_tail).
var queueBodyPhaseNames = [...]string{
	"checkin_inventory", "checkin_memory", "checkin_reconcile",
	"checkin_maintain", "checkin_attempts", "checkin_plans", "transaction_tail",
	"maintain", "placement", "environment_plan", "qualification_plan",
	"model_plan", "copy_plan", "health", "efficiency", "retained_index",
}

func holdNow(clock Clock) time.Time {
	if clock != nil {
		return clock.Now()
	}
	return time.Now()
}
func holdMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

const (
	adviceCapture = iota
	adviceWaitReason
	adviceStatusReason
	adviceDeadline
)

// Submission advice is a separate post-commit phase, absent from update holds.
// Wall timestamps are telemetry only, never scheduling inputs. Log after unlock
// using the same measured diagnostic budget as transaction telemetry.
type submissionAdviceTiming struct {
	started, last time.Time
	active        int
	phases        [4]time.Duration
}

func newSubmissionAdviceTiming() submissionAdviceTiming {
	now := time.Now()
	return submissionAdviceTiming{started: now, last: now}
}

func (t *submissionAdviceTiming) next(phase int) {
	now := time.Now()
	t.phases[t.active] += now.Sub(t.last)
	t.last, t.active = now, phase
}

func (t *submissionAdviceTiming) finish(observer func(submissionAdviceTiming)) {
	t.next(t.active)
	if observer != nil {
		observer(*t)
	}
	duration := t.last.Sub(t.started)
	budget := holdTelemetry("submission_advice")
	if !budget.admit(duration) {
		return
	}
	started := time.Now()
	defer func() { budget.spent(time.Since(started)) }()
	log.Printf("event=submission_advice duration_ms=%.3f capture_ms=%.3f wait_reason_ms=%.3f status_waiting_reason_ms=%.3f deadline_guidance_ms=%.3f", holdMS(duration), holdMS(t.phases[adviceCapture]), holdMS(t.phases[adviceWaitReason]), holdMS(t.phases[adviceStatusReason]), holdMS(t.phases[adviceDeadline]))
}

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

// Body timers run inside a transaction or an explicitly observed diagnostic
// fixture. Only the existing slow-hold logger emits them, after unlocking.
// Their wall clock never controls scheduling.
func (q *Queue) bodyPhaseStart() time.Time {
	if q.activeHold == nil && q.observeBodyPhase == nil {
		return time.Time{}
	}
	return holdNow(q.holdClock)
}
func (q *Queue) bodyPhaseFinish(name string, started time.Time) {
	if !started.IsZero() {
		elapsed := holdNow(q.holdClock).Sub(started)
		if p := q.activeHold; p != nil {
			for i, candidate := range queueBodyPhaseNames {
				if candidate == name {
					p.body[i] += elapsed
					p.checkinBody = p.checkinBody || strings.HasPrefix(name, "checkin_")
					break
				}
			}
		}
		if q.observeBodyPhase != nil {
			q.observeBodyPhase(name, elapsed)
		}
	}
}

type queueBodyTimer struct {
	q       *Queue
	name    string
	started time.Time
}

func (q *Queue) startBodyPhase(name string) queueBodyTimer {
	return queueBodyTimer{q: q, name: name, started: q.bodyPhaseStart()}
}

func (t *queueBodyTimer) next(name string) {
	t.finish()
	t.name, t.started = name, t.q.bodyPhaseStart()
}

func (t *queueBodyTimer) finish() {
	t.q.bodyPhaseFinish(t.name, t.started)
	t.started = time.Time{}
}
