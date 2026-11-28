package myce

import (
	"context"
	"database/sql"
)

// A batch owns only SQLite row operations and their captured values. Queue
// decisions, including time and attempt fences, are evaluated before it runs.
// The writer turn remains exclusive through apply and publication.
type queueWriteBatch struct {
	rows []queueRowWrite
}

type queueRowWrite struct {
	query string
	args  []any
	count *int64
	miss  error // Require exactly one row; abort the entire transaction otherwise.
}

type queueWriteMode uint8

const (
	queueWriteDetached  queueWriteMode = iota // Maintenance owns the turn; mutable state is committed.
	queueWriteLocked                          // Caller owns the turn; mutable state is committed.
	queueWriteJournaled                       // Caller owns a speculative state journal.
)

func (b *queueWriteBatch) add(query string, args ...any) {
	b.rows = append(b.rows, queueRowWrite{query: query, args: args})
}

// applyRows is also used by startup evidence repairs, before a queue can
// serve requests. It owns no queue data or scheduling decisions.
func (b *queueWriteBatch) applyRows(ctx context.Context, tx *sql.Tx, observe func(string, []any)) error {
	for _, row := range b.rows {
		result, err := tx.ExecContext(ctx, row.query, row.args...)
		if err != nil {
			return queueWriteError{err}
		}
		if row.count != nil || row.miss != nil {
			n, err := result.RowsAffected()
			if err != nil {
				return queueWriteError{err}
			}
			if row.miss != nil && n != 1 {
				return row.miss
			}
			if row.count != nil {
				*row.count = n
			}
		}
		if observe != nil {
			observe(row.query, row.args)
		}
	}
	return nil
}

func (q *Queue) writeQueue(batch *queueWriteBatch, timing *queueUpdateTiming, measured *queuePersistenceTiming, mode queueWriteMode) error {
	// A control transaction that owns its journal must reach its commit or
	// rollback boundary even if the client leaves.
	return q.applyQueueWrite(context.Background(), batch, timing, measured, mode)
}

func (q *Queue) writeDetachedQueue(ctx context.Context, batch *queueWriteBatch) error {
	if err := lockUploadContext(ctx, q.mu.Lock, q.mu.Unlock); err != nil {
		return err
	}
	defer q.mu.Unlock()
	return q.applyQueueWrite(ctx, batch, nil, nil, queueWriteDetached)
}

func (q *Queue) applyQueueWrite(ctx context.Context, batch *queueWriteBatch, timing *queueUpdateTiming, measured *queuePersistenceTiming, mode queueWriteMode) error {
	if len(batch.rows) == 0 && q.sharedWriter == nil {
		return nil
	}
	write := func() error {
		writer := queueSQLiteWriter{db: q.db, clock: q.clock, holdClock: q.holdClock, observe: q.observeQueueRow, shared: q.sharedWriter}
		return writer.apply(ctx, batch, timing, measured)
	}
	if mode != queueWriteDetached && q.inLockCommit && q.sharedWriter == nil {
		return write()
	}
	return q.withoutQueueMutex(mode, timing, false, write)
}

// The writer turn fences the speculative state, including before any SQL is
// sent. Readers of the mutable state stay behind the journal barrier.
func (q *Queue) withoutQueueMutex(mode queueWriteMode, timing *queueUpdateTiming, preparing bool, work func() error) error {
	if q.activeHold != nil {
		q.activeHold.pause()
		defer q.activeHold.resume(preparing)
	}
	if timing != nil {
		started := holdNow(timing.clock)
		defer func() { timing.offLock += holdNow(timing.clock).Sub(started) }()
	}
	return q.mu.commitOutsideLock(mode == queueWriteJournaled, work)
}

// Startup evidence migrations also use this writer, without a live queue turn.
type queueSQLiteWriter struct {
	db        *sql.DB
	clock     Clock
	holdClock Clock
	observe   func(string, []any)
	shared    *sharedQueueWriter
}

func (w queueSQLiteWriter) apply(ctx context.Context, batch *queueWriteBatch, timing *queueUpdateTiming, measured *queuePersistenceTiming) error {
	if w.shared != nil {
		if err := w.shared.apply(ctx, w.db, batch, w.observe); err != nil {
			return queueWriteError{err}
		}
		return nil
	}
	started := holdNow(w.holdClock)
	var conn *sql.Conn
	var err error
	if timing != nil && timing.measureWriter {
		conn, err = timing.writer(w.db, w.clock)
	} else {
		conn, err = w.db.Conn(ctx)
	}
	if err != nil {
		return queueWriteError{err}
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return queueWriteError{err}
	}
	defer tx.Rollback()
	if err := batch.applyRows(ctx, tx, w.observe); err != nil {
		return err
	}
	if measured != nil {
		measured.SQL += holdNow(w.holdClock).Sub(started)
	}
	started = holdNow(w.holdClock)
	err = timing.commit(tx)
	if measured != nil {
		measured.Commit = holdNow(w.holdClock).Sub(started)
	}
	if err != nil {
		// Some SQLite commit errors (including BUSY) leave the transaction
		// open. database/sql has already ended its Tx, so clean the pinned
		// connection before returning it to the pool.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return queueWriteError{err}
	}
	return nil
}

type queueWriteError struct{ err error }

func (e queueWriteError) Error() string { return "queue SQLite write: " + e.err.Error() }
func (e queueWriteError) Unwrap() error { return e.err }
