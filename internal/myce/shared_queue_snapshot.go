package myce

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"time"
)

// Change-log retention (#2157). The design keeps "a verified snapshot plus
// the subsequent changes" on the authority. The coordinator exports its own
// SQLite copy at its committed cursor S as stage 1 row chunks, the same
// format cutover imports, and commits them with a digest in one PostgreSQL
// transaction. The floor is the second-newest snapshot: changes at or below
// it, and older snapshots, are pruned. A copy at or above the floor replays;
// a copy below it rebuilds from the newest snapshot, verified before its
// SQLite transaction commits, then replays. Replay already refuses a gap, so
// a copy never skips a change.
//
// When: a new snapshot is taken once the change payload committed since the
// newest one reaches that snapshot's own size. Retention then scales with
// the queue's state, not with time or worker count. At that point loading
// the snapshot costs no more than replaying the changes since it, so older
// changes would only cost storage. Snapshot writes add at most one byte of
// authority WAL per change byte. Keeping the previous generation lets a
// member away for less than one snapshot interval replay instead of
// rebuilding. The authority holds at most two snapshots plus about two
// snapshots' worth of changes, after autovacuum reuses the pruned space.

var errSharedSnapshot = errors.New("shared queue snapshot does not match its digest")

type sharedSnapshot struct {
	sequence, bytes int64
	chunks          int
	logSequence     sql.NullInt64
	digest          []byte
}

type sharedSnapshotHash struct{ hash.Hash }

func newSharedSnapshotHash(s sharedSnapshot) sharedSnapshotHash {
	h := sharedSnapshotHash{sha256.New()}
	h.Write([]byte("myce-snapshot-v1\x00"))
	h.number(s.sequence)
	if s.logSequence.Valid {
		h.number(1)
		h.number(s.logSequence.Int64)
	} else {
		h.number(0)
	}
	return h
}

func (h sharedSnapshotHash) number(n int64) {
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(n)))
}

func (h sharedSnapshotHash) chunk(payload []byte) {
	h.number(int64(len(payload)))
	h.Write(payload)
}

// writeSnapshot records the replica's committed state at its own cursor. It
// returns a zero snapshot when one at or above that sequence already exists.
// The replica must be this authority's copy; its cursor never passes a
// change PostgreSQL has not committed.
func (a *sharedQueueAuthority) writeSnapshot(ctx context.Context, replica *sql.DB) (sharedSnapshot, error) {
	var s sharedSnapshot
	read, err := replica.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return s, err
	}
	defer read.Rollback()
	var identity string
	if err := read.QueryRowContext(ctx, "SELECT identity,sequence FROM myce_shared_cursor WHERE id=1").Scan(&identity, &s.sequence); err != nil {
		return sharedSnapshot{}, err
	}
	if identity != a.identity {
		return sharedSnapshot{}, errors.New("the SQLite copy belongs to a different PostgreSQL authority")
	}
	if s.sequence == 0 {
		return sharedSnapshot{}, nil
	}
	// AUTOINCREMENT state is not a row; carry it so a rebuilt coordinator
	// never reuses a deleted log sequence that replay would have retired.
	err = read.QueryRowContext(ctx, "SELECT seq FROM sqlite_sequence WHERE name='logs'").Scan(&s.logSequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return sharedSnapshot{}, err
	}
	// Read outside the long write transaction, so it holds no lock on the
	// coordinator table that schema setup or appends could queue behind.
	var head, newest int64
	if err := a.db.QueryRowContext(ctx, `SELECT head,GREATEST(floor,COALESCE((SELECT max(sequence) FROM myce_snapshots_v1),0))
		FROM myce_coordinator_v1 WHERE id=1`).Scan(&head, &newest); err != nil {
		return sharedSnapshot{}, err
	}
	if s.sequence > head {
		return sharedSnapshot{}, errSharedHead
	}
	if s.sequence <= newest {
		return sharedSnapshot{}, nil
	}
	tx, err := a.begin(ctx)
	if err != nil {
		return sharedSnapshot{}, err
	}
	defer tx.Rollback()
	h := newSharedSnapshotHash(s)
	err = exportSharedSnapshot(ctx, read, func(payload []byte) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO myce_snapshot_chunks_v1(sequence,chunk,payload) VALUES($1,$2,$3)",
			s.sequence, s.chunks, payload); err != nil {
			return err
		}
		h.chunk(payload)
		s.chunks++
		s.bytes += int64(len(payload))
		return nil
	})
	if err != nil {
		return sharedSnapshot{}, err
	}
	s.digest = h.Sum(nil)
	if _, err := tx.ExecContext(ctx, "INSERT INTO myce_snapshots_v1(sequence,chunks,bytes,log_sequence,digest) VALUES($1,$2,$3,$4,$5)",
		s.sequence, s.chunks, s.bytes, s.logSequence, s.digest); err != nil {
		return sharedSnapshot{}, err
	}
	return s, tx.Commit()
}

// latestSnapshot is the newest snapshot at or above the floor, or zero.
func (a *sharedQueueAuthority) latestSnapshot(ctx context.Context) (sharedSnapshot, error) {
	var s sharedSnapshot
	err := a.db.QueryRowContext(ctx, `SELECT sequence,chunks,bytes,log_sequence,digest FROM myce_snapshots_v1
		WHERE sequence >= (SELECT floor FROM myce_coordinator_v1 WHERE id=1) ORDER BY sequence DESC LIMIT 1`).
		Scan(&s.sequence, &s.chunks, &s.bytes, &s.logSequence, &s.digest)
	if errors.Is(err, sql.ErrNoRows) {
		return sharedSnapshot{}, nil
	}
	return s, err
}

// changeBytes is the payload committed after sequence.
func (a *sharedQueueAuthority) changeBytes(ctx context.Context, after int64) (int64, error) {
	var n int64
	err := a.db.QueryRowContext(ctx, "SELECT COALESCE(sum(octet_length(payload)),0)::bigint FROM myce_changes_v1 WHERE sequence > $1", after).Scan(&n)
	return n, err
}

// prune raises the floor to the second-newest snapshot, then deletes what
// lies below it. The floor commits first, in a short transaction on the
// coordinator row, so a copy never finds changes missing without being told
// to rebuild. The deletes take no coordinator lock and also finish a prune
// that an earlier crash interrupted.
func (a *sharedQueueAuthority) prune(ctx context.Context) (int64, error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var floor int64
	if err := tx.QueryRowContext(ctx, `UPDATE myce_coordinator_v1 SET floor=GREATEST(floor,COALESCE(
		(SELECT sequence FROM myce_snapshots_v1 ORDER BY sequence DESC OFFSET 1 LIMIT 1),0)) WHERE id=1 RETURNING floor`).Scan(&floor); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil || floor == 0 {
		return floor, err
	}
	for _, query := range []string{
		"DELETE FROM myce_snapshots_v1 WHERE sequence < $1",
		"DELETE FROM myce_snapshot_chunks_v1 WHERE sequence < $1",
		"DELETE FROM myce_changes_v1 WHERE sequence <= $1",
	} {
		if _, err := a.db.ExecContext(ctx, query, floor); err != nil {
			return floor, err
		}
	}
	return floor, nil
}

// rebuild replaces db's shared rows with the newest snapshot in one SQLite
// transaction and returns its sequence. The digest, chunk count and size are
// checked before commit, so a partial or corrupt snapshot changes nothing.
// Local-only tables are kept. Each PostgreSQL read gets timeout (0: none).
func (a *sharedQueueAuthority) rebuild(ctx context.Context, db *sql.DB, timeout time.Duration) (int64, error) {
	call := func() (context.Context, context.CancelFunc) {
		if timeout > 0 {
			return context.WithTimeout(ctx, timeout)
		}
		return context.WithCancel(ctx)
	}
	c, cancel := call()
	s, err := a.latestSnapshot(c)
	cancel()
	if err != nil {
		return 0, err
	}
	if s.sequence == 0 {
		return 0, errSharedHead
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var identity string
	var cursor int64
	if err := tx.QueryRowContext(ctx, "SELECT identity,sequence FROM myce_shared_cursor WHERE id=1").Scan(&identity, &cursor); err != nil {
		return 0, err
	}
	if identity != a.identity {
		return 0, errors.New("the SQLite copy belongs to a different PostgreSQL authority")
	}
	if cursor >= s.sequence {
		return cursor, nil
	}
	for name := range sharedQueueTables {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?)", name).Scan(&exists); err != nil {
			return 0, err
		}
		if exists {
			// Fixed table names, never values.
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+name); err != nil {
				return 0, err
			}
		}
	}
	h := newSharedSnapshotHash(s)
	var size int64
	for i := range s.chunks {
		var payload []byte
		c, cancel := call()
		err := a.db.QueryRowContext(c, "SELECT payload FROM myce_snapshot_chunks_v1 WHERE sequence=$1 AND chunk=$2", s.sequence, i).Scan(&payload)
		cancel()
		if errors.Is(err, sql.ErrNoRows) {
			// Pruned under a newer snapshot; the caller tries again.
			return 0, errSharedHead
		}
		if err != nil {
			return 0, err
		}
		h.chunk(payload)
		size += int64(len(payload))
		var rows sharedQueueRows
		if err := json.Unmarshal(payload, &rows); err != nil {
			return 0, errSharedSnapshot
		}
		if err := rows.apply(ctx, tx); err != nil {
			return 0, err
		}
	}
	if size != s.bytes || string(h.Sum(nil)) != string(s.digest) {
		return 0, errSharedSnapshot
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sqlite_sequence WHERE name='logs'"); err != nil {
		return 0, err
	}
	if s.logSequence.Valid {
		if _, err := tx.ExecContext(ctx, "INSERT INTO sqlite_sequence(name,seq) VALUES('logs',?)", s.logSequence.Int64); err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE myce_shared_cursor SET sequence=? WHERE id=1 AND sequence=?", s.sequence, cursor); err != nil {
		return 0, err
	}
	return s.sequence, tx.Commit()
}

// sharedQueueCompactor runs on the coordinator, outside q.mu, reading its
// own copy through a separate SQLite handle.
type sharedQueueCompactor struct {
	authority *sharedQueueAuthority
	source    *sql.DB
	writer    *sharedQueueWriter
	latest    sharedSnapshot
	// pending is the change payload committed since latest; seen is the
	// writer's appended counter already counted in it.
	pending, seen int64
	loaded        bool
	// Fixtures may stop between the snapshot commit and the prune.
	snapshotted func() error
}

func (c *sharedQueueCompactor) step(ctx context.Context) error {
	if !c.loaded {
		seen := c.writer.appended.Load()
		latest, err := c.authority.latestSnapshot(ctx)
		if err != nil {
			return err
		}
		pending, err := c.authority.changeBytes(ctx, latest.sequence)
		if err != nil {
			return err
		}
		if _, err := c.authority.prune(ctx); err != nil {
			return err
		}
		c.latest, c.pending, c.seen, c.loaded = latest, pending, seen, true
	}
	appended := c.writer.appended.Load()
	c.pending += appended - c.seen
	c.seen = appended
	if c.pending == 0 || c.pending < c.latest.bytes {
		return nil
	}
	s, err := c.authority.writeSnapshot(ctx, c.source)
	if err != nil || s.sequence == 0 {
		// Reload: another writer may hold the newest snapshot.
		c.loaded = false
		return err
	}
	seen := c.writer.appended.Load()
	pending, err := c.authority.changeBytes(ctx, s.sequence)
	if err != nil {
		c.loaded = false
		return err
	}
	c.latest, c.pending, c.seen = s, pending, seen
	if c.snapshotted != nil {
		err = c.snapshotted()
	}
	if err == nil {
		_, err = c.authority.prune(ctx)
	}
	if err != nil {
		// Loading prunes again, finishing what this pass left.
		c.loaded = false
	}
	return err
}
