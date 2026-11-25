package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// A change contains the rows SQLite actually accepted, in batch order.
// Table and column names are fixed by version. The log never executes SQL
// supplied by a peer. Capturing RETURNING also resolves ignored offsets,
// usage addition, retention predicates and locally assigned log sequences.
type sharedQueueRows struct {
	Version int              `json:"version"`
	Rows    []sharedQueueRow `json:"rows"`
}

type sharedQueueRow struct {
	Table  string             `json:"table"`
	Delete bool               `json:"delete,omitempty"`
	Values []sharedQueueValue `json:"values"`
}

type sharedQueueValue struct {
	Kind  string  `json:"kind"`
	Int   int64   `json:"int,omitempty"`
	Real  float64 `json:"real,omitempty"`
	Bytes []byte  `json:"bytes,omitempty"`
}

type sharedQueueTable struct {
	keys, columns []string
	upsert, erase string
}

func sharedTable(keys, rest string) sharedQueueTable {
	k := strings.Fields(keys)
	v := strings.Fields(rest)
	c := append(append([]string(nil), k...), v...)
	where := make([]string, len(k))
	for i, key := range k {
		where[i] = key + "=?"
	}
	set := make([]string, len(v))
	for i, col := range v {
		set[i] = col + "=excluded." + col
	}
	// These templates receive the fixed table name, never values from WAL.
	conflict := "DO NOTHING"
	if len(set) != 0 {
		conflict = "DO UPDATE SET " + strings.Join(set, ",")
	}
	return sharedQueueTable{
		keys: k, columns: c,
		upsert: "INSERT INTO %s(" + strings.Join(c, ",") + ") VALUES(" +
			strings.TrimSuffix(strings.Repeat("?,", len(c)), ",") + ") ON CONFLICT(" +
			strings.Join(k, ",") + ") " + conflict,
		erase: "DELETE FROM %s WHERE " + strings.Join(where, " AND "),
	}
}

var sharedQueueTables = map[string]sharedQueueTable{
	"queue":                   sharedTable("id", "data"),
	"queue_state":             sharedTable("key", "data"),
	"enrollment_tickets":      sharedTable("digest", "pin expires used"),
	"logs":                    sharedTable("seq", "job attempt text"),
	"log_offsets":             sharedTable("job attempt offset", ""),
	"environment_log_offsets": sharedTable("job machine session key offset", ""),
	"environment_build_logs":  sharedTable("machine session key offset", "text"),
	"job_summaries":           sharedTable("id", "finished summary learned"),
	"usage_samples":           sharedTable("at span machine device project job model", "busy idle pinned memory_seconds observed"),
	"memory_evidence":         sharedTable("kind key", "data"),
	"gpu_sharing_evidence":    sharedTable("key", "data"),
}

func captureSharedRows(ctx context.Context, tx *sql.Tx, batch *queueWriteBatch, observe func(string, []any)) (sharedQueueRows, error) {
	out := sharedQueueRows{Version: 1}
	for _, write := range batch.rows {
		if write.query == "CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)" {
			// The replica schema already has this table, even for sequence zero.
			continue
		}
		fields := strings.Fields(write.query)
		if len(fields) < 3 {
			return out, errors.New("invalid queue write statement")
		}
		deleting := fields[0] == "DELETE"
		var name string
		switch fields[0] {
		case "INSERT", "DELETE":
			for i := 1; i+1 < len(fields); i++ {
				if fields[i] == "INTO" || fields[i] == "FROM" {
					name, _, _ = strings.Cut(fields[i+1], "(")
					break
				}
			}
		}
		table, ok := sharedQueueTables[name]
		if !ok {
			return out, errors.New("queue write has no shared row format")
		}
		columns := table.columns
		if deleting {
			columns = table.keys
		}
		rows, err := tx.QueryContext(ctx, write.query+" RETURNING "+strings.Join(columns, ","), write.args...)
		if err != nil {
			return out, err
		}
		n, err := out.capture(rows, name, deleting, len(columns))
		if err != nil {
			return out, err
		}
		if write.miss != nil && n != 1 {
			return out, write.miss
		}
		if write.count != nil {
			*write.count = n
		}
		if observe != nil {
			observe(write.query, write.args)
		}
	}
	return out, nil
}

func (out *sharedQueueRows) capture(rows *sql.Rows, name string, deleting bool, ncols int) (int64, error) {
	defer rows.Close()
	src := make([]any, ncols)
	dest := make([]any, ncols)
	for i := range src {
		dest[i] = &src[i]
	}
	var n int64
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return n, err
		}
		row := sharedQueueRow{Table: name, Delete: deleting, Values: make([]sharedQueueValue, ncols)}
		for i, value := range src {
			switch v := value.(type) {
			case nil:
				row.Values[i] = sharedQueueValue{Kind: "null"}
			case int64:
				row.Values[i] = sharedQueueValue{Kind: "int", Int: v}
			case float64:
				row.Values[i] = sharedQueueValue{Kind: "real", Real: v}
			case string:
				// SQLite TEXT may hold non-UTF-8 log bytes. JSON's string
				// encoding replaces invalid UTF-8, so preserve the raw bytes.
				row.Values[i] = sharedQueueValue{Kind: "text", Bytes: []byte(v)}
			case []byte:
				row.Values[i] = sharedQueueValue{Kind: "blob", Bytes: v}
			default:
				return n, fmt.Errorf("unsupported SQLite value type %T", value)
			}
		}
		out.Rows = append(out.Rows, row)
		n++
	}
	return n, rows.Err()
}

func (r sharedQueueRows) apply(ctx context.Context, tx *sql.Tx) error {
	if r.Version != 1 {
		return errors.New("unsupported shared queue row version")
	}
	for _, row := range r.Rows {
		t, ok := sharedQueueTables[row.Table]
		if !ok {
			return errors.New("unknown shared queue table")
		}
		query, cols := t.upsert, t.columns
		if row.Delete {
			query, cols = t.erase, t.keys
		}
		if len(row.Values) != len(cols) {
			return errors.New("invalid shared queue row width")
		}
		args := make([]any, len(cols))
		for i, v := range row.Values {
			switch v.Kind {
			case "null":
			case "text":
				args[i] = string(v.Bytes)
			case "blob":
				args[i] = append([]byte{}, v.Bytes...)
			case "int":
				args[i] = v.Int
			case "real":
				args[i] = v.Real
			default:
				return errors.New("unknown shared queue value type")
			}
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(query, row.Table), args...); err != nil {
			return err
		}
	}
	return nil
}

const sharedQueueCursorSchema = `CREATE TABLE IF NOT EXISTS myce_shared_cursor (
	id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, sequence INTEGER NOT NULL
)`

func setSharedQueueCursor(ctx context.Context, tx *sql.Tx, sequence int64) error {
	result, err := tx.ExecContext(ctx, "UPDATE myce_shared_cursor SET sequence=? WHERE id=1 AND sequence=?", sequence, sequence-1)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errSharedHead
	}
	return err
}

func replaySharedQueueChange(ctx context.Context, db *sql.DB, c sharedQueueChange) error {
	var payload sharedQueueRows
	if err := json.Unmarshal(c.payload, &payload); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := setSharedQueueCursor(ctx, tx, c.sequence); err != nil {
		return err
	}
	if err := payload.apply(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

type sharedQueueWriter struct {
	authority    *sharedQueueAuthority
	turn         sharedQueueTurn
	timeout      time.Duration
	sequence     int64 // Exclusive stage 1 writer turn.
	disabled     atomic.Bool
	storageFault atomic.Bool
	spool        *uploadSpool
}

func (w *sharedQueueWriter) apply(ctx context.Context, db *sql.DB, batch *queueWriteBatch, observe func(string, []any)) error {
	if w.disabled.Load() {
		return errSharedTurn
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := captureSharedRows(ctx, tx, batch, observe)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	change := sharedQueueChange{sequence: w.sequence + 1, handover: w.turn.number, operation: ID(), payload: payload}
	if err := setSharedQueueCursor(ctx, tx, change.sequence); err != nil {
		w.disabled.Store(true)
		return err
	}
	if w.disabled.Load() {
		return errSharedTurn
	}
	commitCtx, cancel := context.WithTimeout(ctx, w.timeout)
	uncertain, err := w.authority.append(commitCtx, w.turn, change)
	cancel()
	if uncertain {
		// The queue writer turn is still held. No later grant can begin
		// until the exact commit is recovered or this turn is disabled.
		probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.timeout)
		seq, turn, probeErr := w.authority.lookup(probeCtx, change.operation)
		cancel()
		if probeErr == nil && seq == change.sequence && turn == change.handover {
			err = nil
		}
	}
	if err != nil {
		w.disabled.Store(true)
		// This turn is over, so the request is retryable under the next one.
		// Never surface the commit deadline as a terminal request timeout.
		if !errors.Is(err, errSharedHead) {
			err = fmt.Errorf("%w: %v", errSharedTurn, err)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		w.disabled.Store(true)
		// A failed SQLite COMMIT can leave the pinned transaction open.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}
	w.sequence = change.sequence
	if w.disabled.Load() {
		return errSharedTurn
	}
	return nil
}
