package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const idleSchema = `
CREATE TABLE IF NOT EXISTS host_idle_intervals (
  host TEXT NOT NULL,
  start INTEGER NOT NULL,
  end INTEGER NOT NULL,
  class TEXT NOT NULL,
  state TEXT NOT NULL,
  reason TEXT NOT NULL,
  bytes INTEGER NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(host,start)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS host_idle_end ON host_idle_intervals(end);
`

// The independent writer keeps interval batches and pruning out of the control
// database. WAL NORMAL avoids an additional storage sync on every tick. A
// machine crash can lose recent evidence, which the report exposes as unknown.
func (q *Queue) openIdleHistory(path string) {
	path, err := filepath.Abs(path)
	if err != nil {
		log.Printf("host idle evidence storage unavailable: %v", err)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Printf("host idle evidence storage unavailable: %v", err)
		return
	}
	err = f.Close()
	if err != nil {
		log.Printf("host idle evidence storage unavailable: %v", err)
		return
	}
	location := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=synchronous(NORMAL)"}
	db, err := sql.Open("sqlite", location.String())
	if err == nil {
		db.SetMaxOpenConns(2) // One retention writer and one concurrent WAL reader.
		_, err = db.Exec("PRAGMA journal_mode=WAL;" + idleSchema)
	}
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		log.Printf("host idle evidence storage unavailable: %v", err)
		return
	}
	q.idleHistory.db = db
}

type idleStored struct {
	host       string
	start, end int64
	evidence   HostIdleEvidence
	data       []byte
}

func (r idleStored) size() uint64 {
	return uint64(len(r.data) + len(r.host) + len(r.evidence.Class) + len(r.evidence.State) + len(r.evidence.Reason) + 3*8)
}

func (h *idleHistory) setBudget(report EvidenceRetention) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.measured = !report.Unavailable
	if h.measured {
		h.allow = report.BudgetBytes - min(report.RetainedBytes, report.BudgetBytes)
	}
}

func (q *Queue) closeIdleHistory() error {
	h := &q.idleHistory
	h.captureMu.Lock()
	h.stopped = true
	done := h.done
	h.captureMu.Unlock()
	if done != nil {
		<-done
	}
	if h.db == nil {
		return nil
	}
	h.flushMu.Lock()
	defer h.flushMu.Unlock()
	return errors.Join(h.flush(context.Background()), h.db.Close())
}

func (q *Queue) flushIdleEvidence(ctx context.Context) {
	h := &q.idleHistory
	if h.db == nil || !h.flushMu.TryLock() {
		return
	}
	defer h.flushMu.Unlock()
	if busy, _ := q.evidencePressure(); busy {
		return
	}
	if err := h.flush(ctx); err != nil && ctx.Err() == nil {
		log.Printf("host idle evidence write deferred: %v", err)
	}
}

func (h *idleHistory) flush(ctx context.Context) error {
	h.mu.Lock()
	allow, measured := h.allow, h.measured
	rows := h.closed
	h.closed, h.bytes = nil, 0
	for host := range h.dirty {
		rows = append(rows, h.open[host])
	}
	clear(h.dirty)
	h.mu.Unlock()
	if measured && (!h.loaded || len(rows) > 0 || h.retained > allow) {
		if err := h.persist(ctx, rows, allow); err != nil {
			// A failed optional write leaves a detectable gap. An open row can
			// still be retried in full the next time it grows.
			return err
		}
	}
	return nil
}

func (h *idleHistory) persist(ctx context.Context, rows []idleStored, allow uint64) error {
	if !h.loaded {
		if err := h.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0) FROM host_idle_intervals").Scan(&h.retained); err != nil {
			return err
		}
		h.loaded = true
	}
	if allow == 0 && h.retained == 0 {
		return nil
	}
	// Build and encode before acquiring SQLite's writer. Neither this write
	// nor any projection reads or takes the queue mutex.
	type write struct {
		Host, Class, State string
		Reason             AdmissionPredicate
		Start, End         int64
		Bytes              uint64
		Data               json.RawMessage
	}
	writes := make([]write, 0, len(rows))
	var incoming uint64
	for _, r := range rows {
		if r.size() > allow {
			continue
		}
		writes = append(writes, write{r.host, r.evidence.Class, r.evidence.State, r.evidence.Reason, r.start, r.end, r.size(), r.data})
		incoming += r.size()
	}
	if len(writes) == 0 && h.retained <= allow {
		return nil
	}
	data, err := json.Marshal(writes)
	if err != nil {
		return err
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	retained := h.retained
	if len(writes) > 0 {
		// Continuing intervals already own a row; charge only new bytes.
		var existing uint64
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(i.bytes),0)
FROM json_each(?) j JOIN host_idle_intervals i
  ON i.host=j.value->>'Host' AND i.start=j.value->>'Start'`, data).Scan(&existing)
		if err != nil {
			return err
		}
		retained += incoming - min(incoming, existing)
		_, err = tx.ExecContext(ctx, `INSERT INTO host_idle_intervals(host,start,end,class,state,reason,bytes,data)
SELECT value->>'Host',value->>'Start',value->>'End',value->>'Class',value->>'State',value->>'Reason',value->>'Bytes',value->'Data'
FROM json_each(?) WHERE true
ON CONFLICT(host,start) DO UPDATE SET end=MAX(end,excluded.end)`, data)
		if err != nil {
			return err
		}
	}

	if retained > allow {
		rows, err := tx.QueryContext(ctx, "SELECT end,bytes FROM host_idle_intervals ORDER BY end")
		if err != nil {
			return err
		}
		var end int64
		for rows.Next() {
			var stop int64
			var size uint64
			if err := rows.Scan(&stop, &size); err != nil {
				rows.Close()
				return err
			}
			if retained <= allow && stop != end {
				break
			}
			retained -= min(retained, size)
			end = stop
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if _, err := tx.ExecContext(ctx, "DELETE FROM host_idle_intervals WHERE end<=?", end); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	h.retained = retained
	return nil
}

func (q *Queue) readIdleStatus(ctx context.Context, options *statusOptions, now time.Time) (*IdleReport, error) {
	since := options.idleSince
	if since.IsZero() {
		since = now.Add(-7 * 24 * time.Hour)
	}
	return q.idleReport(ctx, since, now, options.idleIntervals)
}

func (q *Queue) idleReport(ctx context.Context, since, through time.Time, intervals bool) (*IdleReport, error) {
	r := &IdleReport{Since: since, Through: through, ByMachineClass: map[string]*IdleClassHours{}, ByReason: map[AdmissionPredicate]float64{}}
	if !through.After(since) {
		return r, nil
	}
	cursors := map[string]time.Time{}
	classes := map[string]string{}
	if db := q.idleHistory.db; db != nil {
		if err := r.readIntervals(ctx, db, since, through, intervals, cursors, classes); err != nil {
			return nil, err
		}
	}

	// Current physical hosts can have no stored rows after a restart, a
	// capture gap, or budget pruning. All such uncovered time is unknown.
	if index := q.readIndex(ctx); index != nil {
		state, err := index.capture.generation.controlState()
		if err != nil {
			return nil, err
		}
		hosts := map[string][]*Machine{}
		for _, m := range state.Machines {
			if m.Replacement == nil {
				hosts[hostID(m)] = append(hosts[hostID(m)], m)
			}
		}
		for host, members := range hosts {
			if _, found := classes[host]; !found {
				classes[host] = idleMachineClass(members)
			}
		}
	}
	for host, class := range classes {
		if err := r.add(ctx, host, maxTime(since, cursors[host]), through, HostIdleEvidence{Class: class, State: idleUnknown}, intervals); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *IdleReport) readIntervals(ctx context.Context, db *sql.DB, since, through time.Time, intervals bool, cursors map[string]time.Time, classes map[string]string) error {
	// Aggregate only scalar columns. Decode and charge retained payloads only
	// for an explicit raw interval read.
	query := "SELECT host,start,end,class,state,reason"
	if intervals {
		query += ",data"
	}
	query += " FROM host_idle_intervals WHERE end>? AND start<? ORDER BY host,start"
	rows, err := db.QueryContext(ctx, query, since.UnixNano(), through.UnixNano())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var host, class, state string
		var reason AdmissionPredicate
		var start, end int64
		var data []byte
		columns := []any{&host, &start, &end, &class, &state, &reason}
		if intervals {
			columns = append(columns, &data)
		}
		if err := rows.Scan(columns...); err != nil {
			return err
		}
		cursor := maxTime(since, cursors[host])
		from, until := maxTime(cursor, time.Unix(0, start).UTC()), minTime(through, time.Unix(0, end).UTC())
		if !until.After(from) {
			continue
		}
		if err := r.add(ctx, host, cursor, from, HostIdleEvidence{Class: class, State: idleUnknown}, intervals); err != nil {
			return err
		}
		e := HostIdleEvidence{Class: class, State: state, Reason: reason}
		if intervals {
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
		}
		if err := r.add(ctx, host, from, until, e, intervals); err != nil {
			return err
		}
		cursors[host], classes[host] = until, class
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (r *IdleReport) add(ctx context.Context, host string, from, until time.Time, e HostIdleEvidence, intervals bool) error {
	if !until.After(from) {
		return nil
	}
	class := r.ByMachineClass[e.Class]
	if class == nil {
		class = &IdleClassHours{States: map[string]float64{}, ByReason: map[AdmissionPredicate]float64{}}
		r.ByMachineClass[e.Class] = class
	}
	hours := until.Sub(from).Hours()
	class.States[e.State] += hours
	if e.State == idleRefused {
		r.MachineHours += hours
		r.ByReason[e.Reason] += hours
		class.ByReason[e.Reason] += hours
	}
	if intervals {
		interval := HostIdleInterval{Host: host, Start: from, End: until, HostIdleEvidence: e}
		if err := chargeProjectionValue(ctx, interval); err != nil {
			return err
		}
		r.Intervals = append(r.Intervals, interval)
	}
	return nil
}
