package telemetry

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	store := &SQLiteStore{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) Record(ctx context.Context, m domain.RunMetric) error {
	if m.JobID == "" {
		return fmt.Errorf("run metric missing job id")
	}
	if m.At.IsZero() {
		return fmt.Errorf("run metric missing timestamp")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO run_metrics (
	job_id, instance_id, node_id, preset_id, backend, project, tokens_per_sec, ttft_ms,
	load_wall_clock_ms, peak_vram_mb, context_used, at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.JobID,
		m.InstanceID,
		m.NodeID,
		m.PresetID,
		string(m.Backend),
		m.Project,
		m.TokensPerSec,
		m.TTFTms,
		m.LoadWallClockMS,
		m.PeakVRAMMB,
		m.ContextUsed,
		m.At.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLiteStore) RecordSample(ctx context.Context, m domain.SessionMetric) error {
	if m.SessionID == "" {
		return fmt.Errorf("session metric missing session id")
	}
	if m.JobID == "" {
		return fmt.Errorf("session metric missing job id")
	}
	if m.Phase == "" {
		return fmt.Errorf("session metric missing phase")
	}
	if m.At.IsZero() {
		return fmt.Errorf("session metric missing timestamp")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO session_metrics (
	session_id, sequence, job_id, phase, instance_id, node_id, preset_id, backend, project,
	tokens_in, tokens_out, context_used, bytes_in, bytes_out, tokens_per_sec, ttft_ms, load_wall_clock_ms,
	peak_vram_mb, elapsed_ms, error, at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, sequence) DO UPDATE SET
	job_id = excluded.job_id,
	phase = excluded.phase,
	instance_id = excluded.instance_id,
	node_id = excluded.node_id,
	preset_id = excluded.preset_id,
	backend = excluded.backend,
	project = excluded.project,
	tokens_in = excluded.tokens_in,
	tokens_out = excluded.tokens_out,
	context_used = excluded.context_used,
	bytes_in = excluded.bytes_in,
	bytes_out = excluded.bytes_out,
	tokens_per_sec = excluded.tokens_per_sec,
	ttft_ms = excluded.ttft_ms,
	load_wall_clock_ms = excluded.load_wall_clock_ms,
	peak_vram_mb = excluded.peak_vram_mb,
	elapsed_ms = excluded.elapsed_ms,
	error = excluded.error,
	at = excluded.at`,
		m.SessionID,
		m.Sequence,
		m.JobID,
		string(m.Phase),
		m.InstanceID,
		m.NodeID,
		m.PresetID,
		string(m.Backend),
		m.Project,
		m.TokensIn,
		m.TokensOut,
		m.ContextUsed,
		m.BytesIn,
		m.BytesOut,
		m.TokensPerSec,
		m.TTFTms,
		m.LoadWallClockMS,
		m.PeakVRAMMB,
		m.ElapsedMS,
		m.Error,
		m.At.UTC().Format(time.RFC3339Nano),
	)
	return err
}

func (s *SQLiteStore) Metrics(ctx context.Context, project string) ([]domain.RunMetric, error) {
	query := `
SELECT job_id, instance_id, node_id, preset_id, backend, project, tokens_per_sec, ttft_ms,
	load_wall_clock_ms, peak_vram_mb, context_used, at
FROM run_metrics`
	args := []any{}
	if project != "" {
		query += " WHERE project = ?"
		args = append(args, project)
	}
	query += " ORDER BY at, job_id"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.RunMetric
	for rows.Next() {
		var metric domain.RunMetric
		var backend string
		var at string
		if err := rows.Scan(
			&metric.JobID,
			&metric.InstanceID,
			&metric.NodeID,
			&metric.PresetID,
			&backend,
			&metric.Project,
			&metric.TokensPerSec,
			&metric.TTFTms,
			&metric.LoadWallClockMS,
			&metric.PeakVRAMMB,
			&metric.ContextUsed,
			&at,
		); err != nil {
			return nil, err
		}
		metric.Backend = domain.Backend(backend)
		parsed, err := time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		metric.At = parsed
		out = append(out, metric)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLiteStore) Samples(ctx context.Context, query domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	sqlQuery := `
SELECT session_id, sequence, job_id, phase, instance_id, node_id, preset_id, backend, project,
	tokens_in, tokens_out, context_used, bytes_in, bytes_out, tokens_per_sec, ttft_ms, load_wall_clock_ms,
	peak_vram_mb, elapsed_ms, error, at
FROM session_metrics`
	clauses := []string{}
	args := []any{}
	if query.SessionID != "" {
		clauses = append(clauses, "session_id = ?")
		args = append(args, query.SessionID)
	}
	if query.Project != "" {
		clauses = append(clauses, "project = ?")
		args = append(args, query.Project)
	}
	if query.NodeID != "" {
		clauses = append(clauses, "node_id = ?")
		args = append(args, query.NodeID)
	}
	if !query.Since.IsZero() {
		clauses = append(clauses, "at >= ?")
		args = append(args, query.Since.UTC().Format(time.RFC3339Nano))
	}
	if !query.Until.IsZero() {
		clauses = append(clauses, "at <= ?")
		args = append(args, query.Until.UTC().Format(time.RFC3339Nano))
	}
	if len(clauses) > 0 {
		sqlQuery += " WHERE " + strings.Join(clauses, " AND ")
	}
	sqlQuery += " ORDER BY at, session_id, sequence"
	if query.Limit > 0 {
		sqlQuery += " LIMIT ?"
		args = append(args, query.Limit)
	}

	rows, err := s.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.SessionMetric
	for rows.Next() {
		var sample domain.SessionMetric
		var phase string
		var backend string
		var at string
		if err := rows.Scan(
			&sample.SessionID,
			&sample.Sequence,
			&sample.JobID,
			&phase,
			&sample.InstanceID,
			&sample.NodeID,
			&sample.PresetID,
			&backend,
			&sample.Project,
			&sample.TokensIn,
			&sample.TokensOut,
			&sample.ContextUsed,
			&sample.BytesIn,
			&sample.BytesOut,
			&sample.TokensPerSec,
			&sample.TTFTms,
			&sample.LoadWallClockMS,
			&sample.PeakVRAMMB,
			&sample.ElapsedMS,
			&sample.Error,
			&at,
		); err != nil {
			return nil, err
		}
		sample.Phase = domain.TelemetryPhase(phase)
		sample.Backend = domain.Backend(backend)
		parsed, err := time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		sample.At = parsed
		out = append(out, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS run_metrics (
	job_id TEXT PRIMARY KEY,
	instance_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	preset_id TEXT NOT NULL DEFAULT '',
	backend TEXT NOT NULL DEFAULT '',
	project TEXT NOT NULL,
	tokens_per_sec REAL NOT NULL,
	ttft_ms INTEGER NOT NULL,
	load_wall_clock_ms INTEGER NOT NULL,
	peak_vram_mb INTEGER NOT NULL,
	context_used INTEGER NOT NULL,
	at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_metrics_project_at ON run_metrics(project, at);

CREATE TABLE IF NOT EXISTS session_metrics (
	session_id TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	job_id TEXT NOT NULL,
	phase TEXT NOT NULL,
	instance_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	preset_id TEXT NOT NULL DEFAULT '',
	backend TEXT NOT NULL DEFAULT '',
	project TEXT NOT NULL,
	tokens_in INTEGER NOT NULL DEFAULT 0,
	tokens_out INTEGER NOT NULL DEFAULT 0,
	context_used INTEGER NOT NULL DEFAULT 0,
	bytes_in INTEGER NOT NULL DEFAULT 0,
	bytes_out INTEGER NOT NULL DEFAULT 0,
	tokens_per_sec REAL NOT NULL DEFAULT 0,
	ttft_ms INTEGER NOT NULL DEFAULT 0,
	load_wall_clock_ms INTEGER NOT NULL DEFAULT 0,
	peak_vram_mb INTEGER NOT NULL DEFAULT 0,
	elapsed_ms INTEGER NOT NULL DEFAULT 0,
	error TEXT NOT NULL DEFAULT '',
	at TEXT NOT NULL,
	PRIMARY KEY(session_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_session_metrics_project_at ON session_metrics(project, at);
CREATE INDEX IF NOT EXISTS idx_session_metrics_node_at ON session_metrics(node_id, at);`)
	if err != nil {
		return err
	}
	if err := ensureColumn(ctx, s.db, "run_metrics", "preset_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(ctx, s.db, "run_metrics", "backend", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(ctx, s.db, "session_metrics", "bytes_in", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	return ensureColumn(ctx, s.db, "session_metrics", "bytes_out", "INTEGER NOT NULL DEFAULT 0")
}

func ensureColumn(ctx context.Context, db *sql.DB, table, column, spec string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, spec))
	return err
}

var _ ports.TelemetryStore = (*SQLiteStore)(nil)
