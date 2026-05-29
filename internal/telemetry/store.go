package telemetry

import (
	"context"
	"database/sql"
	"fmt"
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
	job_id, instance_id, node_id, project, tokens_per_sec, ttft_ms,
	load_wall_clock_ms, peak_vram_mb, context_used, at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.JobID,
		m.InstanceID,
		m.NodeID,
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

func (s *SQLiteStore) Metrics(ctx context.Context, project string) ([]domain.RunMetric, error) {
	query := `
SELECT job_id, instance_id, node_id, project, tokens_per_sec, ttft_ms,
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
		var at string
		if err := rows.Scan(
			&metric.JobID,
			&metric.InstanceID,
			&metric.NodeID,
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

func (s *SQLiteStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS run_metrics (
	job_id TEXT PRIMARY KEY,
	instance_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	project TEXT NOT NULL,
	tokens_per_sec REAL NOT NULL,
	ttft_ms INTEGER NOT NULL,
	load_wall_clock_ms INTEGER NOT NULL,
	peak_vram_mb INTEGER NOT NULL,
	context_used INTEGER NOT NULL,
	at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_metrics_project_at ON run_metrics(project, at);`)
	return err
}

var _ ports.TelemetryStore = (*SQLiteStore)(nil)
