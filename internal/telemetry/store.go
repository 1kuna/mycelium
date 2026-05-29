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
CREATE INDEX IF NOT EXISTS idx_run_metrics_project_at ON run_metrics(project, at);`)
	if err != nil {
		return err
	}
	if err := ensureColumn(ctx, s.db, "run_metrics", "preset_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return ensureColumn(ctx, s.db, "run_metrics", "backend", "TEXT NOT NULL DEFAULT ''")
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
