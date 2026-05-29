package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("store path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) SaveProject(ctx context.Context, project domain.Project) error {
	if project.ID == "" {
		return fmt.Errorf("project id is required")
	}
	return s.upsertJSON(ctx, "projects", project.ID, project)
}

func (s *Store) Project(ctx context.Context, id string) (domain.Project, error) {
	var project domain.Project
	err := s.getJSON(ctx, "projects", id, &project)
	return project, err
}

func (s *Store) ListProjects(ctx context.Context) ([]domain.Project, error) {
	var projects []domain.Project
	err := s.listJSON(ctx, "projects", &projects)
	return projects, err
}

func (s *Store) SavePreset(ctx context.Context, preset domain.Preset) error {
	if preset.ID == "" {
		return fmt.Errorf("preset id is required")
	}
	return s.upsertJSON(ctx, "presets", preset.ID, preset)
}

func (s *Store) Preset(ctx context.Context, id string) (domain.Preset, error) {
	var preset domain.Preset
	err := s.getJSON(ctx, "presets", id, &preset)
	return preset, err
}

func (s *Store) ListPresets(ctx context.Context) ([]domain.Preset, error) {
	var presets []domain.Preset
	err := s.listJSON(ctx, "presets", &presets)
	return presets, err
}

func (s *Store) SaveNode(ctx context.Context, node domain.Node) error {
	if node.ID == "" {
		return fmt.Errorf("node id is required")
	}
	return s.upsertJSON(ctx, "nodes", node.ID, node)
}

func (s *Store) Node(ctx context.Context, id string) (domain.Node, error) {
	var node domain.Node
	err := s.getJSON(ctx, "nodes", id, &node)
	return node, err
}

func (s *Store) ListNodes(ctx context.Context) ([]domain.Node, error) {
	var nodes []domain.Node
	err := s.listJSON(ctx, "nodes", &nodes)
	return nodes, err
}

func (s *Store) SaveInstance(ctx context.Context, inst domain.ModelInstance) error {
	if inst.ID == "" {
		return fmt.Errorf("instance id is required")
	}
	return s.upsertJSON(ctx, "instances", inst.ID, inst)
}

func (s *Store) Instance(ctx context.Context, id string) (domain.ModelInstance, error) {
	var inst domain.ModelInstance
	err := s.getJSON(ctx, "instances", id, &inst)
	return inst, err
}

func (s *Store) ListInstances(ctx context.Context) ([]domain.ModelInstance, error) {
	var instances []domain.ModelInstance
	err := s.listJSON(ctx, "instances", &instances)
	return instances, err
}

func (s *Store) DeleteInstance(ctx context.Context, id string) error {
	return s.deleteID(ctx, "instances", id)
}

func (s *Store) SaveLease(ctx context.Context, lease domain.Lease) error {
	if lease.ID == "" {
		return fmt.Errorf("lease id is required")
	}
	return s.upsertJSON(ctx, "leases", lease.ID, lease)
}

func (s *Store) ListLeases(ctx context.Context) ([]domain.Lease, error) {
	var leases []domain.Lease
	err := s.listJSON(ctx, "leases", &leases)
	return leases, err
}

func (s *Store) DeleteLease(ctx context.Context, id string) error {
	return s.deleteID(ctx, "leases", id)
}

func (s *Store) SaveReservation(ctx context.Context, reservation domain.Reservation) error {
	if reservation.ID == "" {
		return fmt.Errorf("reservation id is required")
	}
	return s.upsertJSON(ctx, "reservations", reservation.ID, reservation)
}

func (s *Store) ListReservations(ctx context.Context) ([]domain.Reservation, error) {
	var reservations []domain.Reservation
	err := s.listJSON(ctx, "reservations", &reservations)
	return reservations, err
}

func (s *Store) DeleteReservation(ctx context.Context, id string) error {
	return s.deleteID(ctx, "reservations", id)
}

func (s *Store) SaveJob(ctx context.Context, job domain.Job) error {
	if job.ID == "" {
		return fmt.Errorf("job id is required")
	}
	return s.upsertJSON(ctx, "jobs", job.ID, job)
}

func (s *Store) ListJobs(ctx context.Context) ([]domain.Job, error) {
	var jobs []domain.Job
	err := s.listJSON(ctx, "jobs", &jobs)
	return jobs, err
}

func (s *Store) SaveRecommendation(ctx context.Context, rec domain.RecommendationRecord) error {
	if rec.ID == "" {
		return fmt.Errorf("recommendation id is required")
	}
	if rec.CreatedAt.IsZero() {
		return fmt.Errorf("recommendation created_at is required")
	}
	return s.upsertJSON(ctx, "recommendations", rec.ID, rec)
}

func (s *Store) ListRecommendations(ctx context.Context, projectID string) ([]domain.RecommendationRecord, error) {
	recs, err := s.listRecommendationRecords(ctx)
	if err != nil {
		return nil, err
	}
	if projectID == "" {
		return recs, nil
	}
	out := make([]domain.RecommendationRecord, 0, len(recs))
	for _, rec := range recs {
		if rec.ProjectID == projectID {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (s *Store) Recommendation(ctx context.Context, id string) (domain.RecommendationRecord, error) {
	return s.recommendation(ctx, id)
}

func (s *Store) MarkRecommendationApplied(ctx context.Context, id string, at time.Time) error {
	rec, err := s.recommendation(ctx, id)
	if err != nil {
		return err
	}
	rec.Applied = true
	rec.AppliedAt = at.UTC()
	return s.SaveRecommendation(ctx, rec)
}

func (s *Store) SaveProcessRefs(ctx context.Context, nodeID string, refs []domain.ProcessRef) error {
	if nodeID == "" {
		return fmt.Errorf("node id is required")
	}
	data, err := json.Marshal(refs)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO process_refs (node_id, data) VALUES (?, ?)
ON CONFLICT(node_id) DO UPDATE SET data = excluded.data`, nodeID, string(data))
	return err
}

func (s *Store) ProcessRefs(ctx context.Context, nodeID string) ([]domain.ProcessRef, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT data FROM process_refs WHERE node_id = ?`, nodeID).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var refs []domain.ProcessRef
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func (s *Store) DeleteProcessRefs(ctx context.Context, nodeID string) error {
	return s.deleteColumn(ctx, "process_refs", "node_id", nodeID)
}

func (s *Store) SaveJoinToken(ctx context.Context, token domain.JoinTokenRecord) error {
	if token.Hash == "" {
		return fmt.Errorf("join token hash is required")
	}
	return s.upsertJSON(ctx, "join_tokens", token.Hash, token)
}

func (s *Store) ListJoinTokens(ctx context.Context) ([]domain.JoinTokenRecord, error) {
	var tokens []domain.JoinTokenRecord
	err := s.listJSON(ctx, "join_tokens", &tokens)
	return tokens, err
}

func (s *Store) Record(ctx context.Context, m domain.RunMetric) error {
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO UPDATE SET
	instance_id = excluded.instance_id,
	node_id = excluded.node_id,
	project = excluded.project,
	tokens_per_sec = excluded.tokens_per_sec,
	ttft_ms = excluded.ttft_ms,
	load_wall_clock_ms = excluded.load_wall_clock_ms,
	peak_vram_mb = excluded.peak_vram_mb,
	context_used = excluded.context_used,
	at = excluded.at`,
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

func (s *Store) Metrics(ctx context.Context, project string) ([]domain.RunMetric, error) {
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
	return out, rows.Err()
}

func (s *Store) upsertJSON(ctx context.Context, table, id string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (id, data) VALUES (?, ?)
ON CONFLICT(id) DO UPDATE SET data = excluded.data`, table), id, string(data))
	return err
}

func (s *Store) getJSON(ctx context.Context, table, id string, out any) error {
	if id == "" {
		return fmt.Errorf("%s id is required", table)
	}
	var raw string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT data FROM %s WHERE id = ?`, table), id).Scan(&raw)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(raw), out)
}

func (s *Store) listJSON(ctx context.Context, table string, out any) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT data FROM %s ORDER BY id`, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	values := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		values = append(values, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func (s *Store) listRecommendationRecords(ctx context.Context) ([]domain.RecommendationRecord, error) {
	var recs []domain.RecommendationRecord
	err := s.listJSON(ctx, "recommendations", &recs)
	return recs, err
}

func (s *Store) recommendation(ctx context.Context, id string) (domain.RecommendationRecord, error) {
	var rec domain.RecommendationRecord
	err := s.getJSON(ctx, "recommendations", id, &rec)
	return rec, err
}

func (s *Store) deleteID(ctx context.Context, table, id string) error {
	return s.deleteColumn(ctx, table, "id", id)
}

func (s *Store) deleteColumn(ctx context.Context, table, column, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", column)
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s = ?`, table, column), value)
	return err
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS presets (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS instances (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS leases (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reservations (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recommendations (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS process_refs (node_id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS join_tokens (id TEXT PRIMARY KEY, data TEXT NOT NULL);

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

var _ ports.TelemetryStore = (*Store)(nil)
