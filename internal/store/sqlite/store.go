package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"

	_ "modernc.org/sqlite"
)

type Store struct {
	db             *sql.DB
	mu             sync.Mutex
	jobWatchers    map[int]chan domain.JobRecord
	nextJobWatcher int
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
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
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

func (s *Store) SaveAdmissionState(ctx context.Context, state domain.AdmissionState) error {
	if state.NodeID == "" {
		return fmt.Errorf("admission state node id is required")
	}
	return s.upsertJSON(ctx, "admission_states", state.NodeID, state)
}

func (s *Store) AdmissionState(ctx context.Context, nodeID string) (domain.AdmissionState, bool, error) {
	var state domain.AdmissionState
	err := s.getJSON(ctx, "admission_states", nodeID, &state)
	if err == sql.ErrNoRows {
		return domain.AdmissionState{}, false, nil
	}
	if err != nil {
		return domain.AdmissionState{}, false, err
	}
	return state, true, nil
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

func (s *Store) Job(ctx context.Context, id string) (domain.Job, error) {
	var job domain.Job
	err := s.getJSON(ctx, "jobs", id, &job)
	return job, err
}

func (s *Store) ListJobs(ctx context.Context) ([]domain.Job, error) {
	var jobs []domain.Job
	err := s.listJSON(ctx, "jobs", &jobs)
	return jobs, err
}

func (s *Store) Put(ctx context.Context, rec domain.JobRecord) error {
	if err := validateJobRecord(rec); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec = cloneJobRecord(rec)
	existing, ok, err := s.jobRecord(ctx, rec.JobID)
	if err != nil {
		return err
	}
	if ok && !newerJobRecord(rec, existing) {
		return nil
	}
	if err := s.upsertJSON(ctx, "job_records", rec.JobID, rec); err != nil {
		return err
	}
	for id, ch := range s.jobWatchers {
		select {
		case ch <- cloneJobRecord(rec):
		default:
			delete(s.jobWatchers, id)
			close(ch)
			return fmt.Errorf("sqlite job registry watcher %d is not draining", id)
		}
	}
	return nil
}

func (s *Store) Watch(ctx context.Context, fromCursor string) (<-chan domain.JobRecord, error) {
	cursor, err := parseJobRecordCursor(fromCursor)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, err := s.jobRecordsAfter(ctx, cursor)
	if err != nil {
		return nil, err
	}
	if s.jobWatchers == nil {
		s.jobWatchers = map[int]chan domain.JobRecord{}
	}
	ch := make(chan domain.JobRecord, len(pending)+128)
	for _, rec := range pending {
		ch <- rec
	}
	id := s.nextJobWatcher
	s.nextJobWatcher++
	s.jobWatchers[id] = ch
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		if current, ok := s.jobWatchers[id]; ok {
			delete(s.jobWatchers, id)
			close(current)
		}
	}()
	return ch, nil
}

func (s *Store) Snapshot(ctx context.Context) ([]domain.JobRecord, error) {
	return s.jobRecordsAfter(ctx, time.Time{})
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

func (s *Store) SaveModelLocality(ctx context.Context, locality domain.ModelLocality) error {
	if locality.ID == "" {
		return fmt.Errorf("model locality id is required")
	}
	if locality.PresetID == "" {
		return fmt.Errorf("model locality preset id is required")
	}
	if locality.NodeID == "" {
		return fmt.Errorf("model locality node id is required")
	}
	if locality.State == "" {
		return fmt.Errorf("model locality state is required")
	}
	return s.upsertJSON(ctx, "model_localities", locality.ID, locality)
}

func (s *Store) ListModelLocalities(ctx context.Context) ([]domain.ModelLocality, error) {
	var localities []domain.ModelLocality
	err := s.listJSON(ctx, "model_localities", &localities)
	return localities, err
}

func (s *Store) DeleteModelLocality(ctx context.Context, id string) error {
	return s.deleteID(ctx, "model_localities", id)
}

func (s *Store) SaveLocalityPlan(ctx context.Context, plan domain.LocalityPlan) error {
	if plan.ID == "" {
		return fmt.Errorf("locality plan id is required")
	}
	if plan.CreatedAt.IsZero() {
		return fmt.Errorf("locality plan created_at is required")
	}
	return s.upsertJSON(ctx, "locality_plans", plan.ID, plan)
}

func (s *Store) LocalityPlan(ctx context.Context, id string) (domain.LocalityPlan, error) {
	var plan domain.LocalityPlan
	err := s.getJSON(ctx, "locality_plans", id, &plan)
	return plan, err
}

func (s *Store) ListLocalityPlans(ctx context.Context) ([]domain.LocalityPlan, error) {
	var plans []domain.LocalityPlan
	err := s.listJSON(ctx, "locality_plans", &plans)
	return plans, err
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
	job_id, instance_id, node_id, preset_id, backend, project, tokens_per_sec, ttft_ms,
	load_wall_clock_ms, peak_vram_mb, context_used, at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO UPDATE SET
	instance_id = excluded.instance_id,
	node_id = excluded.node_id,
	preset_id = excluded.preset_id,
	backend = excluded.backend,
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

func (s *Store) RecordSample(ctx context.Context, m domain.SessionMetric) error {
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

func (s *Store) Metrics(ctx context.Context, project string) ([]domain.RunMetric, error) {
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
	return out, rows.Err()
}

func (s *Store) Samples(ctx context.Context, query domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
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

func (s *Store) jobRecord(ctx context.Context, id string) (domain.JobRecord, bool, error) {
	var rec domain.JobRecord
	err := s.getJSON(ctx, "job_records", id, &rec)
	if err == sql.ErrNoRows {
		return domain.JobRecord{}, false, nil
	}
	if err != nil {
		return domain.JobRecord{}, false, err
	}
	return rec, true, nil
}

func (s *Store) jobRecordsAfter(ctx context.Context, cursor time.Time) ([]domain.JobRecord, error) {
	var records []domain.JobRecord
	if err := s.listJSON(ctx, "job_records", &records); err != nil {
		return nil, err
	}
	out := records[:0]
	for _, rec := range records {
		if cursor.IsZero() || rec.UpdatedAt.After(cursor) {
			out = append(out, cloneJobRecord(rec))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].JobID < out[j].JobID
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	return out, nil
}

func validateJobRecord(rec domain.JobRecord) error {
	if rec.JobID == "" {
		return fmt.Errorf("job record id is required")
	}
	if rec.Coordinator == "" {
		return fmt.Errorf("job record coordinator is required")
	}
	if rec.Status == "" {
		return fmt.Errorf("job record status is required")
	}
	if rec.PayloadRedacted && rec.Handling != domain.HandlingPrivate {
		return fmt.Errorf("job record payload redaction requires private handling")
	}
	if len(rec.Request) == 0 && !rec.PayloadRedacted {
		return fmt.Errorf("job record request payload is required")
	}
	if rec.UpdatedAt.IsZero() {
		return fmt.Errorf("job record updated_at is required")
	}
	return nil
}

func parseJobRecordCursor(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	cursor, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse job registry cursor: %w", err)
	}
	return cursor, nil
}

func newerJobRecord(next, current domain.JobRecord) bool {
	if !next.UpdatedAt.Equal(current.UpdatedAt) {
		return next.UpdatedAt.After(current.UpdatedAt)
	}
	if next.Fence != current.Fence {
		return next.Fence > current.Fence
	}
	return jobRecordTieKey(next) > jobRecordTieKey(current)
}

func jobRecordTieKey(rec domain.JobRecord) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", rec.Coordinator, rec.AssignedNode, rec.Status, rec.Request, rec.Fence)
}

func cloneJobRecord(rec domain.JobRecord) domain.JobRecord {
	rec.Request = append([]byte(nil), rec.Request...)
	return rec
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
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS presets (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS instances (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS leases (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS admission_states (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reservations (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS job_records (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recommendations (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS process_refs (node_id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS join_tokens (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS model_localities (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS locality_plans (id TEXT PRIMARY KEY, data TEXT NOT NULL);

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

var _ ports.TelemetryStore = (*Store)(nil)
var _ ports.JobRegistry = (*Store)(nil)
var _ ports.ModelInventory = (*Store)(nil)
var _ ports.LocalityPlanStore = (*Store)(nil)
