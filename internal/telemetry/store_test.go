package telemetry

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func TestSQLiteStoreRecordsAndFiltersMetrics(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)

	first := metric("job1", "project-a", 1000, now)
	second := metric("job2", "project-b", 2000, now.Add(time.Second))
	if err := store.Record(context.Background(), second); err != nil {
		t.Fatalf("Record second: %v", err)
	}
	if err := store.Record(context.Background(), first); err != nil {
		t.Fatalf("Record first: %v", err)
	}

	all, err := store.Metrics(context.Background(), "")
	if err != nil {
		t.Fatalf("Metrics all: %v", err)
	}
	if len(all) != 2 || all[0].JobID != "job1" || all[1].JobID != "job2" {
		t.Fatalf("all metrics = %+v", all)
	}

	filtered, err := store.Metrics(context.Background(), "project-b")
	if err != nil {
		t.Fatalf("Metrics filtered: %v", err)
	}
	if len(filtered) != 1 || filtered[0].JobID != "job2" || filtered[0].PresetID != "preset-job2" || filtered[0].Backend != domain.BackendLlamaCpp {
		t.Fatalf("filtered metrics = %+v", filtered)
	}
}

func TestSQLiteStoreRecordsAndFiltersSessionSamples(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)

	samples := []domain.SessionMetric{
		{
			SessionID:       "session-a",
			Sequence:        1,
			JobID:           "job-a",
			Phase:           domain.TelemetryPhasePlaced,
			InstanceID:      "inst-a",
			NodeID:          "node-a",
			PresetID:        "preset-a",
			Backend:         domain.BackendLlamaCpp,
			Project:         "project-a",
			LoadWallClockMS: 11,
			At:              now,
		},
		{
			SessionID:    "session-a",
			Sequence:     2,
			JobID:        "job-a",
			Phase:        domain.TelemetryPhaseComplete,
			InstanceID:   "inst-a",
			NodeID:       "node-a",
			PresetID:     "preset-a",
			Backend:      domain.BackendLlamaCpp,
			Project:      "project-a",
			TokensIn:     2,
			TokensOut:    5,
			ContextUsed:  7,
			TokensPerSec: 10,
			At:           now.Add(time.Second),
		},
		{
			SessionID: "session-b",
			Sequence:  1,
			JobID:     "job-b",
			Phase:     domain.TelemetryPhaseError,
			NodeID:    "node-b",
			Project:   "project-b",
			Error:     "backend overflow",
			At:        now.Add(2 * time.Second),
		},
	}
	for _, sample := range samples {
		if err := store.RecordSample(context.Background(), sample); err != nil {
			t.Fatalf("RecordSample: %v", err)
		}
	}

	bySession, err := store.Samples(context.Background(), domain.SessionMetricQuery{SessionID: "session-a"})
	if err != nil {
		t.Fatalf("Samples session: %v", err)
	}
	if len(bySession) != 2 || bySession[0].Phase != domain.TelemetryPhasePlaced || bySession[1].ContextUsed != 7 {
		t.Fatalf("bySession = %+v", bySession)
	}

	byNode, err := store.Samples(context.Background(), domain.SessionMetricQuery{NodeID: "node-b", Since: now.Add(time.Second), Limit: 1})
	if err != nil {
		t.Fatalf("Samples node: %v", err)
	}
	if len(byNode) != 1 || byNode[0].Error != "backend overflow" {
		t.Fatalf("byNode = %+v", byNode)
	}
}

func TestSQLiteStoreFailsLoudOnBadMetric(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	if err := store.Record(context.Background(), domain.RunMetric{At: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id err = %v", err)
	}
	if err := store.Record(context.Background(), domain.RunMetric{JobID: "job"}); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("missing timestamp err = %v", err)
	}
	if err := store.RecordSample(context.Background(), domain.SessionMetric{JobID: "job", Phase: domain.TelemetryPhasePlaced, At: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}); err == nil || !strings.Contains(err.Error(), "session id") {
		t.Fatalf("missing session id err = %v", err)
	}
	if err := store.RecordSample(context.Background(), domain.SessionMetric{SessionID: "session", Phase: domain.TelemetryPhasePlaced, At: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id err = %v", err)
	}
	if err := store.RecordSample(context.Background(), domain.SessionMetric{SessionID: "session", JobID: "job", At: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}); err == nil || !strings.Contains(err.Error(), "phase") {
		t.Fatalf("missing phase err = %v", err)
	}
	if err := store.RecordSample(context.Background(), domain.SessionMetric{SessionID: "session", JobID: "job", Phase: domain.TelemetryPhasePlaced}); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("missing sample timestamp err = %v", err)
	}
}

func TestNewSQLiteStoreFailsOnDirectoryPath(t *testing.T) {
	if _, err := NewSQLiteStore(t.TempDir()); err == nil {
		t.Fatal("expected directory path error")
	}
}

func TestSQLiteStoreMigratesOldRunMetricSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE run_metrics (
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
)`)
	if err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw sqlite: %v", err)
	}

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()
	metric := metric("job-old", "project-a", 1234, time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	if err := store.Record(context.Background(), metric); err != nil {
		t.Fatalf("Record migrated metric: %v", err)
	}
	got, err := store.Metrics(context.Background(), "project-a")
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(got) != 1 || got[0].PresetID != metric.PresetID || got[0].Backend != metric.Backend {
		t.Fatalf("migrated metrics = %+v", got)
	}
}

func TestEnsureColumnFailsForMissingTable(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if err := ensureColumn(context.Background(), db, "missing", "new_col", "TEXT"); err == nil {
		t.Fatal("expected missing table error")
	}
}

func TestRollupContextComputesStats(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	for i, ctxUsed := range []int{1000, 2000, 3000, 4000, 5000} {
		if err := store.Record(context.Background(), metric(string(rune('a'+i)), "project-a", ctxUsed, now.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	stats, err := RollupContext(context.Background(), store, "project-a")
	if err != nil {
		t.Fatalf("RollupContext: %v", err)
	}
	if stats.Count != 5 || stats.Average != 3000 || stats.P95 != 4000 || stats.LifetimeMax != 5000 {
		t.Fatalf("stats = %+v", stats)
	}

	empty, err := RollupContext(context.Background(), store, "missing")
	if err != nil {
		t.Fatalf("RollupContext empty: %v", err)
	}
	if empty != (ContextStats{}) {
		t.Fatalf("empty = %+v", empty)
	}
}

func TestRollupContextReturnsStoreError(t *testing.T) {
	_, err := RollupContext(context.Background(), failingStore{}, "project")
	if err == nil {
		t.Fatal("expected store error")
	}
}

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	return store
}

func metric(jobID, project string, contextUsed int, at time.Time) domain.RunMetric {
	return domain.RunMetric{
		JobID:           jobID,
		InstanceID:      "inst",
		NodeID:          "node",
		PresetID:        "preset-" + jobID,
		Backend:         domain.BackendLlamaCpp,
		Project:         project,
		TokensPerSec:    12.5,
		TTFTms:          34,
		LoadWallClockMS: 56,
		PeakVRAMMB:      78,
		ContextUsed:     contextUsed,
		At:              at,
	}
}

type failingStore struct{}

func (failingStore) Record(context.Context, domain.RunMetric) error {
	return nil
}

func (failingStore) RecordSample(context.Context, domain.SessionMetric) error {
	return nil
}

func (failingStore) Metrics(context.Context, string) ([]domain.RunMetric, error) {
	return nil, assertErr{}
}

func (failingStore) Samples(context.Context, domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	return nil, assertErr{}
}

type assertErr struct{}

func (assertErr) Error() string {
	return "assert"
}

var _ ports.TelemetryStore = failingStore{}
