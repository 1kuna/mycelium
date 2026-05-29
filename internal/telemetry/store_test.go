package telemetry

import (
	"context"
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
	if len(filtered) != 1 || filtered[0].JobID != "job2" {
		t.Fatalf("filtered metrics = %+v", filtered)
	}
}

func TestSQLiteStoreFailsLoudOnBadMetric(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	if err := store.Record(context.Background(), domain.RunMetric{At: time.Now()}); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id err = %v", err)
	}
	if err := store.Record(context.Background(), domain.RunMetric{JobID: "job"}); err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("missing timestamp err = %v", err)
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

func (failingStore) Metrics(context.Context, string) ([]domain.RunMetric, error) {
	return nil, assertErr{}
}

type assertErr struct{}

func (assertErr) Error() string {
	return "assert"
}

var _ ports.TelemetryStore = failingStore{}
