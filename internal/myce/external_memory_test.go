package myce

import (
	"math"
	"testing"
	"time"
)

func TestExternalMemoryBucketingRetentionAndSuggestion(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	q := &Queue{clock: clock}
	m := Machine{ID: "worker", HostID: "host", MemoryObservation: &HostMemoryObservation{TotalGB: 24, UsedGB: 12, MyceliumGB: 2}}
	q.recordExternalMemory(&m)
	peak := clock.now
	for range 99 {
		clock.now = clock.now.Add(time.Second)
		m.MemoryObservation.UsedGB = 6
		q.recordExternalMemory(&m)
	}
	h := q.externalMemoryStatus("host")
	if h.Day.Samples != 100 || h.Day.Buckets != 1 || h.Day.MaxGB != 10 || !h.Day.PeakAt.Equal(peak) || h.Day.P50GB < 4 || h.Day.P50GB > 4.8 || h.Day.P95GB != h.Day.P50GB {
		t.Fatalf("wrong summary: %+v", h)
	}
	suggestion, err := SuggestHostBudget(24, 4, 2, h)
	if err != nil || suggestion.SuggestedGB != 8 || !suggestion.DryRun {
		t.Fatalf("suggestion %+v %v", suggestion, err)
	}
	clock.now = peak.Add(24*time.Hour + externalBucketWidth)
	h = q.externalMemoryStatus("host")
	if h.Day.Samples != 0 || h.Week.Samples != 100 {
		t.Fatal("24h retention", h)
	}
	for range externalBucketCount + 5 {
		clock.now = clock.now.Add(externalBucketWidth)
		q.recordExternalMemory(&m)
	}
	h = q.externalMemoryStatus("host")
	if h.Week.MaxGB != 4 || h.Week.Buckets != externalBucketCount {
		t.Fatal("ring retention", h)
	}
	clock.now = clock.now.Add(8 * 24 * time.Hour)
	h = q.externalMemoryStatus("host")
	if h.Week.Summary() != "unknown (no samples)" {
		t.Fatal("missing samples displayed as measured", h)
	}
	if h.Week.Samples != 0 {
		t.Fatal("offline samples did not expire", h)
	}
	if _, err = SuggestHostBudget(24, 4, 0, h); err == nil {
		t.Fatal("suggested without evidence")
	}
}

func TestExternalMemoryRejectsInvalidAndUsesCurrentRSS(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	q := &Queue{clock: clock}
	m := Machine{ID: "host"}
	for _, o := range []*HostMemoryObservation{nil, {TotalGB: 16, UsedGB: 3, MyceliumGB: 4}, {TotalGB: 16, UsedGB: math.NaN()}, {TotalGB: 16, UsedGB: 17}} {
		m.MemoryObservation = o
		q.recordExternalMemory(&m)
	}
	if len(q.s.ExternalMemory) != 0 {
		t.Fatal("retained invalid observations")
	}
	rss, err := workerTreeRSS([]byte("1 0 1024\n10 1 1048576\n12 11 1048576\n11 10 1048576\n20 1 999999\n"), 10)
	if err != nil || rss != 3 {
		t.Fatalf("tree RSS %v %v", rss, err)
	}
	if _, err = workerTreeRSS([]byte("1 0 1\n"), 10); err == nil {
		t.Fatal("missing worker accepted")
	}
}

func TestExternalMemorySuggestionGuardAndClamp(t *testing.T) {
	h := &ExternalMemoryHistory{Week: ExternalMemoryWindow{Samples: 1, MaxGB: 20}}
	s, err := SuggestHostBudget(32, 4, 1, h)
	if err != nil || s.HeadroomGB != 5 || s.SuggestedGB != 6 {
		t.Fatal(s, err)
	}
	s, err = SuggestHostBudget(16, 4, 0, h)
	if err != nil || s.SuggestedGB != 0 {
		t.Fatal(s, err)
	}
	if _, err = SuggestHostBudget(16, math.NaN(), 0, h); err == nil {
		t.Fatal("invalid headroom accepted")
	}
}

func TestExternalMemoryPersistsAcrossQueueRestart(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	q, err := Open(dir+"/queue.db", dir+"/checkpoints", clock)
	if err != nil {
		t.Fatal(err)
	}
	m := machine("host", "fake")
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 64, UsedGB: 10, MyceliumGB: 2}
	if _, err = q.Checkin(Checkin{Machine: m}); err != nil {
		q.Close()
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(dir+"/queue.db", dir+"/checkpoints", clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Machines) != 1 || s.Machines[0].ExternalMemory == nil || s.Machines[0].ExternalMemory.Week.MaxGB != 8 {
		t.Fatal("history did not survive restart", s)
	}
}
