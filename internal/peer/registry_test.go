package peer

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestJobRegistryPutSnapshotAndLWW(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	base := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-b"))
	base.Coordinator = "peer-a"
	base.UpdatedAt = time.Unix(10, 0).UTC()
	base.Request = []byte("base")
	if err := registry.Put(ctx, base); err != nil {
		t.Fatalf("Put base: %v", err)
	}

	older := base
	older.Status = domain.JobRunning
	older.UpdatedAt = base.UpdatedAt.Add(-time.Second)
	if err := registry.Put(ctx, older); err != nil {
		t.Fatalf("Put older: %v", err)
	}

	newer := base
	newer.Status = domain.JobRunning
	newer.AssignedNode = "node-a"
	newer.Fence = 2
	newer.UpdatedAt = base.UpdatedAt.Add(time.Second)
	newer.Request = []byte("newer")
	if err := registry.Put(ctx, newer); err != nil {
		t.Fatalf("Put newer: %v", err)
	}
	newer.Request[0] = 'X'

	lowerFence := newer
	lowerFence.Status = domain.JobDone
	lowerFence.Fence = 1
	lowerFence.Request = []byte("lower")
	if err := registry.Put(ctx, lowerFence); err != nil {
		t.Fatalf("Put lower fence: %v", err)
	}

	tieWinner := newer
	tieWinner.Coordinator = "peer-z"
	tieWinner.Request = []byte("tie")
	if err := registry.Put(ctx, tieWinner); err != nil {
		t.Fatalf("Put tie winner: %v", err)
	}

	other := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-a"))
	other.Coordinator = "peer-a"
	other.UpdatedAt = tieWinner.UpdatedAt
	other.Request = []byte("other")
	if err := registry.Put(ctx, other); err != nil {
		t.Fatalf("Put other: %v", err)
	}
	later := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-c"))
	later.Coordinator = "peer-a"
	later.UpdatedAt = tieWinner.UpdatedAt.Add(time.Second)
	later.Request = []byte("later")
	if err := registry.Put(ctx, later); err != nil {
		t.Fatalf("Put later: %v", err)
	}

	snap, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 3 || snap[0].JobID != "job-a" || snap[1].JobID != "job-b" || snap[2].JobID != "job-c" {
		t.Fatalf("snapshot order = %+v", snap)
	}
	if snap[1].Coordinator != "peer-z" || string(snap[1].Request) != "tie" || snap[1].Status != domain.JobRunning {
		t.Fatalf("lww record = %+v", snap[1])
	}
	snap[1].Request[0] = '!'
	again, err := registry.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot again: %v", err)
	}
	if string(again[1].Request) != "tie" {
		t.Fatalf("snapshot mutated registry request: %q", again[1].Request)
	}
}

func TestJobRegistryValidationAndContext(t *testing.T) {
	registry := NewJobRegistry()
	valid := fixtures.MakeJobRecord()
	checks := []struct {
		name string
		rec  domain.JobRecord
	}{
		{name: "id", rec: recordWith(valid, func(r *domain.JobRecord) { r.JobID = "" })},
		{name: "coordinator", rec: recordWith(valid, func(r *domain.JobRecord) { r.Coordinator = "" })},
		{name: "status", rec: recordWith(valid, func(r *domain.JobRecord) { r.Status = "" })},
		{name: "request", rec: recordWith(valid, func(r *domain.JobRecord) { r.Request = nil })},
		{name: "updated", rec: recordWith(valid, func(r *domain.JobRecord) { r.UpdatedAt = time.Time{} })},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := registry.Put(context.Background(), check.rec); err == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
	redactedStandard := recordWith(valid, func(r *domain.JobRecord) {
		r.Request = nil
		r.PayloadRedacted = true
	})
	if err := registry.Put(context.Background(), redactedStandard); err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("redacted standard err = %v", err)
	}
	redactedPrivate := recordWith(valid, func(r *domain.JobRecord) {
		r.Request = nil
		r.Handling = domain.HandlingPrivate
		r.PayloadRedacted = true
	})
	if err := registry.Put(context.Background(), redactedPrivate); err != nil {
		t.Fatalf("redacted private should be accepted: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.Put(ctx, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Put err = %v", err)
	}
	if _, err := registry.Watch(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Watch err = %v", err)
	}
	if _, err := registry.Watch(context.Background(), "bad-cursor"); err == nil {
		t.Fatal("bad cursor accepted")
	}
}

func TestJobRegistryWatchAndMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	registry := NewJobRegistry()
	first := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-a"))
	first.UpdatedAt = time.Unix(20, 0).UTC()
	second := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-b"))
	second.UpdatedAt = first.UpdatedAt.Add(time.Second)
	if err := registry.Merge(ctx, []domain.JobRecord{second, first}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	watch, err := registry.Watch(ctx, Cursor(first))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := receiveRecord(t, watch); got.JobID != "job-b" {
		t.Fatalf("initial watch record = %+v", got)
	}

	third := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-c"))
	third.UpdatedAt = second.UpdatedAt.Add(time.Second)
	if err := registry.Put(ctx, third); err != nil {
		t.Fatalf("Put future: %v", err)
	}
	if got := receiveRecord(t, watch); got.JobID != "job-c" {
		t.Fatalf("future watch record = %+v", got)
	}
	cancel()
	if got, ok := receiveClosed(t, watch); ok {
		t.Fatalf("watch stayed open with record %+v", got)
	}

	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := registry.Merge(canceled, []domain.JobRecord{third}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Merge err = %v", err)
	}

	zeroCtx, zeroCancel := context.WithCancel(context.Background())
	var zero JobRegistry
	zeroWatch, err := zero.Watch(zeroCtx, "")
	if err != nil {
		t.Fatalf("zero Watch: %v", err)
	}
	zeroCancel()
	if got, ok := receiveClosed(t, zeroWatch); ok {
		t.Fatalf("zero watch stayed open with record %+v", got)
	}
	var zeroPut JobRegistry
	if err := zeroPut.Put(context.Background(), first); err != nil {
		t.Fatalf("zero Put: %v", err)
	}
}

func TestJobRegistryRejectsStalledWatchers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewJobRegistry()
	watch, err := registry.Watch(ctx, "")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	for i := 0; i < watcherBuffer; i++ {
		rec := fixtures.MakeJobRecord(fixtures.WithRecordJobID(fmt.Sprintf("job-%03d", i)))
		rec.UpdatedAt = time.Unix(30+int64(i), 0).UTC()
		if err := registry.Put(ctx, rec); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	overflow := fixtures.MakeJobRecord(fixtures.WithRecordJobID("job-overflow"))
	overflow.UpdatedAt = time.Unix(100, 0).UTC()
	if err := registry.Put(ctx, overflow); err == nil || !strings.Contains(err.Error(), "not draining") {
		t.Fatalf("overflow err = %v", err)
	}
	for range watch {
	}
}

func recordWith(rec domain.JobRecord, mutate func(*domain.JobRecord)) domain.JobRecord {
	mutate(&rec)
	return rec
}

func receiveRecord(t *testing.T, ch <-chan domain.JobRecord) domain.JobRecord {
	t.Helper()
	select {
	case rec, ok := <-ch:
		if !ok {
			t.Fatal("watch closed")
		}
		return rec
	default:
		t.Fatal("record was not ready")
	}
	return domain.JobRecord{}
}

func receiveClosed(t *testing.T, ch <-chan domain.JobRecord) (domain.JobRecord, bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case rec, ok := <-ch:
			return rec, ok
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("watch close was not ready")
	return domain.JobRecord{}, false
}
