package peer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const watcherBuffer = 128

type JobRegistry struct {
	mu          sync.Mutex
	records     map[string]domain.JobRecord
	watchers    map[int]chan domain.JobRecord
	nextWatcher int
}

func NewJobRegistry() *JobRegistry {
	return &JobRegistry{
		records:  map[string]domain.JobRecord{},
		watchers: map[int]chan domain.JobRecord{},
	}
}

func Cursor(rec domain.JobRecord) string {
	return rec.UpdatedAt.UTC().Format(time.RFC3339Nano)
}

func (r *JobRegistry) Put(ctx context.Context, rec domain.JobRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRecord(rec); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.records == nil {
		r.records = map[string]domain.JobRecord{}
	}
	if r.watchers == nil {
		r.watchers = map[int]chan domain.JobRecord{}
	}
	rec = cloneRecord(rec)
	if existing, ok := r.records[rec.JobID]; ok && !newerRecord(rec, existing) {
		return nil
	}
	r.records[rec.JobID] = rec
	for id, ch := range r.watchers {
		select {
		case ch <- cloneRecord(rec):
		default:
			delete(r.watchers, id)
			close(ch)
			return fmt.Errorf("job registry watcher %d is not draining", id)
		}
	}
	return nil
}

func (r *JobRegistry) Merge(ctx context.Context, records []domain.JobRecord) error {
	for _, rec := range records {
		if err := r.Put(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *JobRegistry) Watch(ctx context.Context, fromCursor string) (<-chan domain.JobRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cursor, err := parseCursor(fromCursor)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.records == nil {
		r.records = map[string]domain.JobRecord{}
	}
	if r.watchers == nil {
		r.watchers = map[int]chan domain.JobRecord{}
	}
	pending := recordsAfter(r.records, cursor)
	ch := make(chan domain.JobRecord, watcherBuffer+len(pending))
	for _, rec := range pending {
		ch <- rec
	}
	id := r.nextWatcher
	r.nextWatcher++
	r.watchers[id] = ch
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		defer r.mu.Unlock()
		if current, ok := r.watchers[id]; ok {
			delete(r.watchers, id)
			close(current)
		}
	}()
	return ch, nil
}

func (r *JobRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordsAfter(r.records, time.Time{}), nil
}

func validateRecord(rec domain.JobRecord) error {
	if rec.JobID == "" {
		return fmt.Errorf("job record id is required")
	}
	if rec.Coordinator == "" {
		return fmt.Errorf("job record coordinator is required")
	}
	if rec.Status == "" {
		return fmt.Errorf("job record status is required")
	}
	if len(rec.Request) == 0 {
		return fmt.Errorf("job record request payload is required")
	}
	if rec.UpdatedAt.IsZero() {
		return fmt.Errorf("job record updated_at is required")
	}
	return nil
}

func parseCursor(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	cursor, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse job registry cursor: %w", err)
	}
	return cursor, nil
}

func recordsAfter(records map[string]domain.JobRecord, cursor time.Time) []domain.JobRecord {
	out := make([]domain.JobRecord, 0, len(records))
	for _, rec := range records {
		if cursor.IsZero() || rec.UpdatedAt.After(cursor) {
			out = append(out, cloneRecord(rec))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].JobID < out[j].JobID
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	return out
}

func newerRecord(next, current domain.JobRecord) bool {
	if !next.UpdatedAt.Equal(current.UpdatedAt) {
		return next.UpdatedAt.After(current.UpdatedAt)
	}
	if next.Fence != current.Fence {
		return next.Fence > current.Fence
	}
	return recordTieKey(next) > recordTieKey(current)
}

func recordTieKey(rec domain.JobRecord) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", rec.Coordinator, rec.AssignedNode, rec.Status, rec.Request, rec.Fence)
}

func cloneRecord(rec domain.JobRecord) domain.JobRecord {
	rec.Request = append([]byte(nil), rec.Request...)
	return rec
}

var _ ports.JobRegistry = (*JobRegistry)(nil)
