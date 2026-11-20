package myce

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// Startup holds the queue's exclusive file lock and has not started API or
// maintenance writers. Disjoint rowid ranges can therefore read the same durable
// generation concurrently. No schema/format change or second persisted copy.
// Ordinary readers and all writes retain the single-connection boundary.
func readStartupQueueRecords(db *sql.DB, visit func(string, []byte), workers int) (map[string][]byte, error) {
	var first, last int64
	err := db.QueryRow("SELECT rowid FROM queue_state ORDER BY rowid LIMIT 1").Scan(&first)
	if err == sql.ErrNoRows {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := db.QueryRow("SELECT rowid FROM queue_state ORDER BY rowid DESC LIMIT 1").Scan(&last); err != nil {
		return nil, err
	}
	// Writer-generated rowids are positive. Preserve generic recovery of unusual
	// imported layouts without overflow in range arithmetic.
	if first < 0 || last-first < int64(workers) {
		return readQueueRecordsVisit(db, visit)
	}
	db.SetMaxOpenConns(workers)
	ctx, cancel := context.WithCancel(context.Background())
	rows := &startupRecordRows{cancel: cancel, records: make(chan startupRecord, workers)}
	var group sync.WaitGroup
	low := first
	width, extra := (last-first)/int64(workers), (last-first)%int64(workers)
	for i := 0; i < workers; i++ {
		high := low + width
		if int64(i) < extra {
			high++
		}
		query := "SELECT key,data FROM queue_state WHERE rowid>=? AND rowid<?"
		args := []any{low, high}
		if i == workers-1 {
			query = "SELECT key,data FROM queue_state WHERE rowid>=?"
			args = []any{low}
		}
		group.Add(1)
		go func(query string, args []any) {
			defer group.Done()
			result, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				rows.send(ctx, startupRecord{err: err})
				return
			}
			defer result.Close()
			for result.Next() {
				var record startupRecord
				if err := result.Scan(&record.key, &record.data); err != nil {
					rows.send(ctx, startupRecord{err: err})
					return
				}
				if !rows.send(ctx, record) {
					return
				}
			}
			if err := result.Err(); err != nil {
				rows.send(ctx, startupRecord{err: err})
			}
		}(query, args)
		low = high
	}
	rows.done = make(chan struct{})
	go func() { group.Wait(); close(rows.records); close(rows.done) }()
	defer db.SetMaxOpenConns(1)
	return readQueueRecordRows(rows, visit)
}

type startupRecord struct {
	key  string
	data []byte
	err  error
}
type startupRecordRows struct {
	cancel  context.CancelFunc
	records chan startupRecord
	done    chan struct{}
	current startupRecord
	err     error
}

func (r *startupRecordRows) send(ctx context.Context, record startupRecord) bool {
	select {
	case r.records <- record:
		return true
	case <-ctx.Done():
		return false
	}
}
func (r *startupRecordRows) Next() bool {
	if r.err != nil {
		return false
	}
	record, ok := <-r.records
	if !ok {
		return false
	}
	if record.err != nil {
		r.err = record.err
		return false
	}
	r.current = record
	return true
}
func (r *startupRecordRows) Scan(dest ...any) error {
	if len(dest) != 2 {
		return fmt.Errorf("invalid startup record destination")
	}
	key, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("invalid startup key destination")
	}
	data, ok := dest[1].(*[]byte)
	if !ok {
		return fmt.Errorf("invalid startup data destination")
	}
	*key, *data = r.current.key, r.current.data
	return nil
}
func (r *startupRecordRows) Err() error   { return r.err }
func (r *startupRecordRows) Close() error { r.cancel(); <-r.done; return nil }
