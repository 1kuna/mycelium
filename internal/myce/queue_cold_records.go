package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
)

// Keep comparisons for large settled records without retaining a second
// serialized history. Split rows are compared independently so an update to
// small custody metadata need not rewrite the history payload.
type coldQueueRecord struct {
	size   int
	digest [sha256.Size]byte
	fields map[string][sha256.Size]byte
}

func (q *Queue) coldRecordCandidate(key string, data []byte) bool {
	if len(data) <= q.persistencePageBytes {
		return false
	}
	group, id, _ := strings.Cut(key, "/")
	switch group {
	case "jobs":
		j := q.s.Jobs[id]
		return j != nil && !mutableJob(j)
	case "submissions":
		r := q.s.Submissions[id]
		return r != nil && r.Job != nil && q.s.Jobs[r.JobID] == nil && !mutableJob(r.Job)
	}
	return false
}

func (q *Queue) compactColdRecord(key string, data []byte) bool {
	if !q.coldRecordCandidate(key, data) {
		return false
	}
	fields, err := persistenceFields(data, q.persistencePageBytes)
	if err != nil {
		return false // Keep the original comparison bytes if they cannot be split.
	}
	record := coldQueueRecord{size: len(data), digest: sha256.Sum256(data)}
	if len(fields) > 1 {
		record.fields = make(map[string][sha256.Size]byte, len(fields))
		for name, field := range fields {
			record.fields[name] = sha256.Sum256(field)
		}
	}
	if q.coldRecords == nil {
		q.coldRecords = make(map[string]coldQueueRecord)
	}
	q.coldRecords[key] = record
	delete(q.persisted, key)
	return true
}

// These inputs belong to a pinned publication. Copy mutable fields, borrowing
// immutable strings from the writer instead of decoding another serialized
// job. Ledger entries have their own paginated endpoint.
func (q *Queue) coldReadRecord(key string) *readRecord {
	freeze := func(j *Job) *Job {
		input := *j
		input.LedgerEntries = nil
		input.AdmissionWait = nil
		copy := cloneJob(&input)
		// The queue replaces admission evidence as a generation. It is safe
		// for pinned reads to share the same immutable diagnostic tree.
		copy.AdmissionWait = j.AdmissionWait
		return &copy
	}
	group, id, _ := strings.Cut(key, "/")
	r := &readRecord{}
	if group == "jobs" {
		job := freeze(q.s.Jobs[id])
		r.value = job
		r.history = statusHistoryEntry{source: job}
	} else {
		answer := *q.s.Submissions[id]
		answer.Job = freeze(answer.Job)
		r.value = &answer
	}
	r.decoded.Store(true)
	return r
}

// Evidence maintenance needs the original encoding only when reviewing a
// cold record under storage pressure. Its caller rechecks the writer revision
// before applying any derived changes.
func (q *Queue) readColdQueueRecord(ctx context.Context, key string) ([]byte, error) {
	rows, err := q.usageReader.QueryContext(ctx,
		"SELECT key,data FROM queue_state WHERE key=? OR key GLOB ?", key, fieldPrefix(key)+"*")
	if err != nil {
		return nil, err
	}
	records, err := readQueueRecordRows(rows, nil)
	return records[key], err
}

func (r coldQueueRecord) matches(data []byte) bool {
	return r.size == len(data) && r.digest == sha256.Sum256(data)
}

func (r coldQueueRecord) fieldMatches(key string, data json.RawMessage) bool {
	digest, ok := r.fields[key]
	return ok && digest == sha256.Sum256(data)
}
