package myce

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Legacy retained observations contain maxima, not the individual samples
// that produced them. Project each row once. Component availability, driver
// identity and resource policy are irrelevant to the new evidence key.
func projectMemoryEvidence(points map[string]WorkObservation) []memoryEvidencePoint {
	var out []memoryEvidencePoint
	for _, p := range points {
		key, ok := legacyMemoryEvidenceKey(p)
		if !ok || p.MemoryPeak == nil {
			continue
		}
		limited := legacyWorkloadLimited(p.MemoryPeak)
		if limited || !p.MemoryRecordedAt.IsZero() && !p.MemoryPeak.constrained() {
			out = appendProjectedMemoryPeaks(out, key, memoryEvidencePoint{At: p.MemoryRecordedAt, Limited: limited}, p.MemoryPeak)
		}
	}
	slices.SortFunc(out, func(a, b memoryEvidencePoint) int {
		return cmp.Or(a.At.Compare(b.At), cmp.Compare(a.Key.Platform, b.Key.Platform), cmp.Compare(a.Key.Pool, b.Key.Pool), cmp.Compare(a.Key.Family, b.Key.Family), cmp.Compare(a.Key.Exact, b.Key.Exact), cmp.Compare(a.Peak, b.Peak))
	})
	return out
}

func legacyMemoryEvidenceKey(p WorkObservation) (memoryEvidenceKey, bool) {
	if p.MemoryFamily == "" || p.MemoryConfiguration == "" || p.Key.Target.OS == "" || p.Key.Target.Platform == "" {
		return memoryEvidenceKey{}, false
	}
	return memoryEvidenceKey{
		Platform: memoryPlatform(p.Key.Target, p.MemoryLayout == "unified"),
		Family:   p.MemoryFamily,
		Exact:    memoryEvidenceExact(p.Key, p.MemoryConfiguration),
	}, true
}

func legacyWorkloadLimited(p *AttemptMemoryPeak) bool {
	return pooledSampleLimited(p) || p.Events != nil && p.Events.High > 0
}

func memoryEvidenceExact(k ObservationKey, configuration string) string {
	return identityHash(struct {
		Work, Extension, Configuration string
		Reference                      WorkReference
		Concurrency, BatchSize         int
		Check                          bool
	}{k.Work, k.ExtensionDigest, configuration, k.Reference, k.Concurrency, k.BatchSize, k.Check})
}

func memoryPlatform(target ExecutionTarget, unified bool) string {
	layout := "discrete"
	if unified {
		layout = "unified"
	}
	if target.Platform == "cpu" {
		layout = "host"
	}
	return target.OS + "/" + target.Platform + "/" + layout
}

func appendProjectedMemoryPeaks(out []memoryEvidencePoint, key memoryEvidenceKey, p memoryEvidencePoint, peak *AttemptMemoryPeak) []memoryEvidencePoint {
	key.Pool = "ram"
	p.Peak = memoryValue(peak.HostGB)
	if strings.HasSuffix(key.Platform, "/unified") && peak.DeviceSource != ownedFootprintSource {
		p.Peak += memoryValue(peak.DeviceGB)
	}
	p.Peak = max(p.Peak, memoryValue(peak.CombinedHostGB))
	if validRate(p.Peak) {
		p.Key = key
		out = append(out, p)
	}
	if strings.HasSuffix(key.Platform, "/discrete") {
		key.Pool, p.Peak = "card", memoryValue(peak.DeviceGB)
		if validRate(p.Peak) {
			p.Key = key
			out = append(out, p)
		}
	}
	return out
}

func memoryValue(v *float64) float64 {
	if v == nil || !validMemoryValue(*v) {
		return 0
	}
	return *v
}

// Keep evidence outside queue_state's closed record namespace. The previous
// release can open and update the same queue without dropping these rows.
const memoryEvidenceSchema = `CREATE TABLE IF NOT EXISTS memory_evidence (
	kind TEXT NOT NULL, key TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(kind,key))`

// The predecessor rejects unknown memory_evidence kinds. Keep throughput rows
// in their own table so that version can still open and update this queue.
const gpuSharingSchema = `CREATE TABLE IF NOT EXISTS gpu_sharing_evidence (
	key TEXT PRIMARY KEY, data BLOB NOT NULL)`

type memoryEvidenceRowKey struct {
	kind  string
	key   memoryEvidenceKey
	ratio memoryRatioKey
}

func (e *memoryEvidence) write(key memoryEvidenceRowKey, value any) {
	if e.writes != nil {
		e.writes[key] = value
	}
}

func (e *memoryEvidence) prepareWrite(batch *queueWriteBatch) error {
	if e == nil {
		return nil
	}
	for row, value := range e.writes {
		var key any = row.key
		if row.kind == "ratio" {
			key = row.ratio
		}
		encodedKey, err := json.Marshal(key)
		if err != nil {
			return err
		}
		encodedValue, err := json.Marshal(value)
		if err != nil {
			return err
		}
		batch.add(`INSERT INTO memory_evidence(kind,key,data) VALUES(?,?,?)
			ON CONFLICT(kind,key) DO UPDATE SET data=excluded.data`, row.kind, string(encodedKey), encodedValue)
	}
	for key, history := range e.gpuSharingWrites {
		encodedKey, err := json.Marshal(key)
		if err != nil {
			return err
		}
		data, err := json.Marshal(history)
		if err != nil {
			return err
		}
		batch.add(`INSERT INTO gpu_sharing_evidence(key,data) VALUES(?,?)
			ON CONFLICT(key) DO UPDATE SET data=excluded.data`, string(encodedKey), data)
	}
	return nil
}

// Load or project before the queue begins serving requests. The projection
// marker commits with its rows, so an interrupted upgrade can safely retry.
func loadMemoryEvidence(db *sql.DB, s state) (*memoryEvidence, error) {
	return (queueSQLiteWriter{db: db}).loadMemoryEvidence(s)
}

func (w queueSQLiteWriter) loadMemoryEvidence(s state) (*memoryEvidence, error) {
	db := w.db
	if _, err := db.Exec(memoryEvidenceSchema); err != nil {
		return nil, err
	}
	if _, err := db.Exec(gpuSharingSchema); err != nil {
		return nil, err
	}
	evidence, err := readMemoryEvidence(db)
	if err == nil {
		if err := evidence.readGPUSharing(db); err != nil {
			return nil, err
		}
		if w.shared != nil {
			return w.repairMemoryEvidence(evidence, s)
		}
		return repairMemoryEvidence(db, evidence, s)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	evidence = newMemoryEvidence().stage()
	for _, point := range projectMemoryEvidence(s.WorkObservations) {
		evidence.observe(point)
	}
	var batch queueWriteBatch
	if err := evidence.prepareWrite(&batch); err != nil {
		return nil, err
	}
	batch.add(`INSERT INTO memory_evidence(kind,key,data) VALUES ('projection','v1','true'), ('projection','v2','true'), ('projection','v3','true')`)
	if err := w.apply(context.Background(), &batch, nil, nil); err != nil {
		return nil, err
	}
	evidence.writes = nil
	return evidence, nil
}

func (e *memoryEvidence) readGPUSharing(db *sql.DB) error {
	rows, err := db.Query(`SELECT key,data FROM gpu_sharing_evidence`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key, data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return err
		}
		var id gpuSharingKey
		if err := json.Unmarshal(key, &id); err != nil {
			return err
		}
		if err := decodeQueueRecord(e.gpuSharing, id, data); err != nil {
			return err
		}
	}
	return rows.Err()
}

func readMemoryEvidence(db *sql.DB) (*memoryEvidence, error) {
	var projected bool
	if err := db.QueryRow(`SELECT 1 FROM memory_evidence WHERE kind='projection' AND key='v1'`).Scan(&projected); err != nil {
		return nil, err
	}
	e := newMemoryEvidence()
	rows, err := db.Query(`SELECT kind,key,data FROM memory_evidence WHERE kind!='projection'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, key string
		var data []byte
		if err := rows.Scan(&kind, &key, &data); err != nil {
			return nil, err
		}
		if err := e.readMemoryEvidenceRow(kind, key, data); err != nil {
			return nil, fmt.Errorf("decode memory evidence %s: %w", kind, err)
		}
	}
	return e, rows.Err()
}

func (e *memoryEvidence) readMemoryEvidenceRow(kind, key string, data []byte) error {
	if kind == "feedback" {
		return json.Unmarshal(data, &e.feedback)
	}
	if kind == "ratio" {
		var id memoryRatioKey
		if err := json.Unmarshal([]byte(key), &id); err != nil {
			return err
		}
		return decodeQueueRecord(e.ratios, id, data)
	}
	var id memoryEvidenceKey
	if err := json.Unmarshal([]byte(key), &id); err != nil {
		return err
	}
	switch kind {
	case "exact":
		return decodeQueueRecord(e.exact, id, data)
	case "family":
		if err := decodeQueueRecord(e.family, id, data); err != nil {
			return err
		}
		work := id.workKey()
		if e.foreign[work] == nil {
			e.foreign[work] = map[string]bool{}
		}
		e.foreign[work][id.Platform] = true
		return nil
	case "first":
		return decodeQueueRecord(e.first, id, data)
	default:
		return errors.New("unknown memory evidence row")
	}
}
