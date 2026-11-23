package myce

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Keep evidence outside queue_state's closed record namespace. The previous
// release can open and update the same queue without dropping these rows.
const memoryEvidenceSchema = `CREATE TABLE IF NOT EXISTS memory_evidence (
	kind TEXT NOT NULL, key TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(kind,key))`

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

func (e *memoryEvidence) persist(tx *sql.Tx) error {
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
		if _, err := tx.Exec(`INSERT INTO memory_evidence(kind,key,data) VALUES(?,?,?)
			ON CONFLICT(kind,key) DO UPDATE SET data=excluded.data`, row.kind, string(encodedKey), encodedValue); err != nil {
			return err
		}
	}
	return nil
}

// Load or project before the queue begins serving requests. The projection
// marker commits with its rows, so an interrupted upgrade can safely retry.
func loadMemoryEvidence(db *sql.DB, legacy map[string]WorkObservation) (*memoryEvidence, error) {
	if _, err := db.Exec(memoryEvidenceSchema); err != nil {
		return nil, err
	}
	evidence, err := readMemoryEvidence(db)
	if !errors.Is(err, sql.ErrNoRows) {
		return evidence, err
	}
	evidence = newMemoryEvidence().stage()
	for _, point := range projectMemoryEvidence(legacy) {
		evidence.observe(point)
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := evidence.persist(tx); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO memory_evidence(kind,key,data) VALUES ('projection','v1','true')`); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	evidence.writes = nil
	return evidence, nil
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
