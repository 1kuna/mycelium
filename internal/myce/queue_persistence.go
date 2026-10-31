package myce

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// A JSON string deliberately fails closed in older binaries expecting a state
// object. All fragments and this marker commit in the same SQLite transaction.
var normalizedQueueMarker = []byte(`"myce-normalized-state-v1"`)

func readQueueState(db *sql.DB, data []byte) ([]byte, error) {
	if !bytes.Equal(data, normalizedQueueMarker) {
		return data, nil
	}
	rows, err := db.Query("SELECT key,data FROM queue_state")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var base map[string]json.RawMessage
	groups := map[string]map[string]json.RawMessage{"jobs": {}, "submissions": {}, "extensions": {}, "work_observations": {}, "storage_observations": {}}
	for rows.Next() {
		var key string
		var value []byte
		if err = rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		if key == "state" {
			if err = json.Unmarshal(value, &base); err != nil {
				return nil, err
			}
			continue
		}
		group, id, ok := strings.Cut(key, "/")
		if !ok || groups[group] == nil {
			return nil, fmt.Errorf("unknown queue state fragment")
		}
		groups[group][id] = value
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if base == nil {
		return nil, fmt.Errorf("queue state fragment missing")
	}
	for key, group := range groups {
		if len(group) == 0 && key != "jobs" {
			continue
		}
		base[key], err = json.Marshal(group)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(base)
}

// Immutable history stays in SQLite but is not rewritten by each heartbeat.
// Compare canonical JSON in memory; SQLite sees only changed logical records.
func (q *Queue) persistState(tx *sql.Tx) (map[string][]byte, error) {
	next := map[string][]byte{}
	put := func(key string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		next[key] = data
		if bytes.Equal(data, q.persisted[key]) {
			next[key] = q.persisted[key]
			return nil
		}
		_, err = tx.Exec("INSERT INTO queue_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", key, data)
		return err
	}
	if q.persisted == nil {
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)"); err != nil {
			return nil, err
		}
		if _, err := tx.Exec("DELETE FROM queue_state"); err != nil {
			return nil, err
		}
	}
	base := q.s
	base.Jobs = nil
	base.Submissions = nil
	base.Extensions = nil
	base.WorkObservations = nil
	base.StorageObservations = nil
	if err := put("state", base); err != nil {
		return nil, err
	}
	jobs := q.s.Jobs
	if q.persisted != nil {
		jobs = map[string]*Job{}
		for key := range q.records.jobs {
			jobs[key] = q.s.Jobs[key]
		}
	}
	for key, value := range jobs {
		if value == nil {
			next["jobs/"+key] = nil
			if _, err := tx.Exec("DELETE FROM queue_state WHERE key=?", "jobs/"+key); err != nil {
				return nil, err
			}
			continue
		}
		if err := put("jobs/"+key, value); err != nil {
			return nil, err
		}
	}
	submissions := q.s.Submissions
	if q.persisted != nil {
		submissions = map[string]*submissionRecord{}
		for key := range q.records.submissions {
			submissions[key] = q.s.Submissions[key]
		}
	}
	for key, value := range submissions {
		if err := put("submissions/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.Extensions {
		if err := put("extensions/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.WorkObservations {
		if err := put("work_observations/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.StorageObservations {
		if err := put("storage_observations/"+key, value); err != nil {
			return nil, err
		}
	}
	for key := range q.records.catalog {
		if _, ok := next[key]; !ok {
			next[key] = nil
			if _, err := tx.Exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
				return nil, err
			}
		}
	}
	if q.persisted == nil {
		if _, err := tx.Exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", normalizedQueueMarker); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func (q *Queue) loadPersistedState() error {
	var marker []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}
	if !bytes.Equal(marker, normalizedQueueMarker) {
		return nil
	}
	rows, err := q.db.Query("SELECT key,data FROM queue_state")
	if err != nil {
		return err
	}
	defer rows.Close()
	q.persisted = map[string][]byte{}
	for rows.Next() {
		var key string
		var data []byte
		if err = rows.Scan(&key, &data); err != nil {
			return err
		}
		q.persisted[key] = data
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	q.rebuildRecords()
	return nil
}
