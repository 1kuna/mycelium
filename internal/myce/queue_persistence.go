package myce

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// A JSON string deliberately fails closed in older binaries expecting a state
// object. All fragments and this marker commit in the same SQLite transaction.
var legacyNormalizedQueueMarker = []byte(`"myce-normalized-state-v1"`)
var normalizedQueueMarker = []byte(`"myce-normalized-state-v2"`)

func readQueueState(db *sql.DB, data []byte) ([]byte, error) {
	if !bytes.Equal(data, normalizedQueueMarker) && !bytes.Equal(data, legacyNormalizedQueueMarker) {
		return data, nil
	}
	records, err := readQueueRecords(db)
	if err != nil {
		return nil, err
	}
	var base map[string]json.RawMessage
	groups := map[string]map[string]json.RawMessage{"jobs": {}, "submissions": {}, "extensions": {}, "work_observations": {}, "storage_observations": {}, "external_memory": {}, "health_events": {}}
	for key, value := range records {
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
	if base == nil {
		return nil, fmt.Errorf("queue state fragment missing")
	}
	if events := groups["health_events"]; len(events) > 0 {
		var health map[string]json.RawMessage
		if err := json.Unmarshal(base["health_alerts"], &health); err != nil {
			return nil, err
		}
		ordered := make([]json.RawMessage, len(events))
		for key, value := range events {
			index, e := strconv.Atoi(key)
			if e != nil || index < 0 || index >= len(ordered) {
				return nil, fmt.Errorf("invalid health evidence index")
			}
			ordered[index] = value
		}
		health["events"], err = json.Marshal(ordered)
		if err != nil {
			return nil, err
		}
		base["health_alerts"], err = json.Marshal(health)
		if err != nil {
			return nil, err
		}
	}
	// Live keyed answers reference the same committed job instead of duplicating
	// its payload on every check-in. Removed jobs keep their full retained answer.
	for id, data := range groups["submissions"] {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		if record == nil || len(record["job"]) > 0 {
			continue
		}
		var jobID string
		if err := json.Unmarshal(record["job_id"], &jobID); err != nil {
			continue
		}
		if job := groups["jobs"][jobID]; job != nil {
			record["job"] = job
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, err
			}
			groups["submissions"][id] = encoded
		}
	}
	delete(groups, "health_events")
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
func (q *Queue) persistState(tx *sql.Tx, measured *queuePersistenceTiming) (map[string][]byte, error) {
	if q.persistencePageBytes <= 0 {
		return nil, fmt.Errorf("queue persistence page size is not initialized")
	}
	next := map[string][]byte{}
	exec := func(query string, args ...any) (sql.Result, error) {
		started := holdNow(q.holdClock)
		result, err := tx.Exec(query, args...)
		measured.SQL += holdNow(q.holdClock).Sub(started)
		return result, err
	}
	putData := func(key string, data []byte, fields ...map[string]json.RawMessage) error {
		next[key] = data
		if bytes.Equal(data, q.persisted[key]) {
			next[key] = q.persisted[key]
			return nil
		}
		if splitPersistenceKey(key) && !q.fragmented {
			return nil
		}
		// Ordinary records fit one page and retain the original compact row.
		// No fragment preparation or extra B-tree keys are needed for them.
		compact := len(data) <= q.persistencePageBytes && len(q.persisted[key]) <= q.persistencePageBytes
		if splitPersistenceKey(key) && q.fragmented && !compact {
			var prepared map[string]json.RawMessage
			if len(fields) > 0 {
				prepared = fields[0]
			}
			return q.writeFields(key, q.persisted[key], data, measured, exec, prepared)
		}
		measured.Bytes += int64(len(data))
		measured.Records++
		_, err := exec("INSERT INTO queue_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", key, data)
		return err
	}
	put := func(key string, value any) error {
		started := holdNow(q.holdClock)
		data, fields, err := q.marshalPersistenceValue(key, value)
		measured.Marshal += holdNow(q.holdClock).Sub(started)
		if err != nil {
			return err
		}
		return putData(key, data, fields)
	}
	putCatalog := func(key string, value any) error {
		started := holdNow(q.holdClock)
		entry, err := q.encodeCatalog(key, value)
		measured.Marshal += holdNow(q.holdClock).Sub(started)
		if err != nil {
			return err
		}
		return putData(key, entry.data)
	}
	if q.persisted == nil {
		if _, err := exec("CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)"); err != nil {
			return nil, err
		}
		if _, err := exec("DELETE FROM queue_state"); err != nil {
			return nil, err
		}
	}
	base := q.s
	base.Jobs = nil
	base.Submissions = nil
	base.Extensions = nil
	base.WorkObservations = nil
	base.StorageObservations = nil
	base.ExternalMemory = nil
	base.Health.Events = nil
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
			if err := deleteFields("jobs/"+key, exec); err != nil {
				return nil, err
			}
			if _, err := exec("DELETE FROM queue_state WHERE key=?", "jobs/"+key); err != nil {
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
		if value != nil && value.Job != nil && q.s.Jobs[value.JobID] == value.Job {
			copy := *value
			copy.Job = nil
			value = &copy
		}
		if err := put("submissions/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.Extensions {
		if err := putCatalog("extensions/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.WorkObservations {
		if err := putCatalog("work_observations/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.StorageObservations {
		if err := putCatalog("storage_observations/"+key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range q.s.ExternalMemory {
		if err := putCatalog("external_memory/"+key, value); err != nil {
			return nil, err
		}
	}
	for index := q.records.healthCount; index < len(q.s.Health.Events); index++ {
		if err := put("health_events/"+strconv.Itoa(index), q.s.Health.Events[index]); err != nil {
			return nil, err
		}
	}
	for index := range q.records.healthChanged {
		if err := put("health_events/"+strconv.Itoa(index), q.s.Health.Events[index]); err != nil {
			return nil, err
		}
	}
	for key := range q.records.catalog {
		if _, ok := next[key]; !ok {
			next[key] = nil
			if _, err := exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
				return nil, err
			}
		}
	}
	if !q.fragmented {
		// Convert every old logical record atomically with the new format marker.
		// Unchanged cold jobs must migrate too, without touching scheduling state.
		migration := make(map[string][]byte, len(q.persisted)+len(next))
		for key, data := range q.persisted {
			migration[key] = data
		}
		for key, data := range next {
			migration[key] = data
		}
		for key, data := range migration {
			if !splitPersistenceKey(key) {
				continue
			}
			if data != nil {
				if err := q.writeFields(key, nil, data, measured, exec, nil); err != nil {
					return nil, err
				}
			}
		}
	}
	if !q.fragmented {
		if _, err := exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", normalizedQueueMarker); err != nil {
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
	if !bytes.Equal(marker, normalizedQueueMarker) && !bytes.Equal(marker, legacyNormalizedQueueMarker) {
		return nil
	}
	records, err := readQueueRecords(q.db)
	if err != nil {
		return err
	}
	q.persisted = records
	q.fragmented = bytes.Equal(marker, normalizedQueueMarker)
	q.evidenceBytes = 0
	for key, data := range q.persisted {
		q.noteEvidenceRecord(key, nil, data)
	}
	q.rebuildRecords()
	return nil
}

// Fields share the existing SQLite transaction, FULL sync, and attempt fences.
// JSON paths are encoded rather than delimited: map keys may contain slashes.
func splitPersistenceKey(key string) bool {
	return key == "state" || strings.HasPrefix(key, "jobs/") || strings.HasPrefix(key, "submissions/")
}
func fieldPrefix(key string) string {
	return "fields/" + base64.RawURLEncoding.EncodeToString([]byte(key)) + "/"
}
func persistenceFields(data []byte, pageBytes int) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	return splitPersistenceFields(object, pageBytes)
}
func splitPersistenceFields(object map[string]json.RawMessage, pageBytes int) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if object == nil {
		out[""] = json.RawMessage(`null`)
		return out, nil
	}
	// A separate row for every tiny field increases B-tree page dirties. Split
	// only payloads larger than this database's physical page; keep small control
	// fields together. This is storage granularity, never an admission limit.
	for field, value := range object {
		if len(value) <= pageBytes {
			continue
		}
		out[base64.RawURLEncoding.EncodeToString([]byte(field))] = value
		delete(object, field)
	}
	root, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	out[""] = root
	return out, nil
}

type persistenceExec func(string, ...any) (sql.Result, error)

func deleteFields(key string, exec persistenceExec) error {
	// Base64url contains no GLOB metacharacters.
	_, err := exec("DELETE FROM queue_state WHERE key GLOB ?", fieldPrefix(key)+"*")
	return err
}
func (q *Queue) writeFields(key string, old, data []byte, measured *queuePersistenceTiming, exec persistenceExec, next map[string]json.RawMessage) error {
	started := holdNow(q.holdClock)
	var err error
	if next == nil {
		next, err = persistenceFields(data, q.persistencePageBytes)
	}
	if err != nil {
		return err
	}
	var before map[string]json.RawMessage
	if old != nil {
		cached := q.persistenceFieldsCache[key]
		if len(cached.data) > 0 && &cached.data[0] == &old[0] {
			before = cached.fields
		} else {
			before, err = persistenceFields(old, q.persistencePageBytes)
		}
	}
	if q.persistenceFieldsCache == nil {
		q.persistenceFieldsCache = map[string]persistenceFieldCache{}
	}
	q.persistenceFieldsCache[key] = persistenceFieldCache{data: data, fields: next}
	measured.Marshal += holdNow(q.holdClock).Sub(started)
	if err != nil {
		return err
	}
	if len(next) == 1 {
		// There is no multi-page field to isolate. Keep the ordinary logical row,
		// including when a formerly large field shrinks or is removed.
		if err := deleteFields(key, exec); err != nil {
			return err
		}
		measured.Bytes += int64(len(data))
		measured.Records++
		_, err := exec("INSERT INTO queue_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", key, data)
		return err
	}
	// A one-piece old encoding lived in the ordinary logical row, so none of
	// these field rows exists yet. Publish the root along with its large fields.
	if len(before) == 1 {
		before = nil
	}
	if _, err := exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
		return err
	}
	prefix := fieldPrefix(key)
	for field, value := range next {
		if bytes.Equal(before[field], value) {
			continue
		}
		measured.Bytes += int64(len(value))
		measured.Records++
		if _, err := exec("INSERT INTO queue_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", prefix+field, value); err != nil {
			return err
		}
	}
	for field := range before {
		if _, ok := next[field]; !ok {
			measured.Records++
			if _, err := exec("DELETE FROM queue_state WHERE key=?", prefix+field); err != nil {
				return err
			}
		}
	}
	return nil
}

// Reassemble logical records for startup/recovery, outside ordinary writes.
func readQueueRecords(db *sql.DB, only ...string) (map[string][]byte, error) {
	query := "SELECT key,data FROM queue_state"
	var args []any
	if len(only) > 0 {
		query += " WHERE key=? OR key GLOB ?"
		args = []any{only[0], fieldPrefix(only[0]) + "*"}
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := map[string][]byte{}
	fields := map[string]map[string]json.RawMessage{}
	roots := map[string][]byte{}
	for rows.Next() {
		var key string
		var data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return nil, err
		}
		encoded, ok := strings.CutPrefix(key, "fields/")
		if !ok {
			records[key] = data
			continue
		}
		record, field, ok := strings.Cut(encoded, "/")
		if !ok {
			return nil, fmt.Errorf("invalid queue field key")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(record)
		if err != nil {
			return nil, fmt.Errorf("invalid queue record key")
		}
		key = string(decoded)
		if !splitPersistenceKey(key) {
			return nil, fmt.Errorf("unknown queue field record")
		}
		if fields[key] == nil {
			fields[key] = map[string]json.RawMessage{}
		}
		if field == "" {
			roots[key] = data
			continue
		}
		decoded, err = base64.RawURLEncoding.DecodeString(field)
		if err != nil {
			return nil, fmt.Errorf("invalid queue field name")
		}
		fields[key][string(decoded)] = data
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for key, members := range fields {
		root, ok := roots[key]
		if !ok {
			return nil, fmt.Errorf("queue field root missing")
		}
		if records[key] != nil {
			return nil, fmt.Errorf("mixed queue record formats")
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(root, &object); err != nil {
			return nil, err
		}
		if object == nil {
			if len(members) > 0 {
				return nil, fmt.Errorf("null queue record has fields")
			}
			records[key] = root
			continue
		}
		for field, value := range members {
			if _, exists := object[field]; exists {
				return nil, fmt.Errorf("duplicate queue field")
			}
			object[field] = value
		}
		data, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		records[key] = data
	}
	return records, nil
}

type persistenceFieldCache struct {
	data   []byte
	fields map[string]json.RawMessage
}

type waitPersistenceInput struct {
	evidence *AdmissionWaitEvidence
	base     []byte
}

// Cache detached multi-page fields and immutable admission evidence. Ordinary
// mutable fields compare against owned values, never dirty tracking. Immutable
// evidence can compare by identity. Speculative encodings confer no durability:
// writeFields always compares against the last committed bytes.
func (q *Queue) marshalPersistenceValue(key string, value any) ([]byte, map[string]json.RawMessage, error) {
	if j, ok := value.(*Job); ok && j != nil && j.AdmissionWait != nil {
		// Immutable wait history must not turn every small live job into a
		// full per-field reflection/cache walk on every heartbeat. Encode the
		// ordinary job at its own size and reuse the immutable evidence bytes.
		if q.persistenceEncodings == nil {
			q.persistenceEncodings = map[string]map[string]catalogEncoding{}
		}
		if q.persistenceEncodings[key] == nil {
			q.persistenceEncodings[key] = map[string]catalogEncoding{}
		}
		cache := q.persistenceEncodings[key]
		entry := cache["admission_wait"]
		if entry.value != j.AdmissionWait {
			raw, err := json.Marshal(j.AdmissionWait)
			if err != nil {
				return nil, nil, err
			}
			entry = catalogEncoding{value: j.AdmissionWait, data: raw}
			cache["admission_wait"] = entry
		}
		input := *j
		input.AdmissionWait = nil
		// These entries describe the excluded immutable evidence, not fields
		// in input. Keep them outside the ordinary field-cache cleanup below.
		whole := cache["$admission_wait_job"]
		rest, _, err := q.marshalPersistenceSized(key, &input, max(0, len(q.persisted[key])-len(entry.data)))
		cache["admission_wait"] = entry
		if whole.value != nil {
			cache["$admission_wait_job"] = whole
		}
		if err != nil {
			return nil, nil, err
		}
		if old, ok := whole.value.(waitPersistenceInput); ok && old.evidence == j.AdmissionWait && bytes.Equal(old.base, rest) {
			return whole.data, nil, nil
		}
		data := make([]byte, 0, len(rest)+len(entry.data)+18)
		data = append(data, `{"admission_wait":`...)
		data = append(data, entry.data...)
		if len(rest) > 2 {
			data = append(data, ',')
		}
		data = append(data, rest[1:]...)
		cache["$admission_wait_job"] = catalogEncoding{value: waitPersistenceInput{j.AdmissionWait, rest}, data: data}
		return data, nil, nil
	}
	return q.marshalPersistenceSized(key, value, len(q.persisted[key]))
}

func (q *Queue) marshalPersistenceSized(key string, value any, previousBytes int) ([]byte, map[string]json.RawMessage, error) {
	if !splitPersistenceKey(key) || previousBytes <= q.persistencePageBytes {
		data, err := json.Marshal(value)
		return data, nil, err
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return []byte("null"), nil, nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		data, err := json.Marshal(value)
		return data, nil, err
	}
	if q.persistenceEncodings == nil {
		q.persistenceEncodings = map[string]map[string]catalogEncoding{}
	}
	cache := q.persistenceEncodings[key]
	if cache == nil {
		cache = map[string]catalogEncoding{}
		q.persistenceEncodings[key] = cache
	}
	fields := map[string]json.RawMessage{}
	data := []byte{'{'}
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := typ.Field(i)
		_, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		if field.Anonymous || options != "" && options != "omitempty" {
			data, err := json.Marshal(value)
			return data, nil, err
		}
	}
	for i := 0; i < v.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		member := v.Field(i)
		empty := false
		switch member.Kind() {
		case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
			empty = member.Len() == 0
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
			empty = member.IsZero()
		}
		if options == "omitempty" && empty {
			continue
		}
		owned, ok := cache[name]
		var raw []byte
		if ok && reflect.DeepEqual(member.Interface(), owned.value) {
			raw = owned.data
		} else {
			var err error
			raw, err = json.Marshal(member.Interface())
			if err != nil {
				return nil, nil, err
			}
			if len(raw) > q.persistencePageBytes {
				cache[name] = catalogEncoding{value: copyObservationValue(member).Interface(), data: raw}
			} else {
				delete(cache, name)
			}
		}
		fields[name] = raw
		if len(data) > 1 {
			data = append(data, ',')
		}
		encodedName, _ := json.Marshal(name)
		data = append(data, encodedName...)
		data = append(data, ':')
		data = append(data, raw...)
	}
	for name := range cache {
		if _, ok := fields[name]; !ok {
			delete(cache, name)
		}
	}
	data = append(data, '}')
	split, err := splitPersistenceFields(fields, q.persistencePageBytes)
	return data, split, err
}
