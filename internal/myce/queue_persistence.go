package myce

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// A JSON string deliberately fails closed in older binaries expecting a state
// object. All fragments and this marker commit in the same SQLite transaction.
// r6.10 and later write this per-machine layout; MY-42 retired every older one.
var normalizedQueueMarker = []byte(`"myce-normalized-state-v3"`)

type queueRecordReader interface {
	Query(string, ...any) (*sql.Rows, error)
}

func readQueueState(db queueRecordReader, data []byte) ([]byte, error) {
	if !bytes.Equal(data, normalizedQueueMarker) {
		return nil, fmt.Errorf("unsupported queue persistence format; current and previous releases require normalized-state-v3")
	}
	records, err := readQueueRecords(db)
	if err != nil {
		return nil, err
	}
	var base map[string]json.RawMessage
	groups := map[string]map[string]json.RawMessage{"jobs": {}, "submissions": {}, "extensions": {}, "work_observations": {}, "storage_observations": {}, "external_memory": {}, "health_events": {}, "machines": {}}
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
	for id, data := range groups["jobs"] {
		if groups["jobs"][id], err = expandJobRecord(data); err != nil {
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
		if len(group) == 0 && key != "jobs" && key != "machines" {
			continue
		}
		base[key], err = json.Marshal(group)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(base)
}

// Logical records and prepared root fragments advance together only after a
// successful SQLite commit. A failed write never replaces the comparison base.
type queuePersistenceUpdate struct {
	records     map[string][]byte
	stateFields map[string]json.RawMessage
}

// Immutable history stays in SQLite but is not rewritten by each heartbeat.
// Encode only journaled decisions; SQLite sees only changed logical records.
func (q *Queue) persistState(tx *sql.Tx, measured *queuePersistenceTiming) (*queuePersistenceUpdate, error) {
	if q.persistencePageBytes <= 0 {
		return nil, fmt.Errorf("queue persistence page size is not initialized")
	}
	update := &queuePersistenceUpdate{records: map[string][]byte{}}
	next := update.records
	exec := func(query string, args ...any) (sql.Result, error) {
		started := holdNow(q.holdClock)
		result, err := tx.Exec(query, args...)
		measured.SQL += holdNow(q.holdClock).Sub(started)
		return result, err
	}
	putData := func(key string, data []byte, fields ...map[string]json.RawMessage) error {
		if key == "state" && len(fields) > 0 {
			update.stateFields = fields[0]
		}
		next[key] = data
		if bytes.Equal(data, q.persisted[key]) {
			next[key] = q.persisted[key]
			return nil
		}
		// Ordinary records fit one page and retain the original compact row.
		// No fragment preparation or extra B-tree keys are needed for them.
		compact := len(data) <= q.persistencePageBytes && len(q.persisted[key]) <= q.persistencePageBytes
		if splitPersistenceKey(key) && !compact {
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

	if q.persisted == nil {
		if _, err := exec("CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)"); err != nil {
			return nil, err
		}
		if _, err := exec("DELETE FROM queue_state"); err != nil {
			return nil, err
		}
	}
	base := q.persistentBase()
	if err := put("state", base); err != nil {
		return nil, err
	}
	machines := q.s.Machines
	if q.persisted != nil {
		machines = map[string]*Machine{}
		for id := range q.records.machines {
			machines[id] = q.s.Machines[id]
		}
	}
	for id, m := range machines {
		key := "machines/" + id
		if m == nil {
			next[key] = nil
			if _, err := exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
				return nil, err
			}
		} else if err := put(key, durableMachine(m)); err != nil {
			return nil, err
		}
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
	catalogs := map[string]bool{}
	for key := range q.records.catalogUndo {
		catalogs[key] = true
	}
	if q.persisted == nil {
		for id := range q.s.Extensions {
			catalogs["extensions/"+id] = true
		}
		for id := range q.s.WorkObservations {
			catalogs["work_observations/"+id] = true
		}
		for id := range q.s.StorageObservations {
			catalogs["storage_observations/"+id] = true
		}
		for id := range q.s.ExternalMemory {
			catalogs["external_memory/"+id] = true
		}
	}
	for key := range catalogs {
		value, exists := q.catalogValue(key)
		if !exists {
			next[key] = nil
			if _, err := exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
				return nil, err
			}
			continue
		}
		if point, ok := value.(WorkObservation); ok {
			value = durableWorkObservation(point)
		}
		if err := put(key, value); err != nil {
			return nil, err
		}
	}

	for index := q.records.healthCount; index < len(q.s.Health.Events); index++ {
		if err := put("health_events/"+strconv.Itoa(index), q.s.Health.Events[index]); err != nil {
			return nil, err
		}
	}
	for index := range q.records.healthChanged {
		if index >= len(q.s.Health.Events) {
			continue
		}
		if err := put("health_events/"+strconv.Itoa(index), q.s.Health.Events[index]); err != nil {
			return nil, err
		}
	}
	// One record per open problem: migration and retention shrink the dense rows.
	for index := len(q.s.Health.Events); ; index++ {
		key := "health_events/" + strconv.Itoa(index)
		if _, saved := q.persisted[key]; !saved && index >= q.records.healthCount {
			break
		}
		next[key] = nil
		if _, err := exec("DELETE FROM queue_state WHERE key=?", key); err != nil {
			return nil, err
		}
	}
	// A fresh database publishes its first full state and marker atomically.
	if q.persisted == nil {
		if _, err := exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", normalizedQueueMarker); err != nil {
			return nil, err
		}
	}
	return update, nil
}

func (q *Queue) loadPersistedState() error {
	var marker []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}
	if !bytes.Equal(marker, normalizedQueueMarker) {
		return fmt.Errorf("unsupported queue persistence format")
	}
	records, err := readQueueRecords(q.db)
	if err != nil {
		return err
	}
	q.persisted = records
	q.persistedStateFields, err = persistenceFields(records["state"], q.persistencePageBytes)
	if err != nil {
		return err
	}
	q.efficiencyHealthJSON, err = json.Marshal(q.s.EfficiencyHealth)
	if err != nil {
		return err
	}
	q.evidenceBytes = 0
	for key, data := range q.persisted {
		q.noteEvidenceRecord(key, nil, data)
	}
	q.restoreLastSeen()
	q.rebuildRecords()
	q.histories = cloneHistories(q.s.EfficiencyHosts, q.s.HostLaunchMemory)
	return q.loadExpiredLearning()
}

// A machine record holds what must survive a restart: identity, enrolment,
// settings, the session fence and queue decisions carried between check-ins.
// Each check-in replaces the fields cleared here, so they stay in memory.
// Restart marks every machine unreported, which keeps placement off it until
// its next check-in rebuilds them. A steady heartbeat then rewrites nothing.
func durableMachine(m *Machine) *Machine {
	d := *m
	d.LastSeen = time.Time{} // Contact evidence keeps it (restoreLastSeen).
	d.DiskReceivedAt, d.DiskVolumes = time.Time{}, nil
	if d.InventoryProbe != nil {
		d.InventoryProbe = &InventoryObservation{State: "stale"}
	}
	if d.HostHealth != nil && d.HostHealth.Probe != nil {
		// Keep only that this worker sends timed evidence. The empty probe
		// is invalid, so a restored record reads as expired evidence (unknown
		// until its report, alerting past restart grace), not as a legacy
		// worker without probes. It is constant, so heartbeats leave it alone.
		d.HostHealth = &HostHealth{Probe: &ProbeObservation{}}
	} else {
		d.HostHealth = nil
	}
	d.IOPressure, d.SharedHostPressure, d.MemoryObservation, d.Fleet = nil, nil, nil, nil
	d.HostAvailableMemoryGB, d.FreeMemoryGB, d.MemoryUsedGB, d.CacheFreeBytes = 0, 0, 0, 0
	d.SharedInputEvidence, d.SharedInputCache, d.CopyBytesPerSecond = nil, nil, 0
	if d.CopyReports != nil {
		// Failed copies stay durable for staging alerts; progress does not.
		d.CopyReports = slices.Clone(d.CopyReports)
		for i := range d.CopyReports {
			d.CopyReports[i].Bytes, d.CopyReports[i].BytesPerSecond = 0, 0
		}
	}
	if d.EnvironmentReports != nil {
		// Workers list these from a map; store one order so it is not a change.
		d.EnvironmentReports = slices.Clone(d.EnvironmentReports)
		slices.SortStableFunc(d.EnvironmentReports, func(a, b EnvironmentReport) int { return strings.Compare(a.Key, b.Key) })
	}
	// Recomputed for every machine by refreshHostMemory and status.
	d.HostMemoryDerivation, d.HostMemoryPolicyMismatch, d.HostMemoryAllowanceBytes, d.HostMemoryQualification = nil, false, 0, ""
	d.EffectiveHostMemoryBudgetGB, d.HostMemoryInUseGB, d.FreeHostMemoryGB, d.HostMemory = 0, 0, 0, nil
	d.EffectiveCPUCores, d.CPUInUseCores, d.FreeCPUCores, d.CPUCapacityReason = 0, 0, 0, ""
	d.SharedHostReason, d.ToolInventory, d.ExternalMemory = "", nil, nil
	d.CapabilityProfile = withoutAvailability(d.CapabilityProfile)
	if d.Devices != nil {
		d.Devices = slices.Clone(d.Devices)
		for i := range d.Devices {
			device := &d.Devices[i]
			device.Telemetry, device.FreeMemoryGB = nil, 0
			device.CapabilityProfile = withoutAvailability(device.CapabilityProfile)
		}
	}
	return &d
}

// Owner signals carry their own receipt time; a stored copy would read as
// stale after restart and withdraw running work before the worker reports.
func withoutAvailability(p *CapabilityProfile) *CapabilityProfile {
	if p == nil || p.Availability == nil {
		return p
	}
	copy := *p
	copy.Availability = nil
	return &copy
}

// LastSeen rides in each session's contact evidence, which every check-in
// already rewrites with the small control record. Keep the later of the two.
func (q *Queue) restoreLastSeen() {
	for id, m := range q.s.Machines {
		e, ok := q.s.ContactEvidence[id]
		if ok && e.Session == m.Session && e.Seen.After(m.LastSeen) {
			m.LastSeen = e.Seen
		}
		if !m.LastSeen.IsZero() && (!ok || e.Session == m.Session && e.Seen.IsZero()) {
			// Records written before contact evidence held LastSeen carry it.
			if q.s.ContactEvidence == nil {
				q.s.ContactEvidence = map[string]contactEvidence{}
			}
			e.Session, e.Seen = m.Session, m.LastSeen
			q.s.ContactEvidence[id] = e
		}
	}
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
		if key == "state" && q.persistedStateFields != nil {
			before = q.persistedStateFields
		} else {
			before, err = persistenceFields(old, q.persistencePageBytes)
		}
	}

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
func readQueueRecords(db queueRecordReader, only ...string) (map[string][]byte, error) {
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

// Dirty records are encoded exactly once. No whole-state comparison caches.
func (q *Queue) marshalPersistenceValue(key string, value any) ([]byte, map[string]json.RawMessage, error) {
	if s, ok := value.(state); key == "state" && ok {
		return q.marshalQueueBase(s, true)
	}
	if j, ok := value.(*Job); ok && j != nil && (q.startupPending(j) || q.startCapacityPaused(j)) {
		copy := *j
		copy.Reason = "recomputing admission diagnostics"
		value = &copy
	}
	data, err := json.Marshal(value)
	if j, ok := value.(*Job); ok && err == nil && q.slimJobRecords && strings.HasPrefix(key, "jobs/") && !mutableJob(j) {
		data = slimJobRecord(data)
	}
	return data, nil, err
}

// Keep the existing logical JSON and physical field layout, including rollback
// compatibility with the previous release. The large, immutable diagnostic
// generation has already been encoded outside q.mu. Do not run it through
// json.Marshal/Unmarshal again merely to compare unchanged physical fragments.
func (q *Queue) marshalQueueBase(s state, prepareFields bool) ([]byte, map[string]json.RawMessage, error) {
	var efficiency json.RawMessage
	if len(s.EfficiencyHealth) > 0 {
		if q.efficiencyHealthJSON == nil {
			// Initial/imported state without a prepared diagnostic generation.
			var err error
			q.efficiencyHealthJSON, err = json.Marshal(s.EfficiencyHealth)
			if err != nil {
				return nil, nil, err
			}
		}
		efficiency = q.efficiencyHealthJSON
	}
	s.EfficiencyHealth = nil
	data, err := json.Marshal(s)
	if err != nil {
		return nil, nil, err
	}
	var object map[string]json.RawMessage
	if prepareFields {
		if err := json.Unmarshal(data, &object); err != nil {
			return nil, nil, err
		}
	}
	if len(efficiency) > 0 {
		if prepareFields {
			object["efficiency_health"] = efficiency
		}
		// Only copy already validated bytes. RawMessage in json.Marshal would
		// scan/compact the entire history again while holding the queue lock.
		data = append(data[:len(data)-1], `,"efficiency_health":`...)
		data = append(data, efficiency...)
		data = append(data, '}')
	}
	if !prepareFields {
		return data, nil, nil
	}
	fields, err := splitPersistenceFields(object, q.persistencePageBytes)
	return data, fields, err
}
