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

// Ship the reader before enabling this writer. The preceding queue binary
// must open the same committed database on rollback (MY-42).
const inputClaimRowsDefault = true
const inputClaimRowPrefix = "file_input_claims/"

type queueRecordReader interface {
	Query(string, ...any) (*sql.Rows, error)
}

// Decode each logical record once. Rebuilding one giant JSON document scanned
// retained jobs repeatedly and copied live keyed answers into it a second time.
// Retain the original bytes as the writer's comparison base: disk format and
// adjacent-release compatibility are unchanged.
func (q *Queue) readStartupState(marker []byte) error {
	if !bytes.Equal(marker, normalizedQueueMarker) {
		return fmt.Errorf("unsupported queue persistence format; current and previous releases require normalized-state-v3")
	}
	// One decoder overlaps the SQLite reader and split-record reconstruction.
	// A single queued record bounds extra retained bytes; only the decoder owns
	// this map, and startup joins it on every success/error path before returning.
	type jobRecord struct {
		id   string
		data []byte
	}
	type jobResult struct {
		jobs map[string]*Job
		err  error
	}
	jobs := make(chan jobRecord, 1)
	done := make(chan jobResult, 1)
	go func() {
		result := jobResult{jobs: map[string]*Job{}}
		for record := range jobs {
			if result.err != nil {
				continue // Drain the reader even after a malformed record.
			}
			data, err := expandJobRecord(record.data)
			if err == nil {
				err = decodeQueueRecord(result.jobs, record.id, data)
			}
			if err != nil {
				result.err = fmt.Errorf("decode queue fragment jobs: %w", err)
				continue
			}
			shareAdmissionWaitEndpoints(result.jobs[record.id])
			rebaseCheckpoint(q.root, result.jobs[record.id])
		}
		done <- result
	}()
	records, err := readStartupQueueRecords(q.db, func(key string, data []byte) {
		if id, ok := strings.CutPrefix(key, "jobs/"); ok {
			jobs <- jobRecord{id, data}
		}
	}, q.startupReadWorkers)
	q.traceStartup("load_record_rows")
	close(jobs)
	decoded := <-done
	if err != nil {
		return err
	}
	if decoded.err != nil {
		return decoded.err
	}
	q.traceStartup("load_job_decode")
	base := records["state"]
	if len(base) == 0 || bytes.Equal(bytes.TrimSpace(base), []byte("null")) {
		return fmt.Errorf("queue state fragment missing")
	}
	if err := json.Unmarshal(base, &q.s); err != nil {
		return err
	}
	legacyClaims := len(q.s.FileInputClaims) > 0
	q.s.Jobs, q.s.Machines = decoded.jobs, map[string]*Machine{}
	events := map[int]HealthAlert{}
	for key, data := range records {
		if key == "state" {
			continue
		}
		group, id, ok := strings.Cut(key, "/")
		if !ok {
			return fmt.Errorf("unknown queue state fragment")
		}
		switch group {
		case "jobs":
			continue // Already decoded while reading/reassembling records.
		case "machines":
			err = decodeQueueRecord(q.s.Machines, id, data)
		case "submissions":
			if q.s.Submissions == nil {
				q.s.Submissions = map[string]*submissionRecord{}
			}
			err = decodeQueueRecord(q.s.Submissions, id, data)
		case "extensions":
			if q.s.Extensions == nil {
				q.s.Extensions = map[string]Extension{}
			}
			err = decodeQueueRecord(q.s.Extensions, id, data)
		case "work_observations":
			if q.s.WorkObservations == nil {
				q.s.WorkObservations = map[string]WorkObservation{}
			}
			err = decodeQueueRecord(q.s.WorkObservations, id, data)
		case "storage_observations":
			if q.s.StorageObservations == nil {
				q.s.StorageObservations = map[string]StorageObservation{}
			}
			err = decodeQueueRecord(q.s.StorageObservations, id, data)
		case "external_memory":
			if q.s.ExternalMemory == nil {
				q.s.ExternalMemory = map[string]*externalMemoryRing{}
			}
			err = decodeQueueRecord(q.s.ExternalMemory, id, data)
		case "file_input_claims":
			if legacyClaims {
				return fmt.Errorf("mixed queue input claim formats")
			}
			if q.s.FileInputClaims == nil {
				q.s.FileInputClaims = map[string]FileInputClaim{}
			}
			err = decodeQueueRecord(q.s.FileInputClaims, id, data)
			if err == nil && (id == "" || q.s.FileInputClaims[id].ID != id) {
				err = fmt.Errorf("invalid input claim identity")
			}
			q.inputClaimRows = true
		case "health_events":
			index, e := strconv.Atoi(id)
			if e != nil || index < 0 {
				return fmt.Errorf("invalid health evidence index")
			}
			err = decodeQueueRecord(events, index, data)
		default:
			return fmt.Errorf("unknown queue state fragment")
		}
		if err != nil {
			return fmt.Errorf("decode queue fragment %s: %w", group, err)
		}
	}
	if len(events) > 0 {
		q.s.Health.Events = make([]HealthAlert, len(events))
		for index, event := range events {
			if index >= len(events) {
				return fmt.Errorf("invalid health evidence index")
			}
			q.s.Health.Events[index] = event
		}
	}
	for _, record := range q.s.Submissions {
		if record != nil && record.Job == nil {
			record.Job = q.s.Jobs[record.JobID]
		}
	}
	q.persisted = records
	q.traceStartup("load_decode")
	return nil
}

func decodeQueueRecord[K comparable, V any](records map[K]V, key K, data []byte) error {
	var value V
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	records[key] = value
	return nil
}

// Logical records and prepared root fragments advance together only after a
// successful SQLite commit. A failed write never replaces the comparison base.
type queuePersistenceUpdate struct {
	records     map[string][]byte
	stateFields map[string]json.RawMessage
	stateSize   int
}

func (q *Queue) commitPersistenceRoot(next *queuePersistenceUpdate) {
	q.replaceEvidenceBytes(q.persistedStateSize, next.stateSize)
	q.persistedStateSize, q.persistedStateFields = next.stateSize, next.stateFields
}

// Immutable history stays in SQLite but is not rewritten by each heartbeat.
// Encode only journaled decisions; SQLite sees only changed logical records.
func (q *Queue) prepareStateWrite(batch *queueWriteBatch, measured *queuePersistenceTiming, observationOnly bool) (*queuePersistenceUpdate, error) {
	if q.persistencePageBytes <= 0 {
		return nil, fmt.Errorf("queue persistence page size is not initialized")
	}
	update := &queuePersistenceUpdate{records: map[string][]byte{}}
	next := update.records
	putCompact := func(key string, data []byte) {
		measured.Bytes += int64(len(data))
		measured.Records++
		batch.add(upsertQueueState, key, data)
	}
	putData := func(key string, data []byte) error {
		old := q.persisted[key]
		cold, wasCold := q.coldRecords[key]
		if wasCold && cold.matches(data) {
			return nil
		}
		next[key] = data
		if !wasCold && bytes.Equal(data, old) {
			next[key] = q.persisted[key]
			return nil
		}
		// Ordinary records fit one page and retain the original compact row.
		// No fragment preparation or extra B-tree keys are needed for them.
		compact := len(data) <= q.persistencePageBytes && len(old) <= q.persistencePageBytes && !wasCold
		if splitPersistenceKey(key) && !compact {
			return q.writeFields(key, old, data, measured, batch, nil)
		}
		putCompact(key, data)
		return nil
	}
	put := func(key string, value any) error {
		started := holdNow(q.holdClock)
		data, err := q.marshalPersistenceValue(key, value)
		measured.Marshal += holdNow(q.holdClock).Sub(started)
		if err != nil {
			return err
		}
		return putData(key, data)
	}

	if q.persisted == nil {
		batch.add("CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)")
		batch.add("DELETE FROM queue_state")
	}
	if observationOnly {
		update.stateSize, update.stateFields = q.persistedStateSize, q.persistedStateFields
	} else {
		started := holdNow(q.holdClock)
		stateRoot := q.persistentBase()
		if q.inputClaimRows {
			stateRoot.FileInputClaims = nil
		}
		base, err := q.encodeStateRoot(stateRoot)
		if err != nil {
			return nil, err
		}
		update.stateSize = base.size()
		if update.stateSize > q.persistencePageBytes {
			update.stateFields, err = base.split(q.persistencePageBytes)
			if err != nil {
				return nil, err
			}
		}
		stateData := update.stateFields[""]
		if len(update.stateFields) <= 1 {
			stateData = base.assemble()
		}
		measured.Marshal += holdNow(q.holdClock).Sub(started)
		// Only the small SQLite root belongs in the record journal. Keep its
		// detached fields and logical size in the same pending commit instead.
		next["state"] = stateData
		if len(update.stateFields) > 1 || len(q.persistedStateFields) > 1 {
			err = q.writeFields("state", q.persisted["state"], stateData, measured, batch, update.stateFields)
		} else if bytes.Equal(stateData, q.persisted["state"]) {
			next["state"] = q.persisted["state"]
		} else {
			putCompact("state", stateData)
		}
		if err != nil {
			return nil, err
		}
	}
	for id := range q.records.inputClaims {
		key := inputClaimRowPrefix + id
		if claim, ok := q.s.FileInputClaims[id]; ok {
			if id == "" || claim.ID != id {
				return nil, fmt.Errorf("invalid input claim identity")
			}
			if err := put(key, claim); err != nil {
				return nil, err
			}
		} else if q.persisted[key] != nil {
			measured.Records++
			next[key] = nil
			batch.add("DELETE FROM queue_state WHERE key=?", key)
		}
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
			batch.add("DELETE FROM queue_state WHERE key=?", key)
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
			batch.deleteFields("jobs/" + key)
			batch.add("DELETE FROM queue_state WHERE key=?", "jobs/"+key)
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
			batch.add("DELETE FROM queue_state WHERE key=?", key)
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
		batch.add("DELETE FROM queue_state WHERE key=?", key)
	}
	// A fresh database publishes its first full state and marker atomically.
	if q.persisted == nil {
		batch.add("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", normalizedQueueMarker)
	}
	return update, nil
}

func (q *Queue) loadPersistedState() error {
	if q.persisted == nil {
		return nil
	}
	var err error
	q.persistedStateFields, err = persistenceFields(q.persisted["state"], q.persistencePageBytes)
	if err != nil {
		return err
	}
	q.efficiencyHealthJSON, err = json.Marshal(q.s.EfficiencyHealth)
	if err != nil {
		return err
	}
	// The large claim field was already detached during startup decoding.
	if !q.inputClaimRows {
		claimField := base64.RawURLEncoding.EncodeToString([]byte("file_input_claims"))
		q.fileInputClaimsJSON = q.persistedStateFields[claimField]
	}
	q.persistedStateSize = len(q.persisted["state"])
	q.evidenceBytes = 0
	for key, data := range q.persisted {
		q.noteEvidenceRecord(key, nil, data)
	}
	if len(q.persistedStateFields) > 1 {
		q.persisted["state"] = q.persistedStateFields[""]
	}
	q.restoreLastSeen()
	if err := q.loadExpiredLearning(); err != nil {
		return err
	}
	for _, j := range q.s.Jobs {
		if mutableJob(j) {
			j.preparePlanningMemoryIdentity()
		}
	}
	q.rebuildRecords()
	q.histories = cloneHistories(q.s.EfficiencyHosts)
	return nil
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
	d.ControlPlaneCPUCores = 0
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
	// The queue's record codec puts the reference table first. It can be
	// many pages of immutable history. Borrow that value from the already
	// encoded record instead of decoding and copying it into a RawMessage.
	if bytes.HasPrefix(data, jobRecordRefsPrefix) {
		start := len(jobRecordRefsPrefix) - 1
		end, ok := skipJSONValue(data, start)
		if ok && data[end] == ',' && end-start > pageBytes {
			body := make([]byte, 1, len(data)-end)
			body[0] = '{'
			body = append(body, data[end+1:]...)
			fields, err := persistenceFields(body, pageBytes)
			if err != nil {
				return nil, err
			}
			fields[base64.RawURLEncoding.EncodeToString([]byte("$refs"))] = data[start:end]
			return fields, nil
		}
	}
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

const upsertQueueState = "INSERT INTO queue_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data"

func (b *queueWriteBatch) deleteFields(key string) {
	// Base64url contains no GLOB metacharacters.
	b.add("DELETE FROM queue_state WHERE key GLOB ?", fieldPrefix(key)+"*")
}
func (q *Queue) writeFields(key string, old, data []byte, measured *queuePersistenceTiming, batch *queueWriteBatch, next map[string]json.RawMessage) error {
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
		batch.deleteFields(key)
		measured.Bytes += int64(len(data))
		measured.Records++
		batch.add(upsertQueueState, key, data)
		return nil
	}
	// A one-piece old encoding lived in the ordinary logical row, so none of
	// these field rows exists yet. Publish the root along with its large fields.
	cold := q.coldRecords[key]
	if len(before) <= 1 && len(cold.fields) == 0 {
		batch.add("DELETE FROM queue_state WHERE key=?", key)
	}
	if len(before) == 1 {
		before = nil
	}
	prefix := fieldPrefix(key)
	for field, value := range next {
		if bytes.Equal(before[field], value) || cold.fieldMatches(field, value) {
			continue
		}
		measured.Bytes += int64(len(value))
		measured.Records++
		batch.add(upsertQueueState, prefix+field, value)
	}
	for field := range before {
		if _, ok := next[field]; !ok {
			measured.Records++
			batch.add("DELETE FROM queue_state WHERE key=?", prefix+field)
		}
	}
	for field := range cold.fields {
		if _, ok := next[field]; !ok {
			measured.Records++
			batch.add("DELETE FROM queue_state WHERE key=?", prefix+field)
		}
	}
	return nil
}

// visit receives each immutable, complete logical record exactly once. It may
// hand bytes to a decoder, but must not query this single-connection database.
func readQueueRecordsVisit(db queueRecordReader, visit func(string, []byte), only ...string) (map[string][]byte, error) {
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
	return readQueueRecordRows(rows, visit)
}

type queueRecordRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

func readQueueRecordRows(rows queueRecordRows, visit func(string, []byte)) (map[string][]byte, error) {
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
			if visit != nil {
				visit(key, data)
			}
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
		// A reconstructed row owns a new encoding. Release the consumed
		// physical rows as we go so they do not all survive the load beside it.
		delete(fields, key)
		delete(roots, key)
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
			if visit != nil {
				visit(key, root)
			}
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
		if visit != nil {
			visit(key, data)
		}
	}
	return records, nil
}

// Dirty records are encoded exactly once. No whole-state comparison caches.
func (q *Queue) marshalPersistenceValue(key string, value any) ([]byte, error) {
	if j, ok := value.(*Job); ok && j != nil && (q.startupPending(j) || q.startCapacityPaused(j)) {
		copy := *j
		copy.Reason = "recomputing admission diagnostics"
		value = &copy
	}
	if j, ok := value.(*Job); ok && j != nil && j.State == "queued" && j.AdmissionWait != nil && strings.HasPrefix(key, "jobs/") {
		return marshalWaitingJobRecord(j)
	}
	data, err := json.Marshal(value)
	if j, ok := value.(*Job); ok && err == nil && q.slimJobRecords && strings.HasPrefix(key, "jobs/") && !mutableJob(j) {
		data = slimJobRecord(data)
	}
	return data, err
}

// Large attachments already have their own SQLite rows in normalized-state-v3.
type queueStateEncoding struct {
	base             []byte
	efficiencyHealth json.RawMessage
	fileInputClaims  json.RawMessage
}

func (s queueStateEncoding) equal(other queueStateEncoding) bool {
	return bytes.Equal(s.base, other.base) &&
		bytes.Equal(s.efficiencyHealth, other.efficiencyHealth) && bytes.Equal(s.fileInputClaims, other.fileInputClaims)
}

func (q *Queue) encodeStateRoot(s state) (queueStateEncoding, error) {
	var encoded queueStateEncoding
	if len(s.EfficiencyHealth) > 0 {
		if q.efficiencyHealthJSON == nil {
			// Initial/imported state without a prepared diagnostic generation.
			var err error
			q.efficiencyHealthJSON, err = json.Marshal(s.EfficiencyHealth)
			if err != nil {
				return queueStateEncoding{}, err
			}
		}
		encoded.efficiencyHealth = q.efficiencyHealthJSON
	}
	if len(s.FileInputClaims) > 0 {
		if q.fileInputClaimsJSON == nil {
			var err error
			q.fileInputClaimsJSON, err = json.Marshal(s.FileInputClaims)
			if err != nil {
				return queueStateEncoding{}, err
			}
		}
		encoded.fileInputClaims = q.fileInputClaimsJSON
	}
	s.EfficiencyHealth, s.FileInputClaims = nil, nil
	var err error
	encoded.base, err = json.Marshal(s)
	return encoded, err
}

const efficiencyHealthPrefix = `,"efficiency_health":`
const fileInputClaimsPrefix = `,"file_input_claims":`

func (s queueStateEncoding) size() int {
	size := len(s.base)
	if len(s.efficiencyHealth) > 0 {
		size += len(efficiencyHealthPrefix) + len(s.efficiencyHealth)
	}
	if len(s.fileInputClaims) > 0 {
		size += len(fileInputClaimsPrefix) + len(s.fileInputClaims)
	}
	return size
}

func (s queueStateEncoding) assemble() []byte {
	if len(s.efficiencyHealth)+len(s.fileInputClaims) == 0 {
		return s.base
	}
	data := append(make([]byte, 0, s.size()), s.base[:len(s.base)-1]...)
	if len(s.efficiencyHealth) > 0 {
		data = append(data, efficiencyHealthPrefix...)
		data = append(data, s.efficiencyHealth...)
	}
	if len(s.fileInputClaims) > 0 {
		data = append(data, fileInputClaimsPrefix...)
		data = append(data, s.fileInputClaims...)
	}
	return append(data, '}')
}

func (s queueStateEncoding) split(pageBytes int) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(s.base, &object); err != nil {
		return nil, err
	}
	if len(s.efficiencyHealth) > 0 {
		object["efficiency_health"] = s.efficiencyHealth
	}
	if len(s.fileInputClaims) > 0 {
		object["file_input_claims"] = s.fileInputClaims
	}
	return splitPersistenceFields(object, pageBytes)
}
