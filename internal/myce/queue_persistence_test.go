package myce

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStorageEnvelopeLogicalDedupReplayRestart(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := storageResult(&first, out, 4)
	wire, _ := json.Marshal(r)
	if err := json.Unmarshal(wire, &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	p := onlyStorage(t, q)
	if p.Output.Peak != 8 || p.Managed.Peak != 4 || p.Managed.need(1) != 4 || p.Managed.margin() != 0 || !p.RecordedAt.Equal(clock.Now()) {
		t.Fatalf("logical dedup/4-unit evidence: %+v", p)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, onlyStorage(t, q)) {
		t.Fatal("replayed result taught twice")
	}
	var rows int
	if err := q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key LIKE 'storage_observations/%'").Scan(&rows); err != nil || rows != 1 {
		t.Fatal(rows, err)
	}
	q = storageRestart(t, q, clock)
	check(t, q, m, nil)
	next := startObservation(t, q, m, s)
	e := next.StorageEnvelope
	if e == nil || e.ManagedBytes == nil || *e.ManagedBytes != 4 || *e.OutputBytes != 8 || e.Multiplier != 1 || e.Samples != 1 || e.HardLimit != nil || !strings.Contains(e.Enforcement, "unenforced") {
		t.Fatalf("restart changed bound: %+v", e)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal("terminal replay after restart", err)
	}
	clock.Advance(time.Second)
	r = storageResult(&next, out, 6)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	learned := onlyStorage(t, q)
	if learned.Managed.PositiveError != 2 || learned.Managed.margin() != 2 || learned.Managed.need(1) != 8 {
		t.Fatalf("prediction miss/range: %+v", learned.Managed)
	}
	third := startObservation(t, q, m, s)
	if third.StorageEnvelope.Multiplier != 1+2.0/6 || *third.StorageEnvelope.ManagedBytes != 8 {
		t.Fatal("evidence-derived multiplier", third.StorageEnvelope)
	}
}

func TestQueuePersistenceStorageRollbackAndRemoval(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	if err := q.Complete(storageResult(&first, out, 4)); err != nil {
		t.Fatal(err)
	}
	before := onlyStorage(t, q)
	key := identityHash(before.Key)
	rejected := errors.New("abort storage update")
	if err := q.update(func() error {
		point := q.s.StorageObservations[key]
		point.Managed.Peak = 999
		q.s.StorageObservations[key] = point
		return rejected
	}); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, onlyStorage(t, q)) {
		t.Fatal("rollback lost accepted storage")
	}
	if err := q.update(func() error { delete(q.s.StorageObservations, key); return nil }); err != nil {
		t.Fatal(err)
	}
	q = storageRestart(t, q, clock)
	if len(q.s.StorageObservations) != 0 {
		t.Fatal("removed aggregate returned after restart", q.s.StorageObservations)
	}
}

// Inspect durable logical records independently of their physical row layout.
func durableQueueRecord(t *testing.T, db *sql.DB, key string) []byte {
	t.Helper()
	records, err := readQueueRecords(db, key)
	if err != nil {
		t.Fatal(err)
	}
	data, ok := records[key]
	if !ok {
		t.Fatalf("durable record %q missing", key)
	}
	return data
}

func TestQueuePersistenceLargeFieldsRollbackRestart(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Run.Env = map[string]string{"UNCHANGED": strings.Repeat("payload", 10000)}
	j := &Job{ID: "large/record", Spec: s, State: "paused", Queued: clock.Now()}
	if err := q.update(func() error { q.putJob(j.ID, j); return nil }); err != nil {
		t.Fatal(err)
	}
	var measured queuePersistenceTiming
	q.observePersistence = func(p queuePersistenceTiming) { measured = p }
	// Warm the detached serializer; a live field may mutate through an alias.
	if err := q.update(func() error { q.touchJob(j.ID).Reason = "first"; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { q.touchJob(j.ID).Reason = "second"; return nil }); err != nil {
		t.Fatal(err)
	}
	if measured.Bytes >= int64(len(s.Run.Env["UNCHANGED"]))/4 {
		t.Fatalf("unchanged payload rewritten: %+v", measured)
	}
	before := durableQueueRecord(t, q.db, "jobs/"+j.ID)
	if _, err := q.db.Exec(`CREATE TRIGGER reject_fields BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'test field failure'); END`); err != nil {
		t.Fatal(err)
	}
	change := func() error {
		live := q.touchJob(j.ID)
		live.Reason = "accepted"
		live.Spec.Run.Env["UNCHANGED"] = "small"
		return nil
	}
	if err := q.update(change); err == nil {
		t.Fatal("failed SQL accepted")
	}
	after := durableQueueRecord(t, q.db, "jobs/"+j.ID)
	if !bytes.Equal(before, after) || q.s.Jobs[j.ID].Reason != "second" || len(q.s.Jobs[j.ID].Spec.Run.Env["UNCHANGED"]) < 10000 {
		t.Fatal("rollback lost durable or in-memory state")
	}
	if _, err := q.db.Exec("DROP TRIGGER reject_fields"); err != nil {
		t.Fatal(err)
	}
	if err := q.update(change); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	got := q.s.Jobs[j.ID]
	if got.Reason != "accepted" || got.Spec.Run.Env["UNCHANGED"] != "small" {
		t.Fatal("restart restored stale large field")
	}
	if err := q.update(func() error { q.touchJob(j.ID); delete(q.s.Jobs, j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	records, err := readQueueRecords(q.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := records["jobs/"+j.ID]; ok {
		t.Fatal("deleted record fragments survived")
	}
}

func TestQueuePersistenceLegacyMigration(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "migration"
	j := submit(t, q, s)
	// Construct the exact previous normalized layout, including a cold record
	// that an ordinary update would never touch. The upgrade must retain both.
	if err := q.update(func() error {
		cold := &Job{ID: "cold", State: "succeeded", Spec: s, Queued: clock.Now(), FinishedAt: &clock.now, ReleaseConfirmed: true}
		q.putJob(cold.ID, cold)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Genuine v1 keyed answers embed the complete last durable job.
	legacy := make(map[string][]byte, len(q.persisted))
	for key, data := range q.persisted {
		legacy[key] = data
	}
	for key, record := range q.s.Submissions {
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		legacy["submissions/"+key] = data
	}
	tx, err := q.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM queue_state"); err != nil {
		t.Fatal(err)
	}
	for key, data := range legacy {
		if _, err = tx.Exec("INSERT INTO queue_state(key,data) VALUES(?,?)", key, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec("UPDATE queue SET data=? WHERE id=1", legacyNormalizedQueueMarker); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Resume the old layout in this fixture without a startup write, then
	// force the marker's final write to fail after all field writes ran.
	q.fragmented = false
	q.persisted = legacy
	q.persistenceFieldsCache = nil
	if _, err := q.db.Exec(`CREATE TRIGGER reject_migration BEFORE UPDATE ON queue BEGIN SELECT RAISE(ABORT,'migration interrupted'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { return nil }); err == nil {
		t.Fatal("interrupted migration committed")
	}
	var retainedMarker []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&retainedMarker); err != nil || !bytes.Equal(retainedMarker, legacyNormalizedQueueMarker) {
		t.Fatal("failed migration published format marker", err)
	}
	var fragmentCount int
	if err := q.db.QueryRow("SELECT count(*) FROM queue_state WHERE key GLOB 'fields/*'").Scan(&fragmentCount); err != nil || fragmentCount != 0 {
		t.Fatal("failed migration leaked fragments", fragmentCount, err)
	}
	if _, err := q.db.Exec("DROP TRIGGER reject_migration"); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	if q.s.Jobs[j.ID] == nil || q.s.Jobs["cold"] == nil {
		t.Fatal("migration lost jobs")
	}
	replay := submit(t, q, s)
	if replay.ID != j.ID {
		t.Fatal("migration lost keyed binding")
	}
	var marker []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil || !bytes.Equal(marker, normalizedQueueMarker) {
		t.Fatal("migration marker missing", err)
	}
	q = reopenQueue(t, q, clock)
	if q.s.Jobs["cold"] == nil {
		t.Fatal("cold migrated job lost on second restart")
	}
}

func TestQueuePersistenceCachedEncodingMatchesJSON(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Run.Env = map[string]string{"LARGE": strings.Repeat("x", 16000)}
	j := submit(t, q, s)
	for range 2 {
		if err := q.update(func() error {
			live := q.touchJob(j.ID)
			live.Spec.Run.Env["changed"] = "yes"
			live.Reason = "mutated"
			live.AdmissionWait = retainAdmissionWait(live.AdmissionWait,
				&AdmissionWaitSnapshot{At: clock.Now(), Attempt: live.Attempt, Predicate: "admission"}, 0, true)
			for key, value := range map[string]any{"state": q.s, "jobs/" + j.ID: live} {
				durable := value
				if job, ok := value.(*Job); ok && (q.startupPending(job) || q.startCapacityPaused(job)) {
					copy := *job
					copy.Reason = "recomputing admission diagnostics"
					durable = &copy
				}
				want, err := json.Marshal(durable)
				if err != nil {
					return err
				}
				got, _, err := q.marshalPersistenceValue(key, value)
				if err != nil {
					return err
				}
				if !bytes.Equal(want, got) {
					t.Fatal("cached serializer diverged from JSON")
				}
				if key == "jobs/"+j.ID {
					// Unchanged large jobs must reuse their immutable encoding;
					// ordinary field-cache cleanup used to discard wait evidence.
					again, _, err := q.marshalPersistenceValue(key, value)
					if err != nil {
						return err
					}
					if len(got) == 0 || len(again) == 0 || &got[0] != &again[0] {
						t.Fatal("unchanged large job re-encoded its admission evidence")
					}
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
	}
}

// The durable waiting anchor does not move with telemetry. Reads still expose
// elapsed time and live reasons, while transitions close the old interval.
func TestQueuePersistenceDiagnosticAnchorRestart(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(2, 1)
	s.Priority = "production"
	j := submit(t, q, s)
	drainAdmissionWait(t, q)
	before := durableQueueRecord(t, q.db, "jobs/"+j.ID)
	for range 3 {
		clock.Advance(time.Second)
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	after := durableQueueRecord(t, q.db, "jobs/"+j.ID)
	if !bytes.Equal(before, after) {
		t.Fatal("idle waiting job was rewritten")
	}
	view := cloneJob(q.s.Jobs[j.ID])
	q.projectEfficiency(&view)
	if view.Efficiency.WaitSeconds["unknown"] != 3 {
		t.Fatal("missing read-time wait")
	}
	q = reopenQueue(t, q, clock)
	view = cloneJob(q.s.Jobs[j.ID])
	q.projectEfficiency(&view)
	if view.Efficiency.WaitSeconds["unknown"] != 3 {
		t.Fatal("restart lost anchored wait")
	}
	if _, err := q.PauseJob(j.ID, PauseJobRequest{Reason: "input review"}); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	paused := q.s.Jobs[j.ID]
	if paused.State != "paused" || paused.Pause == nil || paused.Pause.Reason != "input review" || paused.Efficiency.WaitSeconds["unknown"] != 3 {
		t.Fatal("restart lost pause decision or closed interval")
	}
	clock.Advance(2 * time.Second)
	if _, err := q.CancelJob(j.ID); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	cancelled := q.s.Jobs[j.ID]
	if cancelled.CancelledAt == nil || cancelled.State != "cancelled" || cancelled.Efficiency.WaitSeconds["holds"] != 2 {
		t.Fatal("restart lost cancellation or pause interval")
	}
}
