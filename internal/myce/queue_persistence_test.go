package myce

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The installed-cutover fixture uses 7,000 receipts to represent the retained
// production claim catalog without using any real records.
func seedRetainedInputClaims(q *Queue, count int) {
	claims := make(map[string]FileInputClaim, count)
	now := q.clock.Now()
	for i := range count {
		id := fmt.Sprintf("synthetic-claim-%05d", i)
		claims[id] = FileInputClaim{
			ID: id, Project: "synthetic-volume", Class: "experiment",
			File: FileRef{SHA256: fmt.Sprintf("%064x", i), SizeBytes: 1},
			At:   now, SettledAt: now,
		}
	}
	q.s.FileInputClaims = claims
}

func TestQueuePersistenceInputClaimsRollbackRestart(t *testing.T) {
	q, clock := fixture(t)
	if err := q.update(func() error { seedRetainedInputClaims(q, 64); return nil }); err != nil {
		t.Fatal(err)
	}
	assertClaims := func(want map[string]FileInputClaim) {
		t.Helper()
		var disk state
		if err := json.Unmarshal(durableQueueRecord(t, q.db, "state"), &disk); err != nil {
			t.Fatal(err)
		}
		published, err := readGet(q.readRoot, "state").decode("state")
		if err != nil {
			t.Fatal(err)
		}
		for _, actual := range []map[string]FileInputClaim{q.s.FileInputClaims, disk.FileInputClaims, published.(*state).FileInputClaims} {
			if !maps.EqualFunc(actual, want, FileInputClaim.equal) {
				t.Fatal("live, published and durable claims must agree")
			}
		}
	}
	first := maps.Clone(q.s.FileInputClaims)
	assertClaims(first)
	id := "synthetic-claim-00000"
	claim := first[id]
	clock.Advance(time.Second)
	change := func() error {
		q.forgetFileInputClaims(claim.File.SHA256, clock.Now())
		return nil
	}
	if _, err := q.db.Exec(`CREATE TRIGGER reject_claims BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'test claim write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.update(change); err == nil {
		t.Fatal("failed SQLite write removed the claim")
	}
	if _, err := q.db.Exec("DROP TRIGGER reject_claims"); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertClaims(first)
	if err := q.update(change); err != nil {
		t.Fatal(err)
	}
	delete(first, id)
	assertClaims(first)
	q = reopenQueue(t, q, clock)
	assertClaims(first)
}

// The startup fast path must reconstruct exactly the state read by the
// adjacent-release document decoder, including keyed answers and split fields.
func TestQueuePersistenceStartupMatchesDocument(t *testing.T) {
	q, _ := candidateCostFixture(t)
	s := cpuSpec(1, 1)
	s.SubmissionKey = "cutover-key"
	if _, err := q.Submit(s); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.appendHealthAlert(HealthAlert{Kind: "storage_unavailable", Store: "synthetic", At: q.clock.Now()})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	data, err := readQueueState(q.db, normalizedQueueMarker)
	if err != nil {
		t.Fatal(err)
	}
	var expected state
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	// Keep the document reader as the independent sequential reference. Sparse
	// rowids exercise the concurrent ranges without assuming dense inserts.
	if _, err := q.db.Exec("UPDATE queue_state SET rowid=rowid*1000000"); err != nil {
		t.Fatal(err)
	}
	q.s = state{}
	if err := q.readStartupState(normalizedQueueMarker); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, q.s) {
		t.Fatal("record decoding changed the durable state")
	}
	if q.db.Stats().MaxOpenConnections != 1 {
		t.Fatal("startup left concurrent connections enabled for ordinary writes")
	}
	for pragma, expected := range map[string]int{"busy_timeout": 5000, "synchronous": 2} {
		var got int
		if err := q.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil || got != expected {
			t.Fatalf("startup changed writer %s: %d, %v", pragma, got, err)
		}
	}
	for _, record := range q.s.Submissions {
		if record.Job != q.s.Jobs[record.JobID] {
			t.Fatal("keyed answer did not reuse committed job")
		}
	}
	// Both reader and decoder failures must join the bounded startup pipeline.
	// A following load must still return the complete, unchanged state.
	for _, tc := range []struct{ name, key, data string }{
		{"invalid_field_key", "fields/invalid", `{}`},
		{"invalid_json", "jobs/corrupt", `{"id":`},
		{"invalid_reference", "jobs/corrupt", `{"$refs":[],"id":{"$ref":0}}`},
		{"missing_field_root", fieldPrefix("jobs/corrupt") + "cmVhc29u", `"orphan"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := q.db.Exec("INSERT INTO queue_state(key,data) VALUES(?,?)", tc.key, []byte(tc.data)); err != nil {
				t.Fatal(err)
			}
			if err := q.readStartupState(normalizedQueueMarker); err == nil {
				t.Fatal("corrupt startup record accepted")
			}
			if _, err := q.db.Exec("DELETE FROM queue_state WHERE key=?", tc.key); err != nil {
				t.Fatal(err)
			}
			q.s = state{}
			if err := q.readStartupState(normalizedQueueMarker); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expected, q.s) {
				t.Fatal("failed startup changed the following load")
			}
		})
	}
}

func TestQueuePersistenceRetiredDockerJob(t *testing.T) {
	q, clock := fixture(t)
	// An actual pre-retirement job row, not a new submission: validation must
	// refuse new Docker work without making historical records unreadable.
	const record = `{"id":"docker-history","state":"succeeded","attempt":1,
		"queued":"2026-10-08T11:59:00Z","finished_at":"2026-10-08T12:00:00Z",
		"docker_cause":{"operation":"inspect","class":"not_found"},
		"spec":{"name":"historical container","project":"test","priority":"experiment",
		"run":{"command":["registry.invalid/image@sha256:old"],"docker":true,
		"docker_network":"private","docker_memory_gb":2,"docker_memory_swap":0,
		"docker_sidecars":[{"name":"server","image":"registry.invalid/server:old"}]}}}`
	if _, err := q.db.Exec("INSERT INTO queue_state(key,data) VALUES(?,?)", "jobs/docker-history", []byte(record)); err != nil {
		t.Fatal(err)
	}
	var original Job
	if err := json.Unmarshal([]byte(record), &original); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		q = reopenQueue(t, q, clock)
		stored := q.s.Jobs[original.ID]
		if stored == nil || stored.State != original.State || !reflect.DeepEqual(stored.Spec.Run, original.Spec.Run) {
			t.Fatalf("restart %d lost Docker history: %+v", i, stored)
		}
		r := httptest.NewRequest("GET", "/api/jobs/"+original.ID, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		var displayed Job
		if err := json.Unmarshal(w.Body.Bytes(), &displayed); err != nil || w.Code != http.StatusOK || displayed.ID != original.ID || displayed.State != original.State || displayed.Spec.Name != original.Spec.Name {
			t.Fatalf("historical job display: status=%d body=%s err=%v", w.Code, w.Body.String(), err)
		}
		// Force the current writer to persist the row before the next restart.
		if err := q.update(func() error {
			q.touchJob(original.ID).Reason = "retained history"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueuePersistenceIgnoresRetiredResident(t *testing.T) {
	q, clock := fixture(t)
	const record = `{"id":"model-history","state":"succeeded","attempt":1,
		"queued":"2026-10-08T11:59:00Z","finished_at":"2026-10-08T12:00:00Z",
		"spec":{"name":"historical model job","project":"test","priority":"experiment",
		"run":{"command":["true"]},"resident":{"model":"old-model","engine":{"model":{"name":"private-model"}}}}}`
	if _, err := q.db.Exec("INSERT INTO queue_state(key,data) VALUES(?,?)", "jobs/model-history", []byte(record)); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	stored := q.s.Jobs["model-history"]
	if stored == nil || stored.State != "succeeded" || stored.Spec.Resident != nil || stored.Spec.Name != "historical model job" {
		t.Fatalf("retired binding affected restored job: %+v", stored)
	}
}

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
		q.touchCatalog("storage_observations/" + key)
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
	if err := q.update(func() error {
		q.touchCatalog("storage_observations/" + key)
		delete(q.s.StorageObservations, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = storageRestart(t, q, clock)
	if len(q.s.StorageObservations) != 0 {
		t.Fatal("removed aggregate returned after restart", q.s.StorageObservations)
	}
}

// Inspect durable logical records independently of their physical row layout.
func durableQueueRecord(t testing.TB, db *sql.DB, key string) []byte {
	t.Helper()
	records, err := readQueueRecords(db, key)
	if err != nil {
		t.Fatal(err)
	}
	data, ok := records[key]
	if !ok {
		t.Fatalf("durable record %q missing", key)
	}
	if strings.HasPrefix(key, "jobs/") {
		// Settled job rows store shared values; decode what every reader sees.
		if data, err = expandJobRecord(data); err != nil {
			t.Fatal(key, err)
		}
	}
	return data
}

func TestQueuePersistenceRetiredFormatsRefused(t *testing.T) {
	for _, marker := range []string{`"myce-normalized-state-v2"`, `"myce-normalized-state-v1"`, `{"schema":3,"jobs":{}}`} {
		t.Run(marker, func(t *testing.T) {
			q, clock := fixture(t)
			submit(t, q, spec("experiment"))
			if _, err := q.db.Exec("UPDATE queue SET data=? WHERE id=1", []byte(marker)); err != nil {
				t.Fatal(err)
			}
			path, root := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, root, clock)
			if err == nil {
				reopened.Close()
				t.Fatal("retired format accepted")
			}
			if !strings.Contains(err.Error(), "unsupported queue persistence format") {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var got []byte
			if err := db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&got); err != nil || string(got) != marker {
				t.Fatal("refusal changed stored format", err)
			}
		})
	}
}
