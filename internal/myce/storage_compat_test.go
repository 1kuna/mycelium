package myce

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Seed retained custody bytes in the previous release's normalized layout.
// Both opens must preserve its unknown parked fields without importing state.
func TestStorageCompatRetainedDatabaseReopens(t *testing.T) {
	wire, err := os.ReadFile("testdata/parked-storage-state.json")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	database, root, files := filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), filepath.Join(dir, "files")
	if err := PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	seedParkedStorageState(t, database, wire)
	var before state
	if err = json.Unmarshal(wire, &before); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	for n := 0; n < 2; n++ {
		q, err := Open(database, root, clock, WithFileStore(files, 16, true), WithFileStoreProductionReserve(8))
		if err != nil {
			t.Fatal("reopen", n, err)
		}
		func() {
			defer q.Close()
			fakeCheckpointAvailability(t, q, true)
			for _, pair := range [][2]json.RawMessage{{before.StorageMigration, q.s.StorageMigration}, {before.StorageProtection, q.s.StorageProtection}} {
				var a, b any
				if json.Unmarshal(pair[0], &a) != nil || json.Unmarshal(pair[1], &b) != nil || !reflect.DeepEqual(a, b) {
					t.Fatal("parked policy lost", n)
				}
			}
			got := q.s.Jobs["retained"]
			if got == nil || !reflect.DeepEqual(got.StorageAllocation, before.Jobs["retained"].StorageAllocation) || !reflect.DeepEqual(got.LocalOutputs, before.Jobs["retained"].LocalOutputs) || got.Delivery.VerificationRound != 2 || got.Spec.StorageProtocol != "worker-custody/1" || got.OutputReservationBytes == nil || *got.OutputReservationBytes != 8 {
				t.Fatalf("retained fields lost: %+v", got)
			}
			if !reflect.DeepEqual(q.s.Jobs["ordinary"].OutputRefs, before.Jobs["ordinary"].OutputRefs) {
				t.Fatal("accepted manifest lost")
			}
			ref := got.LocalOutputs.Files["result"]
			blob := q.files.blob(ref)
			if err := os.MkdirAll(filepath.Dir(blob), 0700); err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				if err := os.WriteFile(blob, []byte("kept"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if reason := q.diskHeadroomReason(got, nil); reason != parkedStorageReason {
				t.Fatal("retained assignment lost hold", reason)
			}
			plan, err := q.PlanFileRelease(context.Background(), FileReleaseSelection{Project: "example", Jobs: []string{got.ID}})
			if err != nil || len(plan.Jobs) != 1 || !strings.Contains(strings.Join(plan.Jobs[0].Blockers, ","), "parked_storage_custody") {
				t.Fatal("retained custody releasable", plan, err)
			}
			if err := q.Complete(Result{Execution: Execution{ID: got.ID, Attempt: got.Attempt}, Machine: got.Machine, Session: got.Session, Success: true}); err != nil || len(got.OutputRefs) != 0 {
				t.Fatal("terminal replay changed retained availability", err)
			}
			// A retained activation flag must not disable either current guard.
			status := q.fileStoreStatus(context.Background())
			if status == nil || status.LimitBytes != 16 || status.ProductionReserveBytes != 8 {
				t.Fatal("retained mode changed guards", status)
			}
			ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "example", Class: "experiment"})
			_, need := policyInput(t, "12345")
			assertFilePolicyRefusal(t, q.PreflightFileStore(ctx, FileManifest{"next": need}), "production reserve")
			if n == 1 {
				clock.Advance(8 * 24 * time.Hour)
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if err := q.update(func() error { q.touchJob("retained"); return nil }); err != nil {
				t.Fatal(err)
			}
			kept, err := os.ReadFile(blob)
			if err != nil || !bytes.Equal(kept, []byte("kept")) || q.s.Jobs["retained"] == nil || q.s.Jobs["retained"].FilesExpired != before.Jobs["retained"].FilesExpired {
				t.Fatal("retained local custody collected", err)
			}
		}()
	}
}

func TestStorageCompatRejectsNewProtectedTasks(t *testing.T) {
	s := spec("normal")
	s.StorageProtocol = "worker-custody/1"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "parked") {
		t.Fatal(err)
	}
	// Worker JSON is strict: retain its old field reader, but never recover an
	// admitted native boundary as an ordinary unprotected command.
	data := []byte(`{"storage_directory":"saved-allocation","session":"saved","fresh":false,"offset":0}`)
	var saved workerState
	if err := decodeWorkerState(data, &saved); err != nil {
		t.Fatal(err)
	}
	if _, err := (Worker{}).recoverAttempt(&workerStore{state: saved}); err == nil || !strings.Contains(err.Error(), "parked") {
		t.Fatal(err)
	}
	if _, err := (&workerStore{state: saved}).attemptStores(); err == nil || !strings.Contains(err.Error(), "parked") {
		t.Fatal("parked discovery record was ignored", err)
	}

}

// Keep the historical custody document while using the supported r6.8 format.
func seedParkedStorageState(t *testing.T, database string, wire []byte) {
	t.Helper()
	var base map[string]json.RawMessage
	if err := json.Unmarshal(wire, &base); err != nil {
		t.Fatal(err)
	}
	records := map[string]json.RawMessage{}
	for _, group := range []string{"jobs", "submissions", "extensions", "work_observations", "storage_observations", "external_memory"} {
		if data := base[group]; data != nil {
			var members map[string]json.RawMessage
			if err := json.Unmarshal(data, &members); err != nil {
				t.Fatal(err)
			}
			for id, value := range members {
				records[group+"/"+id] = value
			}
			delete(base, group)
		}
	}
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	records["state"] = data
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{"CREATE TABLE queue (id INTEGER PRIMARY KEY, data BLOB NOT NULL)", "CREATE TABLE queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL)"} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO queue VALUES (1, ?)", fieldQueueMarker); err != nil {
		t.Fatal(err)
	}
	for key, value := range records {
		if _, err := db.Exec("INSERT INTO queue_state VALUES (?, ?)", key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
}
