package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Extend the historical wire fixture to a fully settled collection, then
// reopen normalized SQLite records before exercising owner-authorized release.
func TestStorageCompatCollectedCustodyDrains(t *testing.T) {
	wire, err := os.ReadFile("testdata/parked-storage-state.json")
	if err != nil {
		t.Fatal(err)
	}
	var old state
	if err := json.Unmarshal(wire, &old); err != nil {
		t.Fatal(err)
	}
	j := old.Jobs["retained"]
	collected := j.LocalOutputs.StoredAt.Add(time.Hour)
	j.LocalOutputs.State, j.LocalOutputs.CollectedAt = "collected", &collected
	j.Spec.Files = FileManifest{"source": j.LocalOutputs.Files["result"]}
	j.Delivery.ManifestSHA256 = deliveryDigest(*j)
	// Collection removed the backing charge, while retaining job provenance.
	wire, err = json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(wire, &document); err != nil {
		t.Fatal(err)
	}
	document["storage_protection"] = map[string]any{"enabled": true, "allocations": map[string]any{}, "custody": map[string]any{}}
	wire, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	database, root, files := filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), filepath.Join(dir, "files")
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE queue (id INTEGER PRIMARY KEY, data BLOB NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO queue VALUES (1, ?)", wire); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	c := &fakeClock{now: collected.Add(10 * 24 * time.Hour)}
	q, err := Open(database, root, c, WithFileStore(files, 1024, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(database, root, c, WithFileStore(files, 1024, true))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ref := j.LocalOutputs.Files["result"]
	blob := q.files.blob(ref)
	if err := os.MkdirAll(filepath.Dir(blob), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("kept"), 0600); err != nil {
		t.Fatal(err)
	}
	oldTime := c.Now().Add(-filePublicationGrace - time.Hour)
	if err := os.Chtimes(blob, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	// Both historical holders share this identity. Ordinary references still
	// protect it until their owners release the whole selected group.
	selection := FileReleaseSelection{Project: "example", Jobs: []string{"retained", "ordinary"}}
	plan, err := q.PlanFileRelease(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range plan.Jobs {
		if len(item.Blockers) != 0 {
			t.Fatalf("fully collected historical custody cannot release: %+v", plan.Jobs)
		}
	}
	if mutableJob(q.s.Jobs["retained"]) {
		t.Fatal("settled collection still pins history")
	}
	// A stale delivery acknowledgement must retain the pin, even when a
	// collected timestamp survived in historical state.
	if err := q.update(func() error { q.touchJob("retained").Delivery.Attempt++; return nil }); err != nil {
		t.Fatal(err)
	}
	stale, err := q.PlanFileRelease(context.Background(), selection)
	if err != nil || len(stale.Jobs[1].Blockers) == 0 {
		t.Fatal("stale collection was releasable", stale, err)
	}
	if _, err := releaseApply(q, plan); err == nil {
		t.Fatal("old plan released stale custody")
	}
	if err := q.update(func() error { q.touchJob("retained").Delivery.Attempt--; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := releaseApply(q, plan); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blob); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("settled identity did not drain", err)
	}
	if err := q.pruneBatchLogs(); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Minute)
	if err := q.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs["retained"] != nil {
		t.Fatal("settled history did not drain")
	}
}

// A crash after the synced child state, before its root discovery/index write,
// must never turn an existing protected execution into an empty worker.
func TestStorageCompatUnindexedProtectedCustody(t *testing.T) {
	root := &workerStore{dir: t.TempDir(), state: workerState{Session: "saved"}}
	id := strings.Repeat("a", 64)
	j := Job{ID: "retained", Attempt: 1, Session: "saved", StorageAllocation: &StorageAllocation{ID: id}}
	child := workerState{StorageDirectory: id, Session: "saved", Job: &j}
	dir := filepath.Join(root.dir, "protected-attempts", id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(dir, "state-attempt-saved.json", child); err != nil {
		t.Fatal(err)
	}
	stores, err := root.attemptStores()
	if err == nil || !strings.Contains(err.Error(), "parked") || len(stores) != 0 {
		t.Fatal("existing protected execution silently disappeared from recovery; worker will claim no custody")
	}
	if root.state.Session != "saved" || root.state.Fresh || len(root.state.Attempts) != 0 {
		t.Fatal("discovery changed the saved session/index")
	}
	data, err := os.ReadFile(filepath.Join(dir, "state-attempt-saved.json"))
	if err != nil {
		t.Fatal(err)
	}
	var kept workerState
	if err := decodeWorkerState(data, &kept); err != nil || kept.Job == nil || kept.Job.ID != j.ID {
		t.Fatal("discovery discarded child custody", kept, err)
	}
	// A child whose former owner already cleared its custody is historical
	// metadata, not a reason to block this worker forever.
	if err := saveWorkerJSON(dir, "state-attempt-saved.json", workerState{Session: "saved", StorageDirectory: id}); err != nil {
		t.Fatal(err)
	}
	if stores, err := root.attemptStores(); err != nil || len(stores) != 0 {
		t.Fatal("cleared protected child blocked recovery", stores, err)
	}
	// The same cleared child may still have its old discovery/index record.
	// Read the exact child rather than treating that historical pointer as live.
	if err := saveWorkerJSON(root.dir, "state-attempt-saved.json", workerState{Session: "saved", StorageDirectory: id}); err != nil {
		t.Fatal(err)
	}
	root.state.Attempts = []string{"state-attempt-saved.json"}
	if stores, err := root.attemptStores(); err != nil || len(stores) != 0 {
		t.Fatal("cleared indexed child blocked recovery", stores, err)
	}
	if err := os.Remove(filepath.Join(dir, "state-attempt-saved.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := root.attemptStores(); err == nil {
		t.Fatal("missing indexed child was treated as cleared")
	}
}
