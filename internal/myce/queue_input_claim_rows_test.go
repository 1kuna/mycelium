package myce

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func legacyInputClaimField() string {
	return fieldPrefix("state") + base64.RawURLEncoding.EncodeToString([]byte("file_input_claims"))
}

func TestQueueInputClaimRowsMigrationAndRollback(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "queue.db")
	clock := &fakeClock{now: time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)}
	open := func(writeRows bool) (*Queue, error) {
		return Open(path, root, clock, func(q *Queue) { q.inputClaimRows = writeRows })
	}
	q, err := open(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if q != nil {
			_ = q.Close()
		}
	})
	if err := q.update(func() error { seedRetainedInputClaims(q, 64); return nil }); err != nil {
		t.Fatal(err)
	}
	want := maps.Clone(q.s.FileInputClaims)
	assertRows := func(wantRows bool) {
		t.Helper()
		var rowCount, legacyCount int
		if err := q.db.QueryRow(`SELECT COUNT(*) FILTER (WHERE key GLOB ?), COUNT(*) FILTER (WHERE key=?) FROM queue_state`, inputClaimRowPrefix+"*", legacyInputClaimField()).Scan(&rowCount, &legacyCount); err != nil {
			t.Fatal(err)
		}
		if wantRows && (rowCount != len(want) || legacyCount != 0) || !wantRows && (rowCount != 0 || legacyCount != 1) {
			t.Fatalf("wrong claim layout: rows=%d legacy=%d want rows=%t", rowCount, legacyCount, wantRows)
		}
		data, err := readQueueState(q.db, normalizedQueueMarker)
		if err != nil {
			t.Fatal(err)
		}
		var disk state
		if err := json.Unmarshal(data, &disk); err != nil {
			t.Fatal(err)
		}
		published, err := readGet(q.publishedReads.Load().root, "state").decode("state")
		if err != nil {
			t.Fatal(err)
		}
		for _, actual := range []map[string]FileInputClaim{q.s.FileInputClaims, disk.FileInputClaims, published.(*state).FileInputClaims} {
			if !maps.EqualFunc(actual, want, FileInputClaim.equal) {
				t.Fatal("live, durable and published claim receipts differ")
			}
		}
	}
	assertRows(false)
	if _, err := q.db.Exec(`CREATE TRIGGER fail_input_claim_row BEFORE INSERT ON queue_state WHEN NEW.key GLOB 'file_input_claims/*' BEGIN SELECT RAISE(ABORT,'test claim write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if migrated, err := open(true); err == nil {
		_ = migrated.Close()
		t.Fatal("migration committed past an injected row failure")
	}
	q, err = open(false)
	if err != nil {
		t.Fatal(err)
	}
	assertRows(false)
	if _, err := q.db.Exec("DROP TRIGGER fail_input_claim_row"); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = open(true)
	if err != nil {
		t.Fatal(err)
	}
	assertRows(true)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	// The reader-first release opens the active layout on rollback and writes
	// changes in that same layout. The active release then reads those changes.
	q, err = open(false)
	if err != nil {
		t.Fatal(err)
	}
	assertRows(true)
	pinned := q.publishedReads.Load()
	old := want[rootFixtureClaim]
	if err := q.update(func() error { q.forgetFileInputClaims(old.File.SHA256, clock.Now()); return nil }); err != nil {
		t.Fatal(err)
	}
	delete(want, old.ID)
	added, err := q.claimFileInput(t.Context(), Spec{Project: old.Project, Class: old.Class}, old.File)
	if err != nil || added.ID == old.ID {
		t.Fatalf("fresh claim after deletion: %v", err)
	}
	want[added.ID] = added
	assertRows(true)
	claimContext := context.WithValue(t.Context(), fileInputClaimKey{}, added.ID)
	if _, err := q.inputClaimContext(claimContext, added.File, true); err != nil {
		t.Fatal(err)
	}
	added.Allocated = true
	want[added.ID] = added
	assertRows(true)
	clock.Advance(DefaultFileRetention + time.Second)
	released, err := q.ReleaseFileInputClaim(t.Context(), FileInputClaimRelease{Project: added.Project, ClaimID: added.ID, AcceptExpiry: true})
	if err != nil || released.ReleasedAt.IsZero() {
		t.Fatalf("allocated claim release: %+v %v", released, err)
	}
	added.Released, added.SettledAt = true, released.ReleasedAt
	want[added.ID] = added
	assertRows(true)
	before, err := readGet(pinned.root, "state").decode("state")
	if err != nil || !before.(*state).FileInputClaims[old.ID].equal(old) || before.(*state).FileInputClaims[added.ID].ID != "" {
		t.Fatalf("edits changed a pinned publication: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = open(true)
	if err != nil {
		t.Fatal(err)
	}
	assertRows(true)
}

func TestQueueInputClaimRowsBoundWriterTurn(t *testing.T) {
	root := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(root, "queue.db"), root, clock, func(q *Queue) { q.inputClaimRows = true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	fakeCheckpointAvailability(t, q, true)
	if err := q.update(func() error { seedRetainedInputClaims(q, 14000); return nil }); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Files = FileManifest{}
	const batchSize = 16
	refs := make([]FileRef, 0, batchSize)
	for i := range batchSize {
		data := []byte(fmt.Sprintf("synthetic claim payload %d", i))
		ref := FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), SizeBytes: int64(len(data))}
		if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
		for alias := range 8 {
			s.Files[fmt.Sprintf("inputs/%02d/%d", i, alias)] = ref
		}
	}
	drainAdmissionWait(t, q)
	observational := &fakeClock{now: clock.Now()}
	q.holdClock = observational
	entered, resume := make(chan struct{}), make(chan struct{})
	resumeWriter := sync.OnceFunc(func() { close(resume) })
	defer resumeWriter()
	firstClaim := sync.OnceFunc(func() { close(entered); <-resume })
	var order []string
	var claimBytes, turnClaimRows, maxClaimBytes, maxClaimRows int
	var maxTurn time.Duration
	q.observeQueueRow = func(query string, args []any) {
		if query != upsertQueueState {
			return
		}
		key := args[0].(string)
		var bytes int
		switch data := args[1].(type) {
		case []byte:
			bytes = len(data)
		case json.RawMessage:
			bytes = len(data)
		default:
			t.Errorf("unexpected SQLite payload %T", data)
		}
		// Replay the statement cost of 24.5 s for 3.58 MB in #2257. This
		// models byte-sensitive writer occupation, not the host's fsync tail.
		observational.Advance(time.Duration(bytes) * 7 * time.Microsecond)
		if strings.HasPrefix(key, inputClaimRowPrefix) || key == legacyInputClaimField() {
			order = append(order, "claim")
			claimBytes += bytes
			turnClaimRows++
			firstClaim()
		}
		if key == "machines/checkin" {
			order = append(order, "checkin")
		}
	}
	q.observePersistenceHold = func(hold queueHoldPhases) {
		if turnClaimRows > 0 {
			maxTurn = max(maxTurn, hold.duration())
			maxClaimBytes = max(maxClaimBytes, claimBytes)
			maxClaimRows = max(maxClaimRows, turnClaimRows)
			claimBytes, turnClaimRows = 0, 0
		}
	}
	batch := make(chan error, 1)
	go func() {
		for _, ref := range refs {
			if _, err := q.claimFileInput(t.Context(), Spec{Project: s.Project}, ref); err != nil {
				batch <- err
				return
			}
		}
		batch <- nil
	}()
	awaitQueueWrite(t, entered)
	checkin := make(chan error, 1)
	go func() {
		_, err := q.Checkin(Checkin{Machine: machine("checkin", "fake"), period: time.Second})
		checkin <- err
	}()
	await(t, func() bool { return q.mu.progress().Waiting > 0 })
	resumeWriter()
	if err := awaitQueueWrite(t, checkin); err != nil {
		t.Fatal(err)
	}
	if err := awaitQueueWrite(t, batch); err != nil {
		t.Fatal(err)
	}
	if len(order) != batchSize+1 || order[0] != "claim" || order[1] != "checkin" {
		t.Fatalf("check-in writer did not proceed between claim writes: %v", order)
	}
	if maxClaimRows != 1 || maxClaimBytes > 512 || maxTurn <= 0 || maxTurn >= 250*time.Millisecond {
		t.Fatalf("issuance max writer turn %s, changed claim rows %d, bytes %d", maxTurn, maxClaimRows, maxClaimBytes)
	}
	t.Logf("issuance max modeled writer turn %s, claim payload %d bytes", maxTurn, maxClaimBytes)
	maxTurn, maxClaimBytes, maxClaimRows = 0, 0, 0
	if _, err := q.Submit(s); err != nil {
		t.Fatal(err)
	}
	drainAdmissionWait(t, q)
	if maxClaimRows != batchSize || maxClaimBytes > batchSize*512 || maxTurn <= 0 || maxTurn >= 250*time.Millisecond {
		t.Fatalf("settlement writer turn %s, changed claim rows %d, bytes %d", maxTurn, maxClaimRows, maxClaimBytes)
	}
	t.Logf("settlement modeled writer turn %s, claim payload %d bytes across %d rows", maxTurn, maxClaimBytes, maxClaimRows)
}
