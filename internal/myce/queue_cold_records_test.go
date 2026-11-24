package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestColdJobRecordKeepsCommittedHistory(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	reason := strings.Repeat("retained failure evidence ", 360)
	ledgerReason := strings.Repeat("ledger failure evidence ", 420)
	if err := q.update(func() error {
		stored := q.touchJob(j.ID)
		now := clock.Now()
		stored.State, stored.ReleaseConfirmed, stored.FinishedAt = "failed", true, &now
		stored.Reason = reason
		q.appendJobLedger(stored, ledgerAttempt(stored, "result", ledgerReason))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := q.readGeneration
	key := "jobs/" + j.ID
	if q.persisted[key] != nil || q.coldRecords[key].size == 0 || readGet(before.root, key).data != nil {
		t.Fatal("large settled job retained a serialized copy")
	}

	sample := func(g *readGeneration, want string) {
		t.Helper()
		stored, err := g.job(context.Background(), j.ID)
		if err != nil || stored == nil || stored.Reason != want || len(stored.LedgerEntries) != 0 {
			t.Fatalf("published record changed: %v", err)
		}
	}
	sample(before, reason)
	var timing queuePersistenceTiming
	q.observePersistence = func(observed queuePersistenceTiming) { timing = observed }
	if err := q.update(func() error {
		q.touchJob(j.ID).Reason = "small receipt"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if timing.Bytes >= int64(len(reason)) {
		t.Fatalf("scalars rewrote unchanged split history: %d bytes", timing.Bytes)
	}
	sample(before, reason)
	sample(q.readGeneration, "small receipt")

	expected := errors.New("abort")
	if err := q.update(func() error {
		q.touchJob(j.ID).Reason = "uncommitted"
		return expected
	}); err != expected {
		t.Fatal(err)
	}
	sample(q.readGeneration, "small receipt")
	path, files, checkpoint := strings.TrimSuffix(q.lock.Name(), ".lock"), q.files.root, q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, checkpoint, clock, WithFileStore(files, 100<<30, false))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	sample(reopened.readGeneration, "small receipt")
	page, err := reopened.JobLedger(j.ID, 0, -1, 100)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Reason != ledgerReason {
		t.Fatalf("lost ledger after restart: %v", err)
	}
}
