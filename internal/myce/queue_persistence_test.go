package myce

import (
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
