package myce

import (
	"encoding/json"
	"errors"
	"testing"
)

// Catalog values contain mutable nested facts. Exercise reuse, a failed body,
// a failed SQLite write after encoding, another mutation after rollback, and
// restart so cached encodings can neither leak nor lose an accepted change.
func TestQueueCatalogRollbackAndRestart(t *testing.T) {
	q, clock := fixture(t)
	peak := 3.0
	e := fakeExtension(t)
	if err := q.update(func() error {
		q.s.WorkObservations = map[string]WorkObservation{"curve": {MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}}
		q.s.Extensions = map[string]Extension{"example": e}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	wire := func() string {
		t.Helper()
		data, err := json.Marshal(q.s)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	before := wire()
	rejected := errors.New("rejected")
	for _, storageFailure := range []bool{false, true} {
		if storageFailure {
			if _, err := q.db.Exec(`CREATE TRIGGER reject_catalog BEFORE INSERT ON queue_state WHEN NEW.key='work_observations/curve' BEGIN SELECT RAISE(ABORT, 'rejected'); END`); err != nil {
				t.Fatal(err)
			}
		}
		err := q.update(func() error {
			*q.s.WorkObservations["curve"].MemoryPeak.HostGB = 99
			q.s.Extensions["example"].Run[0] = "changed"
			if storageFailure {
				return nil
			}
			return rejected
		})
		if err == nil || !storageFailure && !errors.Is(err, rejected) {
			t.Fatal("mutation was not rejected", err)
		}
		if got := wire(); got != before {
			t.Fatal("rejected update changed the queue")
		}
		if storageFailure {
			if _, err := q.db.Exec(`DROP TRIGGER reject_catalog`); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := q.update(func() error {
		*q.s.WorkObservations["curve"].MemoryPeak.HostGB = 7
		delete(q.s.Extensions, "example")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	if *q.s.WorkObservations["curve"].MemoryPeak.HostGB != 7 || len(q.s.Extensions) != 0 {
		t.Fatal("restart lost accepted nested change or restored removed registration")
	}
}
