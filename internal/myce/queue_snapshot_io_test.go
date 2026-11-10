package myce

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// Replay the retained-history trigger with real SQLite and current frozen refs.
// A timestamp-only update must not rewrite
// the unchanged history to the WAL. No wall-clock performance assertion is used.
func TestQueueHistoryDoesNotAmplifyControlWrites(t *testing.T) {
	q, c := fixture(t)
	var seq int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	q.s.Extensions = map[string]Extension{}
	for i := 0; i < 32; i++ {
		name := fmt.Sprintf("fixture-%d", i)
		e := fakeExtension(t)
		e.Name = name
		e.Files["fixture"] = make([]byte, 600<<10)
		e = freezeComposition(t, e)
		if err := q.files.freezeExtension(&e); err != nil {
			t.Fatal(err)
		}
		q.s.Extensions[extensionKey(e)] = e
	}
	want := map[string]Extension{}
	for key := range q.s.Extensions {
		want[key] = registeredExtension(t, q, key)
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	var err error
	q.Close()
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	for key, expected := range want {
		if !reflect.DeepEqual(registeredExtension(t, q, key), expected) {
			t.Fatal("retained history changed during restart")
		}
	}
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err = q.db.Exec("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	c.Advance(137 * time.Millisecond)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	wal, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	// Normal metadata fits comfortably below this budget; the old implementation
	// writes approximately 26 MB for the same single control update.
	if wal.Size() > 256<<10 {
		t.Errorf("control update rewrote retained history: %d WAL bytes", wal.Size())
	}
	q.Close()
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	for key, expected := range want {
		if !reflect.DeepEqual(registeredExtension(t, q, key), expected) {
			t.Fatal("retained history lost on restart")
		}
	}
}
