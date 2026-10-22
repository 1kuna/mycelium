package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// Replay the 26 MB retained-history trigger with real SQLite, including upgrade
// from the existing full-state format. A timestamp-only update must not rewrite
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
		q.s.Extensions[name] = Extension{Name: name, Files: map[string][]byte{"fixture": make([]byte, 600<<10)}}
	}
	want := q.s.Extensions
	legacy, err := json.Marshal(q.s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.db.Exec("UPDATE queue SET data=? WHERE id=1", legacy); err != nil {
		t.Fatal(err)
	}
	q.Close()
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.s.Extensions, want) {
		t.Fatal("legacy history changed during upgrade")
	}
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	} // First write may migrate all legacy history.
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
	if !reflect.DeepEqual(q.s.Extensions, want) {
		t.Fatal("retained history lost on restart")
	}
}
