package myce

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DowngradeQueue converts a stopped queue to the adjacent release's v2 layout.
// It never opens a Queue: startup maintenance would change custody and decisions.
// Raw JSON retains even fields this binary does not understand; all other SQLite
// tables and external files stay in place. The queue's exclusive lock fences
// both binaries, and SQLite publishes the layout and marker atomically.
func DowngradeQueue(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err = os.Stat(abs); err != nil {
		return err
	}
	lock, err := os.OpenFile(abs+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("queue database already in use; stop the queue before downgrade: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	u := url.URL{Scheme: "file", Path: abs, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000"); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var marker []byte
	if err = tx.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil {
		return err
	}
	if !bytes.Equal(marker, normalizedQueueMarker) && !bytes.Equal(marker, fieldQueueMarker) {
		return fmt.Errorf("queue downgrade requires normalized v3 or v2 state")
	}
	before, err := readQueueState(tx, marker)
	if err != nil {
		return err
	}
	if bytes.Equal(marker, fieldQueueMarker) {
		return nil // Verified, already compatible; safe to repeat after interruption.
	}
	records, err := readQueueRecords(tx)
	if err != nil {
		return err
	}
	var base map[string]json.RawMessage
	if err = json.Unmarshal(records["state"], &base); err != nil {
		return err
	}
	machines := map[string]json.RawMessage{}
	for key, value := range records {
		if id, ok := strings.CutPrefix(key, "machines/"); ok {
			machines[id] = value
		}
	}
	base["machines"], err = json.Marshal(machines)
	if err != nil {
		return err
	}
	data, err := json.Marshal(base)
	if err != nil {
		return err
	}
	if err = deleteFields("state", tx.Exec); err != nil {
		return err
	}
	if _, err = tx.Exec("INSERT INTO queue_state(key,data) VALUES('state',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", data); err != nil {
		return err
	}
	if _, err = tx.Exec("DELETE FROM queue_state WHERE key GLOB 'machines/*'"); err != nil {
		return err
	}
	// Verify the complete reconstructed state, including keyed answers, custody,
	// histories and unknown JSON fields, before publishing the v2 marker.
	after, err := readQueueState(tx, fieldQueueMarker)
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return fmt.Errorf("queue downgrade verification changed durable state")
	}
	if _, err = tx.Exec("UPDATE queue SET data=? WHERE id=1", fieldQueueMarker); err != nil {
		return err
	}
	return tx.Commit()
}
