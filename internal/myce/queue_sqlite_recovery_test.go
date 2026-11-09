package myce

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

const sqliteRecoveryPayload = "durable-checkpoint-and-migration-payload/"

func sqliteRecoveryFixture(t *testing.T) (*Queue, *fakeClock, string, Spec) {
	t.Helper()
	q, clock := fixture(t)
	s := spec("experiment")
	s.Name, s.SubmissionKey = "acknowledged", "sqlite-recovery"
	// Force real split fields and multiple checkpoint pages, not just a marker.
	s.Run.Env = map[string]string{"PAYLOAD": strings.Repeat(sqliteRecoveryPayload, 2048)}
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	return q, clock, j.ID, s
}

func sqliteKilledHelper(t *testing.T, test, mode, dir string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+test+"$")
	cmd.Env = append(os.Environ(), "MYCE_TEST_SQLITE_RECOVERY="+mode, "MYCE_TEST_SQLITE_DIR="+dir)
	out, err := cmd.CombinedOutput()
	// Reuse the SIGKILL check from the commit fixtures, not just a nonzero exit.
	assertSQLiteKilled(t, err, out, mode)
	return string(out)
}

func sqliteRecoveryImage(t *testing.T, db *sql.DB, marker []byte, jobID string) state {
	t.Helper()
	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("recovery integrity: %s %v", integrity, err)
	}
	var actual []byte
	if err := db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&actual); err != nil || !bytes.Equal(actual, marker) {
		t.Fatalf("recovery marker: %s want %s: %v", actual, marker, err)
	}
	var fields int
	if err := db.QueryRow("SELECT count(*) FROM queue_state WHERE key GLOB 'fields/*'").Scan(&fields); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(marker, legacyNormalizedQueueMarker) && fields != 0 || bytes.Equal(marker, normalizedQueueMarker) && fields == 0 {
		t.Fatalf("mixed marker/fragment generation: %s with %d fields", marker, fields)
	}
	data, err := readQueueState(db, actual)
	if err != nil {
		t.Fatal(err)
	}
	var state state
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	j := state.Jobs[jobID]
	if len(state.Jobs) != 1 || len(state.Submissions) != 1 || j == nil || j.CancelledAt == nil || j.Spec.Run.Env["PAYLOAD"] != strings.Repeat(sqliteRecoveryPayload, 2048) {
		t.Fatal("lost acknowledged job, cancellation, keyed answer or payload fragment")
	}
	assertQueueSQLiteSettings(t, db)
	return state
}

func sqliteRecoveryWritable(t *testing.T, path, jobID string, s Spec, clock *fakeClock) {
	t.Helper()
	q, err := Open(path, filepath.Dir(path), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	sqliteRecoveryImage(t, q.db, normalizedQueueMarker, jobID)
	if got := submit(t, q, s); got.ID != jobID || got.CancelledAt == nil {
		t.Fatal("recovery lost keyed cancellation")
	}
	submit(t, q, spec("experiment"))
}

func assertSQLiteSyncPolicy(kind string, flags int64) {
	want := int64(sqlite3.SQLITE_SYNC_NORMAL)
	if runtime.GOOS == "darwin" {
		want = sqlite3.SQLITE_SYNC_FULL
	}
	if flags&0xf != want {
		panic(fmt.Sprintf("%s sync flags=%d, want %d", kind, flags, want))
	}
}

func TestQueueSQLiteCheckpointFaultRecovery(t *testing.T) {
	if mode := os.Getenv("MYCE_TEST_SQLITE_RECOVERY"); mode != "" {
		runSQLiteCheckpointFault(t, mode, os.Getenv("MYCE_TEST_SQLITE_DIR"))
		return
	}
	for _, mode := range []string{"database-page", "database-sync", "wal-recycle"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, id, s := sqliteRecoveryFixture(t)
			path := strings.TrimSuffix(q.lock.Name(), ".lock")
			q.Close()
			out := sqliteKilledHelper(t, "TestQueueSQLiteCheckpointFaultRecovery", mode, filepath.Dir(path))
			for _, kind := range []string{"database", "wal"} {
				if !strings.Contains(out, "checkpoint sync policy: "+kind) {
					t.Fatalf("missing %s sync evidence: %s", kind, out)
				}
				// A page-write kill precedes the main-file barrier; its priming
				// checkpoint proves that policy. Later kills observe both again.
				if (kind == "wal" || mode != "database-page") && !strings.Contains(out, "armed checkpoint sync: "+kind) {
					t.Fatalf("missing faulting checkpoint %s barrier: %s", kind, out)
				}
			}
			db := openQueueSQLite(path, false)
			image := sqliteRecoveryImage(t, db, normalizedQueueMarker, id)
			if image.Jobs[id].Reason != strings.Repeat("checkpoint-page/", 2048) {
				t.Fatal("checkpoint lost the last acknowledged fragmented update")
			}
			db.Close()
			sqliteRecoveryWritable(t, path, id, s, clock)
		})
	}
}

func runSQLiteCheckpointFault(t *testing.T, mode, dir string) {
	armed := false
	installSQLiteFileFault(t, func(kind, operation string, next func() int32, value int64) int32 {
		if operation == "sync" {
			assertSQLiteSyncPolicy(kind, value)
			fmt.Println("checkpoint sync policy: " + kind)
			if armed {
				fmt.Println("armed checkpoint sync: " + kind)
			}
		}
		rc := next()
		if rc == sqlite3.SQLITE_OK && armed && (kind == "database" && operation == "write" && mode == "database-page" || kind == "database" && operation == "sync" && mode == "database-sync" || kind == "wal" && operation == "truncate" && value == 0 && mode == "wal-recycle") {
			fmt.Println("fault reached: " + mode)
			killSQLiteFaultHelper()
		}
		return rc
	})
	q, err := openConversionQueue(ServiceConfig{DB: filepath.Join(dir, "queue.db"), Checkpoints: dir}, &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	// Prime one real checkpoint so even a page-write kill has observed both
	// files' sync policy. The next acknowledged change dirties multiple pages.
	if _, err = q.db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	if err = q.update(func() error {
		for id := range q.s.Jobs {
			q.touchJob(id).Reason = "checkpoint base"
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = q.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if err = q.update(func() error {
		for id := range q.s.Jobs {
			q.touchJob(id).Reason = strings.Repeat("checkpoint-page/", 2048)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	armed = true
	if _, err = q.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	t.Fatal("checkpoint fault not reached")
}

func TestQueueSQLiteMigrationFaultRecovery(t *testing.T) {
	if mode := os.Getenv("MYCE_TEST_SQLITE_RECOVERY"); mode != "" {
		dir := os.Getenv("MYCE_TEST_SQLITE_DIR")
		armed := false
		var synced int64
		installSQLiteSyncFault(t, func(next func() int32, flags int32) int32 {
			assertSQLiteSyncPolicy("wal", int64(flags))
			path := filepath.Join(dir, "queue.db-wal")
			info, err := os.Stat(path)
			if err != nil {
				panic(err)
			}
			// A new WAL syncs its header before writing transaction frames.
			// Target the frame/commit barrier, not that header-only sync.
			if armed && info.Size() > 32 {
				if mode == "migration-before-sync" {
					if err = os.Truncate(path, synced); err != nil {
						panic(err)
					}
				} else if rc := next(); rc != sqlite3.SQLITE_OK {
					panic(rc)
				}
				fmt.Println("fault reached: " + mode)
				killSQLiteFaultHelper()
			}
			rc := next()
			if rc == sqlite3.SQLITE_OK {
				synced = info.Size()
			}
			return rc
		})
		q, err := openConversionQueue(ServiceConfig{DB: filepath.Join(dir, "queue.db"), Checkpoints: dir}, &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatal(err)
		}
		armed = true
		if err = q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		t.Fatal("migration fault not reached")
		return
	}
	for _, mode := range []string{"migration-before-sync", "migration-after-sync"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, id, s := sqliteRecoveryFixture(t)
			path := strings.TrimSuffix(q.lock.Name(), ".lock")
			writeLegacyQueueFixture(t, q)
			q.Close()
			sqliteKilledHelper(t, "TestQueueSQLiteMigrationFaultRecovery", mode, filepath.Dir(path))
			db := openQueueSQLite(path, false)
			want := normalizedQueueMarker
			if mode == "migration-before-sync" {
				want = legacyNormalizedQueueMarker
			}
			// Inspect BEFORE Open's startup migration can hide a mixed image.
			sqliteRecoveryImage(t, db, want, id)
			db.Close()
			sqliteRecoveryWritable(t, path, id, s, clock)
		})
	}
}

// These are DB-only rehearsals of the documented stopped-state copy/hash
// procedure, not a new production backup API or complete root-set qualification.
func TestQueueSQLiteBackupRecovery(t *testing.T) {
	if mode := os.Getenv("MYCE_TEST_SQLITE_RECOVERY"); mode != "" {
		runSQLiteBackupFault(t, mode, os.Getenv("MYCE_TEST_SQLITE_DIR"))
		return
	}
	for _, mode := range []string{"backup-copy", "backup-publish", "backup-complete", "restore-copy", "restore-complete"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, id, s := sqliteRecoveryFixture(t)
			path := strings.TrimSuffix(q.lock.Name(), ".lock")
			q.Close() // Stopped source: Close checkpoints WAL before copying.
			dir := filepath.Dir(path)
			sqliteKilledHelper(t, "TestQueueSQLiteBackupRecovery", mode, dir)
			generation := filepath.Join(dir, "generation")
			image, err := readSQLiteRecoveryGeneration(generation)
			if mode == "backup-copy" || mode == "backup-publish" {
				if err == nil {
					t.Fatal("accepted an incomplete backup generation")
				}
				// The unchanged stopped source remains the last good generation.
				sqliteRecoveryWritable(t, path, id, s, clock)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			restored := filepath.Join(dir, "restored")
			if mode == "restore-copy" || mode == "restore-complete" {
				restoredImage, restoreErr := readSQLiteRecoveryGeneration(restored)
				if (mode == "restore-copy") != (restoreErr != nil) {
					t.Fatalf("restore generation acceptance: %v", restoreErr)
				}
				if mode == "restore-complete" && !bytes.Equal(restoredImage, image) {
					t.Fatal("completed restore differs from verified backup")
				}
			}
			// Repeat an interrupted restore from the verified backup, without
			// touching the good source or opening a partial DB as an empty queue.
			if err = os.MkdirAll(restored, 0700); err != nil {
				t.Fatal(err)
			}
			restoredDB := filepath.Join(restored, "queue.db")
			if mode != "restore-complete" {
				if err = os.WriteFile(restoredDB, image, 0600); err != nil {
					t.Fatal(err)
				}
			}
			db := openQueueSQLite(restoredDB, false)
			sqliteRecoveryImage(t, db, normalizedQueueMarker, id)
			db.Close()
			sqliteRecoveryWritable(t, restoredDB, id, s, clock)
			if mode == "restore-complete" {
				// SQL integrity alone cannot detect a missing logical field root.
				// The runtime must also reject a marker with incomplete fragments.
				db := openQueueSQLite(restoredDB, false)
				deleted, err := db.Exec("DELETE FROM queue_state WHERE key=?", fieldPrefix("jobs/"+id))
				if err != nil {
					t.Fatal(err)
				}
				if n, err := deleted.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("fixture did not delete exactly one field root: %d %v", n, err)
				}
				var integrity string
				if err = db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
					t.Fatalf("logical corruption fixture is not SQL-integrity-valid: %s %v", integrity, err)
				}
				db.Close()
				broken, err := Open(restoredDB, restored, clock)
				if broken != nil {
					broken.Close()
					t.Fatal("opened an incomplete fragment generation")
				}
				if err == nil || !strings.Contains(err.Error(), "queue field root missing") {
					t.Fatalf("missing fragment rejection: %v", err)
				}
			}
		})
	}
}

func readSQLiteRecoveryGeneration(dir string) ([]byte, error) {
	manifest, err := os.ReadFile(filepath.Join(dir, "queue.db.sha256"))
	if err != nil {
		return nil, err
	}
	image, err := os.ReadFile(filepath.Join(dir, "queue.db"))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(manifest)) != fmt.Sprintf("%x", sha256.Sum256(image)) {
		return nil, fmt.Errorf("incomplete recovery generation: database hash mismatch")
	}
	return image, nil
}

func runSQLiteBackupFault(t *testing.T, mode, dir string) {
	image, err := os.ReadFile(filepath.Join(dir, "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	copyGeneration := func(name, stage string) {
		dest := filepath.Join(dir, name)
		if err := os.Mkdir(dest, 0700); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(dest, "queue.db"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(image[:len(image)/2]); err != nil {
			t.Fatal(err)
		}
		if mode == stage+"-copy" {
			// Simulate an interrupted copy even if a manifest was pre-copied.
			if err = os.WriteFile(filepath.Join(dest, "queue.db.sha256"), []byte(fmt.Sprintf("%x\n", sha256.Sum256(image))), 0600); err != nil {
				t.Fatal(err)
			}
			fmt.Println("fault reached: " + mode)
			killSQLiteFaultHelper()
		}
		if _, err = file.Write(image[len(image)/2:]); err != nil {
			t.Fatal(err)
		}
		if err = file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
		if mode == stage+"-publish" {
			fmt.Println("fault reached: " + mode)
			killSQLiteFaultHelper()
		}
		manifest, err := os.OpenFile(filepath.Join(dest, "queue.db.sha256"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = fmt.Fprintf(manifest, "%x\n", sha256.Sum256(image)); err != nil {
			t.Fatal(err)
		}
		if err = manifest.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = manifest.Close(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{dest, dir} {
			directory, err := os.Open(name)
			if err != nil {
				t.Fatal(err)
			}
			if err = directory.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = directory.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	copyGeneration("generation", "backup")
	if strings.HasPrefix(mode, "restore-") {
		image, err = readSQLiteRecoveryGeneration(filepath.Join(dir, "generation"))
		if err != nil {
			t.Fatal(err)
		}
		copyGeneration("restored", "restore")
	}
	fmt.Println("fault reached: " + mode)
	killSQLiteFaultHelper()
}
