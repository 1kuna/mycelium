package myce

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const rootFixtureClaim = "synthetic-claim-00000"
const rootFixtureFront = -17

func openQueueRootFixture(t *testing.T, dir string) *Queue {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	return q
}

func seedQueueRootFixture(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.update(func() error {
		seedRetainedInputClaims(q, 64)
		q.s.EfficiencyHealth = make(map[string]efficiencyBaseline, 80)
		for i := range 80 {
			q.s.EfficiencyHealth[fmt.Sprintf("baseline-%04d", i)] = efficiencyBaseline{Observed: true, At: q.clock.Now(), History: alertSeries{1, 2, 3}}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"file_input_claims", "efficiency_health"} {
		key := fieldPrefix("state") + base64.RawURLEncoding.EncodeToString([]byte(field))
		var size int
		if err := q.db.QueryRow("SELECT length(data) FROM queue_state WHERE key=?", key).Scan(&size); err != nil || size <= q.persistencePageBytes {
			t.Fatalf("fixture field %s is not isolated: size=%d err=%v", field, size, err)
		}
	}
}

func replaceQueueRootFixture(q *Queue) error {
	return q.update(func() error {
		q.s.Front = rootFixtureFront
		delete(q.editInputClaims(), rootFixtureClaim)
		q.s.EfficiencyHealth = nil
		return nil
	})
}

func assertQueueRootFixture(t *testing.T, q *Queue, updated bool) {
	t.Helper()
	var integrity string
	if err := q.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("SQLite integrity=%q: %v", integrity, err)
	}
	var disk state
	if err := json.Unmarshal(durableQueueRecord(t, q.db, "state"), &disk); err != nil {
		t.Fatal(err)
	}
	view, err := readGet(q.publishedReads.Load().root, "state").decode("state")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []state{q.s, disk, *view.(*state)} {
		_, originalClaim := s.FileInputClaims[rootFixtureClaim]
		wantClaims, wantBaselines, wantFront := 64, 80, int64(0)
		if updated {
			wantClaims, wantBaselines, wantFront = 63, 0, rootFixtureFront
		}
		if s.Front != wantFront || originalClaim == updated || len(s.FileInputClaims) != wantClaims || len(s.EfficiencyHealth) != wantBaselines {
			t.Fatalf("mixed or missing root fragments: updated=%t front=%d original=%t claims=%d baselines=%d", updated, s.Front, originalClaim, len(s.FileInputClaims), len(s.EfficiencyHealth))
		}
	}
}

func TestQueueRootKeepsPinnedAttachments(t *testing.T) {
	q := openQueueRootFixture(t, t.TempDir())
	seedQueueRootFixture(t, q)
	old := q.publishedReads.Load()
	before, err := readGet(old.root, "state").decode("state")
	if err != nil {
		t.Fatal(err)
	}
	if err := replaceQueueRootFixture(q); err != nil {
		t.Fatal(err)
	}
	assertQueueRootFixture(t, q, true)
	pinned := before.(*state)
	if _, ok := pinned.FileInputClaims[rootFixtureClaim]; !ok || len(pinned.EfficiencyHealth) != 80 {
		t.Fatal("committing new fragments changed an older read generation")
	}
	if err := q.update(func() error {
		clear(q.editInputClaims())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var splitRows int
	if err := q.db.QueryRow("SELECT COUNT(*) FROM queue_state WHERE key GLOB ?", fieldPrefix("state")+"*").Scan(&splitRows); err != nil || splitRows != 0 {
		t.Fatalf("shrunken root still has %d split rows: %v", splitRows, err)
	}
	var compact state
	if err := json.Unmarshal(durableQueueRecord(t, q.db, "state"), &compact); err != nil || compact.Front != rootFixtureFront || len(compact.FileInputClaims) != 0 {
		t.Fatalf("wrong compact root after clearing the last fragment: %v", err)
	}
}

func TestQueueRootFragmentCrash(t *testing.T) {
	if phase := os.Getenv("MYCE_TEST_QUEUE_ROOT_CRASH"); phase != "" {
		runQueueRootFragmentCrash(t, os.Getenv("MYCE_TEST_QUEUE_ROOT_DIR"), phase)
		return
	}
	for _, test := range []struct {
		phase   string
		updated bool
	}{
		{"before-first-row", false},
		{"between-rows", false},
		{"lost-wal-tail", false},
		{"torn-wal-tail", false},
		{"after-wal-sync", true},
		{"after-commit", true},
	} {
		t.Run(test.phase, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestQueueRootFragmentCrash$")
			child.Env = append(os.Environ(), "MYCE_TEST_QUEUE_ROOT_CRASH="+test.phase, "MYCE_TEST_QUEUE_ROOT_DIR="+dir)
			out, err := child.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL || !strings.Contains(string(out), "queue root crash: "+test.phase) {
				t.Fatalf("fixture missed %s: %v\n%s", test.phase, err, out)
			}
			q := openQueueRootFixture(t, dir)
			assertQueueRootFixture(t, q, test.updated)
			if err := replaceQueueRootFixture(q); err != nil {
				t.Fatal(err)
			}
			assertQueueRootFixture(t, q, true)
		})
	}
}

func runQueueRootFragmentCrash(t *testing.T, dir, phase string) {
	t.Helper()
	crash := func() {
		fmt.Println("queue root crash: " + phase)
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			panic(err)
		}
		select {}
	}
	if phase == "before-first-row" || phase == "between-rows" {
		rows := 0
		sqlite.MustRegisterScalarFunction("myce_root_crash", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			rows++
			if rows == 1 && phase == "before-first-row" || rows == 2 && phase == "between-rows" {
				crash()
			}
			return nil, nil
		})
	}
	var walSize int64
	wal := filepath.Join(dir, "queue.db-wal")
	if phase == "lost-wal-tail" || phase == "torn-wal-tail" || phase == "after-wal-sync" {
		installQueueRootSyncHook(t, func(sync func() int32) int32 {
			if walSize == 0 {
				return sync()
			}
			if phase == "after-wal-sync" {
				if rc := sync(); rc != sqlite3.SQLITE_OK {
					panic(fmt.Sprintf("WAL sync returned %d", rc))
				}
			} else {
				end := walSize
				if phase == "torn-wal-tail" {
					end += 100 // Keep part of the next WAL frame.
				}
				if err := os.Truncate(wal, end); err != nil {
					panic(err)
				}
			}
			crash()
			return sqlite3.SQLITE_IOERR_FSYNC
		})
	}
	q := openQueueRootFixture(t, dir)
	// Preserve the durable prefix of this private WAL for lost/torn images.
	if _, err := q.db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	seedQueueRootFixture(t, q)
	if phase == "before-first-row" || phase == "between-rows" {
		if _, err := q.db.Exec("CREATE TEMP TRIGGER root_crash BEFORE INSERT ON queue_state WHEN NEW.key GLOB '" + fieldPrefix("state") + "*' BEGIN SELECT myce_root_crash(); END"); err != nil {
			t.Fatal(err)
		}
	}
	if phase == "after-commit" {
		q.observePersistence = func(_ queuePersistenceTiming) { crash() }
	}
	info, err := os.Stat(wal)
	if err != nil {
		t.Fatal(err)
	}
	walSize = info.Size()
	if err := replaceQueueRootFixture(q); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("update returned before crash phase %s", phase)
}

// This VFS shim lives only in the killed subprocess. It intercepts the same
// WAL xSync that SQLite calls for a FULL commit, independent of Go file I/O.
func installQueueRootSyncHook(t *testing.T, hook func(func() int32) int32) {
	t.Helper()
	tls := libc.NewTLS()
	original := readRootFaultMemory[sqlite3.Tsqlite3_vfs](sqlite3.Xsqlite3_vfs_find(tls, 0))
	vfs := original
	var callbacks []any
	keep := func(fn any) uintptr {
		callbacks = append(callbacks, fn)
		return (*[2]uintptr)(unsafe.Pointer(&callbacks[len(callbacks)-1]))[1]
	}
	vfs.FxOpen = keep(func(tls *libc.TLS, vfs, name, file uintptr, flags int32, out uintptr) int32 {
		open := *(*func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&original.FxOpen))
		rc := open(tls, vfs, name, file, flags, out)
		if rc != sqlite3.SQLITE_OK || flags&sqlite3.SQLITE_OPEN_WAL == 0 {
			return rc
		}
		f := readRootFaultMemory[sqlite3.Tsqlite3_file](file)
		methods := readRootFaultMemory[sqlite3.Tsqlite3_io_methods](f.FpMethods)
		wrapped := methods
		wrapped.FxSync = keep(func(tls *libc.TLS, file uintptr, flags int32) int32 {
			sync := *(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&methods.FxSync))
			return hook(func() int32 { return sync(tls, file, flags) })
		})
		f.FpMethods = libc.Xmalloc(tls, uint64(unsafe.Sizeof(wrapped)))
		writeRootFaultMemory(f.FpMethods, &wrapped)
		writeRootFaultMemory(file, &f)
		return rc
	})
	ptr := libc.Xmalloc(tls, uint64(unsafe.Sizeof(vfs)))
	writeRootFaultMemory(ptr, &vfs)
	if rc := sqlite3.Xsqlite3_vfs_register(tls, ptr, 1); rc != sqlite3.SQLITE_OK {
		t.Fatal(rc)
	}
	t.Cleanup(func() { runtime.KeepAlive(callbacks); runtime.KeepAlive(tls) })
}

// SQLite's generated ABI represents its C-managed memory as numeric addresses.
func readRootFaultMemory[T any](address uintptr) T {
	var value T
	size := int(unsafe.Sizeof(value))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&value)), size), libc.GoBytes(address, size))
	return value
}

func writeRootFaultMemory[T any](address uintptr, value *T) {
	size := int(unsafe.Sizeof(*value))
	copy(libc.GoBytes(address, size), unsafe.Slice((*byte)(unsafe.Pointer(value)), size))
}

// The same source can be overlaid into the tagged predecessor to seed and
// read back a private database with its actual SQLite writer and loader.
func TestQueueRootUpgradeProcess(t *testing.T) {
	mode := os.Getenv("MYCE_TEST_QUEUE_ROOT_UPGRADE")
	if mode == "" {
		t.Skip("run with an actual predecessor and shared fixture directory")
	}
	q := openQueueRootFixture(t, os.Getenv("MYCE_TEST_QUEUE_ROOT_DIR"))
	switch mode {
	case "seed":
		seedQueueRootFixture(t, q)
		assertQueueRootFixture(t, q, false)
	case "upgrade":
		assertQueueRootFixture(t, q, false)
		if err := replaceQueueRootFixture(q); err != nil {
			t.Fatal(err)
		}
		assertQueueRootFixture(t, q, true)
	case "readback":
		assertQueueRootFixture(t, q, true)
	default:
		t.Fatalf("unknown queue-root fixture mode %q", mode)
	}
}
