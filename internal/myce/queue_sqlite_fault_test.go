package myce

import (
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
	sqlite3 "modernc.org/sqlite/lib"
)

// The VFS override lives only in a killed helper, never the gate's shared test
// process. This intercepts SQLite's actual WAL xSync (not Go File.Sync). The
// lost/torn suffix cases model a crash image, not a physical power-cut drill.
func TestQueueSQLiteFaultRecovery(t *testing.T) {
	if mode := os.Getenv("MYCE_TEST_SQLITE_FAULT"); mode != "" {
		runQueueSQLiteFault(t, mode, os.Getenv("MYCE_TEST_SQLITE_DIR"))
		return
	}
	for _, mode := range []string{"sync-error", "lost-wal-suffix", "torn-wal-suffix", "after-sync"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestQueueSQLiteFaultRecovery$")
			cmd.Env = append(os.Environ(), "MYCE_TEST_SQLITE_FAULT="+mode, "MYCE_TEST_SQLITE_DIR="+dir)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL || !strings.Contains(string(out), "fault reached: "+mode) {
				t.Fatalf("helper did not reach kill point: %v\n%s", err, out)
			}
			q, err := Open(filepath.Join(dir, "queue.db"), dir, &fakeClock{now: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			var integrity string
			if err = q.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
				t.Fatalf("recovery integrity: %s %v", integrity, err)
			}
			if len(q.s.Jobs) != 1 {
				t.Fatalf("lost acknowledged job: %+v", q.s.Jobs)
			}
			for _, j := range q.s.Jobs {
				if j.Spec.Name != "acknowledged" || j.CancelledAt == nil {
					t.Fatalf("lost acknowledged intent/cancellation: %+v", j)
				}
			}
			pending := q.s.Machines["pending"]
			if mode == "lost-wal-suffix" || mode == "torn-wal-suffix" {
				if pending != nil {
					t.Fatal("recovered an unsynced/torn check-in")
				}
			} else if mode == "after-sync" && pending == nil {
				t.Fatal("lost the fully synced check-in")
			}
			// A sync error is an uncertain commit, never a successful reply.
			// Its disk image may contain old or new state, but never partial JSON.
			if pending != nil && pending.Session != "pending-session" {
				t.Fatalf("partial check-in: %+v", pending)
			}
			submit(t, q, spec("experiment")) // Recovery remains writable.
		})
	}
}

func runQueueSQLiteFault(t *testing.T, mode, dir string) {
	armed := false
	durableSize := int64(0)
	walPath := filepath.Join(dir, "queue.db-wal")
	installSQLiteSyncFault(t, func(next func() int32, flags int32) int32 {
		want := int32(sqlite3.SQLITE_SYNC_NORMAL)
		if runtime.GOOS == "darwin" {
			want = sqlite3.SQLITE_SYNC_FULL
		}
		if flags&0xf != want {
			panic(fmt.Sprintf("WAL sync flags=%d, want %d", flags, want))
		}
		if !armed {
			rc := next()
			if rc == sqlite3.SQLITE_OK {
				info, err := os.Stat(walPath)
				if err != nil {
					panic(err)
				}
				durableSize = info.Size()
			}
			return rc
		}
		fmt.Println("fault reached: " + mode)
		switch mode {
		case "sync-error":
			return sqlite3.SQLITE_IOERR_FSYNC
		case "lost-wal-suffix", "torn-wal-suffix":
			// Preserve every earlier successful WAL barrier. Retain either no
			// new bytes or half a frame, so recovery must reject the new tail.
			end := durableSize
			if mode == "torn-wal-suffix" {
				end += 100
			}
			if err := os.Truncate(walPath, end); err != nil {
				panic(err)
			}
		case "after-sync":
			if rc := next(); rc != sqlite3.SQLITE_OK {
				panic(rc)
			}
		default:
			panic(mode)
		}
		killSQLiteFaultHelper()
		return sqlite3.SQLITE_IOERR_FSYNC // unreachable
	})
	clock := &fakeClock{now: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	// Disable automatic checkpointing only in this tiny crash fixture so its
	// earlier synced prefix cannot be recycled while building the crash image.
	if _, err = q.db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Name = "acknowledged"
	j := submit(t, q, s)
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if durableSize == 0 {
		t.Fatal("WAL sync hook never ran")
	}
	armed = true
	m := machine("pending", "fake")
	m.Session = "pending-session"
	_, err = q.Checkin(Checkin{Machine: m})
	if mode != "sync-error" || err == nil || q.s.Machines[m.ID] != nil {
		t.Fatalf("failed sync acknowledged or retained a check-in: %v", err)
	}
	killSQLiteFaultHelper()
}

func killSQLiteFaultHelper() {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		panic(err)
	}
	select {} // No writes, Close, checkpoint, or defers after the crash point.
}

func installSQLiteSyncFault(t *testing.T, sync func(func() int32, int32) int32) {
	t.Helper()
	tls := libc.NewTLS()
	// Allocations deliberately survive until this helper is killed. SQLite's
	// generated C ABI stores function-value pointers as uintptr, as __ccgo_fp
	// does. Keep Go callbacks reachable while SQLite owns those pointers.
	original := *(*sqlite3.Tsqlite3_vfs)(unsafe.Pointer(sqlite3.Xsqlite3_vfs_find(tls, 0)))
	p := libc.Xmalloc(tls, uint64(unsafe.Sizeof(original)))
	vfs := (*sqlite3.Tsqlite3_vfs)(unsafe.Pointer(p))
	*vfs = original
	var callbacks []any
	open := func(tls *libc.TLS, v, name, file uintptr, flags int32, out uintptr) int32 {
		fn := *(*func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&original.FxOpen))
		rc := fn(tls, v, name, file, flags, out)
		if rc != sqlite3.SQLITE_OK || flags&sqlite3.SQLITE_OPEN_WAL == 0 {
			return rc
		}
		f := (*sqlite3.Tsqlite3_file)(unsafe.Pointer(file))
		methods := *(*sqlite3.Tsqlite3_io_methods)(unsafe.Pointer(f.FpMethods))
		mp := libc.Xmalloc(tls, uint64(unsafe.Sizeof(methods)))
		copy := (*sqlite3.Tsqlite3_io_methods)(unsafe.Pointer(mp))
		*copy = methods
		callback := func(tls *libc.TLS, file uintptr, flags int32) int32 {
			next := func() int32 {
				fn := *(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&methods.FxSync))
				return fn(tls, file, flags)
			}
			return sync(next, flags)
		}
		callbacks = append(callbacks, callback)
		copy.FxSync = (*[2]uintptr)(unsafe.Pointer(&callbacks[len(callbacks)-1]))[1]
		f.FpMethods = mp
		return rc
	}
	callbacks = append(callbacks, open)
	vfs.FxOpen = (*[2]uintptr)(unsafe.Pointer(&callbacks[0]))[1]
	if rc := sqlite3.Xsqlite3_vfs_register(tls, p, 1); rc != sqlite3.SQLITE_OK {
		t.Fatal(rc)
	}
	t.Cleanup(func() { runtime.KeepAlive(callbacks); runtime.KeepAlive(tls) })
}
