package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	modernsqlite "modernc.org/sqlite"
)

var rowsLeakFunctionID atomic.Uint64

func TestCanceledQueryDoesNotLeakRowsOrPinWAL(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("database file descriptor counting is implemented on darwin and linux")
	}

	const attempts = 64
	path := filepath.Join(t.TempDir(), "rows-leak.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(attempts)
	db.SetMaxIdleConns(0)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE test (value INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}
	baselineFDs := databaseFileDescriptorCount(t, path)

	queryCtx, cancelQueries := context.WithCancel(t.Context())
	entered := make(chan struct{}, attempts)
	functionName := fmt.Sprintf("mycelium_rows_leak_race_%d", rowsLeakFunctionID.Add(1))
	modernsqlite.MustRegisterDeterministicScalarFunction(
		functionName,
		0,
		func(*modernsqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			entered <- struct{}{}
			<-queryCtx.Done()
			return int64(42), nil
		},
	)

	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := db.QueryContext(queryCtx, "SELECT "+functionName+"()")
			if rows != nil {
				if closeErr := rows.Close(); err == nil {
					err = closeErr
				}
			}
			errs <- err
		}()
	}

	waitCtx, stopWaiting := context.WithTimeout(t.Context(), 10*time.Second)
	defer stopWaiting()
	for i := 0; i < attempts; i++ {
		select {
		case <-entered:
		case <-waitCtx.Done():
			cancelQueries()
			t.Fatalf("only %d of %d queries reached the first-row race: %v", i, attempts, waitCtx.Err())
		}
	}
	cancelQueries()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-waitCtx.Done():
		t.Fatalf("canceled queries did not return: %v", waitCtx.Err())
	}
	close(errs)
	canceledResults := 0
	for err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "interrupted (9)") {
			canceledResults++
			continue
		}
		t.Errorf("canceled query returned unexpected error: %v", err)
	}
	if canceledResults == 0 {
		t.Error("none of the queries observed cancellation")
	}

	afterFDs := databaseFileDescriptorCount(t, path)
	if afterFDs > baselineFDs+3 {
		t.Errorf("database file descriptors grew from %d to %d after %d canceled queries", baselineFDs, afterFDs, attempts)
	}

	var busy, logFrames, checkpointedFrames int
	if err := db.QueryRowContext(t.Context(), `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		t.Fatalf("truncate WAL checkpoint: %v", err)
	}
	if busy != 0 {
		t.Errorf("truncate WAL checkpoint remained busy: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointedFrames)
	}
}

func databaseFileDescriptorCount(t *testing.T, databasePath string) int {
	t.Helper()

	switch runtime.GOOS {
	case "darwin":
		return darwinDatabaseFileDescriptorCount(t, databasePath)
	case "linux":
		return linuxDatabaseFileDescriptorCount(t, databasePath)
	default:
		t.Fatalf("unsupported platform %s", runtime.GOOS)
		return 0
	}
}

func darwinDatabaseFileDescriptorCount(t *testing.T, databasePath string) int {
	t.Helper()

	count := 0
	for _, path := range []string{databasePath, databasePath + "-wal", databasePath + "-shm"} {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat database file %s: %v", path, err)
		}
		realPath, err := filepath.EvalSymlinks(path)
		if err != nil {
			t.Fatalf("resolve database file %s: %v", path, err)
		}
		output, err := exec.Command(
			"/usr/sbin/lsof",
			"-nP",
			"-a",
			"-p", strconv.Itoa(os.Getpid()),
			"-Fn",
			"--", realPath,
		).Output()
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("count descriptors for %s: %v", path, err)
			}
		}
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "n") {
				count++
			}
		}
	}
	return count
}

func linuxDatabaseFileDescriptorCount(t *testing.T, databasePath string) int {
	t.Helper()

	var targets []os.FileInfo
	for _, path := range []string{databasePath, databasePath + "-wal", databasePath + "-shm"} {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat database file %s: %v", path, err)
		}
		targets = append(targets, info)
	}

	const descriptorDirectory = "/proc/self/fd"
	entries, err := os.ReadDir(descriptorDirectory)
	if err != nil {
		t.Fatalf("read process file descriptors: %v", err)
	}

	count := 0
	for _, entry := range entries {
		descriptor, err := os.Open(filepath.Join(descriptorDirectory, entry.Name()))
		if err != nil {
			continue
		}
		info, statErr := descriptor.Stat()
		closeErr := descriptor.Close()
		if statErr != nil || closeErr != nil {
			continue
		}
		for _, target := range targets {
			if os.SameFile(info, target) {
				count++
				break
			}
		}
	}
	return count
}
