package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCommandRecordCleanup(t *testing.T) {
	for _, inspectionFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "unconfirmed"}[inspectionFails], func(t *testing.T) {
			scope := commandScope{StateDir: t.TempDir(), Clock: RealClock{}}
			if err := scope.prepare(); err != nil {
				t.Fatal(err)
			}
			if inspectionFails {
				bin := t.TempDir()
				if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			}
			const record = "attempt-command-test.json"
			err := runRecordedCommand(context.Background(), []string{"/bin/sh", "-c", "exit 0"}, t.TempDir(), []string{"PATH=" + os.Getenv("PATH")}, io.Discard, scope, record)
			if inspectionFails {
				if err == nil || !strings.Contains(err.Error(), "inspect process groups") {
					t.Fatal("missing custody confirmation failure", err)
				}
				if _, err := os.Stat(filepath.Join(scope.StateDir, record)); err != nil {
					t.Fatal("unconfirmed custody discarded", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(scope.StateDir, record)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("confirmed command record retained", err)
				}
			}
		})
	}
}

// Failed inspection still delivers TERM, but cannot authorize KILL before an
// accelerator helper's checkpoint grace expires (#704 review).
func TestCommandAcceleratorInspectionFailurePreservesGrace(t *testing.T) {
	realPS, err := exec.LookPath("ps")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	denied := filepath.Join(dir, "deny-inspection")
	if err := os.WriteFile(filepath.Join(dir, "ps"), []byte("#!/bin/sh\nif [ -f \"$MYCE_TEST_PS_DENY\" ]; then exit 1; fi\nexec \"$MYCE_TEST_REAL_PS\" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TEST_PS_DENY", denied)
	t.Setenv("MYCE_TEST_REAL_PS", realPS)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	clock := &manualWorkerClock{now: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const record = "attempt-command-test.json"
	done := make(chan error, 1)
	finished := false
	go func() {
		done <- runRecordedCommand(ctx, []string{"sh", "-c", `trap 'touch term' TERM; echo $$ > pid; while :; do sleep .01; done`}, dir,
			[]string{"PATH=" + os.Getenv("PATH"), "MYCE_PLATFORM=xpu"}, io.Discard, commandScope{StateDir: dir, Clock: clock}, record)
	}()
	// Use the durable launch record to clean up only this fixture's group,
	// including the payload if WaitDelay has already reaped its supervisor.
	t.Cleanup(func() {
		_ = os.Remove(denied)
		clock.Advance(StopGrace)
		data, _ := os.ReadFile(filepath.Join(dir, record))
		var state workerState
		if json.Unmarshal(data, &state) == nil && state.PID > 0 {
			_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		}
		if !finished {
			select {
			case <-done:
			case <-time.After(testWaitTimeout):
				t.Error("fixture cleanup did not finish")
			}
		}
	})
	nativeFile(t, dir, "pid")
	data, err := os.ReadFile(filepath.Join(dir, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(denied, nil, 0600); err != nil {
		t.Fatal(err)
	}
	cancel()
	await(t, func() bool {
		select {
		case err := <-done:
			finished = true
			if err == nil {
				t.Fatal("unconfirmed helper discarded custody")
			}
			return true
		default:
			_, err := processBirth(pid)
			return errors.Is(err, os.ErrNotExist)
		}
	})
	data, err = os.ReadFile(filepath.Join(dir, record))
	var state workerState
	if err != nil || json.Unmarshal(data, &state) != nil || state.StopAt == nil || !clock.Now().Before(*state.StopAt) {
		t.Fatal("missing future accelerator stop deadline", err)
	}
	if _, err := processBirth(pid); err != nil {
		t.Fatal("helper killed before accelerator grace", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "term")); err != nil {
		t.Fatal("inspection failure prevented owned-group TERM", err)
	}
}
