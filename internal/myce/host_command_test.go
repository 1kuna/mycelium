package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostCommandRecordCleanup(t *testing.T) {
	for _, inspectionFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "unconfirmed"}[inspectionFails], func(t *testing.T) {
			scope := hostCommandScope{HostDir: readyTestHost(t, t.TempDir()), Clock: RealClock{}}
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
			err := runRecordedHostCommand(context.Background(), []string{"/bin/sh", "-c", "exit 0"}, t.TempDir(), []string{"PATH=" + os.Getenv("PATH")}, io.Discard, scope, record)
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

func TestHostCommandsDoNotRegisterWorkers(t *testing.T) {
	dir := readyTestHost(t, t.TempDir())
	scope := hostCommandScope{HostDir: dir}
	if err := scope.prepare(); err != nil {
		t.Fatal(err)
	}
	workers, err := HostWorkers(dir)
	if err != nil || len(workers) != 0 {
		t.Fatal("command custody registered as a worker", workers, err)
	}
	if err := runExtensionCommand(context.Background(), []string{"/bin/sh", "-c", "exit 0"}, t.TempDir(), nil, io.Discard, scope); err != nil {
		t.Fatal(err)
	}
	workers, err = HostWorkers(dir)
	if err != nil || len(workers) != 0 {
		t.Fatal("completed command registered as a worker", workers, err)
	}
	queues, err := (HostControl{Observe: testHostExclusion, Dir: dir}).Queues()
	if err != nil || len(queues) != 0 {
		t.Fatal("command created queue custody", queues, err)
	}
}
