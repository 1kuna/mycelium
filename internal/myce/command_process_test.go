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
