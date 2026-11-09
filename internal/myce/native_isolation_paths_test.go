//go:build linux || darwin

package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeIsolationDeclaredRoots(t *testing.T) {
	home, nas, install := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	spool := filepath.Join(install, "worker", "state")
	base := Worker{JobFileRoot: nas, ServiceConfig: &ServiceConfig{Config: filepath.Join(install, "worker", "config.json"), NAS: nas}}
	private := filepath.Join(spool, "job-files", "other", "1", ".scratch")
	models, runtime := filepath.Join(nas, "models"), filepath.Join(home, "runtime", "bin")
	for _, path := range []string{private, models, runtime} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	alias := filepath.Join(home, "private-alias")
	if err := os.Symlink(private, alias); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{private, alias} {
		for _, executable := range []bool{false, true} {
			w := base
			if executable {
				w.NativeExecuteRoots = []string{path}
			} else {
				w.NativeReadRoots = []string{path}
			}
			if _, _, err := w.nativeDeclaredRoots(spool); err == nil {
				t.Errorf("unsafe operator grant accepted: path=%s execute=%t", path, executable)
			}
		}
	}
	base.CheckpointRoot = filepath.Join(nas, "checkpoints")
	base.SecretRoot = filepath.Join(spool, "secrets")
	for _, private := range []string{filepath.Join(spool, "file-checkpoints", "sibling/attempt-1/save"), filepath.Join(base.CheckpointRoot, "sibling/attempt-1/save"), filepath.Join(base.SecretRoot, "nested/credential")} {
		if err := os.MkdirAll(filepath.Dir(private), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(private, []byte("synthetic private bytes"), 0600); err != nil {
			t.Fatal(err)
		}
		w := base
		w.NativeReadRoots = []string{private}
		if _, _, err := w.nativeDeclaredRoots(spool); err == nil || !strings.Contains(err.Error(), "runtime_overlap") {
			t.Error("read grant inside private content accepted", private, err)
		}
	}
	base.NativeReadRoots, base.NativeExecuteRoots = []string{models}, []string{runtime}
	read, execute, err := base.nativeDeclaredRoots(spool)
	if err != nil || len(read) != 1 || len(execute) != 1 {
		t.Fatalf("narrow model/runtime roots refused: read=%v execute=%v err=%v", read, execute, err)
	}
	if err := nativeReadOnlyOverlap(read, execute); err != nil {
		t.Fatal("separate model/runtime trees overlap", err)
	}
	if err := nativeReadOnlyOverlap(read, []string{filepath.Join(models, "runtime")}); err == nil {
		t.Error("nested executable grant defeated read-only declaration")
	}
}
