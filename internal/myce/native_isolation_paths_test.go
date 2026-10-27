//go:build linux || darwin

package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeIsolationReadGrantPrivateSubtrees(t *testing.T) {
	home, nas := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	state := filepath.Join(home, ".local/share/mycelium/worker/state")
	w := Worker{JobFileRoot: nas, CheckpointRoot: filepath.Join(nas, "checkpoints"), SecretRoot: filepath.Join(state, "secrets"), ServiceConfig: &ServiceConfig{State: state}}
	for _, private := range []string{
		filepath.Join(state, "job-files/sibling/1/.tmp/secret"),
		filepath.Join(state, "file-checkpoints/sibling/attempt-1/save"),
		filepath.Join(w.SecretRoot, "nested/credential"),
		filepath.Join(w.CheckpointRoot, "sibling/attempt-1/save"),
	} {
		if err := os.MkdirAll(filepath.Dir(private), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(private, []byte("synthetic private bytes"), 0600); err != nil {
			t.Fatal(err)
		}
		for _, grant := range []string{filepath.Dir(private), private} {
			if err := w.validateNativeReadGrants([]string{grant}, state); err == nil || !strings.Contains(err.Error(), "runtime_overlap") {
				t.Error("read grant inside private content accepted", grant, err)
			}
		}
	}
	// Default installed caches live below both the worker spool and install
	// root; NAS models likewise live below the general job-file storage root.
	for _, grant := range []string{filepath.Join(state, "model-cache"), filepath.Join(state, "environment-cache"), filepath.Join(nas, "models")} {
		if err := os.MkdirAll(grant, 0700); err != nil {
			t.Fatal(err)
		}
		if err := w.validateNativeReadGrants([]string{grant}, state); err != nil {
			t.Error("permitted cache/model subtree refused", grant, err)
		}
	}
}
