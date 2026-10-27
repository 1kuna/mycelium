package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeIsolationRuntimeLayout(t *testing.T) {
	runtimeRoot, state := t.TempDir(), t.TempDir()
	credential := filepath.Join(runtimeRoot, "installation", "joining.json")
	w := Worker{ServiceConfig: &ServiceConfig{JoiningFile: credential}}
	if err := w.validateNativeReadGrants([]string{runtimeRoot}, state); err == nil || !strings.Contains(err.Error(), "runtime_overlap") {
		t.Fatal("runtime root exposing worker credential was accepted", err)
	}
	// A supported /opt or /usr installation must be refused before payload exec,
	// even when its state/credential leaves do not yet exist.
	for _, root := range []string{"/usr", "/opt"} {
		if err := (Worker{}).validateNativeReadGrants([]string{root}, filepath.Join(root, "mycelium", "state")); err == nil {
			t.Fatal("broad runtime grant accepted installed state", root)
		}
	}
	alias := filepath.Join(t.TempDir(), "runtime")
	if err := os.Symlink(runtimeRoot, alias); err != nil {
		t.Fatal(err)
	}
	w.ServiceConfig.JoiningFile = filepath.Join(alias, "future", "joining.json")
	if err := w.validateNativeReadGrants([]string{runtimeRoot}, state); err == nil {
		t.Fatal("symlinked absent credential leaf bypassed validation")
	}
	prepared := filepath.Join(state, "environment-cache")
	if err := os.Mkdir(prepared, 0700); err != nil {
		t.Fatal(err)
	}
	if err := (Worker{}).validateNativeReadGrants([]string{prepared}, state); err != nil {
		t.Fatal("isolated prepared cache rejected", err)
	}
}
