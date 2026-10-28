package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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

func TestNativeIsolationModelRoots(t *testing.T) {
	state, work := t.TempDir(), t.TempDir()
	cmd := exec.Command("/bin/true")
	cmd.Dir = work
	for _, configured := range []bool{false, true} {
		w := Worker{ModelRoot: filepath.Join(t.TempDir(), "models")}
		cache := filepath.Join(state, "model-cache")
		if configured {
			cache = filepath.Join(t.TempDir(), "cache")
			w.CacheRoot = cache
		}
		policy, err := w.nativeLinuxPolicy(cmd, state, work, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{cache, w.ModelRoot} {
			if !slices.Contains(policy.ReadOnly, path) || slices.Contains(policy.Read, path) || slices.Contains(policy.Write, path) {
				t.Fatal("operator model root lacks read-only content access", path)
			}
		}
		w.ServiceConfig = &ServiceConfig{JoiningFile: filepath.Join(cache, "joining.json")}
		if _, err := w.nativeLinuxPolicy(cmd, state, work, nil, nil); err == nil || !strings.Contains(err.Error(), "runtime_overlap") {
			t.Fatal("model cache containing worker authority accepted", err)
		}
	}
}
