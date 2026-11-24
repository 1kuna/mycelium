package myce

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStateRootInvalidMarker(t *testing.T) {
	for _, data := range []string{"", "v2\n", "v4\n", "v3\nextra"} {
		t.Run(data, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, FormatMarker), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, root)
			if err := PrepareStateRoots(root); !errors.Is(err, ErrLegacyState) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, stateTree(t, root)) {
				t.Fatal("invalid marker changed")
			}
		})
	}
}

func newTestStateRoot(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	if err := PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestDevStartupRefusalNoWrites(t *testing.T) {
	for _, cache := range []string{"environment-cache", "model-cache", "file-cache"} {
		t.Run(cache, func(t *testing.T) {
			parent := newTestStateRoot(t)
			worker := filepath.Join(parent, "workers", "fake-1")
			if err := PrepareStateRoots(worker); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(worker, cache)
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "arbitrary"), []byte("surviving data"), 0600); err != nil {
				t.Fatal(err)
			}
			before := stateTree(t, parent)
			d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Database: filepath.Join(parent, "queue.db"), Checkpoints: filepath.Join(parent, "checkpoints"), Workers: 1, MemoryGB: 8, Token: "synthetic-fixture"})
			if d != nil {
				d.Close()
			}
			if !errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), root) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, stateTree(t, parent)) {
				t.Fatal("dev refusal changed filesystem")
			}
		})
	}
}
