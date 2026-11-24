package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func assertInstallRootRefusal(t *testing.T, c ServiceConfig, source, root string) {
	t.Helper()
	home := filepath.Dir(source)
	before := stateTree(t, home)
	calls := 0
	err := Install(context.Background(), c, source, "synthetic", func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return nil, nil
	})
	var legacy *LegacyStateError
	if !errors.Is(err, ErrLegacyState) || !errors.As(err, &legacy) || !reflect.DeepEqual(legacy.Paths, []string{root}) {
		t.Errorf("expected legacy refusal for %s, got %v", root, err)
	}
	if calls != 0 {
		t.Errorf("refusal called service manager %d times", calls)
	}
	if !reflect.DeepEqual(before, stateTree(t, home)) {
		t.Error("refusal changed filesystem entries or contents")
	}
}

func TestInstallFileCachePreflight(t *testing.T) {
	for _, mode := range []string{"existing-host", "missing-host"} {
		t.Run(mode, func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			if mode == "existing-host" {
				if err := Install(context.Background(), c, source, "synthetic", fakeInstallManager(c)); err != nil {
					t.Fatal(err)
				}
			} else if err := PrepareStateRoots(c.State); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(c.State, "file-cache")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, FormatMarker)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "old-blob"), []byte("RC file-cache survivor"), 0600); err != nil {
				t.Fatal(err)
			}
			c.Heartbeat *= 2
			assertInstallRootRefusal(t, c, source, root)
		})
	}
}
