package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallPolicyPreflight(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		for _, tc := range []struct{ name, file, data, key, classification string }{
			{"retired", "hosts.yaml", "hosts:\n  fixture:\n    production_idle: 10s\n", "production_idle", "retired"},
			{"unknown host", "hosts.yaml", "hosts:\n  fixture:\n    typo: true\n", "typo", "unknown"},
			{"unknown project", "projects.yaml", "fixture:\n  weight: 1\n  typo: true\n", "typo", "unknown"},
			{"valid", "", "", "", ""},
		} {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				f := newInstallMatrixFixture(t, "linux", role, 3)
				directory := filepath.Dir(f.source)
				f.c.HostConfig = filepath.Join(directory, "hosts.yaml")
				f.c.ProjectConfig = filepath.Join(directory, "projects.yaml")
				for path, data := range map[string]string{f.c.HostConfig: "hosts:\n  fixture:\n    production_hold: true\n", f.c.ProjectConfig: "fixture:\n  weight: 1\n"} {
					if filepath.Base(path) == tc.file {
						data = tc.data
					}
					if err := os.WriteFile(path, []byte(data), 0600); err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				err := Install(context.Background(), f.c, f.source, "fixture", func(ctx context.Context, name string, args ...string) ([]byte, error) {
					calls++
					return f.manager.run(ctx, name, args...)
				})
				if tc.key == "" {
					if err != nil {
						t.Fatal(err)
					}
					if calls == 0 {
						t.Fatal("valid policy did not install")
					}
					return
				}
				if err == nil {
					t.Errorf("invalid policy installed")
				} else {
					for _, want := range []string{tc.file, tc.key, "line 3", tc.classification} {
						if !strings.Contains(err.Error(), want) {
							t.Errorf("missing %q in %v", want, err)
						}
					}
				}
				if calls != 0 {
					t.Errorf("invalid policy reached service manager: %d calls", calls)
				}
				f.assertRestored(t)
			})
		}
	}
}
