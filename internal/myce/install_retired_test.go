package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallRetiresObsoleteHostUnits(t *testing.T) {
	for _, failure := range []string{"", "stop-timer", "disable-service", "retire-timer", "retire-service", "retire-dropin", "activate"} {
		t.Run(failure, func(t *testing.T) {
			f := newInstallMatrixFixture(t, "linux", "worker", 3)
			unitDir := filepath.Dir(f.c.Service)
			obsolete := []string{"myce-host-reconcile.timer", "myce-host-reconcile.service"}
			links := map[string]string{}
			// The fake owns manager state and enablement links; no host systemctl runs.
			for _, name := range obsolete {
				path := filepath.Join(unitDir, name)
				data := []byte("old " + name)
				if err := os.WriteFile(path, data, 0640); err != nil {
					t.Fatal(err)
				}
				f.files[path], f.modes[path] = data, 0640
				state := installUnitState{enabled: "enabled", active: true}
				f.states[name], f.manager.states[name] = state, state
				f.cached[name], f.manager.cached[name] = data, data
				wants := "default.target.wants"
				if strings.HasSuffix(name, ".timer") {
					wants = "timers.target.wants"
				}
				link := filepath.Join(unitDir, wants, name)
				if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("../"+name, link); err != nil {
					t.Fatal(err)
				}
				links[name] = link
			}
			dropin := filepath.Join(f.c.Service+".d", "10-host-check.conf")
			if err := os.MkdirAll(filepath.Dir(dropin), 0700); err != nil {
				t.Fatal(err)
			}
			data := []byte("[Service]\nExecStartPre=myce _host-start-check --config fixture\n")
			if err := os.WriteFile(dropin, data, 0640); err != nil {
				t.Fatal(err)
			}
			f.files[dropin], f.modes[dropin] = data, 0640
			sibling := filepath.Join(unitDir, "myce-worker-other.service")
			if err := os.WriteFile(sibling, []byte("unrelated worker"), 0600); err != nil {
				t.Fatal(err)
			}
			fired := false
			fail := func(point string) error {
				if !fired && failure == point {
					fired = true
					return errors.New("retirement fault")
				}
				return nil
			}
			runner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				out, err := f.manager.run(ctx, name, args...)
				if name != "systemctl" || err != nil {
					return out, err
				}
				action, unit := args[1], args[len(args)-1]
				if action == "daemon-reload" {
					for _, n := range obsolete {
						s := f.manager.states[n]
						if f.manager.cached[n] == nil {
							s.enabled = "not-found"
						} else if s.enabled == "not-found" {
							s.enabled = "disabled"
						}
						f.manager.states[n] = s
					}
				}
				if link := links[unit]; link != "" {
					if action == "disable" {
						if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
							t.Fatal(err)
						}
					}
					if action == "enable" {
						if _, err := os.Lstat(link); errors.Is(err, os.ErrNotExist) {
							if err := os.Symlink("../"+unit, link); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				point := ""
				if action == "stop" && unit == obsolete[0] {
					point = "stop-timer"
				}
				if action == "disable" && unit == obsolete[1] {
					point = "disable-service"
				}
				if action == "enable" && unit == filepath.Base(f.c.Service) {
					point = "activate"
				}
				if point != "" {
					err = fail(point)
				}
				return out, err
			}
			files := installFiles{before: func(op, path string) error {
				if op != "retire" {
					return nil
				}
				point := "retire-dropin"
				if filepath.Base(path) == obsolete[0] {
					point = "retire-timer"
				}
				if filepath.Base(path) == obsolete[1] {
					point = "retire-service"
				}
				return fail(point)
			}}
			err := installWithFiles(context.Background(), f.c, f.source, "fixture", runner, files)
			if failure != "" {
				if err == nil || !fired {
					t.Fatal("fault not exercised", err)
				}
				f.assertRestored(t)
				for name, link := range links {
					target, err := os.Readlink(link)
					if err != nil || target != "../"+name {
						t.Fatal("enablement not restored", name, err)
					}
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{filepath.Join(unitDir, obsolete[0]), filepath.Join(unitDir, obsolete[1]), dropin, links[obsolete[0]], links[obsolete[1]]} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("obsolete artifact retained", filepath.Base(path), err)
					}
				}
				for _, name := range obsolete {
					s := f.manager.states[name]
					if s.active || s.enabled != "not-found" {
						t.Fatal("obsolete unit not stopped/disabled", name, s)
					}
				}
			}
			if b, err := os.ReadFile(sibling); err != nil || string(b) != "unrelated worker" {
				t.Fatal("sibling modified")
			}
		})
	}
}
