package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"strings"
	"syscall"
	"testing"
	"time"
)

func preserveWorkerFixture(t *testing.T) (ServiceConfig, string) {
	t.Helper()
	home := t.TempDir()
	c, err := PlanInstall("worker", home, "", home, "http://queue.invalid", "fixture", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	c.HostID = "fixture-host"
	c.AllowDeviceless = true
	source := filepath.Join(home, "source")
	if err = os.WriteFile(source, []byte("first binary"), 0700); err != nil {
		t.Fatal(err)
	}
	return c, source
}
func fakeInstallManager(context.Context, string, ...string) ([]byte, error) { return nil, nil }

type inactiveInstallUnit struct{}

func (inactiveInstallUnit) Error() string { return "inactive fixture unit" }
func (inactiveInstallUnit) ExitCode() int { return 3 }

func TestInstallManagerRollback(t *testing.T) {
	for _, role := range []string{"worker", "queue"} {
		for _, active := range []bool{false, true} {
			for _, failure := range []string{"reload", "enable", "restart"} {
				t.Run(fmt.Sprintf("%s/active=%t/%s", role, active, failure), func(t *testing.T) {
					c, source := preserveWorkerFixture(t)
					if role == "queue" {
						var err error
						c, err = PlanInstall(role, c.NAS, "", c.NAS, c.Queue, c.Name, c.Detection)
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := Install(context.Background(), c, source, "fixture", fakeInstallManager); err != nil {
						t.Fatal(err)
					}
					paths := []string{c.Binary, c.Config, c.Service}
					unit := filepath.Base(c.Service)
					before, loaded := map[string][]byte{}, map[string][]byte{}
					for _, path := range paths {
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						before[path], loaded[path] = data, data
					}
					currentActive, fired, reloads := active, false, 0
					injected := errors.New("injected manager failure")
					manager := func(ctx context.Context, name string, args ...string) ([]byte, error) {
						call := strings.Join(args, " ")
						if call == "--user is-active --quiet "+unit {
							if currentActive {
								return []byte("active"), nil
							}
							return nil, inactiveInstallUnit{}
						}
						if call == "--user daemon-reload" {
							reloads++
							for _, path := range paths {
								data, err := os.ReadFile(path)
								if err != nil {
									t.Fatal(err)
								}
								loaded[path] = data
							}
						}
						if call == "--user enable --now "+unit || call == "--user restart "+unit {
							currentActive = true // Even a failing command can partially activate.
						}
						if !fired && (failure == "reload" && call == "--user daemon-reload" || failure == "enable" && call == "--user enable --now "+unit || failure == "restart" && call == "--user restart "+unit) {
							fired = true
							return nil, injected
						}
						if fired && (call == "--user restart "+unit || call == "--user stop "+unit) {
							for path, data := range before {
								if !bytes.Equal(loaded[path], data) {
									t.Fatal("restored active state before reloading restored files")
								}
							}
							currentActive = call == "--user restart "+unit
						}
						return fakeInstallManager(ctx, name, args...)
					}
					c.Heartbeat = 2 * time.Second
					if err := Install(context.Background(), c, source, "", manager); !errors.Is(err, injected) || !fired {
						t.Fatal("fault was not exercised", err)
					}
					for path, want := range before {
						got, err := os.ReadFile(path)
						if err != nil || !bytes.Equal(got, want) || !bytes.Equal(loaded[path], want) {
							t.Fatalf("disk and manager did not both restore %s", filepath.Base(path))
						}
					}
					if reloads != 2 || currentActive != active {
						t.Fatalf("rollback: reloads=%d active=%t; want 2 and %t", reloads, currentActive, active)
					}
				})
			}
		}
	}
}

func TestInstallManagerRollbackRecoveryFailure(t *testing.T) {
	for _, failure := range []string{"reload", "restart", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			if err := Install(context.Background(), c, source, "fixture", fakeInstallManager); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected, recoveryFailure := errors.New("registration failed"), errors.New("recovery failed")
			failed, stopped, recovered := false, false, false
			manager := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				call := strings.Join(args, " ")
				if call == "--user enable --now myce-worker.service" {
					failed = true
					cancel()
					return nil, injected
				}
				if failed {
					if ctx.Err() != nil {
						t.Fatal("rollback reused canceled install context")
					}
					if failure == "reload" && call == "--user daemon-reload" || failure == "restart" && call == "--user restart myce-worker.service" {
						return nil, recoveryFailure
					}
					stopped = stopped || call == "--user stop myce-worker.service"
					recovered = recovered || call == "--user restart myce-worker.service"
				}
				return fakeInstallManager(ctx, name, args...)
			}
			c.Heartbeat = 2 * time.Second
			err := Install(ctx, c, source, "", manager)
			if !errors.Is(err, injected) {
				t.Fatal("lost original failure", err)
			}
			if failure == "canceled" {
				if !recovered || stopped {
					t.Fatal("canceled install did not recover prior active worker")
				}
			} else if !errors.Is(err, recoveryFailure) || !stopped || !strings.Contains(err.Error(), "rollback incomplete") {
				t.Fatal("failed rollback did not stop worker and report recovery failure", err)
			}
		})
	}
}

func TestInstallFailureRecoveryAndFilesystems(t *testing.T) {
	for _, failure := range []string{"write", "rename", "fsync", "directory-fsync", "EXDEV", "reload", "preactivation", "enable"} {
		t.Run(failure, func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			// Deliberately separate runtime artifacts, durable config/state and units.
			c.Binary = filepath.Join(t.TempDir(), "runtime-myce")
			c.Service = filepath.Join(t.TempDir(), "myce-worker.service")
			if err := Install(context.Background(), c, source, "synthetic-fixture", fakeInstallManager); err != nil {
				t.Fatal(err)
			}
			paths := []string{c.Binary, c.Config, c.Service, c.GPUEnvironmentFile()}
			before := map[string][]byte{}
			for _, path := range paths {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				before[path] = data
			}
			custody := filepath.Join(c.State, "custody-fixture")
			os.WriteFile(custody, []byte("unique custody"), 0600)
			os.WriteFile(c.DB, []byte("queue fixture bytes"), 0600)
			os.WriteFile(source, []byte("replacement binary"), 0700)
			fired := false
			injected := errors.New("injected install failure")
			files := installFiles{before: func(op, path string) error {
				if path == c.Config && op == failure {
					fired = true
					return injected
				}
				return nil
			}, rename: func(source, destination string) error {
				if filepath.Dir(source) != filepath.Dir(destination) {
					return syscall.EXDEV
				}
				if failure == "EXDEV" && destination == c.Config {
					fired = true
					return syscall.EXDEV
				}
				return os.Rename(source, destination)
			}}
			dropInInspections := 0
			manager := func(_ context.Context, name string, args ...string) ([]byte, error) {
				call := strings.Join(args, " ")
				if strings.Contains(call, "--property=DropInPaths") {
					dropInInspections++
				}
				if call == "--user is-active --quiet myce-worker.service" {
					return nil, inactiveInstallUnit{}
				}

				if failure == "reload" && strings.Contains(call, "daemon-reload") || failure == "preactivation" && strings.Contains(call, "--property=DropInPaths") && dropInInspections == 2 || failure == "enable" && call == "--user enable --now myce-worker.service" {
					fired = true
					return nil, injected
				}

				return nil, nil
			}
			changed := c
			changed.Nice = 0
			if err := installWithFiles(context.Background(), changed, source, "", manager, files); err == nil || !fired {
				t.Fatal("fault was not exercised", err)
			}
			for path, want := range before {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("original artifact not restored: %s", filepath.Base(path))
				}
			}
			if got, _ := os.ReadFile(custody); string(got) != "unique custody" {
				t.Fatal("custody changed")
			}
			if got, _ := os.ReadFile(c.DB); string(got) != "queue fixture bytes" {
				t.Fatal("queue bytes changed")
			}
			for _, path := range []string{c.Binary, c.Config} {
				backups, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".Trash", "*-"+filepath.Base(path)))
				found := false
				for _, backup := range backups {
					data, _ := os.ReadFile(backup)
					found = found || bytes.Equal(data, before[path])
				}
				if !found {
					t.Fatal("missing recoverable original", filepath.Base(path))
				}
			}
		})
	}
	t.Run("same-directory-renames", func(t *testing.T) {
		c, source := preserveWorkerFixture(t)
		c.Binary = filepath.Join(t.TempDir(), "runtime-myce")
		c.Service = filepath.Join(t.TempDir(), "myce-worker.service")
		renames := map[string]bool{}
		files := installFiles{rename: func(source, destination string) error {
			renames[destination] = true
			if filepath.Dir(source) != filepath.Dir(destination) {
				return syscall.EXDEV
			}
			return os.Rename(source, destination)
		}}
		if err := installWithFiles(context.Background(), c, source, "synthetic-fixture", fakeInstallManager, files); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{c.Binary, c.Config, c.Credentials, c.GPUEnvironmentFile(), c.Service} {
			if !renames[path] {
				t.Fatal("artifact publication was not exercised", filepath.Base(path))
			}
		}
	})
}

func TestInstallDropInsPreservedAndConflictsBlock(t *testing.T) {
	for _, body := range []string{"[Service]\nCPUQuota=45%\n", "[Service]\nExecStart=\nExecStart=/usr/bin/true\n", "[Service]\nEnvironment=MYCE_TOKEN=other\n"} {
		t.Run(strings.Fields(body)[1], func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			path := filepath.Join(t.TempDir(), "override.conf")
			os.WriteFile(path, []byte(body), 0600)
			enabled := false
			manager := func(_ context.Context, _ string, args ...string) ([]byte, error) {
				if strings.Contains(strings.Join(args, " "), "--property=DropInPaths") {
					return []byte(path), nil
				}
				if strings.Contains(strings.Join(args, " "), " enable ") {
					enabled = true
				}
				return nil, nil
			}
			err := Install(context.Background(), c, source, "synthetic-fixture", manager)
			safe := strings.Contains(body, "CPUQuota=")
			if (err == nil) != safe || enabled != safe {
				t.Fatal("wrong service-contract decision", err)
			}
			if data, _ := os.ReadFile(path); !bytes.Equal(data, []byte(body)) {
				t.Fatal("consumer drop-in changed")
			}
		})
	}
}

func TestServiceConfigVersionAndSanitizedDiff(t *testing.T) {
	c, _ := preserveWorkerFixture(t)
	changed := c
	changed.EnvironmentFile = "/fixture/secret-reference"
	changed.Path = "/fixture/private-command-path"
	changed.Nice = 0
	diff := InstallConfigDiff(c, changed)
	for _, key := range []string{"environment_file: changed", "path: changed", "nice: changed"} {
		if !strings.Contains(diff, key) {
			t.Fatal("missing changed field")
		}
	}
	if strings.Contains(diff, "secret-reference") || strings.Contains(diff, "private-command-path") || strings.Contains(diff, "/fixture/") {
		t.Fatal("diff exposed a value")
	}
	os.MkdirAll(filepath.Dir(c.Config), 0700)
	for _, data := range []string{`{"version":2}`, `{"version":-1}`, `{"version":1,"unrecognized_setting":true}`} {
		if err := os.WriteFile(c.Config, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadServiceConfig(c.Config); err == nil {
			t.Fatal("unsupported config was accepted")
		}
	}
}

func TestServiceConfigRetiredHostSettingsOverlay(t *testing.T) {
	c, _ := preserveWorkerFixture(t)
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var saved map[string]json.RawMessage
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	saved["host_state_dir"] = json.RawMessage(`"/fixture/retired-control"`)
	saved["start_disabled"] = json.RawMessage(`true`)
	saved["required_workers"] = json.RawMessage(`["gpu0","gpu1"]`)
	data, err = json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadServiceConfig(c.Config)
	if err != nil || !reflect.DeepEqual(got, c) {
		t.Fatal("saved settings lost during retired-field overlay", err)
	}
	current, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"host_state_dir", "start_disabled", "required_workers"} {
		if bytes.Contains(current, []byte(key)) {
			t.Fatal("retired setting written", key)
		}
	}
}
