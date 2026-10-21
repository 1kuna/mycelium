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
	"testing"
	"time"
)

type installTestExit int

func (e installTestExit) Error() string { return fmt.Sprintf("fixture exit %d", e) }
func (e installTestExit) ExitCode() int { return int(e) }

type installMatrixManager struct {
	t      *testing.T
	c      ServiceConfig
	states map[string]installUnitState
	cached map[string][]byte
	event  func(string) error
}

func (m *installMatrixManager) read(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		m.t.Fatal(err)
	}
	return b
}
func (m *installMatrixManager) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "launchctl" && len(args) > 0 {
		if args[0] == "bootstrap" && filepath.Base(args[len(args)-1]) == "probe.plist" {
			if err := completeLaunchdProbe(m.t, args[len(args)-1], m.c); err != nil {
				return nil, err
			}
			if m.event != nil {
				return nil, m.event("launchd-probe bootstrap")
			}
			return nil, nil
		}
		if args[0] == "bootout" && strings.Contains(args[len(args)-1], "local.mycelium.preflight.") {
			if m.event != nil {
				return nil, m.event("launchd-probe bootout")
			}
			return nil, nil
		}
	}
	call := name + " " + strings.Join(args, " ")
	var out []byte
	var result error
	if name == "systemctl" {
		action := args[1]
		unit := args[len(args)-1]
		s := m.states[unit]
		switch action {
		case "is-active":
			if !s.active {
				result = installTestExit(3)
			}
		case "daemon-reload":
			for _, path := range installUnitPaths(m.c) {
				unit := filepath.Base(path)
				m.cached[unit] = m.read(path)
				state := m.states[unit]
				if m.cached[unit] == nil {
					state.enabled = "not-found"
				} else if state.enabled == "not-found" || state.enabled == "" {
					state.enabled = "disabled"
				}
				m.states[unit] = state
			}
		case "enable":
			s.enabled = "enabled"
			if len(args) > 3 && args[2] == "--now" {
				s.active = true
			}
			m.states[unit] = s
		case "disable":
			if m.cached[unit] == nil {
				return []byte("Failed to disable unit: Unit file does not exist"), installTestExit(1)
			}
			s.enabled = "disabled"
			if m.cached[unit] == nil {
				s.enabled = "not-found"
			}
			m.states[unit] = s
		case "restart":
			s.active = true
			m.states[unit] = s
		case "stop":
			s.active = false
			m.states[unit] = s
		case "show":
			if strings.Contains(call, "--property=UnitFileState") {
				unit = args[2]
				out = []byte(m.states[unit].enabled)
			} // Other show calls find no consumer drop-ins.
		default:
			m.t.Fatalf("unexpected manager operation: %s", call)
		}
	} else if name == "launchctl" {
		target := args[len(args)-1]
		unit := filepath.Base(target)
		if !strings.HasSuffix(unit, ".plist") {
			unit += ".plist"
		}
		s := m.states[unit]
		switch args[0] {
		case "print":
			if !s.loaded {
				result = installTestExit(113)
			} else if s.active {
				out = []byte("state = running\npid = 42\n")
			} else {
				out = []byte("state = not running\n")
			}
		case "bootout":
			s.loaded, s.active = false, false
			m.states[unit] = s
			delete(m.cached, unit)
		case "bootstrap":
			data := m.read(target)
			if data == nil {
				result = errors.New("missing fixture plist")
			} else {
				s.loaded, s.active = true, true
				m.states[unit] = s
				m.cached[unit] = data
			}
		case "kickstart":
			if !s.loaded {
				result = installTestExit(113)
			} else {
				s.active = true
				m.states[unit] = s
			}
		case "kill":
			s.active = false
			m.states[unit] = s
		default:
			m.t.Fatalf("unexpected manager operation: %s", call)
		}
	} else if name == "systemd-run" {
		out, _ = json.Marshal(m.c.Detection)
	} else if name == "cat" && len(args) == 1 && args[0] == "/proc/mounts" {
		out = []byte("fixture " + m.c.NAS + " nfs rw 0 0\n")
	} else {
		m.t.Fatalf("unexpected live command: %s", name)
	}
	// Inject after the operation, so errors can have already changed manager state.
	if m.event != nil {
		if err := m.event(call); err != nil {
			return nil, err
		}
	}
	return out, result
}

type installMatrixFixture struct {
	c       ServiceConfig
	source  string
	manager *installMatrixManager
	files   map[string][]byte
	modes   map[string]os.FileMode
	states  map[string]installUnitState
	cached  map[string][]byte
}

func newInstallMatrixFixture(t *testing.T, platform, role string, prior int) installMatrixFixture {
	t.Helper()
	home := t.TempDir()
	c, err := PlanInstall(role, home, "", home, "http://queue.invalid", "fixture", Detection{OS: platform, Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	c.HostID = "fixture-host"
	c.RequireMountedNAS = platform == "linux"
	if role == "worker" {
		if err := PrepareStateRoots(c.State); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(home, "fixture-source")
	if err := os.WriteFile(source, []byte("new binary"), 0700); err != nil {
		t.Fatal(err)
	}
	m := &installMatrixManager{t: t, c: c, states: map[string]installUnitState{}, cached: map[string][]byte{}}
	paths := append([]string{c.Binary, c.Config, c.Credentials}, installUnitPaths(c)...)
	if platform == "linux" && role == "worker" {
		paths = append(paths, c.GPUEnvironmentFile())
	}
	f := installMatrixFixture{c: c, source: source, manager: m, files: map[string][]byte{}, modes: map[string]os.FileMode{}, states: map[string]installUnitState{}, cached: map[string][]byte{}}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if prior >= 0 {
			data := []byte("old " + filepath.Base(path))
			if path == c.Config {
				data, _ = json.Marshal(c)
			}
			if path == c.Service {
				data = []byte(c.ServiceText())
			}
			if err := os.WriteFile(path, data, 0640); err != nil {
				t.Fatal(err)
			}
			f.files[path], f.modes[path] = data, 0640
		} else {
			f.files[path] = nil
		}
	}
	// Fresh workers exercise invitation publication; fresh queues exercise the
	// credential-file alternative. Both use only synthetic local fixtures.
	if role == "worker" && prior < 0 {
		c.JoiningFile = filepath.Join(home, "fixture-invitation.json")
		invitation := Invitation{Version: 1, Fingerprint: strings.Repeat("ab", 32), Token: "synthetic-fixture", Address: "https://queue.invalid"}
		if err := WriteInvitation(c.JoiningFile, invitation); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(c.JoiningFile)
		if err != nil {
			t.Fatal(err)
		}
		f.files[c.JoiningFile], f.modes[c.JoiningFile] = data, 0600
		f.files[filepath.Join(filepath.Dir(c.Config), "joining.json")] = nil
		f.c = c
		m.c = c
	}
	for i, path := range installUnitPaths(c) {
		unit := filepath.Base(path)
		s := installUnitState{}
		if platform == "linux" {
			s.enabled = "not-found"
			if prior >= 0 {
				// Rotate the four enabled/active combinations between units, so snapshots
				// cannot accidentally use another unit's state.
				pattern := (prior + i) % 4
				s.enabled = "disabled"
				if pattern&1 != 0 {
					s.enabled = "enabled"
				}
				s.active = pattern&2 != 0
			}
			m.cached[unit] = append([]byte(nil), f.files[path]...)
		} else if prior >= 0 {
			s.loaded = prior != 0
			s.active = prior == 2
			if s.loaded {
				m.cached[unit] = append([]byte(nil), f.files[path]...)
			}
		}
		m.states[unit], f.states[unit] = s, s
	}
	for k, v := range m.cached {
		f.cached[k] = append([]byte(nil), v...)
	}
	return f
}
func (f installMatrixFixture) assertFiles(t *testing.T) {
	t.Helper()
	for path, want := range f.files {
		got, err := os.ReadFile(path)
		if want == nil {
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("new artifact retained: %s", filepath.Base(path))
			}
			continue
		}
		st, statErr := os.Stat(path)
		if err != nil || statErr != nil || !bytes.Equal(got, want) || st.Mode().Perm() != f.modes[path] {
			t.Fatalf("file/mode not restored: %s", filepath.Base(path))
		}
	}
}
func (f installMatrixFixture) assertRestored(t *testing.T) {
	t.Helper()
	f.assertFiles(t)
	if !reflect.DeepEqual(f.states, f.manager.states) {
		t.Fatalf("manager state differs: got %+v want %+v", f.manager.states, f.states)
	}
	for k, want := range f.cached {
		if !bytes.Equal(want, f.manager.cached[k]) {
			t.Fatalf("cached definition differs: %s", k)
		}
	}
	for k, got := range f.manager.cached {
		if !bytes.Equal(got, f.cached[k]) {
			t.Fatalf("new cached definition retained: %s", k)
		}
	}
	if f.c.Role == "worker" {
	}
}

// Each successful trace defines its own complete failure matrix. A newly added
// publication/manager operation automatically adds a fault case. No fixed step
// count or hand-maintained list can silently omit a future operation.
func TestInstallSnapshotFailureMatrix(t *testing.T) {
	cases := 0
	for _, platform := range []string{"linux", "darwin"} {
		states := 4
		if platform == "darwin" {
			states = 3
		}
		for _, role := range []string{"queue", "worker"} {
			for prior := -1; prior < states; prior++ {
				label := fmt.Sprintf("%s/%s/prior=%d", platform, role, prior)
				baseline := newInstallMatrixFixture(t, platform, role, prior)
				trace := []string{}
				baseline.manager.event = func(event string) error { trace = append(trace, event); return nil }
				files := installFiles{before: func(op, path string) error { trace = append(trace, op+" "+filepath.Base(path)); return nil }}
				if err := installWithFiles(context.Background(), baseline.c, baseline.source, "fixture", baseline.manager.run, files); err != nil {
					t.Fatalf("%s baseline: %v", label, err)
				}
				for step := range trace {
					cases++
					t.Run(fmt.Sprintf("%s/step=%02d", label, step), func(t *testing.T) {
						f := newInstallMatrixFixture(t, platform, role, prior)
						count := 0
						reloadSeen := false
						fired := false
						injected := errors.New("injected snapshot matrix failure")
						event := func(event string) error {
							if fired {
								f.assertRecoveryOrder(t, event, &reloadSeen)
							}
							n := count
							count++
							if n == step {
								fired = true
								return injected
							}
							return nil
						}
						f.manager.event = event
						files := installFiles{before: func(op, path string) error { return event(op + " " + filepath.Base(path)) }}
						changed := f.c
						changed.Heartbeat = 2 * time.Second
						err := installWithFiles(context.Background(), changed, f.source, "fixture", f.manager.run, files)
						// Drop-in diagnostics deliberately sanitize the underlying manager error.
						if err == nil || !fired {
							t.Fatalf("fault not exercised: %v", err)
						}
						f.assertRestored(t)
					})
				}
			}
		}
	}
	t.Logf("failure matrix cases: %d", cases)
}

func TestInstallSnapshotRecoveryFailureMatrix(t *testing.T) {
	cases := 0
	for _, platform := range []string{"linux", "darwin"} {
		states := 4
		if platform == "darwin" {
			states = 3
		}
		for _, role := range []string{"queue", "worker"} {
			for prior := -1; prior < states; prior++ {
				label := fmt.Sprintf("%s/%s/prior=%d", platform, role, prior)
				baseline := newInstallMatrixFixture(t, platform, role, prior)
				forward := 0
				baseline.manager.event = func(string) error { forward++; return nil }
				files := installFiles{before: func(string, string) error { forward++; return nil }}
				if err := installWithFiles(context.Background(), baseline.c, baseline.source, "fixture", baseline.manager.run, files); err != nil {
					t.Fatal(err)
				}
				// A failure at the last forward operation has exercised every publication
				// and every manager mutation before recovery starts.
				probe := newInstallMatrixFixture(t, platform, role, prior)
				n, recoverySteps := 0, 0
				primary := errors.New("injected final install failure")
				event := func(string) error {
					n++
					if n == forward {
						return primary
					}
					if n > forward {
						recoverySteps++
					}
					return nil
				}
				probe.manager.event = event
				files = installFiles{before: func(op, path string) error { return event(op + " " + filepath.Base(path)) }}
				if err := installWithFiles(context.Background(), probe.c, probe.source, "fixture", probe.manager.run, files); !errors.Is(err, primary) {
					t.Fatalf("%s primary fault not hit: %v", label, err)
				}
				probe.assertRestored(t)
				for step := 0; step < recoverySteps; step++ {
					cases++
					t.Run(fmt.Sprintf("%s/recovery=%02d", label, step), func(t *testing.T) {
						f := newInstallMatrixFixture(t, platform, role, prior)
						n := 0
						fired := false
						recoveryFailure := errors.New("injected recovery failure")
						event := func(string) error {
							n++
							if n == forward {
								return primary
							}
							if n == forward+step+1 {
								fired = true
								return recoveryFailure
							}
							return nil
						}
						f.manager.event = event
						files := installFiles{before: func(op, path string) error { return event(op + " " + filepath.Base(path)) }}
						err := installWithFiles(context.Background(), f.c, f.source, "fixture", f.manager.run, files)
						if !errors.Is(err, primary) || !fired || !strings.Contains(err.Error(), "rollback incomplete") {
							t.Fatalf("recovery failure not reported: %v", err)
						}
						// Verify the diagnostic against the fake's actual final state, including
						// changes caused by fail-closed stopping after an incomplete rollback.
						for _, path := range installUnitPaths(f.c) {
							unit := filepath.Base(path)
							fields := []string{}
							data, readErr := os.ReadFile(path)
							st, statErr := os.Stat(path)
							want := f.files[path]
							matches := want == nil && errors.Is(readErr, os.ErrNotExist) || want != nil && readErr == nil && statErr == nil && bytes.Equal(want, data) && st.Mode().Perm() == f.modes[path]
							if !matches {
								fields = append(fields, "file")
							}
							got, wantState := f.manager.states[unit], f.states[unit]
							if got.enabled != wantState.enabled {
								fields = append(fields, "enabled")
							}
							if got.loaded != wantState.loaded {
								fields = append(fields, "loaded")
							}
							if got.active != wantState.active {
								fields = append(fields, "active")
							}
							for _, field := range []string{"file", "enabled", "loaded", "active"} {
								reported := false
								for _, chunk := range strings.Split(strings.SplitN(err.Error(), "snapshot differences: ", 2)[1], "; ") {
									if strings.HasPrefix(chunk, unit+": ") {
										details := strings.SplitN(strings.TrimPrefix(chunk, unit+": "), ": ", 2)[0]
										for _, value := range strings.Split(details, ",") {
											reported = reported || value == field
										}
									}
								}
								different := false
								for _, value := range fields {
									different = different || value == field
								}
								if reported != different {
									t.Fatalf("inaccurate difference for %s/%s: %v", unit, field, err)
								}
							}
						}
						if role == "worker" {
						}
					})
				}
			}
		}
	}
	t.Logf("recovery-failure matrix cases: %d", cases)
}

func (f installMatrixFixture) assertRecoveryOrder(t *testing.T, event string, reloaded *bool) {
	t.Helper()
	if event == "systemctl --user daemon-reload" || strings.HasPrefix(event, "launchctl bootout ") || strings.HasPrefix(event, "launchctl print ") && !f.manager.states[filepath.Base(f.c.Service)].loaded {
		f.assertFiles(t)
		*reloaded = true
	}
	registration := strings.HasPrefix(event, "systemctl --user enable ") || strings.HasPrefix(event, "systemctl --user disable ") || strings.HasPrefix(event, "launchctl bootstrap ")
	activity := strings.HasPrefix(event, "systemctl --user restart ") || strings.HasPrefix(event, "systemctl --user stop ") || strings.HasPrefix(event, "launchctl kickstart ") || strings.HasPrefix(event, "launchctl kill ")
	if (registration || activity) && !*reloaded {
		t.Fatal("restored manager state before restored definitions were reloaded")
	}
	if activity {
		for unit, want := range f.states {
			got := f.manager.states[unit]
			if got.enabled != want.enabled || got.loaded != want.loaded {
				t.Fatal("restored activity before all registrations were restored")
			}
		}
	}
}

func TestInstallSnapshotUnknownRecoveryStaysStopped(t *testing.T) {
	for _, platform := range []string{"linux", "darwin"} {
		t.Run(platform, func(t *testing.T) {
			f := newInstallMatrixFixture(t, platform, "worker", 2)
			primary := errors.New("injected publication failure")
			restoreFailure := errors.New("injected restore failure")
			recovery := false
			f.manager.event = func(event string) error {
				if recovery && (strings.Contains(event, "--property=UnitFileState") || strings.Contains(event, " is-active ") || strings.HasPrefix(event, "launchctl print ")) {
					return restoreFailure
				}
				return nil
			}
			files := installFiles{before: func(op, path string) error {
				if op == "directory-fsync" && path == f.c.Service {
					recovery = true
					return primary
				}
				if op == "restore" && path == f.c.Binary {
					return restoreFailure
				}
				return nil
			}}
			err := installWithFiles(context.Background(), f.c, f.source, "fixture", f.manager.run, files)
			if !errors.Is(err, primary) || !errors.Is(err, restoreFailure) || !strings.Contains(err.Error(), "manager state unknown") {
				t.Fatal("uncertain recovery was not reported", err)
			}
			for unit, state := range f.manager.states {
				if state.active || !strings.Contains(err.Error(), unit) {
					t.Fatal("uncertain worker was not stopped/reported", unit)
				}
			}
		})
	}
}
