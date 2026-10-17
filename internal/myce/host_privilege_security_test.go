package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMain dispatches the real _host-worker-mutation entry before creating any
// test state. This handler exists only in the test binary; ForceChild has no CLI
// or environment switch in the product binary.
func hostProtocolTestHandler(ctx context.Context, v HostInvocation, m HostMutation) (HostMutationResult, error) {
	expected, actual := WorkerChildEnvironment(v.Account), os.Environ()
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		return HostMutationResult{}, ErrHostIdentity
	}
	if home := os.Getenv("HOME"); home != v.Account.Home {
		return HostMutationResult{}, ErrHostIdentity
	}
	if cwd, e := os.Getwd(); e != nil || cwd != "/" {
		return HostMutationResult{}, ErrHostIdentity
	}
	if m.Reason == "fail" {
		_, e := LocalMachineProbe().Command(ctx, "false")
		return HostMutationResult{}, e
	}
	s, e := changeHostIntent(v.StateDir, RealClock{}, hostIntentChange{Desired: "closed"})
	if e != nil {
		return HostMutationResult{}, e
	}
	if m.Reason == "deadline" || m.Reason == "deadline-race" {
		<-ctx.Done()
		if m.Reason == "deadline-race" {
			return HostMutationResult{HostRevision: s.HostRevision, CleanupConfirmed: true}, nil
		}
		return HostMutationResult{HostRevision: s.HostRevision}, ctx.Err()
	}
	return HostMutationResult{HostRevision: s.HostRevision}, nil
}
func realHostChildFixture(t *testing.T) (HostInvocation, HostIdentityResolver) {
	t.Helper()
	r, v := hostIdentityFixture(t)
	a, e := LookupWorkerAccount("")
	if e != nil {
		t.Fatal(e)
	}
	c := v.Config
	c.WorkerUser = a.Name
	c.WorkerHome = a.Home
	b, _ := json.Marshal(c)
	if e = os.WriteFile(c.Config, b, 0600); e != nil {
		t.Fatal(e)
	}
	r = LocalHostIdentityResolver()
	v, e = r.Resolve(a.Name, c.Config, "")
	if e != nil {
		t.Fatal(e)
	}
	v.ForceChild = true
	return v, r
}
func TestHostRealLauncherAndEntryEnvironment(t *testing.T) {
	v, _ := realHostChildFixture(t)
	t.Setenv("MYCE_TOKEN", "secret-fixture")
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "root-bus")
	t.Setenv("ROOT_ONLY_FIXTURE", "discard")
	result, e := v.Mutate(context.Background(), HostMutation{Operation: "close"}, nil, nil)
	if e != nil || result.Revision != 2 {
		t.Fatal(result, e)
	}
	result, e = v.Mutate(context.Background(), HostMutation{Operation: "close", Reason: "fail"}, nil, nil)
	if !errors.Is(e, ErrHostChild) || result.Revision != 0 {
		t.Fatal(result, e)
	}
	before := snapshotHostSecurityFiles(t, v.StateDir)
	resolved, e := v.Mutate(context.Background(), HostMutation{Operation: "resolve"}, nil, nil)
	if e != nil || resolved.StateDir != v.StateDir || resolved.Revision != 2 {
		t.Fatal(resolved, e)
	}
	result, e = v.Mutate(context.Background(), HostMutation{Operation: "status"}, nil, nil)
	if e != nil || result.Desired != "closed" || result.Revision != 2 {
		t.Fatal(result, e)
	}
	if !reflect.DeepEqual(before, snapshotHostSecurityFiles(t, v.StateDir)) {
		t.Fatal("read-only child wrote state")
	}
}
func snapshotHostSecurityFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	e := filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			st, e := d.Info()
			if e != nil {
				return e
			}
			out[path] = st.ModTime().String() + string(b)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestHostChildDeadlinePartialSurvives(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(map[bool]string{false: "child-deadline", true: "parent-sigterm"}[cancelParent], func(t *testing.T) {
			v, _ := realHostChildFixture(t)
			var result HostMutationResult
			var e error
			if !cancelParent {
				ctx, cancel := context.WithTimeout(context.Background(), 2300*time.Millisecond)
				defer cancel()
				result, e = v.Mutate(ctx, HostMutation{Operation: "close", Reason: "deadline"}, nil, nil)
			} else {
				// Trigger cancellation after the durable intent, so this tests cmd.Cancel's
				// SIGTERM path rather than the independent child deadline.
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() {
					for {
						s, err := ReadHostIntent(v.StateDir)
						if err == nil && s.Desired == "closed" {
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Millisecond):
						}
					}
				}()
				request := HostMutation{Operation: "close", Reason: "deadline", WorkerUser: v.Account.Name, ConfigPath: v.ConfigPath, StateDir: v.StateDir, Timeout: 30 * time.Second}
				b, err := LaunchHostWorkerChild(ctx, HostChildLaunch{Account: v.Account, Request: request, ForceChild: true})
				if err != nil {
					t.Fatal(err)
				}
				var response hostChildResponse
				if err = json.Unmarshal(b, &response); err != nil {
					t.Fatal(err)
				}
				result, e = response.Result, hostProtocolError(response.Code)
			}
			if !errors.Is(e, ErrHostDeadline) || !result.Partial || result.Revision != 2 || result.CleanupConfirmed {
				t.Fatal(result, e)
			}
		})
	}
}
func TestHostRootSelectionDoesNotReadFiles(t *testing.T) {
	r, v := hostIdentityFixture(t)
	r.ReadConfig = func(string) (ServiceConfig, error) { t.Fatal("root config read"); return ServiceConfig{}, nil }
	r.ValidatePath = func(string, uint32, bool) error { t.Fatal("root path stat"); return nil }
	selected, e := r.Resolve(v.Account.Name, "/not-present/config.json", "/not-present/state")
	if e != nil || !selected.Privileged || selected.Config.Role != "" {
		t.Fatal(selected, e)
	}
}
func TestHostUnsafeFileRegression(t *testing.T) {
	for _, target := range []string{"host-state.json", "config.json"} {
		for _, kind := range []string{"symlink", "fifo", "zero", "writable"} {
			t.Run(target+"/"+kind, func(t *testing.T) {
				v, _ := realHostChildFixture(t)
				path := v.ConfigPath
				if target == "host-state.json" {
					path = filepath.Join(v.StateDir, target)
				}
				// These fixtures are unique and reproducible; replace only the file created
				// by this test, preserving its valid bytes for symlink/writability cases.
				b, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.Remove(path); e != nil {
					t.Fatal(e)
				}
				switch kind {
				case "symlink":
					copy := path + ".copy"
					if e = os.WriteFile(copy, b, 0600); e == nil {
						e = os.Symlink(copy, path)
					}
				case "fifo":
					e = syscall.Mkfifo(path, 0600)
				case "zero":
					e = os.Symlink("/dev/zero", path)
				case "writable":
					e = os.WriteFile(path, b, 0666)
					if e == nil {
						e = os.Chmod(path, 0666)
					}
				}
				if e != nil {
					t.Fatal(e)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				got, e := v.Mutate(ctx, HostMutation{Operation: "status"}, nil, nil)
				if !errors.Is(e, ErrHostTarget) || got.Revision != 0 || strings.Contains(e.Error(), path) {
					t.Fatal(got, e)
				}
			})
		}
	}
}
func TestHostChildPreservesRevisionAndStateErrors(t *testing.T) {
	v, _ := realHostChildFixture(t)
	_, e := v.Mutate(context.Background(), HostMutation{Operation: "close", Expected: HostRevision{Revision: 99}}, nil, nil)
	if !errors.Is(e, ErrHostRevision) {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(v.StateDir, hostStateFile), []byte("private malformed fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	_, e = v.Mutate(context.Background(), HostMutation{Operation: "status"}, nil, nil)
	if !errors.Is(e, ErrHostState) || strings.Contains(e.Error(), "private malformed") {
		t.Fatal(e)
	}
}

func TestHostChildDeadlineRaceIsUnconfirmed(t *testing.T) {
	v, _ := realHostChildFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	result, e := v.Mutate(ctx, HostMutation{Operation: "close", Reason: "deadline-race"}, nil, nil)
	if !errors.Is(e, ErrHostDeadline) || !result.Partial || result.Revision != 2 || result.CleanupConfirmed {
		t.Fatal(result, e)
	}
}

func TestHostSystemdFailureClassification(t *testing.T) {
	failure := errors.New("mutation failed")
	s := SystemdHostService{Command: func(context.Context, string, ...string) ([]byte, error) { return nil, failure }}
	if _, e := s.call(context.Background(), "show"); !errors.Is(e, ErrHostUserManager) {
		t.Fatal(e)
	}
	if _, e := s.call(context.Background(), "stop"); !errors.Is(e, failure) || errors.Is(e, ErrHostUserManager) {
		t.Fatal(e)
	}
}

// Exercise the opened-inode checks directly too: pre-open path validation alone
// would not protect against a leaf replaced between lstat and open.
func TestHostSafeReadRejectsReplacedLeaf(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "device", "writable", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			var e error
			switch kind {
			case "symlink":
				e = os.Symlink("/dev/zero", path)
			case "fifo":
				e = syscall.Mkfifo(path, 0600)
			case "device":
				path = "/dev/zero"
			case "writable":
				e = os.WriteFile(path, []byte("{}"), 0600)
				if e == nil {
					e = os.Chmod(path, 0666)
				}
			case "oversize":
				e = os.WriteFile(path, make([]byte, 1025), 0600)
			}
			if e != nil {
				t.Fatal(e)
			}
			b, e := readHostRegular(path, uint32(os.Geteuid()), 1024)
			if !errors.Is(e, ErrHostTarget) || len(b) != 0 || strings.Contains(e.Error(), path) {
				t.Fatal(len(b), e)
			}
		})
	}
}

func TestHostAncestorModesAndACLs(t *testing.T) {
	for _, tc := range []struct {
		uid  uint32
		mode os.FileMode
		safe bool
	}{
		{0, os.ModeDir | 0755, true}, {0, os.ModeDir | 0777, false},
		{0, os.ModeDir | 0770, false}, {0, os.ModeDir | os.ModeSticky | 0777, true},
		{1001, os.ModeDir | os.ModeSticky | 0777, false}, {1001, os.ModeDir | 0700, true},
	} {
		if got := safeHostAncestor(rootStatFixture{uid: tc.uid, mode: tc.mode}, 1001); got != tc.safe {
			t.Fatal(tc, got)
		}
	}
	v, _ := realHostChildFixture(t)
	for _, denied := range []string{v.ConfigPath, filepath.Dir(v.ConfigPath), "/"} {
		calls := 0
		e := validateHostTargetLinks(v.ConfigPath, v.Account.UID, false, 0, func(path string) error {
			calls++
			if path == denied {
				return ErrHostTarget
			}
			return nil
		})
		if !errors.Is(e, ErrHostTarget) || calls == 0 {
			t.Fatal(e, calls)
		}
	}
}
func TestHostChildWaitDelayStaysWithinDeadline(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	child, cancel := hostChildLaunchContext(parent)
	defer cancel()
	outer, _ := parent.Deadline()
	inner, _ := child.Deadline()
	if inner.Add(hostChildWaitDelay) != outer {
		t.Fatal("child cancellation did not reserve WaitDelay")
	}
}
