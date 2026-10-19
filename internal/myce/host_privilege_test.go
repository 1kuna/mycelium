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

func hostIdentityFixture(t *testing.T) (HostIdentityResolver, HostInvocation) {
	t.Helper()
	home, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	state := filepath.Join(home, "host")
	spool := filepath.Join(home, "spool")
	for _, p := range []string{state, spool} {
		if e = os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	testHostOpen(t, state, RealClock{})
	a := WorkerAccount{Name: "worker", UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Groups: []uint32{10, 20}, Home: home}
	c := ServiceConfig{Role: "worker", HostID: "test-host", WorkerUser: a.Name, WorkerHome: home, State: spool, HostStateDir: state, Config: filepath.Join(home, "config.json"), Service: filepath.Join(home, "myce-worker.service")}
	b, _ := json.Marshal(c)
	if e = os.WriteFile(c.Config, b, 0600); e != nil {
		t.Fatal(e)
	}
	r := HostIdentityResolver{EUID: 0, OS: "linux", Lookup: func(name string) (WorkerAccount, error) {
		if name != a.Name {
			return WorkerAccount{}, ErrHostIdentity
		}
		return a, nil
	}, ReadConfig: ReadServiceConfig, ValidatePath: validateHostTarget}
	r.EUID = int(a.UID)
	v, e := r.Resolve(a.Name, c.Config, "")
	if e != nil {
		t.Fatal(e)
	}
	r.EUID = 0
	v.Privileged = true
	return r, v
}
func childProtocolLaunch(t *testing.T, r HostIdentityResolver, handler HostMutationHandler) HostChildLauncher {
	t.Helper()
	return func(ctx context.Context, s HostChildLaunch) ([]byte, error) {
		if !reflect.DeepEqual(s.Env, WorkerChildEnvironment(s.Account)) {
			t.Fatal("environment selection")
		}
		b, _ := json.Marshal(s.Request)
		var out bytes.Buffer
		r.EUID = int(s.Account.UID)
		if e := ServeHostWorkerChild(ctx, bytes.NewReader(b), &out, r, handler); e != nil {
			return nil, e
		}
		return out.Bytes(), nil
	}
}
func TestHostPrivilegeAccountEnvironmentAndWorkerWrites(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "root-secret-fixture")
	t.Setenv("HOME", "/untrusted-root-home")
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "root-bus")
	r, v := hostIdentityFixture(t)
	if !v.Privileged || !v.ObservationContext().Privileged {
		t.Fatal("missing root context")
	}
	calls := 0
	handler := func(ctx context.Context, child HostInvocation, m HostMutation) (HostMutationResult, error) {
		calls++
		if child.Privileged || child.Account.Name != v.Account.Name || child.StateDir != v.StateDir {
			t.Fatal("wrong child identity")
		}
		// Both lock creation and durable intent happen only inside the child handler.
		if _, e := os.Stat(filepath.Join(v.StateDir, "intake.lock")); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("parent created lock")
		}
		s, e := changeHostIntent(child.StateDir, RealClock{}, hostIntentChange{Desired: "closed", Grace: m.Grace})
		return HostMutationResult{HostRevision: s.HostRevision}, e
	}
	launcher := childProtocolLaunch(t, r, handler)
	result, e := v.Mutate(context.Background(), HostMutation{Operation: "close"}, func(ctx context.Context, s HostChildLaunch) ([]byte, error) {
		if !reflect.DeepEqual(s.Account, v.Account) {
			t.Fatal("wrong UID/GID/groups")
		}
		env := strings.Join(s.Env, "\n")
		if strings.Contains(env, "secret") || strings.Contains(env, "DBUS") || strings.Contains(env, "root-home") || !strings.Contains(env, "XDG_RUNTIME_DIR=/run/user/") {
			t.Fatal("unsafe environment")
		}
		return launcher(ctx, s)
	}, func(context.Context, HostInvocation, HostMutation) (HostMutationResult, error) {
		t.Fatal("parent handler called")
		return HostMutationResult{}, nil
	})
	if e != nil || result.Revision != 2 || calls != 1 {
		t.Fatal(result, e, calls)
	}
}
func TestHostPrivilegeIdentityAndTargetErrors(t *testing.T) {
	r, v := hostIdentityFixture(t)
	for _, name := range []string{"", "other"} {
		if _, e := r.Resolve(name, v.ConfigPath, ""); !errors.Is(e, ErrHostIdentity) {
			t.Fatal(e)
		}
	}
	r.EUID = int(v.Account.UID) + 1
	if _, e := r.Resolve(v.Account.Name, v.ConfigPath, ""); !errors.Is(e, ErrHostIdentity) {
		t.Fatal(e)
	}
	r.EUID = 0
	r.OS = "darwin"
	if _, e := r.Resolve(v.Account.Name, v.ConfigPath, ""); !errors.Is(e, ErrHostPrivilegeUnsupported) {
		t.Fatal(e)
	}
	r.OS = "linux"
	// Override retains the installed host and account, rather than adopting data.
	r.EUID = int(v.Account.UID)
	alt := filepath.Join(v.Account.Home, "alternate")
	if e := os.Mkdir(alt, 0700); e != nil {
		t.Fatal(e)
	}
	testHostOpen(t, alt, RealClock{})
	selected, e := r.Resolve(v.Account.Name, v.ConfigPath, alt)
	if e != nil || selected.StateDir != alt || selected.Config.State != v.Config.State {
		t.Fatal(selected, e)
	}
	s, e := ReadHostIntent(alt)
	if e != nil {
		t.Fatal(e)
	}
	s.Host = "other-host"
	if e = atomicHostJSON(alt, hostStateFile, s, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Resolve(v.Account.Name, v.ConfigPath, alt); !errors.Is(e, ErrHostIdentity) {
		t.Fatal(e)
	}
	link := filepath.Join(v.Account.Home, "link")
	if e = os.Symlink(v.StateDir, link); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Resolve(v.Account.Name, v.ConfigPath, link); !errors.Is(e, ErrHostTarget) {
		t.Fatal(e)
	}
	if e = validateHostTarget(v.StateDir, v.Account.UID+1, true); !errors.Is(e, ErrHostTarget) {
		t.Fatal("unowned", e)
	}
	if e = os.Symlink(v.ConfigPath, filepath.Join(v.StateDir, "intake.lock")); e != nil {
		t.Fatal(e)
	}
	if e = ValidateHostControlTree(v.StateDir, v.Account.UID); !errors.Is(e, ErrHostTarget) {
		t.Fatal("symlinked lock", e)
	}
}
func TestHostPrivilegeErrorsAreTyped(t *testing.T) {
	for _, err := range []error{ErrHostIdentity, ErrHostTarget, ErrHostUserManager, ErrHostChild, ErrHostPrivilegeUnsupported} {
		var typed *HostInvocationError
		if !errors.As(err, &typed) || typed.Code == "" {
			t.Fatal("untyped host error")
		}
	}
}

func TestHostPrivilegeChildFailureCannotAcceptOrLeak(t *testing.T) {
	r, v := hostIdentityFixture(t)
	for _, payload := range []string{`{}`, `null`, `{"version":1,"operation":"close","result":{}}`, `{"version":1,"operation":"open","result":{"revision":2,"operation_id":"old"}}`, `{"version":1,"operation":"close","code":"child","result":{"revision":2,"operation_id":"unaccepted"}}`, `{} {}`} {
		got, e := v.Mutate(context.Background(), HostMutation{Operation: "close"}, func(context.Context, HostChildLaunch) ([]byte, error) { return []byte(payload), nil }, nil)
		if !errors.Is(e, ErrHostChild) || got.Revision != 0 || got.CleanupConfirmed {
			t.Fatal(got, e)
		}
	}
	got, e := v.Mutate(context.Background(), HostMutation{Operation: "close"}, func(context.Context, HostChildLaunch) ([]byte, error) { return nil, errors.New("root-secret-fixture") }, nil)
	if !errors.Is(e, ErrHostChild) || strings.Contains(e.Error(), "secret") || got.Revision != 0 {
		t.Fatal(got, e)
	}
	// A failed worker operation cannot leak credentials in JSON, even when it
	// has already durably fenced the host. No accepted revision is returned.
	handler := func(context.Context, HostInvocation, HostMutation) (HostMutationResult, error) {
		return HostMutationResult{HostRevision: HostRevision{Revision: 42, Operation: "unaccepted"}, CleanupConfirmed: true}, errors.New("worker-secret-fixture")
	}
	got, e = v.Mutate(context.Background(), HostMutation{Operation: "close"}, childProtocolLaunch(t, r, handler), nil)
	if !errors.Is(e, ErrHostChild) || got.Revision != 0 || strings.Contains(e.Error(), "secret") {
		t.Fatal(got, e)
	}
}
func TestHostPrivilegeCloseBeforeManagerDiscovery(t *testing.T) {
	r, v := hostIdentityFixture(t)
	observed := false
	x := HostMutationExecutor{Probe: MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
		observed = true
		s, e := ReadHostIntent(v.StateDir)
		if e != nil || s.Desired != "closed" {
			t.Fatal("discovery before durable child close", s, e)
		}
		return nil, errors.New("manager absent with worker-secret-fixture")
	}}}
	result, e := v.Mutate(context.Background(), HostMutation{Operation: "close"}, childProtocolLaunch(t, r, x.Execute), nil)
	if !errors.Is(e, ErrHostUserManager) || !observed || result.Revision == 0 || !result.Partial || result.CleanupConfirmed {
		t.Fatal(result, e, observed)
	}
	s, e := ReadHostIntent(v.StateDir)
	if e != nil || s.Desired != "closed" {
		t.Fatal(s, e)
	}
}
func TestHostPrivilegeDevProtocolWithFakeIdentity(t *testing.T) {
	// Dev uses fake inventory and a temporary installed identity, with the real
	// launcher, child entry, command runner and scrubbed process environment.
	v, _ := realHostChildFixture(t)
	fixtureRoot := filepath.Dir(v.ConfigPath)
	d, e := StartDev(DevConfig{HostStateDir: v.StateDir, HostID: "test-host", Database: filepath.Join(fixtureRoot, "queue.db"), Checkpoints: filepath.Join(fixtureRoot, "checkpoints"), Address: "127.0.0.1:0", Token: "dev-test", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	jobSpec := spec("experiment")
	jobSpec.Run.Command = []string{"true"}
	job, e := d.Queue.Submit(jobSpec)
	if e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		snap, err := d.Queue.Snapshot()
		if err != nil {
			return false
		}
		for _, got := range snap.Jobs {
			if got.ID == job.ID {
				return got.State == "succeeded"
			}
		}
		return false
	})
	for _, fail := range []bool{false, true} {
		reason := ""
		if fail {
			reason = "fail"
		}
		result, e := v.Mutate(context.Background(), HostMutation{Operation: "close", Reason: reason}, nil, nil)
		if fail {
			if !errors.Is(e, ErrHostChild) || result.Revision != 0 {
				t.Fatal(result, e)
			}
		} else if e != nil || result.Revision != 2 {
			t.Fatal(result, e)
		}
	}
	v.Privileged = false
	v.ForceChild = false
	called := false
	_, e = v.Mutate(context.Background(), HostMutation{Operation: "reconcile"}, func(context.Context, HostChildLaunch) ([]byte, error) { t.Fatal("non-root launch"); return nil, nil }, func(context.Context, HostInvocation, HostMutation) (HostMutationResult, error) {
		called = true
		return HostMutationResult{}, nil
	})
	if e != nil || !called {
		t.Fatal(e, called)
	}
}
func TestHostPrivilegeSystemdObservationAndMutation(t *testing.T) {
	_, v := hostIdentityFixture(t)
	o := v.ObservationContext()
	show := 0
	mutate := 0
	s := SystemdHostService{Unit: "myce-worker.service", Observation: &o, WorkerInvocation: &v, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		show++
		if name != "systemctl" || !reflect.DeepEqual(args[:3], []string{"--user", "--machine=worker@.host", "show"}) {
			t.Fatal(name, args)
		}
		return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
	}, Launch: func(_ context.Context, launch HostChildLaunch) ([]byte, error) {
		mutate++
		if launch.Request.Operation != "unit-stop" || launch.Request.Unit != "myce-worker.service" {
			t.Fatal(launch.Request)
		}
		return json.Marshal(hostChildResponse{Version: 1, Operation: launch.Request.Operation})
	}}
	if e := s.Stop(context.Background(), 0); e != nil {
		t.Fatal(e)
	}
	if show != 1 || mutate != 1 {
		t.Fatal(show, mutate)
	}
	s.Launch = func(context.Context, HostChildLaunch) ([]byte, error) { return nil, fmt.Errorf("failed") }
	if e := s.Stop(context.Background(), time.Second); !errors.Is(e, ErrHostChild) {
		t.Fatal(e)
	}
}
