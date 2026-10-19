package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"mycelium/internal/myce"
)

func TestHostLocalReconcileUsesWorkerExecutor(t *testing.T) {
	r, account, config := localIdentityFixture(t)
	config.Service = filepath.Join(account.Home, "myce-worker.service")
	b, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config.Config, b, 0600); err != nil {
		t.Fatal(err)
	}
	readConfig, validatePath := r.ReadConfig, r.ValidatePath
	r.ReadConfig = func(string) (myce.ServiceConfig, error) {
		t.Fatal("root read worker config")
		return myce.ServiceConfig{}, nil
	}
	r.ValidatePath = func(string, uint32, bool) error { t.Fatal("root inspected worker files"); return nil }
	insideChild := false
	stops := 0
	probe := myce.MachineProbe{OS: "linux", Command: func(_ context.Context, command string, args ...string) ([]byte, error) {
		if !insideChild {
			t.Fatal("root ran mutation or worker discovery")
		}
		if command != "systemctl" {
			t.Fatal(command)
		}
		for _, arg := range args {
			if arg == "stop" {
				stops++
			}
		}
		return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
	}}
	var out bytes.Buffer
	err = hostLocalCommandWithIdentity([]string{"reconcile", "--local", "--worker-user", account.Name, "--config", config.Config}, probe, &out, r, func(ctx context.Context, launch myce.HostChildLaunch) ([]byte, error) {
		worker := r
		worker.EUID = int(account.UID)
		worker.ReadConfig, worker.ValidatePath = readConfig, validatePath
		b, err := json.Marshal(launch.Request)
		if err != nil {
			return nil, err
		}
		var response bytes.Buffer
		insideChild = true
		defer func() { insideChild = false }()
		err = myce.ServeHostWorkerChild(ctx, bytes.NewReader(b), &response, worker, hostMutationExecutor(probe).Execute)
		return response.Bytes(), err
	})
	if err != nil || stops != 1 {
		t.Fatal(err, stops, out.String())
	}
	state, err := myce.ReadHostIntent(config.HostStateDir)
	if err != nil || state.LocalExclusion == nil || state.LocalExclusion.Confirmed || state.LocalExclusion.Evidence == nil || state.LocalExclusion.Evidence.Verdict != "UNKNOWN" {
		t.Fatal(state, err)
	}
}

func localIdentityFixture(t *testing.T) (myce.HostIdentityResolver, myce.WorkerAccount, myce.ServiceConfig) {
	t.Helper()
	home, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(home, "host")
	spool := filepath.Join(home, "spool")
	if e = os.Mkdir(spool, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = myce.InitializeHostIntent(dir, "test-host", myce.RealClock{}); e != nil {
		t.Fatal(e)
	}
	a := myce.WorkerAccount{Name: "worker", UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Groups: []uint32{10, 20}, Home: home}
	c := myce.ServiceConfig{Role: "worker", HostID: "test-host", State: spool, HostStateDir: dir, Config: filepath.Join(home, "config.json")}
	b, _ := json.Marshal(c)
	if e = os.WriteFile(c.Config, b, 0600); e != nil {
		t.Fatal(e)
	}
	r := myce.LocalHostIdentityResolver()
	r.EUID = 0
	r.OS = "linux"
	r.Lookup = func(name string) (myce.WorkerAccount, error) {
		if name != a.Name {
			return a, myce.ErrHostIdentity
		}
		return a, nil
	}
	return r, a, c
}
func snapshotHostFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	result := map[string]string{}
	if e := filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
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
			result[path] = st.ModTime().String() + ":" + string(b)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return result
}
func TestHostLocalStatusUsesReadOnlyChildAndNeverWrites(t *testing.T) {
	r, a, c := localIdentityFixture(t)
	before := snapshotHostFiles(t, c.HostStateDir)
	var out bytes.Buffer
	e := hostLocalCommandWithIdentity([]string{"status", "--local", "--worker-user", "worker", "--config", c.Config}, myce.MachineProbe{OS: "linux", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "systemctl" || !reflect.DeepEqual(args, []string{"--user", "--machine=worker@.host", "show", "--property=Version", "--value"}) {
			t.Fatal("status attempted mutation", name, args)
		}
		return []byte("test-manager"), nil
	}}, &out, r, func(ctx context.Context, s myce.HostChildLaunch) ([]byte, error) {
		if s.Request.Operation != "status" && s.Request.Operation != "inspect-exclusion" && s.Request.Operation != "resolve" {
			t.Fatal("status mutation child")
		}
		worker := r
		worker.EUID = int(a.UID)
		b, _ := json.Marshal(s.Request)
		var result bytes.Buffer
		if e := myce.ServeHostWorkerChild(ctx, bytes.NewReader(b), &result, worker, nil); e != nil {
			return nil, e
		}
		return result.Bytes(), nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if after := snapshotHostFiles(t, c.HostStateDir); !reflect.DeepEqual(before, after) {
		t.Fatal("status wrote host files")
	}
	if bytes.Contains(out.Bytes(), []byte("PASS")) {
		t.Fatal("identity-only status claimed exclusion")
	}
}
func TestHostLocalAllMutationsEnterChildAndFailClosed(t *testing.T) {
	for _, op := range []string{"close", "open", "reconcile"} {
		t.Run(op, func(t *testing.T) {
			r, a, c := localIdentityFixture(t)
			calls := 0
			var out bytes.Buffer
			e := hostLocalCommandWithIdentity([]string{op, "--local", "--worker-user", "worker", "--config", c.Config, "--expect-revision", "1"}, myce.MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) { t.Fatal("root mutation"); return nil, nil }}, &out, r, func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
				calls++
				if !reflect.DeepEqual(s.Account, a) || !reflect.DeepEqual(s.Env, myce.WorkerChildEnvironment(a)) || s.Request.Operation != op || s.Request.StateDir != "" {
					t.Fatal("wrong child selection")
				}
				return nil, errors.New("child failed")
			})
			if !errors.Is(e, myce.ErrHostChild) || calls != 1 || op == "reconcile" && out.Len() != 0 || op != "reconcile" && !strings.Contains(out.String(), "UNKNOWN") {
				t.Fatal(e, calls, out.String())
			}
		})
	}
}

func TestHostLocalUnsafePathsWithRealValidation(t *testing.T) {
	for _, kind := range []string{"state-symlink", "config-symlink", "config-fifo", "config-unowned", "config-writable", "ancestor-writable"} {
		t.Run(kind, func(t *testing.T) {
			r, a, c := localIdentityFixture(t)
			config, state := c.Config, ""
			var e error
			switch kind {
			case "state-symlink":
				state = filepath.Join(a.Home, "alias")
				e = os.Symlink(c.HostStateDir, state)
			case "config-symlink":
				config = filepath.Join(a.Home, "alias.json")
				e = os.Symlink(c.Config, config)
			case "config-fifo":
				config = filepath.Join(a.Home, "fifo")
				e = syscall.Mkfifo(config, 0600)
			case "config-unowned":
				// Root-owned on both supported laptop/native platforms. Validation
				// must reject ownership before any content is opened.
				config = "/etc/passwd"
			case "config-writable":
				e = os.Chmod(c.Config, 0666)
			case "ancestor-writable":
				e = os.Chmod(a.Home, 0770)
			}
			if e != nil {
				t.Fatal(e)
			}
			var out bytes.Buffer
			calls := 0
			e = hostLocalCommandWithIdentity([]string{"status", "--local", "--worker-user", a.Name, "--config", config, "--state-dir", state}, myce.MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("observation after unsafe read")
				return nil, nil
			}}, &out, r, func(ctx context.Context, launch myce.HostChildLaunch) ([]byte, error) {
				calls++
				worker := r
				worker.EUID = int(a.UID)
				b, _ := json.Marshal(launch.Request)
				var response bytes.Buffer
				if err := myce.ServeHostWorkerChild(ctx, bytes.NewReader(b), &response, worker, nil); err != nil {
					return nil, err
				}
				return response.Bytes(), nil
			})
			if !errors.Is(e, myce.ErrHostTarget) || calls != 1 || !strings.Contains(out.String(), "UNKNOWN") || strings.Contains(out.String(), config) || strings.Contains(e.Error(), config) {
				t.Fatal(e, calls, out.String())
			}
		})
	}
}
