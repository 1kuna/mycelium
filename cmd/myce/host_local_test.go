package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"mycelium/internal/myce"
)

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
	r := myce.HostIdentityResolver{EUID: 0, OS: "linux", Lookup: func(name string) (myce.WorkerAccount, error) {
		if name != a.Name {
			return a, myce.ErrHostIdentity
		}
		return a, nil
	}, ReadConfig: myce.ReadServiceConfig, ValidatePath: func(string, uint32, bool) error { return nil }}
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
func TestHostLocalStatusNeverLaunchesOrWrites(t *testing.T) {
	r, _, c := localIdentityFixture(t)
	before := snapshotHostFiles(t, c.HostStateDir)
	var out bytes.Buffer
	e := hostLocalCommandWithIdentity([]string{"status", "--local", "--worker-user", "worker", "--config", c.Config}, myce.MachineProbe{OS: "linux", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "systemctl" || !reflect.DeepEqual(args, []string{"--user", "--machine=worker@.host", "show", "--property=Version", "--value"}) {
			t.Fatal("status attempted mutation", name, args)
		}
		return []byte("test-manager"), nil
	}}, &out, r, func(context.Context, myce.HostChildLaunch) ([]byte, error) { t.Fatal("status child"); return nil, nil })
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
			e := hostLocalCommandWithIdentity([]string{op, "--local", "--worker-user", "worker", "--config", c.Config}, myce.MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) { t.Fatal("root mutation"); return nil, nil }}, &out, r, func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
				calls++
				if !reflect.DeepEqual(s.Account, a) || !reflect.DeepEqual(s.Env, myce.WorkerChildEnvironment(a)) || s.Request.Operation != op || s.Request.StateDir != c.HostStateDir {
					t.Fatal("wrong child selection")
				}
				return nil, errors.New("child failed")
			})
			if !errors.Is(e, myce.ErrHostChild) || calls != 1 || out.Len() != 0 {
				t.Fatal(e, calls, out.String())
			}
		})
	}
}
