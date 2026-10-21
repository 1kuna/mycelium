package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

type installFixtureExit int

func (e installFixtureExit) Error() string { return "inactive synthetic unit" }
func (e installFixtureExit) ExitCode() int { return int(e) }

type installFixtureTransport struct {
	target string
	base   http.RoundTripper
}

func (x installFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	u, _ := url.Parse(x.target)
	r.URL.Scheme = u.Scheme
	r.URL.Host = u.Host
	return x.base.RoundTrip(r)
}

func TestLegacyInstallArchiveFresh(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "http://ambient.invalid")
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	environment := filepath.Join(home, "private-reference")
	if err := os.WriteFile(environment, []byte("MYCE_TOKEN=synthetic-fresh-install\nMYCE_QUEUE=http://fixture-queue.invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var configs []myce.ServiceConfig
	for i, instance := range []string{"", "gpu0", "gpu1"} {
		role := "worker"
		if i == 0 {
			role = "queue"
		}
		// Independent install accounts keep this archive-custody test valid with
		// one machine unit per account; split conversion has separate coverage.
		installHome := home
		if role == "worker" {
			installHome = filepath.Join(home, instance)
		}
		c, err := myce.PlanInstall(role, installHome, "", home, "http://fixture-queue.invalid", fmt.Sprintf("fixture-%d", i), myce.Detection{OS: "linux", Arch: runtime.GOARCH, Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
		if err != nil {
			t.Fatal(err)
		}
		c.EnvironmentFile, c.HostID = environment, fmt.Sprintf("fixture-host-%d", i)
		c.DockerEndpoint = "unix:///var/run/docker.sock"
		if role == "worker" {
			c.AllowDeviceless = true
		}
		if err := os.MkdirAll(filepath.Dir(c.Config), 0700); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(c.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		configs = append(configs, c)
	}
	// Any unmarked non-empty root suffices; payload contents are never read.
	for _, dir := range []string{configs[0].State, configs[1].State, configs[2].State} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "arbitrary"), []byte("surviving data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var calls []string
	probe := myce.MachineProbe{OS: "linux", Arch: runtime.GOARCH, Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		calls = append(calls, n+" "+call)
		if strings.Contains(call, "--property=UnitFileState") {
			return []byte("disabled"), nil
		}
		if n == "systemctl" && strings.Contains(call, "is-active") {
			return nil, installFixtureExit(3)
		}
		return nil, nil
	}}
	// Even install refuses before consulting/changing the manager, or publishing.
	for _, c := range configs {
		before, _ := os.ReadFile(c.Config)
		calls = nil
		err := setupCommandWithProbe([]string{"install", c.Role, "--config", c.Config}, probe, home)
		if err == nil || !strings.Contains(err.Error(), "legacy_state_present") {
			t.Fatal("install accepted unmarked state", err)
		}
		after, _ := os.ReadFile(c.Config)
		if !bytes.Equal(before, after) {
			t.Fatal("refusal changed config")
		}
		for _, call := range calls {
			if strings.HasPrefix(call, "systemctl ") {
				t.Fatal("refusal touched manager")
			}
		}
	}
	// Operator action, outside the installer: move whole data dirs, not rows/files.
	for _, c := range configs {
		if e := os.Rename(c.State, c.State+".operator-archive"); e != nil {
			t.Fatal(e)
		}
	}
	archives := []string{}
	for _, c := range configs {
		archives = append(archives, c.State+".operator-archive")
	}
	archiveHashes := func() map[string][32]byte {
		out := map[string][32]byte{}
		for _, root := range archives {
			err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					out[path] = sha256.Sum256([]byte("directory"))
					return nil
				}
				b, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				out[path] = sha256.Sum256(b)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	archived := archiveHashes()
	for _, c := range configs {
		if e := setupCommandWithProbe([]string{"install", c.Role, "--config", c.Config}, probe, home); e != nil {
			t.Fatal("fresh install", c.Role, e)
		}
		got, e := myce.ReadServiceConfig(c.Config)
		if e != nil {
			t.Fatal(e)
		}
		want := c
		want.Version = 1
		if !reflect.DeepEqual(want, got) {
			t.Fatal("overlay changed omitted current config values")
		}
		unit, e := os.ReadFile(c.Service)
		if e != nil || string(unit) != got.ServiceText() {
			t.Fatal("current unit was not published", e)
		}
		if c.Role == "worker" && bytes.Contains(unit, []byte("_host-start-check")) {
			t.Fatal("removed local gate still installed")
		}
	}
	// Once an explicit endpoint is provisioned,
	// the same overlay must retain it on the next omitted-flag invocation.
	endpoint := "unix:///run/fixture-pinned.sock"
	args := []string{"install", "worker", "--config", configs[1].Config, "--docker-host", endpoint}
	if e := setupCommandWithProbe(args, probe, home); e != nil {
		t.Fatal(e)
	}
	if e := setupCommandWithProbe(args[:4], probe, home); e != nil {
		t.Fatal(e)
	}
	got, e := myce.ReadServiceConfig(configs[1].Config)
	if e != nil {
		t.Fatal(e)
	}
	want := configs[1]
	want.Version = 1
	want.DockerEndpoint = endpoint
	if !reflect.DeepEqual(got, want) {
		t.Fatal("saved endpoint or unrelated config changed")
	}
	configs[1] = got
	if !reflect.DeepEqual(archived, archiveHashes()) {
		t.Fatal("archive bytes changed")
	}
}

func TestQueueStartupRefusalNoWrites(t *testing.T) {
	for _, refused := range []string{"queue", "upload-spool", "file-store", "identity"} {
		t.Run(refused, func(t *testing.T) {
			parent := t.TempDir()
			roots := map[string]string{}
			for _, name := range []string{"queue", "upload-spool", "file-store", "identity"} {
				roots[name] = filepath.Join(parent, name)
			}
			if err := os.Mkdir(roots[refused], 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(roots[refused], "arbitrary"), []byte("surviving data"), 0600); err != nil {
				t.Fatal(err)
			}
			snapshot := func() map[string]string {
				t.Helper()
				tree := map[string]string{}
				if err := filepath.WalkDir(parent, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if d.IsDir() {
						tree[path] = "directory"
						return nil
					}
					data, err := os.ReadFile(path)
					if err == nil {
						tree[path] = string(data)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return tree
			}
			before := snapshot()
			t.Setenv("MYCE_TOKEN", "synthetic-fixture-token")
			err := run([]string{"queue", "--db", filepath.Join(roots["queue"], "queue.db"), "--upload-spool-dir", roots["upload-spool"], "--file-store", roots["file-store"], "--identity-dir", roots["identity"], "--listen", "127.0.0.1:0", "--discovery=false"})
			expected := "legacy_state_present: stop the old services, archive " + roots[refused] + ", then do a fresh install (preserve config and secret references)"
			if err == nil || err.Error() != expected {
				t.Fatalf("refusal: %v; want %s", err, expected)
			}
			if !reflect.DeepEqual(before, snapshot()) {
				t.Fatal("queue startup changed filesystem before refusal")
			}
		})
	}
}

func currentTestStateRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := myce.PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	return root
}
