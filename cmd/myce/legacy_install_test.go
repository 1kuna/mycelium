package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

type rcFixtureExit int

func (e rcFixtureExit) Error() string { return "inactive synthetic unit" }
func (e rcFixtureExit) ExitCode() int { return int(e) }

type rcFixtureTransport struct{ target string }

func (x rcFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	u, _ := url.Parse(x.target)
	r.URL.Scheme = u.Scheme
	r.URL.Host = u.Host
	return http.DefaultTransport.RoundTrip(r)
}

func TestLegacyInstallArchiveFresh(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	generator := filepath.Join("..", "..", "internal", "myce", "testdata", "generate-release-fixtures.py")
	cmd := exec.Command("python3.14", generator, "--scratch", filepath.Join(root, "rc-source"), "--output", filepath.Join(root, "fixtures"), "--install-home", home)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("real RC writers: %v %s", err, out)
	}
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, ".local/share/myce"))
	t.Setenv("MYCE_QUEUE", "http://ambient.invalid")
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	configs := []myce.ServiceConfig{}
	for _, role := range []string{"queue", "worker-gpu0", "worker-gpu1"} {
		c, e := myce.ReadServiceConfig(filepath.Join(home, ".local/share/mycelium", role, "config.json"))
		if e != nil {
			t.Fatal(e)
		}
		configs = append(configs, c)
	}
	var calls []string
	probe := myce.MachineProbe{OS: "linux", Arch: runtime.GOARCH, Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		calls = append(calls, n+" "+call)
		if strings.Contains(call, "--property=UnitFileState") {
			return []byte("disabled"), nil
		}
		if n == "systemctl" && strings.Contains(call, "is-active") {
			return nil, rcFixtureExit(3)
		}
		return nil, nil
	}}
	// Even install refuses before consulting/changing the manager, or publishing.
	for _, c := range configs {
		before, _ := os.ReadFile(c.Config)
		calls = nil
		err := setupCommandWithProbe([]string{"install", c.Role, "--config", c.Config}, probe, home)
		if err == nil || !strings.Contains(err.Error(), "legacy_state_present") {
			t.Fatal("install accepted RC state", err)
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
	if e := os.Rename(configs[0].HostStateDir, configs[0].HostStateDir+".operator-archive"); e != nil {
		t.Fatal(e)
	}
	archives := append([]string{}, configs[0].HostStateDir+".operator-archive")
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
			t.Fatal("WP7 overlay changed omitted RC config values")
		}
		unit, e := os.ReadFile(c.Service)
		if e != nil || string(unit) != got.ServiceText() {
			t.Fatal("RC unit was not replaced", e)
		}
		if c.Role == "worker" && !bytes.Contains(unit, []byte("_host-start-check")) {
			t.Fatal("template instance missing current fence")
		}
	}
	// The RC had no endpoint field. Once an explicit endpoint is provisioned,
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
	w, qc := configs[1], configs[0]
	for _, c := range configs[1:] {
		if e := runWithProbe([]string{"_host-start-check", "--host-state-dir", c.HostStateDir}, probe); e == nil || !strings.Contains(e.Error(), "host disabled by operator") {
			t.Fatal("CLOSED ExecCondition was not fenced", e)
		}
		if e := serviceCommand([]string{"--config", c.Config}); e != nil {
			t.Fatal("closed start", e)
		}
	}
	q, e := myce.Open(qc.DB, qc.Checkpoints, myce.RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	if e = q.ConfigureLocalHost(w.HostStateDir, qc.Queue); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "synthetic-fresh-install"}.Handler())
	defer server.Close()
	savedClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: rcFixtureTransport{server.URL}}
	defer func() { http.DefaultClient = savedClient }()
	resolver := myce.LocalHostIdentityResolver()
	resolver.OS = "linux"
	resolver.Lookup = func(string) (myce.WorkerAccount, error) {
		return myce.WorkerAccount{Name: "worker", UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Home: home}, nil
	}
	var out bytes.Buffer
	if e = hostLocalCommandWithIdentity([]string{"status", "--local", "--config", w.Config, "--json"}, probe, &out, resolver, nil); e != nil {
		t.Fatal(e, out.String())
	}
	var envelope myce.HostEnvelope
	if e = json.Unmarshal(out.Bytes(), &envelope); e != nil || envelope.Desired.State == nil || *envelope.Desired.State != "closed" || envelope.Desired.Revision == nil || envelope.Settlement != "confirmed" || envelope.Accepted != nil {
		t.Fatal("invalid fresh CLOSED envelope", e, out.String())
	}
	validateEnvelopeOutput(t, out.Bytes())
	t.Log("empty fresh install: CLOSED/confirmed; status desired.revision is present; accepted_revision is null; both template instances fenced")
	spec := myce.Spec{Name: "fresh-job", Project: "fixture", Priority: "experiment"}
	spec.Needs.Platform.Any = true
	spec.Needs.MemoryGB = 1
	spec.Run.Command = []string{"sh", "-c", "true"}
	if e := spec.Validate(); e != nil {
		t.Fatal(e)
	}
	client := myce.Client{URL: server.URL, Token: "synthetic-fresh-install"}
	var snapshot myce.Snapshot
	if e = client.Call(context.Background(), "GET", "/api/status", nil, &snapshot); e != nil || len(snapshot.Jobs) != 0 {
		t.Fatal("fresh queue retained RC jobs", e)
	}
	var job myce.Job
	if e = client.Call(context.Background(), "POST", "/api/jobs", spec, &job); e != nil {
		t.Fatal(e)
	}
	m := myce.Machine{ID: "fresh-worker", Session: "fresh-session", HostID: w.HostID, GPU: "fake", FreeMemoryGB: 8, CapabilityProfile: &myce.CapabilityProfile{MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []myce.AcceleratorProfile{{Type: "fake", MemoryBytes: 64 << 30}}}}
	reply, e := q.Checkin(myce.Checkin{Machine: m})
	if e != nil || reply.Job != nil || !reply.Leave {
		t.Fatal("CLOSED admitted work", reply, e)
	}
	out.Reset()
	if e = hostLocalCommandWithIdentity([]string{"open", "--local", "--config", w.Config, "--expect-revision", fmt.Sprint(*envelope.Desired.Revision + 1), "--json"}, probe, &out, resolver, nil); e == nil {
		t.Fatal("stale revision opened host")
	}
	out.Reset()
	e = hostLocalCommandWithIdentity([]string{"open", "--local", "--config", w.Config, "--expect-revision", fmt.Sprint(*envelope.Desired.Revision), "--json"}, probe, &out, resolver, nil)
	if e != nil {
		t.Fatal("revision-bound open", e, out.String())
	}
	opened, e := myce.ReadHostIntent(w.HostStateDir)
	if e != nil {
		t.Fatal(e)
	}
	m.HostRevision = opened.HostRevision
	m.Session = "after-open-session"
	reply, e = q.Checkin(myce.Checkin{Machine: m, Fresh: true, PreviousSession: "fresh-session"})
	if e != nil || reply.Job == nil || reply.Job.ID != job.ID {
		t.Fatal("OPEN did not admit fresh job", reply, e)
	}
	if !reflect.DeepEqual(archived, archiveHashes()) {
		t.Fatal("fresh install/open modified archived RC state")
	}
	t.Log("real RC PlanInstall/Install/registerHost layout archived by operator; queue/NAS/name/Docker/environment reference preserved; work admitted only after explicit open using status desired.revision")
}
