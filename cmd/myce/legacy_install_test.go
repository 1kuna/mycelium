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
	t.Setenv("MYCE_HOST_STATE_DIR", filepath.Join(home, ".local/share/myce"))
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
		c, err := myce.PlanInstall(role, home, "", home, "http://fixture-queue.invalid", fmt.Sprintf("fixture-%d", i), myce.Detection{OS: "linux", Arch: runtime.GOARCH, Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
		if err != nil {
			t.Fatal(err)
		}
		c.EnvironmentFile, c.HostID = environment, "fixture-host"
		c.DockerEndpoint = "unix:///var/run/docker.sock"
		if role == "worker" {
			c.AllowDeviceless = true
			if err := c.WorkerInstance(instance); err != nil {
				t.Fatal(err)
			}
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
	for _, dir := range []string{configs[0].State, configs[1].State, configs[2].State, configs[0].HostStateDir} {
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
			t.Fatal("overlay changed omitted current config values")
		}
		unit, e := os.ReadFile(c.Service)
		if e != nil || string(unit) != got.ServiceText() {
			t.Fatal("current unit was not published", e)
		}
		if c.Role == "worker" && !bytes.Contains(unit, []byte("_host-start-check")) {
			t.Fatal("template instance missing current fence")
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
	savedTransport := http.DefaultTransport
	http.DefaultTransport = installFixtureTransport{server.URL, savedTransport}
	defer func() { http.DefaultTransport = savedTransport }()
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
	// Fresh install has never created a worker session. Exercise the same close
	// and status command adapters as FULL apply, with only hardware/service fakes.
	executionProbe := probe
	executionProbe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "systemctl" && strings.Contains(strings.Join(args, " "), "-p LoadState") {
			return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
		}
		return probe.Command(ctx, name, args...)
	}
	executor := hostMutationExecutor(executionProbe)
	executor.Observe = envelopeHardware
	handler := func(ctx context.Context, v myce.HostInvocation, m myce.HostMutation) (myce.HostMutationResult, error) {
		r, e := executor.Execute(ctx, v, m)
		if e != nil {
			t.Logf("mutation %s: %v", m.Operation, e)
		}
		return r, e
	}
	observe := func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
		state, err := myce.ReadHostIntent(v.StateDir)
		return envelopeHardware(ctx, state), err
	}
	out.Reset()
	if e := hostEnvelopeCommandWithObservation([]string{"close", "--local", "--config", w.Config, "--grace", "0", "--json"}, probe, &out, resolver, nil, handler, observe); e != nil {
		t.Fatal("never-started close", e, out.String())
	}
	out.Reset()
	if e := hostEnvelopeCommandWithObservation([]string{"status", "--local", "--config", w.Config, "--expect", "closed", "--verdict", "--json"}, probe, &out, resolver, nil, handler, observe); e != nil {
		t.Fatal("never-started closed verdict", e, out.String())
	}
	envelope = validateEnvelopeOutput(t, out.Bytes())
	if envelope.Verdict != "PASS" || envelope.QueueRevision == nil || envelope.Counts.Active == nil || *envelope.Counts.Active != 0 || envelope.Counts.Unsettled == nil || *envelope.Counts.Unsettled != 0 {
		t.Fatal("fresh host lacks positive queue release evidence", out.String())
	}
	var freshSnapshot myce.Snapshot
	if e := (myce.Client{URL: server.URL, Token: "synthetic-fresh-install"}).Call(context.Background(), "GET", "/api/status", nil, &freshSnapshot); e != nil || len(freshSnapshot.Machines) != 0 {
		t.Fatal("test created a session before closed PASS", e)
	}
	t.Log("fresh install -> CLOSED -> host close --local -> closed PASS with zero registered sessions and explicit queue acknowledgement")
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
		t.Fatal("fresh queue contains jobs", e)
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
		t.Fatal("fresh install/open modified archived unmarked state")
	}
	t.Log("current config preserved through archive/fresh install; work admitted only after explicit open")
}

func TestQueueStartupRefusalNoWrites(t *testing.T) {
	for _, refused := range []string{"queue", "host", "upload-spool", "file-store", "identity"} {
		t.Run(refused, func(t *testing.T) {
			parent := t.TempDir()
			roots := map[string]string{}
			for _, name := range []string{"queue", "host", "upload-spool", "file-store", "identity"} {
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
			err := run([]string{"queue", "--db", filepath.Join(roots["queue"], "queue.db"), "--host-state-dir", roots["host"], "--upload-spool-dir", roots["upload-spool"], "--file-store", roots["file-store"], "--identity-dir", roots["identity"], "--listen", "127.0.0.1:0", "--discovery=false"})
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
