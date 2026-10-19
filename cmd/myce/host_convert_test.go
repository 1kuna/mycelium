package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostConvertCLI(t *testing.T) {
	for _, kind := range []string{"release-7285a88", "release-7285a88-idle"} {
		t.Run(kind, func(t *testing.T) { testHostConvertCLI(t, kind) })
	}
}
func testHostConvertCLI(t *testing.T, kind string) {
	var liveQueue *myce.Queue
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if liveQueue == nil {
			http.Error(w, "not started", 503)
			return
		}
		myce.API{Queue: liveQueue, Token: "test"}.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	home, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	d := myce.Detection{OS: "linux", Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8}
	q, e := myce.PlanInstall("queue", home, "", home, server.URL, "fixture-queue", d)
	if e != nil {
		t.Fatal(e)
	}
	w, e := myce.PlanInstall("worker", home, "", home, q.Queue, "fixture-worker", d)
	if e != nil {
		t.Fatal(e)
	}
	if e = w.WorkerInstance("gpu0"); e != nil {
		t.Fatal(e)
	}
	q.HostID = "fixture-host"
	w.HostID = "fixture-host"
	q.Version = 0
	w.Version = 0
	w.AllowDeviceless = true
	source := filepath.Join("../../internal/myce/testdata", kind)
	for _, pair := range [][2]string{{"host", w.HostStateDir}, {"workers/one", w.State}} {
		if e = filepath.WalkDir(filepath.Join(source, pair[0]), func(p string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			rel, _ := filepath.Rel(filepath.Join(source, pair[0]), p)
			dest := filepath.Join(pair[1], rel)
			if d.IsDir() {
				return os.MkdirAll(dest, 0700)
			}
			b, e := os.ReadFile(p)
			if e != nil {
				return e
			}
			for a, z := range map[string]string{"http://fixture-queue.invalid": server.URL, "/fixture/worker-state": w.State, "/fixture/config.json": w.Config, "/fixture/environment-cache": w.EnvironmentCache} {
				b = bytes.ReplaceAll(b, []byte(a), []byte(z))
			}
			return os.WriteFile(dest, b, 0600)
		}); e != nil {
			t.Fatal(e)
		}
	}
	for _, c := range []myce.ServiceConfig{q, w} {
		for _, p := range []string{c.Config, c.Binary, c.Credentials, c.Service, c.DB} {
			if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
				t.Fatal(e)
			}
		}
		b, _ := json.Marshal(c)
		for p, data := range map[string][]byte{c.Config: b, c.Binary: []byte("old executable"), c.Credentials: []byte(`{"token":"test"}`), c.Service: []byte("old unit")} {
			if e = os.WriteFile(p, data, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	b, e := os.ReadFile(filepath.Join(source, "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(q.DB, b, 0600); e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{q.DB + ".lock", filepath.Join(w.State, "worker.lock")} {
		if e = os.WriteFile(p, nil, 0600); e != nil {
			t.Fatal(e)
		}
	}
	probe := myce.MachineProbe{OS: "linux", Arch: "arm64", Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		if n == "systemd-run" {
			b, _ := json.Marshal(d)
			return b, nil
		}
		if strings.Contains(call, "--property=LoadState,ActiveState") {
			return []byte("loaded\ninactive\n"), nil
		}
		if strings.Contains(call, "--property=UnitFileState") {
			return []byte("disabled"), nil
		}
		if n == "systemctl" && strings.Contains(call, "is-active") {
			return nil, cliFixtureExit(3)
		}
		return nil, nil
	}}
	for _, args := range [][]string{{"--local", "--dry-run"}, {"--local"}, {"--local"}} {
		var out bytes.Buffer
		if e = hostConvertCommandWithProbe(args, probe, &out); e != nil {
			t.Fatal(e, out.String())
		}
	}
	for _, c := range []myce.ServiceConfig{w, q} {
		if e = setupCommandWithProbe([]string{"install", c.Role, "--config", c.Config}, probe, home); e != nil {
			t.Fatal("upgrade", e)
		}
	}
	liveQueue, e = myce.Open(q.DB, w.Checkpoints, myce.RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer liveQueue.Close()
	if e = liveQueue.ConfigureLocalHost(w.HostStateDir, q.Queue); e != nil {
		t.Fatal(e)
	}
	if e = runWithProbe([]string{"_host-start-check", "--host-state-dir", w.HostStateDir}, probe); e != nil {
		t.Fatal("CLOSED start gate", e)
	}
	// No credential loading occurs for the CLOSED service; it registers and exits
	// fenced. Open will issue the normal start request for the same instance.
	if e = serviceCommand([]string{"--config", w.Config}); e != nil {
		t.Fatal("fenced service start", e)
	}
	ws, e := myce.HostWorkers(w.HostStateDir)
	if e != nil || len(ws) != 1 || ws[0].Session != "fixture-session" || ws[0].Config != w.Config {
		t.Fatal(ws, e)
	}
	s, e := myce.ReadHostIntent(w.HostStateDir)
	settlement := "pending"
	if kind == "release-7285a88-idle" {
		settlement = "confirmed"
	}
	if e != nil || s.Desired != "closed" || s.HostSettlement() != settlement {
		t.Fatal(s, e)
	}
	resolver := myce.LocalHostIdentityResolver()
	resolver.OS = "linux"
	account := myce.WorkerAccount{Name: "worker", UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Home: home}
	resolver.Lookup = func(string) (myce.WorkerAccount, error) { return account, nil }
	var out bytes.Buffer
	if e = hostLocalCommandWithIdentity([]string{"status", "--local", "--config", w.Config, "--json"}, probe, &out, resolver, nil); e != nil {
		t.Fatal("status", e, out.String())
	}
	var envelope myce.HostEnvelope
	if e = json.Unmarshal(out.Bytes(), &envelope); e != nil || envelope.Desired.State == nil || *envelope.Desired.State != "closed" {
		t.Fatal("invalid CLOSED envelope", e, out.String())
	}
	intent, e := myce.ReadHostIntent(w.HostStateDir)
	if e != nil {
		t.Fatal(e)
	}
	out.Reset()
	e = hostLocalCommandWithIdentity([]string{"open", "--local", "--config", w.Config, "--expect-revision", fmt.Sprint(intent.Revision), "--json"}, probe, &out, resolver, nil)
	if kind == "release-7285a88-idle" {
		if e != nil {
			t.Fatal("idle open", e, out.String())
		}
		m := myce.Machine{ID: "fresh-worker", Session: "fresh-session", HostID: w.HostID, GPU: "fake", FreeMemoryGB: 8, CapabilityProfile: &myce.CapabilityProfile{MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []myce.AcceleratorProfile{{Type: "fake", MemoryBytes: 64 << 30}}}}
		opened, e := myce.ReadHostIntent(w.HostStateDir)
		if e != nil {
			t.Fatal(e)
		}
		m.HostRevision = opened.HostRevision
		reply, e := liveQueue.Checkin(myce.Checkin{Machine: m})
		if e != nil || reply.Job == nil {
			t.Fatal("open failed to admit", reply, e)
		}
		return
	}
	if e == nil || !strings.Contains(e.Error(), "previous_close_unsettled") {
		t.Fatal("unsettled open", e)
	}
	if e = myce.CheckHostRevision(w.HostStateDir, myce.HostRevision{}); e == nil {
		t.Fatal("CLOSED admitted execution")
	}
	// Strict syntax replaces the obsolete separate-target/manual host conversion.
	for _, args := range [][]string{{}, {"--local", "--apply"}, {"--source", "old", "--target", "new"}} {
		var out bytes.Buffer
		if e = hostConvertCommandWithProbe(args, probe, &out); e == nil {
			t.Fatal("obsolete syntax accepted", args)
		}
	}
}

type cliFixtureExit int

func (e cliFixtureExit) Error() string { return "fixture exit" }
func (e cliFixtureExit) ExitCode() int { return int(e) }

func TestLegacyEnableHasMigrationPointer(t *testing.T) {
	var out bytes.Buffer
	err := hostCommand([]string{"enable"}, myce.MachineProbe{}, &out)
	if err == nil || !strings.Contains(err.Error(), "myce host open --expect-revision") {
		t.Fatal(err)
	}
}
func TestBenchmarkCLIClosedHostRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := myce.InitializeHostIntent(dir, "test-host", myce.RealClock{}); err != nil {
		t.Fatal(err)
	}
	if err := benchmarkCommand([]string{"--state-dir", t.TempDir(), "--gpu", "fake", "--host-state-dir", dir}); err == nil || !strings.Contains(err.Error(), "host disabled") {
		t.Fatal("closed benchmark admitted", err)
	}
}
