package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestObserverNewerCLIHandoff(t *testing.T) {
	t.Setenv("MYCE_DEV_SESSION_ROOT", t.TempDir())
	next := filepath.Join(os.Getenv("MYCE_TEST_ROOT"), "observer-next", "myce-dev-test")
	if err := os.MkdirAll(filepath.Dir(next), 0700); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-p", "1", "-tags=myce_test", "-race", "-ldflags", "-X mycelium/internal/myce.BuildVersion=v3.1.0", "-o", next, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build upgrade image: %v %s", err, out)
	}
	var terminal atomic.Bool
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := "running"
		if terminal.Load() {
			state = "succeeded"
		}
		if r.URL.Path == "/api/wait" {
			reads.Add(1)
			_ = json.NewEncoder(w).Encode(myce.WaitReply{Jobs: []myce.WaitState{{ID: "upgrade-job", State: state, Revision: 1}}})
			return
		}
		fmt.Fprintf(w, `{"id":"upgrade-job","state":%q}`, state)
	}))
	defer server.Close()
	root := t.TempDir()
	profiles := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profiles, 0700); err != nil {
		t.Fatal(err)
	}
	profile := "name: recovery\nproject: upgrade\nclass: experiment\nsource: {directory: source}\nrun: {command: [echo, accepted]}\nneeds: {platform: cpu, cpu_cores: 1, memory_gb: 0.125}\nportable: true\ntargets: [{os: " + runtime.GOOS + ", arch: " + runtime.GOARCH + ", platform: cpu}]\ncheck: [true]\n"
	if err := os.WriteFile(filepath.Join(profiles, "recovery.yaml"), []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"upgrade", "key"}))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(root, "events")
	hook := filepath.Join(root, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nif [ \"${1:-}\" = --check ]; then exit 0; fi\ncat >> "+shellArg(events)+"\nprintf '\\n' >> "+shellArg(events)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := profileReceipt{Project: "upgrade", Key: "key", JobID: "upgrade-job", Status: "queued", Observation: "armed", ArmNonce: "arm", Nonce: "nonce", Intent: profileHash(struct {
		Profile string
		Args    []string
	}{profile, []string{}})}
	cfg := profileObserver{Queue: server.URL, Notify: hook}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(dir, "observer.json"), cfg); err != nil {
		t.Fatal(err)
	}
	registry, err := sharedProfileRoot()
	if err != nil {
		t.Fatal(err)
	}
	if err = saveProfileJSON(filepath.Join(registry, "upgrade.json"), sharedProfileRegistration{Directory: dir, Observer: cfg, Token: "fixture", Expires: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	lock, err := profileLock(filepath.Join(registry, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	old := filepath.Join(filepath.Dir(next), "old", "myce-dev-test")
	if err := os.MkdirAll(filepath.Dir(old), 0700); err != nil {
		t.Fatal(err)
	}
	buildOld := exec.Command("go", "build", "-p", "1", "-tags=myce_test", "-race", "-ldflags", "-X mycelium/internal/myce.BuildVersion=v3.0.0", "-o", old, ".")
	if out, err := buildOld.CombinedOutput(); err != nil {
		t.Fatalf("build previous image: %v %s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := startSharedObserver(ctx, old, registry, lock); err != nil {
		t.Fatal(err)
	}
	lock.Close()
	var previous observerSupervision
	if err := readProfileJSON(filepath.Join(registry, "observer.supervision"), &previous); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(previous.PID, syscall.SIGTERM) })
	wait(t, ctx, func() bool { return reads.Load() > 0 })
	invoke := func() {
		t.Helper()
		cmd := exec.CommandContext(ctx, next, "run", "--profile", "recovery", "--key", "key", "--queue", server.URL, "--notify", hook)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN=fixture")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("new CLI arm: %v %s", err, out)
		}
	}
	invoke()
	wait(t, ctx, func() bool {
		var owner observerSupervision
		var state observerDiagnostic
		return readProfileJSON(filepath.Join(registry, "observer.supervision"), &owner) == nil && owner.PID != previous.PID &&
			readProfileJSON(filepath.Join(registry, "observer.status"), &state) == nil && state.Version == "v3.1.0"
	})
	var replacement observerSupervision
	if err := readProfileJSON(filepath.Join(registry, "observer.supervision"), &replacement); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(replacement.PID, syscall.SIGTERM) })
	var state observerDiagnostic
	if err = readProfileJSON(filepath.Join(registry, "observer.status"), &state); err != nil || state.PID == os.Getpid() || state.Version != "v3.1.0" {
		t.Fatalf("replacement metadata: %+v %v", state, err)
	}
	if err := syscall.Kill(state.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var recovered observerDiagnostic
		return readProfileJSON(filepath.Join(registry, "observer.status"), &recovered) == nil && recovered.PID != state.PID && recovered.Version == "v3.1.0"
	})
	terminal.Store(true)
	wait(t, ctx, func() bool {
		var saved profileReceipt
		return readProfileJSON(filepath.Join(dir, "receipt.json"), &saved) == nil && saved.DeliveryStatus == "delivered"
	})
	invoke()
	data, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 1 {
		t.Fatal("handoff/replay duplicated notification")
	}
	if _, err = os.Stat(filepath.Join(registry, "observer.upgrade")); !os.IsNotExist(err) {
		t.Fatal("handoff request retained", err)
	}
	wait(t, ctx, func() bool {
		lock, err := profileLock(filepath.Join(registry, "observer.lock"))
		if err != nil {
			return false
		}
		lock.Close()
		return true
	})
}
