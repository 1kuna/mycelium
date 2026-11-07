package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Exercise recovery through ordinary keyed profile CLI invocations, including
// registrations made by other processes/workspaces before the observer starts.
func TestSharedObserverCLIAdoptsPreexistingArms(t *testing.T) {
	t.Setenv("MYCE_DEV_SESSION_ROOT", t.TempDir())
	binary := devTestBinary(t)
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "recovery-fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	events := filepath.Join(t.TempDir(), "events")
	hook := filepath.Join(t.TempDir(), "notify")
	if err = os.WriteFile(hook, []byte("#!/bin/sh\nif [ \"${1:-}\" = --check ]; then exit 0; fi\ncat >> \"$OBSERVER_ADOPTION_EVENTS\"\nprintf '\\n' >> \"$OBSERVER_ADOPTION_EVENTS\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	registry, err := sharedProfileRoot()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := profileLock(filepath.Join(registry, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	roots := []string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		if err = os.MkdirAll(filepath.Join(root, ".mycelium", "profiles"), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
			t.Fatal(err)
		}
		profile := "name: adoption\nproject: recovery\nclass: experiment\nsource: {directory: source}\nrun: {command: [sh, -c, 'printf accepted > result']}\nneeds: {cpu_cores: 1, memory_gb: 0.125}\ntargets: [{os: " + runtime.GOOS + ", arch: " + runtime.GOARCH + ", platform: cpu}]\noutputs: {result: results}\ncheck: [test, -s, result]\n"
		if err = os.WriteFile(filepath.Join(root, ".mycelium", "profiles", "adoption.yaml"), []byte(profile), 0600); err != nil {
			t.Fatal(err)
		}
	}
	invoke := func(root, key string) profileReceipt {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "run", "--profile", "adoption", "--queue", d.URL, "--key", key, "--notify", hook)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN=recovery-fixture", "OBSERVER_ADOPTION_EVENTS="+events)
		var diagnostic bytes.Buffer
		cmd.Stderr = &diagnostic
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("profile recovery: %v %s", err, &diagnostic)
		}
		var receipt profileReceipt
		if err = json.Unmarshal(out, &receipt); err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	type arm struct{ root, key, id string }
	arms := []arm{{roots[0], "first", ""}, {roots[1], "second", ""}, {roots[1], "third", ""}}
	for i := range arms {
		r := invoke(arms[i].root, arms[i].key)
		arms[i].id = r.JobID
	}
	for _, a := range arms {
		wait(t, ctx, func() bool { j, e := d.Client().GetJob(ctx, a.id); return e == nil && j.State == "succeeded" })
	}
	if _, err = os.Stat(events); !os.IsNotExist(err) {
		t.Fatal("delivery occurred while previous owner held registry", err)
	}
	// Closing the old lifetime lock is the only simulated restart operation.
	// The next normal same-key replay must launch and discover every registration.
	owner.Close()
	invoke(arms[0].root, arms[0].key)
	verify := func(want int) {
		t.Helper()
		wait(t, ctx, func() bool {
			for _, a := range arms {
				var r profileReceipt
				path := filepath.Join(a.root, ".mycelium", "receipts", profileHash([]string{"recovery", a.key}), "receipt.json")
				if readProfileJSON(path, &r) != nil || r.DeliveryStatus != "delivered" || r.Observation != "not_armed" || r.Status != "succeeded" {
					return false
				}
			}
			entries, _ := filepath.Glob(filepath.Join(registry, "*.json"))
			if len(entries) != 0 {
				return false
			}
			lock, e := profileLock(filepath.Join(registry, "observer.lock"))
			if e != nil {
				return false
			}
			lock.Close()
			return true
		})
		data, err := os.ReadFile(events)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		seen := map[string]bool{}
		for _, line := range lines {
			var n profileNotification
			if err = json.Unmarshal([]byte(line), &n); err != nil {
				t.Fatal(err)
			}
			if seen[n.DeliveryID] {
				t.Fatal("duplicate delivery", n.JobID)
			}
			seen[n.DeliveryID] = true
		}
		if len(lines) != want {
			t.Fatalf("notifications=%d want=%d", len(lines), want)
		}
	}
	verify(3)
	for _, a := range arms {
		invoke(a.root, a.key)
	}
	verify(3)
	// A new arm after recovery starts a fresh owner and also delivers once.
	a := arm{roots[0], "new-arm", ""}
	a.id = invoke(a.root, a.key).JobID
	arms = append(arms, a)
	verify(4)
}
