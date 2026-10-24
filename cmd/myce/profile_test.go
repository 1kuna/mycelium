package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestProfileDevReceiptRecoveryAndNotification(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profileDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source", "run.sh"), []byte("test -z \"$MYCE_TOKEN\"\nprintf '%s' \"$1\" > result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := `name: fixture
project: test
class: experiment
source: {directory: source}
run:
  command: [sh, run.sh]
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
outputs: {result: results}
check: [test, -s, result]
`
	path := filepath.Join(profileDir, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	notifier := filepath.Join(root, "notify")
	if err := os.WriteFile(notifier, []byte("#!/bin/sh\nif [ \"${1:-}\" = --check ]; then test -n \"${NOTIFICATION_FILE:-}\"; exit $?; fi\ntest -z \"${MYCE_TOKEN:-}\" || exit 99\ncat >> \"$NOTIFICATION_FILE\"\nprintf '\\n' >> \"$NOTIFICATION_FILE\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	binary := devTestBinary(t)
	const token = "profile-fixture-token"
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	notificationFile := filepath.Join(root, "notifications")
	invoke := func(extra ...string) (profileReceipt, error) {
		cmd := exec.CommandContext(ctx, binary, append([]string{"run", "--queue", d.URL, "--profile", path}, extra...)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token, "NOTIFICATION_FILE="+notificationFile)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, e := cmd.Output()
		var r profileReceipt
		if len(out) > 0 {
			if decode := json.Unmarshal(out, &r); decode != nil {
				t.Fatalf("receipt: %s %v", out, decode)
			}
		}
		if e != nil {
			t.Logf("profile exit: %v %s", e, stderr.String())
		}
		if bytes.Contains(out, []byte(token)) {
			t.Fatal("receipt leaked credential")
		}
		return r, e
	}
	// A supplied notifier that is absent must never report armed.
	missing, err := invoke("--key", "missing", "--notify", filepath.Join(root, "absent"), "--", "literal")
	if exitCodeForTest(err) != 74 || missing.Delivery != "not_configured" || missing.Observation != "not_armed" {
		t.Fatal("missing notifier armed", missing, err)
	}
	// Recover that exact job with configured delivery and literal argv.
	r, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	if err != nil || r.JobID != missing.JobID || r.Delivery != "armed" {
		t.Fatal("recovery", r, err)
	}
	recovered, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	if err != nil || recovered.JobID != r.JobID {
		t.Fatal("duplicate submit", recovered, err)
	}
	receiptPath := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", r.Key}), "receipt.json")
	wait(t, ctx, func() bool {
		var current profileReceipt
		return readProfileJSON(receiptPath, &current) == nil && current.DeliveryStatus == "delivered"
	})
	var delivered profileReceipt
	if err = readProfileJSON(receiptPath, &delivered); err != nil {
		t.Fatal(err)
	}
	if delivered.Status != "succeeded" || len(delivered.ResultReference) != 1 {
		t.Fatal("result missing", delivered)
	}
	dest := filepath.Join(root, "downloaded")
	if err = d.Client().DownloadResult(ctx, r.JobID, "result", dest); err != nil {
		t.Fatal(err)
	}
	result, _ := os.ReadFile(dest)
	if string(result) != "literal" {
		t.Fatal("bad result", string(result))
	}
	replay, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	if err != nil || replay.JobID != r.JobID {
		t.Fatal("completed replay", replay, err)
	}
	notifications, _ := os.ReadFile(notificationFile)
	if bytes.Count(notifications, []byte("\n")) != 1 {
		t.Fatal("duplicate notifier", string(notifications))
	}
	var n profileNotification
	if err = json.Unmarshal(bytes.TrimSpace(notifications), &n); err != nil || n.Status != "succeeded" || n.JobID != r.JobID || n.DeliveryID == "" {
		t.Fatal("notification", n, err)
	}
	// No notifier remains an honest not_configured, while observation is started.
	unconfigured, err := invoke("--key", "no-notifier", "--", "literal")
	if err != nil || unconfigured.Delivery != "not_configured" {
		t.Fatal(unconfigured, err)
	}
	wait(t, ctx, func() bool {
		var current profileReceipt
		return readProfileJSON(filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", unconfigured.Key}), "receipt.json"), &current) == nil && current.Status == "succeeded"
	})
	// A result check failure must become a failed job and failed notification.
	if err = os.WriteFile(path, []byte(strings.Replace(profile, "check: [test, -s, result]", "check: [false]", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	failed, err := invoke("--key", "bad-result", "--notify", notifier, "--", "literal")
	if err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var current profileReceipt
		return readProfileJSON(filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", failed.Key}), "receipt.json"), &current) == nil && current.DeliveryStatus == "delivered" && current.Status == "failed"
	})
	t.Log("local dev: frozen source, result check/download, missing notifier, detached delivery, caller replay and single notification")
}

func exitCodeForTest(err error) int {
	var e *exec.ExitError
	if errors.As(err, &e) {
		return e.ExitCode()
	}
	if err == nil {
		return 0
	}
	return -1
}

func TestProfileQueueFailurePersistsIntent(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "fixture.yaml")
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	profile := `name: fixture
project: test
class: experiment
source: {directory: source}
run: {command: [false]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	// Nonstandard location uses cwd, so make the declared source absolute.
	profile = strings.Replace(profile, "directory: source", "directory: "+filepath.Join(root, "source"), 1)
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	// Put the profile at the conventional location to retain all caller state in root.
	dir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(dir, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	var out bytes.Buffer
	err := runProfile(context.Background(), path, nil, "stable", "", "", "", server.URL, "fixture", &out)
	if exitCode(err) != 69 {
		t.Fatal(err)
	}
	var receipt profileReceipt
	if err = json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Key != "stable" || receipt.JobID != "" || receipt.Status != "intent" {
		t.Fatal("lost intent", receipt)
	}
	var saved profileReceipt
	if err = readProfileJSON(filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", "stable"}), "receipt.json"), &saved); err != nil || len(saved.Spec.Run.Command) == 0 {
		t.Fatal("intent not persisted", err)
	}
}
