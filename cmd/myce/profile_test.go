package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
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
portable: true
targets: [{os: ` + runtime.GOOS + `}]
outputs: {result: results}
check: [test, -s, result]
`
	path := filepath.Join(profileDir, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	notifierFIFO := filepath.Join(root, "notifier-fifo")
	if err := syscall.Mkfifo(notifierFIFO, 0600); err != nil {
		t.Fatal(err)
	}
	notifierPID := filepath.Join(root, "notifier-child")
	defer func() {
		if b, e := os.ReadFile(notifierPID); e == nil {
			var pid int
			if _, e = fmt.Sscanf(string(b), "%d", &pid); e == nil {
				_ = syscall.Kill(pid, syscall.SIGTERM)
			}
		}
	}()
	notifier := filepath.Join(root, "notify")
	if err := os.WriteFile(notifier, []byte(`#!/bin/sh
if [ "${1:-}" = --check ]; then test -n "${NOTIFICATION_FILE:-}"; exit $?; fi
test -z "${MYCE_TOKEN:-}" || exit 99
if [ ! -f "$NOTIFIER_PID" ]; then
 sh -c 'read value < "$NOTIFIER_FIFO"' &
 echo "$!" > "$NOTIFIER_PID"
fi
cat >> "$NOTIFICATION_FILE"
printf '\n' >> "$NOTIFICATION_FILE"
`), 0700); err != nil {
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
		cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token, "NOTIFICATION_FILE="+notificationFile, "NOTIFIER_FIFO="+notifierFIFO, "NOTIFIER_PID="+notifierPID)
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
	for _, upload := range missing.Uploads {
		t.Fatal("accepted upload retained in receipt", upload)
	}
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", missing.Key}))
	if _, e := os.Stat(filepath.Join(dir, "source.tar")); !os.IsNotExist(e) {
		t.Fatal("accepted upload copy retained", e)
	}
	// Recover that exact job with configured delivery and literal argv.
	r, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	if err != nil || r.JobID != missing.JobID || r.Delivery == "not_configured" || r.Nonce == "" {
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
	wait(t, ctx, func() bool {
		lock, e := profileLock(filepath.Join(dir, "watch.lock"))
		if e != nil {
			return false
		}
		lock.Close()
		return true
	})
	childBytes, e := os.ReadFile(notifierPID)
	if e != nil {
		t.Fatal(e)
	}
	var childPID int
	if _, e = fmt.Sscanf(string(childBytes), "%d", &childPID); e != nil || syscall.Kill(childPID, 0) != nil {
		t.Fatal("notifier child did not survive for descriptor regression", e)
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

	// A new receipt root for the same intent adopts the terminal queue binding.
	other := t.TempDir()
	otherProfiles := filepath.Join(other, ".mycelium", "profiles")
	if err = os.MkdirAll(otherProfiles, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(other, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(filepath.Join(root, "source", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(other, "source", "run.sh"), script, 0600); err != nil {
		t.Fatal(err)
	}
	otherPath := filepath.Join(otherProfiles, "fixture.yaml")
	if err = os.WriteFile(otherPath, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	originalPath := path
	path = otherPath
	adopted, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	path = originalPath
	if err != nil || !adopted.SubmissionReused || adopted.JobID != r.JobID || adopted.Nonce == r.Nonce {
		t.Fatal("cross-root terminal adoption", adopted, err)
	}
	adoptedReceipt := filepath.Join(other, ".mycelium", "receipts", profileHash([]string{"test", "missing"}), "receipt.json")
	wait(t, ctx, func() bool {
		var saved profileReceipt
		return readProfileJSON(adoptedReceipt, &saved) == nil && saved.DeliveryStatus == "delivered" && saved.SubmissionReused
	})
	notifications, err = os.ReadFile(notificationFile)
	if err != nil {
		t.Fatal(err)
	}
	var events []profileNotification
	for _, line := range bytes.Split(bytes.TrimSpace(notifications), []byte("\n")) {
		var event profileNotification
		if err = json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if len(events) != 2 || events[0].DeliveryID == events[1].DeliveryID || events[0].JobID != events[1].JobID {
		t.Fatal("per-receipt delivery identity", events)
	}
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
	if err = os.WriteFile(path, []byte(strings.Replace(profile, "os: "+runtime.GOOS, "os: plan9", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	impossible, err := invoke("--key", "impossible", "--", "literal")
	if exitCodeForTest(err) != 78 || impossible.Status != "intent" || impossible.JobID != "" {
		t.Fatal("impossible target did not retain typed hold", impossible, err)
	}
	// Keep job metadata longer than result bytes so this journey still proves
	// the typed result_expired response and exact expired receipt recovery.
	if err := d.Queue.ConfigureLogRetention(2 * myce.DefaultFileRetention); err != nil {
		t.Fatal(err)
	}
	// The same real profile result remains recoverable until the queue's
	// fake clock advances beyond the documented recovery window.
	if err = d.Client().Call(ctx, "POST", "/api/dev/clock/advance", map[string]any{"seconds": (myce.DefaultFileRetention + time.Second).Seconds()}, nil); err != nil {
		t.Fatal(err)
	}
	var expired *myce.HTTPError
	if err = d.Client().DownloadResult(ctx, r.JobID, "result", filepath.Join(root, "expired")); !errors.As(err, &expired) || expired.Status != 410 || expired.Code != "result_expired" {
		t.Fatal("profile result did not expire loudly", err)
	}
	job, err := d.Client().GetJob(ctx, r.JobID)
	if err != nil || !job.FilesExpired {
		t.Fatal("expiry status missing", job, err)
	}
	for _, ref := range job.Spec.Files {
		expired = nil
		if err = d.Client().HasFile(ctx, ref); !errors.As(err, &expired) || expired.Status != 410 || expired.Code != "result_expired" {
			t.Fatal("profile source not reclaimed", err)
		}
	}
	if err = os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	expiredReceipt, err := invoke("--key", "missing", "--notify", notifier, "--", "literal")
	if exitCodeForTest(err) != 65 || !expiredReceipt.FilesExpired || expiredReceipt.JobID != r.JobID {
		t.Fatal("expired profile replay", expiredReceipt, err)
	}
	t.Log("local dev: frozen source, result check/download, receipt recovery, notification and fake-clock result expiry")
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
	receipts := filepath.Join(root, ".mycelium", "receipts")
	abandoned := filepath.Join(receipts, ".prepare-abandoned")
	if err := os.MkdirAll(abandoned, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(abandoned, "source.tar"), []byte("disposable staging"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := runProfile(context.Background(), path, nil, nil, "stable", "", "", "", server.URL, "fixture", &out, io.Discard)
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
	if _, err = os.Stat(abandoned); !os.IsNotExist(err) {
		t.Fatal("stale preparation retained", err)
	}
	out.Reset()
	if err = runProfile(context.Background(), path, nil, nil, "", "", "", "", server.URL, "fixture", &out, io.Discard); exitCode(err) != 69 {
		t.Fatal(err)
	}
	var generated profileReceipt
	if err = json.Unmarshal(out.Bytes(), &generated); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(receipts, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(receipts, 0700)
	out.Reset()
	err = runProfile(context.Background(), path, nil, nil, "", "", "", "", server.URL, "fixture", &out, io.Discard)
	if exitCode(err) != 69 {
		t.Fatal("existing automatic key attempted to stage again", err)
	}
	var adopted profileReceipt
	if err = json.Unmarshal(out.Bytes(), &adopted); err != nil || adopted.Key != generated.Key {
		t.Fatal("automatic key not adopted", adopted, err)
	}
}

func TestProfileTargetRefusalStderrAndReceipt(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profileDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	profile := `name: fixture
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 2, memory_gb: 8}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	path := filepath.Join(profileDir, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	q, err := myce.Open(filepath.Join(state, "queue.db"), filepath.Join(state, "checkpoints"), &bindClock{now: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureFileStore(filepath.Join(state, "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := myce.Machine{Capabilities: []string{"native_isolation_landlock"}, ID: "worker", Session: "fixture", GPU: "cpu", CPUIsolation: "select-none",
		HostMemoryMeasured: true, HostAvailableMemoryGB: 32, HostMemoryBudgetGB: 8, MemoryCeilingSlackGB: .125,
		CapabilityProfile: &myce.CapabilityProfile{OS: "linux", Arch: "amd64", LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	const token = "refusal-fixture-secret"
	server := httptest.NewServer(myce.API{Queue: q, Token: token}.Handler())
	defer server.Close()
	binary := devTestBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "run", "--profile", path, "--queue", server.URL, "--key", "refused")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	want := "host memory need 8.125 GiB (8 + 0.125 slack) exceeds allowance 8 GiB on worker"
	if exitCodeForTest(err) != 78 || !strings.Contains(stderr.String(), "no_qualified_target: "+want) {
		t.Fatalf("exit=%v stderr=%s", err, &stderr)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out, &receipt); err != nil {
		t.Fatal(err)
	}
	var saved profileReceipt
	receiptPath := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", "refused"}), "receipt.json")
	if err := readProfileJSON(receiptPath, &saved); err != nil {
		t.Fatal(err)
	}
	if receipt.RefusalCode != "no_qualified_target" || !strings.Contains(receipt.RefusalReason, want) || saved.RefusalReason != receipt.RefusalReason || saved.RefusalCode != receipt.RefusalCode {
		t.Fatalf("printed/saved refusal mismatch: %+v / %+v", receipt, saved)
	}
	if strings.Contains(stderr.String(), token) || bytes.Contains(out, []byte(token)) || strings.Contains(receipt.RefusalReason, root) {
		t.Fatal("refusal leaked credential or private path")
	}
}

func TestProfileOSOnlyPrebuiltError(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	// A native amd64 ELF payload, without a compiler or a runnable Linux host.
	data := make([]byte, 64)
	copy(data, "\x7fELF\x02\x01\x01")
	binary.LittleEndian.PutUint16(data[16:], 2)
	binary.LittleEndian.PutUint16(data[18:], 62)
	binary.LittleEndian.PutUint32(data[20:], 1)
	binary.LittleEndian.PutUint16(data[52:], 64)
	if err := os.WriteFile(filepath.Join(root, "source", "program"), data, 0700); err != nil {
		t.Fatal(err)
	}
	p := executionProfile{Spec: myce.Spec{Name: "binary", Project: "test", Class: "experiment", Targets: []myce.ExecutionTarget{{OS: "linux"}}, Run: myce.RunSpec{Command: []string{"./program"}}}, Source: profileSource{Directory: "source"}, Check: []string{"true"}}
	err := runPreparedProfile(context.Background(), root, p, []byte("binary-fixture"), nil, nil, "", "", "", "", "", "", io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "declare portable: true if this runs on any arch, or name the arch") || strings.Contains(err.Error(), root) {
		t.Fatalf("CLI lost actionable/sanitized reason: %v", err)
	}
}
