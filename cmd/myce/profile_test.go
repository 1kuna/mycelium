package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mycelium/internal/myce"
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
)

// Receipt/preparation fixtures need a permanent stop before submission. A
// generic 503 is a transient outage and retries until its caller stops it.
func refuseProfilePreparation(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, `{"code":"file_store_unavailable"}`)
}

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
	{
		script, err := os.ReadFile(filepath.Join(root, "source", "run.sh"))
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(http.HandlerFunc(refuseProfilePreparation))
		defer server.Close()
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
		if err := os.RemoveAll(filepath.Join(root, "source")); err != nil {
			t.Fatal(err)
		}
		out.Reset()
		if err = runProfile(context.Background(), path, nil, nil, "stable", "", "", "", server.URL, "fixture", &out, io.Discard); exitCode(err) != 69 {
			t.Fatal(err)
		}
		var adopted profileReceipt
		if err = json.Unmarshal(out.Bytes(), &adopted); err != nil || adopted.Key != receipt.Key {
			t.Fatal("explicit key not adopted", adopted, err)
		}
		if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "source", "run.sh"), script, 0600); err != nil {
			t.Fatal(err)
		}
		{
			source := filepath.Join(root, "source")
			if err := os.MkdirAll(source, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "tool"), []byte("source helper"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "replacement"), []byte("explicit replacement"), 0600); err != nil {
				t.Fatal(err)
			}
			profile := `name: fixture
project: test
class: experiment
source: {directory: source}
inputs: {tool: replacement}
run: {command: [sh, tool]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: darwin, arch: arm64, platform: cpu}]
check: [true]
`
			file := filepath.Join(profileDir, "legacy.yaml")
			if err := os.WriteFile(file, []byte(profile), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err := runProfile(context.Background(), file, nil, nil, "legacy-prelaunch", "", "", "", server.URL, "fixture", &out, io.Discard)
			if exitCode(err) != 69 {
				t.Fatal(err, out.String())
			}
			var receipt profileReceipt
			if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
				t.Fatal(err)
			}
			if !receipt.Spec.PrepareSource || strings.Contains(strings.Join(receipt.Spec.Run.Command, " "), "tar -xf") {
				t.Fatal("source hidden until payload launch", receipt.Spec.Run)
			}
			if _, ok := receipt.Spec.Files["tool"]; !ok {
				t.Fatal("explicit input lost its replacement destination", receipt.Spec.Files)
			}
			if _, ok := receipt.Uploads["files/tool"]; !ok {
				t.Fatal("input upload not bound to destination", receipt.Uploads)
			}
			for _, path := range []string{filepath.Join(source, "tool"), filepath.Join(root, "replacement"), file} {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
		}

		{
			local := filepath.Join(root, "runtime.tar")
			if err := os.WriteFile(local, []byte("runtime"), 0600); err != nil {
				t.Fatal(err)
			}
			profile := `name: shared
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
shared_inputs:
  runtime: {local: runtime.tar, archive: tar, unpacked_bytes: 100}
  original: {nas: media/original.mov, sha256: "` + strings.Repeat("0", 64) + `", size_bytes: 2000000000}
`
			path := filepath.Join(profileDir, "shared.yaml")
			if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err := runProfile(context.Background(), path, nil, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard)
			if exitCode(err) != 69 {
				t.Fatal(err)
			}
			var receipt profileReceipt
			if err = json.Unmarshal(out.Bytes(), &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.Spec.SharedInputs["runtime"].Local != "" || receipt.Spec.SharedInputs["runtime"].SHA256 == "" {
				t.Fatal("local shared input not lowered")
			}
			if len(receipt.Spec.Files) != 1 || len(receipt.Uploads) != 2 || len(receipt.Spec.StoredInputFiles()) != 2 {
				t.Fatal("NAS input uploaded or local input copied per-job", receipt)
			}
			if strings.Contains(receipt.Spec.Run.Command[2], "runtime.tar") || strings.Contains(receipt.Spec.Run.Command[2], "original.mov") {
				t.Fatal("shared input copied by wrapper")
			}
			if err = receipt.Spec.Validate(); err != nil {
				t.Fatal(err)
			}
			// A retained explicit key needs neither source nor local import on replay.
			if err = os.Remove(local); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			if err = runProfile(context.Background(), path, nil, nil, "shared", "", "", "", server.URL, "test", &out, io.Discard); exitCode(err) != 69 {
				t.Fatal(err)
			}

			{
				if err := os.WriteFile(local, []byte("runtime"), 0600); err != nil {
					t.Fatal(err)
				}
				profile = strings.Replace(profile, "local: runtime.tar, archive: tar, unpacked_bytes: 100", "local: runtime.tar, sha256: \""+strings.Repeat("0", 64)+"\"", 1)
				if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
					t.Fatal(err)
				}
				_, findings, err := resolveInput(path, "", "", true)
				if err != nil {
					t.Fatal(err)
				}
				want := "shared input runtime: sha256 differs"
				if len(findings.findings) != 1 || !strings.Contains(findings.findings[0].Problem, want) {
					t.Fatalf("resolve: %+v", findings)
				}
				var out bytes.Buffer
				err = runProfile(context.Background(), path, nil, nil, "", "", "", "", server.URL, "test", &out, io.Discard)
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("got %v", err)
				}
			}
			if err := os.Remove(local); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}

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
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	{
		var host string
		wait(t, ctx, func() bool {
			s, err := status(ctx, d.Client())
			if err == nil && len(s.Machines) == 1 {
				host = s.Machines[0].ID
				return true
			}
			return false
		})
		for _, format := range []string{"adapter"} {
			t.Run(format, func(t *testing.T) {
				root := filepath.Join(root, "reason-input")
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "source", "run.sh"), []byte("printf ok > result\n"), 0600); err != nil {
					t.Fatal(err)
				}
				text := strings.Replace(cpuAdapterYAML("source"), "    source:", fmt.Sprintf("    allowed_machines: [%q]\n    requirement_reasons: {host: 'private explanation /private/input'}\n    source:", host), 1)

				file := filepath.Join(root, format+".yaml")
				if err := os.WriteFile(file, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(ctx, binary, "run", "--"+format, file, "--queue", d.URL, "--key", "reason-"+format)
				command.Dir = root
				command.Env = append(os.Environ(), "MYCE_TOKEN="+token, "MYCE_NOTIFY=")
				var stderr bytes.Buffer
				command.Stderr = &stderr
				out, err := command.Output()
				if err != nil {
					t.Fatal("submission", err, &stderr)
				}
				var receipt profileReceipt
				if err := json.Unmarshal(out, &receipt); err != nil || receipt.Spec.RequirementReasons["host"] != "private explanation /private/input" {
					t.Fatal("receipt lost authored map", err)
				}
				var saved profileReceipt
				path := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{receipt.Project, receipt.Key}), "receipt.json")
				if err := readProfileJSON(path, &saved); err != nil || saved.Spec.RequirementReasons["host"] != receipt.Spec.RequirementReasons["host"] {
					t.Fatal("durable receipt lost map", err)
				}
				got, err := d.Client().Wait(ctx, receipt.JobID)
				if err != nil || got.State != "succeeded" || len(got.RequirementReasons) != 1 || got.RequirementReasons[0] != (myce.RequirementReasonStatus{Constraint: "host", ReasonSource: "authored", Source: "spec.requirement_reasons[host]"}) || len(got.Spec.RequirementReasons) != 0 {
					t.Fatal("job GET lost safe reason/provenance", got.RequirementReasons, err)
				}
				snapshot, err := status(ctx, d.Client())
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, j := range snapshot.Jobs {
					if j.ID == receipt.JobID {
						found = len(j.RequirementReasons) == 1 && j.RequirementReasons[0].ReasonSource == "authored"
						data, _ := json.Marshal(j)
						if bytes.Contains(data, []byte("private")) {
							t.Fatal("raw prose leaked in status")
						}
					}
				}
				if !found {
					t.Fatal("status lost reason provenance")
				}
			})
		}
		// Raw job YAML has only the shared Spec field, not the legacy request map.
		raw := filepath.Join(root, "reason-job.yaml")
		text := fmt.Sprintf("name: raw-reason\nproject: test\nclass: experiment\nrun: {command: [echo, ok]}\nneeds: {cpu_cores: 1, memory_gb: 1, platform: cpu}\nallowed_machines: [%q]\nrequirement_reasons: {host: 'private raw-job explanation'}\n", host)
		if err := os.WriteFile(raw, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		var report bytes.Buffer
		if err := specCommand([]string{"check", "--resolve", "--json", "--queue", d.URL, "--token", token, raw}, &report, &report); err != nil {
			t.Fatal("raw-job resolve", err, &report)
		}
		if strings.Contains(report.String(), "Host pin has no declared") || strings.Contains(report.String(), "private raw-job") {
			t.Fatal("raw-job explanation ignored or leaked", &report)
		}
	}

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
	urgent, err := invoke("--key", "urgent", "--class", "deadline", "--deadline", "2031-02-03T04:05:06Z", "--", "literal")
	if err != nil || urgent.Spec.Class != "deadline" || urgent.Spec.Deadline == nil {
		t.Fatalf("urgent receipt: %+v %v", urgent.Spec, err)
	}
	queued, err := d.Client().GetJob(ctx, urgent.JobID)
	if err != nil || queued.Spec.Class != urgent.Spec.Class || queued.Spec.Deadline == nil || !queued.Spec.Deadline.Equal(*urgent.Spec.Deadline) {
		t.Fatalf("urgent queued spec: %+v %v", queued, err)
	}
	// A supplied notifier that is absent must never report armed.
	missing, err := invoke("--key", "missing", "--notify", filepath.Join(root, "absent"), "--", "literal")
	if exitCodeForTest(err) != 74 || missing.Delivery != "not_configured" || missing.Observation != "not_armed" {
		t.Fatal("missing notifier armed", missing, err)
	}
	unchanged, err := d.Client().GetJob(ctx, missing.JobID)
	if err != nil || unchanged.Spec.Class != "experiment" || unchanged.Spec.Deadline != nil || missing.Spec.Class != unchanged.Spec.Class || missing.Spec.Deadline != nil {
		t.Fatalf("authored urgency: receipt=%+v job=%+v err=%v", missing.Spec, unchanged, err)
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
	// Explicit operator payload expiry exercises typed result_expired recovery;
	// the omitted policy now protects unique results until release.
	const payloadPolicy = 7 * 24 * time.Hour
	if err := d.Queue.ConfigureFileRetention(payloadPolicy); err != nil {
		t.Fatal(err)
	}
	// The same real profile result remains recoverable until the queue's
	// fake clock advances beyond the documented recovery window.
	if err = d.Client().Call(ctx, "POST", "/api/dev/clock/advance", map[string]any{"seconds": (payloadPolicy + time.Second).Seconds()}, nil); err != nil {
		t.Fatal(err)
	}
	// Maintenance yields between records; await this result's durable expiry
	// before asserting the transport/replay error contract.
	wait(t, ctx, func() bool { job, err := d.Client().GetJob(ctx, r.JobID); return err == nil && job.FilesExpired })
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

	{
		dir := filepath.Join(root, "rejected-notification")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		hook := filepath.Join(dir, "notify")
		if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
			t.Fatal(err)
		}
		receipt := profileReceipt{Key: "key", Project: "test", JobID: r.JobID, Status: "running", Observation: "armed", Delivery: "armed"}
		if err := observeProfile(context.Background(), d.Client(), dir, receipt, profileObserver{Notify: hook}); err == nil {
			t.Fatal("delivery rejection hidden")
		}
		var saved profileReceipt
		if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.Status != "succeeded" || saved.DeliveryStatus != "failed" || saved.Observation != "not_armed" || saved.Delivery == "armed" {
			t.Fatal("delivery rejection changed computation state", saved, err)
		}
	}
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
