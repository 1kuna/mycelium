package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestHostEnvelopeDevProvisioning(t *testing.T) {
	root, err := filepath.EvalSymlinks(currentTestStateRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	binary := os.Getenv("MYCE_WP3_BINARY")
	if binary == "" {
		binary = filepath.Join(root, "myce")
		if out, err := exec.Command("go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
			t.Fatalf("build: %v %s", err, out)
		}
	}
	// Every process uses an allowlisted environment and isolated paths. Only the
	// GPU/service/exclusion seams are fake; API, SQLite and commands are real.
	dir, extensions := filepath.Join(root, "host"), filepath.Join(root, "extensions")
	models := filepath.Join(root, "models.yaml")
	if err := os.WriteFile(models, []byte("models:\n  - name: fixture-model\n    engine: fake\n    path: fake\n    memory_gb: 1\n    platforms: [fake]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "TMPDIR=" + root, "MYCE_TOKEN=fixture-token", "MYCE_HOST_STATE_DIR=" + dir}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir, cmd.Env = root, env
		cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
		cmd.WaitDelay = time.Second
		return cmd
	}
	run := func(args ...string) []byte {
		t.Helper()
		cmd := command(args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%s: %v; stderr=%s", args[0], err, stderr.Bytes())
		}
		return out
	}
	start := func(cmd *exec.Cmd) func() {
		t.Helper()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		stopped := false
		stop := func() {
			t.Helper()
			if stopped {
				return
			}
			stopped = true
			_ = cmd.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("owned process exit: %v", err)
					if log, ok := cmd.Stderr.(*os.File); ok {
						data, _ := os.ReadFile(log.Name())
						t.Logf("owned process stderr: %s", data)
					}
				}
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("owned process did not exit within teardown budget")
			}
		}
		t.Cleanup(stop)
		return stop
	}
	dev := command("dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "queue.db"), "--checkpoints", filepath.Join(root, "cp"), "--file-store", filepath.Join(root, "files"), "--model-config", models, "--host-state-dir", dir, "--host-id", "fixture-host", "--workers", "1", "--memory-gb", "8")
	stdout, err := dev.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	devLog, err := os.Create(filepath.Join(root, "dev.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { devLog.Close() })
	dev.Stderr = devLog
	stopDev := start(dev)
	endpoint := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			parts := strings.Fields(s.Text())
			if len(parts) >= 3 && parts[0] == "Mycelium" && parts[1] == "dev:" {
				endpoint <- parts[2]
			}
		}
	}()
	var url string
	select {
	case url = <-endpoint:
	case <-ctx.Done():
		t.Fatal("dev startup deadline")
	}
	client := myce.Client{URL: url, Token: "fixture-token", HTTP: &http.Client{Timeout: time.Second}}
	wait := func(predicate func() bool) {
		t.Helper()
		for !predicate() {
			if err := waitPause(ctx, 10*time.Millisecond); err != nil {
				t.Fatal("dev effect deadline", err)
			}
		}
	}
	// The startup line identifies the listener; the first healthy check-in proves
	// the dev worker has finished provisioning before we close that revision.
	wait(func() bool {
		var snapshot myce.Snapshot
		if client.Call(ctx, "GET", "/api/status", nil, &snapshot) != nil {
			return false
		}
		for _, m := range snapshot.Machines {
			if m.ID == "fake-1" && !m.Gone && !m.Leaving && m.HostRevision.Revision > 0 {
				return true
			}
		}
		return false
	})
	spool := filepath.Join(root, "control-spool")
	if err := os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	config := myce.ServiceConfig{Role: "worker", HostID: "fixture-host", State: spool, HostStateDir: dir, Config: filepath.Join(root, "worker.json"), Queue: url, Name: "fake-1"}
	data, _ := json.Marshal(config)
	if err := os.WriteFile(config.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	closeHost := func() myce.HostIntent {
		t.Helper()
		report, err := (myce.HostControl{Dir: dir, Clock: myce.RealClock{}, FreshClose: true, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "fixture close", 0)
		if err != nil || !report.Confirmed {
			t.Fatal("local close", report, err)
		}
		var state myce.HostIntent
		wait(func() bool {
			state, err = myce.ReadHostIntent(dir)
			return err == nil && state.Desired == "closed" && state.HostSettlement() == "confirmed"
		})
		return state
	}
	closed := closeHost()
	opened := validateEnvelopeOutput(t, run("host", "open", "--local", "--config", config.Config, "--expect-revision", strconv.FormatUint(closed.Revision, 10), "--json"))
	if opened.Accepted == nil || *opened.Accepted != closed.Revision+1 || opened.Settlement != "confirmed" {
		t.Fatal("host open did not preserve settled close")
	}

	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"extension.yaml": `name: sample
version: '1'
purpose: Laptop provisioning proof.
platforms: [fake]
kinds: [sample]
memory_gb: 1
resumable: false
run: [sh, check.sh]
setup: [sh, setup.sh]
check: [sh, check.sh]
benchmark: [sh, benchmark.sh]
baselines:
  kind:sample:
    input: input
    output: output
    measure: numeric
    absolute: 0.01
    native: fake fixture
    variation: identical fixture output
`,
		"setup.sh":     "test -z \"$MYCE_TOKEN\" || exit 99\nprintf setup > \"$MYCE_EXTENSION_CACHE/setup-effect\"\n",
		"check.sh":     "test -z \"$MYCE_TOKEN\" || exit 99\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\n",
		"benchmark.sh": "test -z \"$MYCE_TOKEN\" || exit 99\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\nprintf '%s' '{\"units\":8,\"seconds\":2,\"unit\":\"references/s\",\"reference_units\":1}' > \"$MYCE_BENCHMARK_OUTPUT\"\n",
		"input":        "[1]\n", "output": "[1]\n",
	} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	run("extension", "add", "--root", extensions, "--queue", url, source)
	run("extension", "setup", "--root", extensions, "--gpu", "fake", "sample@1")
	run("extension", "check", "--root", extensions, "--gpu", "fake", "sample@1")
	fingerprint, err := myce.ExtensionFingerprint(ctx, "fake", "")
	if err != nil {
		t.Fatal(err)
	}
	reports := myce.LocalExtensionReports(extensions, "fake", fingerprint)
	if len(reports) != 1 || !reports[0].Qualified {
		t.Fatal("extension CLI did not qualify reference", reports)
	}
	var profile myce.CapabilityProfile
	if err := json.Unmarshal(run("benchmark", "--state-dir", filepath.Join(root, "benchmark"), "--host-state-dir", dir, "--extension-root", extensions, "--queue", url, "--gpu", "fake"), &profile); err != nil {
		t.Fatal(err)
	}
	if profile.Benchmark == nil || profile.Benchmark.State != "complete" || len(profile.Benchmark.Measurements) != 5 {
		t.Fatal("benchmark CLI did not complete", profile.Benchmark)
	}
	join := command("join", "--queue", url, "--gpu", "fake", "--name", "fixture-joined", "--host-id", "fixture-host", "--model-config", models, "--memory-mode", "fixed", "--memory-gb", "8", "--heartbeat", "10ms", "--state-dir", filepath.Join(root, "joined"), "--host-state-dir", dir)
	joinLog, err := os.Create(filepath.Join(root, "join.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { joinLog.Close() })
	join.Stdout, join.Stderr = io.Discard, joinLog
	stopJoin := start(join)
	wait(func() bool {
		var snapshot myce.Snapshot
		if client.Call(ctx, "GET", "/api/status", nil, &snapshot) != nil {
			return false
		}
		for _, m := range snapshot.Machines {
			if m.ID == "fixture-joined" && !m.Gone && !m.Leaving {
				return true
			}
		}
		return false
	})
	marker := filepath.Join(root, "joined-effect")
	spec := myce.Spec{Name: "fixture-job", Project: "fixture", Priority: "experiment", AllowedMachines: []string{"fixture-joined"}}
	spec.Needs.MemoryGB = 1
	spec.Needs.Platform.Any = true
	spec.Run.Command = []string{"sh", "-c", "test -z \"$MYCE_TOKEN\" || exit 99; printf done > \"$1\"", "fixture", marker}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	var job myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		var snapshot myce.Snapshot
		if client.Call(ctx, "GET", "/api/status", nil, &snapshot) != nil {
			return false
		}
		for _, j := range snapshot.Jobs {
			if j.ID == job.ID && j.State == "succeeded" && j.Machine == "fixture-joined" {
				return true
			}
		}
		return false
	})
	if effect, err := os.ReadFile(marker); err != nil || string(effect) != "done" {
		t.Fatal("joined worker effect missing", err)
	}
	stopJoin()
	final := closeHost()
	if final.LastClose == nil || final.LastClose.Revision != final.Revision || final.LastClose.Settlement != "confirmed" {
		t.Fatal("final close not settled")
	}
	stopDev()
	t.Log("production dev/open, extension setup/check, five-resource benchmark, joined-worker command effect, and final CLOSED/confirmed; owned processes exited")
}
