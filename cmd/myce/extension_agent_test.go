package main

import (
	"bufio"
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

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func TestExtensionAgentDevEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("uv"); err != nil {
		t.Fatal("agent journey requires uv: ", err)
	}
	root := t.TempDir()
	binary := os.Getenv("MYCE_DEV_SMOKE_BINARY")
	if binary == "" {
		binary = devTestBinary(t)
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const token = "agent-journey-test-token"
	t.Setenv("MYCE_TOKEN", token)
	extensionRoot := filepath.Join(root, "extensions")
	cmd := exec.CommandContext(ctx, binary, "dev", "--listen", "127.0.0.1:0", "--db", filepath.Join(root, "state", "queue.db"), "--checkpoints", filepath.Join(root, "checkpoints"), "--file-store", filepath.Join(root, "files"), "--extension-root", extensionRoot, "--host-id", "dev-host", "--workers", "1", "--memory-gb", "8")
	cmd.Env = os.Environ()
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	devLog, err := os.Create(filepath.Join(root, "dev.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer devLog.Close()
	cmd.Stderr = devLog
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Error("dev exit", err)
			}
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			<-done
			t.Error("dev shutdown deadline")
		}
		b, _ := os.ReadFile(devLog.Name())
		t.Logf("dev log:\n%s", b)
	}()
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) >= 3 && fields[0] == "Mycelium" && fields[1] == "dev:" {
				select {
				case address <- fields[2]:
				default:
				}
			}
		}
	}()
	var queue string
	select {
	case queue = <-address:
	case <-ctx.Done():
		t.Fatal("dev startup", ctx.Err())
	}
	t.Setenv("MYCE_QUEUE", queue)
	run := func(args ...string) {
		t.Helper()
		c := exec.CommandContext(ctx, binary, args...)
		c.Env = os.Environ()
		out, err := c.CombinedOutput()
		t.Logf("myce %s\n%s", strings.Join(args, " "), out)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Scripted fresh-agent flow: inspect the existing app and copy it unchanged,
	// create the skeleton, then author the complete target from the guide example.
	folder := filepath.Join(root, "extension")
	run("extension", "init", "--name", "http-stage", folder)
	sample := filepath.Join("..", "..", "examples", "extensions", "http-stage")
	entries, err := os.ReadDir(sample)
	if err != nil {
		t.Fatal(err)
	}
	var original []byte
	for _, entry := range entries {
		b, err := os.ReadFile(filepath.Join(sample, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Name() == "app.py" {
			original = bytes.Clone(b)
		}
		if entry.Name() == "extension.yaml" {
			var e myce.Extension
			if err = yaml.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			e.Variants[0].Target.OS = runtime.GOOS
			e.Variants[0].Target.Arch = runtime.GOARCH
			b, err = yaml.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err = os.WriteFile(filepath.Join(folder, entry.Name()), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run("extension", "check", "--static", folder)
	run("extension", "add", "--root", extensionRoot, folder)
	run("extension", "check", "--root", extensionRoot, "http-stage@1")
	python, err := exec.LookPath("python3.11")
	if err != nil {
		python, err = exec.LookPath("python3")
	}
	if err != nil {
		t.Fatal(err)
	}
	script := `import json, sys
from pathlib import Path
from mycelium import Client
c=Client(poll_interval=.02)
j=c.run("http-stage@1", inputs={"input.json":sys.argv[1]}, outputs=sys.argv[2], project="demo", class_="batch", key="revision-1", timeout=45)
assert c.run("http-stage@1", inputs={"input.json":sys.argv[1]}, outputs=sys.argv[2], project="demo", class_="batch", key="revision-1", timeout=5)["id"]==j["id"]
assert json.loads((Path(sys.argv[2])/"result.json").read_text())=={"value":3}
ack=c.cancel_submission("demo","never-submit")
assert ack["will_not_run"] and ack["state"]=="absent"
print(j["id"])
`
	c := exec.CommandContext(ctx, python, "-c", script, filepath.Join(folder, "input.json"), filepath.Join(root, "python-results"))
	clientPath, err := filepath.Abs(filepath.Join("..", "..", "clients", "python", "src"))
	if err != nil {
		t.Fatal(err)
	}
	c.Env = append(os.Environ(), "PYTHONPATH="+clientPath)
	out, err := c.CombinedOutput()
	t.Logf("Python client result:\n%s", out)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(out))
	client := myce.Client{URL: queue, Token: token}
	job, err := client.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := client.Run(ctx, "http-stage@1", map[string]string{"input.json": filepath.Join(folder, "input.json")}, filepath.Join(root, "go-results"), myce.RunOptions{Project: "demo", Class: "batch", Key: "revision-1"})
	if err != nil || replay.ID != job.ID {
		t.Fatal("Go run keyed replay", err)
	}
	run("logs", myce.ShortID(id))
	run("wait", myce.ShortID(id), "--timeout", "5s")
	cache := filepath.Join(root, "state", "workers", "fake-1", "environment-cache")
	ready, err := filepath.Glob(filepath.Join(cache, "*", "ready.json"))
	if err != nil || len(ready) != 1 {
		t.Fatalf("main/check/sidecar/job should share one prepared environment: %v %v", ready, err)
	}
	if myce.JobAge(job, time.Now()) == "duration 0s" {
		t.Fatal("nonzero run rounded to zero duration")
	}
	b, err := os.ReadFile(filepath.Join(root, "go-results", "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]int
	if err = json.Unmarshal(b, &result); err != nil || result["value"] != 3 {
		t.Fatal("Go result", string(b), err)
	}
	if ack, err := client.CancelSubmission(ctx, "demo", "go-never-submit"); err != nil || !ack.WillNotRun {
		t.Fatal("Go cancel by key", err)
	}
	unchanged, err := os.ReadFile(filepath.Join(folder, "app.py"))
	if err != nil || !bytes.Equal(unchanged, original) {
		t.Fatal("existing app changed", err)
	}
	// A successful smoke command must never print PASS or qualify its version.
	smoke, err := myce.ReadExtension(folder)
	if err != nil {
		t.Fatal(err)
	}
	smoke.Version = "2"
	smoke.Variants[0].Check = nil
	smoke.Variants[0].Baselines = nil
	smoke.Variants[0].Execution.Main.Command = []string{"sh", "-c", "exit 0"}
	smokeYAML, err := yaml.Marshal(smoke)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(folder, "extension.yaml"), smokeYAML, 0600); err != nil {
		t.Fatal(err)
	}
	run("extension", "add", "--root", extensionRoot, folder)
	smokeCommand := exec.CommandContext(ctx, binary, "extension", "check", "--root", extensionRoot, "http-stage@2")
	smokeCommand.Env = os.Environ()
	smokeOutput, smokeErr := smokeCommand.CombinedOutput()
	t.Logf("smoke check exit: %v\n%s", smokeErr, smokeOutput)
	if smokeErr == nil || strings.Contains(string(smokeOutput), "PASS check") || !strings.Contains(string(smokeOutput), "ran OK but not qualified: declare a check command") || !strings.Contains(string(smokeOutput), "baselines[extension:http-stage]") {
		t.Fatalf("false smoke PASS: %v %s", smokeErr, smokeOutput)
	}
}

func TestExtensionInitAndStaticCLI(t *testing.T) {
	folder := filepath.Join(t.TempDir(), "new-extension")
	if err := extensionCommand([]string{"init", "--name", "fixture", "--outputs", `["profile.json","report.csv"]`, folder}); err != nil {
		t.Fatal(err)
	}
	if err := extensionCommand([]string{"check", "--static", folder}); err != nil {
		t.Fatal(err)
	}
	if err := extensionCommand([]string{"init", folder}); err == nil {
		t.Fatal("overwrote existing folder")
	}
	if err := extensionCommand([]string{"check", "--static", "--digest", strings.Repeat("0", 64), folder}); err == nil {
		t.Fatal("accepted wrong digest")
	}
	f := filepath.Join(folder, "extension.yaml")
	b, _ := os.ReadFile(f)
	var skeleton map[string]any
	if err := yaml.Unmarshal(b, &skeleton); err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{"platforms", "memory_gb", "run"} {
		if _, ok := skeleton[old]; ok {
			t.Fatal("legacy field in skeleton", old)
		}
	}
	variant := skeleton["variants"].([]any)[0].(map[string]any)
	if _, old := variant["needs"].(map[string]any)["platform"]; old {
		t.Fatal("legacy needs.platform")
	}
	outputs := skeleton["outputs"].([]any)
	if len(outputs) != 2 || outputs[0] != "profile.json" || outputs[1] != "report.csv" {
		t.Fatal("ignored --outputs", outputs)
	}
	if variant["environments"] == nil || variant["execution"] == nil || skeleton["outputs"] == nil {
		t.Fatal("incomplete variant skeleton")
	}
	b = append(b, []byte("unknown_field: true\n")...)
	_ = os.WriteFile(f, b, 0600)
	if err := extensionCommand([]string{"check", "--static", folder}); err == nil {
		t.Fatal("unknown schema field accepted")
	}
}
