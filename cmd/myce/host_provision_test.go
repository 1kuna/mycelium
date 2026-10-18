package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestFreshHostCLIProvisioningMessage(t *testing.T) {
	// Run the production executable: neither package's TestMain may provision it.
	binary := filepath.Join(t.TempDir(), "myce")
	if out, err := exec.Command("go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	source, root := t.TempDir(), t.TempDir()
	manifest := `name: sample
version: '1'
purpose: Test provisioning diagnostics with fake resources.
platforms: [fake]
kinds: [sample]
memory_gb: 1
resumable: false
run: [sh, -c, "exit 0"]
setup: [sh, -c, "exit 0"]
check: [sh, -c, "exit 0"]
baselines:
  kind:sample:
    input: input
    output: output
    measure: numeric
    absolute: 0.01
    native: fake fixture
    variation: identical fixture output
`
	for name, contents := range map[string]string{"extension.yaml": manifest, "input": "[1]\n", "output": "[1]\n"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	e, err := myce.ReadExtension(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := myce.InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"benchmark", "--gpu", "fake", "--state-dir", t.TempDir()},
		{"join", "--gpu", "fake", "--state-dir", t.TempDir()},
		{"extension", "setup", "--gpu", "fake", "--root", root, "sample@1"},
		{"extension", "check", "--gpu", "fake", "--root", root, "sample@1"},
	} {
		name := args[0]
		if name == "extension" {
			name += "-" + args[1]
		}
		t.Run(name, func(t *testing.T) {
			if name == "extension-check" {
				// Prepare only the extension fixture in the test host. The CLI
				// child still runs with an empty home and no test host override.
				if _, err := myce.PrepareExtension(context.Background(), root, e, "fake", "", false, io.Discard); err != nil {
					t.Fatal(err)
				}
			}
			home := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, args...)
			for _, value := range os.Environ() {
				if strings.HasPrefix(value, "HOME=") || strings.HasPrefix(value, "MYCE_") {
					continue
				}
				cmd.Env = append(cmd.Env, value)
			}
			cmd.Env = append(cmd.Env, "HOME="+home)
			out, err := cmd.CombinedOutput()
			if err == nil || ctx.Err() != nil {
				t.Fatalf("expected prompt refusal: %v, deadline: %v", err, ctx.Err())
			}
			if !strings.Contains(string(out), "this host has no Mycelium host state yet; provision it with `myce host open` or `myce dev`") || strings.Contains(string(out), "convert offline") {
				t.Fatalf("misleading fresh-host error: %s", out)
			}
			if _, err := os.Stat(filepath.Join(home, ".local", "share", "myce", "host-state.json")); !os.IsNotExist(err) {
				t.Fatal("command silently provisioned host state", err)
			}
		})
	}
}
