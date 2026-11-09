package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProfileInitCommandAndSuccessCheck(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	var output bytes.Buffer
	args := []string{"init", "--name", "script", "--environment", "project-tools", "--target", "darwin/arm64", "--", "sh", "task.sh"}
	if err := profileCommand(args, &output, &output); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".mycelium", "profiles", "script.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var p executionProfile
	if err = yaml.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	validation := p.Spec
	validation.Outputs = nil
	if err = validation.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.Environment.Name != "project-tools" || p.Source.Git != "HEAD" || len(p.AllowedMachines) != 0 || p.Targets[0].OS != "darwin" || strings.Contains(string(data), root) {
		t.Fatalf("nonportable starter: %s", data)
	}
	if !strings.Contains(output.String(), "myce run --profile script") {
		t.Fatalf("missing run command: %s", output.String())
	}
	if err = profileCommand(args, &output, &output); err == nil {
		t.Fatal("overwrote existing profile")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(data, after) {
		t.Fatal("existing profile changed")
	}
	run := func(argv []string) error { return exec.Command(argv[0], argv[1:]...).Run() }
	// A real shell script sees the appended test filter as one literal argument.
	if err = os.WriteFile("task.sh", []byte("test \"$1\" = 'two words; exit 99'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = run(append(p.Run.Command, "two words; exit 99")); err != nil {
		t.Fatal(err)
	}
	if err = run(p.Check); err != nil {
		t.Fatal("success marker failed check", err)
	}
	if err = os.WriteFile("task.sh", []byte("exit 17\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = run(p.Run.Command); err == nil {
		t.Fatal("failed command succeeded")
	}
	if err = run(p.Check); err == nil {
		t.Fatal("stale success marker survived failed command")
	}

	t.Run("extension-authoring", func(t *testing.T) {
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

	})
}
