package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProfileInitInference(t *testing.T) {
	for _, tc := range []struct {
		name, file, content string
		want                []string
	}{
		{"go", "go.mod", "module example.org/tiny\n\ngo 1.25.6\n", []string{"go", "test", "./..."}},
		{"requirements", "requirements.txt", "pytest==8.3.5\n", []string{"uv", "run", "--no-project", "--managed-python", "--with", "pytest", "--with-requirements", "requirements.txt", "python", "-m", "pytest"}},
		{"project", "pyproject.toml", "[project]\nname='tiny'\nversion='0.1'\nrequires-python='>=3.12'\n", []string{"uv", "run", "--managed-python", "--with", "pytest", "python", "-m", "pytest"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, tc.file), []byte(tc.content), 0600); err != nil {
				t.Fatal(err)
			}
			got, _, err := inferProfileCommand(root, nil)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("command %q, error %v", got, err)
			}
			if tc.name == "requirements" {
				got, _, err = inferProfileCommand(root, []string{"python", "task.py"})
				want := append(append([]string{}, tc.want[:len(tc.want)-3]...), "python", "task.py")
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("script lost managed dependencies: %q %v", got, err)
				}
			}
		})
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "uv.lock"), []byte("version=1"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := inferProfileCommand(root, nil); err == nil {
		t.Fatal("accepted an incomplete uv project")
	}
	if err := os.WriteFile(filepath.Join(root, "pyproject.toml"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	got, _, err := inferProfileCommand(root, nil)
	if err != nil || !strings.Contains(strings.Join(got, " "), "--locked") {
		t.Fatalf("locked project: %q %v", got, err)
	}
}

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
}
