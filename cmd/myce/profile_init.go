package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"golang.org/x/mod/modfile"
	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

// Initialization only inspects files and validates the starter. Execution and
// environment preparation remain owned by run --profile and the worker.
func profileCommand(args []string, out, errOut io.Writer) error {
	if len(args) == 0 || args[0] != "init" {
		return errors.New("usage: myce profile init [--name NAME] [--environment CATALOG_NAME] [-- COMMAND ARGS...]")
	}
	f := flag.NewFlagSet("profile init", flag.ContinueOnError)
	f.SetOutput(errOut)
	name := f.String("name", "test", "profile filename stem")
	environment := f.String("environment", "mycelium-gate", "managed catalog environment supplying the command's tools (default supplies Go, Python, uv, Git)")
	target := f.String("target", runtime.GOOS+"/"+runtime.GOARCH, "starter OS/arch target; add alternatives only after proving them")
	source := f.String("source-ref", "HEAD", "Git source ref (default requires committed, clean source when run)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`).MatchString(*name) || *environment == "" || *source == "" {
		return errors.New("profile_invalid: use a simple profile name, a managed catalog environment and a Git source ref")
	}
	parts := strings.Split(*target, "/")
	if len(parts) != 2 {
		return errors.New("profile_invalid: target must be OS/ARCH")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	command, tool, err := inferProfileCommand(root, f.Args())
	if err != nil {
		return err
	}
	project := strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_-]+`).ReplaceAllString(filepath.Base(root), "-"), "-")
	if project == "" {
		project = "project"
	}
	// The wrapper forwards appended argv literally. Removing the marker before
	// launch prevents an old committed result from satisfying the success check.
	const marker = ".myce-profile-result.txt"
	script := "set -eu\nrm -f " + marker + "\ncommand -v " + shellArg(tool) + " >/dev/null\n" + shellCommand(command) + " \"$@\"\nprintf 'passed\\n' > " + marker
	doc := map[string]any{
		"name": project + "-" + *name, "project": project, "class": "test",
		"source":      profileSource{Git: *source},
		"targets":     []myce.ExecutionTarget{{OS: parts[0], Arch: parts[1], Platform: "cpu"}},
		"environment": *environment,
		"needs":       map[string]any{"cpu_cores": 1, "memory": "auto", "platform": "cpu"},
		"run":         map[string]any{"command": []string{"sh", "-ec", script, "myce-profile"}, "env": map[string]string{"GOMAXPROCS": "1", "GOFLAGS": "-p=1", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "POLARS_MAX_THREADS": "1"}},
		"outputs":     map[string]string{marker: "results"},
		"check":       []string{"grep", "-qx", "passed", marker},
	}
	data, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	var p executionProfile
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err = d.Decode(&p); err != nil {
		return err
	}
	// Outputs become legal when runProfile lowers source into a file manifest.
	validation := p.Spec
	validation.Outputs = nil
	if err = validation.Validate(); err != nil {
		return fmt.Errorf("profile_invalid: %w", err)
	}
	path := filepath.Join(".mycelium", "profiles", *name+".yaml")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("profile_invalid: cannot create starter (existing profiles are preserved): %w", err)
	}
	_, err = file.Write(append([]byte("# Starter: review targets, dependencies and source before submitting.\n"), data...))
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	fmt.Fprintf(out, "Created %s; profile validation passed.\nCommand: %s\nManaged catalog prerequisite: %s (must be registered on the queue).\nReview the starter, ignore .mycelium/receipts/, and commit intended source for HEAD.\nRun: myce run --profile %s\n", path, shellCommand(command), *environment, *name)
	return nil
}

func inferProfileCommand(root string, command []string) ([]string, string, error) {
	has := func(name string) bool {
		info, err := os.Stat(filepath.Join(root, name))
		return err == nil && info.Mode().IsRegular()
	}
	if len(command) > 0 {
		if command[0] == "" {
			return nil, "", errors.New("profile_invalid: command executable is empty")
		}
		for _, arg := range command {
			if strings.ContainsRune(arg, '\x00') || filepath.IsAbs(arg) {
				return nil, "", errors.New("profile_invalid: command arguments must be portable; use project-relative paths")
			}
		}
	}
	if has("go.mod") {
		if len(command) > 0 {
			return command, command[0], nil
		}
		data, err := os.ReadFile(filepath.Join(root, "go.mod"))
		if err != nil {
			return nil, "", err
		}
		module, err := modfile.Parse("go.mod", data, nil)
		if err != nil || module.Go == nil {
			return nil, "", errors.New("profile_invalid: go.mod must declare a valid Go toolchain version")
		}
		// Go's standard toolchain selection honors go.mod/go.work, including a
		// required newer version than the catalog's base toolchain.
		return []string{"go", "test", "./..."}, "go", nil
	}
	pythonCommand := func(prefix []string) ([]string, string, error) {
		if len(command) == 0 {
			command = []string{"python", "-m", "pytest"}
		}
		if command[0] == "uv" { // The caller supplied its own uv invocation.
			return command, "uv", nil
		}
		return append(prefix, command...), "uv", nil
	}
	if has("uv.lock") {
		if !has("pyproject.toml") {
			return nil, "", errors.New("profile_invalid: uv.lock requires pyproject.toml")
		}
		return pythonCommand([]string{"uv", "run", "--locked", "--managed-python", "--with", "pytest"})
	}
	if has("pyproject.toml") {
		// uv interprets requires-python, dependency groups and .python-version;
		// keep that responsibility with the project's package manager.
		return pythonCommand([]string{"uv", "run", "--managed-python", "--with", "pytest"})
	}
	tests, _ := os.Stat(filepath.Join(root, "tests"))
	if has("requirements.txt") || has("pytest.ini") || (tests != nil && tests.IsDir()) {
		args := []string{"uv", "run", "--no-project", "--managed-python", "--with", "pytest"}
		if has("requirements.txt") {
			args = append(args, "--with-requirements", "requirements.txt")
		}
		return pythonCommand(args)
	}
	if len(command) > 0 {
		return command, command[0], nil
	}
	return nil, "", errors.New("profile_invalid: no Go or Python project found; provide --environment CATALOG_NAME -- COMMAND ARGS...")
}
