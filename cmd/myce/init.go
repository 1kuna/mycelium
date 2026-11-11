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
	"strings"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

// initCommand writes the smallest adapter: one entry whose command runs in the
// environment the project's lock file describes. No wrapper, marker, target
// list or needs: Mycelium places the entry wherever it fits (#1341, #853).
// It only inspects files and validates; it never submits.
func initCommand(args []string, out, errOut io.Writer) error {
	f := flag.NewFlagSet("init", flag.ContinueOnError)
	f.SetOutput(errOut)
	entry := f.String("entry", "test", "entry name")
	lock := f.String("lock", "", "lock file to use when the project has both pixi.lock and uv.lock")
	python := f.String("python", "", "Python version for a uv lock (default: .python-version, then requires-python)")
	if err := f.Parse(args); err != nil {
		return err
	}
	if !adapterName.MatchString(*entry) {
		return errors.New("init: entry names use lowercase letters, digits, '.', '_' and '-'")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	environment, err := detectProjectLock(root, *lock, *python)
	if err != nil {
		return err
	}
	command := f.Args()
	if len(command) == 0 {
		if environment == nil {
			return errors.New("init: no pixi.lock or uv.lock found; pass the command to run with installed tools: myce init -- COMMAND ARGS")
		}
		command = []string{"python", "-m", "pytest"}
	}
	for _, arg := range command {
		if command[0] == "" || strings.ContainsRune(arg, '\x00') || filepath.IsAbs(arg) {
			return errors.New("init: command arguments must be portable; use project-relative paths")
		}
	}
	project := strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_-]+`).ReplaceAllString(filepath.Base(root), "-"), "-")
	if project == "" {
		project = "project"
	}
	type initRun struct {
		Command []string `yaml:"command"`
	}
	type initEntry struct {
		Run         initRun           `yaml:"run,flow"`
		Environment *myce.Environment `yaml:"environment,omitempty,flow"`
	}
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err = encoder.Encode(struct {
		Project string               `yaml:"project"`
		Entries map[string]initEntry `yaml:"entries"`
	}{project, map[string]initEntry{*entry: {initRun{command}, environment}}}); err != nil {
		return err
	}
	data := buffer.Bytes()
	a, err := decodeAdapter(data)
	if err != nil {
		return fmt.Errorf("adapter_invalid: %w", err)
	}
	p, _, err := adapterProfile(a, *entry)
	if err != nil {
		return fmt.Errorf("adapter_invalid: %w", err)
	}
	if err = p.Spec.Validate(); err != nil {
		return fmt.Errorf("adapter_invalid: %w", err)
	}
	path := filepath.Join(".mycelium", "adapter.yaml")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("init: cannot create %s (an existing adapter is preserved): %w", path, err)
	}
	if _, err = file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	using := "installed tools"
	if environment != nil {
		using = environment.LockFile
	}
	fmt.Fprintf(out, "Created %s: entry %s runs %s using %s.\nIt runs on any machine type the lock and platform fit; production needs explicit targets.\nIgnore .mycelium/receipts/ and commit the source to run.\nRun: myce run --entry %s\n", path, *entry, strings.Join(command, " "), using, *entry)
	return nil
}

// detectProjectLock returns the inline lock environment for a Pixi or uv
// project root, or nil when neither lock exists.
func detectProjectLock(root, lock, python string) (*myce.Environment, error) {
	has := func(name string) bool {
		info, err := os.Stat(filepath.Join(root, name))
		return err == nil && info.Mode().IsRegular()
	}
	if lock == "" {
		switch {
		case has("pixi.lock") && has("uv.lock"):
			return nil, errors.New("init: both pixi.lock and uv.lock exist; choose one with --lock")
		case has("pixi.lock"):
			lock = "pixi.lock"
		case has("uv.lock"):
			lock = "uv.lock"
		default:
			return nil, nil
		}
	}
	switch lock {
	case "pixi.lock":
		if !has("pixi.lock") || !has("pixi.toml") {
			return nil, errors.New("init: a Pixi environment needs pixi.lock and pixi.toml in the project root")
		}
		return &myce.Environment{Backend: "pixi", LockFile: "pixi.lock"}, nil
	case "uv.lock":
		if !has("uv.lock") || !has("pyproject.toml") {
			return nil, errors.New("init: a uv environment needs uv.lock and pyproject.toml in the project root")
		}
		if python == "" {
			python = projectPython(root)
		}
		if python == "" {
			return nil, errors.New("init: cannot tell the Python version from .python-version or requires-python; pass --python 3.X")
		}
		return &myce.Environment{LockFile: "uv.lock", Python: python}, nil
	}
	return nil, errors.New("init: --lock must be pixi.lock or uv.lock")
}

var pythonMinor = regexp.MustCompile(`(\d+\.\d+)`)

func projectPython(root string) string {
	if b, err := os.ReadFile(filepath.Join(root, ".python-version")); err == nil {
		if m := pythonMinor.FindString(strings.TrimSpace(strings.SplitN(string(b), "\n", 2)[0])); m != "" {
			return m
		}
	}
	b, err := os.ReadFile(filepath.Join(root, "pyproject.toml"))
	if err != nil {
		return ""
	}
	if m := regexp.MustCompile(`(?m)^\s*requires-python\s*=\s*["'][^"']*>=\s*(\d+\.\d+)`).FindSubmatch(b); m != nil {
		return string(m[1])
	}
	return ""
}
