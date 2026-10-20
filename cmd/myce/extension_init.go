package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func extensionInit(args []string) error {
	f := flag.NewFlagSet("extension init", flag.ContinueOnError)
	name := f.String("name", "example", "extension name")
	version := f.String("version", "1", "immutable version")
	platform := f.String("gpu", "fake", "target backend (fake for laptop CPU smoke)")
	targetOS := f.String("os", runtime.GOOS, "target OS")
	arch := f.String("arch", runtime.GOARCH, "target architecture")
	command := f.String("command", `["{env.app.python}","{extension}/app.py","input.json","result.json"]`, "existing app argv as JSON array")
	check := f.String("check", "[]", "check argv as JSON array; empty uses main smoke run")
	outputs := f.String("outputs", `["result.json"]`, "relative output filenames as JSON array")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("extension init [flags] NEW_FOLDER; see docs/extensions/AGENT-GUIDE.md")
	}
	var argv, checkArgv, outputNames []string
	if err := json.Unmarshal([]byte(*command), &argv); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(*check), &checkArgv); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(*outputs), &outputNames); err != nil {
		return err
	}
	// Authoring fields only; legacy wire fields never enter the skeleton.
	description := map[string]any{
		"name": *name, "version": *version,
		"purpose":       "Wrap an existing app; describe its prerequisites and result contract",
		"default_class": "batch", "outputs": outputNames,
		"variants": []any{map[string]any{
			"target":       myce.ExecutionTarget{OS: *targetOS, Arch: *arch, Platform: *platform},
			"environments": map[string]myce.EnvironmentRecipe{"app": {Backend: "uv", Python: "3.11", LockFile: "requirements.txt"}},
			"needs":        map[string]float64{"memory_gb": 1, "host_memory_gb": 1},
			"execution":    myce.ExecutionSpec{Main: myce.ExecutionMember{Environment: "app", Command: argv}},
			"check":        checkArgv,
		}},
	}
	b, err := yaml.Marshal(description)
	if err != nil {
		return err
	}
	// Validate in an owned temporary folder before reserving the destination.
	folder := f.Arg(0)
	temporary, err := os.MkdirTemp(filepath.Dir(folder), ".extension-init-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	if err = os.WriteFile(filepath.Join(temporary, "extension.yaml"), b, 0600); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(temporary, "requirements.txt"), []byte("# Add the existing app's pinned dependencies. Empty for standard-library apps.\n"), 0600); err != nil {
		return err
	}
	if _, err = myce.ReadExtension(temporary); err != nil {
		return fmt.Errorf("invalid skeleton: %w", err)
	}
	if err = os.Mkdir(folder, 0700); err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(temporary, "extension.yaml"), filepath.Join(folder, "extension.yaml")); err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(temporary, "requirements.txt"), filepath.Join(folder, "requirements.txt")); err != nil {
		return err
	}
	fmt.Printf("created %s; add app/locks, then extension check --static %s; docs/extensions/AGENT-GUIDE.md\n", folder, folder)
	return nil
}
