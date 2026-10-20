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
	command := f.String("command", `["python3","{extension}/app.py"]`, "existing app argv as JSON array")
	check := f.String("check", "[]", "check argv as JSON array; empty uses main smoke run")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("extension init [flags] NEW_FOLDER; see docs/extensions/AGENT-GUIDE.md")
	}
	var argv, checkArgv []string
	if err := json.Unmarshal([]byte(*command), &argv); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(*check), &checkArgv); err != nil {
		return err
	}
	e := myce.Extension{Name: *name, Version: *version, Purpose: "Wrap an existing app; describe its prerequisites and result contract", DefaultClass: "batch", Variants: []myce.ExtensionVariant{{Target: myce.ExecutionTarget{OS: *targetOS, Arch: *arch, Platform: *platform}, Needs: myce.ResourceNeeds{MemoryGB: 1, HostMemoryGB: 1}, Execution: myce.ExecutionSpec{Main: myce.ExecutionMember{Command: argv}}, Check: checkArgv}}}
	b, err := yaml.Marshal(e)
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
	if _, err = myce.ReadExtension(temporary); err != nil {
		return fmt.Errorf("invalid skeleton: %w", err)
	}
	if err = os.Mkdir(folder, 0700); err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(temporary, "extension.yaml"), filepath.Join(folder, "extension.yaml")); err != nil {
		return err
	}
	fmt.Printf("created %s; add app/locks, then extension check --static %s; docs/extensions/AGENT-GUIDE.md\n", folder, folder)
	return nil
}
