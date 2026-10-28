package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

type adapterConversionSource struct{ kind, path string }

// Conversion changes authored files only. In particular it never registers a
// recipe, migrates a report, or changes a saved operation's frozen identity.
func adapterCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 || args[0] != "convert" {
		return errors.New("usage: myce adapter convert --profile PATH | --extension DIR [--project NAME] [--target OS/ARCH/PLATFORM] [--default ENTRY] [--out PATH] [--apply]")
	}
	f := flag.NewFlagSet("adapter convert", flag.ContinueOnError)
	f.SetOutput(errOut)
	var sources []adapterConversionSource
	for _, kind := range []string{"profile", "extension"} {
		f.Func(kind, "explicit source (repeatable)", func(path string) error {
			if path == "" {
				return errors.New("source path cannot be empty")
			}
			sources = append(sources, adapterConversionSource{kind, path})
			return nil
		})
	}
	project := f.String("project", "", "project for extension-only conversion; must match profiles")
	def := f.String("default", "", "default entry (otherwise first name in sorted order)")
	path := f.String("out", "", "reviewed new output path; no write without --apply")
	apply := f.Bool("apply", false, "create --out exclusively; never overwrite")
	var targets []myce.ExecutionTarget
	f.Func("target", "explicit legacy target OS/ARCH/PLATFORM (repeatable)", func(value string) error {
		parts := strings.Split(value, "/")
		if len(parts) != 3 || !adapterName.MatchString(parts[0]) || !adapterName.MatchString(parts[1]) || !slices.Contains([]string{"cpu", "cuda", "xpu", "metal", "fake"}, parts[2]) {
			return errors.New("target must be exact OS/ARCH/PLATFORM")
		}
		t := myce.ExecutionTarget{OS: parts[0], Arch: parts[1], Platform: parts[2]}
		if slices.Contains(targets, t) {
			return fmt.Errorf("duplicate target %s", value)
		}
		targets = append(targets, t)
		return nil
	})
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || len(sources) == 0 || *apply && *path == "" {
		return errors.New("conversion requires explicit --profile/--extension sources; --apply requires --out PATH")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	// Sort both sources and supplied legacy alternatives so flag order cannot
	// change the reviewed output or which errors are reported first.
	slices.SortFunc(sources, func(a, b adapterConversionSource) int { return strings.Compare(a.kind+":"+a.path, b.kind+":"+b.path) })
	slices.SortFunc(targets, func(a, b myce.ExecutionTarget) int {
		return strings.Compare(a.OS+"/"+a.Arch+"/"+a.Platform, b.OS+"/"+b.Arch+"/"+b.Platform)
	})
	entries := map[string]any{}
	owners := map[string]string{}
	usedTargets := map[myce.ExecutionTarget]bool{}
	var problems []error
	var report []string
	caller := "myce run --adapter ADAPTER_PATH"
	if *path != "" {
		caller = "myce run --adapter " + shellArg(*path)
	}
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		var name, sourceProject string
		var entry map[string]any
		var sourceErrors []error
		if source.kind == "profile" {
			name, sourceProject, entry, sourceErrors = convertAdapterProfile(root, source.path)
		} else {
			name, entry, sourceErrors = convertAdapterExtension(root, source.path, targets, usedTargets)
		}
		for _, err := range sourceErrors {
			problems = append(problems, fmt.Errorf("%s %s: %w", source.kind, source.path, err))
		}
		if sourceProject != "" {
			if *project == "" {
				*project = sourceProject
			} else if *project != sourceProject {
				problems = append(problems, fmt.Errorf("profile %s: conflicting project %q (expected %q)", source.path, sourceProject, *project))
			}
		}
		if name == "" {
			continue
		}
		if previous, exists := owners[name]; exists {
			problems = append(problems, fmt.Errorf("conflicting entry name %q: %s and %s", name, previous, source.path))
		}
		owners[name], entries[name] = source.path, entry
		if source.kind == "profile" {
			report = append(report, fmt.Sprintf("profile %s -> entry %s: project -> header; check -> checks.result; remaining fields preserved. Caller: %s --entry %s (same appended argv and --input).", source.path, name, caller, name))
		} else {
			report = append(report, fmt.Sprintf("extension %s -> entry %s: directory source; default_class -> class; outputs -> filename: results; check -> checks.qualification; complete native recipes preserved. Caller: %s --entry %s with explicit caller inputs. Frozen bundle identity changes: fresh native qualification required; no reports migrated.", source.path, name, caller, name))
		}
	}
	for _, target := range targets {
		if !usedTargets[target] {
			problems = append(problems, fmt.Errorf("--target %s/%s/%s matches no legacy source platform", target.OS, target.Arch, target.Platform))
		}
	}
	if *project == "" {
		problems = append(problems, errors.New("extension sources have no project: supply --project NAME or a profile with the same project"))
	}
	if *def == "" && len(entries) > 0 {
		names := make([]string, 0, len(entries))
		for name := range entries {
			names = append(names, name)
		}
		slices.Sort(names)
		*def = names[0]
	}
	if *path != "" {
		if _, err := os.Lstat(*path); err == nil {
			problems = append(problems, fmt.Errorf("output %s already exists; choose a new file (sources are never overwritten)", *path))
		} else if !os.IsNotExist(err) {
			problems = append(problems, err)
		}
	}
	if len(problems) > 0 {
		return conversionFailure(errOut, problems)
	}
	data, err := yaml.Marshal(map[string]any{"schema": 1, "project": *project, "default": *def, "entries": entries})
	if err != nil {
		return err
	}
	if _, err = decodeAdapter(data); err != nil {
		return conversionFailure(errOut, []error{fmt.Errorf("converted adapter cannot represent these sources: %w", err)})
	}
	for _, line := range report {
		fmt.Fprintln(errOut, line)
	}
	fmt.Fprintf(errOut, "Project: %s; default entry: %s. Review source roots, caller inputs and the YAML before apply. Existing profiles/extensions, queued jobs, keys, receipts and checkpoints are unchanged.\n", *project, *def)
	if _, err = out.Write(data); err != nil {
		return err
	}
	if !*apply {
		if *path != "" {
			fmt.Fprintf(errOut, "Plan: create %s; no files written. Repeat with --apply after review.\n", *path)
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// O_EXCL also refuses symlinks and a destination created after the preview.
	file, err := os.OpenFile(*path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("cannot create adapter (no files overwritten): %w", err)
	}
	_, writeErr := file.Write(data)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return fmt.Errorf("creating adapter failed; inspect new file %s: %w", *path, err)
	}
	fmt.Fprintf(errOut, "Created %s exclusively.\n", *path)
	return nil
}

func conversionFailure(out io.Writer, problems []error) error {
	for _, err := range problems {
		fmt.Fprintf(out, "Conversion refused: %v\n", err)
	}
	return fmt.Errorf("adapter conversion refused (%d errors); no files written", len(problems))
}

// Use strict typed decoding to retain the old grammar, and the authored map to
// retain scalar/catalog forms, explicit zeros, and existing inline recipes.
func readConversionYAML(path string, value any) (map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, errors.New("description exceeds 1 MiB")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(value); err != nil {
		return nil, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("expected one YAML description")
	}
	var authored map[string]any
	err = yaml.Unmarshal(data, &authored)
	return authored, err
}

func convertAdapterProfile(root, path string) (string, string, map[string]any, []error) {
	var p executionProfile
	entry, err := readConversionYAML(path, &p)
	if err != nil {
		return "", "", nil, []error{err}
	}
	var problems []error
	if !adapterName.MatchString(p.Name) {
		problems = append(problems, errors.New("profile name must be a valid entry name; no filename-derived name is guessed"))
	}
	if p.Project == "" || p.Class == "" || len(p.Targets) == 0 || len(p.Run.Command) == 0 || p.Run.Command[0] == "" || len(p.Check) == 0 || p.Check[0] == "" {
		problems = append(problems, errors.New("profile requires project, class, exact targets, run.command and check"))
	}
	// The old profile reader supports only finite plain commands. Do not turn a
	// previously invalid profile into a different native/execution contract.
	if p.Execution != nil || len(p.Run.Sidecars) > 0 {
		problems = append(problems, errors.New("profile execution/sidecars are not representable by the old plain-command profile contract"))
	}
	modes := 0
	for _, value := range []string{p.Source.Git, p.Source.Directory, p.Source.Snapshot} {
		if value != "" {
			modes++
		}
	}
	if modes != 1 || p.Source.Paths != nil && (len(p.Source.Paths) == 0 || p.Source.Git == "") {
		problems = append(problems, errors.New("source requires exactly one of git, directory or snapshot; paths require git"))
	}
	// Profiles under .mycelium/profiles resolve from their containing project;
	// other explicit profile paths already resolve from the invocation root.
	abs, _ := filepath.Abs(path)
	oldRoot := root
	if filepath.Base(filepath.Dir(abs)) == "profiles" && filepath.Base(filepath.Dir(filepath.Dir(abs))) == ".mycelium" {
		oldRoot = filepath.Dir(filepath.Dir(filepath.Dir(abs)))
	}
	if oldRoot != root {
		if p.Source.Git != "" {
			problems = append(problems, errors.New("Git profile belongs to a different root; invoke conversion from that project to preserve its source"))
		}
		if source, ok := entry["source"].(map[string]any); ok {
			for _, field := range []string{"directory", "snapshot"} {
				if value, ok := source[field].(string); ok && !filepath.IsAbs(value) {
					source[field], _ = filepath.Rel(root, filepath.Join(oldRoot, value))
				}
			}
		}
		if inputs, ok := entry["inputs"].(map[string]any); ok {
			for name, value := range inputs {
				if local, ok := value.(string); ok && !filepath.IsAbs(local) {
					inputs[name], _ = filepath.Rel(root, filepath.Join(oldRoot, local))
				}
			}
		}
	}
	delete(entry, "project")
	entry["checks"] = map[string]any{"result": entry["check"]}
	delete(entry, "check")
	if err := validateConvertedEntry(entry); err != nil {
		problems = append(problems, err)
	}
	for _, target := range p.Targets {
		if !adapterName.MatchString(target.OS) || !adapterName.MatchString(target.Arch) || !slices.Contains([]string{"cpu", "cuda", "xpu", "metal", "fake"}, target.Platform) {
			problems = append(problems, errors.New("profile targets require exact OS/arch/platform; no targets are guessed"))
		}
	}
	return p.Name, p.Project, entry, problems
}

func convertAdapterExtension(root, folder string, targets []myce.ExecutionTarget, used map[myce.ExecutionTarget]bool) (string, map[string]any, []error) {
	var e myce.Extension
	entry, err := readConversionYAML(filepath.Join(folder, "extension.yaml"), &e)
	if err != nil {
		return "", nil, []error{err}
	}
	var problems []error
	if _, err := myce.ReadExtension(folder); err != nil {
		problems = append(problems, err)
	}
	// ReadExtension validates the frozen schema/closure but normalizes and
	// snapshots environments. Keep its returned values out of authored YAML.
	problems = append(problems, missingConversionBytes(folder, e)...)
	if len(e.Benchmark) > 0 {
		problems = append(problems, errors.New("legacy benchmark has no adapter equivalent; author explicit references/sweeps rather than guessing"))
	}
	abs, err := filepath.Abs(folder)
	if err != nil {
		problems = append(problems, err)
	}
	directory, err := filepath.Rel(root, abs)
	if err != nil {
		problems = append(problems, err)
	}
	entry["source"] = map[string]any{"directory": filepath.ToSlash(directory)}
	if class, ok := entry["default_class"]; ok {
		entry["class"] = class
	}
	delete(entry, "default_class")
	if _, ok := entry["outputs"]; ok {
		outputs := map[string]string{}
		for _, name := range e.Outputs {
			outputs[name] = "results" // Existing extension output-list convention.
		}
		entry["outputs"] = outputs
	}
	if len(e.Variants) > 0 {
		for _, value := range entry["variants"].([]any) {
			v := value.(map[string]any)
			if check, ok := v["check"]; ok {
				v["checks"] = map[string]any{"qualification": check}
				delete(v, "check")
			}
		}
		// These may be explicitly empty in serialized composed author files.
		// ReadExtension rejects nonempty legacy fields mixed with variants.
		for _, field := range []string{"platforms", "memory_gb", "host_memory_gb", "run", "setup", "check", "baselines", "platform_baselines", "benchmark"} {
			delete(entry, field)
		}
	} else {
		var variants []any
		for _, platform := range e.Platforms {
			matched := false
			for _, target := range targets {
				if target.Platform != platform {
					continue
				}
				matched, used[target] = true, true
				v := map[string]any{"target": target, "needs": map[string]any{"memory_gb": e.MemoryGB, "host_memory_gb": e.HostMemoryGB}, "execution": map[string]any{"main": map[string]any{"command": e.Run}}}
				for _, field := range []string{"setup", "check", "baselines"} {
					if value, ok := entry[field]; ok {
						v[field] = value
					}
				}
				if check, ok := v["check"]; ok {
					v["checks"] = map[string]any{"qualification": check}
					delete(v, "check")
				}
				if len(e.PlatformBaselines) > 0 {
					if baselines, ok := e.PlatformBaselines[platform]; ok {
						v["baselines"] = baselines
					}
				}
				variants = append(variants, v)
			}
			if !matched {
				problems = append(problems, fmt.Errorf("legacy platform %s has no exact OS/arch: supply --target OS/ARCH/%s for each intended alternative", platform, platform))
			}
		}
		entry["variants"] = variants
		for _, field := range []string{"platforms", "memory_gb", "host_memory_gb", "run", "setup", "check", "baselines", "platform_baselines", "benchmark"} {
			delete(entry, field)
		}
	}
	if err := validateConvertedEntry(entry); err != nil {
		problems = append(problems, err)
	}
	return e.Name, entry, problems
}

func validateConvertedEntry(entry map[string]any) error {
	data, err := yaml.Marshal(entry)
	if err != nil {
		return err
	}
	var converted adapterEntry
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&converted); err != nil {
		return err
	}
	if err := validateAdapterEntry(converted); err != nil {
		return err
	}
	_, _, err = adapterProfile(projectAdapter{Project: "conversion", Entries: map[string]adapterEntry{"converted": converted}}, "converted")
	return err
}

// Check every explicit native file reference, collecting missing bytes rather
// than allowing the first missing baseline/lock to hide the rest. Command names
// and arbitrary argv are never interpreted as guessed filenames.
func missingConversionBytes(folder string, e myce.Extension) []error {
	names := map[string]bool{}
	command := func(argv []string) {
		for _, value := range argv {
			// A literal extension path is an explicit reference, including spaces.
			// Do not parse shell fragments or expand dynamic placeholders.
			if name, ok := strings.CutPrefix(value, "{extension}/"); ok && !strings.ContainsAny(name, "{}") {
				names[strings.TrimSuffix(name, "/")] = true
			}
		}
	}
	baselines := func(values map[string]myce.ExtensionBaseline) {
		for _, b := range values {
			names[b.Input], names[b.Output] = true, true
		}
	}
	command(e.Run)
	command(e.Setup)
	command(e.Check)
	command(e.Benchmark)
	baselines(e.Baselines)
	for _, values := range e.PlatformBaselines {
		baselines(values)
	}
	for _, v := range e.Variants {
		command(v.Setup)
		command(v.Check)
		command(v.Execution.Main.Command)
		for _, sidecar := range v.Execution.Sidecars {
			command(sidecar.Command)
		}
		for _, sweep := range v.Sweeps {
			command(sweep.Command)
			command(sweep.Run)
		}
		baselines(v.Baselines)
		for _, recipe := range v.Environments {
			command(recipe.Build)
			for _, name := range []string{recipe.LockFile, recipe.Manifest} {
				if name != "" {
					names[name] = true
				}
			}
			switch filepath.Base(recipe.LockFile) {
			case "uv.lock", "pyproject.toml":
				names[filepath.Join(filepath.Dir(recipe.LockFile), "uv.lock")] = true
				names[filepath.Join(filepath.Dir(recipe.LockFile), "pyproject.toml")] = true
			case "pixi.lock":
				names[filepath.Join(filepath.Dir(recipe.LockFile), "pixi.toml")] = true
			}
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	slices.Sort(ordered)
	var problems []error
	for _, name := range ordered {
		if !profileRelative(name) {
			problems = append(problems, fmt.Errorf("native reference %q must name bundled relative bytes", name))
			continue
		}
		info, err := os.Lstat(filepath.Join(folder, name))
		if err != nil || !info.Mode().IsRegular() && !info.IsDir() {
			problems = append(problems, fmt.Errorf("missing native bytes %q (requires a bundled file or directory)", name))
		}
	}
	return problems
}
