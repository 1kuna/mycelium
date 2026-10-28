package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

// Adapter recipes reuse the job and native recipe contracts. Only the selected
// entry is frozen: unrelated entries never become submission identity.
type projectAdapter struct {
	Schema  int                     `yaml:"schema"`
	Project string                  `yaml:"project"`
	Default string                  `yaml:"default"`
	Entries map[string]adapterEntry `yaml:"entries"`
}
type adapterChecks struct {
	Result        []string `yaml:"result,omitempty"`
	Qualification []string `yaml:"qualification,omitempty"`
}
type adapterVariant struct {
	Target       myce.ExecutionTarget              `yaml:"target"`
	Environments map[string]myce.EnvironmentRecipe `yaml:"environments,omitempty"`
	Needs        myce.ResourceNeeds                `yaml:"needs"`
	Execution    myce.ExecutionSpec                `yaml:"execution"`
	Setup        []string                          `yaml:"setup,omitempty"`
	Checks       adapterChecks                     `yaml:"checks,omitempty"`
	Baselines    map[string]myce.ExtensionBaseline `yaml:"baselines,omitempty"`
	References   map[string]myce.WorkReference     `yaml:"references,omitempty"`
	Sweeps       map[string]myce.ReferenceSweep    `yaml:"sweeps,omitempty"`
}
type adapterEntry struct {
	myce.Spec    `yaml:",inline"`
	Source       profileSource     `yaml:"source,omitempty"`
	Checks       adapterChecks     `yaml:"checks,omitempty"`
	MemoryClass  string            `yaml:"memory_class,omitempty"`
	Variants     []adapterVariant  `yaml:"variants,omitempty"`
	Version      string            `yaml:"version,omitempty"`
	Purpose      string            `yaml:"purpose,omitempty"`
	Kinds        []string          `yaml:"kinds,omitempty"`
	ProgramTypes []string          `yaml:"program_types,omitempty"`
	Assets       myce.FileManifest `yaml:"assets,omitempty"`
	Notes        string            `yaml:"notes,omitempty"`
	// Legacy native recipes are converted to exact variants. These shared fields
	// also make a flat native recipe possible, using the same recipe types.
	Environments map[string]myce.EnvironmentRecipe `yaml:"environments,omitempty"`
	Setup        []string                          `yaml:"setup,omitempty"`
	Baselines    map[string]myce.ExtensionBaseline `yaml:"baselines,omitempty"`
	References   map[string]myce.WorkReference     `yaml:"references,omitempty"`
	Sweeps       map[string]myce.ReferenceSweep    `yaml:"sweeps,omitempty"`
}

var adapterName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func decodeAdapter(data []byte) (projectAdapter, error) {
	var a projectAdapter
	if len(data) > 1<<20 {
		return a, errors.New("adapter exceeds 1 MiB")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&a); err != nil {
		return a, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return a, errors.New("expected one YAML adapter")
	}
	if a.Schema != 1 || a.Project == "" || len(a.Entries) == 0 {
		return a, errors.New("declare schema: 1, project, entries and default")
	}
	if _, ok := a.Entries[a.Default]; !ok {
		return a, errors.New("default must name an entry")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return a, err
	}
	var entriesNode *yaml.Node
	for i := 0; i < len(document.Content[0].Content); i += 2 {
		if document.Content[0].Content[i].Value == "entries" {
			entriesNode = document.Content[0].Content[i+1]
		}
	}
	if entriesNode != nil {
		for i := 0; i < len(entriesNode.Content); i += 2 {
			node := entriesNode.Content[i+1]
			if node.Kind == yaml.AliasNode {
				node = node.Alias
			}
			present := map[string]bool{}
			for k := 0; k < len(node.Content); k += 2 {
				present[node.Content[k].Value] = true
			}
			if present["environment"] && present["environments"] {
				return a, errors.New("choose environment or environments")
			}
			if present["variants"] {
				for _, key := range []string{"run", "execution", "needs", "targets", "environment", "environments", "setup", "checks", "baselines", "references", "sweeps"} {
					if present[key] {
						return a, fmt.Errorf("variants cannot accompany flat %s", key)
					}
				}
			}
		}
	}
	for name, e := range a.Entries {
		if !adapterName.MatchString(name) {
			return a, fmt.Errorf("invalid entry name %q", name)
		}
		if err := validateAdapterEntry(e); err != nil {
			return a, fmt.Errorf("entry %s: %w", name, err)
		}
		if _, _, err := adapterProfile(a, name); err != nil {
			return a, fmt.Errorf("entry %s: %w", name, err)
		}
	}
	return a, nil
}
func validateAdapterEntry(e adapterEntry) error {
	s := e.Spec
	if err := myce.ValidateNativeIsolation(s.Isolation, s.Reason); err != nil {
		return err
	}
	if e.Version != "" && !adapterName.MatchString(e.Version) {
		return errors.New("version must be a native version label of at most 64 characters")
	}
	if s.Project != "" || s.SubmissionKey != "" || s.Extension != "" || s.ExtensionDigest != "" || s.ExtensionCheck != nil || s.RequireTarget || len(s.Files) > 0 {
		return errors.New("project belongs to the header; persisted selectors/files/keys are not author fields")
	}
	if e.MemoryClass != "" && e.MemoryClass != "per_item" {
		return errors.New("memory_class must be per_item or omitted")
	}
	if s.Run.Workdir != "" || s.Run.Docker || len(s.Run.DockerSidecars) > 0 {
		return errors.New("source entries use an isolated attempt directory and native commands")
	}
	if len(e.Variants) > 0 {
		if !reflect.ValueOf(s.Run).IsZero() || s.Execution != nil || !reflect.ValueOf(s.Needs).IsZero() || len(s.Targets) > 0 || !reflect.ValueOf(s.Environment).IsZero() || len(e.Environments) > 0 || len(e.Setup) > 0 || len(e.Checks.Result)+len(e.Checks.Qualification)+len(e.Baselines)+len(e.References)+len(e.Sweeps) > 0 {
			return errors.New("variants are complete recipes; flat recipe fields cannot accompany variants")
		}
	} else {
		if (len(s.Run.Command) > 0) == (s.Execution != nil) {
			return errors.New("declare exactly one of run or execution")
		}
		command := s.Run.Command
		if s.Execution != nil {
			command = s.Execution.Main.Command
		}
		if len(command) == 0 || command[0] == "" {
			return errors.New("declare application argv before result checks or appended arguments")
		}
		if len(s.Targets) == 0 || reflect.ValueOf(s.Needs).IsZero() {
			return errors.New("declare exact targets and resource needs")
		}
	}
	if !reflect.ValueOf(s.Environment).IsZero() && len(e.Environments) > 0 {
		return errors.New("choose environment or environments")
	}
	if s.Environment.LockFile != "" {
		return errors.New("project-supplied environments await #251; use installed tools or a catalog name")
	}
	if s.After != nil && (s.FanOut != nil || len(e.Checks.Result) > 0 || s.Program != nil) {
		return errors.New("unsupported after/check/fan_out combination (#180)")
	}
	if s.After != nil {
		for _, v := range e.Variants {
			if len(v.Checks.Result) > 0 {
				return errors.New("unsupported after/check combination in variant (#180)")
			}
		}
	}
	if s.Kind != "" || s.Program != nil {
		return errors.New("kind/program are low-level selectors; declare kinds/program_types on the native recipe")
	}
	for dest, local := range s.Inputs {
		if !profileInputName(dest) || local == "" {
			return errors.New("inputs require clean destination names and local paths")
		}
	}
	for dest, prefix := range s.Outputs {
		if !profileRelative(dest) || !profileRelative(prefix) {
			return errors.New("outputs require clean relative names and prefixes")
		}
	}
	return nil
}
func adapterProfile(a projectAdapter, name string) (executionProfile, []byte, error) {
	if name == "" {
		name = a.Default
	}
	e, ok := a.Entries[name]
	if !ok {
		return executionProfile{}, nil, errors.New("entry does not exist")
	}
	p := executionProfile{Spec: e.Spec, Source: e.Source, Check: e.Checks.Result, MemoryClass: e.MemoryClass, Adapter: true}
	p.Project = a.Project
	if p.Name == "" {
		p.Name = name
	}
	if p.Class == "" && p.Priority == "" {
		p.Class = "experiment"
	}
	if reflect.ValueOf(p.Source).IsZero() {
		p.Source.Git = "HEAD"
	}
	native := len(e.Variants) > 0 || len(e.Checks.Qualification) > 0 || len(e.Baselines) > 0 || len(e.Environments) > 0 || len(e.Setup) > 0 || len(e.Kinds)+len(e.ProgramTypes)+len(e.Assets)+len(e.References)+len(e.Sweeps) > 0
	if native {
		x := myce.Extension{Name: p.Name, Version: e.Version, Purpose: e.Purpose, DefaultClass: p.Class, Resumable: p.Resumable, Kinds: e.Kinds, ProgramTypes: e.ProgramTypes, Assets: e.Assets, Notes: e.Notes, Isolation: e.Isolation, Reason: e.Reason}
		for name := range e.Outputs {
			x.Outputs = append(x.Outputs, name)
		}
		slices.Sort(x.Outputs)
		if x.Version == "" {
			x.Version = "1"
		}
		if x.Purpose == "" {
			x.Purpose = "Project entry " + name
		}
		variants := e.Variants
		if len(variants) == 0 {
			execution := e.Execution
			if execution == nil {
				execution = &myce.ExecutionSpec{Main: myce.ExecutionMember{Command: e.Run.Command, Env: e.Run.Env, MemoryGB: e.Run.MemoryGB}, Sidecars: e.Run.Sidecars}
			}
			if e.Environment.Key() != "" {
				return p, nil, errors.New("native recipes use existing named environments, not a flat catalog override")
			}
			for _, target := range e.Targets {
				needs := e.Needs
				if len(needs.Platform.Preferred) > 0 {
					return p, nil, errors.New("flat native platform preferences cannot be retained; declare complete variants with explicit targets")
				}
				if len(needs.Platform.Required) > 0 && !slices.Contains(needs.Platform.Required, target.Platform) {
					return p, nil, errors.New("flat native targets conflict with needs.platform requirements")
				}
				needs.Platform = myce.Platform{}
				variants = append(variants, adapterVariant{Target: target, Needs: needs, Execution: *execution, Environments: e.Environments, Setup: e.Setup, Checks: e.Checks, Baselines: e.Baselines, References: e.References, Sweeps: e.Sweeps})
			}
		}
		for _, v := range variants {
			if len(v.Execution.Main.Command) == 0 || v.Execution.Main.Command[0] == "" {
				return p, nil, errors.New("native execution.main requires application argv before result checks")
			}
			if _, ok := v.Baselines["extension:"+x.Name]; !ok {
				return p, nil, fmt.Errorf("direct native entry requires its authored extension:%s baseline; kind/program-only recipes retain their low-level registry callers", x.Name)
			}
			execution := v.Execution
			execution.Main.Command = checkedAdapterCommand(execution.Main.Command, v.Checks.Result)
			sweeps := map[string]myce.ReferenceSweep{}
			for work, sweep := range v.Sweeps {
				if len(sweep.Run) > 0 {
					sweep.Run = checkedAdapterCommand(sweep.Run, v.Checks.Result)
				}
				sweeps[work] = sweep
			}
			x.Variants = append(x.Variants, myce.ExtensionVariant{Target: v.Target, Needs: v.Needs, Environments: v.Environments, Execution: execution, Setup: v.Setup, Check: v.Checks.Qualification, Baselines: v.Baselines, References: v.References, Sweeps: sweeps})
			p.Targets = append(p.Targets, v.Target)
		}
		p.Targets = slices.Compact(p.Targets)
		p.Native = &x
		p.Isolation, p.Reason = "", "" // The frozen registration owns native policy.
		p.Execution = nil
		p.Run = myce.RunSpec{}
		p.Needs = myce.ResourceNeeds{}
		p.Environment = myce.Environment{}
		p.Check = nil
	}
	// Canonical selected source authoring is separate from its frozen bytes.
	b, err := yaml.Marshal(struct {
		Project string       `yaml:"project"`
		Name    string       `yaml:"entry"`
		Value   adapterEntry `yaml:"value"`
	}{a.Project, name, e})
	return p, b, err
}

// Check argv occupies fixed positional parameters; the main argv and later CLI
// arguments stay separate shell parameters, including quotes and metacharacters.
func checkedAdapterCommand(command, check []string) []string {
	if len(check) == 0 || len(command) == 0 || command[0] == "" {
		return slices.Clone(command)
	}
	var script strings.Builder
	for i := range check {
		fmt.Fprintf(&script, "myce_check_%d=$1; shift\n", i)
	}
	script.WriteString("\"$@\"\n")
	for i := range check {
		if i > 0 {
			script.WriteByte(' ')
		}
		fmt.Fprintf(&script, "\"$myce_check_%d\"", i)
	}
	argv := []string{"sh", "-ec", script.String(), "myce-result"}
	return append(append(argv, check...), command...)
}

// Native descriptions retain the existing 1 MiB bundled-description contract.
// Large immutable assets continue to use explicit file references.
func freezeAdapterExtension(e myce.Extension, archive string) (myce.Extension, error) {
	f, err := os.Open(archive)
	if err != nil {
		return e, err
	}
	defer f.Close()
	e.Files = map[string][]byte{}
	tr := tar.NewReader(f)
	total := 0
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return e, err
		}
		if h.Name == "extension.yaml" || h.Name == ".mycelium/adapter.yaml" {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return e, errors.New("native description sources require regular files")
		}
		total += int(h.Size)
		if total > 1<<20 {
			return e, errors.New("native source exceeds the existing 1 MiB bundle limit; select its small description closure and use assets for large inputs")
		}
		b, err := io.ReadAll(io.LimitReader(tr, (1<<20)+1))
		if err != nil {
			return e, err
		}
		e.Files[h.Name] = b
	}
	// The content suffix permits edited entries to register a new immutable plan.
	b, err := yaml.Marshal(e)
	if err != nil {
		return e, err
	}
	label := e.Version
	if len(label) > 47 {
		label = label[:47]
	}
	e.Version = label + "-" + profileHash(struct {
		Recipe string
		Files  map[string][]byte
	}{string(b), e.Files})[:16]
	b, err = yaml.Marshal(e)
	if err != nil {
		return e, err
	}
	e.Files["extension.yaml"] = b
	folder, err := os.MkdirTemp(filepath.Dir(archive), "native-")
	if err != nil {
		return e, err
	}
	defer os.RemoveAll(folder)
	for name, data := range e.Files {
		p := filepath.Join(folder, name)
		if err = os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			return e, err
		}
		if err = os.WriteFile(p, data, 0600); err != nil {
			return e, err
		}
	}
	return myce.ReadExtension(folder)
}

func runAdapter(ctx context.Context, file, entry string, args []string, inputs map[string]string, key, notify, config, joining, address, token string, out, errOut io.Writer) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	if file == "" {
		file = filepath.Join(root, ".mycelium", "adapter.yaml")
	}
	abs, err := filepath.Abs(file)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return waitError(64, "adapter_invalid: cannot read adapter")
	}
	a, err := decodeAdapter(data)
	if err != nil {
		return waitError(64, "adapter_invalid: "+err.Error())
	}
	p, selected, err := adapterProfile(a, entry)
	if err != nil {
		return waitError(64, "adapter_invalid: "+err.Error())
	}
	if rel, e := filepath.Rel(root, abs); e == nil && profileRelative(rel) {
		p.Source.AdapterFile = rel
	}
	return runPreparedProfile(ctx, root, p, selected, args, inputs, key, notify, config, joining, address, token, out, errOut)
}
