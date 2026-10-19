package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
)

// A target is exact, never a fallback or inheritance rule. Platform is the
// existing accelerator backend (cuda/xpu/metal/fake), not an OS package set.
type ExecutionTarget struct {
	OS       string `json:"os" yaml:"os"`
	Arch     string `json:"arch" yaml:"arch"`
	Platform string `json:"platform" yaml:"platform"`
}

type ExtensionVariant struct {
	Target       ExecutionTarget              `json:"target" yaml:"target"`
	Environments map[string]EnvironmentRecipe `json:"environments,omitempty" yaml:"environments,omitempty"`
	Needs        ResourceNeeds                `json:"needs" yaml:"needs"`
	Execution    ExecutionSpec                `json:"execution" yaml:"execution"`
	Setup        []string                     `json:"setup,omitempty" yaml:"setup,omitempty"`
	Check        []string                     `json:"check,omitempty" yaml:"check,omitempty"`
	Baselines    map[string]ExtensionBaseline `json:"baselines,omitempty" yaml:"baselines,omitempty"`
}

func (e Extension) validateVariants() error {
	if !extensionName.MatchString(e.Name) || !extensionName.MatchString(e.Version) || e.Purpose == "" {
		return errors.New("extension requires name, version and purpose")
	}
	if len(e.Run) > 0 || len(e.Platforms) > 0 || e.MemoryGB != 0 || e.HostMemoryGB != 0 || len(e.Setup) > 0 || len(e.Check) > 0 || len(e.Baselines) > 0 || len(e.PlatformBaselines) > 0 || len(e.Benchmark) > 0 {
		return errors.New("variants are complete recipes; do not mix legacy execution fields with variants")
	}
	for _, n := range append(slices.Clone(e.Kinds), e.ProgramTypes...) {
		if !extensionName.MatchString(n) {
			return errors.New("invalid extension work type")
		}
	}
	seen := map[ExecutionTarget]bool{}
	for _, v := range e.Variants {
		t := v.Target
		if t.OS == "" || t.Arch == "" || !extensionName.MatchString(t.OS) || !extensionName.MatchString(t.Arch) || !validGPU(t.Platform) || seen[t] {
			return errors.New("variants require unique complete OS/arch/platform targets")
		}
		seen[t] = true
		if v.Needs.Model != "" || v.Needs.MemoryGB <= 0 || !validMemoryValue(v.Needs.MemoryGB) || !validMemoryValue(v.Needs.HostMemoryGB) || v.Needs.Platform.Any || len(v.Needs.Platform.Required)+len(v.Needs.Platform.Preferred) > 0 {
			return errors.New("variant needs require finite GPU/RAM floors; target owns platform")
		}
		if err := v.Execution.validate(); err != nil {
			return err
		}
		for name := range v.Environments {
			if !extensionName.MatchString(name) {
				return errors.New("invalid environment reference name")
			}
		}
		refs := []string{v.Execution.Main.Environment}
		for _, m := range v.Execution.Sidecars {
			refs = append(refs, m.Environment)
		}
		for _, ref := range refs {
			if ref != "" {
				if _, ok := v.Environments[ref]; !ok {
					return fmt.Errorf("unknown member environment %q", ref)
				}
			}
		}
		view := e
		view.Check = v.Check
		if err := view.validateBaselines(v.Baselines); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot bundled uv inputs through the existing recipe/cache contract. No
// filesystem reads, dependency solving or installs happen at registration.
func (e *Extension) snapshotVariantEnvironments() error {
	for i := range e.Variants {
		v := &e.Variants[i]
		for name, r := range v.Environments {
			if r.Backend == "" {
				r.Backend = "uv"
			}
			if r.Backend != "uv" && r.Backend != "pixi" {
				return errors.New("unsupported environment backend")
			}
			if !safeRelative(r.LockFile) || r.Binary != "" {
				return errors.New("environment requires a bundled lock_file")
			}
			filenames := []string{"requirements.txt"}
			if r.Backend == "pixi" {
				if path.Base(r.LockFile) != "pixi.lock" || (r.Manifest != "" && r.Manifest != path.Join(path.Dir(r.LockFile), "pixi.toml")) {
					return errors.New("pixi requires pixi.toml beside pixi.lock")
				}
				filenames = []string{"pixi.toml", "pixi.lock"}
			} else {
				if r.Python == "" {
					return errors.New("uv requires python version")
				}
				switch path.Base(r.LockFile) {
				case "uv.lock", "pyproject.toml":
					filenames = []string{"uv.lock", "pyproject.toml"}
				case "requirements.txt":
				default:
					return errors.New("unsupported uv lock file")
				}
			}
			r.Files = map[string][]byte{}
			for _, file := range filenames {
				b, ok := e.Files[path.Join(path.Dir(r.LockFile), file)]
				if !ok {
					return fmt.Errorf("environment %s: missing bundled %s", name, file)
				}
				if r.Backend != "pixi" {
					r.Files[file] = slices.Clone(b)
				}
			}
			// Pixi activation and build hooks can reference any bundled file.
			// Preserve the full digest-verified folder, including relative paths.
			if r.Backend == "pixi" || len(r.Build) > 0 {
				for file, b := range e.Files {
					r.Files["extension/"+file] = slices.Clone(b)
				}
			}
			r.InputDigest = e.Digest
			target := v.Target
			r.Target = &target
			r.Platform = target.Platform
			r.Key = ""
			identity := r
			identity.LockFile = ""
			identity.Manifest = ""
			b, err := json.Marshal(identity)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(b)
			r.Key = hex.EncodeToString(sum[:])
			v.Environments[name] = r
		}
	}
	return nil
}

func validateExtensionSelector(s Spec) error {
	if s.Extension != "" {
		name, version, has := strings.Cut(s.Extension, "@")
		if !extensionName.MatchString(name) || has && !extensionName.MatchString(version) {
			return errors.New("extension must be name or name@version")
		}
	}
	if s.ExtensionDigest != "" {
		b, err := hex.DecodeString(s.ExtensionDigest)
		if err != nil || len(b) != sha256.Size || strings.ToLower(s.ExtensionDigest) != s.ExtensionDigest {
			return errors.New("extension_digest must be a lowercase SHA-256")
		}
	}
	return nil
}

func (q *Queue) resolveExplicitExtension(s *Spec) ([]Extension, error) {
	var matches []Extension
	for key, e := range q.s.Extensions {
		name, _, versioned := strings.Cut(s.Extension, "@")
		if s.Extension != "" && (versioned && key != s.Extension || !versioned && e.Name != name) || s.ExtensionDigest != "" && e.Digest != s.ExtensionDigest {
			continue
		}
		if (s.Kind != "" || s.Program != nil) && !e.handles(*s) {
			continue
		}
		matches = append(matches, e)
	}
	if len(matches) != 1 {
		return nil, errors.New("extension reference must resolve to one registered version; use name@version or extension_digest")
	}
	e := matches[0]
	if len(e.Variants) > 0 && len(s.Run.Env) > 0 {
		return nil, errors.New("variant jobs use extension-owned execution env")
	}
	s.Extension = extensionKey(e)
	s.ExtensionDigest = e.Digest
	if s.Class == "" && s.Priority == "" {
		s.Class = e.DefaultClass
	}
	s.Needs.MemoryGB = max(e.MemoryGB, s.SizeHint.MemoryGB)
	s.Needs.HostMemoryGB = max(e.HostMemoryGB, s.SizeHint.HostMemoryGB)
	for _, v := range e.Variants {
		s.Needs.MemoryGB = max(s.Needs.MemoryGB, v.Needs.MemoryGB)
		s.Needs.HostMemoryGB = max(s.Needs.HostMemoryGB, v.Needs.HostMemoryGB)
	}
	s.Needs.Platform = Platform{Required: e.platforms()}
	return matches, nil
}

func machineTarget(m *Machine) ExecutionTarget {
	t := ExecutionTarget{Platform: m.GPU}
	if m.CapabilityProfile != nil {
		t.OS = m.CapabilityProfile.OS
		t.Arch = m.CapabilityProfile.Arch
	}
	return t
}
func (e Extension) variant(t ExecutionTarget) *ExtensionVariant {
	for i := range e.Variants {
		if e.Variants[i].Target == t {
			return &e.Variants[i]
		}
	}
	return nil
}
func (e Extension) matchesTarget(m *Machine) bool {
	return len(e.Variants) == 0 || e.variant(machineTarget(m)) != nil
}
func (e Extension) targetUnavailableReason(t ExecutionTarget) string {
	if len(e.Variants) == 0 {
		return ""
	}
	v := e.variant(t)
	if v == nil {
		return fmt.Sprintf("no variant matches this host's OS/arch/backend: missing target %s/%s/%s", t.OS, t.Arch, t.Platform)
	}
	if v.Execution.Main.MemoryGB > 0 {
		return "main member RAM caps require D5"
	}
	return ""
}
func (e Extension) forMachine(m *Machine) (Extension, bool) {
	if len(e.Variants) == 0 {
		return e, true
	}
	// Refuse targets whose requested member caps are not yet enforceable.
	if e.targetUnavailableReason(machineTarget(m)) != "" {
		return e, false
	}
	v := e.variant(machineTarget(m))
	e.Run = slices.Clone(v.Execution.Main.Command)
	e.Setup = v.Setup
	e.Check = v.Check
	e.Baselines = v.Baselines
	e.PlatformBaselines = nil
	return e, true
}
func (e Extension) lower(s *Spec, m *Machine) error {
	legacyEnv := s.Run.Env
	if len(e.Variants) == 0 {
		s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: slices.Clone(e.Run)}}
	} else {
		if len(legacyEnv) > 0 {
			return errors.New("variant jobs use extension-owned execution env")
		}
		v := e.variant(machineTarget(m))
		if v == nil {
			return errors.New("no extension variant for worker target")
		}
		execution := v.Execution
		execution.Main.Environment = "" // Bound to its immutable recipe below.
		s.Environment = Environment{Name: v.Execution.Main.Environment}
		s.Execution = &execution
	}
	s.Run = RunSpec{} // A worker copy may already contain a staged workdir.
	if err := lowerExecution(s); err != nil {
		return err
	}
	if len(e.Variants) == 0 {
		// Legacy caller configuration is not authored member env. Keep the
		// existing worker's authoritative identity/device/usage overrides.
		s.Run.Env = maps.Clone(legacyEnv)
	}
	return nil
}
func extensionRecipe(e Extension, m *Machine) (EnvironmentRecipe, bool) {
	v := e.variant(machineTarget(m))
	if v == nil {
		return EnvironmentRecipe{}, false
	}
	r, ok := v.Environments[v.Execution.Main.Environment]
	return r, ok
}

func (e Extension) platforms() []string {
	if len(e.Variants) == 0 {
		return slices.Clone(e.Platforms)
	}
	out := []string{}
	for _, v := range e.Variants {
		out = append(out, v.Target.Platform)
	}
	slices.Sort(out)
	return slices.Compact(out)
}
