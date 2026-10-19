package myce

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const extensionMaxBytes = 1 << 20

var extensionName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Extension is one small folder. Files and Digest are immutable transport data,
// not extra authoring fields in extension.yaml. Downloads live in its cache.
type Extension struct {
	Assets            FileManifest                            `json:"assets,omitempty" yaml:"assets,omitempty"`
	DefaultClass      string                                  `json:"default_class,omitempty" yaml:"default_class,omitempty"`
	Variants          []ExtensionVariant                      `json:"variants,omitempty" yaml:"variants,omitempty"`
	Name              string                                  `json:"name" yaml:"name"`
	Version           string                                  `json:"version" yaml:"version"`
	Purpose           string                                  `json:"purpose" yaml:"purpose"`
	Platforms         []string                                `json:"platforms" yaml:"platforms"`
	Kinds             []string                                `json:"kinds,omitempty" yaml:"kinds,omitempty"`
	ProgramTypes      []string                                `json:"program_types,omitempty" yaml:"program_types,omitempty"`
	MemoryGB          float64                                 `json:"memory_gb" yaml:"memory_gb"`
	HostMemoryGB      float64                                 `json:"host_memory_gb,omitempty" yaml:"host_memory_gb,omitempty"`
	Resumable         bool                                    `json:"resumable,omitempty" yaml:"resumable,omitempty"`
	Setup             []string                                `json:"setup,omitempty" yaml:"setup,omitempty"`
	Run               []string                                `json:"run" yaml:"run"`
	Benchmark         []string                                `json:"benchmark,omitempty" yaml:"benchmark,omitempty"`
	Check             []string                                `json:"check,omitempty" yaml:"check,omitempty"`
	Baselines         map[string]ExtensionBaseline            `json:"baselines,omitempty" yaml:"baselines,omitempty"`
	PlatformBaselines map[string]map[string]ExtensionBaseline `json:"platform_baselines,omitempty" yaml:"platform_baselines,omitempty"`
	Notes             string                                  `json:"notes,omitempty" yaml:"notes,omitempty"`
	Files             map[string][]byte                       `json:"files" yaml:"-"`
	Digest            string                                  `json:"digest" yaml:"-"`
}

// Baselines are per kind/program type, so a program check cannot qualify an
// unrelated kind. Inputs and native outputs are bundled; check receives only
// inputs and writes its actual output to MYCE_CHECK_OUTPUT.
type ExtensionBaseline struct {
	Input     string  `json:"input" yaml:"input"`
	Output    string  `json:"output" yaml:"output"`
	Measure   string  `json:"measure" yaml:"measure"` // exact or numeric (JSON array)
	Absolute  float64 `json:"absolute,omitempty" yaml:"absolute,omitempty"`
	Relative  float64 `json:"relative,omitempty" yaml:"relative,omitempty"`
	Native    string  `json:"native" yaml:"native"`
	Variation string  `json:"variation" yaml:"variation"`
}
type Program struct {
	Type string   `json:"type" yaml:"type"`
	File string   `json:"file" yaml:"file"` // local name in inputs, never a machine path
	Args []string `json:"args,omitempty" yaml:"args,omitempty"`
}
type SizeHint struct {
	MemoryGB     float64 `json:"memory_gb,omitempty" yaml:"memory_gb,omitempty"`
	HostMemoryGB float64 `json:"host_memory_gb,omitempty" yaml:"host_memory_gb,omitempty"`
}

func (s SizeHint) valid() bool {
	return validMemoryValue(s.MemoryGB) && validMemoryValue(s.HostMemoryGB) && (s.MemoryGB > 0 || s.HostMemoryGB > 0)
}

type ExtensionSelection struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

func safeRelative(p string) bool {
	return p != "" && !strings.Contains(p, "\\") && !strings.Contains(p, ":") && !filepath.IsAbs(p) && filepath.Clean(p) == p && p != "." && p != ".." && !strings.HasPrefix(p, "../")
}
func extensionKey(e Extension) string { return e.Name + "@" + e.Version }
func (s Spec) extensionJob() bool {
	return s.Kind != "" || s.Program != nil || s.Extension != "" || s.ExtensionDigest != ""
}
func (s Spec) workType() string {
	if s.Program != nil {
		return "program:" + s.Program.Type
	}
	if s.Kind != "" {
		return "kind:" + s.Kind
	}
	name, _, _ := strings.Cut(s.Extension, "@")
	return "extension:" + name
}
func (e Extension) handles(s Spec) bool {
	if s.Kind == "" && s.Program == nil {
		name, _, _ := strings.Cut(s.Extension, "@")
		return name == e.Name
	}
	if s.Program != nil {
		return slices.Contains(e.ProgramTypes, s.Program.Type)
	}
	return slices.Contains(e.Kinds, s.Kind)
}
func (e Extension) validate() error {
	if err := e.Assets.validate(); err != nil {
		return err
	}
	if strings.Contains(e.DefaultClass, "/") {
		return errors.New("default_class cannot contain slash")
	}
	if len(e.Variants) > 0 {
		return e.validateVariants()
	}
	if !extensionName.MatchString(e.Name) || !extensionName.MatchString(e.Version) || e.Purpose == "" || len(e.Platforms) == 0 || len(e.Kinds)+len(e.ProgramTypes) == 0 || len(e.Run) == 0 || e.Run[0] == "" {
		return errors.New("extension requires name, version, purpose, platforms, work types and run argv")
	}
	if !validMemoryValue(e.MemoryGB) || e.MemoryGB <= 0 || !validMemoryValue(e.HostMemoryGB) {
		return errors.New("extension memory reservations must be finite; GPU memory must be positive")
	}
	for _, p := range e.Platforms {
		if !validGPU(p) {
			return fmt.Errorf("unknown extension platform %s", p)
		}
	}
	for _, n := range append(slices.Clone(e.Kinds), e.ProgramTypes...) {
		if !extensionName.MatchString(n) {
			return errors.New("invalid extension work type")
		}
	}
	if len(e.PlatformBaselines) > 0 && len(e.Baselines) > 0 {
		return errors.New("choose baselines or platform_baselines; cross-platform fallback is not allowed")
	}
	if err := e.validateBaselines(e.Baselines); err != nil {
		return err
	}
	for platform, baselines := range e.PlatformBaselines {
		if !slices.Contains(e.Platforms, platform) || len(baselines) == 0 {
			return errors.New("platform baselines require a supported platform and native work references")
		}
		if err := e.validateBaselines(baselines); err != nil {
			return err
		}
	}
	return nil
}

// Platform-specific references never fall back to another backend's output.
// Older frozen extensions retain their declared common reference.
func (e Extension) baseline(work, platform string) (ExtensionBaseline, bool) {
	if len(e.PlatformBaselines) > 0 {
		b, ok := e.PlatformBaselines[platform][work]
		return b, ok
	}
	b, ok := e.Baselines[work]
	return b, ok
}

func (e Extension) validateBaselines(baselines map[string]ExtensionBaseline) error {
	for k, b := range baselines {
		s := Spec{Kind: strings.TrimPrefix(k, "kind:")}
		if strings.HasPrefix(k, "program:") {
			s = Spec{Program: &Program{Type: strings.TrimPrefix(k, "program:")}}
		} else if strings.HasPrefix(k, "extension:") {
			s = Spec{Extension: strings.TrimPrefix(k, "extension:")}
		} else if !strings.HasPrefix(k, "kind:") {
			return errors.New("baseline key must be kind:NAME or program:TYPE")
		}
		if !e.handles(s) || !safeRelative(b.Input) || !safeRelative(b.Output) || b.Native == "" || b.Variation == "" || !validMemoryValue(b.Absolute) || !validMemoryValue(b.Relative) {
			return errors.New("baseline requires handled work, bundled input/output, native provenance and measured variation")
		}
		if b.Measure != "exact" && b.Measure != "numeric" {
			return errors.New("baseline measure must be exact or numeric")
		}
		if b.Measure == "exact" && (b.Absolute != 0 || b.Relative != 0) {
			return errors.New("exact comparison cannot declare numeric tolerance")
		}
		if len(e.Check) == 0 || e.Check[0] == "" {
			return errors.New("baseline requires check argv")
		}
		if _, ok := e.Files[b.Input]; !ok {
			return errors.New("baseline input not bundled")
		}
		if _, ok := e.Files[b.Output]; !ok {
			return errors.New("native output not bundled")
		}
	}
	return nil
}
func extensionFromFiles(files map[string][]byte) (Extension, error) {
	owned := make(map[string][]byte, len(files))
	for n, b := range files {
		owned[n] = slices.Clone(b)
	}
	files = owned
	var e Extension
	total := 0
	for n, b := range files {
		if !safeRelative(n) {
			return e, errors.New("invalid extension file path")
		}
		total += len(b)
	}
	if total > extensionMaxBytes {
		return e, errors.New("extension exceeds 1 MiB; downloads belong in the cache")
	}
	d := yaml.NewDecoder(bytes.NewReader(files["extension.yaml"]))
	d.KnownFields(true)
	if err := d.Decode(&e); err != nil {
		return e, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return e, errors.New("expected one extension description")
	}
	e.Files = files
	if err := e.validate(); err != nil {
		return e, err
	}
	b, err := json.Marshal(files)
	if err != nil {
		return e, err
	}
	h := sha256.Sum256(b)
	e.Digest = hex.EncodeToString(h[:])
	if err := e.snapshotVariantEnvironments(); err != nil {
		return e, err
	}
	return e, nil
}
func ReadExtension(folder string) (Extension, error) {
	files := map[string][]byte{}
	total := int64(0)
	err := filepath.WalkDir(folder, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errors.New("extension files must be regular (no symlinks)")
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if total > extensionMaxBytes {
			return errors.New("extension exceeds 1 MiB")
		}
		n, err := filepath.Rel(folder, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(n)] = b
		return nil
	})
	if err != nil {
		return Extension{}, err
	}
	return extensionFromFiles(files)
}
func (q *Queue) AddExtension(e Extension) error {
	checked, err := extensionFromFiles(e.Files)
	if err != nil {
		return err
	}
	return q.update(func() error {
		if q.s.Extensions == nil {
			q.s.Extensions = map[string]Extension{}
		}
		k := extensionKey(checked)
		if old, ok := q.s.Extensions[k]; ok && old.Digest != checked.Digest {
			return errors.New("extension version is frozen; publish a new version")
		}
		q.s.Extensions[k] = checked
		for _, j := range q.s.Jobs {
			if j.State == "queued" && j.Spec.extensionJob() && len(j.Extensions) == 0 {
				j.Extensions, _ = q.resolveExtensions(&j.Spec)
			}
		}
		return nil
	})
}
func (q *Queue) Extensions() []Extension {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := []Extension{}
	for _, e := range q.s.Extensions {
		e.Files = nil
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b Extension) int { return strings.Compare(extensionKey(a), extensionKey(b)) })
	return out
}
func (q *Queue) resolveExtensions(s *Spec) ([]Extension, error) {
	if !s.extensionJob() {
		return nil, nil
	}
	if s.Extension != "" || s.ExtensionDigest != "" {
		return q.resolveExplicitExtension(s)
	}
	out := []Extension{}
	platforms := []string{}
	for _, e := range q.s.Extensions {
		if !e.handles(*s) {
			continue
		}
		if len(e.Variants) > 0 && len(s.Run.Env) > 0 {
			return nil, errors.New("variant jobs use extension-owned execution env")
		}
		out = append(out, e)
		for _, v := range e.Variants {
			s.Needs.MemoryGB = max(s.Needs.MemoryGB, v.Needs.MemoryGB)
			s.Needs.HostMemoryGB = max(s.Needs.HostMemoryGB, v.Needs.HostMemoryGB)
		}
		platforms = append(platforms, e.platforms()...)
		s.Needs.MemoryGB = max(s.Needs.MemoryGB, e.MemoryGB)
		s.Needs.HostMemoryGB = max(s.Needs.HostMemoryGB, e.HostMemoryGB)
	}
	sort.Slice(out, func(i, j int) bool { return extensionKey(out[i]) < extensionKey(out[j]) })
	slices.Sort(platforms)
	platforms = slices.Compact(platforms)
	// Unknown work waits and can pick up an extension added later. Its hint is
	// conservative admission only, not a measured capability profile (#37).
	s.Needs.MemoryGB = max(s.Needs.MemoryGB, s.SizeHint.MemoryGB)
	s.Needs.HostMemoryGB = max(s.Needs.HostMemoryGB, s.SizeHint.HostMemoryGB)
	if len(platforms) == 0 {
		s.Needs.Platform = Platform{Any: true}
	} else {
		s.Needs.Platform = Platform{Required: platforms}
	}
	return out, nil
}
func qualifiedExtension(j *Job, m *Machine) *Extension {
	for i := range j.Extensions {
		e := &j.Extensions[i]
		if !e.matchesTarget(m) || !slices.Contains(e.platforms(), m.GPU) || (j.Spec.Resumable && !e.Resumable) {
			continue
		}
		view, ok := e.forMachine(m)
		if !ok {
			continue
		}
		if _, ok := view.baseline(j.Spec.workType(), m.GPU); !ok {
			continue
		}
		for _, r := range m.ExtensionReports {
			if len(e.Variants) > 0 && (r.Target == nil || *r.Target != machineTarget(m)) {
				continue
			}
			if r.Digest == e.Digest && r.Work == j.Spec.workType() && r.Qualified && r.Platform == m.GPU && currentExtensionQualification(*e, r, m) {
				return e
			}
		}
	}
	return nil
}
func extensionCanRun(j *Job, m *Machine) bool {
	return !j.Spec.extensionJob() || qualifiedExtension(j, m) != nil
}
func extensionWaitingReason(j *Job, machines map[string]*Machine) string {
	if !j.Spec.extensionJob() {
		return ""
	}
	if len(j.Extensions) == 0 {
		return "waiting for an extension for " + j.Spec.workType()
	}
	reasons := []string{}
	for _, m := range machines {
		if !m.Gone && !m.Leaving && qualifiedExtension(j, m) != nil {
			return ""
		}
		for _, e := range j.Extensions {
			if reason := e.targetUnavailableReason(machineTarget(m)); reason != "" {
				reasons = append(reasons, extensionKey(e)+": "+reason)
				continue
			}
			for _, r := range m.EnvironmentReports {
				if r.Key == extensionPreparationKey(e, m) {
					if r.State == "failed" || r.State == "uploading" {
						reasons = append(reasons, extensionKey(e)+": environment build failed: "+r.Error)
					}
					if r.State == "building" {
						reasons = append(reasons, extensionKey(e)+": preparing environment")
					}
				}
			}
			for _, r := range m.ExtensionReports {
				if r.Digest == e.Digest && r.Work == j.Spec.workType() && r.Reason != "" {
					reasons = append(reasons, extensionKey(e)+": "+r.Reason)
				}
			}
		}
	}
	slices.Sort(reasons)
	reasons = slices.Compact(reasons)
	message := "waiting for a qualified extension for " + j.Spec.workType()
	if len(reasons) > 0 {
		message += " (" + strings.Join(reasons, "; ") + ")"
	}
	return message
}
func compareExtensionOutput(actual, expected []byte, b ExtensionBaseline) error {
	if b.Measure == "exact" {
		if !bytes.Equal(actual, expected) {
			return errors.New("output differs from native baseline")
		}
		return nil
	}
	var a, w []float64
	if json.Unmarshal(actual, &a) != nil || json.Unmarshal(expected, &w) != nil || len(w) == 0 || len(a) != len(w) {
		return errors.New("numeric outputs must be matching nonempty JSON arrays")
	}
	for i, v := range a {
		if math.IsNaN(v) || math.IsInf(v, 0) || math.IsNaN(w[i]) || math.IsInf(w[i], 0) || math.Abs(v-w[i]) > b.Absolute+b.Relative*math.Abs(w[i]) {
			return errors.New("output exceeds declared native variation")
		}
	}
	return nil
}
