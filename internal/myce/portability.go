package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// PortabilityRequest is advice input, never a registration or submission.
// Extension metadata may be incomplete: missing build inputs are reportable gaps.
type PortabilityRequest struct {
	Project            string            `json:"project,omitempty"`
	Extension          string            `json:"extension,omitempty"`
	Recipe             *Extension        `json:"recipe,omitempty"`
	Spec               *Spec             `json:"spec,omitempty"`
	RequirementReasons map[string]string `json:"requirement_reasons,omitempty"`
}

type PortabilityReport struct {
	ObservedAt   time.Time                `json:"observed_at"`
	Inventory    string                   `json:"inventory"`
	Requirements []PortabilityRequirement `json:"requirements"`
	Targets      []PortabilityTarget      `json:"targets"`
	Notes        []string                 `json:"notes"`
}
type PortabilityRequirement struct {
	Constraint   string `json:"constraint"`
	ReasonSource string `json:"reason_source"`
	// Free-form authored prose stays in the source: it can contain secrets/paths.
	Reason string `json:"reason"`
}
type PortabilityTarget struct {
	Target        ExecutionTarget          `json:"target"`
	Observed      bool                     `json:"observed"`
	Declared      bool                     `json:"declared"`
	Runtime       string                   `json:"runtime"`
	VariantDigest string                   `json:"variant_digest,omitempty"`
	Needs         *PortabilityResources    `json:"needs,omitempty"`
	Environments  []PortabilityEnvironment `json:"environments"`
	Hosts         []PortabilityHost        `json:"hosts"`
	Actions       []string                 `json:"actions"`
}
type PortabilityResources struct {
	CPUCores        int     `json:"cpu_cores"`
	MemoryGB        float64 `json:"memory_gb"`
	HostMemoryGB    float64 `json:"host_memory_gb"`
	AutomaticMemory bool    `json:"automatic_memory"`
}

func portabilityResources(n ResourceNeeds) PortabilityResources {
	return PortabilityResources{n.CPUCores, n.MemoryGB, n.HostMemoryGB, n.Memory == "auto"}
}

type PortabilityEnvironment struct {
	Name          string   `json:"name"`
	Backend       string   `json:"backend"`
	Key           string   `json:"key,omitempty"`
	Inputs        string   `json:"inputs"`
	LockCoverage  string   `json:"lock_coverage"`
	MissingInputs []string `json:"missing_inputs,omitempty"`
}
type PortabilityWork struct {
	Work    string `json:"work"`
	State   string `json:"state"`
	CheckID string `json:"check_id,omitempty"`
}
type PortabilityHost struct {
	Work       string            `json:"work,omitempty"`
	WorkChecks []PortabilityWork `json:"work_checks,omitempty"`
	// Opaque ordinal avoids disclosing arbitrary host identifiers/paths.
	Host                    string            `json:"host"`
	LastSeen                time.Time         `json:"last_seen"`
	HardwareObservedAt      time.Time         `json:"hardware_observed_at"`
	Freshness               string            `json:"freshness"`
	LogicalCPUs             int               `json:"logical_cpus"`
	FreeCPUCores            int               `json:"free_cpu_cores"`
	AcceleratorMemoryGB     float64           `json:"accelerator_memory_gb"`
	FreeAcceleratorMemoryGB float64           `json:"free_accelerator_memory_gb"`
	Prepared                string            `json:"prepared"`
	Builds                  map[string]string `json:"builds"`
	Qualification           string            `json:"qualification"`
	Fingerprint             string            `json:"fingerprint,omitempty"`
	EnvironmentIdentity     string            `json:"environment_identity,omitempty"`
	CheckDigest             string            `json:"check_digest,omitempty"`
	CheckID                 string            `json:"check_id,omitempty"`
	Admission               string            `json:"admission"`
}

// Portability captures the ordinary detached read view. File hydration, hashing,
// lock parsing, qualification and planner checks all happen after q.mu releases.
func (q *Queue) Portability(req PortabilityRequest) (PortabilityReport, error) {
	if req.Extension != "" && (req.Recipe != nil || req.Spec != nil) || req.Recipe != nil && req.Spec != nil {
		return PortabilityReport{}, errors.New("choose extension, recipe or spec")
	}
	view, _, err := q.captureObservation("")
	if err != nil {
		return PortabilityReport{}, err
	}
	if req.Extension != "" {
		e, ok := view.s.Extensions[req.Extension]
		if !ok {
			return PortabilityReport{}, errors.New("extension requires a registered name@version")
		}
		// Hydrate the exact captured registration, not a later queue generation.
		q.fileLifecycle.RLock()
		err = q.files.hydrateExtension(&e)
		q.fileLifecycle.RUnlock()
		if err != nil {
			return PortabilityReport{}, errors.New("extension inputs unavailable")
		}
		req.Recipe = &e
	}
	if req.Recipe != nil {
		copy := *req.Recipe
		// Caller-authored metadata is not a frozen queue identity, even if the
		// caller supplies a digest that happens to match a registration.
		if req.Extension == "" {
			copy.Digest = ""
		}
		req.Recipe = &copy
		if len(req.RequirementReasons) == 0 {
			req.RequirementReasons = copy.RequirementReasons
		}
	}
	return view.portability(req), nil
}

func safePortabilityID(s string) string {
	if extensionName.MatchString(s) {
		return s
	}
	return "unknown"
}
func portabilityTarget(t ExecutionTarget) ExecutionTarget {
	return ExecutionTarget{safePortabilityID(t.OS), safePortabilityID(t.Arch), safePortabilityID(t.Platform)}
}
func targetLabel(t ExecutionTarget) string { return t.OS + "/" + t.Arch + "/" + t.Platform }
func portabilityRuntime(t ExecutionTarget) string {
	if t.OS == "unknown" || t.Arch == "unknown" {
		return "unknown"
	}
	for _, r := range ReleaseTargets {
		if r.OS == t.OS && r.Arch == t.Arch {
			return "supported"
		}
	}
	return "unsupported: Mycelium runtime port required"
}
func lockPlatform(t ExecutionTarget) string {
	return map[string]string{"linux/amd64": "linux-64", "linux/arm64": "linux-aarch64", "darwin/amd64": "osx-64", "darwin/arm64": "osx-arm64", "windows/amd64": "win-64"}[t.OS+"/"+t.Arch]
}

func (q *Queue) portability(req PortabilityRequest) PortabilityReport {
	report := PortabilityReport{ObservedAt: q.clock.Now(), Inventory: "unknown", Requirements: []PortabilityRequirement{}, Targets: []PortabilityTarget{}, Notes: []string{
		"Read-only advice; no registration, preparation, execution or placement change. Admission is a snapshot, not a reservation.",
		"Missing variants are portability gaps, not proof of hard software requirements. Unauthored requirement reasons remain unknown.",
		"MY-19: obtain owner-accepted output parity before splitting work across variants; preserve checkpoint compatibility.",
	}}
	rows := map[ExecutionTarget]*PortabilityTarget{}
	row := func(t ExecutionTarget) *PortabilityTarget {
		t = portabilityTarget(t)
		if rows[t] == nil {
			rows[t] = &PortabilityTarget{Target: t, Runtime: portabilityRuntime(t), Environments: []PortabilityEnvironment{}, Hosts: []PortabilityHost{}, Actions: []string{}}
		}
		return rows[t]
	}
	e := req.Recipe
	if e != nil && e.Digest == "" {
		report.Notes = append(report.Notes, "Local authored metadata has no frozen registration digest; preparation, qualification and admission remain unknown. Use --extension name@version for registered evidence.")
	}
	if e != nil {
		for _, v := range e.Variants {
			r := row(v.Target)
			r.Declared = true
			r.VariantDigest = identityHash(struct {
				Variant ExtensionVariant
				Digest  string
				Files   map[string][]byte
			}{v, e.Digest, e.Files})
			needs := portabilityResources(v.Needs)
			r.Needs = &needs
			for _, name := range sortedPortabilityKeys(v.Environments) {
				r.Environments = append(r.Environments, portabilityEnvironment(name, v.Environments[name], e.Files, v.Target))
			}
		}
	}
	if req.Spec != nil {
		for _, t := range req.Spec.Targets {
			r := row(t)
			r.Declared = true
			needs := portabilityResources(req.Spec.Needs)
			r.Needs = &needs
		}
	}
	// Constraints are explicitly authored; never derive them from variant coverage.
	constraints := map[string]bool{}
	for key := range req.RequirementReasons {
		constraints[key] = true
	}
	if req.Spec != nil {
		for _, p := range req.Spec.Needs.Platform.Required {
			constraints["backend:"+p] = true
		}
	}
	for _, c := range sortedPortabilityKeys(constraints) {
		source, reason := "unknown", "investigate whether this is a hard software requirement"
		if strings.TrimSpace(req.RequirementReasons[c]) != "" {
			source, reason = "authored", "consult authored requirement_reasons in the source metadata"
		}
		// Only expose constrained identifiers; never free-form reason/keys.
		safe := c
		if strings.HasPrefix(c, "backend:") {
			safe = "backend:" + safePortabilityID(strings.TrimPrefix(c, "backend:"))
		} else {
			safe = safePortabilityID(c)
		}
		report.Requirements = append(report.Requirements, PortabilityRequirement{safe, source, reason})
	}
	anyFresh, anyStale := false, false
	ids := sortedPortabilityKeys(q.s.Machines)
	for index, id := range ids {
		m := q.s.Machines[id]
		targets := q.machineTargets(m)
		// CPU hardware is useful inventory even on legacy per-card workers. This
		// view does not claim CPU admission on a worker that cannot dispatch it.
		if m.Devices == nil && m.GPU != "cpu" {
			targets = append(targets, cpuMachineView(m))
		}
		for _, target := range targets {
			r := row(machineTarget(target))
			r.Observed = true
			h := PortabilityHost{Host: fmt.Sprintf("host-%d", index+1), LastSeen: m.LastSeen, Freshness: "fresh", LogicalCPUs: logicalCPUs(m), FreeCPUCores: m.FreeCPUCores, AcceleratorMemoryGB: target.TotalMemoryGB, FreeAcceleratorMemoryGB: target.FreeMemoryGB, Prepared: "unknown", Builds: map[string]string{}, Qualification: "unknown", Admission: "unknown"}
			if m.CapabilityProfile != nil {
				h.HardwareObservedAt = m.CapabilityProfile.RecordedAt
			}
			if m.LastSeen.IsZero() || machineTarget(target).OS == "" || machineTarget(target).Arch == "" {
				h.Freshness = "unknown"
			} else if m.Gone || q.clock.Now().Before(m.LastSeen) || q.clock.Now().Sub(m.LastSeen) > q.machineLossTimeout(id) {
				h.Freshness = "stale"
			}
			if h.Freshness == "fresh" {
				anyFresh = true
			} else {
				anyStale = true
			}
			var j *Job
			if e != nil {
				v := e.variant(machineTarget(target))
				declared := v != nil || len(e.Variants) == 0 && slices.Contains(e.Platforms, target.GPU)
				if declared {
					r.Declared = true
					if v == nil {
						r.VariantDigest = identityHash(e)
					}
					selected := Spec{Extension: extensionKey(*e), ExtensionDigest: e.Digest, Project: req.Project, Class: e.DefaultClass, Resumable: e.Resumable}
					if len(e.Variants) == 0 {
						if len(e.Kinds) > 0 {
							selected.Kind = e.Kinds[0]
						} else if len(e.ProgramTypes) > 0 {
							selected.Program = &Program{Type: e.ProgramTypes[0]}
						}
					}
					h.Work = portabilityWorkID(selected.workType())
					h.Prepared = "not prepared"
					h.Qualification = "not qualified"
					h.Fingerprint = portabilityHash(target.ExtensionFingerprint)
					h.EnvironmentIdentity = extensionEnvironmentIdentity(*e, target)
					h.CheckDigest = extensionCheckIdentity(*e, target)
					for _, work := range extensionWorks(*e) {
						status := PortabilityWork{Work: portabilityWorkID(work), State: "not qualified"}
						if e.Digest == "" {
							status.State = "unknown: frozen registration required"
						} else {
							for _, er := range target.ExtensionReports {
								if er.Work == work && extensionQualificationReason(*e, er, target) == "" {
									status.State = "qualified"
									status.CheckID = portabilityHash(er.CheckID)
									break
								}
							}
						}
						h.WorkChecks = append(h.WorkChecks, status)
					}

					for _, er := range target.ExtensionReports {
						if er.Digest != e.Digest || er.Platform != target.GPU {
							continue
						}
						if er.Setup && (len(e.Variants) == 0 || matchingExtensionReport(*e, target, er)) {
							h.Prepared = "prepared"
						}
						if er.Work == selected.workType() && extensionQualificationReason(*e, er, target) == "" && e.Digest != "" {
							h.Qualification = "qualified"
							h.CheckID = portabilityHash(er.CheckID)
						}
					}
					for _, env := range r.Environments {
						h.Builds[env.Name] = "not built"
						if slices.Contains(target.Environments, env.Key) && env.Key != "" {
							h.Builds[env.Name] = "ready"
						}
						for _, b := range target.EnvironmentReports {
							if b.Key == env.Key && env.Key != "" {
								h.Builds[env.Name] = portabilityBuildState(b.State)
							}
						}
					}
					for _, b := range target.EnvironmentReports {
						if b.Key == extensionPreparationKey(*e, target) {
							h.Builds["preparation"] = portabilityBuildState(b.State)
						}
					}
					s := selected
					j = &Job{ID: "portability", State: "queued", Spec: s, Extensions: []Extension{*e}}
				} else {
					h.Qualification = "missing variant"
					h.Admission = "missing variant"
				}
			} else if req.Spec != nil {
				s := *req.Spec
				j = &Job{ID: "portability", State: "queued", Spec: s}
				h.Qualification = "plain command: result checks required; native qualification not applicable"
				if s.Environment.Key() == "" {
					h.Prepared = "installed tools: unverified"
				} else {
					recipes, err := portabilityCatalog(q.environments, s.Environment)
					if err == nil {
						j.EnvironmentRecipes = recipes
						if recipe, ok := recipes[target.GPU]; ok {
							found := false
							for _, existing := range r.Environments {
								found = found || existing.Key == recipe.Key
							}
							if !found {
								r.Environments = append(r.Environments, portabilityEnvironment("catalog", recipe, nil, machineTarget(target)))
							}
							h.Prepared = "not prepared"
							if slices.Contains(target.Environments, recipe.Key) {
								h.Prepared = "prepared"
							}
							h.Builds["catalog"] = h.Prepared
							for _, b := range target.EnvironmentReports {
								if b.Key == recipe.Key {
									h.Builds["catalog"] = portabilityBuildState(b.State)
								}
							}
						}
					} else {
						h.Prepared = "catalog recipe unavailable"
					}
				}
			}
			if j != nil {
				if normalizeResources(&j.Spec) != nil {
					h.Admission = "incomplete resource metadata"
				} else {
					candidates := q.memoryCandidates(j, target)
					h.Admission = "blocked by resources, environment or qualification"
					for _, candidate := range candidates {
						fit := targetFit{}
						fitsResourcesChecked(candidate, target, target.FreeMemoryGB, &fit)
						if fit.failures > 0 {
							h.Admission = "blocked: " + fit.first
							continue
						}
						if !hostFits(target, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) {
							h.Admission = "blocked: host memory"
							continue
						}
						if q.immediateCandidate(candidate, target) == nil {
							h.Admission = "blocked: preparation or environment not ready"
							continue
						}
						if !q.placementAllowed(candidate, target) || !q.hostLaunchAllowed(target, target.Session) || !q.workerIdle(target, candidate) || !q.checkpointReady(candidate, target) {
							h.Admission = "blocked by current capacity, maintenance, policy or release"
							continue
						}
						h.Admission = "can admit now"
						if e != nil && req.Project == "" {
							h.Admission = "unknown: project policy not selected"
						}
						break
					}
				}
			}
			if e != nil && e.Digest == "" && r.Declared {
				h.Prepared = "unknown: authored metadata only"
				h.Qualification = "unknown: frozen registration required"
				h.Admission = "unknown: frozen registration required"
			}
			if h.Freshness != "fresh" {
				h.Admission = "unknown: inventory " + h.Freshness
			}
			if r.Runtime != "supported" {
				h.Admission = "unknown: " + r.Runtime
			}
			if target.GPU == "cpu" && m.Devices == nil && m.GPU != "cpu" {
				h.Admission = "unknown: CPU hardware observed; worker CPU dispatch not advertised"
			}
			r.Hosts = append(r.Hosts, h)
		}
	}
	if anyFresh && anyStale {
		report.Inventory = "mixed freshness"
	} else if anyFresh {
		report.Inventory = "fresh"
	} else if anyStale {
		report.Inventory = "stale or unknown"
	}
	for _, r := range rows {
		if !r.Observed {
			r.Actions = append(r.Actions, "Declared target absent from inventory; add a native worker before claiming cluster support.")
		}
		if r.Runtime != "supported" {
			r.Actions = append(r.Actions, r.Runtime+"; a software variant cannot supply the missing Mycelium runtime.")
		}
		if (e != nil || req.Spec != nil) && !r.Declared {
			r.Actions = append(r.Actions, "Investigate an exact variant with its own resources, implementation and checks; preserve genuine authored hard requirements. Missing coverage alone does not prove one.")
		}
		platform := lockPlatform(r.Target)
		if platform == "" {
			platform = r.Target.OS + "/" + r.Target.Arch + " (verify package-manager target name)"
		}
		r.Actions = append(r.Actions, "Cover "+platform+" with uv.lock + pyproject.toml or pixi.lock + pixi.toml; reuse a multi-target lock when valid, otherwise split recipes. Verify frozen installation on this target; never solve on workers.")
		for _, env := range r.Environments {
			if env.Inputs != "present" {
				r.Actions = append(r.Actions, "Supply missing manifest/lock/build inputs for environment "+env.Name+"; lock coverage is "+env.LockCoverage+".")
			}
		}
		device := r.Target.Platform + ":N"
		if r.Target.Platform == "metal" {
			device = "mps (queue backend metal)"
		}
		if r.Target.Platform == "cpu" {
			device = "cpu"
		}
		r.Actions = append(r.Actions, "Use MYCE_DEVICE and assigned visibility; select application device "+device+", never a fixed GPU index.", "Use portable OS/path APIs; replace target-specific shell, library and filesystem assumptions; verify native wheels and build toolchain for "+r.Target.OS+"/"+r.Target.Arch+".")
		if r.Target.Platform == "cpu" {
			r.Actions = append(r.Actions, "Use an explicit CPU implementation with its own resource needs and CPU checks; never silently run GPU qualification on CPU.")
		} else {
			r.Actions = append(r.Actions, "Investigate a CPU alternative where feasible; GPU qualification must exercise "+r.Target.Platform+", never silently fall back to CPU.")
		}
		r.Actions = append(r.Actions, "Prepare environments through the existing operator catalog or bundled variant mechanism, then qualify each native variant against its recorded baseline. Plain commands retain result checks.")
		report.Targets = append(report.Targets, *r)
	}
	slices.SortFunc(report.Targets, func(a, b PortabilityTarget) int { return strings.Compare(targetLabel(a.Target), targetLabel(b.Target)) })
	return report
}
func sortedPortabilityKeys[M ~map[string]V, V any](m M) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
func portabilityWorkID(work string) string {
	kind, name, ok := strings.Cut(work, ":")
	if !ok {
		return "unknown"
	}
	switch kind {
	case "extension", "kind", "program":
		return kind + ":" + safePortabilityID(name)
	}
	return "unknown"
}
func portabilityHash(s string) string { // Identity fields can originate on workers; expose only hashes.
	if s == "" {
		return ""
	}
	return identityHash(s)
}
func portabilityBuildState(s string) string {
	switch s {
	case "ready", "building", "failed", "uploading", "queued":
		return s
	}
	return "unknown"
}

func portabilityEnvironment(name string, r EnvironmentRecipe, files map[string][]byte, t ExecutionTarget) PortabilityEnvironment {
	backend := r.Backend
	if backend == "" {
		backend = "uv"
	}
	out := PortabilityEnvironment{Name: safePortabilityID(name), Backend: safePortabilityID(backend), Key: r.Key, Inputs: "present", LockCoverage: "unknown: native frozen install required"}
	// Catalog keys may be raw paths; only immutable SHA-256 identifiers are public.
	if len(out.Key) != 64 || strings.Trim(out.Key, "0123456789abcdef") != "" {
		out.Key = ""
	}
	read := func(name string) []byte {
		if b := r.Files[name]; len(b) > 0 {
			return b
		}
		if b := r.Files["extension/"+path.Join(path.Dir(r.LockFile), name)]; len(b) > 0 {
			return b
		}
		return files[path.Join(path.Dir(r.LockFile), name)]
	}
	names := []string{"uv.lock", "pyproject.toml"}
	if backend == "pixi" {
		names = []string{"pixi.lock", "pixi.toml"}
	} else if path.Base(r.LockFile) == "requirements.txt" {
		names = []string{"requirements.txt"}
	}
	for _, name := range names {
		if len(read(name)) == 0 {
			out.MissingInputs = append(out.MissingInputs, name)
		}
	}
	if len(out.MissingInputs) > 0 {
		out.Inputs = "missing"
		out.LockCoverage = "unknown: missing inputs"
	}
	for i, arg := range r.Build {
		if file, ok := strings.CutPrefix(arg, "{extension}/"); ok && safeRelative(file) && len(files[file]) == 0 && len(r.Files["extension/"+file]) == 0 {
			out.Inputs = "missing"
			out.MissingInputs = append(out.MissingInputs, fmt.Sprintf("build argument %d: bundled source", i+1))
		}
	}
	if len(r.Build) > 0 && len(files) == 0 && len(r.Files) == 0 {
		out.Inputs = "missing"
		out.MissingInputs = append(out.MissingInputs, "bundled build sources")
	}
	if r.LockFile == "" && (r.Binary != "" || len(r.Build) > 0) {
		out.Inputs = "unknown: native build toolchain required"
		out.LockCoverage = "not applicable: binary build"
		out.MissingInputs = nil
	}
	if backend == "pixi" && out.Inputs == "present" && len(read("pixi.lock")) > 0 {
		var lock struct {
			Environments map[string]struct {
				Packages map[string]any `yaml:"packages"`
			} `yaml:"environments"`
		}
		if yaml.Unmarshal(read("pixi.lock"), &lock) == nil {
			name := r.Environment
			if name == "" {
				name = "default"
			}
			if env, ok := lock.Environments[name]; ok {
				if _, ok := env.Packages[lockPlatform(t)]; ok && lockPlatform(t) != "" {
					out.LockCoverage = "declared: native frozen install required"
				} else {
					out.LockCoverage = "missing target"
				}
			}
		}
	}
	return out
}

func WritePortability(dest io.Writer, r PortabilityReport, asJSON bool) error {
	if asJSON {
		e := json.NewEncoder(dest)
		e.SetIndent("", "  ")
		return e.Encode(r)
	}
	var out strings.Builder
	if _, err := fmt.Fprintf(&out, "Inventory: %s (observed %s)\n", r.Inventory, r.ObservedAt.Format(time.RFC3339)); err != nil {
		return err
	}
	for _, requirement := range r.Requirements {
		fmt.Fprintf(&out, "Requirement %s: %s reason; %s\n", requirement.Constraint, requirement.ReasonSource, requirement.Reason)
	}
	for _, t := range r.Targets {
		fmt.Fprintf(&out, "\n%s: observed=%t declared=%t runtime=%s\n", targetLabel(t.Target), t.Observed, t.Declared, t.Runtime)
		if t.Needs != nil {
			fmt.Fprintf(&out, "  resources: CPU=%d accelerator=%g GiB host=%g GiB automatic_memory=%t\n", t.Needs.CPUCores, t.Needs.MemoryGB, t.Needs.HostMemoryGB, t.Needs.AutomaticMemory)
		}
		if t.VariantDigest != "" {
			fmt.Fprintf(&out, "  variant digest: %s\n", t.VariantDigest)
		}
		for _, env := range t.Environments {
			fmt.Fprintf(&out, "  environment %s (%s): inputs=%s lock=%s\n", env.Name, env.Backend, env.Inputs, env.LockCoverage)
			if len(env.MissingInputs) > 0 {
				fmt.Fprintf(&out, "    missing inputs: %s\n", strings.Join(env.MissingInputs, ", "))
			}
		}
		for _, h := range t.Hosts {
			fmt.Fprintf(&out, "  %s: %s; CPU=%d free=%d; accelerator=%g GiB free=%g; %s; %s; admission=%s\n", h.Host, h.Freshness, h.LogicalCPUs, h.FreeCPUCores, h.AcceleratorMemoryGB, h.FreeAcceleratorMemoryGB, h.Prepared, h.Qualification, h.Admission)
			fmt.Fprintf(&out, "    inventory last seen: %s; hardware observed: %s\n", h.LastSeen.Format(time.RFC3339), h.HardwareObservedAt.Format(time.RFC3339))
			if h.Work != "" {
				fmt.Fprintf(&out, "    admission work: %s\n", h.Work)
			}
			for _, work := range h.WorkChecks {
				fmt.Fprintf(&out, "    check %s: %s id=%s\n", work.Work, work.State, work.CheckID)
			}
			for _, name := range sortedPortabilityKeys(h.Builds) {
				fmt.Fprintf(&out, "    build %s: %s\n", name, h.Builds[name])
			}
			if h.EnvironmentIdentity != "" {
				fmt.Fprintf(&out, "    qualification identity: fingerprint=%s environment=%s check=%s id=%s\n", h.Fingerprint, h.EnvironmentIdentity, h.CheckDigest, h.CheckID)
			}
		}
		for _, action := range t.Actions {
			fmt.Fprintln(&out, "  - "+action)
		}
	}
	for _, note := range r.Notes {
		fmt.Fprintln(&out, "\n"+note)
	}
	_, err := io.WriteString(dest, out.String())
	return err
}

// Advice never resolves inline environment paths or reads caller-selected files.
func portabilityCatalog(c EnvironmentCatalog, e Environment) (map[string]EnvironmentRecipe, error) {
	if e.Name == "" {
		return nil, errors.New("catalog name required")
	}
	recipes, ok := c.Environments[e.Name]
	if !ok {
		return nil, errors.New("catalog recipe unavailable")
	}
	return recipes, nil
}
