package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"
)

// PortabilityRequest is advice input, never a registration or submission.
// Extension metadata may be incomplete: missing build inputs are reportable gaps.
type PortabilityRequest struct {
	SourceUnverified   bool              `json:"source_unverified,omitempty"`
	Project            string            `json:"project,omitempty"`
	Extension          string            `json:"extension,omitempty"`
	Recipe             *Extension        `json:"recipe,omitempty"`
	Spec               *Spec             `json:"spec,omitempty"`
	RequirementReasons map[string]string `json:"requirement_reasons,omitempty"`
}

type PortabilityReport struct {
	Linkage      []MachOLinkage           `json:"linkage,omitempty"`
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
	AuthoredEligibility string                   `json:"authored_eligibility,omitempty"`
	EnvironmentCoverage string                   `json:"environment_coverage,omitempty"`
	Parity              string                   `json:"parity,omitempty"`
	Target              ExecutionTarget          `json:"target"`
	Observed            bool                     `json:"observed"`
	Declared            bool                     `json:"declared"`
	Runtime             string                   `json:"runtime"`
	VariantDigest       string                   `json:"variant_digest,omitempty"`
	Needs               *PortabilityResources    `json:"needs,omitempty"`
	Environments        []PortabilityEnvironment `json:"environments"`
	Hosts               []PortabilityHost        `json:"hosts"`
	Actions             []string                 `json:"actions"`
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
	SharedHost *SharedHostProfile `json:"shared_host,omitempty"`
	Work       string             `json:"work,omitempty"`
	WorkChecks []PortabilityWork  `json:"work_checks,omitempty"`
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
func (q *Queue) Portability(req PortabilityRequest, contexts ...context.Context) (PortabilityReport, error) {
	ctx := requestContext(contexts)
	if req.RequirementReasons == nil && req.Spec != nil {
		req.RequirementReasons = req.Spec.RequirementReasons
	}
	if req.Extension != "" && (req.Recipe != nil || req.Spec != nil) || req.Recipe != nil && req.Spec != nil {
		return PortabilityReport{}, errors.New("choose extension, recipe or spec")
	}
	// Same hypothetical-placement question as resolve: borrow retained history
	// from the committed generation. The full status capture copied and charged
	// every retained execution and was refused with projection_capacity_exceeded
	// once history outgrew one request's memory share.
	view, err := q.captureResolveView(ctx)
	if err != nil {
		return PortabilityReport{}, err
	}
	if req.Extension != "" {
		e, ok := view.s.Extensions[req.Extension]
		if !ok {
			return PortabilityReport{}, errors.New("extension requires a registered name@version")
		}
		// Hydrate the exact captured registration, not a later queue generation.
		if err := lockUploadContext(ctx, q.fileLifecycle.RLock, q.fileLifecycle.RUnlock); err != nil {
			return PortabilityReport{}, err
		}
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
	if err := ctx.Err(); err != nil {
		return PortabilityReport{}, err
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
	sets := map[string]map[string]EnvironmentRecipe{}
	if req.Spec != nil && !req.Spec.extensionJob() {
		s := cloneJob(&Job{Spec: *req.Spec}).Spec
		if normalizeResources(&s) == nil && lowerExecution(&s) == nil {
			req.Spec = &s
			if s.Environment.Key() != "" {
				sets["main"], _ = portabilityCatalog(q.environments, s.Environment)
			}
			for _, side := range s.Run.Sidecars {
				if side.Environment != "" {
					sets[side.Name], _ = portabilityCatalog(q.environments, Environment{Name: side.Environment})
				}
			}
		}
	}
	return q.portabilityWithRecipes(req, sets)
}

func (q *Queue) portabilityWithRecipes(req PortabilityRequest, sets map[string]map[string]EnvironmentRecipe) PortabilityReport {
	report := PortabilityReport{ObservedAt: q.clock.Now(), Inventory: "unknown", Requirements: []PortabilityRequirement{}, Targets: []PortabilityTarget{}, Notes: []string{
		"Read-only advice; no registration, preparation, execution or placement change. Admission is a snapshot, not a reservation.",
		"Missing variants are portability gaps, not proof of hard software requirements. Unauthored requirement reasons remain unknown.",
		"Shared orchestration/interactive hosts are usable for fitting light native CPU commands within their profile; submit normally, respect maintenance, and let last-resort placement choose. CPU priority/reservations are not hard shares or pressure-triggered migration.",
		"MY-19: obtain owner-accepted output parity before splitting work across variants; preserve checkpoint compatibility.",
	}}
	if req.Spec != nil {
		report.Notes = append(report.Notes, PlacementAdvisories(*req.Spec, req.RequirementReasons)...)
	} else if req.Recipe != nil {
		report.Notes = append(report.Notes, PlacementAdvisories(Spec{}, req.RequirementReasons, req.Recipe.Variants...)...)
	}
	rows := map[ExecutionTarget]*PortabilityTarget{}
	row := func(t ExecutionTarget) *PortabilityTarget {
		t = portabilityTarget(t)
		if rows[t] == nil {
			rows[t] = &PortabilityTarget{Target: t, Runtime: portabilityRuntime(t), Environments: []PortabilityEnvironment{}, Hosts: []PortabilityHost{}, Actions: []string{}}
		}
		return rows[t]
	}
	var discovery *CommandDiscovery
	var discoveryError error
	if req.Spec != nil && WantsCommandDiscovery(*req.Spec) {
		sides := map[string]map[string]EnvironmentRecipe{}
		for name, recipes := range sets {
			if name != "main" {
				sides[name] = recipes
			}
		}
		discovery, discoveryError = deriveCommandDiscovery(*req.Spec, sets["main"], sides)
		if discoveryError != nil {
			discovery = &CommandDiscovery{}
		}
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
				environment := portabilityEnvironment(name, v.Environments[name], e.Files, v.Target)
				if req.SourceUnverified && e.Digest == "" {
					environment.Inputs, environment.LockCoverage = "unknown: selected source not inspected", "unknown: selected source not inspected"
					environment.MissingInputs = nil
				}
				r.Environments = append(r.Environments, environment)
			}
		}
	}
	if req.Spec != nil {
		for _, t := range req.Spec.Targets {
			if t.Arch == "" || t.Platform == "" {
				continue
			}
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
	if req.Spec != nil && !req.Spec.extensionJob() {
		for _, recipes := range sets {
			for platform, recipe := range recipes {
				for _, target := range environmentLockCoverage(recipe, nil, platform).Targets {
					row(target)
				}
			}
		}
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
			if req.Spec != nil && !req.Spec.extensionJob() && req.Spec.Needs.cpuOnly() && target.GPU != "cpu" {
				continue
			}
			r := row(machineTarget(target))
			r.Observed = true
			if req.Spec != nil && slices.ContainsFunc(req.Spec.Targets, func(t ExecutionTarget) bool { return t.matches(machineTarget(target)) }) {
				r.Declared = true
				needs := portabilityResources(req.Spec.Needs)
				r.Needs = &needs
			}
			h := PortabilityHost{SharedHost: cloneSharedHost(q.s.HostMaintenance[hostID(m)].SharedHost), Host: fmt.Sprintf("host-%d", index+1), LastSeen: m.LastSeen, Freshness: "fresh", LogicalCPUs: logicalCPUs(m), FreeCPUCores: m.FreeCPUCores, AcceleratorMemoryGB: target.TotalMemoryGB, FreeAcceleratorMemoryGB: target.FreeMemoryGB, Prepared: "unknown", Builds: map[string]string{}, Qualification: "unknown", Admission: "unknown"}
			if m.CapabilityProfile != nil {
				h.HardwareObservedAt = m.CapabilityProfile.RecordedAt
			}
			if m.LastSeen.IsZero() || machineTarget(target).OS == "" || machineTarget(target).Arch == "" {
				h.Freshness = "unknown"
			} else if !q.workerFresh(m, q.clock.Now()) {
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
				j = &Job{ID: "portability", State: "queued", Spec: s, CommandDiscovery: discovery}
				h.Qualification = "plain command: native qualification not applicable; result acceptance is separate"
				j.EnvironmentRecipes = sets["main"]
				j.SidecarEnvironmentRecipes = map[string]map[string]EnvironmentRecipe{}
				h.Prepared = "not required: installed tools unverified"
				for _, name := range sortedPortabilityKeys(sets) {
					recipes := sets[name]
					if name != "main" {
						j.SidecarEnvironmentRecipes[name] = recipes
					}
					state := "unknown: recipe unavailable"
					if recipe, ok := commandEnvironmentRecipe(j, recipes, target); ok {
						state = "prepare-needed"
						if slices.Contains(target.Environments, recipe.Key) && recipe.Key != "" {
							state = "ready"
						}
						for _, b := range target.EnvironmentReports {
							if b.Key == recipe.Key && recipe.Key != "" && state != "ready" {
								state = portabilityBuildState(b.State)
							}
						}
					}
					h.Builds[safePortabilityID(name)] = state
				}
				if len(sets) > 0 {
					h.Prepared = "prepared"
					for _, state := range h.Builds {
						if state != "ready" {
							h.Prepared = "prepare-needed or unavailable: inspect member builds"
						}
					}
				}
			}
			if j != nil {
				if normalizeResources(&j.Spec) != nil {
					h.Admission = "incomplete resource metadata"
				} else {
					candidates := q.memoryCandidates(j, target)
					h.Admission = "blocked by resources, environment or qualification"
					if req.Spec != nil && !req.Spec.extensionJob() {
						h.Admission = "blocked by authored requirements, resources or environments"
					}
					for _, candidate := range candidates {
						fit := targetFit{}
						fitsResourcesChecked(candidate, target, target.FreeMemoryGB, &fit)
						if fit.failures > 0 {
							h.Admission = "blocked: " + fit.first
							if fit.first == "shared host profile" && target.SharedHostReason != "" && target.SharedHostReason == q.hostPressureReason(target) {
								h.Admission = "temporarily unavailable: " + target.SharedHostReason
							}
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
		if req.Spec != nil && !req.Spec.extensionJob() {
			commandPortabilityTarget(r, *req.Spec, sets)
			if discovery != nil {
				r.AuthoredEligibility = "excluded: outside discovered member-lock/runtime intersection"
				if slices.Contains(discovery.Targets, r.Target) {
					r.AuthoredEligibility = "discovered candidate: fresh inventory and admission required"
				}
				if discoveryError != nil {
					r.Actions = append(r.Actions, discoveryError.Error())
				}
				if q.commandDiscoveryProduction(&Job{Spec: *req.Spec, CommandDiscovery: discovery}) {
					r.Parity = "held: exact target/recipe parity required"
					r.Actions = append(r.Actions, commandDiscoveryParityAction)
				}
			}
		}
		if !r.Observed {
			r.Actions = append(r.Actions, "Target absent from inventory; add a native worker before claiming cluster support.")
		}
		if r.Runtime != "supported" {
			r.Actions = append(r.Actions, r.Runtime+"; a software variant cannot supply the missing Mycelium runtime.")
		}
		if e != nil && !r.Declared {
			r.Actions = append(r.Actions, "Investigate an exact variant with its own resources, implementation and checks; preserve genuine authored hard requirements. Missing coverage alone does not prove one.")
		}
		platform := lockPlatform(r.Target)
		if platform == "" {
			platform = r.Target.OS + "/" + r.Target.Arch + " (verify package-manager target name)"
		}
		r.Actions = append(r.Actions, "Cover "+platform+" with uv.lock + pyproject.toml or pixi.lock + pixi.toml; reuse a multi-target lock when valid, otherwise split recipes. Verify frozen installation on this target; never solve on workers.")
		for _, env := range r.Environments {
			if env.Inputs == "unknown: selected source not inspected" {
				r.Actions = append(r.Actions, "Inspect the selected Git/snapshot source or query a registered frozen extension; working-tree lock files do not establish selected-source coverage.")
			} else if env.Inputs != "present" {
				r.Actions = append(r.Actions, "Supply missing manifest/lock/build inputs for environment "+env.Name+"; lock coverage is "+env.LockCoverage+".")
			}
		}
		device := r.Target.Platform + ":N"
		if name := backendFor(r.Target.Platform).applicationDevice; name != "" {
			device = name
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
	read := func(name string) []byte { return environmentInput(r, files, name) }
	names := []string{"uv.lock", "pyproject.toml"}
	if backend == "pixi" {
		names = []string{"pixi.lock", "pixi.toml"}
	} else if backend == "npm" {
		names = []string{"package-lock.json", "package.json"}
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
	if backend == "pixi" && out.Inputs == "present" {
		coverage := environmentLockCoverage(r, files, t.Platform)
		out.LockCoverage = coverage.State
		if strings.HasPrefix(coverage.State, "declared:") && !slices.Contains(coverage.Targets, t) {
			out.LockCoverage = "missing target"
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
	for _, d := range r.Linkage {
		fmt.Fprintln(&out, "Warning: "+d.String())
	}
	if _, err := fmt.Fprintf(&out, "Inventory: %s (observed %s)\n", r.Inventory, r.ObservedAt.Format(time.RFC3339)); err != nil {
		return err
	}
	for _, requirement := range r.Requirements {
		fmt.Fprintf(&out, "Requirement %s: %s reason; %s\n", requirement.Constraint, requirement.ReasonSource, requirement.Reason)
	}
	for _, t := range r.Targets {
		fmt.Fprintf(&out, "\n%s: observed=%t declared=%t runtime=%s\n", targetLabel(t.Target), t.Observed, t.Declared, t.Runtime)
		if t.AuthoredEligibility != "" {
			fmt.Fprintf(&out, "  authored=%s; environment coverage=%s; parity=%s\n", t.AuthoredEligibility, t.EnvironmentCoverage, t.Parity)
		}
		if t.Needs != nil {
			if t.Target.Platform == "cpu" {
				fmt.Fprintf(&out, "  resources: CPU=%d host=%g GiB automatic_memory=%t\n", t.Needs.CPUCores, max(t.Needs.MemoryGB, t.Needs.HostMemoryGB), t.Needs.AutomaticMemory)
			} else {
				fmt.Fprintf(&out, "  resources: CPU=%d accelerator=%g GiB host=%g GiB automatic_memory=%t\n", t.Needs.CPUCores, t.Needs.MemoryGB, t.Needs.HostMemoryGB, t.Needs.AutomaticMemory)
			}
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
			fmt.Fprintf(&out, "  %s: %s; CPU=%d free=%d; ", h.Host, h.Freshness, h.LogicalCPUs, h.FreeCPUCores)
			if t.Target.Platform != "cpu" {
				fmt.Fprintf(&out, "accelerator=%g GiB free=%g; ", h.AcceleratorMemoryGB, h.FreeAcceleratorMemoryGB)
			}
			fmt.Fprintf(&out, "%s; %s; admission=%s\n", h.Prepared, h.Qualification, h.Admission)
			if h.SharedHost != nil {
				fmt.Fprintf(&out, "    shared interactive fallback: reserve=%g GiB; native CPU commands in prepared environments; nice>=10; inspect worker CPU enforcement and pressure status\n", h.SharedHost.InteractiveReserveGB)
			}
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
