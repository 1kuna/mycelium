package myce

import (
	"slices"
	"strings"
)

// commandPortabilityTarget explains breadth without changing the authored spec.
// All members intersect: a broad main lock cannot cover a narrower sidecar.
func commandPortabilityTarget(r *PortabilityTarget, s Spec, sets map[string]map[string]EnvironmentRecipe) {
	r.AuthoredEligibility = "included"
	if len(s.Targets) == 0 {
		r.AuthoredEligibility = "unrestricted targets"
	} else if !slices.ContainsFunc(s.Targets, func(t ExecutionTarget) bool { return t.matches(r.Target) }) {
		r.AuthoredEligibility = "excluded by targets"
	}
	n := s.Needs
	_ = normalizeResources(&s)
	if s.Needs.cpuOnly() && r.Target.Platform != "cpu" || !s.Needs.cpuOnly() && (r.Target.Platform == "cpu" || len(n.Platform.Required) > 0 && !slices.Contains(n.Platform.Required, r.Target.Platform)) {
		r.AuthoredEligibility = "excluded by needs.platform"
	}
	r.Parity = "unknown: owner-accepted output parity required before production widening"
	r.EnvironmentCoverage = "unknown: installed tools are not lock coverage"
	if len(sets) > 0 {
		r.EnvironmentCoverage = "declared: all member locks cover target; native installation unverified"
	}
	_, managedMain := sets["main"]
	unknown, missing := !managedMain, false
	if !managedMain && len(sets) > 0 {
		r.Environments = append(r.Environments, PortabilityEnvironment{Name: "main", Backend: "installed-tools", Inputs: "unknown", LockCoverage: "unknown: installed tools are not lock coverage"})
	}
	for _, name := range sortedPortabilityKeys(sets) {
		recipe, ok := sets[name][r.Target.Platform]
		if !ok {
			r.Environments = append(r.Environments, PortabilityEnvironment{Name: safePortabilityID(name), Backend: "unknown", Inputs: "unknown", LockCoverage: "unknown: recipe unavailable for backend"})
			missing = true
			continue
		}
		env := portabilityEnvironment(name, recipe, nil, r.Target)
		r.Environments = append(r.Environments, env)
		if env.LockCoverage == "missing target" || recipe.Target != nil && *recipe.Target != r.Target {
			missing = true
		} else if env.Inputs != "present" || !strings.HasPrefix(env.LockCoverage, "declared:") {
			unknown = true
		}
	}
	if missing {
		r.EnvironmentCoverage = "missing: a member recipe or lock excludes target"
	} else if unknown {
		r.EnvironmentCoverage = "unknown: member lock coverage or build support unverified"
	}
	if strings.HasPrefix(r.AuthoredEligibility, "excluded") {
		r.Actions = append(r.Actions, "Keep explicit targets binding. Before authoring a wider target list, declare whole-payload portability, including binaries, inputs, archives, shell commands and runtime downloads; obtain owner-accepted output parity for production.")
	}
	switch {
	case missing:
		r.Actions = append(r.Actions, "Add the target to every required member's selected immutable lock/recipe; publish through the existing environment library. Do not infer OS/architecture coverage from cpu alone.")
	case unknown || len(sets) == 0:
		r.Actions = append(r.Actions, "Inspect missing or unsupported lock metadata and verify target-native frozen preparation; unknown coverage is neither Linux-only nor universal support.")
	default:
		r.Actions = append(r.Actions, "Dependency candidate only: reuse ready member environments or prepare them through normal authorized demand. Preparation does not prove application compatibility or MY-19 parity; inspect host build and capacity evidence separately.")
	}
}
