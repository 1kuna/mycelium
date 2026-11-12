package myce

import (
	"errors"
	"slices"
	"strings"
)

// CommandDiscovery is server-derived from frozen member locks. Its presence,
// rather than a historical spec's portable bit, opts an accepted job into
// discovery. Inventory and normal admission are rechecked at every placement.
type CommandDiscovery struct {
	Targets []ExecutionTarget `json:"targets"`
}

func WantsCommandDiscovery(s Spec) bool {
	return s.Portable && len(s.Targets) == 0 && !s.extensionJob()
}

// validateCommandDiscovery checks declarations only, never paths or payloads.
// portable asserts the whole payload, including downloaded tools and archives.
func validateCommandDiscovery(s Spec) error {
	if !WantsCommandDiscovery(s) {
		return nil
	}
	if err := normalizeResources(&s); err != nil {
		return err
	}
	if err := lowerExecution(&s); err != nil {
		return err
	}
	if !s.Needs.cpuOnly() || s.Environment.Name == "" {
		return errors.New("omitted-target portable discovery requires native CPU commands and a named managed environment with selected immutable lock coverage; otherwise declare explicit targets")
	}
	return nil
}

// Called before frozen files are moved to the blob store, outside q.mu. The
// intersection is frozen with the job; a later catalog edit cannot widen it.
func deriveCommandDiscovery(s Spec, main map[string]EnvironmentRecipe, sidecars map[string]map[string]EnvironmentRecipe) (*CommandDiscovery, error) {
	if !WantsCommandDiscovery(s) {
		return nil, nil
	}
	if err := validateCommandDiscovery(s); err != nil {
		return nil, err
	}
	sets := []map[string]EnvironmentRecipe{main}
	for _, name := range sortedPortabilityKeys(sidecars) {
		sets = append(sets, sidecars[name])
	}
	var targets []ExecutionTarget
	for i, recipes := range sets {
		r, ok := recipes["cpu"]
		coverage := environmentLockCoverage(r, nil, "cpu")
		if !ok || r.Key == "" || !strings.HasPrefix(coverage.State, "declared:") {
			return nil, errors.New("portable discovery has unknown member lock coverage; select a managed CPU recipe with machine-readable selected-lock targets, or declare explicit targets")
		}
		covered := func(t ExecutionTarget) bool {
			return portabilityRuntime(t) == "supported" && slices.Contains(coverage.Targets, t) && (r.Target == nil || *r.Target == t)
		}
		if i == 0 {
			for _, t := range coverage.Targets {
				if covered(t) {
					targets = append(targets, t)
				}
			}
		} else {
			targets = slices.DeleteFunc(targets, func(t ExecutionTarget) bool { return !covered(t) })
		}
	}
	if len(targets) == 0 {
		return nil, errors.New("portable discovery has no supported target covered by every member's selected immutable lock; fix coverage or declare explicit targets")
	}
	return &CommandDiscovery{Targets: placementTargets(targets)}, nil
}

func commandDiscoveryAllows(j *Job, m *Machine) bool {
	return j.CommandDiscovery == nil || slices.Contains(j.CommandDiscovery.Targets, machineTarget(m)) && portabilityRuntime(machineTarget(m)) == "supported"
}

// Keep the base frozen bundle in the job for transport and retention. Only the
// small recipe/target identity changes; no file reading or hashing under q.mu.
func commandEnvironmentRecipe(j *Job, recipes map[string]EnvironmentRecipe, m *Machine) (EnvironmentRecipe, bool) {
	r, ok := recipes[m.GPU]
	if !ok || j.CommandDiscovery == nil {
		return r, ok
	}
	if !commandDiscoveryAllows(j, m) {
		return EnvironmentRecipe{}, false
	}
	target := machineTarget(m)
	r.Target = &target
	r.Key = identityHash(struct {
		Recipe string
		Target ExecutionTarget
	}{r.Key, target})
	return r, true
}

func commandEnvironmentRecipes(j *Job, m *Machine) []EnvironmentRecipe {
	out := []EnvironmentRecipe{}
	if r, ok := commandEnvironmentRecipe(j, j.EnvironmentRecipes, m); ok {
		out = append(out, r)
	}
	for _, name := range sortedPortabilityKeys(j.SidecarEnvironmentRecipes) {
		if r, ok := commandEnvironmentRecipe(j, j.SidecarEnvironmentRecipes[name], m); ok && !slices.ContainsFunc(out, func(old EnvironmentRecipe) bool { return old.Key == r.Key }) {
			out = append(out, r)
		}
	}
	return out
}

func commandPreparationRecipe(j *Job, m *Machine) EnvironmentRecipe {
	recipes := commandEnvironmentRecipes(j, m)
	for _, r := range recipes {
		if !slices.Contains(m.Environments, r.Key) {
			return r
		}
	}
	if len(recipes) > 0 {
		return recipes[0]
	}
	return EnvironmentRecipe{}
}

const commandDiscoveryParityAction = "MY-19: automatic plain-command production discovery is held until exact target/recipe output parity is accepted; use the existing native variant/baseline and production parity workflow for production, or declare an explicit owner-accepted target list. Environment preparation and portable: true are not parity acceptance"

// Policy tiers can promote any authored class; do not rely on its label alone.
func (q *Queue) commandDiscoveryProduction(j *Job) bool {
	return j.CommandDiscovery != nil && (jobClass(j.Spec) == "production" || q.priorityMatch(j.Spec).Tier == "production")
}
