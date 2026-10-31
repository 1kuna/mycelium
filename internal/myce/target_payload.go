package myce

import (
	"fmt"
	"slices"
)

func needsPortabilityProof(s Spec) bool {
	return !s.Portable && slices.ContainsFunc(s.Targets, func(t ExecutionTarget) bool { return t.Arch == "" })
}

func portabilityRequired(s Spec, why string) error {
	if !needsPortabilityProof(s) {
		return nil
	}
	return rejectFieldRule("job_placement_invalid", fmt.Errorf("OS-only targets need an explicit arch or portable: true because %s; declare the artifact's required arch, build from source on the worker, or assert portable: true after verifying every matching architecture", why))
}

// ValidateTargetPortability uses positive evidence, never binary sniffing.
// FileRef carries no executable/type provenance: uploaded bytes (including the
// generated source archive and shared inputs) need the author's assertion.
func ValidateTargetPortability(s Spec) error {
	if len(s.Files) > 0 || len(s.SharedInputs) > 0 || s.After != nil {
		return portabilityRequired(s, "uploaded or inherited payloads may contain executables or archives")
	}
	if s.Run.Docker {
		return portabilityRequired(s, "a container image is not known to cover every architecture")
	}
	return nil
}

func requirePayloadArch(s Spec, osName string, arches []string, why string) error {
	if !needsPortabilityProof(s) {
		return nil
	}
	slices.Sort(arches)
	arches = slices.Compact(arches)
	if len(arches) != 1 {
		return nil
	}
	for _, t := range s.Targets {
		if t.Arch == "" && t.OS == osName {
			return rejectFieldRule("job_placement_invalid", fmt.Errorf("target os: %s requires arch: %s because %s has only that architecture; declare arch: %s or portable: true after verifying portability", osName, arches[0], why, arches[0]))
		}
	}
	return nil
}

func validateTargetExtensions(s Spec, extensions []Extension) error {
	if !needsPortabilityProof(s) {
		return nil
	}
	for _, t := range s.Targets {
		if t.Arch != "" {
			continue
		}
		var arches []string
		for _, e := range extensions {
			if len(e.Variants) == 0 {
				return portabilityRequired(s, "the extension has no architecture-specific variant coverage")
			}
			for _, v := range e.Variants {
				if t.matches(v.Target) {
					arches = append(arches, v.Target.Arch)
				}
			}
		}
		if err := requirePayloadArch(Spec{Targets: []ExecutionTarget{t}}, t.OS, arches, "the matching extension variants"); err != nil {
			return err
		}
	}
	return nil
}

func targetPayloadPlatform(s Spec, platform string) bool {
	if s.Needs.cpuOnly() {
		return platform == "cpu"
	}
	return platform != "cpu" && (len(s.Needs.Platform.Required) == 0 || slices.Contains(s.Needs.Platform.Required, platform))
}

func validateTargetRecipes(s Spec, recipes map[string]EnvironmentRecipe) error {
	if !needsPortabilityProof(s) {
		return nil
	}
	for platform, r := range recipes {
		if !targetPayloadPlatform(s, platform) {
			continue
		}
		relevant := slices.ContainsFunc(s.Targets, func(t ExecutionTarget) bool { return t.Arch == "" && (t.Platform == "" || t.Platform == platform) })
		if !relevant {
			continue
		}
		if r.Target != nil {
			if err := requirePayloadArch(s, r.Target.OS, []string{r.Target.Arch}, "the environment recipe"); err != nil {
				return err
			}
		}
		// A hook alongside Binary still copies the prebuilt artifact; only a source
		// build with no shipped binary is positive worker-native build evidence.
		if r.Binary != "" {
			return portabilityRequired(s, "the environment ships a prebuilt artifact, even if it also has a build hook")
		}
		if len(r.Build) == 0 {
			return portabilityRequired(s, "the environment is not built from source on the worker")
		}
	}
	return nil
}
