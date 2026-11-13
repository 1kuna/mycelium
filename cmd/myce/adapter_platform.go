package main

import (
	"fmt"
	"slices"
	"strings"

	"mycelium/internal/myce"
)

// entryPlatformError preserves the allowed set for callers of run and resolve.
type entryPlatformError struct {
	Platform string
	Allowed  []string
}

func (e *entryPlatformError) Error() string {
	return fmt.Sprintf("entry_platform_not_allowed: platform %q is not allowed; allowed: [%s]", e.Platform, strings.Join(e.Allowed, ", "))
}

// Narrow the lowered recipe so native variants and plain commands share the
// same boundary. Explicit targets remain constraints, including their OS/arch.
func narrowEntryPlatform(p *executionProfile, platform string) error {
	allows := func(candidate string) bool {
		if p.Native != nil {
			return slices.ContainsFunc(p.Native.Variants, func(v myce.ExtensionVariant) bool { return v.Target.Platform == candidate })
		}
		n := p.Needs
		cpu := slices.Equal(n.Platform.Required, []string{"cpu"}) || n.CPUCores > 0 && !n.Platform.Any && len(n.Platform.Required)+len(n.Platform.Preferred) == 0
		if candidate == "cpu" {
			if !cpu && !p.AllowCPUFallback {
				return false
			}
		} else if cpu || len(n.Platform.Required) > 0 && !slices.Contains(n.Platform.Required, candidate) {
			return false
		}
		return len(p.Targets) == 0 || slices.ContainsFunc(p.Targets, func(t myce.ExecutionTarget) bool { return t.Platform == "" || t.Platform == candidate })
	}
	var allowed []string
	for _, candidate := range []string{"cpu", "cuda", "fake", "metal", "xpu"} {
		if allows(candidate) {
			allowed = append(allowed, candidate)
		}
	}
	if !slices.Contains(allowed, platform) {
		return &entryPlatformError{platform, allowed}
	}
	p.Targets = slices.DeleteFunc(slices.Clone(p.Targets), func(t myce.ExecutionTarget) bool { return t.Platform != "" && t.Platform != platform })
	if p.Native != nil {
		x := *p.Native
		x.Variants = slices.DeleteFunc(slices.Clone(x.Variants), func(v myce.ExtensionVariant) bool { return v.Target.Platform != platform })
		p.Native = &x
	} else {
		p.Needs.Platform = myce.Platform{Required: []string{platform}}
		// A GPU-narrowed test must stay on that GPU even if the entry opted
		// into fallback. CPU narrowing has already checked the authored opt-in.
		p.AllowCPUFallback = false
	}
	return nil
}
