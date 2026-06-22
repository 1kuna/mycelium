package engine

import (
	"context"
	"fmt"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
	"mycelium/internal/ports"
)

type ReadinessChecker struct {
	Registry ports.EngineRegistry
	Mode     domain.EngineReadinessMode
	Target   domain.CompatibilityKey
}

func NewReadinessChecker(registry ports.EngineRegistry, mode domain.EngineReadinessMode) ReadinessChecker {
	if mode == "" {
		mode = domain.EngineReadinessLegacyAllow
	}
	return ReadinessChecker{Registry: registry, Mode: mode}
}

func NewReadinessCheckerForKey(registry ports.EngineRegistry, mode domain.EngineReadinessMode, target domain.CompatibilityKey) ReadinessChecker {
	checker := NewReadinessChecker(registry, mode)
	checker.Target = target
	return checker
}

func (c ReadinessChecker) CheckEngineReadiness(ctx context.Context, node domain.Node, preset domain.Preset) (domain.EngineReadinessCheck, error) {
	if preset.Backend == "" {
		return domain.EngineReadinessCheck{}, fmt.Errorf("preset %q has no backend for engine readiness preflight", preset.ID)
	}
	if c.Registry == nil {
		return c.missing(preset.Backend, "engine registry is not configured")
	}
	profiles, err := c.Registry.ListEngineProfiles(ctx)
	if err != nil {
		return domain.EngineReadinessCheck{}, fmt.Errorf("engine readiness registry read failed: %w", err)
	}
	var unready *domain.EngineProfile
	var incomplete string
	var mismatch string
	for i := range profiles {
		profile := profiles[i]
		if !profileBackendAndLabelsMatch(profile, node, preset) {
			continue
		}
		_, keyMatches, reason := profileCompatibilityMatches(profile, node, preset, c.Target)
		if !keyMatches {
			if strings.HasPrefix(reason, "compatibility_key_incomplete") {
				if incomplete == "" {
					incomplete = reason
				}
				continue
			}
			if mismatch == "" {
				mismatch = reason
			}
			continue
		}
		if profile.Ready {
			return domain.EngineReadinessCheck{
				Backend:   preset.Backend,
				ProfileID: profile.ID,
				Status:    domain.EngineReadinessReadyProfile,
				Ready:     true,
				Reason:    "saved ready engine profile",
			}, nil
		}
		if unready == nil {
			unready = &profile
		}
	}
	if unready != nil {
		reason := unready.UnreadyReason
		if reason == "" {
			reason = "saved engine profile is not ready"
		}
		return domain.EngineReadinessCheck{}, fmt.Errorf("engine profile %q for backend %s is unready: %s", unready.ID, preset.Backend, reason)
	}
	if mismatch != "" {
		return domain.EngineReadinessCheck{}, fmt.Errorf("%s", mismatch)
	}
	if incomplete != "" {
		return c.incomplete(preset.Backend, incomplete)
	}
	return c.missing(preset.Backend, fmt.Sprintf("no saved ready engine profile for backend %s", preset.Backend))
}

func (c ReadinessChecker) missing(backend domain.Backend, reason string) (domain.EngineReadinessCheck, error) {
	reason = "legacy_config_unverified: " + reason + "; run mycelium bootstrap --doctor --save-plan"
	if c.Mode == domain.EngineReadinessStrict {
		return domain.EngineReadinessCheck{}, fmt.Errorf("%s", reason)
	}
	return domain.EngineReadinessCheck{
		Backend: backend,
		Status:  domain.EngineReadinessLegacyConfigUnverified,
		Ready:   true,
		Reason:  reason,
	}, nil
}

func (c ReadinessChecker) incomplete(backend domain.Backend, reason string) (domain.EngineReadinessCheck, error) {
	reason = reason + "; run mycelium bootstrap --doctor --save-plan"
	if c.Mode == domain.EngineReadinessStrict {
		return domain.EngineReadinessCheck{}, fmt.Errorf("%s", reason)
	}
	return domain.EngineReadinessCheck{
		Backend: backend,
		Status:  domain.EngineReadinessCompatibilityKeyIncomplete,
		Ready:   true,
		Reason:  reason,
	}, nil
}

func profileBackendAndLabelsMatch(profile domain.EngineProfile, node domain.Node, preset domain.Preset) bool {
	if profile.Backend != preset.Backend {
		return false
	}
	for key, want := range profile.RequiredLabels {
		if node.Labels == nil || node.Labels[key] != want {
			return false
		}
	}
	if node.OS != "" && len(profile.SupportedPlatforms) > 0 {
		if !platformListContainsOS(profile.SupportedPlatforms, node.OS) {
			return false
		}
	}
	if node.OS != "" && profile.ArtifactPlatform != "" && !platformContainsOS(profile.ArtifactPlatform, node.OS) {
		return false
	}
	return true
}

func profileCompatibilityMatches(profile domain.EngineProfile, node domain.Node, preset domain.Preset, target domain.CompatibilityKey) (domain.CompatibilityKey, bool, string) {
	if target != (domain.CompatibilityKey{}) {
		if target.ModelFormat == "" {
			target.ModelFormat = preset.ModelFormat
		}
		return enginecompat.ProfileMatchesKey(profile, target)
	}
	return enginecompat.ProfileMatchesNode(profile, node, preset.ModelFormat)
}

func platformListContainsOS(platforms []string, os string) bool {
	for _, platform := range platforms {
		if platformContainsOS(platform, os) {
			return true
		}
	}
	return false
}

func platformContainsOS(platform, os string) bool {
	if platform == "" || os == "" {
		return true
	}
	return platform == os || strings.HasPrefix(platform, os+"/")
}

var _ ports.EngineReadinessChecker = ReadinessChecker{}
