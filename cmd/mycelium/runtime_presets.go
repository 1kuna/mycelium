package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
	storesqlite "mycelium/internal/store/sqlite"
)

type runtimePresetLoadReport struct {
	Presets []domain.Preset
	Blocked []engineApplyBlocked
}

func loadRuntimePresets(ctx context.Context, store *storesqlite.Store, nodes ...domain.Node) (runtimePresetLoadReport, error) {
	base, err := store.ListPresets(ctx)
	if err != nil {
		return runtimePresetLoadReport{}, err
	}
	profiles, err := store.ListEngineProfiles(ctx)
	if err != nil {
		return runtimePresetLoadReport{}, err
	}
	plan, ok, err := latestBootstrapPlan(ctx, store)
	if err != nil {
		return runtimePresetLoadReport{}, err
	}
	targetNode := domain.Node{}
	if len(nodes) > 0 {
		targetNode = nodes[0]
	}
	targetHost := domain.HostFacts{}
	if ok {
		targetHost = plan.Host
	}
	annotated, blocked := annotateStoredPresetsWithEngineReadiness(base, profiles, targetHost, targetNode)
	if !ok {
		sortPresets(annotated)
		return runtimePresetLoadReport{Presets: annotated, Blocked: blocked}, nil
	}
	ready := readyEngineProfilesForPlan(plan, profiles)
	generated, generatedBlocked := generatedPresetsFromPlan(plan, ready, annotated)
	for i := range generated {
		if generated[i].EngineReadiness == "" {
			generated[i].EngineReadiness = domain.EngineReadinessReadyProfile
			generated[i].EngineReadinessReason = "saved ready engine profile"
		}
	}
	presets := mergeGeneratedPresets(annotated, generated)
	sortPresets(presets)
	blocked = append(blocked, generatedBlocked...)
	return runtimePresetLoadReport{Presets: presets, Blocked: blocked}, nil
}

func latestBootstrapPlan(ctx context.Context, store *storesqlite.Store) (domain.BootstrapPlan, bool, error) {
	plans, err := store.ListBootstrapPlans(ctx)
	if err != nil {
		return domain.BootstrapPlan{}, false, err
	}
	if len(plans) == 0 {
		return domain.BootstrapPlan{}, false, nil
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.After(plans[j].CreatedAt) })
	return plans[0], true, nil
}

func readyEngineProfilesForPlan(plan domain.BootstrapPlan, profiles []domain.EngineProfile) []domain.EngineProfile {
	savedByID := engineProfilesByID(profiles)
	ready := make([]domain.EngineProfile, 0, len(plan.ResultingProfiles))
	for _, planned := range plan.ResultingProfiles {
		saved, ok := savedByID[planned.ID]
		if !ok || !saved.Ready {
			continue
		}
		if _, ok, _ := enginecompat.ProfileMatchesHost(saved, plan.Host, ""); !ok {
			continue
		}
		ready = append(ready, markGeneratedEngineProfile(saved))
	}
	return ready
}

func annotateStoredPresetsWithEngineReadiness(presets []domain.Preset, profiles []domain.EngineProfile, targetHost domain.HostFacts, targetNode domain.Node) ([]domain.Preset, []engineApplyBlocked) {
	profileByID := engineProfilesByID(profiles)
	out := make([]domain.Preset, 0, len(presets))
	var blocked []engineApplyBlocked
	for _, preset := range presets {
		if preset.EngineProfileID == "" {
			preset.EngineReadiness = domain.EngineReadinessLegacyConfigUnverified
			preset.EngineReadinessReason = fmt.Sprintf("legacy_config_unverified: preset %q has no saved engine profile id", preset.ID)
			out = append(out, preset)
			continue
		}
		profile, ok := profileByID[preset.EngineProfileID]
		if !ok {
			preset.EngineReadiness = domain.EngineReadinessLegacyConfigUnverified
			preset.EngineReadinessReason = fmt.Sprintf("legacy_config_unverified: engine profile %q is not saved", preset.EngineProfileID)
			out = append(out, preset)
			continue
		}
		if !profile.Ready {
			reason := profile.UnreadyReason
			if reason == "" {
				reason = "saved engine profile is not ready"
			}
			blocked = append(blocked, engineApplyBlocked{Kind: "preset", ID: preset.ID, Backend: preset.Backend, Reason: reason})
			continue
		}
		if ok, reason := profileMatchesRuntimeTarget(profile, preset, targetHost, targetNode); !ok {
			if isCompatibilityKeyIncomplete(reason) {
				preset.EngineReadiness = domain.EngineReadinessCompatibilityKeyIncomplete
				preset.EngineReadinessReason = reason
				out = append(out, preset)
				continue
			}
			blocked = append(blocked, engineApplyBlocked{Kind: "preset", ID: preset.ID, Backend: preset.Backend, Reason: reason})
			continue
		}
		preset.EngineReadiness = domain.EngineReadinessReadyProfile
		preset.EngineReadinessReason = "saved ready engine profile"
		out = append(out, preset)
	}
	return out, blocked
}

func profileMatchesRuntimeTarget(profile domain.EngineProfile, preset domain.Preset, host domain.HostFacts, node domain.Node) (bool, string) {
	var ok bool
	var reason string
	switch {
	case host.OS != "" || host.Arch != "" || host.Platform != "":
		_, ok, reason = enginecompat.ProfileMatchesHost(profile, host, preset.ModelFormat)
	case node.OS != "" || node.Arch != "":
		_, ok, reason = enginecompat.ProfileMatchesNode(profile, node, preset.ModelFormat)
	default:
		if enginecompat.KeyComplete(profile.CompatibilityKey) {
			return true, ""
		}
		reason = enginecompat.IncompleteReason(profile)
	}
	return ok, reason
}

func isCompatibilityKeyIncomplete(reason string) bool {
	return strings.HasPrefix(reason, "compatibility_key_incomplete")
}

func logRuntimePresetLoadReport(report runtimePresetLoadReport) {
	for _, blocked := range report.Blocked {
		log.Printf("mycelium runtime preset blocked: kind=%s id=%s backend=%s reason=%s", blocked.Kind, blocked.ID, blocked.Backend, blocked.Reason)
	}
	for _, preset := range report.Presets {
		switch preset.EngineReadiness {
		case domain.EngineReadinessLegacyConfigUnverified, domain.EngineReadinessCompatibilityKeyIncomplete:
			log.Printf("mycelium runtime preset %s: id=%s backend=%s reason=%s", preset.EngineReadiness, preset.ID, preset.Backend, preset.EngineReadinessReason)
		}
	}
}

func sortPresets(presets []domain.Preset) {
	sort.Slice(presets, func(i, j int) bool { return presets[i].ID < presets[j].ID })
}
