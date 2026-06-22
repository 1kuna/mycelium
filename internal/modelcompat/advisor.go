package modelcompat

import (
	"fmt"
	"sort"
	"strings"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/enginecatalog"
	"mycelium/internal/enginecompat"
)

const (
	StatusCanRunNow                  = "can_run_now"
	StatusCanRunAfterEngineSetup     = "can_run_after_engine_setup"
	StatusNeedsArtifactFormat        = "needs_artifact_format"
	StatusNotSupported               = "not_supported"
	StatusBlocked                    = "blocked"
	StatusLegacyConfigUnverified     = "legacy_config_unverified"
	StatusCompatibilityKeyIncomplete = "compatibility_key_incomplete"
)

type Artifact struct {
	Ref    string `json:"ref"`
	Format string `json:"format"`
	Scope  string `json:"scope,omitempty"`
}

type Request struct {
	Model         string
	Artifacts     []Artifact
	Plans         []domain.BootstrapPlan
	Profiles      []domain.EngineProfile
	LegacyPresets []domain.Preset
}

type Report struct {
	Model     string     `json:"model"`
	Artifacts []Artifact `json:"artifacts"`
	Rows      []Row      `json:"rows"`
}

type Row struct {
	HostID          string         `json:"host_id"`
	HostPlatform    string         `json:"host_platform,omitempty"`
	ArtifactRef     string         `json:"artifact_ref"`
	ArtifactFormat  string         `json:"artifact_format"`
	ArtifactScope   string         `json:"artifact_scope"`
	Backend         domain.Backend `json:"backend,omitempty"`
	EngineFamily    string         `json:"engine_family,omitempty"`
	EngineProfileID string         `json:"engine_profile_id,omitempty"`
	Source          string         `json:"source,omitempty"`
	Status          string         `json:"status"`
	Reason          string         `json:"reason,omitempty"`
	NeededFormat    string         `json:"needed_format,omitempty"`
	Multimodal      string         `json:"multimodal,omitempty"`
}

func Advise(req Request) (Report, error) {
	model := strings.TrimSpace(req.Model)
	if model == "" {
		return Report{}, fmt.Errorf("model is required")
	}
	if len(req.Artifacts) == 0 {
		return Report{}, fmt.Errorf("at least one artifact is required")
	}
	artifacts := normalizeArtifacts(req.Artifacts)
	for _, artifact := range artifacts {
		if artifact.Ref == "" {
			return Report{}, fmt.Errorf("artifact ref is required")
		}
		if artifact.Format == "" {
			return Report{}, fmt.Errorf("artifact format is required for %q", artifact.Ref)
		}
	}
	plans := latestPlansByHost(req.Plans)
	saved := profilesByID(req.Profiles)
	report := Report{Model: model, Artifacts: artifacts}
	capabilities := enginecatalog.Default()
	for _, plan := range plans {
		report.Rows = append(report.Rows, rowsForPlan(plan, saved, artifacts, capabilities)...)
	}
	report.Rows = append(report.Rows, legacyRows(req.LegacyPresets, artifacts, plannedHosts(plans))...)
	sortRows(report.Rows)
	return report, nil
}

func normalizeArtifacts(in []Artifact) []Artifact {
	out := make([]Artifact, 0, len(in))
	for _, artifact := range in {
		artifact.Ref = strings.TrimSpace(artifact.Ref)
		artifact.Format = normalizeFormat(artifact.Format)
		artifact.Scope = artifactScope(artifact.Format)
		out = append(out, artifact)
	}
	return out
}

func latestPlansByHost(plans []domain.BootstrapPlan) []domain.BootstrapPlan {
	byHost := map[string]domain.BootstrapPlan{}
	for _, plan := range plans {
		hostID := hostID(plan.Host)
		if hostID == "" {
			hostID = plan.ID
		}
		existing, ok := byHost[hostID]
		if !ok || plan.CreatedAt.After(existing.CreatedAt) {
			byHost[hostID] = plan
		}
	}
	out := make([]domain.BootstrapPlan, 0, len(byHost))
	for _, plan := range byHost {
		out = append(out, plan)
	}
	sort.Slice(out, func(i, j int) bool { return hostID(out[i].Host) < hostID(out[j].Host) })
	return out
}

func rowsForPlan(plan domain.BootstrapPlan, saved map[string]domain.EngineProfile, artifacts []Artifact, capabilities []enginecatalog.Capability) []Row {
	host := plan.Host
	hostName := hostID(host)
	if hostName == "" {
		hostName = plan.ID
	}
	platform := host.Platform
	if platform == "" && (host.OS != "" || host.Arch != "") {
		platform = host.OS + "/" + host.Arch
	}
	rows := catalogRows(hostName, platform, host, artifacts, capabilities)
	for _, artifact := range artifacts {
		for _, planned := range plan.ResultingProfiles {
			profile := planned
			found := true
			if planned.ID != "" {
				var ok bool
				profile, ok = saved[planned.ID]
				found = ok
			}
			row := savedProfileRow(hostName, platform, artifact, planned)
			if !found {
				row.Status = StatusBlocked
				row.Source = "saved_readiness"
				row.Reason = fmt.Sprintf("engine profile %q is present in plan but not saved in registry", planned.ID)
				rows = upsertRow(rows, row)
				continue
			}
			if !supportsFormat(profile, artifact.Format) {
				row.Status = StatusNeedsArtifactFormat
				row.Source = "saved_readiness"
				row.NeededFormat = neededFormat(profile)
				row.Reason = formatAdvice(profile, artifact.Format)
				rows = upsertRow(rows, row)
				continue
			}
			if !profile.Ready {
				row.Status = StatusBlocked
				row.Source = "saved_readiness"
				row.Reason = profile.UnreadyReason
				if row.Reason == "" {
					row.Reason = "saved engine profile is not ready"
				}
				rows = upsertRow(rows, row)
				continue
			}
			if _, ok, reason := enginecompat.ProfileMatchesHost(profile, host, artifact.Format); !ok {
				row.Status = StatusBlocked
				row.Source = "saved_readiness"
				if strings.HasPrefix(reason, "compatibility_key_incomplete") {
					row.Status = StatusCompatibilityKeyIncomplete
				}
				row.Reason = reason
				rows = upsertRow(rows, row)
				continue
			}
			row.Status = StatusCanRunNow
			row.Source = "saved_readiness"
			row.Reason = "saved ready engine profile matches host compatibility key and artifact format"
			row.EngineProfileID = profile.ID
			row.EngineFamily = engineFamilyForBackend(profile.Backend, capabilities)
			rows = upsertRow(rows, row)
		}
	}
	return rows
}

func catalogRows(hostID, platform string, host domain.HostFacts, artifacts []Artifact, capabilities []enginecatalog.Capability) []Row {
	rows := make([]Row, 0, len(artifacts)*len(capabilities))
	for _, artifact := range artifacts {
		for _, capability := range capabilities {
			support := capability.AssessHost(host)
			row := Row{
				HostID:         hostID,
				HostPlatform:   platform,
				ArtifactRef:    artifact.Ref,
				ArtifactFormat: artifact.Format,
				ArtifactScope:  artifact.Scope,
				Backend:        capability.Backend,
				EngineFamily:   capability.Family,
				Source:         "engine_capability_catalog",
				NeededFormat:   capability.NeededFormat(),
				Multimodal:     capability.Multimodal,
				Reason:         support.Reason,
			}
			switch {
			case !support.Supported:
				row.Status = StatusNotSupported
			case !capability.SupportsFormat(artifact.Format):
				row.Status = StatusNeedsArtifactFormat
				row.Reason = fmt.Sprintf("%s requires %s artifacts on this host; supplied %s", capability.Family, capability.NeededFormat(), artifact.Format)
			default:
				row.Status = StatusCanRunAfterEngineSetup
				row.Reason = fmt.Sprintf("%s; inspect setup with mycelium engines install-plan --backend %s --host %s", row.Reason, capability.Backend, hostID)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func savedProfileRow(hostName, platform string, artifact Artifact, profile domain.EngineProfile) Row {
	return Row{
		HostID:          hostName,
		HostPlatform:    platform,
		ArtifactRef:     artifact.Ref,
		ArtifactFormat:  artifact.Format,
		ArtifactScope:   artifact.Scope,
		Backend:         profile.Backend,
		EngineProfileID: profile.ID,
	}
}

func upsertRow(rows []Row, next Row) []Row {
	for i := range rows {
		if sameCompatibilityRow(rows[i], next) {
			rows[i] = mergeRow(rows[i], next)
			return rows
		}
	}
	return append(rows, next)
}

func sameCompatibilityRow(a, b Row) bool {
	return a.HostID == b.HostID &&
		a.ArtifactRef == b.ArtifactRef &&
		a.ArtifactFormat == b.ArtifactFormat &&
		a.Backend == b.Backend
}

func mergeRow(existing, next Row) Row {
	if next.EngineFamily == "" {
		next.EngineFamily = existing.EngineFamily
	}
	if next.NeededFormat == "" {
		next.NeededFormat = existing.NeededFormat
	}
	if next.Multimodal == "" {
		next.Multimodal = existing.Multimodal
	}
	return next
}

func legacyRows(presets []domain.Preset, artifacts []Artifact, knownHosts map[string]struct{}) []Row {
	seen := map[string]struct{}{}
	var rows []Row
	for _, preset := range presets {
		if preset.GeneratedBy != "" || preset.EngineProfileID != "" {
			continue
		}
		host := strings.TrimSpace(preset.NodeID)
		if host == "" {
			host = "legacy-config"
		}
		key := host + "\x00" + string(preset.Backend)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if _, ok := knownHosts[host]; ok {
			continue
		}
		for _, artifact := range artifacts {
			rows = append(rows, Row{
				HostID:         host,
				ArtifactRef:    artifact.Ref,
				ArtifactFormat: artifact.Format,
				ArtifactScope:  artifact.Scope,
				Backend:        preset.Backend,
				Status:         StatusLegacyConfigUnverified,
				Source:         "legacy_config",
				Reason:         "legacy_config_unverified: manual preset has no saved bootstrap readiness facts",
			})
		}
	}
	return rows
}

func plannedHosts(plans []domain.BootstrapPlan) map[string]struct{} {
	out := map[string]struct{}{}
	for _, plan := range plans {
		if id := hostID(plan.Host); id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func profilesByID(profiles []domain.EngineProfile) map[string]domain.EngineProfile {
	out := map[string]domain.EngineProfile{}
	for _, profile := range profiles {
		if profile.ID != "" {
			out[profile.ID] = profile
		}
	}
	return out
}

func hostID(host domain.HostFacts) string {
	if host.NodeID != "" {
		return host.NodeID
	}
	if host.Platform != "" {
		return host.Platform
	}
	if host.OS != "" || host.Arch != "" {
		return host.OS + "/" + host.Arch
	}
	return ""
}

func supportsFormat(profile domain.EngineProfile, format string) bool {
	if len(profile.SupportedModels) == 0 {
		return false
	}
	for _, supported := range profile.SupportedModels {
		if enginecatalog.FormatsCompatible(supported, format) {
			return true
		}
	}
	return false
}

func neededFormat(profile domain.EngineProfile) string {
	if len(profile.SupportedModels) > 0 {
		formats := append([]string(nil), profile.SupportedModels...)
		sort.Strings(formats)
		return strings.Join(formats, ",")
	}
	switch profile.Backend {
	case domain.BackendMLX:
		return "mlx"
	case domain.BackendOpenVINO:
		return catalog.FormatOpenVINOIR
	case domain.BackendLlamaCpp:
		return catalog.FormatGGUF
	case domain.BackendVLLM, domain.BackendSGLang:
		return catalog.FormatHFTransformers + ",safetensors"
	default:
		return ""
	}
}

func formatAdvice(profile domain.EngineProfile, artifactFormat string) string {
	needed := neededFormat(profile)
	if len(profile.SupportedModels) == 0 {
		return fmt.Sprintf("engine profile %q does not declare supported model formats; needed %s to prove compatibility with %s", profile.ID, needed, artifactFormat)
	}
	switch profile.Backend {
	case domain.BackendMLX:
		return fmt.Sprintf("mlx required for Apple Silicon MLX; artifact is %s", artifactFormat)
	case domain.BackendOpenVINO:
		return fmt.Sprintf("openvino-ir required for OpenVINO; artifact is %s", artifactFormat)
	case domain.BackendLlamaCpp:
		return fmt.Sprintf("gguf required for llama.cpp; vision routes also need a matching mmproj; artifact is %s", artifactFormat)
	case domain.BackendVLLM, domain.BackendSGLang:
		return fmt.Sprintf("hf-transformers or safetensors required for %s-compatible runtime; artifact is %s", profile.Backend, artifactFormat)
	default:
		return fmt.Sprintf("artifact format %s is not compatible; needed %s", artifactFormat, needed)
	}
}

func normalizeFormat(format string) string {
	return enginecatalog.NormalizeFormat(format)
}

func artifactScope(format string) string {
	switch normalizeFormat(format) {
	case enginecatalog.FormatMLX, catalog.FormatOpenVINOIR:
		return "host_specific"
	case catalog.FormatGGUF, catalog.FormatHFTransformers, enginecatalog.FormatSafetensors:
		return "shared_where_backend_matches"
	default:
		return "unknown"
	}
}

func engineFamilyForBackend(backend domain.Backend, capabilities []enginecatalog.Capability) string {
	for _, capability := range capabilities {
		if capability.Backend == backend {
			return capability.Family
		}
	}
	if backend == domain.BackendCustom {
		return "custom/native process"
	}
	return string(backend)
}

func sortRows(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].HostID != rows[j].HostID {
			return rows[i].HostID < rows[j].HostID
		}
		if rows[i].ArtifactRef != rows[j].ArtifactRef {
			return rows[i].ArtifactRef < rows[j].ArtifactRef
		}
		if rows[i].Backend != rows[j].Backend {
			return rows[i].Backend < rows[j].Backend
		}
		return rows[i].Status < rows[j].Status
	})
}
