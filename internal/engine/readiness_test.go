package engine

import (
	"context"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
	"mycelium/test/mocks"
)

func TestReadinessCheckerAcceptsSavedReadyProfile(t *testing.T) {
	host := domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{keyedProfile(domain.EngineProfile{
		ID:                 "b70-openvino",
		Backend:            domain.BackendOpenVINO,
		ManagedBy:          "system",
		DisplayName:        "OpenVINO",
		BinaryPath:         "openvino-genai-openai",
		Ready:              true,
		SupportedPlatforms: []string{"linux/amd64"},
	}, host, "")}}, domain.EngineReadinessStrict)

	check, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}},
		domain.Preset{ID: "google/gemma-4-31B", Backend: domain.BackendOpenVINO})
	if err != nil {
		t.Fatalf("CheckEngineReadiness: %v", err)
	}
	if !check.Ready || check.ProfileID != "b70-openvino" || check.Status != domain.EngineReadinessReadyProfile {
		t.Fatalf("check = %+v", check)
	}
}

func TestReadinessCheckerReportsMissingProfileAsLegacyUnverified(t *testing.T) {
	checker := NewReadinessChecker(&mocks.EngineRegistry{}, domain.EngineReadinessLegacyAllow)

	check, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "node-a", OS: "linux"},
		domain.Preset{ID: "preset-a", Backend: domain.BackendVLLM})
	if err != nil {
		t.Fatalf("CheckEngineReadiness: %v", err)
	}
	if !check.Ready || check.Status != domain.EngineReadinessLegacyConfigUnverified || !strings.Contains(check.Reason, "legacy_config_unverified") {
		t.Fatalf("check = %+v", check)
	}
}

func TestReadinessCheckerRejectsMissingProfileInStrictMode(t *testing.T) {
	checker := NewReadinessChecker(&mocks.EngineRegistry{}, domain.EngineReadinessStrict)

	_, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "node-a", OS: "linux"},
		domain.Preset{ID: "preset-a", Backend: domain.BackendVLLM})
	if err == nil || !strings.Contains(err.Error(), "legacy_config_unverified") {
		t.Fatalf("err = %v", err)
	}
}

func TestReadinessCheckerRejectsCompatibilityKeyPlatformMismatch(t *testing.T) {
	host := domain.HostFacts{OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{keyedProfile(domain.EngineProfile{
		ID:          "spark-vllm",
		Backend:     domain.BackendVLLM,
		ManagedBy:   "system",
		DisplayName: "vLLM",
		BinaryPath:  "vllm",
		Ready:       true,
	}, host, "")}}, domain.EngineReadinessLegacyAllow)

	_, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
		domain.Preset{ID: "qwen", Backend: domain.BackendVLLM})
	if err == nil || !strings.Contains(err.Error(), "compatibility_key_mismatch") || !strings.Contains(err.Error(), "cpu_arch") {
		t.Fatalf("err = %v", err)
	}
}

func TestReadinessCheckerRejectsCompatibilityKeyAcceleratorMismatch(t *testing.T) {
	host := domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{keyedProfile(domain.EngineProfile{
		ID:          "nvidia-vllm",
		Backend:     domain.BackendVLLM,
		ManagedBy:   "system",
		DisplayName: "vLLM",
		BinaryPath:  "vllm",
		Ready:       true,
	}, host, "")}}, domain.EngineReadinessLegacyAllow)

	_, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}},
		domain.Preset{ID: "qwen", Backend: domain.BackendVLLM})
	if err == nil || !strings.Contains(err.Error(), "compatibility_key_mismatch") || !strings.Contains(err.Error(), "accelerator_vendor") {
		t.Fatalf("err = %v", err)
	}
}

func TestReadinessCheckerMarksIncompleteCompatibilityKey(t *testing.T) {
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{{
		ID:      "old-vllm",
		Backend: domain.BackendVLLM,
		Ready:   true,
	}}}, domain.EngineReadinessLegacyAllow)

	check, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "node-a", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
		domain.Preset{ID: "qwen", Backend: domain.BackendVLLM})
	if err != nil {
		t.Fatalf("CheckEngineReadiness: %v", err)
	}
	if !check.Ready || check.Status != domain.EngineReadinessCompatibilityKeyIncomplete || !strings.Contains(check.Reason, "compatibility_key_incomplete") {
		t.Fatalf("check = %+v", check)
	}
}

func TestReadinessCheckerRejectsSavedUnreadyProfile(t *testing.T) {
	host := domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{keyedProfile(domain.EngineProfile{
		ID:            "b70-vllm",
		Backend:       domain.BackendVLLM,
		ManagedBy:     "system",
		DisplayName:   "vLLM",
		BinaryPath:    "vllm",
		Ready:         false,
		UnreadyReason: "verification failed",
	}, host, "")}}, domain.EngineReadinessLegacyAllow)

	_, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "b70", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
		domain.Preset{ID: "qwen", Backend: domain.BackendVLLM})
	if err == nil || !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestReadinessCheckerRequiresMatchingLabels(t *testing.T) {
	checker := NewReadinessChecker(&mocks.EngineRegistry{Profiles: []domain.EngineProfile{{
		ID:             "custom",
		Backend:        domain.BackendCustom,
		Ready:          true,
		RequiredLabels: map[string]string{"engine.profile": "custom"},
	}}}, domain.EngineReadinessStrict)

	_, err := checker.CheckEngineReadiness(context.Background(),
		domain.Node{ID: "node-a", OS: "linux", Labels: map[string]string{"engine.profile": "other"}},
		domain.Preset{ID: "custom", Backend: domain.BackendCustom})
	if err == nil || !strings.Contains(err.Error(), "no saved ready engine profile") {
		t.Fatalf("err = %v", err)
	}
}

func keyedProfile(profile domain.EngineProfile, host domain.HostFacts, modelFormat string) domain.EngineProfile {
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, modelFormat)
	return profile
}
