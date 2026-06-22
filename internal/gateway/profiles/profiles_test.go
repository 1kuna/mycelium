package profiles

import (
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestRegistryFailsUnknownBackendLoudly(t *testing.T) {
	_, err := DefaultRegistry().ForBackend(domain.Backend("missing"))
	if err == nil || !strings.Contains(err.Error(), "unknown provider profile") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultRegistryResolvesOpenAICompatibleBackends(t *testing.T) {
	for _, backend := range []domain.Backend{domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendMLX, domain.BackendCustom} {
		profile, err := DefaultRegistry().ForBackend(backend)
		if err != nil {
			t.Fatalf("ForBackend(%s): %v", backend, err)
		}
		if profile.Format != FormatOpenAI || profile.ChatPath != "/v1/chat/completions" || profile.CompletionPath != "/v1/completions" {
			t.Fatalf("profile = %+v", profile)
		}
	}
	openvino, err := DefaultRegistry().ForBackend(domain.BackendOpenVINO)
	if err != nil {
		t.Fatalf("ForBackend(openvino): %v", err)
	}
	if openvino.Format != FormatOpenAI || openvino.ChatPath != "/v1/chat/completions" || openvino.CompletionPath != "" || openvino.SupportsStream {
		t.Fatalf("openvino profile = %+v", openvino)
	}
	profile, err := DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	byID, err := DefaultRegistry().ByID(profile.ID)
	if err != nil || byID.ID != profile.ID {
		t.Fatalf("ByID = %+v %v", byID, err)
	}
	if _, err := DefaultRegistry().ByID("missing"); err == nil {
		t.Fatal("missing profile succeeded")
	}
	anthropic, err := DefaultRegistry().ByID("anthropic")
	if err != nil || anthropic.Format != FormatAnthropic || anthropic.Backend != "" {
		t.Fatalf("anthropic profile = %+v %v", anthropic, err)
	}
	if (Registry{}).IsZero() != true || DefaultRegistry().IsZero() {
		t.Fatal("IsZero mismatch")
	}
}
