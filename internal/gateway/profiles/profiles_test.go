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
	for _, backend := range []domain.Backend{domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendMLX} {
		profile, err := DefaultRegistry().ForBackend(backend)
		if err != nil {
			t.Fatalf("ForBackend(%s): %v", backend, err)
		}
		if profile.Format != FormatOpenAI || profile.ChatPath != "/v1/chat/completions" || profile.CompletionPath != "/v1/completions" {
			t.Fatalf("profile = %+v", profile)
		}
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
	if (Registry{}).IsZero() != true || DefaultRegistry().IsZero() {
		t.Fatal("IsZero mismatch")
	}
}
