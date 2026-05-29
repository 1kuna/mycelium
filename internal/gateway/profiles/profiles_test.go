package profiles

import (
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestRegistryFailsUnknownBackendLoudly(t *testing.T) {
	_, err := DefaultRegistry().ForBackend(domain.BackendVLLM)
	if err == nil || !strings.Contains(err.Error(), "unknown provider profile") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultRegistryResolvesLlamaCpp(t *testing.T) {
	profile, err := DefaultRegistry().ForBackend(domain.BackendLlamaCpp)
	if err != nil {
		t.Fatalf("ForBackend: %v", err)
	}
	if profile.Format != FormatOpenAI || profile.ChatPath != "/v1/chat/completions" {
		t.Fatalf("profile = %+v", profile)
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
