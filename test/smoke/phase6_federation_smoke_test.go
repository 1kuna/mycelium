//go:build smoke

package smoke

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPhase6FederationSubmitAnywhereSmoke(t *testing.T) {
	gatewayA := os.Getenv("MYCELIUM_FEDERATION_GATEWAY_A")
	gatewayB := os.Getenv("MYCELIUM_FEDERATION_GATEWAY_B")
	model := os.Getenv("MYCELIUM_FEDERATION_MODEL")
	if gatewayA != "" || gatewayB != "" || model != "" {
		if gatewayA == "" || gatewayB == "" || model == "" {
			t.Fatal("set MYCELIUM_FEDERATION_GATEWAY_A, MYCELIUM_FEDERATION_GATEWAY_B, and MYCELIUM_FEDERATION_MODEL together")
		}
		runPhase6ManualFederationSmoke(t, gatewayA, gatewayB, model)
		return
	}
	t.Skip("set MYCELIUM_FEDERATION_GATEWAY_A, MYCELIUM_FEDERATION_GATEWAY_B, and MYCELIUM_FEDERATION_MODEL for real multi-peer Phase 6 federation smoke")
}

func runPhase6ManualFederationSmoke(t *testing.T, gatewayA, gatewayB, model string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	assertGatewayChatEventually(t, ctx, gatewayA, model)
	assertGatewayChatEventually(t, ctx, gatewayB, model)
}
