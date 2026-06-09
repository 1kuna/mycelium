//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
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
	_, _, nodeA := assertGatewayChatEventually(t, ctx, gatewayA, model, smokeGatewayToken("MYCELIUM_FEDERATION_GATEWAY_TOKEN_A"))
	if want := os.Getenv("MYCELIUM_FEDERATION_EXPECT_NODE_A"); want != "" && nodeA != want {
		t.Fatalf("gateway A placed on node %q, want %q", nodeA, want)
	}
	_, _, nodeB := assertGatewayChatEventually(t, ctx, gatewayB, model, smokeGatewayToken("MYCELIUM_FEDERATION_GATEWAY_TOKEN_B"))
	if want := os.Getenv("MYCELIUM_FEDERATION_EXPECT_NODE_B"); want != "" && nodeB != want {
		t.Fatalf("gateway B placed on node %q, want %q", nodeB, want)
	}
}

func TestPhase6DeadPeerRescueSmoke(t *testing.T) {
	if os.Getenv("MYCELIUM_DEAD_PEER_RESCUE_ENABLE") != "1" {
		t.Skip("set MYCELIUM_DEAD_PEER_RESCUE_ENABLE=1 plus rescue env vars to run destructive real dead-peer rescue smoke")
	}
	gatewayURL := os.Getenv("MYCELIUM_DEAD_PEER_GATEWAY")
	model := os.Getenv("MYCELIUM_DEAD_PEER_MODEL")
	ownerNode := os.Getenv("MYCELIUM_DEAD_PEER_OWNER_NODE")
	killCommand := os.Getenv("MYCELIUM_DEAD_PEER_KILL_COMMAND")
	registryURL := os.Getenv("MYCELIUM_DEAD_PEER_REGISTRY_URL")
	rpcToken := os.Getenv("MYCELIUM_DEAD_PEER_RPC_TOKEN")
	if gatewayURL == "" || model == "" || ownerNode == "" || killCommand == "" || registryURL == "" || rpcToken == "" {
		t.Fatal("set MYCELIUM_DEAD_PEER_GATEWAY, MYCELIUM_DEAD_PEER_MODEL, MYCELIUM_DEAD_PEER_OWNER_NODE, MYCELIUM_DEAD_PEER_KILL_COMMAND, MYCELIUM_DEAD_PEER_REGISTRY_URL, and MYCELIUM_DEAD_PEER_RPC_TOKEN")
	}
	runPhase6DeadPeerRescueSmoke(t, gatewayURL, model, ownerNode, killCommand, registryURL, rpcToken, smokeGatewayToken("MYCELIUM_DEAD_PEER_GATEWAY_TOKEN"))
}

func runPhase6DeadPeerRescueSmoke(t *testing.T, gatewayURL, model, ownerNode, killCommand, registryURL, rpcToken, gatewayToken string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	resp := startLongGatewayChat(t, ctx, gatewayURL, model, gatewayToken)
	defer resp.Body.Close()
	jobID := resp.Header.Get(gateway.HeaderJob)
	if jobID == "" {
		t.Fatalf("gateway response missing %s headers=%+v", gateway.HeaderJob, resp.Header)
	}
	if got := resp.Header.Get(gateway.HeaderNode); got != ownerNode {
		t.Fatalf("gateway placed on node %q, want owner %q before kill; job=%s", got, ownerNode, jobID)
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", killCommand)
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dead peer kill command failed: %v\n%s", err, data)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr == nil && !bytes.Contains(body, []byte(`"choices"`)) {
		t.Logf("gateway body after owner kill did not complete normally: %s", body)
	} else if readErr != nil {
		t.Logf("gateway body read after owner kill: %v", readErr)
	}
	rec := waitForRegistryEvidence(t, ctx, registryURL, rpcToken, jobID)
	if rec.Status == domain.JobDone {
		return
	}
	if rec.Status == domain.JobFailed || rec.CleanupRequired || rec.CleanupError != "" || rec.RecoveryNote != "" {
		return
	}
	t.Fatalf("registry record for %s has no terminal or recovery evidence: %+v", jobID, rec)
}

func startLongGatewayChat(t *testing.T, ctx context.Context, gatewayURL, model, token string) *http.Response {
	t.Helper()
	prompt := os.Getenv("MYCELIUM_DEAD_PEER_PROMPT")
	if prompt == "" {
		prompt = "Count slowly from one to five hundred. Emit one number per line."
	}
	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":` + quote(prompt) + `}],"max_tokens":512,"stream":true}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("start gateway request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("gateway status = %s body=%s", resp.Status, data)
	}
	return resp
}

func waitForRegistryEvidence(t *testing.T, ctx context.Context, registryURL, rpcToken, jobID string) domain.JobRecord {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last []domain.JobRecord
	for {
		records := fetchRegistrySnapshot(t, ctx, registryURL, rpcToken)
		last = records
		for _, rec := range records {
			if rec.JobID == jobID {
				if rec.Status == domain.JobDone || rec.Status == domain.JobFailed || rec.CleanupRequired || rec.CleanupError != "" || rec.RecoveryNote != "" {
					return rec
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for registry evidence for %s: %v last=%+v", jobID, ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func fetchRegistrySnapshot(t *testing.T, ctx context.Context, registryURL, rpcToken string) []domain.JobRecord {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(registryURL, "/")+"/registry/snapshot", nil)
	if err != nil {
		t.Fatalf("registry request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+rpcToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("registry snapshot: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read registry snapshot: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("registry status = %s body=%s", resp.Status, data)
	}
	var records []domain.JobRecord
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("decode registry snapshot: %v body=%s", err, data)
	}
	return records
}
