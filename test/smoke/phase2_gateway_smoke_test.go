//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	"mycelium/internal/node"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
)

func TestPhase2GatewayLocalLlamaCppSmoke(t *testing.T) {
	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model := os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for Phase 2 gateway smoke")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	backendAddr := freeAddr(t)
	agentNode := fixtures.MakeNode()
	agent := node.NewAgent(
		agentNode,
		newSmokeAdapter(binary),
		clock.System{},
		node.WithListenAddr(backendAddr),
		node.WithAllocator(lease.NewAllocator()),
	)
	defer unloadAll(t, ctx, agent)

	preset := fixtures.MakePreset(
		fixtures.WithModelRef(model),
		fixtures.WithContextLength(2048),
		fixtures.WithWeights(1),
		fixtures.WithKVPerToken(0.01),
	)
	directory := gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{agentNode.ID: agent}}
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock.System{}, preset)
	server := httptest.NewServer(gateway.Server{Router: &gateway.Router{
		Placer:   placer,
		Fleet:    directory,
		Nodes:    directory,
		Presets:  gateway.NewPresetRegistry(preset),
		MaxTries: 1,
	}})
	defer server.Close()

	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway do: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway status = %s body=%s", resp.Status, data)
	}
	if resp.Header.Get(gateway.HeaderDecision) == "" || resp.Header.Get(gateway.HeaderInstance) == "" {
		t.Fatalf("missing X-Myc headers: %+v", resp.Header)
	}
	if !strings.Contains(string(data), `"choices"`) {
		t.Fatalf("gateway body lacks OpenAI choices: %s", data)
	}
}

func unloadAll(t *testing.T, ctx context.Context, agent *node.Agent) {
	t.Helper()
	snap, err := agent.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot cleanup: %v", err)
	}
	for _, inst := range snap.Instances {
		if err := agent.Unload(ctx, inst.ID); err != nil {
			t.Fatalf("unload cleanup %s: %v", inst.ID, err)
		}
	}
}
