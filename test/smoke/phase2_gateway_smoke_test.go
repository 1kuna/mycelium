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
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	"mycelium/internal/node"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/internal/telemetry"
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

	store, err := telemetry.NewSQLiteStore(t.TempDir() + "/telemetry.sqlite")
	if err != nil {
		t.Fatalf("telemetry store: %v", err)
	}
	defer store.Close()

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
		fixtures.WithAliases(filepathSafeModelName(model)),
		fixtures.WithContextLength(2048),
		fixtures.WithWeights(1),
		fixtures.WithKVPerToken(0.01),
	)
	largePreset := preset
	largePreset.ID = preset.ID + "_ctx4096"
	largePreset.ContextLength = 4096
	largePreset.Aliases = nil
	directory := gateway.NodeDirectory{Agents: map[string]ports.NodeAgent{agentNode.ID: agent}}
	placer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock.System{}, preset)
	server := httptest.NewServer(gateway.Server{Router: &gateway.Router{
		Placer:         placer,
		Fleet:          directory,
		Nodes:          directory,
		Presets:        gateway.NewPresetRegistry(preset),
		Telemetry:      store,
		DefaultProject: "smoke",
		MaxTries:       1,
	}})
	defer server.Close()

	streamBody := postGateway(t, ctx, server.URL+"/v1/chat/completions", `{"model":`+quote(model)+`,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1,"stream":true}`)
	if streamBody.status != http.StatusOK {
		t.Fatalf("stream status = %s body=%s", streamBody.statusText, streamBody.body)
	}
	if streamBody.header.Get("Content-Type") != "text/event-stream" || streamBody.header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("stream headers = %+v", streamBody.header)
	}
	if !strings.Contains(streamBody.body, "event: loading") || !strings.Contains(streamBody.body, "event: ready") || !strings.Contains(streamBody.body, "data:") {
		t.Fatalf("stream body lacks loading/ready/upstream chunks: %s", streamBody.body)
	}

	openai := postGateway(t, ctx, server.URL+"/v1/chat/completions", `{"model":`+quote(model)+`,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	if openai.status != http.StatusOK {
		t.Fatalf("openai status = %s body=%s", openai.statusText, openai.body)
	}
	if openai.header.Get(gateway.HeaderDecision) == "" || openai.header.Get(gateway.HeaderInstance) == "" {
		t.Fatalf("missing X-Myc headers: %+v", openai.header)
	}
	if !strings.Contains(openai.body, `"choices"`) {
		t.Fatalf("gateway body lacks OpenAI choices: %s", openai.body)
	}

	anthropic := postGateway(t, ctx, server.URL+"/v1/messages", `{"model":`+quote(model)+`,"max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"Say hi."}]}]}`)
	if anthropic.status != http.StatusOK {
		t.Fatalf("anthropic status = %s body=%s", anthropic.statusText, anthropic.body)
	}
	if !strings.Contains(anthropic.body, `"type":"message"`) || !strings.Contains(anthropic.body, `"content"`) {
		t.Fatalf("gateway body lacks Anthropic message: %s", anthropic.body)
	}

	overflowPlacer := scheduler.NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), clock.System{}, preset, largePreset)
	overflowServer := httptest.NewServer(gateway.Server{Router: &gateway.Router{
		Placer:         overflowPlacer,
		Fleet:          directory,
		Nodes:          directory,
		Presets:        gateway.NewPresetRegistry(preset, largePreset),
		Telemetry:      store,
		DefaultProject: "smoke",
		MaxTries:       2,
	}})
	defer overflowServer.Close()
	overflowPrompt := strings.Repeat("overflow ", preset.ContextLength+300)
	overflow := postGateway(t, ctx, overflowServer.URL+"/v1/completions", `{"model":`+quote(model)+`,"prompt":`+quote(overflowPrompt)+`,"max_tokens":1}`)
	if overflow.status != http.StatusOK || overflow.header.Get(gateway.HeaderAttempts) != "2" {
		t.Fatalf("overflow requeue status=%s attempts=%s body=%s", overflow.statusText, overflow.header.Get(gateway.HeaderAttempts), overflow.body)
	}

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead backend", http.StatusInternalServerError)
	}))
	defer failed.Close()
	failover := &smokeFailureReporter{}
	fakeFailed := domain.ModelInstance{
		ID:             "inst_0_failed",
		PresetID:       preset.ID,
		NodeID:         agentNode.ID,
		AcceleratorSet: []int{0},
		Claim:          domain.Claim{WeightsMB: 1, KVReservedMB: 1},
		State:          domain.InstReady,
		Addr:           failed.URL,
	}
	failoverFleet := smokeFailoverFleet{Directory: directory, Failed: fakeFailed}
	failoverResolver := smokeFailoverResolver{Agent: agent, FakeInstanceID: fakeFailed.ID}
	failoverServer := httptest.NewServer(gateway.Server{Router: &gateway.Router{
		Placer:         placer,
		Fleet:          failoverFleet,
		Nodes:          failoverResolver,
		Presets:        gateway.NewPresetRegistry(preset),
		Telemetry:      store,
		Reporter:       failover,
		DefaultProject: "smoke",
		MaxTries:       2,
	}})
	defer failoverServer.Close()
	rescued := postGateway(t, ctx, failoverServer.URL+"/v1/chat/completions", `{"model":`+quote(model)+`,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	if rescued.status != http.StatusOK || rescued.header.Get(gateway.HeaderAttempts) != "2" || rescued.header.Get(gateway.HeaderInstance) == fakeFailed.ID {
		t.Fatalf("failover response status=%s headers=%+v body=%s", rescued.statusText, rescued.header, rescued.body)
	}
	if len(failover.failed) != 1 || failover.failed[0] != fakeFailed.ID {
		t.Fatalf("failover reporter = %+v", failover.failed)
	}

	metrics, err := store.Metrics(ctx, "smoke")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if len(metrics) < 3 {
		t.Fatalf("expected gateway metrics for smoke requests, got %+v", metrics)
	}
	hasContext := false
	for _, metric := range metrics {
		if metric.InstanceID == "" || metric.NodeID == "" || metric.Project != "smoke" {
			t.Fatalf("incomplete metric = %+v", metric)
		}
		hasContext = hasContext || metric.ContextUsed > 0
	}
	if !hasContext {
		t.Fatalf("gateway metrics never recorded context usage: %+v", metrics)
	}
}

type gatewayResponse struct {
	status     int
	statusText string
	header     http.Header
	body       string
}

func postGateway(t *testing.T, ctx context.Context, url, body string) gatewayResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(body)))
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
	return gatewayResponse{status: resp.StatusCode, statusText: resp.Status, header: resp.Header.Clone(), body: string(data)}
}

type smokeFailoverFleet struct {
	Directory gateway.NodeDirectory
	Failed    domain.ModelInstance
}

func (f smokeFailoverFleet) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	snap, err := f.Directory.Snapshot(ctx)
	if err != nil {
		return domain.FleetSnapshot{}, err
	}
	snap.Instances = append([]domain.ModelInstance{f.Failed}, snap.Instances...)
	return snap, nil
}

type smokeFailoverResolver struct {
	Agent          ports.NodeAgent
	FakeInstanceID string
}

func (r smokeFailoverResolver) NodeAgent(string) (ports.NodeAgent, error) {
	return smokeFailoverAgent{NodeAgent: r.Agent, FakeInstanceID: r.FakeInstanceID}, nil
}

type smokeFailoverAgent struct {
	ports.NodeAgent
	FakeInstanceID string
}

func (a smokeFailoverAgent) BeginRequest(ctx context.Context, instanceID string) error {
	if instanceID == a.FakeInstanceID {
		return ctx.Err()
	}
	return a.NodeAgent.BeginRequest(ctx, instanceID)
}

func (a smokeFailoverAgent) EndRequest(ctx context.Context, instanceID string) error {
	if instanceID == a.FakeInstanceID {
		return ctx.Err()
	}
	return a.NodeAgent.EndRequest(ctx, instanceID)
}

type smokeFailureReporter struct {
	failed []string
}

func (r *smokeFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) error {
	r.failed = append(r.failed, instanceID)
	return nil
}

func filepathSafeModelName(model string) string {
	parts := strings.Split(model, string(os.PathSeparator))
	if len(parts) == 0 {
		return model
	}
	return parts[len(parts)-1]
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
