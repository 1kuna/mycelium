package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPhase2GatewayRoutesOpenAIAndAnthropicWithHeaders(t *testing.T) {
	preset := fixtures.MakePreset()
	upstreamCalls := []string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls = append(upstreamCalls, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("hello")))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance()
	inst.Addr = upstream.URL
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}}
	server := newGatewayTestServer(t, preset, fleet, staticNodeResolver{})

	resp := postJSON(t, server.URL+"/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("openai status = %s", resp.Status)
	}
	if resp.Header.Get(gateway.HeaderDecision) != string(domain.ActionWarmInstance) || resp.Header.Get(gateway.HeaderInstance) != inst.ID {
		t.Fatalf("openai headers = %+v", resp.Header)
	}

	resp = postJSON(t, server.URL+"/v1/messages", `{"model":"qwen2.5-9b-instruct","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`)
	defer resp.Body.Close()
	var anthropic struct {
		Type    string `json:"type"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&anthropic); err != nil {
		t.Fatalf("decode anthropic response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || anthropic.Type != "message" || anthropic.Content[0].Text != "hello" {
		t.Fatalf("anthropic status/body = %s %+v", resp.Status, anthropic)
	}
	if strings.Join(upstreamCalls, ",") != "/v1/chat/completions,/v1/chat/completions" {
		t.Fatalf("upstream calls = %+v", upstreamCalls)
	}
}

func TestPhase2GatewayColdTargetStreamsLoadingState(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\n"))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold"))
	inst.Addr = upstream.URL
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	server := newGatewayTestServer(t, preset, fleet, staticNodeResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadOnlyNode{node: node, inst: inst},
	}})

	resp := postJSON(t, server.URL+"/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`)
	defer resp.Body.Close()
	body := readBody(t, resp)
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type = %s", resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(body, "event: loading") || !strings.Contains(body, `"instance_id":"inst_cold"`) || !strings.Contains(body, "data: done") {
		t.Fatalf("stream body = %q", body)
	}
}

func TestPhase2GatewayFailoverReportsFailedInstance(t *testing.T) {
	preset := fixtures.MakePreset()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("rescued")))
	}))
	defer second.Close()

	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"))
	instA.Addr = first.URL
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"))
	instB.Addr = second.URL
	reporter := &recordingFailureReporter{}
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{instA, instB}}}
	server := newGatewayTestServerWithReporter(t, preset, fleet, staticNodeResolver{}, reporter)

	resp := postJSON(t, server.URL+"/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`)
	defer resp.Body.Close()
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "rescued") {
		t.Fatalf("status/body = %s %s", resp.Status, body)
	}
	if resp.Header.Get(gateway.HeaderInstance) != "inst_b" || resp.Header.Get(gateway.HeaderAttempts) != "2" {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if len(reporter.failed) != 1 || reporter.failed[0] != "inst_a" {
		t.Fatalf("failed = %+v", reporter.failed)
	}
}

func newGatewayTestServer(t *testing.T, preset domain.Preset, fleet gateway.FleetSource, nodes gateway.NodeResolver) *httptest.Server {
	t.Helper()
	return newGatewayTestServerWithReporter(t, preset, fleet, nodes, nil)
}

func newGatewayTestServerWithReporter(t *testing.T, preset domain.Preset, fleet gateway.FleetSource, nodes gateway.NodeResolver, reporter gateway.FailureReporter) *httptest.Server {
	t.Helper()
	placer := scheduler.NewPlacer(
		estimate.NewInMemory(),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		preset,
	)
	return httptest.NewServer(gateway.Server{Router: &gateway.Router{
		Placer:   placer,
		Fleet:    fleet,
		Nodes:    nodes,
		Presets:  gateway.NewPresetRegistry(preset),
		Reporter: reporter,
		MaxTries: 2,
	}})
}

type staticGatewayFleet struct {
	fleet domain.FleetSnapshot
}

func (s staticGatewayFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return s.fleet, nil
}

type staticNodeResolver struct {
	agents map[string]ports.NodeAgent
}

func (s staticNodeResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, ok := s.agents[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

type loadOnlyNode struct {
	node domain.Node
	inst domain.ModelInstance
}

func (n loadOnlyNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n loadOnlyNode) Load(context.Context, domain.Preset) (domain.ModelInstance, error) {
	return n.inst, nil
}

func (n loadOnlyNode) Unload(context.Context, string) error {
	return nil
}

func (n loadOnlyNode) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n loadOnlyNode) BeginRequest(context.Context, string) error {
	return nil
}

func (n loadOnlyNode) EndRequest(context.Context, string) error {
	return nil
}

type recordingFailureReporter struct {
	failed []string
}

func (r *recordingFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) {
	r.failed = append(r.failed, instanceID)
}

func postJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return buf.String()
}

func openAIChatBody(text string) string {
	return `{"id":"chatcmpl-test","model":"qwen2.5-9b-instruct","choices":[{"index":0,"message":{"role":"assistant","content":"` + text + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
}
