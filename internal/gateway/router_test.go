package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestRouterPassesThroughOpenAIAndWritesHeaders(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("hello")))
	}))
	defer upstream.Close()

	inst := fixtures.MakeInstance()
	inst.Addr = upstream.URL
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "proj-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Header.Get(HeaderDecision) != string(domain.ActionWarmInstance) || resp.Header.Get(HeaderInstance) != inst.ID {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if !strings.Contains(string(resp.Body), "hello") {
		t.Fatalf("body = %s", resp.Body)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].Project != "proj-a" || sink.Metrics[0].ContextUsed != 4 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if want := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC); !sink.Metrics[0].At.Equal(want) {
		t.Fatalf("metric time = %s want %s", sink.Metrics[0].At, want)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestParseRequestReadsMyceliumIntentHeaders(t *testing.T) {
	raw := `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}]}`
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(raw))
	httpReq.Header.Set(HeaderProject, "proj-a")
	httpReq.Header.Set(HeaderPriority, string(domain.PriorityBackground))
	httpReq.Header.Set(HeaderSpeedPref, string(domain.SpeedLatency))
	httpReq.Header.Set(HeaderContextCap, "4096")
	httpReq.Header.Set(HeaderPreemption, string(domain.PreemptHard))
	httpReq.Header.Set(HeaderConversation, "thread-a")

	req, err := parseRequest(httpReq)
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if req.Project != "proj-a" || req.Priority != domain.PriorityBackground || req.SpeedPref != domain.SpeedLatency || req.ContextRequest != 4096 || req.Preemption != domain.PreemptHard || req.ConversationKey != "thread-a" {
		t.Fatalf("req = %+v", req)
	}

	httpReq.Header.Set(HeaderContextCap, "nope")
	if _, err := parseRequest(httpReq); err == nil {
		t.Fatal("expected invalid context cap")
	}
	httpReq.Header.Set(HeaderContextCap, "4096")
	httpReq.Header.Set(HeaderPriority, "urgent")
	if _, err := parseRequest(httpReq); err == nil {
		t.Fatal("expected invalid priority")
	}
}

func TestRouterRetriesContextOverflowOnLargerPreset(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request exceeds context window", http.StatusBadRequest)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("retried")))
	}))
	defer second.Close()

	node := fixtures.MakeNode()
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID))
	instSmall.Addr = first.URL
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID))
	instLarge.Addr = second.URL
	router := newTestRouter(small, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instSmall, instLarge},
	}, staticResolver{}, large)
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"preset_small","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != "inst_large" || resp.Attempts != 2 || !strings.Contains(string(resp.Body), "retried") {
		t.Fatalf("resp=%+v body=%s", resp, resp.Body)
	}
}

func TestRouterUsesStickyConversationInstance(t *testing.T) {
	preset := fixtures.MakePreset()
	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("first")))
	}))
	defer upstreamA.Close()
	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("sticky")))
	}))
	defer upstreamB.Close()
	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instA.Addr = upstreamA.URL
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instB.Addr = upstreamB.URL
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instA, instB},
	}, staticResolver{agents: map[string]ports.NodeAgent{node.ID: agent}})
	router.Sticky = NewStickyTable(router.Clock, time.Minute)
	router.Sticky.Put("thread-a", instB)
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.ConversationKey = "thread-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != instB.ID || !strings.Contains(string(resp.Body), "sticky") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
}

func TestRouterMergesProjectDefaultsIntoJobIntent(t *testing.T) {
	router := &Router{
		Projects: map[string]domain.Project{
			"proj-a": {
				ID:         "proj-a",
				Priority:   domain.PriorityBackground,
				SpeedPref:  domain.SpeedLatency,
				ContextCap: 4096,
				Preemption: domain.PreemptHard,
			},
		},
		DefaultProject: "proj-a",
	}
	req := translate.IngressRequest{
		Model: "preset-a",
		Kind:  translate.KindOpenAIChat,
	}

	job := router.jobFromIngress(req, 1)
	if job.Project != "proj-a" || job.Priority != domain.PriorityBackground || job.SpeedPref != domain.SpeedLatency || job.ContextRequest != 4096 || job.Preemption != domain.PreemptHard {
		t.Fatalf("job = %+v", job)
	}

	req.Project = "proj-a"
	req.Priority = domain.PriorityInteractive
	req.ContextRequest = 8192
	job = router.jobFromIngress(req, 2)
	if job.Priority != domain.PriorityInteractive || job.ContextRequest != 8192 {
		t.Fatalf("override job = %+v", job)
	}
}

func TestRouterColdStreamPrependsLoadingState(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\n"))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold"))
	inst.Addr = upstream.URL
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	body := string(resp.Body)
	if !resp.ColdLoad || !strings.Contains(body, "event: loading") || !strings.Contains(body, "data: done") {
		t.Fatalf("resp = cold:%v body:%q", resp.ColdLoad, body)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("headers = %+v", resp.Header)
	}
}

func TestRouterFailoverReportsFailure(t *testing.T) {
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
	reporter := &testFailureReporter{}
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{instA, instB}}, staticResolver{})
	router.Reporter = reporter
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != "inst_b" || resp.Attempts != 2 || len(reporter.failed) != 1 || reporter.failed[0] != "inst_a" {
		t.Fatalf("resp=%+v failed=%+v", resp, reporter.failed)
	}
}

func TestServerRejectsUnknownRoute(t *testing.T) {
	rec := httptest.NewRecorder()
	Server{Router: &Router{}}.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNodeDirectoryCombinesSnapshots(t *testing.T) {
	node := fixtures.MakeNode()
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{fixtures.MakeInstance()}
	directory := NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}}
	fleet, err := directory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(fleet.Nodes) != 1 || len(fleet.Instances) != 1 {
		t.Fatalf("fleet = %+v", fleet)
	}
	if _, err := directory.NodeAgent(node.ID); err != nil {
		t.Fatalf("NodeAgent: %v", err)
	}
}

func newTestRouter(preset domain.Preset, fleet domain.FleetSnapshot, nodes NodeResolver, extra ...domain.Preset) *Router {
	presets := append([]domain.Preset{preset}, extra...)
	fakeClock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	return &Router{
		Placer: scheduler.NewPlacer(
			estimate.NewInMemory(),
			lease.NewAllocator(),
			fakeClock,
			presets...,
		),
		Fleet:    staticFleet{fleet: fleet},
		Nodes:    nodes,
		Presets:  NewPresetRegistry(presets...),
		Clock:    fakeClock,
		MaxTries: 2,
	}
}

type staticFleet struct {
	fleet domain.FleetSnapshot
}

func (s staticFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	return s.fleet, nil
}

type staticResolver struct {
	agents map[string]ports.NodeAgent
}

func (s staticResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, ok := s.agents[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

type loadNode struct {
	node domain.Node
	inst domain.ModelInstance
}

func (n loadNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n loadNode) Load(context.Context, domain.Preset) (domain.ModelInstance, error) {
	return n.inst, nil
}

func (n loadNode) Unload(context.Context, string) error {
	return nil
}

func (n loadNode) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n loadNode) BeginRequest(context.Context, string) error {
	return nil
}

func (n loadNode) EndRequest(context.Context, string) error {
	return nil
}

type testFailureReporter struct {
	failed []string
}

func (r *testFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) {
	r.failed = append(r.failed, instanceID)
}

func openAIChatBody(text string) string {
	return `{"id":"chatcmpl-test","model":"qwen2.5-9b-instruct","choices":[{"index":0,"message":{"role":"assistant","content":"` + text + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
}
