package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("hello")))
	}))

	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.MemorySampler = fixedMemorySampler{Peak: 512}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "proj-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Header.Get(HeaderDecision) != string(domain.ActionWarmInstance) || resp.Header.Get(HeaderInstance) != inst.ID || !strings.Contains(resp.Header.Get(HeaderTrace), "warm compatible instance") {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if !strings.Contains(string(resp.Body), "hello") {
		t.Fatalf("body = %s", resp.Body)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].Project != "proj-a" || sink.Metrics[0].ContextUsed != 4 || sink.Metrics[0].PresetID != inst.PresetID || sink.Metrics[0].Backend != domain.BackendLlamaCpp || sink.Metrics[0].PeakVRAMMB != 512 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if want := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC); !sink.Metrics[0].At.Equal(want) {
		t.Fatalf("metric time = %s want %s", sink.Metrics[0].At, want)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterPushesMetricToRemoteOwner(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("remote")))
	}))

	inst := fixtures.MakeInstance(fixtures.OnNode("remote-node"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithNodeID("remote-node"))}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	localSink := &mocks.TelemetrySink{}
	peerClient := &mocks.TelemetryPeerClient{}
	router.Telemetry = localSink
	router.SelfNodeID = "local-node"
	router.TelemetryPeers = peerMap{"remote-node": {ID: "peer-remote", Addresses: []string{"127.0.0.1:62000"}, Compute: true}}
	router.TelemetryPeerClient = peerClient
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if _, err := router.Route(context.Background(), req); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(localSink.Metrics) != 0 {
		t.Fatalf("local metrics = %+v", localSink.Metrics)
	}
	metrics := peerClient.PushedMetrics["peer-remote"]
	if len(metrics) != 1 || metrics[0].NodeID != "remote-node" || metrics[0].InstanceID != inst.ID || metrics[0].Backend != domain.BackendLlamaCpp {
		t.Fatalf("pushed metrics = %+v", peerClient.PushedMetrics)
	}
	if got := strings.Join(peerClient.Calls, ","); got != "push-metrics:peer-remote" {
		t.Fatalf("calls = %s", got)
	}
}

func TestRouterFailsLoudlyWhenRemoteOwnerTelemetryCannotRoute(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("remote")))
	}))

	inst := fixtures.MakeInstance(fixtures.OnNode("remote-node"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithNodeID("remote-node"))}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	router.Telemetry = &mocks.TelemetrySink{}
	router.SelfNodeID = "local-node"
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if _, err := router.Route(context.Background(), req); err == nil || !strings.Contains(err.Error(), "telemetry peer resolver") {
		t.Fatalf("err = %v", err)
	}
}

func TestRouterAssignsUniqueGatewayJobIDs(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("hello")))
	}))

	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := router.Route(context.Background(), req); err != nil {
			t.Fatalf("Route %d: %v", i, err)
		}
	}
	if len(sink.Metrics) != 2 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if sink.Metrics[0].JobID == "" || sink.Metrics[0].JobID == sink.Metrics[1].JobID {
		t.Fatalf("job ids are not unique: %+v", sink.Metrics)
	}
}

func TestRouterUsesProjectDefaultModel(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !strings.Contains(string(body), `"model":"`+preset.ID+`"`) {
			t.Fatalf("body = %s", body)
		}
		_, _ = w.Write([]byte(openAIChatBody("defaulted")))
	}))
	inst := fixtures.MakeInstance(fixtures.WithInstancePreset(preset.ID))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	router.Projects = map[string]domain.Project{"proj-a": {ID: "proj-a", DefaultModel: preset.ID}}
	req, err := translate.ParseOpenAIChat([]byte(`{"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "proj-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !strings.Contains(string(resp.Body), "defaulted") {
		t.Fatalf("body = %s", resp.Body)
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
	httpReq.Header.Set(HeaderHandling, string(domain.HandlingPrivate))
	httpReq.Header.Set(HeaderSubmitter, "submitter-a")

	req, err := parseRequest(httpReq)
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if req.Project != "proj-a" || req.Priority != domain.PriorityBackground || req.SpeedPref != domain.SpeedLatency || req.ContextRequest != 4096 || req.Preemption != domain.PreemptHard || req.ConversationKey != "thread-a" || req.Handling != domain.HandlingPrivate || req.Submitter != "submitter-a" {
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

func TestMetricTimingCalculations(t *testing.T) {
	start := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	first := start.Add(100 * time.Millisecond)
	end := first.Add(2 * time.Second)
	if got := durationMS(start, first); got != 100 {
		t.Fatalf("durationMS = %d", got)
	}
	if got := tokensPerSecond(10, first, end); got != 5 {
		t.Fatalf("tokensPerSecond = %f", got)
	}
	if got := tokensPerSecond(0, first, end); got != 0 {
		t.Fatalf("zero tokens/sec = %f", got)
	}
	if got := durationMS(end, start); got != 0 {
		t.Fatalf("negative duration = %d", got)
	}
}

func TestUsageFromBodyAnthropicAndFallback(t *testing.T) {
	prompt, completion := usageFromBody([]byte(`{"usage":{"input_tokens":7,"output_tokens":3}}`))
	if prompt != 7 || completion != 3 {
		t.Fatalf("anthropic usage = %d/%d", prompt, completion)
	}
	prompt, completion = usageFromBody([]byte(`not-json`))
	if prompt != 0 || completion != 0 {
		t.Fatalf("fallback usage = %d/%d", prompt, completion)
	}
}

func TestRouterUtilityFallbacks(t *testing.T) {
	if (&Router{}).clock() == nil {
		t.Fatal("default clock missing")
	}
	if got := joinURL("https://example.test/", "/v1"); got != "https://example.test/v1" {
		t.Fatalf("joinURL = %s", got)
	}
	if got := cloneHeader(nil); got == nil {
		t.Fatal("cloneHeader nil returned nil")
	}
	var w strings.Builder
	result, err := copyAndFlush(noFlushWriter{Builder: &w}, errReader{}, (&Router{}).clock())
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) || result.Body != nil {
		t.Fatalf("copy result=%+v err=%v", result, err)
	}
	table := NewStickyTable(nil, 0)
	if table.clock == nil || table.ttl == 0 {
		t.Fatalf("sticky defaults = %+v", table)
	}
}

func TestPresetRegistryResolvesAliases(t *testing.T) {
	preset := fixtures.MakePreset(
		fixtures.WithPresetID("preset-a"),
		fixtures.WithModelRef("/models/a.gguf"),
		fixtures.WithAliases("qwen-alias"),
	)
	registry := NewPresetRegistry(preset)
	got, err := registry.Resolve("qwen-alias")
	if err != nil || got.ID != preset.ID {
		t.Fatalf("Resolve alias = %+v %v", got, err)
	}
}

func TestPresetRegistrySkipsEmptyModelKeys(t *testing.T) {
	preset := fixtures.MakePreset(
		fixtures.WithPresetID("preset-a"),
		fixtures.WithModelRef(""),
		fixtures.WithAliases(""),
	)
	registry := NewPresetRegistry(preset)
	if got, err := registry.Resolve("preset-a"); err != nil || got.ID != preset.ID {
		t.Fatalf("Resolve id = %+v %v", got, err)
	}
	if _, err := registry.Resolve(""); err == nil {
		t.Fatal("empty model key resolved")
	}
}

func TestRouterRetriesContextOverflowOnLargerPreset(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	small.Capabilities = []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion}
	large.Capabilities = []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion}
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"request (1202 tokens) exceeds the available context size (1024 tokens), try increasing it"}}`, http.StatusBadRequest)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("retried")))
	}))

	node := fixtures.MakeNode()
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID))
	instSmall.Addr = first
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID))
	instLarge.Addr = second
	router := newTestRouter(small, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instSmall, instLarge},
	}, staticResolver{}, large)
	req, err := translate.ParseOpenAICompletion([]byte(`{"model":"preset_small","prompt":"hi","max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAICompletion: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != "inst_large" || resp.Attempts != 2 || !strings.Contains(string(resp.Body), "retried") {
		t.Fatalf("resp=%+v body=%s", resp, resp.Body)
	}
}

func TestRouterRetriesContextOverflowByColdLoadingLargerPreset(t *testing.T) {
	small := fixtures.MakePreset(
		fixtures.WithModelRef("/models/smoke.gguf"),
		fixtures.WithAliases("smoke.gguf"),
		fixtures.WithContextLength(1024),
		fixtures.WithWeights(1),
		fixtures.WithKVPerToken(0.01),
	)
	small.Capabilities = []domain.Capability{domain.CapabilityChat, domain.CapabilityCompletion}
	large := small
	large.ID = small.ID + "_ctx2048"
	large.ContextLength = 2048
	large.Aliases = nil
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"request (1202 tokens) exceeds the available context size (1024 tokens), try increasing it"}}`, http.StatusBadRequest)
	}))
	secondBody := ""
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		secondBody = string(data)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAICompletionBody("retried")))
	}))

	node := fixtures.MakeNode(fixtures.WithVRAM(8192))
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 11)))
	instSmall.Addr = first
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 21)))
	instLarge.Addr = second
	agent := recordingLoadAgent{node: node, inst: instLarge}
	router := newTestRouter(small, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instSmall},
	}, staticResolver{agents: map[string]ports.NodeAgent{node.ID: &agent}}, large)
	req, err := translate.ParseOpenAICompletion([]byte(`{"model":"/models/smoke.gguf","prompt":"hi","max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAICompletion: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != "inst_large" || resp.Attempts != 2 || !strings.Contains(string(resp.Body), "retried") {
		t.Fatalf("resp=%+v body=%s", resp, resp.Body)
	}
	if !strings.Contains(secondBody, `"model":"`+large.ID+`"`) {
		t.Fatalf("retry body did not switch model: %s", secondBody)
	}
	if len(agent.loads) != 1 || agent.loads[0].Preset.ID != large.ID || agent.loads[0].Preset.ContextLength != large.ContextLength {
		t.Fatalf("loads = %+v", agent.loads)
	}
}

func TestRouterClassifiesOverflowBeforeServerErrorFailover(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "prompt exceeds context window", http.StatusInternalServerError)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("retried")))
	}))

	node := fixtures.MakeNode()
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID))
	instSmall.Addr = first
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID))
	instLarge.Addr = second
	reporter := &testFailureReporter{}
	router := newTestRouter(small, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instSmall, instLarge},
	}, staticResolver{}, large)
	router.Reporter = reporter
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"preset_small","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != "inst_large" || resp.Attempts != 2 || len(reporter.failed) != 0 {
		t.Fatalf("resp=%+v failed=%+v", resp, reporter.failed)
	}
}

func TestRouterUsesStickyConversationInstance(t *testing.T) {
	preset := fixtures.MakePreset()
	upstreamA := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("first")))
	}))
	upstreamB := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("sticky")))
	}))
	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instA.Addr = upstreamA
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instB.Addr = upstreamB
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

func TestRouterValidatesStickyInstanceThroughOwnerAdmission(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("sticky")))
	}))
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_sticky"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: agent},
		admissions: map[string]ports.AdmissionController{node.ID: admission},
	}
	store := &gatewayRuntimeStore{}
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}, resolver)
	router.Runtime = &scheduler.Service{
		Placer:      router.Placer,
		Fleet:       router.Fleet,
		Nodes:       resolver,
		Owners:      resolver,
		Coordinator: notClaimedCoordinator{},
		Queue:       scheduler.NewQueue(router.Clock),
		Store:       store,
		Clock:       router.Clock,
		Presets:     map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	router.Sticky = NewStickyTable(router.Clock, time.Minute)
	router.Sticky.Put("thread-a", inst)
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.ConversationKey = "thread-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != inst.ID || !strings.Contains(string(resp.Body), "sticky") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if len(admission.Requests) != 1 || admission.Requests[0].InstanceID != inst.ID {
		t.Fatalf("admission requests = %+v", admission.Requests)
	}
	if !strings.Contains(strings.Join(admission.Calls, ","), "release:"+resp.Lease.ID) || strings.Join(store.deletedLeases, ",") != resp.Lease.ID {
		t.Fatalf("admission calls=%+v deleted=%+v lease=%+v", admission.Calls, store.deletedLeases, resp.Lease)
	}
}

func TestRouterIgnoresStickyWhenOwnerAdmissionRejects(t *testing.T) {
	preset := fixtures.MakePreset()
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("fallback")))
	}))
	sticky := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("stale-sticky")))
	}))
	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instA.Addr = first
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	instB.Addr = sticky
	agent := mocks.NewNodeAgent(node)
	admission := &rejectStickyAdmission{rejectInstanceID: instB.ID, AdmissionController: &mocks.AdmissionController{}}
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: agent},
		admissions: map[string]ports.AdmissionController{node.ID: admission},
	}
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{instA, instB}}
	router := newTestRouter(preset, fleet, resolver)
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   router.Fleet,
		Nodes:   resolver,
		Owners:  resolver,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   &gatewayRuntimeStore{},
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
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
	if resp.Instance.ID != instA.ID || !strings.Contains(string(resp.Body), "fallback") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if len(admission.Requests) < 2 || admission.Requests[0].InstanceID != instB.ID || admission.Requests[1].InstanceID != instA.ID {
		t.Fatalf("admission requests = %+v", admission.Requests)
	}
}

func TestRouterPrivateHandlingRequiresStorageAndLocalPlacement(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(openAIChatBody("private")))
	}))
	remote := fixtures.MakeNode(fixtures.WithNodeID("remote-node"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_remote"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(remote.ID))
	inst.Addr = upstream
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Handling = domain.HandlingPrivate

	noKey := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{remote}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	if _, err := noKey.Route(context.Background(), req); err == nil || !strings.Contains(err.Error(), "private storage") {
		t.Fatalf("missing private storage err = %v", err)
	}

	remoteOnly := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{remote}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	remoteOnly.PrivateStorage = true
	remoteOnly.PrivateLocalNodeID = "local-node"
	if _, err := remoteOnly.Route(context.Background(), req); err == nil || !strings.Contains(err.Error(), "local encrypted placement") {
		t.Fatalf("remote private placement err = %v", err)
	}

	local := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{remote}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	local.PrivateStorage = true
	local.PrivateLocalNodeID = remote.ID
	resp, err := local.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("local private Route: %v", err)
	}
	if resp.Instance.NodeID != remote.ID || !strings.Contains(string(resp.Body), "private") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
}

func TestRouterMergesProjectDefaultsIntoJobIntent(t *testing.T) {
	router := &Router{
		Projects: map[string]domain.Project{
			"proj-a": {
				ID:                  "proj-a",
				Priority:            domain.PriorityBackground,
				SpeedPref:           domain.SpeedLatency,
				ContextCap:          4096,
				ExpectedConcurrency: 3,
				Preemption:          domain.PreemptHard,
			},
		},
		DefaultProject: "proj-a",
	}
	req := translate.IngressRequest{
		Model:     "preset-a",
		Kind:      translate.KindOpenAIChat,
		Submitter: "submitter-a",
		Handling:  domain.HandlingPrivate,
	}

	job := router.jobFromIngress(req, 1)
	if job.Project != "proj-a" || job.Priority != domain.PriorityBackground || job.SpeedPref != domain.SpeedLatency || job.ContextRequest != 4096 || job.ExpectedConcurrency != 3 || job.Preemption != domain.PreemptHard || job.Submitter != "submitter-a" || job.Handling != domain.HandlingPrivate {
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
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold"))
	inst.Addr = upstream
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

func TestRouterStreamColdLoadWritesLoadingReadyAndChunks(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: one\n\n"))
		_, _ = w.Write([]byte("data: two\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_stream"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	body := rec.Body.String()
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/event-stream" || rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("status=%d headers=%+v", rec.Code, rec.Header())
	}
	if !strings.Contains(body, "event: loading") || !strings.Contains(body, "event: ready") || !strings.Contains(body, `"instance_id":"inst_stream"`) || !strings.Contains(body, "data: one") || !strings.Contains(body, "data: two") {
		t.Fatalf("body = %q", body)
	}
	if strings.Index(body, "event: loading") > strings.Index(body, "data: one") {
		t.Fatalf("loading event came after data: %q", body)
	}
}

func TestRouterStreamWarmInstanceCopiesHeadersAndBody(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Upstream", "ok")
		_, _ = w.Write([]byte("data: warm\n\n"))
	}))

	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if rec.Header().Get("X-Upstream") != "ok" || rec.Header().Get(HeaderDecision) != string(domain.ActionWarmInstance) || rec.Header().Get(HeaderInstance) != inst.ID {
		t.Fatalf("headers = %+v", rec.Header())
	}
	if rec.Body.String() != "data: warm\n\n" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestRouterStreamWritesErrorEventAfterStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: errorLoadNode{err: errors.New("load exploded")},
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, "event: loading") || !strings.Contains(body, "event: error") || !strings.Contains(body, "load exploded") {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterStreamWritesUpstreamErrorEventAfterStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad input", http.StatusBadRequest)
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_bad"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, "event: error") || !strings.Contains(body, "bad input") {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterStreamWritesBuildErrorEventAfterStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_translate"))
	inst.Addr = "http://example.invalid"
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	req, err := translate.ParseAnthropicMessages([]byte(`{"model":"qwen2.5-9b-instruct","max_tokens":1,"stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, "event: error") || !strings.Contains(body, "streaming anthropic-to-openai translation") {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterStreamWritesTransportErrorEventAfterStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_bad_url"))
	inst.Addr = "http://%"
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, "event: error") {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterStreamFailoverBeforeResponseStarts(t *testing.T) {
	preset := fixtures.MakePreset()
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: rescued\n\n"))
	}))

	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"))
	instA.Addr = first
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"))
	instB.Addr = second
	reporter := &testFailureReporter{}
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{instA, instB}}, staticResolver{})
	router.Reporter = reporter
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "rescued") || rec.Header().Get(HeaderInstance) != instB.ID || len(reporter.failed) != 1 {
		t.Fatalf("headers=%+v body=%q failed=%+v", rec.Header(), rec.Body.String(), reporter.failed)
	}
}

func TestRouterStreamEarlyErrors(t *testing.T) {
	preset := fixtures.MakePreset()
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	checks := []struct {
		name   string
		router *Router
		req    translate.IngressRequest
		want   string
	}{
		{name: "unconfigured", router: &Router{}, req: req, want: "not configured"},
		{name: "missing model", router: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}), req: translate.IngressRequest{Kind: translate.KindOpenAIChat, Stream: true}, want: "model is required"},
		{name: "unknown model", router: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}), req: translate.IngressRequest{Kind: translate.KindOpenAIChat, Model: "missing", Stream: true}, want: "unknown model"},
		{name: "fleet", router: &Router{Placer: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}).Placer, Fleet: staticFleet{err: errors.New("fleet failed")}, Nodes: staticResolver{}, Presets: NewPresetRegistry(preset)}, req: req, want: "fleet failed"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.router.Stream(context.Background(), check.req, httptest.NewRecorder())
			if err == nil || !strings.Contains(err.Error(), check.want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestRouterUsesRuntimeServiceForColdLoad(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("runtime")))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
	inst.Addr = upstream
	admission := &mocks.AdmissionController{}
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: loadNode{node: node, inst: inst}},
		admissions: map[string]ports.AdmissionController{node.ID: admission},
	}
	fleet := staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	router := newTestRouter(preset, fleet.fleet, resolver)
	router.Fleet = fleet
	router.Nodes = resolver
	store := &gatewayRuntimeStore{}
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   fleet,
		Nodes:   resolver,
		Owners:  resolver,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   store,
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !resp.ColdLoad || resp.Instance.ID != "inst_runtime" || !strings.Contains(string(resp.Body), "runtime") {
		t.Fatalf("resp=%+v body=%s", resp, resp.Body)
	}
	if resp.Lease.ID == "" || strings.Join(store.deletedLeases, ",") != resp.Lease.ID {
		t.Fatalf("lease=%+v deleted=%+v", resp.Lease, store.deletedLeases)
	}
}

func TestRouterReturnsRuntimeReleaseError(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("runtime")))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
	inst.Addr = upstream
	deleteErr := errors.New("delete lease")
	router := newRuntimeRouterForInstance(preset, node, inst, &gatewayRuntimeStore{deleteLeaseErr: deleteErr})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if _, err := router.Route(context.Background(), req); !errors.Is(err, deleteErr) {
		t.Fatalf("Route err = %v", err)
	}
}

func TestRouterStreamReturnsRuntimeReleaseError(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: ok\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
	inst.Addr = upstream
	deleteErr := errors.New("delete lease")
	router := newRuntimeRouterForInstance(preset, node, inst, &gatewayRuntimeStore{deleteLeaseErr: deleteErr})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if err := router.Stream(context.Background(), req, httptest.NewRecorder()); !errors.Is(err, deleteErr) {
		t.Fatalf("Stream err = %v", err)
	}
}

func TestRouterReleaseLeaseReturnsRuntimeError(t *testing.T) {
	preset := fixtures.MakePreset()
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	deleteErr := errors.New("delete lease")
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   router.Fleet,
		Nodes:   router.Nodes,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   &gatewayRuntimeStore{deleteLeaseErr: deleteErr},
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{preset.ID: preset},
	}
	if err := router.releaseLease(context.Background(), domain.Lease{}); err != nil {
		t.Fatalf("empty lease release = %v", err)
	}
	if err := router.releaseLease(context.Background(), domain.Lease{ID: "lease-a"}); !errors.Is(err, deleteErr) {
		t.Fatalf("release err = %v", err)
	}
}

func newRuntimeRouterForInstance(preset domain.Preset, node domain.Node, inst domain.ModelInstance, store *gatewayRuntimeStore) *Router {
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: loadNode{node: node, inst: inst}},
		admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}},
	}
	fleet := staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	router := newTestRouter(preset, fleet.fleet, resolver)
	router.Fleet = fleet
	router.Nodes = resolver
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   fleet,
		Nodes:   resolver,
		Owners:  resolver,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   store,
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
	}
	return router
}

func TestRouterStreamRetriesContextOverflowBeforeResponseStarts(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request exceeds context window", http.StatusBadRequest)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: enlarged\n\n"))
	}))

	node := fixtures.MakeNode()
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID))
	instSmall.Addr = first
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID))
	instLarge.Addr = second
	router := newTestRouter(small, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{instSmall, instLarge},
	}, staticResolver{}, large)
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"preset_small","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "enlarged") || rec.Header().Get(HeaderInstance) != "inst_large" || rec.Header().Get(HeaderAttempts) != "2" {
		t.Fatalf("headers=%+v body=%q", rec.Header(), rec.Body.String())
	}
}

func TestRouterStreamReturnsBeginRequestErrorBeforeResponseStarts(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.OnNode(node.ID))
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data: should-not-run\n\n"))
	}))
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(node)
	agent.BeginErr = errors.New("begin failed")
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: agent,
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	err = router.Stream(context.Background(), req, httptest.NewRecorder())
	if err == nil || !strings.Contains(err.Error(), "begin failed") {
		t.Fatalf("Stream err = %v", err)
	}
}

func TestRouterStreamExhaustsFailoverBeforeResponseStarts(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	router.MaxTries = 1
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	err = router.Stream(context.Background(), req, httptest.NewRecorder())
	if err == nil || !strings.Contains(err.Error(), "failover exhausted") {
		t.Fatalf("Stream err = %v", err)
	}
}

func TestServerUsesStreamingRouterPath(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: server-stream\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_server_stream"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	rec := httptest.NewRecorder()
	body := `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`
	Server{Router: router}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "server-stream") {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestRouterFailoverReportsFailure(t *testing.T) {
	preset := fixtures.MakePreset()
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("rescued")))
	}))

	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"))
	instA.Addr = first
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"))
	instB.Addr = second
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

func TestServerErrorResponses(t *testing.T) {
	rec := httptest.NewRecorder()
	Server{}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("nil router status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(HeaderSpeedPref, "warp")
	Server{Router: &Router{}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "X-Myc-Speed-Pref") {
		t.Fatalf("bad header status/body = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	Server{Router: &Router{}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("route error status = %d", rec.Code)
	}
}

func TestServerWritesRouteResponse(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		_, _ = w.Write([]byte(openAIChatBody("server")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{})
	rec := httptest.NewRecorder()

	Server{Router: router}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != http.StatusOK || rec.Header().Get("X-Upstream") != "yes" || !strings.Contains(rec.Body.String(), "server") {
		t.Fatalf("status=%d headers=%+v body=%s", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestServerWritesStreamResponse(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: server\n\n"))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{})
	rec := httptest.NewRecorder()

	Server{Router: router}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"stream":true}`)))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "data: server") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestParseRequestRoutesAndHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(HeaderProject, "project-a")
	req.Header.Set(HeaderPriority, string(domain.PriorityBackground))
	req.Header.Set(HeaderSpeedPref, string(domain.SpeedAuto))
	req.Header.Set(HeaderPreemption, string(domain.PreemptHard))
	req.Header.Set(HeaderContextCap, "1234")
	req.Header.Set(HeaderConversation, "thread-a")
	req.Header.Set(HeaderHandling, string(domain.HandlingPrivate))
	req.Header.Set(HeaderSubmitter, "submitter-a")
	got, err := parseRequest(req)
	if err != nil {
		t.Fatalf("parse chat: %v", err)
	}
	if got.Project != "project-a" || got.Priority != domain.PriorityBackground || got.SpeedPref != domain.SpeedAuto || got.Preemption != domain.PreemptHard || got.ContextRequest != 1234 || got.ConversationKey != "thread-a" || got.Handling != domain.HandlingPrivate || got.Submitter != "submitter-a" {
		t.Fatalf("parsed headers = %+v", got)
	}

	completion, err := parseRequest(httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"hi"}`)))
	if err != nil || completion.Kind != translate.KindOpenAICompletion {
		t.Fatalf("parse completion = %+v %v", completion, err)
	}
	anthropic, err := parseRequest(httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`)))
	if err != nil || anthropic.Kind != translate.KindAnthropicMessages {
		t.Fatalf("parse messages = %+v %v", anthropic, err)
	}
	notFound, err := parseRequest(httptest.NewRequest(http.MethodPost, "/missing", strings.NewReader(`{}`)))
	if routeErr, ok := err.(*routeError); !ok || routeErr.status != http.StatusNotFound || notFound.Kind != "" {
		t.Fatalf("not found = %+v %v", notFound, err)
	}
}

func TestParseRequestRejectsBadControlHeaders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		value  string
		want   string
	}{
		{name: "priority", header: HeaderPriority, value: "urgent", want: "Priority"},
		{name: "preemption", header: HeaderPreemption, value: "break-glass", want: "Preemption"},
		{name: "handling", header: HeaderHandling, value: "secret", want: "Handling"},
		{name: "context", header: HeaderContextCap, value: "0", want: "Context-Cap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
			req.Header.Set(tc.header, tc.value)
			_, err := parseRequest(req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
		})
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
	if _, err := directory.NodeAgent("missing"); err == nil {
		t.Fatal("missing node agent succeeded")
	}
	if _, err := directory.AdmissionController(node.ID); err == nil {
		t.Fatal("plain node agent exposed admission")
	}
	if _, err := directory.LeaseInspector(node.ID); err == nil {
		t.Fatal("plain node agent exposed lease inspection")
	}

	admitting := admittingAgent{NodeAgent: mocks.NewNodeAgent(node), AdmissionController: &mocks.AdmissionController{}}
	directory = NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: admitting}}
	if _, err := directory.AdmissionController(node.ID); err != nil {
		t.Fatalf("AdmissionController: %v", err)
	}
	if _, err := directory.LeaseInspector(node.ID); err != nil {
		t.Fatalf("LeaseInspector: %v", err)
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
		Client:   testUpstreams.client(),
		Clock:    fakeClock,
		MaxTries: 2,
	}
}

var testUpstreams = &directUpstreams{handlers: map[string]http.Handler{}}

type directUpstreams struct {
	handlers map[string]http.Handler
}

type peerMap map[string]domain.Peer

func (m peerMap) PeerForNode(nodeID string) (domain.Peer, bool) {
	peer, ok := m[nodeID]
	return peer, ok
}

type fixedMemorySampler struct {
	Peak int
	Err  error
}

func (s fixedMemorySampler) PeakMemoryMB(context.Context, string, string) (int, error) {
	return s.Peak, s.Err
}

func directUpstream(handler http.Handler) string {
	return testUpstreams.url(handler)
}

func (d *directUpstreams) url(handler http.Handler) string {
	host := fmt.Sprintf("upstream-%d.mycelium.test", len(d.handlers)+1)
	d.handlers[host] = handler
	return "http://" + host
}

func (d *directUpstreams) client() *http.Client {
	return &http.Client{Transport: d}
}

func (d *directUpstreams) RoundTrip(req *http.Request) (*http.Response, error) {
	handler := d.handlers[req.URL.Host]
	if handler == nil {
		return nil, fmt.Errorf("unregistered test upstream %q", req.URL.Host)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

type staticFleet struct {
	fleet domain.FleetSnapshot
	err   error
}

func (s staticFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	if s.err != nil {
		return domain.FleetSnapshot{}, s.err
	}
	return s.fleet, nil
}

type staticResolver struct {
	agents     map[string]ports.NodeAgent
	admissions map[string]ports.AdmissionController
}

func (s staticResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, ok := s.agents[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

func (s staticResolver) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	admission, ok := s.admissions[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	return admission, nil
}

func (s staticResolver) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	admission, ok := s.admissions[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	inspector, ok := admission.(ports.LeaseInspector)
	if !ok {
		return nil, domain.ErrUnsupported
	}
	return inspector, nil
}

type loadNode struct {
	node domain.Node
	inst domain.ModelInstance
}

type recordingLoadAgent struct {
	node  domain.Node
	inst  domain.ModelInstance
	loads []domain.LoadRequest
}

type admittingAgent struct {
	*mocks.NodeAgent
	*mocks.AdmissionController
}

func (n loadNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n loadNode) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
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

func (n *recordingLoadAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n *recordingLoadAgent) Load(_ context.Context, req domain.LoadRequest) (domain.ModelInstance, error) {
	n.loads = append(n.loads, req)
	return n.inst, nil
}

func (n *recordingLoadAgent) Unload(context.Context, string) error {
	return nil
}

func (n *recordingLoadAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n *recordingLoadAgent) BeginRequest(context.Context, string) error {
	return nil
}

func (n *recordingLoadAgent) EndRequest(context.Context, string) error {
	return nil
}

type errorLoadNode struct {
	err error
}

func (n errorLoadNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{}, nil
}

func (n errorLoadNode) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
	return domain.ModelInstance{}, n.err
}

func (n errorLoadNode) Unload(context.Context, string) error {
	return nil
}

func (n errorLoadNode) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n errorLoadNode) BeginRequest(context.Context, string) error {
	return nil
}

func (n errorLoadNode) EndRequest(context.Context, string) error {
	return nil
}

type testFailureReporter struct {
	failed []string
}

func (r *testFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) error {
	r.failed = append(r.failed, instanceID)
	return nil
}

type noFlushWriter struct {
	*strings.Builder
}

func (w noFlushWriter) Header() http.Header {
	return http.Header{}
}

func (w noFlushWriter) WriteHeader(int) {}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

type gatewayRuntimeStore struct {
	deletedLeases  []string
	deleteLeaseErr error
}

func (s *gatewayRuntimeStore) SaveJob(context.Context, domain.Job) error {
	return nil
}

func (s *gatewayRuntimeStore) SaveLease(context.Context, domain.Lease) error {
	return nil
}

func (s *gatewayRuntimeStore) ListLeases(context.Context) ([]domain.Lease, error) {
	return nil, nil
}

func (s *gatewayRuntimeStore) DeleteLease(_ context.Context, id string) error {
	if s.deleteLeaseErr != nil {
		return s.deleteLeaseErr
	}
	s.deletedLeases = append(s.deletedLeases, id)
	return nil
}

func (s *gatewayRuntimeStore) SaveInstance(context.Context, domain.ModelInstance) error {
	return nil
}

func (s *gatewayRuntimeStore) DeleteInstance(context.Context, string) error {
	return nil
}

type notClaimedCoordinator struct{}

func (notClaimedCoordinator) ClaimJob(context.Context, string) error {
	return nil
}

func (notClaimedCoordinator) Plan(context.Context, string) (domain.PlacementDecision, error) {
	return domain.PlacementDecision{}, domain.ErrUnsupported
}

func (notClaimedCoordinator) Commit(context.Context, domain.PlacementDecision) (domain.Lease, error) {
	return domain.Lease{}, domain.ErrUnsupported
}

func (notClaimedCoordinator) Release(_ context.Context, jobID string) error {
	return fmt.Errorf("job %q is not claimed by this coordinator", jobID)
}

type rejectStickyAdmission struct {
	rejectInstanceID string
	*mocks.AdmissionController
}

func (a *rejectStickyAdmission) Offer(ctx context.Context, req domain.AdmissionRequest) (domain.LeaseOffer, error) {
	if req.InstanceID == a.rejectInstanceID {
		a.Calls = append(a.Calls, "offer:"+req.Job.ID)
		a.Requests = append(a.Requests, req)
		return domain.LeaseOffer{}, domain.ErrNoFit
	}
	return a.AdmissionController.Offer(ctx, req)
}

func openAIChatBody(text string) string {
	return `{"id":"chatcmpl-test","model":"qwen2.5-9b-instruct","choices":[{"index":0,"message":{"role":"assistant","content":"` + text + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
}

func openAICompletionBody(text string) string {
	return `{"id":"cmpl-test","model":"qwen2.5-9b-instruct","choices":[{"index":0,"text":"` + text + `","finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
}
