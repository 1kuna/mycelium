package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/internal/reconcile"
	"mycelium/internal/scheduler"
	"mycelium/internal/serving"
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
	router.SelfNodeID = inst.NodeID
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
	if resp.Header.Get(HeaderDecision) != string(domain.ActionWarmInstance) || resp.Header.Get(HeaderInstance) != inst.ID || resp.Header.Get(HeaderJob) == "" || !strings.Contains(resp.Header.Get(HeaderTrace), "warm compatible instance") {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if resp.Header.Get(HeaderProject) != "proj-a" {
		t.Fatalf("project header = %q headers=%+v", resp.Header.Get(HeaderProject), resp.Header)
	}
	if resp.Header.Get(HeaderColdLoad) != "false" || resp.Header.Get(HeaderQueueWaitMS) != "0" || resp.Header.Get(HeaderStructuredOutput) != "none" || resp.Header.Get(HeaderGuidedDecodingRequested) != "false" || resp.Header.Get(HeaderUpstreamWallMS) != "0" {
		t.Fatalf("diagnostic headers = %+v", resp.Header)
	}
	if resp.Header.Get(HeaderToolsRequested) != "false" || resp.Header.Get(HeaderToolChoice) != "none" || resp.Header.Get(HeaderVLLMToolParser) != "false" || resp.Header.Get(HeaderVLLMAutoToolChoice) != "false" {
		t.Fatalf("tool diagnostics = %+v", resp.Header)
	}
	if !strings.Contains(string(resp.Body), "hello") {
		t.Fatalf("body = %s", resp.Body)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].Project != "proj-a" || sink.Metrics[0].ContextUsed != 4 || sink.Metrics[0].PresetID != inst.PresetID || sink.Metrics[0].Backend != domain.BackendLlamaCpp || sink.Metrics[0].PeakVRAMMB != 512 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if len(sink.SamplesOut) != 3 {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	if sink.SamplesOut[0].Phase != domain.TelemetryPhasePlaced || sink.SamplesOut[1].Phase != domain.TelemetryPhaseUpstreamStart || sink.SamplesOut[2].Phase != domain.TelemetryPhaseComplete {
		t.Fatalf("sample phases = %+v", sink.SamplesOut)
	}
	if sink.SamplesOut[2].SessionID == "" || sink.SamplesOut[2].ContextUsed != 4 || sink.SamplesOut[2].TokensIn != 3 || sink.SamplesOut[2].TokensOut != 1 || sink.SamplesOut[2].PeakVRAMMB != 512 {
		t.Fatalf("complete sample = %+v", sink.SamplesOut[2])
	}
	if want := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC); !sink.Metrics[0].At.Equal(want) {
		t.Fatalf("metric time = %s want %s", sink.Metrics[0].At, want)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterSharedModelUsesSelectedPresetForTelemetry(t *testing.T) {
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-large"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 49152
		p.NodeID = "owner-large"
	})
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-small"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 16384
		p.NodeID = "owner-small"
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("small owner")))
	}))
	inst := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-small"),
		fixtures.WithInstancePreset(small.ID),
		fixtures.OnNode("owner-small"),
	)
	inst.Addr = upstream
	largeNode := fixtures.MakeNode(fixtures.WithNodeID("owner-large"), fixtures.WithVRAM(65536), fixtures.WithMaxUtil(1))
	smallNode := fixtures.MakeNode(fixtures.WithNodeID("owner-small"), fixtures.WithVRAM(32768), fixtures.WithMaxUtil(1))
	router := newTestRouter(large, domain.FleetSnapshot{
		Nodes:     []domain.Node{largeNode, smallNode},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{"owner-small": mocks.NewNodeAgent(smallNode)}}, small)
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"vision","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Decision.Preset.ID != small.ID || resp.Header.Get(HeaderNode) != "owner-small" || resp.Header.Get(HeaderBackend) != string(domain.BackendVLLM) {
		t.Fatalf("response decision=%+v headers=%+v", resp.Decision, resp.Header)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].PresetID != small.ID || sink.Metrics[0].NodeID != "owner-small" || sink.Metrics[0].Backend != domain.BackendVLLM {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
}

func TestPresetRegistryNextLargerContext(t *testing.T) {
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "small"
		p.ModelRef = "model-a"
		p.Backend = domain.BackendLlamaCpp
		p.ContextLength = 1024
	})
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "large"
		p.ModelRef = "model-a"
		p.Backend = domain.BackendLlamaCpp
		p.ContextLength = 4096
	})
	otherBackend := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "other"
		p.ModelRef = "model-a"
		p.Backend = domain.BackendVLLM
		p.ContextLength = 8192
	})
	registry := NewPresetRegistry(small, large, otherBackend)
	got, ok := registry.NextLargerContext(small)
	if !ok || got.ID != large.ID {
		t.Fatalf("NextLargerContext = %+v, %t", got, ok)
	}
	if _, ok := registry.NextLargerContext(large); ok {
		t.Fatal("largest context unexpectedly had a next larger context")
	}
}

func TestPresetRegistryResolveAllGroupsAliasesButPinsExactPresetIDs(t *testing.T) {
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-small"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
	})
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-large"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
	})
	registry := NewPresetRegistry(small, large)

	group := registry.ResolveAll("vision")
	if len(group) != 2 || group[0].ID != small.ID || group[1].ID != large.ID {
		t.Fatalf("alias group = %+v", group)
	}
	pinned := registry.ResolveAll("vision-large")
	if len(pinned) != 1 || pinned[0].ID != large.ID {
		t.Fatalf("exact preset = %+v", pinned)
	}
	if got := registry.ResolveAll("missing"); len(got) != 0 {
		t.Fatalf("missing group = %+v", got)
	}
}

func TestNoInstanceAvailableRetryAfterRoundsUp(t *testing.T) {
	err := noInstanceAvailableError{JobID: "job-a", RetryAfter: 1500 * time.Millisecond}
	if got := err.retryAfterSeconds(); got != 2 {
		t.Fatalf("retryAfterSeconds = %d", got)
	}
	if got := (noInstanceAvailableError{JobID: "job-a"}).retryAfterSeconds(); got != 1 {
		t.Fatalf("default retryAfterSeconds = %d", got)
	}
}

func TestRouterTerminalWrappersRespectMissingRuntime(t *testing.T) {
	router := &Router{}
	job := domain.Job{ID: "job-a"}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID}
	if err := router.completeJob(context.Background(), job, lease); err != nil {
		t.Fatalf("complete without runtime: %v", err)
	}
	if err := router.failJob(context.Background(), job, lease, errors.New("failed")); err != nil {
		t.Fatalf("fail without runtime: %v", err)
	}
	router.Runtime = &scheduler.Service{}
	if err := router.completeJob(context.Background(), job, lease); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("complete with unconfigured runtime err = %v", err)
	}
	if err := router.failJob(context.Background(), job, lease, errors.New("failed")); err == nil || !strings.Contains(err.Error(), "not fully configured") {
		t.Fatalf("fail with unconfigured runtime err = %v", err)
	}
}

func TestRouterExecutePlacedProviderRunsWithoutFinishingJob(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		if !strings.Contains(string(body), `"messages"`) {
			t.Fatalf("upstream body = %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("queued")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	job := domain.Job{ID: "job-a", Model: req.Model, Project: "project-a", Status: domain.JobRunning}
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionLoadedNew, NodeID: inst.NodeID, Preset: preset}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: inst.NodeID, InstanceID: inst.ID}

	resp, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, true, 7)
	if err != nil {
		t.Fatalf("ExecutePlacedProvider: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "queued") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if resp.Header.Get(HeaderDecision) != string(domain.ActionLoadedNew) || resp.Header.Get(HeaderJob) != job.ID || resp.Header.Get(HeaderColdLoad) != "true" || resp.Header.Get(HeaderLoadWallMS) != "7" {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].JobID != job.ID || sink.Metrics[0].Project != "project-a" {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if len(sink.SamplesOut) != 4 || sink.SamplesOut[0].Phase != domain.TelemetryPhasePlaced || sink.SamplesOut[1].Phase != domain.TelemetryPhaseLoadReady || sink.SamplesOut[2].Phase != domain.TelemetryPhaseUpstreamStart || sink.SamplesOut[3].Phase != domain.TelemetryPhaseComplete {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	req.Stream = true
	if _, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, false, 0); err == nil || !strings.Contains(err.Error(), "streaming") {
		t.Fatalf("streaming err = %v", err)
	}
}

func TestRouterExecutePlacedProviderUsesSelectedPresetForSharedModel(t *testing.T) {
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-large"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 49152
		p.NodeID = "owner-large"
	})
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-small"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 16384
		p.NodeID = "owner-small"
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("queued small")))
	}))
	inst := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-small"),
		fixtures.WithInstancePreset(small.ID),
		fixtures.OnNode("owner-small"),
	)
	inst.Addr = upstream
	node := fixtures.MakeNode(fixtures.WithNodeID("owner-small"))
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(large, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}}, small)
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"vision","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	job := domain.Job{ID: "job-a", Model: req.Model, Project: "project-a", Status: domain.JobRunning}
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: small}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: inst.NodeID, InstanceID: inst.ID}

	resp, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, false, 0)
	if err != nil {
		t.Fatalf("ExecutePlacedProvider: %v", err)
	}
	if resp.Decision.Preset.ID != small.ID || !strings.Contains(string(resp.Body), "queued small") {
		t.Fatalf("resp = %+v body=%s headers=%+v", resp, resp.Body, resp.Header)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].PresetID != small.ID || sink.Metrics[0].NodeID != "owner-small" {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
}

func TestRouterExecutePlacedProviderMarksUpstreamFailure(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream failed"}`))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	job := domain.Job{ID: "job-a", Model: req.Model, Project: "project-a", Status: domain.JobRunning}
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: inst.NodeID, InstanceID: inst.ID}

	resp, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, false, 0)
	if err == nil || !strings.Contains(err.Error(), "upstream returned 500") {
		t.Fatalf("ExecutePlacedProvider err = %v", err)
	}
	if resp.Status != http.StatusInternalServerError || !strings.Contains(string(resp.Body), "upstream failed") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if len(sink.SamplesOut) != 3 || sink.SamplesOut[2].Phase != domain.TelemetryPhaseError || !strings.Contains(sink.SamplesOut[2].Error, "upstream returned 500") {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterExecutePlacedProviderFailsWhenOwnerRejectsRequest(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance()
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	agent.BeginErr = errors.New("owner busy")
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	job := domain.Job{ID: "job-a", Model: req.Model, Project: "project-a", Status: domain.JobRunning}
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: inst.NodeID, InstanceID: inst.ID}

	if _, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, false, 0); err == nil || !strings.Contains(err.Error(), "owner busy") {
		t.Fatalf("ExecutePlacedProvider err = %v", err)
	}
	if len(sink.SamplesOut) != 2 || sink.SamplesOut[1].Phase != domain.TelemetryPhaseError || !strings.Contains(sink.SamplesOut[1].Error, "owner busy") {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterExecutePlacedProviderFailsOnTranslatedResponseLoss(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{`))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	req, err := translate.ParseAnthropicMessages([]byte(`{"model":"qwen2.5-9b-instruct","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	job := domain.Job{ID: "job-a", Model: req.Model, Project: "project-a", Status: domain.JobRunning}
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset}
	lease := domain.Lease{ID: "lease-a", JobID: job.ID, NodeID: inst.NodeID, InstanceID: inst.ID}

	if _, err := router.ExecutePlacedProvider(context.Background(), req, job, decision, inst, lease, 1, false, 0); err == nil {
		t.Fatal("ExecutePlacedProvider accepted malformed translated response")
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterRouteUsesPresentedWorkloadLease(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("leased route ok")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	node := fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID))
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	store := &gatewayRuntimeStore{}
	lease := domain.Lease{ID: "lease-workload", JobID: "workload-a", NodeID: inst.NodeID, InstanceID: inst.ID}
	job := domain.Job{
		ID:             "workload-a",
		Model:          preset.ModelRef,
		Project:        "project-a",
		ContextRequest: 2048,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-a", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	store.jobs = []domain.Job{job}
	store.leases = []domain.Lease{lease}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.ContextRequest = 1024
	req.WorkloadID = job.ID
	req.LeaseID = lease.ID

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route with workload lease: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "leased route ok") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if resp.Header.Get(HeaderJob) != job.ID || resp.Header.Get(HeaderNode) != inst.NodeID {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if !store.sawJobStatus(domain.JobDone) || len(store.deletedLeases) != 1 || store.deletedLeases[0] != lease.ID {
		t.Fatalf("jobs=%+v deleted=%+v", store.jobs, store.deletedLeases)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterRouteRejectsInvalidPresentedWorkloadLease(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance()
	lease := domain.Lease{ID: "lease-workload", JobID: "workload-a", NodeID: inst.NodeID, InstanceID: inst.ID}
	baseJob := domain.Job{
		ID:             "workload-a",
		Model:          preset.ModelRef,
		Project:        "project-a",
		ContextRequest: 2048,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-a", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	for _, tc := range []struct {
		name   string
		job    domain.Job
		leases []domain.Lease
		req    func(translate.IngressRequest) translate.IngressRequest
		status int
		want   string
	}{
		{
			name:   "project mismatch",
			job:    baseJob,
			leases: []domain.Lease{lease},
			req: func(req translate.IngressRequest) translate.IngressRequest {
				req.Project = "project-b"
				return req
			},
			status: http.StatusForbidden,
			want:   "different project",
		},
		{
			name:   "context mismatch",
			job:    baseJob,
			leases: []domain.Lease{lease},
			req: func(req translate.IngressRequest) translate.IngressRequest {
				req.Project = "project-a"
				req.ContextRequest = 4096
				return req
			},
			status: http.StatusConflict,
			want:   "context envelope",
		},
		{
			name:   "released lease",
			job:    baseJob,
			leases: nil,
			req: func(req translate.IngressRequest) translate.IngressRequest {
				req.Project = "project-a"
				return req
			},
			status: http.StatusConflict,
			want:   "not active",
		},
		{
			name: "expired lease",
			job:  baseJob,
			leases: []domain.Lease{{
				ID:         lease.ID,
				JobID:      lease.JobID,
				NodeID:     lease.NodeID,
				InstanceID: lease.InstanceID,
				ExpiresAt:  routerTestNow().Add(-time.Second),
			}},
			req: func(req translate.IngressRequest) translate.IngressRequest {
				req.Project = "project-a"
				return req
			},
			status: http.StatusGone,
			want:   "expired",
		},
		{
			name: "terminal job",
			job: func() domain.Job {
				job := baseJob
				job.Result = cloneJobResult(baseJob.Result)
				job.Result.Terminal = true
				return job
			}(),
			leases: []domain.Lease{lease},
			req: func(req translate.IngressRequest) translate.IngressRequest {
				req.Project = "project-a"
				return req
			},
			status: http.StatusConflict,
			want:   "already terminal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
			store := &gatewayRuntimeStore{jobs: []domain.Job{tc.job}, leases: tc.leases}
			router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}
			req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
			if err != nil {
				t.Fatalf("ParseOpenAIChat: %v", err)
			}
			req.WorkloadID = tc.job.ID
			req.LeaseID = lease.ID
			req.ContextRequest = 1024
			req = tc.req(req)
			_, err = router.Route(context.Background(), req)
			var statusErr interface{ StatusCode() int }
			if err == nil || !errors.As(err, &statusErr) || statusErr.StatusCode() != tc.status || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v status=%v", err, statusErr)
			}
		})
	}
}

func TestRouterUsesModelServiceHeldLeaseForHeaderlessRequest(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset-service"
		p.ModelRef = "model-service"
		p.Aliases = []string{"served-model"}
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("served route ok")))
	}))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-service"), fixtures.WithInstancePreset(preset.ID))
	inst.Addr = upstream
	lease := domain.Lease{ID: "lease-service", JobID: "svc-job", NodeID: inst.NodeID, InstanceID: inst.ID}
	service := domain.ModelService{
		ID:             serving.ServiceID("project-a", "served-model", 4096),
		Model:          "served-model",
		Project:        "project-a",
		ContextRequest: 4096,
		MinReplicas:    1,
		MaxReplicas:    1,
		Status:         domain.ModelServiceActive,
	}
	job := domain.Job{
		ID:             "svc-job",
		ParentID:       service.ID,
		Model:          service.Model,
		PresetID:       preset.ID,
		Project:        service.Project,
		ContextRequest: service.ContextRequest,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "svc-job", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	agent := mocks.NewNodeAgent(fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID)))
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID))}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	store := &gatewayRuntimeStore{services: []domain.ModelService{service}, jobs: []domain.Job{job}, leases: []domain.Lease{lease}}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"served-model","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.ContextRequest = 1024
	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "served route ok") {
		t.Fatalf("resp=%+v body=%s", resp, resp.Body)
	}
	if resp.Header.Get("X-Myc-Model-Service") != service.ID || resp.Header.Get(HeaderLeaseHold) != "true" || resp.Header.Get(HeaderJob) != job.ID {
		t.Fatalf("headers = %+v", resp.Header)
	}
	if len(store.deletedLeases) != 0 || !store.sawJobStatus(domain.JobRunning) {
		t.Fatalf("managed service request should keep held lease running: jobs=%+v deleted=%+v", store.jobs, store.deletedLeases)
	}
}

func TestRouterModelServiceWithoutUsableCapacityReturnsRetryableDemand(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset-service"
		p.ModelRef = "model-service"
		p.Aliases = []string{"served-model"}
	})
	service := domain.ModelService{
		ID:             serving.ServiceID("project-a", "served-model", 4096),
		Model:          "served-model",
		Project:        "project-a",
		ContextRequest: 4096,
		MinReplicas:    1,
		MaxReplicas:    1,
		Status:         domain.ModelServiceActive,
	}
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	store := &gatewayRuntimeStore{services: []domain.ModelService{service}}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"served-model","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.ContextRequest = 1024
	_, err = router.Route(context.Background(), req)
	var noInstance noInstanceAvailableError
	if err == nil || !errors.As(err, &noInstance) || !strings.Contains(err.Error(), "model service has no gateway-usable held capacity") {
		t.Fatalf("err = %v", err)
	}
	if noInstance.retryAfterSeconds() != 1 || noInstance.Decision.Action != domain.ActionQueued {
		t.Fatalf("noInstance = %+v", noInstance)
	}
	if len(store.services) != 1 || store.services[0].DemandCount != 1 || store.services[0].LastDemandJobID == "" || store.services[0].LastReason != string(domain.ActionQueued) {
		t.Fatalf("service demand was not bumped: %+v", store.services)
	}
}

func TestRouterModelServiceStaleHeldLeaseDoesNotFallbackToNormalAdmission(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset-service"
		p.ModelRef = "model-service"
		p.Aliases = []string{"served-model"}
	})
	service := domain.ModelService{
		ID:             serving.ServiceID("project-a", "served-model", 4096),
		Model:          "served-model",
		Project:        "project-a",
		ContextRequest: 4096,
		MinReplicas:    1,
		MaxReplicas:    1,
		Status:         domain.ModelServiceActive,
	}
	job := domain.Job{
		ID:             "svc-job",
		ParentID:       service.ID,
		Model:          service.Model,
		PresetID:       preset.ID,
		Project:        service.Project,
		ContextRequest: service.ContextRequest,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			LeaseID:  "missing-lease",
			NodeID:   "node-a",
			PresetID: preset.ID,
		},
	}
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	store := &gatewayRuntimeStore{services: []domain.ModelService{service}, jobs: []domain.Job{job}}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"served-model","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.ContextRequest = 1024
	_, err = router.Route(context.Background(), req)
	var noInstance noInstanceAvailableError
	if err == nil || !errors.As(err, &noInstance) || !strings.Contains(err.Error(), "lease \"missing-lease\" is not active") {
		t.Fatalf("err = %v", err)
	}
	if len(store.services) != 1 || store.services[0].DemandCount != 1 || store.services[0].LastDemandJobID == "" {
		t.Fatalf("service demand was not bumped: %+v", store.services)
	}
}

func TestRouterResolveWorkloadLeaseValidationEdges(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance()
	lease := domain.Lease{ID: "lease-workload", JobID: "workload-a", NodeID: inst.NodeID, InstanceID: inst.ID}
	baseJob := domain.Job{
		ID:             "workload-a",
		Model:          preset.ModelRef,
		Project:        "project-a",
		ContextRequest: 2048,
		Status:         domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-a", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	baseReq, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	baseReq.Project = "project-a"
	baseReq.WorkloadID = baseJob.ID
	baseReq.LeaseID = lease.ID
	baseReq.ContextRequest = 1024

	for _, tc := range []struct {
		name     string
		mutate   func(*Router, *gatewayRuntimeStore, *domain.Job, *translate.IngressRequest)
		want     string
		status   int
		wantInst bool
	}{
		{
			name: "missing lease id",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, _ *domain.Job, req *translate.IngressRequest) {
				req.LeaseID = ""
			},
			want:   "both X-Myc-Workload",
			status: http.StatusBadRequest,
		},
		{
			name: "streaming unsupported",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, _ *domain.Job, req *translate.IngressRequest) {
				req.Stream = true
			},
			want:   "does not support streaming",
			status: http.StatusBadRequest,
		},
		{
			name: "missing runtime",
			mutate: func(router *Router, _ *gatewayRuntimeStore, _ *domain.Job, _ *translate.IngressRequest) {
				router.Runtime = nil
			},
			want:   "requires a configured runtime store",
			status: http.StatusConflict,
		},
		{
			name: "missing job",
			mutate: func(_ *Router, store *gatewayRuntimeStore, job *domain.Job, _ *translate.IngressRequest) {
				job.ID = ""
				store.jobs = nil
			},
			want:   "was not found",
			status: http.StatusNotFound,
		},
		{
			name: "job not running",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, job *domain.Job, _ *translate.IngressRequest) {
				job.Status = domain.JobQueued
			},
			want:   "not running",
			status: http.StatusConflict,
		},
		{
			name: "no result",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, job *domain.Job, _ *translate.IngressRequest) {
				job.Result = nil
			},
			want:   "no active lease",
			status: http.StatusConflict,
		},
		{
			name: "model mismatch",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, _ *domain.Job, req *translate.IngressRequest) {
				req.Model = "other-model"
			},
			want:   "not reserved for model",
			status: http.StatusConflict,
		},
		{
			name: "store lease read failure",
			mutate: func(_ *Router, store *gatewayRuntimeStore, _ *domain.Job, _ *translate.IngressRequest) {
				store.listLeasesErr = errors.New("list failed")
			},
			want: "list failed",
		},
		{
			name: "lease belongs to different job",
			mutate: func(_ *Router, store *gatewayRuntimeStore, _ *domain.Job, _ *translate.IngressRequest) {
				store.leases = []domain.Lease{{ID: lease.ID, JobID: "other-job"}}
			},
			want:   "different workload",
			status: http.StatusConflict,
		},
		{
			name: "instance not visible",
			mutate: func(router *Router, _ *gatewayRuntimeStore, _ *domain.Job, _ *translate.IngressRequest) {
				router.Fleet = staticFleet{fleet: domain.FleetSnapshot{}}
			},
			want:   "not visible",
			status: http.StatusConflict,
		},
		{
			name: "fleet lookup fills instance",
			mutate: func(_ *Router, _ *gatewayRuntimeStore, _ *domain.Job, _ *translate.IngressRequest) {
			},
			wantInst: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
			store := &gatewayRuntimeStore{leases: []domain.Lease{lease}}
			router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}
			job := baseJob
			if baseJob.Result != nil {
				job.Result = cloneJobResult(baseJob.Result)
			}
			req := baseReq
			if tc.mutate != nil {
				tc.mutate(router, store, &job, &req)
			}
			if job.ID != "" {
				store.jobs = []domain.Job{job}
			}
			_, _, gotInst, _, err := router.resolveWorkloadLease(context.Background(), req)
			if tc.wantInst {
				if err != nil || gotInst.ID != inst.ID {
					t.Fatalf("inst=%+v err=%v", gotInst, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if tc.status != 0 {
				var statusErr interface{ StatusCode() int }
				if !errors.As(err, &statusErr) || statusErr.StatusCode() != tc.status {
					t.Fatalf("status err = %v", err)
				}
			}
		})
	}
}

func TestRouterWorkloadLeaseHelperBranches(t *testing.T) {
	preset := fixtures.MakePreset()
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	job := domain.Job{
		ID:       "workload-a",
		Model:    "alias-model",
		PresetID: preset.ID,
		Result: &domain.JobResult{
			PresetID: preset.ID,
		},
	}
	if !router.requestCanUseWorkloadPreset("alias-model", job) {
		t.Fatal("job model should match request model")
	}
	if !router.requestCanUseWorkloadPreset(preset.ID, job) {
		t.Fatal("preset id should match request model")
	}
	job.Model = "other"
	preset.Aliases = []string{"shared-alias"}
	router.Presets = NewPresetRegistry(preset)
	if !router.requestCanUseWorkloadPreset("shared-alias", job) {
		t.Fatal("shared alias should match reserved preset id")
	}
	if router.requestCanUseWorkloadPreset("", job) {
		t.Fatal("empty model matched")
	}
	if got := nonEmptyAction("", domain.ActionWarmInstance); got != domain.ActionWarmInstance {
		t.Fatalf("nonEmptyAction = %q", got)
	}
	if got := nonEmptyAction(""); got != "" {
		t.Fatalf("empty nonEmptyAction = %q", got)
	}
	if got := firstString("", "value"); got != "value" {
		t.Fatalf("firstString = %q", got)
	}
	if got := firstString(""); got != "" {
		t.Fatalf("empty firstString = %q", got)
	}
	if got := router.projectForRequest(translate.IngressRequest{Project: "explicit"}); got != "explicit" {
		t.Fatalf("explicit project = %q", got)
	}
	router.DefaultProject = "default"
	if got := router.projectForRequest(translate.IngressRequest{}); got != "default" {
		t.Fatalf("default project = %q", got)
	}
}

func TestWriteGatewayRouteErrorPreservesLeasedStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	writeGatewayRouteError(rec, leasedRouteError{status: http.StatusGone, msg: "lease expired"})
	if rec.Code != http.StatusGone || !strings.Contains(rec.Body.String(), "lease expired") {
		t.Fatalf("status/body = %d %s", rec.Code, rec.Body.String())
	}
}

func cloneJobResult(result *domain.JobResult) *domain.JobResult {
	if result == nil {
		return nil
	}
	clone := *result
	return &clone
}

func routerTestNow() time.Time {
	return time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
}

func TestRouterPassesThroughVLLMStructuredOutputControls(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "b70-qwen35-9b-vision-vllm"
		p.ModelRef = "/models/Qwen3.5-9B"
		p.Aliases = []string{"qwen3.5-9b"}
		p.Backend = domain.BackendVLLM
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatalf("decode upstream body: %v body=%s", err, body)
		}
		if string(fields["response_format"]) != `{"type":"json_schema","json_schema":{"name":"tiny","schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}}}` {
			t.Fatalf("response_format = %s", fields["response_format"])
		}
		if string(fields["structured_outputs"]) != `{"json":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}}` {
			t.Fatalf("structured_outputs = %s", fields["structured_outputs"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("structured")))
	}))

	inst := fixtures.MakeInstance(func(i *domain.ModelInstance) {
		i.PresetID = preset.ID
		i.Addr = upstream
	})
	agent := mocks.NewNodeAgent(fixtures.MakeNode())
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen3.5-9b","messages":[{"role":"user","content":"json"}],"max_tokens":8,"response_format":{"type":"json_schema","json_schema":{"name":"tiny","schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}}},"structured_outputs":{"json":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}}}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Profile.ID != "vllm" || resp.Header.Get(HeaderBackend) != string(domain.BackendVLLM) || !strings.Contains(string(resp.Body), "structured") {
		t.Fatalf("resp profile=%+v headers=%+v body=%s", resp.Profile, resp.Header, resp.Body)
	}
	if resp.Header.Get(HeaderStructuredOutput) != "json_schema+structured_outputs" || resp.Header.Get(HeaderGuidedDecodingRequested) != "true" {
		t.Fatalf("structured diagnostics = %+v", resp.Header)
	}
	if resp.Header.Get(HeaderColdLoad) != "false" || resp.Header.Get(HeaderQueueWaitMS) != "0" || resp.Header.Get(HeaderUpstreamWallMS) != "0" {
		t.Fatalf("timing diagnostics = %+v", resp.Header)
	}
}

func TestRouterReportsToolRequestDiagnostics(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "b70-qwen27b-vllm"
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--served-model-name", "b70-qwen27b", "--enable-auto-tool-choice", "--tool-call-parser", "hermes"}
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream body: %v", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatalf("decode upstream body: %v body=%s", err, body)
		}
		if string(fields["tool_choice"]) != `"auto"` || !strings.Contains(string(fields["tools"]), `"echo"`) {
			t.Fatalf("tool request not passed through: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"echo","arguments":"{\"value\":\"ok\"}"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	inst := fixtures.MakeInstance(func(i *domain.ModelInstance) {
		i.PresetID = preset.ID
		i.Addr = upstream
	})
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"use echo"}],"tools":[{"type":"function","function":{"name":"echo","parameters":{"type":"object"}}}],"tool_choice":"auto","max_tokens":8}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Header.Get(HeaderToolsRequested) != "true" || resp.Header.Get(HeaderToolChoice) != "auto" || resp.Header.Get(HeaderVLLMToolParser) != "true" || resp.Header.Get(HeaderVLLMAutoToolChoice) != "true" {
		t.Fatalf("tool diagnostics = %+v", resp.Header)
	}
	if !strings.Contains(string(resp.Body), `"tool_calls"`) {
		t.Fatalf("body = %s", resp.Body)
	}
}

func TestNormalizeOpenAIReasoningBoundaryMovesLengthFinishedQwenContent(t *testing.T) {
	body := []byte(`{
		"id": "chatcmpl-test",
		"model": "qwen3.5-9b",
		"choices": [{
			"index": 0,
			"message": {"role": "assistant", "content": "The user wants me to reason before answering."},
			"finish_reason": "length"
		}],
		"usage": {"prompt_tokens": 3, "completion_tokens": 9, "total_tokens": 12}
	}`)
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "b70-qwen35-9b-vision-vllm"
		p.ModelRef = "/models/Qwen3.5-9B"
		p.Aliases = []string{"qwen3.5-9b"}
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--reasoning-parser", "qwen3"}
	})
	req := translate.IngressRequest{Kind: translate.KindOpenAIChat, Model: "qwen3.5-9b"}

	normalized, err := normalizeOpenAIReasoningBoundary(req, preset, body)
	if err != nil {
		t.Fatalf("normalizeOpenAIReasoningBoundary: %v", err)
	}
	var resp struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				Reasoning        string `json:"reasoning"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(normalized, &resp); err != nil {
		t.Fatalf("unmarshal normalized: %v", err)
	}
	msg := resp.Choices[0].Message
	if msg.Content != "" {
		t.Fatalf("content = %q, want empty final content", msg.Content)
	}
	if msg.Reasoning == "" || msg.ReasoningContent == "" || msg.Reasoning != msg.ReasoningContent {
		t.Fatalf("reasoning not preserved separately: %+v", msg)
	}
}

func TestNormalizeOpenAIReasoningBoundaryLeavesCompletedReasoningAlone(t *testing.T) {
	body := []byte(`{
		"id": "chatcmpl-test",
		"model": "qwen3.5-9b",
		"choices": [{
			"index": 0,
			"message": {"role": "assistant", "content": "{\"ok\":true}", "reasoning_content": "hidden"},
			"finish_reason": "stop"
		}]
	}`)
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "b70-qwen35-9b-vision-vllm"
		p.ModelRef = "/models/Qwen3.5-9B"
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--reasoning-parser=qwen3"}
	})
	req := translate.IngressRequest{Kind: translate.KindOpenAIChat, Model: "qwen3.5-9b"}

	normalized, err := normalizeOpenAIReasoningBoundary(req, preset, body)
	if err != nil {
		t.Fatalf("normalizeOpenAIReasoningBoundary: %v", err)
	}
	if string(normalized) != string(body) {
		t.Fatalf("completed response changed: %s", normalized)
	}
}

func TestNormalizeOpenAIReasoningBoundaryRequiresConfiguredReasoningParser(t *testing.T) {
	body := []byte(`{
		"id": "chatcmpl-test",
		"model": "qwen3.5-9b",
		"choices": [{
			"index": 0,
			"message": {"role": "assistant", "content": "reasoning text"},
			"finish_reason": "length"
		}]
	}`)
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "b70-qwen35-9b-vision-vllm"
		p.ModelRef = "/models/Qwen3.5-9B"
		p.Aliases = []string{"qwen3.5-9b"}
		p.Backend = domain.BackendVLLM
	})
	req := translate.IngressRequest{Kind: translate.KindOpenAIChat, Model: "qwen3.5-9b"}

	normalized, err := normalizeOpenAIReasoningBoundary(req, preset, body)
	if err != nil {
		t.Fatalf("normalizeOpenAIReasoningBoundary: %v", err)
	}
	if string(normalized) != string(body) {
		t.Fatalf("response changed without configured reasoning parser: %s", normalized)
	}
}

func TestRouterFailsBeforeUpstreamWhenOwnerNodeCannotBeResolved(t *testing.T) {
	preset := fixtures.MakePreset()
	called := false
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(openAIChatBody("should-not-run")))
	}))
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.OnNode(node.ID))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if _, err := router.Route(context.Background(), req); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("Route err = %v", err)
	}
	if called {
		t.Fatal("upstream was called after unresolved owner node")
	}
}

func TestRouterUsesPresetProviderProfile(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("claude-local"))
	preset.ProviderProfile = "anthropic"
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Anthropic-Version"); got != "2023-06-01" {
			t.Fatalf("Anthropic-Version = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !strings.Contains(string(body), `"max_tokens":1`) || !strings.Contains(string(body), `"type":"text"`) {
			t.Fatalf("body = %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg-test","type":"message","role":"assistant","content":[{"type":"text","text":"pong"}],"usage":{"input_tokens":3,"output_tokens":1}}`))
	}))
	inst := fixtures.MakeInstance(fixtures.WithInstancePreset(preset.ID))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	req, err := translate.ParseAnthropicMessages([]byte(`{"model":"claude-local","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Header.Get(HeaderBackend) != "anthropic" || !strings.Contains(string(resp.Body), "pong") {
		t.Fatalf("headers=%+v body=%s", resp.Header, resp.Body)
	}
}

func TestRouterRejectsUnknownPresetProviderProfile(t *testing.T) {
	preset := fixtures.MakePreset()
	preset.ProviderProfile = "missing"
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	_, err = router.Route(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), `unknown provider profile "missing"`) {
		t.Fatalf("Route err = %v", err)
	}
}

func TestRouterRejectsModelGroupWithIncompatibleProviderProfiles(t *testing.T) {
	openai := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-openai"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
	})
	openvino := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-openvino"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendOpenVINO
	})
	router := newTestRouter(openai, domain.FleetSnapshot{}, staticResolver{}, openvino)
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"vision","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	_, err = router.Route(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), `maps to incompatible provider profiles`) {
		t.Fatalf("Route err = %v", err)
	}
}

func TestRouterPreflightsUpstreamBeforeColdLoad(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("claude-local"))
	preset.ProviderProfile = "anthropic"
	node := fixtures.MakeNode()
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: agent,
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"claude-local","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	_, err = router.Route(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "max_tokens") {
		t.Fatalf("Route err = %v", err)
	}
	if len(agent.Loaded) != 0 || len(agent.Calls) != 0 {
		t.Fatalf("agent should not load or begin before preflight failure: calls=%+v loaded=%+v", agent.Calls, agent.Loaded)
	}
}

func TestRouterStreamPreflightsTranslationBeforeStartingSSE(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("claude-local"))
	preset.ProviderProfile = "anthropic"
	node := fixtures.MakeNode()
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: agent,
	}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"claude-local","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	err = router.Stream(context.Background(), req, rec)
	if err == nil || !strings.Contains(err.Error(), "streaming openai-to-anthropic translation") {
		t.Fatalf("Stream err = %v", err)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("stream started before preflight failure: %q", rec.Body.String())
	}
	if len(agent.Loaded) != 0 || len(agent.Calls) != 0 {
		t.Fatalf("agent should not load or begin before stream preflight failure: calls=%+v loaded=%+v", agent.Calls, agent.Loaded)
	}
}

func TestRouterResolveWarmInstanceUsesOwnerNode(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("spark-preset"))
	preset.Backend = domain.BackendVLLM
	colliding := fixtures.MakeInstance(fixtures.WithInstanceID("inst_1"), fixtures.WithInstancePreset("b70-preset"), fixtures.OnNode("b70-bench"))
	target := fixtures.MakeInstance(fixtures.WithInstanceID("inst_1"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode("spark-bench"))
	router := &Router{}

	got, loaded, err := router.resolveInstance(context.Background(), domain.Job{}, domain.PlacementDecision{
		InstanceID: target.ID,
		NodeID:     target.NodeID,
		Action:     domain.ActionWarmInstance,
	}, preset, domain.FleetSnapshot{Instances: []domain.ModelInstance{colliding, target}})
	if err != nil {
		t.Fatalf("resolveInstance: %v", err)
	}
	if loaded {
		t.Fatal("warm resolve should not load")
	}
	if got.NodeID != target.NodeID || got.PresetID != preset.ID {
		t.Fatalf("resolved wrong warm instance: %+v", got)
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
	samples := peerClient.PushedSamples["peer-remote"]
	if len(samples) != 3 || samples[0].Phase != domain.TelemetryPhasePlaced || samples[2].Phase != domain.TelemetryPhaseComplete || samples[2].NodeID != "remote-node" {
		t.Fatalf("pushed samples = %+v", peerClient.PushedSamples)
	}
	if got := strings.Join(peerClient.Calls, ","); got != "push-samples:peer-remote,push-samples:peer-remote,push-metrics:peer-remote,push-samples:peer-remote" {
		t.Fatalf("calls = %s", got)
	}
}

func TestRouterRecordsMetricLocallyWhenEmbeddedOwnerHasNoSelfID(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("local")))
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

	if _, err := router.Route(context.Background(), req); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].NodeID != inst.NodeID || sink.Metrics[0].InstanceID != inst.ID {
		t.Fatalf("metrics = %+v", sink.Metrics)
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

func TestRouterReturnsRouteHeadersForNonRetryableUpstreamStatus(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--tool-call-parser", "hermes"}
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `"auto" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set`, http.StatusBadRequest)
	}))
	inst := fixtures.MakeInstance()
	inst.PresetID = preset.ID
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"echo","parameters":{"type":"object"}}}],"tool_choice":"auto","max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route err = %v", err)
	}
	if resp.Status != http.StatusBadRequest || !strings.Contains(string(resp.Body), "tool choice requires") {
		t.Fatalf("resp status=%d body=%s", resp.Status, resp.Body)
	}
	if resp.Header.Get(HeaderNode) == "" || resp.Header.Get(HeaderBackend) != "vllm" || resp.Header.Get(HeaderToolsRequested) != "true" || resp.Header.Get(HeaderToolChoice) != "auto" || resp.Header.Get(HeaderVLLMToolParser) != "true" || resp.Header.Get(HeaderVLLMAutoToolChoice) != "false" {
		t.Fatalf("diagnostic headers = %+v", resp.Header)
	}
	if len(sink.Metrics) != 0 {
		t.Fatalf("metrics should not be recorded on failed run: %+v", sink.Metrics)
	}
	if len(sink.SamplesOut) != 3 || sink.SamplesOut[2].Phase != domain.TelemetryPhaseError || !strings.Contains(sink.SamplesOut[2].Error, "tool choice requires") {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
}

func TestRouterRecordsSessionErrorForTransportFailover(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance()
	inst.Addr = "http://missing-upstream.mycelium.test"
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	router.MaxTries = 1
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	_, err = router.Route(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "failover exhausted") {
		t.Fatalf("Route err = %v", err)
	}
	if len(sink.SamplesOut) != 3 || sink.SamplesOut[2].Phase != domain.TelemetryPhaseError || !strings.Contains(sink.SamplesOut[2].Error, "missing-upstream") {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
}

func TestRouterRouteFinalizesJobAfterClientCanceledUpstreamError(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithInstanceID("inst_cancel"))
	inst.Addr = "http://cancel-upstream.mycelium.test"
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	store := &gatewayRuntimeStore{}
	router := newRuntimeRouterForWarmInstance(preset, node, inst, agent, &mocks.AdmissionController{}, store)
	router.MaxTries = 1
	sink := &contextCheckingTelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = node.ID
	ctx, cancel := context.WithCancel(context.Background())
	router.Client = &http.Client{Transport: gatewayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		cancel()
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	_, err = router.Route(ctx, req)
	if err == nil || !strings.Contains(err.Error(), "failover exhausted") {
		t.Fatalf("Route err = %v", err)
	}
	if !store.sawJobStatus(domain.JobFailed) || store.sawJobStatus(domain.JobDone) || len(store.deletedLeases) != 1 {
		t.Fatalf("jobs=%+v deletedLeases=%+v", store.jobs, store.deletedLeases)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_cancel,end:inst_cancel" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	if len(sink.SamplesOut) == 0 || sink.SamplesOut[len(sink.SamplesOut)-1].Phase != domain.TelemetryPhaseError {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
}

func TestRouterFailsJobWhenTranslatedResponseIsLossy(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("claude-local"))
	preset.ProviderProfile = "anthropic"
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg-test","type":"message","role":"assistant","content":[{"type":"tool_use","id":"tool-a","name":"lookup","input":{}}],"usage":{"input_tokens":3,"output_tokens":1}}`))
	}))
	inst := fixtures.MakeInstance(fixtures.WithInstancePreset(preset.ID))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"claude-local","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	_, err = router.Route(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), `content block "tool_use" cannot be translated`) {
		t.Fatalf("Route err = %v", err)
	}
}

func TestRouterWarnsOnMetricWriteErrorsAfterUpstreamSuccess(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("metric-fails")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	metricErr := errors.New("metric write failed")
	sink := &mocks.TelemetrySink{Err: metricErr}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route returned telemetry error after upstream success: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "metric-fails") {
		t.Fatalf("response = %d %s", resp.Status, string(resp.Body))
	}
	if got := resp.Header.Get(HeaderTelemetryWarning); got != "completion_telemetry_failed" {
		t.Fatalf("telemetry warning = %q", got)
	}
	if len(sink.SamplesOut) != 2 || len(sink.Metrics) != 0 {
		t.Fatalf("telemetry after metric failure: samples=%+v metrics=%+v", sink.SamplesOut, sink.Metrics)
	}
	if !errors.Is(metricErr, sink.Err) {
		t.Fatalf("test setup lost metric error")
	}
}

func TestRouterWarnsOnCompleteSampleErrorsAfterMetricSuccess(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("sample-fails")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: mocks.NewNodeAgent(fixtures.MakeNode())}})
	sampleErr := errors.New("complete sample failed")
	sink := &phaseFailTelemetry{failPhase: domain.TelemetryPhaseComplete, err: sampleErr}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route returned completion sample error after upstream success: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "sample-fails") {
		t.Fatalf("response = %d %s", resp.Status, string(resp.Body))
	}
	if got := resp.Header.Get(HeaderTelemetryWarning); got != "completion_telemetry_failed" {
		t.Fatalf("telemetry warning = %q", got)
	}
	if len(sink.metrics) != 1 || len(sink.samples) != 3 {
		t.Fatalf("telemetry after complete sample failure: samples=%+v metrics=%+v", sink.samples, sink.metrics)
	}
	if !errors.Is(sampleErr, sink.err) {
		t.Fatalf("test setup lost sample error")
	}
}

func TestRouterRouteReleasesRuntimeLeaseWhenSetupTelemetryFails(t *testing.T) {
	for _, failPhase := range []domain.TelemetryPhase{
		domain.TelemetryPhasePlaced,
		domain.TelemetryPhaseLoadReady,
		domain.TelemetryPhaseUpstreamStart,
	} {
		t.Run(string(failPhase), func(t *testing.T) {
			preset := fixtures.MakePreset()
			upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("upstream should not be called after setup telemetry failure")
			}))
			node := fixtures.MakeNode()
			inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
			inst.Addr = upstream
			store := &gatewayRuntimeStore{}
			router := newRuntimeRouterForInstance(preset, node, inst, store)
			sampleErr := fmt.Errorf("%s sample failed", failPhase)
			router.Telemetry = &phaseFailTelemetry{failPhase: failPhase, err: sampleErr}
			router.SelfNodeID = node.ID
			req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
			if err != nil {
				t.Fatalf("ParseOpenAIChat: %v", err)
			}

			if _, err := router.Route(context.Background(), req); !errors.Is(err, sampleErr) {
				t.Fatalf("Route err = %v", err)
			}
			if len(store.deletedLeases) != 1 {
				t.Fatalf("deleted leases = %+v", store.deletedLeases)
			}
		})
	}
}

func TestRouterStreamWritesSetupTelemetryErrorAfterColdStart(t *testing.T) {
	for _, failPhase := range []domain.TelemetryPhase{
		domain.TelemetryPhasePlaced,
		domain.TelemetryPhaseLoadReady,
		domain.TelemetryPhaseUpstreamStart,
	} {
		t.Run(string(failPhase), func(t *testing.T) {
			preset := fixtures.MakePreset()
			upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("upstream should not be called after setup telemetry failure")
			}))
			node := fixtures.MakeNode()
			inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
			inst.Addr = upstream
			store := &gatewayRuntimeStore{}
			router := newRuntimeRouterForInstance(preset, node, inst, store)
			sampleErr := fmt.Errorf("stream %s sample failed", failPhase)
			router.Telemetry = &phaseFailTelemetry{failPhase: failPhase, err: sampleErr}
			router.SelfNodeID = node.ID
			req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
			if err != nil {
				t.Fatalf("ParseOpenAIChat: %v", err)
			}
			rec := httptest.NewRecorder()

			if err := router.Stream(context.Background(), req, rec); err != nil {
				t.Fatalf("Stream returned error after start: %v", err)
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: loading") || !strings.Contains(body, "event: error") || !strings.Contains(body, sampleErr.Error()) {
				t.Fatalf("body = %q", body)
			}
			if len(store.deletedLeases) != 1 {
				t.Fatalf("deleted leases = %+v", store.deletedLeases)
			}
		})
	}
}

func TestStreamEventHelpersSerializeJSON(t *testing.T) {
	decision := domain.PlacementDecision{Action: domain.ActionLoadedNew}
	inst := domain.ModelInstance{ID: "inst-a", NodeID: "node-a"}
	if body := string(loadingEvent(decision, inst)); !strings.Contains(body, "event: loading") || !strings.Contains(body, `"instance_id":"inst-a"`) {
		t.Fatalf("loading event = %q", body)
	}
	if body := string(readyEvent(decision, inst)); !strings.Contains(body, "event: ready") || !strings.Contains(body, `"node_id":"node-a"`) {
		t.Fatalf("ready event = %q", body)
	}
	rec := httptest.NewRecorder()
	writeStreamError(rec, errors.New("bad\n\"value\""))
	if body := rec.Body.String(); !strings.Contains(body, "event: error") || !strings.Contains(body, `bad\n\"value\"`) {
		t.Fatalf("error event = %q", body)
	}
	got := quoteJSON("bad\n\"value\"")
	var decoded string
	if err := json.Unmarshal([]byte(got), &decoded); err != nil || decoded != "bad\n\"value\"" {
		t.Fatalf("quoteJSON = %s decoded=%q err=%v", got, decoded, err)
	}
}

func TestRouterSessionTelemetryRoutingFailsLoudly(t *testing.T) {
	ctx := context.Background()
	sample := domain.SessionMetric{
		SessionID: "session-a",
		Sequence:  1,
		JobID:     "job-a",
		Phase:     domain.TelemetryPhaseComplete,
		NodeID:    "node-a",
		Project:   "project-a",
		At:        time.Unix(1, 0).UTC(),
	}
	for _, tt := range []struct {
		name   string
		router *Router
		sample domain.SessionMetric
		want   string
	}{
		{
			name:   "missing owner node",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node"},
			sample: sampleWith(sample, func(s *domain.SessionMetric) { s.NodeID = "" }),
			want:   "owner node",
		},
		{
			name:   "local owner missing sink",
			router: &Router{SelfNodeID: "node-a", TelemetryPeerClient: &mocks.TelemetryPeerClient{}},
			sample: sample,
			want:   "local owner telemetry sink",
		},
		{
			name:   "remote owner missing resolver",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node"},
			sample: sample,
			want:   "telemetry peer resolver",
		},
		{
			name:   "remote owner missing client",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node", TelemetryPeers: peerMap{"node-a": {ID: "peer-a"}}},
			sample: sample,
			want:   "telemetry client",
		},
		{
			name:   "remote owner unknown peer",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node", TelemetryPeers: peerMap{}, TelemetryPeerClient: &mocks.TelemetryPeerClient{}},
			sample: sample,
			want:   "is not known",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.router.recordSample(ctx, tt.sample); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("recordSample err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRouterRunMetricRoutingFailsLoudly(t *testing.T) {
	ctx := context.Background()
	metric := domain.RunMetric{
		JobID:      "job-a",
		InstanceID: "inst-a",
		NodeID:     "node-a",
		Project:    "project-a",
		At:         time.Unix(1, 0).UTC(),
	}
	for _, tt := range []struct {
		name   string
		router *Router
		metric domain.RunMetric
		want   string
	}{
		{
			name:   "local owner missing sink",
			router: &Router{SelfNodeID: "node-a", TelemetryPeerClient: &mocks.TelemetryPeerClient{}},
			metric: metric,
			want:   "local owner telemetry sink",
		},
		{
			name:   "remote owner missing resolver",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node"},
			metric: metric,
			want:   "telemetry peer resolver",
		},
		{
			name:   "remote owner missing client",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node", TelemetryPeers: peerMap{"node-a": {ID: "peer-a"}}},
			metric: metric,
			want:   "telemetry client",
		},
		{
			name:   "remote owner unknown peer",
			router: &Router{Telemetry: &mocks.TelemetrySink{}, SelfNodeID: "local-node", TelemetryPeers: peerMap{}, TelemetryPeerClient: &mocks.TelemetryPeerClient{}},
			metric: metric,
			want:   "is not known",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.router.recordMetricValue(ctx, tt.metric); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("recordMetricValue err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRouterRecordMetricFailsOnMemorySamplerError(t *testing.T) {
	router := &Router{
		Telemetry:     &mocks.TelemetrySink{},
		SelfNodeID:    "node-a",
		Clock:         mocks.NewFakeClock(time.Unix(1, 0).UTC()),
		MemorySampler: fixedMemorySampler{Err: errors.New("memory unavailable")},
	}
	_, err := router.recordMetric(context.Background(), domain.Job{ID: "job-a"}, fixtures.MakePreset(), fixtures.MakeInstance(fixtures.OnNode("node-a")), []byte(openAIChatBody("hi")), metricTiming{
		Start:     time.Unix(1, 0).UTC(),
		FirstByte: time.Unix(1, 0).UTC(),
		End:       time.Unix(2, 0).UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "memory unavailable") {
		t.Fatalf("recordMetric err = %v", err)
	}
}

func TestCopyAndFlushPropagatesChunkCallbackError(t *testing.T) {
	var out strings.Builder
	callbackErr := errors.New("sample write failed")
	result, err := copyAndFlush(noFlushWriter{Builder: &out}, strings.NewReader("chunk"), (&Router{}).clock(), false, func(copyResult) error {
		return callbackErr
	})
	if !errors.Is(err, callbackErr) || string(result.Body) != "chunk" || out.String() != "chunk" {
		t.Fatalf("copy result=%+v out=%q err=%v", result, out.String(), err)
	}
}

func TestCopyAndFlushEmptyReaderStillSetsEnd(t *testing.T) {
	var out strings.Builder
	result, err := copyAndFlush(noFlushWriter{Builder: &out}, strings.NewReader(""), (&Router{}).clock(), false, nil)
	if err != nil || result.End.IsZero() || out.Len() != 0 {
		t.Fatalf("copy result=%+v out=%q err=%v", result, out.String(), err)
	}
}

func TestCopyAndFlushCapsRetainedTelemetryButCountsAllBytes(t *testing.T) {
	body := strings.Repeat("x", MaxStreamTelemetryBodyBytes+123)
	var out strings.Builder
	result, err := copyAndFlush(noFlushWriter{Builder: &out}, strings.NewReader(body), (&Router{}).clock(), false, nil)
	if err != nil {
		t.Fatalf("copyAndFlush: %v", err)
	}
	if result.Bytes != len(body) || len(result.Body) != MaxStreamTelemetryBodyBytes || out.Len() != len(body) {
		t.Fatalf("copy result=%+v retained=%d written=%d want bytes=%d retained=%d", result, len(result.Body), out.Len(), len(body), MaxStreamTelemetryBodyBytes)
	}
}

func TestCopyAndFlushTracksSSETerminalBeyondRetainedTelemetry(t *testing.T) {
	body := strings.Repeat("x", MaxStreamTelemetryBodyBytes+123) + "\ndata: [DONE]\n\n"
	var out strings.Builder
	result, err := copyAndFlush(noFlushWriter{Builder: &out}, strings.NewReader(body), (&Router{}).clock(), true, nil)
	if err != nil {
		t.Fatalf("copyAndFlush: %v", err)
	}
	if !result.SSETerminal || len(result.Body) != MaxStreamTelemetryBodyBytes || out.Len() != len(body) {
		t.Fatalf("copy result=%+v retained=%d written=%d", result, len(result.Body), out.Len())
	}
}

func TestReadLimitedRejectsOversizeBody(t *testing.T) {
	body, err := readLimited(strings.NewReader("abcd"), 3, "tiny body")
	if err == nil || !strings.Contains(err.Error(), "tiny body exceeds 3") || body != nil {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestReadLimitedReturnsBodyAndReaderErrors(t *testing.T) {
	body, err := readLimited(strings.NewReader("abc"), 3, "tiny body")
	if err != nil || string(body) != "abc" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if body, err := readLimited(errReader{}, 3, "tiny body"); err == nil || body != nil {
		t.Fatalf("reader error body=%q err=%v", body, err)
	}
}

func TestJoinURLPreservesExplicitSchemes(t *testing.T) {
	if got := joinURL("https://peer.test/", "/v1/chat"); got != "https://peer.test/v1/chat" {
		t.Fatalf("https join = %q", got)
	}
	if got := joinURL("peer.test", "/v1/chat"); got != "http://peer.test/v1/chat" {
		t.Fatalf("plain join = %q", got)
	}
}

func TestSessionRecorderHelpersUsePresetFallbacksAndNoopWhenNil(t *testing.T) {
	var nilRecorder *sessionRecorder
	if err := nilRecorder.emitMetric(context.Background(), domain.RunMetric{}, domain.TelemetryPhaseComplete, 0, 0, 0, 0); err != nil {
		t.Fatalf("nil emitMetric: %v", err)
	}
	sink := &mocks.TelemetrySink{}
	router := &Router{
		Telemetry:  sink,
		SelfNodeID: "node-a",
		Clock:      mocks.NewFakeClock(time.Unix(10, 0).UTC()),
	}
	recorder := router.newSessionRecorder()
	job := domain.Job{ID: "job-a", Project: "project-a"}
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.WithInstancePreset(""), fixtures.OnNode("node-a"))
	if err := recorder.emitAt(context.Background(), job, preset, inst, domain.TelemetryPhasePlaced, time.Time{}, nil); err != nil {
		t.Fatalf("emitAt: %v", err)
	}
	if len(sink.SamplesOut) != 1 || sink.SamplesOut[0].PresetID != "preset-a" || sink.SamplesOut[0].At.IsZero() {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
}

func TestCleanupContextDropsCancellationButKeepsValues(t *testing.T) {
	type contextKey string
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey("job"), "job-a"))
	cancel()
	clean := cleanupContext(parent)
	if err := clean.Err(); err != nil {
		t.Fatalf("cleanup context err = %v", err)
	}
	if got := clean.Value(contextKey("job")); got != "job-a" {
		t.Fatalf("cleanup context value = %v", got)
	}
	if cleanupContext(nil).Err() != nil {
		t.Fatal("nil cleanup context should be background")
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
	router.SelfNodeID = inst.NodeID
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

func TestRouterGatewayJobIDsDoNotReuseAfterRestart(t *testing.T) {
	req := translate.IngressRequest{Model: "preset-a", Kind: translate.KindOpenAIChat}
	first := &Router{Clock: mocks.NewFakeClock(time.Unix(10, 0).UTC())}
	second := &Router{Clock: mocks.NewFakeClock(time.Unix(11, 0).UTC())}

	firstJob := first.jobFromIngress(req, 1)
	secondJob := second.jobFromIngress(req, 1)
	if firstJob.ID == secondJob.ID {
		t.Fatalf("restart reused gateway job id %q", firstJob.ID)
	}
	if !strings.HasPrefix(firstJob.ID, "gateway-10000000000-") || !strings.HasPrefix(secondJob.ID, "gateway-11000000000-") {
		t.Fatalf("job ids do not include restart prefix: %q %q", firstJob.ID, secondJob.ID)
	}
}

func TestRouterRequiresCoordinatorRuntimeForProductionPlacement(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not be called without coordinator runtime")
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	fakeClock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	router := &Router{
		Placer: scheduler.NewPlacer(
			estimate.NewInMemory(),
			lease.NewAllocator(),
			fakeClock,
			preset,
		),
		Fleet:   staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}},
		Nodes:   staticResolver{},
		Presets: NewPresetRegistry(preset),
		Client:  testUpstreams.client(),
		Clock:   fakeClock,
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}

	if _, err := router.Route(context.Background(), req); err == nil || !strings.Contains(err.Error(), "requires coordinator runtime") {
		t.Fatalf("Route err = %v", err)
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
	httpReq.Header.Set(HeaderSubmitter, "submitter-a")

	req, err := parseRequest(httpReq)
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if req.Project != "proj-a" || req.Priority != domain.PriorityBackground || req.SpeedPref != domain.SpeedLatency || req.ContextRequest != 4096 || req.Preemption != domain.PreemptHard || req.ConversationKey != "thread-a" || req.Handling != "" || req.Submitter != "submitter-a" {
		t.Fatalf("req = %+v", req)
	}

	privateReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(raw))
	privateReq.Header.Set(HeaderHandling, string(domain.HandlingPrivate))
	if _, err := parseRequest(privateReq); err == nil || !strings.Contains(err.Error(), "invalid X-Myc-Handling") {
		t.Fatalf("expected private handling rejection, got %v", err)
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

func TestParseRequestIgnoresUntrustedControlHeaders(t *testing.T) {
	raw := `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}]}`
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(raw))
	httpReq.Header.Set(HeaderProject, "proj-a")
	httpReq.Header.Set(HeaderPriority, "urgent")
	httpReq.Header.Set(HeaderPreemption, string(domain.PreemptHard))
	httpReq.Header.Set(HeaderHandling, string(domain.HandlingPrivate))

	req, err := parseRequestWithControlHeaders(httpReq, false)
	if err != nil {
		t.Fatalf("parse untrusted: %v", err)
	}
	if req.Project != "" || req.Priority != "" || req.Preemption != "" || req.Handling != "" {
		t.Fatalf("untrusted control headers were applied: %+v", req)
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
	if got := metricTokensPerSecond(10, metricTiming{Start: start, FirstByte: first, End: end}); got != 5 {
		t.Fatalf("stream metric tokens/sec = %f", got)
	}
	if got := metricTokensPerSecond(10, metricTiming{Start: start, FirstByte: end, End: end}); got != 10/2.1 {
		t.Fatalf("buffered metric tokens/sec = %f", got)
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
	result, err := copyAndFlush(noFlushWriter{Builder: &w}, errReader{}, (&Router{}).clock(), false, nil)
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) || result.Body != nil {
		t.Fatalf("copy result=%+v err=%v", result, err)
	}
}

func TestRouteDiagnosticsClassifiesToolModesAndVLLMToolConfig(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "invalid", body: `{`, want: "none"},
		{name: "none", body: `{"model":"m"}`, want: "none"},
		{name: "tools default auto", body: `{"tools":[{"type":"function","function":{"name":"lookup"}}]}`, want: "auto_default"},
		{name: "string", body: `{"tool_choice":"required"}`, want: "required"},
		{name: "empty string", body: `{"tool_choice":" "}`, want: "none"},
		{name: "function object", body: `{"tool_choice":{"type":"function","function":{"name":"lookup"}}}`, want: "function:lookup"},
		{name: "typed object", body: `{"tool_choice":{"type":"required"}}`, want: "required"},
		{name: "custom object", body: `{"tool_choice":{"unexpected":true}}`, want: "custom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := toolChoiceMode(translate.IngressRequest{Body: []byte(tc.body)})
			if got != tc.want {
				t.Fatalf("toolChoiceMode = %q, want %q", got, tc.want)
			}
		})
	}

	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--tool-call-parser", "hermes", "--enable-auto-tool-choice"}
	})
	parser, auto := vllmToolConfig(preset)
	if !parser || !auto {
		t.Fatalf("vllmToolConfig parser=%t auto=%t", parser, auto)
	}
	preset.LaunchArgs = []string{"--tool-call-parser=qwen3"}
	parser, auto = vllmToolConfig(preset)
	if !parser || auto {
		t.Fatalf("vllmToolConfig equals parser=%t auto=%t", parser, auto)
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
		http.Error(w, `{"error":{"message":"request (3000 tokens) exceeds the available context size (2048 tokens), try increasing it"}}`, http.StatusBadRequest)
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
		http.Error(w, `{"error":{"message":"request (1500 tokens) exceeds the available context size (1024 tokens), try increasing it"}}`, http.StatusBadRequest)
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
	req.Priority = domain.PriorityBackground

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
	if agent.loads[0].Priority != domain.PriorityBackground {
		t.Fatalf("load priority = %q", agent.loads[0].Priority)
	}
}

func TestRouterColdLoadUsesPlacementTunedContext(t *testing.T) {
	preset := fixtures.MakePreset(
		fixtures.WithPresetID("b70-qwen27b"),
		fixtures.WithContextLength(262144),
		fixtures.WithWeights(16039),
		fixtures.WithKVPerToken(0.0625),
	)
	node := fixtures.MakeNode(fixtures.WithNodeID("b70"), fixtures.WithVRAM(31023), fixtures.WithMaxUtil(0.85))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-b70"), fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(16039, 5120)))
	agent := recordingLoadAgent{node: node, inst: inst}
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{node}}
	router := newTestRouter(preset, fleet, staticResolver{agents: map[string]ports.NodeAgent{node.ID: &agent}})
	job := fixtures.MakeJob(
		fixtures.WithJobID("b70-27b-normal"),
		fixtures.WithPreset(preset.ID),
		fixtures.WithContext(81920),
		fixtures.WithModel(preset.ModelRef),
	)

	decision, _, _, _, err := router.placeAndLoad(context.Background(), job, []byte(`{}`), preset, fleet, nil)
	if err != nil {
		t.Fatalf("placeAndLoad: %v", err)
	}
	if decision.Preset.ContextLength != 81920 {
		t.Fatalf("decision context = %d, want 81920", decision.Preset.ContextLength)
	}
	if len(agent.loads) != 1 || agent.loads[0].Preset.ContextLength != 81920 {
		t.Fatalf("loads = %+v", agent.loads)
	}
}

func TestRouterClassifiesOverflowBeforeServerErrorFailover(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "prompt of 3000 tokens exceeds context window", http.StatusInternalServerError)
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

func TestRouterStickyTableDoesNotBypassCoordinatorPlacement(t *testing.T) {
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
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.ConversationKey = "thread-a"

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if resp.Instance.ID != instA.ID || !strings.Contains(string(resp.Body), "first") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
}

func TestRouterWarmPlacementUsesOwnerAdmission(t *testing.T) {
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
		Placer:  router.Placer,
		Fleet:   router.Fleet,
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

func TestRouterWarmPlacementUsesCoordinatorWhenOwnerAdmissionRejectsStaleInstance(t *testing.T) {
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
	if len(admission.Requests) != 1 || admission.Requests[0].InstanceID != instA.ID {
		t.Fatalf("admission requests = %+v", admission.Requests)
	}
}

func TestRouterRejectsPrivateHandlingUntilRecoverySemanticsExist(t *testing.T) {
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
	if _, err := noKey.Route(context.Background(), req); err == nil || !strings.Contains(err.Error(), "private handling is disabled") {
		t.Fatalf("private err = %v", err)
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

func TestRouterDefaultsJobContextToEffectivePresetContext(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "spark-large"
		p.Backend = domain.BackendVLLM
		p.ContextLength = 49152
		p.LaunchArgs = []string{"--max-model-len", "8192"}
	})
	router := &Router{Presets: NewPresetRegistry(preset)}
	req := translate.IngressRequest{
		Model: "spark-large",
		Kind:  translate.KindOpenAIChat,
	}

	job := router.jobFromIngress(req, 1)
	if job.ContextRequest != 8192 {
		t.Fatalf("context request = %d, want 8192", job.ContextRequest)
	}

	req.ContextRequest = 4096
	job = router.jobFromIngress(req, 2)
	if job.ContextRequest != 4096 {
		t.Fatalf("explicit context request = %d, want 4096", job.ContextRequest)
	}
}

func TestRouterLeavesModelGroupContextForScheduler(t *testing.T) {
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-large"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 49152
		p.LaunchArgs = []string{"--max-model-len", "49152"}
	})
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-small"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 16384
		p.LaunchArgs = []string{"--max-model-len", "16384"}
	})
	router := &Router{Presets: NewPresetRegistry(large, small)}

	job := router.jobFromIngress(translate.IngressRequest{
		Model: "vision",
		Kind:  translate.KindOpenAIChat,
	}, 1)
	if job.ContextRequest != 0 {
		t.Fatalf("context request = %d, want scheduler default", job.ContextRequest)
	}

	job = router.jobFromIngress(translate.IngressRequest{
		Model:          "vision",
		Kind:           translate.KindOpenAIChat,
		ContextRequest: 32768,
	}, 2)
	if job.ContextRequest != 32768 {
		t.Fatalf("explicit context request = %d, want 32768", job.ContextRequest)
	}
}

func TestRouterColdStreamPrependsLoadingState(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\ndata: [DONE]\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = node.ID
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
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_stream"))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = node.ID
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
	gotPhases := make([]domain.TelemetryPhase, 0, len(sink.SamplesOut))
	for _, sample := range sink.SamplesOut {
		gotPhases = append(gotPhases, sample.Phase)
	}
	wantPhases := []domain.TelemetryPhase{
		domain.TelemetryPhasePlaced,
		domain.TelemetryPhaseLoadReady,
		domain.TelemetryPhaseUpstreamStart,
		domain.TelemetryPhaseFirstByte,
		domain.TelemetryPhaseStreamChunk,
		domain.TelemetryPhaseComplete,
	}
	if !reflect.DeepEqual(gotPhases, wantPhases) {
		t.Fatalf("sample phases = %+v", gotPhases)
	}
	last := sink.SamplesOut[len(sink.SamplesOut)-1]
	if last.SessionID == "" || last.BytesOut == 0 || last.InstanceID != inst.ID || last.NodeID != node.ID {
		t.Fatalf("complete sample = %+v", last)
	}
}

func TestRouterStreamUsesSelectedPresetForSharedModel(t *testing.T) {
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-large"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 49152
		p.NodeID = "owner-large"
	})
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-small"
		p.ModelRef = "vision-model"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.ContextLength = 16384
		p.NodeID = "owner-small"
	})
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	inst := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-small"),
		fixtures.WithInstancePreset(small.ID),
		fixtures.OnNode("owner-small"),
	)
	inst.Addr = upstream
	largeNode := fixtures.MakeNode(fixtures.WithNodeID("owner-large"), fixtures.WithVRAM(65536), fixtures.WithMaxUtil(1))
	smallNode := fixtures.MakeNode(fixtures.WithNodeID("owner-small"), fixtures.WithVRAM(32768), fixtures.WithMaxUtil(1))
	router := newTestRouter(large, domain.FleetSnapshot{
		Nodes:     []domain.Node{largeNode, smallNode},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{"owner-small": mocks.NewNodeAgent(smallNode)}}, small)
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"vision","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"content":"ok"`) {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get(HeaderNode) != "owner-small" || rec.Header().Get(HeaderBackend) != string(domain.BackendVLLM) {
		t.Fatalf("headers = %+v", rec.Header())
	}
	last := sink.SamplesOut[len(sink.SamplesOut)-1]
	if last.PresetID != small.ID || last.NodeID != "owner-small" {
		t.Fatalf("stream telemetry = %+v", sink.SamplesOut)
	}
}

func TestRouterStreamWarmInstanceCopiesHeadersAndBody(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Upstream", "ok")
		_, _ = w.Write([]byte("data: warm\n\ndata: [DONE]\n\n"))
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
	if rec.Body.String() != "data: warm\n\ndata: [DONE]\n\n" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestRouterStreamPersistsFinalMetricFromSSEUsage(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":3,"stream":true,"stream_options":{"include_usage":true}}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-stream"
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total_tokens":10`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	metric := sink.Metrics[0]
	if metric.Project != "project-stream" || metric.ContextUsed != 10 || metric.TTFTms < 0 {
		t.Fatalf("metric = %+v", metric)
	}
	if len(sink.SamplesOut) == 0 {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	complete := sink.SamplesOut[len(sink.SamplesOut)-1]
	if complete.Phase != domain.TelemetryPhaseComplete || complete.Project != "project-stream" || complete.ContextUsed != 10 || complete.TokensIn != 7 || complete.TokensOut != 3 {
		t.Fatalf("complete sample = %+v", complete)
	}
}

func TestRouterStreamFinalTelemetryUsesCleanupContextAfterClientCancel(t *testing.T) {
	preset := fixtures.MakePreset()
	ctx, cancel := context.WithCancel(context.Background())
	inst := fixtures.MakeInstance()
	inst.Addr = "http://upstream.test"
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	sink := &contextCheckingTelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	router.Client = &http.Client{Transport: gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
		body := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\ndata: [DONE]\n\n")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       &cancelAfterReadBody{data: body, cancel: cancel, errAfterSent: context.Canceled},
		}, nil
	})}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":3,"stream":true,"stream_options":{"include_usage":true}}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-stream"
	rec := httptest.NewRecorder()

	if err := router.Stream(ctx, req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("test did not cancel request context at provider EOF")
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v calls=%+v", sink.Metrics, sink.Calls)
	}
	complete := sink.SamplesOut[len(sink.SamplesOut)-1]
	if complete.Phase != domain.TelemetryPhaseComplete || complete.ContextUsed != 10 {
		t.Fatalf("complete sample = %+v", complete)
	}
}

func TestRouterStreamDoesNotAppendMetricErrorAfterProviderStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: metric\n\ndata: [DONE]\n\n"))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	metricErr := errors.New("stream metric failed")
	router.Telemetry = &mocks.TelemetrySink{}
	router.SelfNodeID = inst.NodeID
	router.MemorySampler = fixedMemorySampler{Err: metricErr}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); body != "data: metric\n\ndata: [DONE]\n\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterStreamDoesNotAppendCompleteSampleErrorAfterProviderStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: sample\n\ndata: [DONE]\n\n"))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}, Instances: []domain.ModelInstance{inst}}, staticResolver{})
	sampleErr := errors.New("stream sample failed")
	router.Telemetry = &phaseFailTelemetry{failPhase: domain.TelemetryPhaseComplete, err: sampleErr}
	router.SelfNodeID = inst.NodeID
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	if body := rec.Body.String(); body != "data: sample\n\ndata: [DONE]\n\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestRouterColdStreamDoesNotAppendMyceliumFrameAfterProviderBodyStarts(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: cold\n\ndata: [DONE]\n\n"))
	}))
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold_stream"), fixtures.OnNode(node.ID))
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: loadNode{node: node, inst: inst},
	}})
	metricErr := errors.New("memory sample failed")
	router.Telemetry = &mocks.TelemetrySink{}
	router.SelfNodeID = node.ID
	router.MemorySampler = fixedMemorySampler{Err: metricErr}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after start: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: loading") || !strings.Contains(body, "event: ready") || !strings.Contains(body, "data: cold") {
		t.Fatalf("body = %q", body)
	}
	if strings.Contains(body, "event: error") || strings.Contains(body, metricErr.Error()) {
		t.Fatalf("provider body was followed by Mycelium error frame: %q", body)
	}
}

func TestRouterStreamCopyErrorReportsAndRemovesFailedInstance(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_copy_err"), fixtures.OnNode(node.ID))
	inst.Addr = "http://copy-error.mycelium.test"
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	store := &gatewayRuntimeStore{}
	router := newRuntimeRouterForWarmInstance(preset, node, inst, agent, &mocks.AdmissionController{}, store)
	router.Client = &http.Client{Transport: gatewayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       &chunkThenErrorReadCloser{chunk: []byte("data: partial\n\n"), err: io.ErrUnexpectedEOF},
			Request:    req,
		}, nil
	})}
	failureStore := newFailureStore(inst)
	router.Reporter = InstanceFailureReporter{
		Store: failureStore,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}},
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after provider start: %v", err)
	}
	if body := rec.Body.String(); body != "data: partial\n\n" {
		t.Fatalf("body = %q", body)
	}
	if _, ok := failureStore.instances[inst.ID]; ok || strings.Join(failureStore.deleted, ",") != inst.ID {
		t.Fatalf("failure store instances=%+v deleted=%+v", failureStore.instances, failureStore.deleted)
	}
	if !containsCall(agent.Calls, "unload:"+inst.ID) {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	if !store.sawJobStatus(domain.JobFailed) || len(store.deletedLeases) != 1 {
		t.Fatalf("jobs=%+v deletedLeases=%+v", store.jobs, store.deletedLeases)
	}
}

func TestRouterStreamProxiedTruncatedSSEFails(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_truncated"), fixtures.OnNode(node.ID))
	inst.Addr = "http://node-proxy.mycelium.test/instances/inst_truncated"
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	store := &gatewayRuntimeStore{}
	router := newRuntimeRouterForWarmInstance(preset, node, inst, agent, &mocks.AdmissionController{}, store)
	router.Client = &http.Client{Transport: gatewayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("data: partial\n\n")),
			Request:    req,
		}, nil
	})}
	failureStore := newFailureStore(inst)
	router.Reporter = InstanceFailureReporter{
		Store: failureStore,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}},
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream returned error after provider start: %v", err)
	}
	if body := rec.Body.String(); body != "data: partial\n\n" || strings.Contains(body, "event: error") {
		t.Fatalf("body = %q", body)
	}
	if !store.sawJobStatus(domain.JobFailed) || store.sawJobStatus(domain.JobDone) || len(store.deletedLeases) != 1 {
		t.Fatalf("jobs=%+v deletedLeases=%+v", store.jobs, store.deletedLeases)
	}
	if _, ok := failureStore.instances[inst.ID]; ok || strings.Join(failureStore.deleted, ",") != inst.ID {
		t.Fatalf("failure store instances=%+v deleted=%+v", failureStore.instances, failureStore.deleted)
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

func TestRouterStreamReturnsBuildErrorBeforeStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_translate"))
	inst.Addr = "http://example.invalid"
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}, staticResolver{agents: map[string]ports.NodeAgent{
		node.ID: agent,
	}})
	req, err := translate.ParseAnthropicMessages([]byte(`{"model":"qwen2.5-9b-instruct","max_tokens":1,"stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	if err != nil {
		t.Fatalf("ParseAnthropicMessages: %v", err)
	}
	rec := httptest.NewRecorder()

	err = router.Stream(context.Background(), req, rec)
	if err == nil || !strings.Contains(err.Error(), "streaming anthropic-to-openai translation") {
		t.Fatalf("Stream err = %v", err)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("stream started before build failure: %q", rec.Body.String())
	}
	if len(agent.Calls) != 0 {
		t.Fatalf("agent should not begin before build failure: %+v", agent.Calls)
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
		_, _ = w.Write([]byte("data: rescued\n\ndata: [DONE]\n\n"))
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

func TestRouterRouteEarlyErrors(t *testing.T) {
	preset := fixtures.MakePreset()
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
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
		{name: "missing model", router: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}), req: translate.IngressRequest{Kind: translate.KindOpenAIChat}, want: "model is required"},
		{name: "unknown model", router: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}), req: translate.IngressRequest{Kind: translate.KindOpenAIChat, Model: "missing"}, want: "unknown model"},
		{name: "fleet", router: &Router{Placer: newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{}).Placer, Fleet: staticFleet{err: errors.New("fleet failed")}, Nodes: staticResolver{}, Presets: NewPresetRegistry(preset)}, req: req, want: "fleet failed"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			_, err := check.router.Route(context.Background(), check.req)
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
	if resp.Header.Get(HeaderColdLoad) != "true" || resp.Header.Get(HeaderLoadWallMS) != "0" || resp.Header.Get(HeaderQueueWaitMS) != "0" {
		t.Fatalf("cold diagnostic headers = %+v", resp.Header)
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

func TestRouterStreamDoesNotAppendRuntimeReleaseErrorAfterProviderStarted(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: ok\n\ndata: [DONE]\n\n"))
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

	rec := httptest.NewRecorder()
	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream err = %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, "data: ok") || strings.Contains(body, "event: error") || strings.Contains(body, deleteErr.Error()) {
		t.Fatalf("body = %q", body)
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

func newRuntimeRouterForWarmInstance(preset domain.Preset, node domain.Node, inst domain.ModelInstance, agent ports.NodeAgent, admission ports.AdmissionController, store *gatewayRuntimeStore) *Router {
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: agent},
		admissions: map[string]ports.AdmissionController{node.ID: admission},
	}
	fleet := staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}}
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
		http.Error(w, "request of 3000 tokens exceeds context window", http.StatusBadRequest)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: enlarged\n\ndata: [DONE]\n\n"))
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

func TestRouterColdStreamRetriesContextOverflowAfterLoadingFrame(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request of 3000 tokens exceeds context window", http.StatusBadRequest)
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: enlarged-cold\n\ndata: [DONE]\n\n"))
	}))

	node := fixtures.MakeNode()
	instSmall := fixtures.MakeInstance(fixtures.WithInstanceID("inst_small"), fixtures.WithInstancePreset(small.ID), fixtures.OnNode(node.ID))
	instSmall.Addr = first
	instLarge := fixtures.MakeInstance(fixtures.WithInstanceID("inst_large"), fixtures.WithInstancePreset(large.ID), fixtures.OnNode(node.ID))
	instLarge.Addr = second
	agent := &presetLoadAgent{
		node: node,
		instances: map[string]domain.ModelInstance{
			small.ID: instSmall,
			large.ID: instLarge,
		},
	}
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: agent},
		admissions: map[string]ports.AdmissionController{node.ID: &mocks.AdmissionController{}},
	}
	fleet := staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	router := newTestRouter(small, fleet.fleet, resolver, large)
	router.Fleet = fleet
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   fleet,
		Nodes:   resolver,
		Owners:  resolver,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   &gatewayRuntimeStore{},
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{small.ID: small, large.ID: large},
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"preset_small","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: loading") || !strings.Contains(body, `"instance_id":"inst_large"`) || !strings.Contains(body, "enlarged-cold") || strings.Contains(body, "event: error") {
		t.Fatalf("headers=%+v body=%q", rec.Header(), body)
	}
	if len(agent.loads) != 2 || agent.loads[0].Preset.ID != small.ID || agent.loads[1].Preset.ID != large.ID {
		t.Fatalf("loads = %+v", agent.loads)
	}
}

func TestRouterStreamFailsOverBeginRequestErrorBeforeResponseStarts(t *testing.T) {
	preset := fixtures.MakePreset()
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"))
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(nodeA.ID))
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst-b"), fixtures.OnNode(nodeB.ID))
	first := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data: should-not-run\n\n"))
	}))
	second := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: rescued\n\ndata: [DONE]\n\n"))
	}))
	instA.Addr = first
	instB.Addr = second
	agentA := mocks.NewNodeAgent(nodeA)
	agentA.BeginErr = errors.New("begin failed")
	agentB := mocks.NewNodeAgent(nodeB)
	reporter := &testFailureReporter{}
	router := newTestRouter(preset, domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}, Instances: []domain.ModelInstance{instA, instB}}, staticResolver{agents: map[string]ports.NodeAgent{
		nodeA.ID: agentA,
		nodeB.ID: agentB,
	}})
	router.Reporter = reporter
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	rec := httptest.NewRecorder()

	if err := router.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if rec.Header().Get(HeaderInstance) != "inst-b" || !strings.Contains(rec.Body.String(), "rescued") || len(reporter.failed) != 1 || reporter.failed[0] != "inst-a" {
		t.Fatalf("headers=%+v body=%q failed=%+v", rec.Header(), rec.Body.String(), reporter.failed)
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
		_, _ = w.Write([]byte("data: server-stream\n\ndata: [DONE]\n\n"))
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

func TestServerListsPublicModels(t *testing.T) {
	preset := fixtures.MakePreset()
	preset.ID = "preset-a"
	preset.ModelRef = "/private/models/model.gguf"
	preset.Aliases = []string{"public-a", "public-alt"}
	private := fixtures.MakePreset()
	private.ID = "preset-b"
	private.ModelRef = "/private/models/other.gguf"
	private.Aliases = nil
	router := &Router{Presets: NewPresetRegistry(preset, private)}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret")
	Server{Router: router, RequireAuth: true, AuthToken: "secret"}.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Object string `json:"object"`
		Data   []struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode models: %v", err)
	}
	var ids []string
	for _, model := range out.Data {
		ids = append(ids, model.ID)
		if model.Object != "model" || model.OwnedBy != "mycelium" {
			t.Fatalf("model metadata = %+v", model)
		}
	}
	got := strings.Join(ids, ",")
	for _, want := range []string{"preset-a", "public-a", "public-alt", "preset-b"} {
		if !strings.Contains(got, want) {
			t.Fatalf("models missing %s: %s", want, got)
		}
	}
	if strings.Contains(rec.Body.String(), "/private/") {
		t.Fatalf("models endpoint leaked private model ref: %s", rec.Body.String())
	}
}

func TestServerModelsEchoesAuthenticatedProject(t *testing.T) {
	router := &Router{Presets: NewPresetRegistry(fixtures.MakePreset())}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer project-token")

	Server{
		Router:            router,
		RequireAuth:       true,
		AuthTokenProjects: map[string]string{"project-token": "project-a"},
	}.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get(HeaderProject) != "project-a" {
		t.Fatalf("project header = %q", rec.Header().Get(HeaderProject))
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
	if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "X-Myc-Speed-Pref") {
		t.Fatalf("spoofed header status/body = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(HeaderSpeedPref, "warp")
	Server{Router: &Router{}, TrustControlHeaders: true}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "X-Myc-Speed-Pref") {
		t.Fatalf("trusted bad header status/body = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	Server{Router: &Router{}}.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("route error status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	writeGatewayRouteError(rec, fmt.Errorf("failover: %w", noInstanceAvailableError{
		JobID:      "job-a",
		RetryAfter: 1500 * time.Millisecond,
		Decision: domain.PlacementDecision{
			JobID:  "job-a",
			Action: domain.ActionQueued,
			Trace:  []domain.TraceStep{{Step: "resolve", Result: "model group"}},
		},
		Attempts: 2,
	}))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "2" || !strings.Contains(rec.Body.String(), "no instance available") {
		t.Fatalf("no-instance status/header/body = %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if rec.Header().Get(HeaderDecision) != string(domain.ActionQueued) || rec.Header().Get(HeaderAttempts) != "2" || !strings.Contains(rec.Header().Get(HeaderTrace), "model group") {
		t.Fatalf("no-instance diagnostics = %+v", rec.Header())
	}
}

func TestServerRequiresGatewayTokenWhenConfigured(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}]}`
	server := Server{Router: &Router{}, RequireAuth: true, AuthToken: "secret"}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status/body = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("authorized request did not reach router: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPlacementNoFitErrorRecognizesLegacyOwnerAdmissionText(t *testing.T) {
	if !placementNoFitError(fmt.Errorf("node http /admission/offer: no unit can fit this preset under its constraints")) {
		t.Fatal("legacy no-fit text was not classified")
	}
	if !placementNoFitError(fmt.Errorf("node http /admission/offer: warm instance \"inst_1\" concurrency envelope is full")) {
		t.Fatal("legacy envelope text was not classified")
	}
	if !placementNoFitError(fmt.Errorf("no preset for model \"chandra\" can satisfy job: small:preset \"small\" context cap 16384 is below requested context 49152")) {
		t.Fatal("model-group context refusal was not classified")
	}
	if placementNoFitError(fmt.Errorf("upstream returned 500: model crashed")) {
		t.Fatal("provider failure classified as no-fit")
	}
}

func TestRouterContextRefusalCreatesReconciliationDemand(t *testing.T) {
	small := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "small"
		p.ModelRef = "model-small"
		p.Aliases = []string{"chandra"}
		p.ContextLength = 1024
	})
	large := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "large"
		p.ModelRef = "model-large"
		p.Aliases = []string{"chandra"}
		p.ContextLength = 2048
	})
	store := &gatewayRuntimeStore{}
	router := newTestRouter(small, domain.FleetSnapshot{}, staticResolver{}, large)
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   router.Fleet,
		Nodes:   staticResolver{},
		Owners:  staticResolver{},
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   store,
		Clock:   router.Clock,
		Presets: map[string]domain.Preset{small.ID: small, small.ModelRef: small, large.ID: large, large.ModelRef: large},
	}
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"chandra","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.ContextRequest = 4096
	_, err = router.Route(context.Background(), req)
	var noInstance noInstanceAvailableError
	if !errors.As(err, &noInstance) {
		t.Fatalf("Route error = %v, want no instance", err)
	}
	if len(store.reconcileTasks) != 2 {
		t.Fatalf("reconciliation tasks = %+v", store.reconcileTasks)
	}
	if len(store.services) != 1 || store.services[0].Model != "chandra" || store.services[0].ContextRequest != 4096 || len(store.services[0].CandidatePresetIDs) != 0 {
		t.Fatalf("model services = %+v", store.services)
	}
	for _, task := range store.reconcileTasks {
		if task.Kind != string(reconcile.TaskRecordIncompatibility) || task.Status != domain.ReconciliationTaskAllowed || !strings.Contains(task.Reason, "context cap") {
			t.Fatalf("task = %+v", task)
		}
	}
}

func TestRouterNoInstanceCreatesReconciliationDemand(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ModelRef = "chandra"
		p.Aliases = []string{"chandra"}
		p.ContextLength = 49152
	})
	node := fixtures.MakeNode(func(n *domain.Node) {
		n.ID = "node-small"
		n.Accelerators = []domain.Accelerator{{Index: 0, VRAMTotalMB: 1}}
	})
	store := &gatewayRuntimeStore{}
	resolver := staticResolver{
		agents:     map[string]ports.NodeAgent{node.ID: loadNode{node: node, inst: fixtures.MakeInstance(fixtures.OnNode(node.ID))}},
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
	req, err := translate.ParseOpenAIChat([]byte(`{"model":"chandra","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	_, err = router.Route(context.Background(), req)
	var noInstance noInstanceAvailableError
	if !errors.As(err, &noInstance) {
		t.Fatalf("Route error = %v, want no instance", err)
	}
	if len(store.reconcileTasks) == 0 {
		t.Fatalf("expected gateway demand to create reconciliation tasks")
	}
	var found bool
	for _, task := range store.reconcileTasks {
		if task.Kind == "wait_capacity" && task.JobID == noInstance.JobID && task.Status == domain.ReconciliationTaskAllowed && task.Model == "chandra" {
			found = true
		}
	}
	if !found {
		t.Fatalf("reconciliation tasks = %+v", store.reconcileTasks)
	}
	if len(store.services) != 1 || store.services[0].Model != "chandra" || store.services[0].DemandCount != 1 {
		t.Fatalf("model services = %+v", store.services)
	}

	before := len(store.reconcileTasks)
	_, err = router.Route(context.Background(), req)
	if !errors.As(err, &noInstance) {
		t.Fatalf("second Route error = %v, want no instance", err)
	}
	if len(store.reconcileTasks) != before+1 {
		t.Fatalf("independent gateway jobs should create independent demand records, before=%d after=%d tasks=%+v", before, len(store.reconcileTasks), store.reconcileTasks)
	}
	if len(store.services) != 1 || store.services[0].DemandCount != 2 {
		t.Fatalf("second demand should bump service instead of creating ceremony: %+v", store.services)
	}
}

func TestRouterMissingCatalogModelCreatesServiceDemand(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "other-preset"
		p.ModelRef = "other-model"
		p.Aliases = []string{"other-model"}
	})
	router := newTestRouter(preset, domain.FleetSnapshot{}, staticResolver{})
	store := &gatewayRuntimeStore{}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"new-model","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.ContextRequest = 2048
	_, err = router.Route(context.Background(), req)
	var noInstance noInstanceAvailableError
	if err == nil || !errors.As(err, &noInstance) || !strings.Contains(err.Error(), "model is not ready in the local catalog") {
		t.Fatalf("err = %v", err)
	}
	if noInstance.Decision.Action != domain.ActionQueued || noInstance.retryAfterSeconds() != 1 {
		t.Fatalf("noInstance = %+v", noInstance)
	}
	if len(store.services) != 1 {
		t.Fatalf("services = %+v", store.services)
	}
	service := store.services[0]
	if service.ID != serving.ServiceID("project-a", "new-model", 2048) || service.Model != "new-model" || service.Project != "project-a" || service.ContextRequest != 2048 || service.Status != domain.ModelServiceActive || service.DemandCount != 1 || service.LastDemandJobID == "" {
		t.Fatalf("service demand = %+v", service)
	}
	report, err := serving.Reconcile(context.Background(), store, serving.ReconcileOptions{Now: router.Clock.Now(), Limit: 1})
	if err != nil {
		t.Fatalf("serving reconcile: %v", err)
	}
	if report.Created != 1 || len(store.reconcileTasks) != 1 || store.reconcileTasks[0].Kind != string(reconcile.TaskRefreshCatalog) || store.reconcileTasks[0].JobID != service.ID {
		t.Fatalf("catalog refresh work was not created: report=%+v tasks=%+v", report, store.reconcileTasks)
	}
}

func TestGatewayReconciliationDemandBumpsExistingNonTerminalTask(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ModelRef = "model-a"
		p.Aliases = []string{"model-a"}
	})
	store := &gatewayRuntimeStore{}
	router := &Router{
		Runtime: &scheduler.Service{Store: store},
		Clock:   mocks.NewFakeClock(time.Unix(100, 0).UTC()),
	}
	job := domain.Job{ID: "job-a", Model: "model-a"}
	decision := domain.PlacementDecision{
		JobID:  job.ID,
		Preset: preset,
		Action: domain.ActionQueued,
		Trace: []domain.TraceStep{{
			Step:   "filter",
			Result: "all candidates dropped",
			Data:   map[string]any{"dropped": map[string]string{"node-a": "fit"}},
		}},
	}
	if err := router.persistGatewayReconciliationDemand(context.Background(), job, decision, preset); err != nil {
		t.Fatalf("persist demand: %v", err)
	}
	if len(store.reconcileTasks) != 1 {
		t.Fatalf("tasks = %+v", store.reconcileTasks)
	}
	if err := router.persistGatewayReconciliationDemand(context.Background(), job, decision, preset); err != nil {
		t.Fatalf("bump demand: %v", err)
	}
	if len(store.reconcileTasks) != 1 || len(store.reconcileTasks[0].Events) != 2 || store.reconcileTasks[0].Events[1].Message != "observed again from gateway demand" {
		t.Fatalf("bumped tasks = %+v", store.reconcileTasks)
	}
	store.reconcileTasks[0].Status = domain.ReconciliationTaskDone
	if err := router.persistGatewayReconciliationDemand(context.Background(), job, decision, preset); err != nil {
		t.Fatalf("terminal demand: %v", err)
	}
	if len(store.reconcileTasks[0].Events) != 2 {
		t.Fatalf("terminal task should not be bumped: %+v", store.reconcileTasks[0])
	}
}

func TestServerScopesProjectByGatewayTokenWithoutTrustingControlHeaders(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("project token ok")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{})
	sink := &mocks.TelemetrySink{}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	server := Server{
		Router:            router,
		RequireAuth:       true,
		AuthTokenProjects: map[string]string{"project-token-a": "project-a"},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer project-token-a")
	req.Header.Set(HeaderProject, "spoofed-project")
	req.Header.Set(HeaderPriority, "warp")

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status/body = %d %s", rec.Code, rec.Body.String())
	}
	if len(sink.Metrics) != 1 || sink.Metrics[0].Project != "project-a" {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
}

func TestServerRejectsOversizeRequestBody(t *testing.T) {
	rec := httptest.NewRecorder()
	body := io.LimitReader(repeatReader{b: 'x'}, MaxGatewayRequestBodyBytes+1)
	Server{Router: &Router{}}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "gateway request body exceeds") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
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
		_, _ = w.Write([]byte("data: server\n\ndata: [DONE]\n\n"))
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
	req.Header.Set(HeaderSubmitter, "submitter-a")
	got, err := parseRequest(req)
	if err != nil {
		t.Fatalf("parse chat: %v", err)
	}
	if got.Project != "project-a" || got.Priority != domain.PriorityBackground || got.SpeedPref != domain.SpeedAuto || got.Preemption != domain.PreemptHard || got.ContextRequest != 1234 || got.ConversationKey != "thread-a" || got.Handling != "" || got.Submitter != "submitter-a" {
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

func TestParseRequestAcceptsContextEnvelopeWithoutTrustingControlHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(HeaderContextCap, "16384")
	req.Header.Set(HeaderPriority, "warp")
	req.Header.Set(HeaderWorkload, "workload-a")
	req.Header.Set(HeaderLease, "lease-a")
	got, err := parseRequestWithControlHeaders(req, false)
	if err != nil {
		t.Fatalf("parse untrusted context header: %v", err)
	}
	if got.ContextRequest != 16384 {
		t.Fatalf("context request = %d, want 16384", got.ContextRequest)
	}
	if got.WorkloadID != "workload-a" || got.LeaseID != "lease-a" {
		t.Fatalf("lease headers = %q %q", got.WorkloadID, got.LeaseID)
	}
	if got.Priority != "" {
		t.Fatalf("untrusted priority parsed: %+v", got)
	}

	bad := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	bad.Header.Set(HeaderContextCap, "0")
	if _, err := parseRequestWithControlHeaders(bad, false); err == nil || !strings.Contains(err.Error(), "Context-Cap") {
		t.Fatalf("bad context header err = %v", err)
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
	if _, err := directory.NodeAgent("missing"); !errors.Is(err, domain.ErrUnreachable) {
		t.Fatalf("missing node agent err = %v", err)
	}
	if _, err := directory.AdmissionController(node.ID); err == nil {
		t.Fatal("plain node agent exposed admission")
	}
	if _, err := directory.LeaseInspector(node.ID); err == nil {
		t.Fatal("plain node agent exposed lease inspection")
	}
	if _, err := directory.JobStatusInspector(node.ID); err == nil {
		t.Fatal("plain node agent exposed job status inspection")
	}

	admitting := admittingAgent{NodeAgent: mocks.NewNodeAgent(node), AdmissionController: &mocks.AdmissionController{JobStatusVal: domain.JobDone, JobStatusFound: true}}
	directory = NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: admitting}}
	if _, err := directory.AdmissionController(node.ID); err != nil {
		t.Fatalf("AdmissionController: %v", err)
	}
	if _, err := directory.LeaseInspector(node.ID); err != nil {
		t.Fatalf("LeaseInspector: %v", err)
	}
	statusInspector, err := directory.JobStatusInspector(node.ID)
	status, found, err := mustJobStatusInspector(t, statusInspector, err).JobStatus(context.Background(), "job-a")
	if err != nil || !found || status != domain.JobDone {
		t.Fatalf("JobStatus = %q found=%v err=%v", status, found, err)
	}
}

func mustJobStatusInspector(t *testing.T, inspector ports.JobStatusInspector, err error) ports.JobStatusInspector {
	t.Helper()
	if err != nil {
		t.Fatalf("JobStatusInspector: %v", err)
	}
	return inspector
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

		allowDirectPlacement: true,
	}
}

var testUpstreams = &directUpstreams{handlers: map[string]http.Handler{}}

type directUpstreams struct {
	handlers map[string]http.Handler
}

type gatewayRoundTripFunc func(*http.Request) (*http.Response, error)

func (f gatewayRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type peerMap map[string]domain.Peer

func (m peerMap) PeerForNode(nodeID string) (domain.Peer, bool) {
	peer, ok := m[nodeID]
	return peer, ok
}

type contextCheckingTelemetrySink struct {
	mocks.TelemetrySink
}

func (s *contextCheckingTelemetrySink) Record(ctx context.Context, metric domain.RunMetric) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.TelemetrySink.Record(ctx, metric)
}

func (s *contextCheckingTelemetrySink) RecordSample(ctx context.Context, sample domain.SessionMetric) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.TelemetrySink.RecordSample(ctx, sample)
}

type cancelAfterReadBody struct {
	data         []byte
	sent         bool
	cancel       func()
	errAfterSent error
}

func (b *cancelAfterReadBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		n := copy(p, b.data)
		if b.cancel != nil {
			b.cancel()
		}
		return n, nil
	}
	if b.errAfterSent != nil {
		return 0, b.errAfterSent
	}
	return 0, io.EOF
}

func (b *cancelAfterReadBody) Close() error {
	return nil
}

func sampleWith(sample domain.SessionMetric, mutate func(*domain.SessionMetric)) domain.SessionMetric {
	mutate(&sample)
	return sample
}

type fixedMemorySampler struct {
	Peak int
	Err  error
}

func (s fixedMemorySampler) PeakMemoryMB(context.Context, string, string) (int, error) {
	return s.Peak, s.Err
}

type phaseFailTelemetry struct {
	failPhase domain.TelemetryPhase
	err       error
	metrics   []domain.RunMetric
	samples   []domain.SessionMetric
}

func (s *phaseFailTelemetry) Record(_ context.Context, metric domain.RunMetric) error {
	s.metrics = append(s.metrics, metric)
	return nil
}

func (s *phaseFailTelemetry) RecordSample(_ context.Context, sample domain.SessionMetric) error {
	s.samples = append(s.samples, sample)
	if sample.Phase == s.failPhase {
		return s.err
	}
	return nil
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
	if s.agents == nil {
		return mocks.NewNodeAgent(fixtures.MakeNode(fixtures.WithNodeID(nodeID))), nil
	}
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

type presetLoadAgent struct {
	node      domain.Node
	instances map[string]domain.ModelInstance
	loads     []domain.LoadRequest
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

func (n *presetLoadAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n *presetLoadAgent) Load(_ context.Context, req domain.LoadRequest) (domain.ModelInstance, error) {
	n.loads = append(n.loads, req)
	inst, ok := n.instances[req.Preset.ID]
	if !ok {
		return domain.ModelInstance{}, fmt.Errorf("no instance for preset %q", req.Preset.ID)
	}
	return inst, nil
}

func (n *presetLoadAgent) Unload(context.Context, string) error {
	return nil
}

func (n *presetLoadAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n *presetLoadAgent) BeginRequest(context.Context, string) error {
	return nil
}

func (n *presetLoadAgent) EndRequest(context.Context, string) error {
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

type repeatReader struct {
	b byte
}

func (r repeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}
	return len(p), nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

type chunkThenErrorReadCloser struct {
	chunk []byte
	err   error
	read  bool
}

func (r *chunkThenErrorReadCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, r.err
	}
	r.read = true
	return copy(p, r.chunk), nil
}

func (r *chunkThenErrorReadCloser) Close() error {
	return nil
}

type gatewayRuntimeStore struct {
	deletedLeases  []string
	leases         []domain.Lease
	jobs           []domain.Job
	services       []domain.ModelService
	reconcileTasks []domain.ReconciliationTaskRecord
	deleteLeaseErr error
	listLeasesErr  error
}

func (s *gatewayRuntimeStore) SaveJob(_ context.Context, job domain.Job) error {
	s.jobs = append(s.jobs, job)
	return nil
}

func (s *gatewayRuntimeStore) Job(_ context.Context, id string) (domain.Job, error) {
	for i := len(s.jobs) - 1; i >= 0; i-- {
		if s.jobs[i].ID == id {
			return s.jobs[i], nil
		}
	}
	return domain.Job{}, sql.ErrNoRows
}

func (s *gatewayRuntimeStore) ListJobs(context.Context) ([]domain.Job, error) {
	return append([]domain.Job(nil), s.jobs...), nil
}

func (s *gatewayRuntimeStore) SaveLease(_ context.Context, lease domain.Lease) error {
	s.leases = append(s.leases, lease)
	return nil
}

func (s *gatewayRuntimeStore) ListLeases(context.Context) ([]domain.Lease, error) {
	if s.listLeasesErr != nil {
		return nil, s.listLeasesErr
	}
	return append([]domain.Lease(nil), s.leases...), nil
}

func (s *gatewayRuntimeStore) DeleteLease(_ context.Context, id string) error {
	if s.deleteLeaseErr != nil {
		return s.deleteLeaseErr
	}
	s.deletedLeases = append(s.deletedLeases, id)
	for i, lease := range s.leases {
		if lease.ID == id {
			s.leases = append(s.leases[:i], s.leases[i+1:]...)
			break
		}
	}
	return nil
}

func (s *gatewayRuntimeStore) ListModelServices(_ context.Context) ([]domain.ModelService, error) {
	return append([]domain.ModelService(nil), s.services...), nil
}

func (s *gatewayRuntimeStore) SaveModelService(_ context.Context, service domain.ModelService) error {
	for i := range s.services {
		if s.services[i].ID == service.ID {
			s.services[i] = service
			return nil
		}
	}
	s.services = append(s.services, service)
	return nil
}

func (s *gatewayRuntimeStore) SaveInstance(context.Context, domain.ModelInstance) error {
	return nil
}

func (s *gatewayRuntimeStore) DeleteInstance(context.Context, string) error {
	return nil
}

func (s *gatewayRuntimeStore) ListReconciliationTasks(_ context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	var out []domain.ReconciliationTaskRecord
	for _, task := range s.reconcileTasks {
		if jobID == "" || task.JobID == jobID {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *gatewayRuntimeStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	for i := range s.reconcileTasks {
		if s.reconcileTasks[i].ID == task.ID {
			s.reconcileTasks[i] = task
			return nil
		}
	}
	s.reconcileTasks = append(s.reconcileTasks, task)
	return nil
}

func (s *gatewayRuntimeStore) sawJobStatus(status domain.JobStatus) bool {
	for _, job := range s.jobs {
		if job.Status == status {
			return true
		}
	}
	return false
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
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

func (notClaimedCoordinator) MarkRunning(context.Context, string) error {
	return domain.ErrUnsupported
}

func (notClaimedCoordinator) Release(_ context.Context, jobID string) error {
	return fmt.Errorf("job %q is not claimed by this coordinator", jobID)
}

func (notClaimedCoordinator) Complete(context.Context, string) error {
	return domain.ErrUnsupported
}

func (notClaimedCoordinator) Fail(context.Context, string, error) error {
	return domain.ErrUnsupported
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

func TestRouterRouteHoldsWorkloadLeaseForReuse(t *testing.T) {
	preset := fixtures.MakePreset()
	served := 0
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("held route ok")))
	}))
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	node := fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID))
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	metricErr := errors.New("owner telemetry metrics timed out")
	sink := &mocks.TelemetrySink{Err: metricErr}
	router.Telemetry = sink
	router.SelfNodeID = inst.NodeID
	store := &gatewayRuntimeStore{}
	lease := domain.Lease{ID: "lease-held", JobID: "workload-held", NodeID: inst.NodeID, InstanceID: inst.ID}
	job := domain.Job{
		ID:      "workload-held",
		Model:   preset.ModelRef,
		Project: "project-a",
		Status:  domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-held", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	store.jobs = []domain.Job{job}
	store.leases = []domain.Lease{lease}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	makeReq := func() translate.IngressRequest {
		req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
		if err != nil {
			t.Fatalf("ParseOpenAIChat: %v", err)
		}
		req.Project = "project-a"
		req.WorkloadID = job.ID
		req.LeaseID = lease.ID
		req.HoldLease = true
		return req
	}

	// Two consecutive requests must both ride the held lease: the workload
	// stays running and the lease is never deleted.
	for attempt := 1; attempt <= 2; attempt++ {
		resp, err := router.Route(context.Background(), makeReq())
		if err != nil {
			t.Fatalf("Route attempt %d with held lease: %v", attempt, err)
		}
		if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "held route ok") {
			t.Fatalf("attempt %d resp = %+v body=%s", attempt, resp, resp.Body)
		}
		if resp.Header.Get(HeaderLeaseHold) != "true" {
			t.Fatalf("attempt %d missing %s response header", attempt, HeaderLeaseHold)
		}
		if got := resp.Header.Get(HeaderTelemetryWarning); got != "completion_telemetry_failed" {
			t.Fatalf("attempt %d telemetry warning = %q", attempt, got)
		}
	}
	if served != 2 {
		t.Fatalf("upstream served %d, want 2", served)
	}
	if store.sawJobStatus(domain.JobDone) {
		t.Fatalf("held workload must not be finished: jobs=%+v", store.jobs)
	}
	if len(store.deletedLeases) != 0 {
		t.Fatalf("held lease must not be deleted: %+v", store.deletedLeases)
	}
	if len(sink.Metrics) != 0 {
		t.Fatalf("metric writes should have failed: %+v", sink.Metrics)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test,begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterHeldLeaseWarnsOnRemoteSetupTelemetryFailure(t *testing.T) {
	preset := fixtures.MakePreset()
	served := 0
	upstream := directUpstream(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("held route ok")))
	}))
	inst := fixtures.MakeInstance(fixtures.OnNode("remote-node"))
	inst.Addr = upstream
	node := fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID))
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	router.Telemetry = &mocks.TelemetrySink{}
	router.SelfNodeID = "local-node"
	router.TelemetryPeers = peerMap{"remote-node": {ID: "peer-remote", Addresses: []string{"127.0.0.1:62000"}, Compute: true}}
	router.TelemetryPeerClient = &mocks.TelemetryPeerClient{PushSamplesErr: errors.New("remote samples timeout")}
	store := &gatewayRuntimeStore{}
	lease := domain.Lease{ID: "lease-held", JobID: "workload-held", NodeID: inst.NodeID, InstanceID: inst.ID}
	job := domain.Job{
		ID:      "workload-held",
		Model:   preset.ModelRef,
		Project: "project-a",
		Status:  domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-held", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	store.jobs = []domain.Job{job}
	store.leases = []domain.Lease{lease}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.WorkloadID = job.ID
	req.LeaseID = lease.ID
	req.HoldLease = true

	resp, err := router.Route(context.Background(), req)
	if err != nil {
		t.Fatalf("Route with held lease: %v", err)
	}
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), "held route ok") {
		t.Fatalf("resp = %+v body=%s", resp, resp.Body)
	}
	if got := resp.Header.Get(HeaderTelemetryWarning); got != "completion_telemetry_failed" {
		t.Fatalf("telemetry warning = %q", got)
	}
	if served != 1 {
		t.Fatalf("upstream served %d, want 1", served)
	}
	if store.sawJobStatus(domain.JobDone) || store.sawJobStatus(domain.JobFailed) {
		t.Fatalf("held workload must remain running after telemetry failure: jobs=%+v", store.jobs)
	}
	if len(store.deletedLeases) != 0 {
		t.Fatalf("held lease must not be deleted: %+v", store.deletedLeases)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterHeldLeaseClientCancelDoesNotFailWorker(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance()
	inst.Addr = "http://upstream.test"
	node := fixtures.MakeNode(fixtures.WithNodeID(inst.NodeID))
	agent := mocks.NewNodeAgent(node)
	router := newTestRouter(preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{inst},
	}, staticResolver{agents: map[string]ports.NodeAgent{inst.NodeID: agent}})
	router.Client = &http.Client{Transport: gatewayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	store := &gatewayRuntimeStore{}
	lease := domain.Lease{ID: "lease-held", JobID: "workload-held", NodeID: inst.NodeID, InstanceID: inst.ID}
	job := domain.Job{
		ID:      "workload-held",
		Model:   preset.ModelRef,
		Project: "project-a",
		Status:  domain.JobRunning,
		Result: &domain.JobResult{
			Decision: domain.PlacementDecision{JobID: "workload-held", Action: domain.ActionWarmInstance, NodeID: inst.NodeID, InstanceID: inst.ID, Preset: preset},
			Instance: inst,
			LeaseID:  lease.ID,
			NodeID:   inst.NodeID,
			PresetID: preset.ID,
		},
	}
	store.jobs = []domain.Job{job}
	store.leases = []domain.Lease{lease}
	router.Runtime = &scheduler.Service{Placer: router.Placer, Fleet: router.Fleet, Nodes: router.Nodes, Store: store, Queue: scheduler.NewQueue(router.Clock), Clock: router.Clock}

	req, err := translate.ParseOpenAIChat([]byte(`{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`))
	if err != nil {
		t.Fatalf("ParseOpenAIChat: %v", err)
	}
	req.Project = "project-a"
	req.WorkloadID = job.ID
	req.LeaseID = lease.ID
	req.HoldLease = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = router.Route(ctx, req)
	var statusErr interface {
		StatusCode() int
	}
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != statusClientClosedRequest {
		t.Fatalf("Route err = %v", err)
	}
	if !store.sawJobStatus(domain.JobRunning) {
		t.Fatalf("held workload should remain running after client cancel: jobs=%+v", store.jobs)
	}
	if len(store.deletedLeases) != 0 {
		t.Fatalf("held lease must not be deleted after client cancel: %+v", store.deletedLeases)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestParseRequestParsesLeaseHoldHeader(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"user","content":"hi"}]}`

	held := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	held.Header.Set(HeaderWorkload, "workload-a")
	held.Header.Set(HeaderLease, "lease-a")
	held.Header.Set(HeaderLeaseHold, "true")
	got, err := parseRequestWithControlHeaders(held, false)
	if err != nil || !got.HoldLease {
		t.Fatalf("hold parse = %+v %v", got, err)
	}

	explicitOff := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	explicitOff.Header.Set(HeaderWorkload, "workload-a")
	explicitOff.Header.Set(HeaderLease, "lease-a")
	explicitOff.Header.Set(HeaderLeaseHold, "false")
	got, err = parseRequestWithControlHeaders(explicitOff, false)
	if err != nil || got.HoldLease {
		t.Fatalf("explicit false parse = %+v %v", got, err)
	}

	invalid := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	invalid.Header.Set(HeaderWorkload, "workload-a")
	invalid.Header.Set(HeaderLease, "lease-a")
	invalid.Header.Set(HeaderLeaseHold, "maybe")
	if _, err := parseRequestWithControlHeaders(invalid, false); err == nil || !strings.Contains(err.Error(), "Lease-Hold") {
		t.Fatalf("invalid hold err = %v", err)
	}

	missingLease := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	missingLease.Header.Set(HeaderLeaseHold, "true")
	if _, err := parseRequestWithControlHeaders(missingLease, false); err == nil || !strings.Contains(err.Error(), "requires X-Myc-Workload") {
		t.Fatalf("hold without lease err = %v", err)
	}
}
