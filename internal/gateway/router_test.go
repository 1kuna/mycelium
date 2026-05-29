package gateway

import (
	"context"
	"errors"
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
	if len(sink.Metrics) != 1 || sink.Metrics[0].Project != "proj-a" || sink.Metrics[0].ContextUsed != 4 || sink.Metrics[0].PresetID != inst.PresetID || sink.Metrics[0].Backend != domain.BackendLlamaCpp {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	if want := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC); !sink.Metrics[0].At.Equal(want) {
		t.Fatalf("metric time = %s want %s", sink.Metrics[0].At, want)
	}
	if strings.Join(agent.Calls, ",") != "begin:inst_test,end:inst_test" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
}

func TestRouterUsesProjectDefaultModel(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !strings.Contains(string(body), `"model":"`+preset.ID+`"`) {
			t.Fatalf("body = %s", body)
		}
		_, _ = w.Write([]byte(openAIChatBody("defaulted")))
	}))
	defer upstream.Close()
	inst := fixtures.MakeInstance(fixtures.WithInstancePreset(preset.ID))
	inst.Addr = upstream.URL
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

func TestRouterStreamColdLoadWritesLoadingReadyAndChunks(t *testing.T) {
	preset := fixtures.MakePreset()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: one\n\n"))
		_, _ = w.Write([]byte("data: two\n\n"))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_stream"))
	inst.Addr = upstream.URL
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Upstream", "ok")
		_, _ = w.Write([]byte("data: warm\n\n"))
	}))
	defer upstream.Close()

	inst := fixtures.MakeInstance()
	inst.Addr = upstream.URL
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad input", http.StatusBadRequest)
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_bad"))
	inst.Addr = upstream.URL
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
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: rescued\n\n"))
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("runtime")))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_runtime"))
	inst.Addr = upstream.URL
	resolver := staticResolver{agents: map[string]ports.NodeAgent{node.ID: loadNode{node: node, inst: inst}}}
	fleet := staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	router := newTestRouter(preset, fleet.fleet, resolver)
	router.Fleet = fleet
	router.Nodes = resolver
	router.Runtime = &scheduler.Service{
		Placer:  router.Placer,
		Fleet:   fleet,
		Nodes:   resolver,
		Queue:   scheduler.NewQueue(router.Clock),
		Store:   &gatewayRuntimeStore{},
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
}

func TestRouterStreamRetriesContextOverflowBeforeResponseStarts(t *testing.T) {
	small := fixtures.MakePreset(fixtures.WithPresetID("preset_small"), fixtures.WithContextLength(2048))
	large := fixtures.MakePreset(fixtures.WithPresetID("preset_large"), fixtures.WithContextLength(8192))
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request exceeds context window", http.StatusBadRequest)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: enlarged\n\n"))
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data: should-not-run\n\n"))
	}))
	defer upstream.Close()
	inst.Addr = upstream.URL
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance()
	inst.Addr = upstream.URL
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: server-stream\n\n"))
	}))
	defer upstream.Close()

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_server_stream"))
	inst.Addr = upstream.URL
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
	err   error
}

func (s staticFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	if s.err != nil {
		return domain.FleetSnapshot{}, s.err
	}
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

type errorLoadNode struct {
	err error
}

func (n errorLoadNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{}, nil
}

func (n errorLoadNode) Load(context.Context, domain.Preset) (domain.ModelInstance, error) {
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

func (r *testFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) {
	r.failed = append(r.failed, instanceID)
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

type gatewayRuntimeStore struct{}

func (s *gatewayRuntimeStore) SaveJob(context.Context, domain.Job) error {
	return nil
}

func (s *gatewayRuntimeStore) SaveLease(context.Context, domain.Lease) error {
	return nil
}

func (s *gatewayRuntimeStore) SaveInstance(context.Context, domain.ModelInstance) error {
	return nil
}

func (s *gatewayRuntimeStore) DeleteInstance(context.Context, string) error {
	return nil
}

func openAIChatBody(text string) string {
	return `{"id":"chatcmpl-test","model":"qwen2.5-9b-instruct","choices":[{"index":0,"message":{"role":"assistant","content":"` + text + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
}
