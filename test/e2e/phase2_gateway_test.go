package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
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

var directHTTP = &directHTTPRegistry{handlers: map[string]http.Handler{}}

type directHTTPRegistry struct {
	handlers map[string]http.Handler
}

type directHTTPServer struct {
	URL    string
	client *http.Client
}

func directURL(handler http.Handler) string {
	host := "e2e-" + string(rune('a'+len(directHTTP.handlers))) + ".mycelium.test"
	directHTTP.handlers[host] = handler
	return "http://" + host
}

func directClient() *http.Client {
	return &http.Client{Transport: directHTTP}
}

func (d *directHTTPRegistry) RoundTrip(req *http.Request) (*http.Response, error) {
	handler := d.handlers[req.URL.Host]
	if handler == nil {
		return nil, domain.ErrUnreachable
	}
	reader, writer := io.Pipe()
	rec := &streamingResponseWriter{
		header: make(http.Header),
		code:   http.StatusOK,
		ready:  make(chan struct{}),
		writer: writer,
	}
	go func() {
		handler.ServeHTTP(rec, req)
		rec.WriteHeader(http.StatusOK)
		_ = writer.Close()
	}()
	<-rec.ready
	rec.mu.Lock()
	code := rec.code
	header := rec.header.Clone()
	rec.mu.Unlock()
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Header:     header,
		Body:       reader,
		Request:    req,
	}, nil
}

type streamingResponseWriter struct {
	header http.Header
	code   int
	ready  chan struct{}
	mu     sync.Mutex
	wrote  bool
	once   sync.Once
	writer *io.PipeWriter
}

func (w *streamingResponseWriter) Header() http.Header {
	return w.header
}

func (w *streamingResponseWriter) WriteHeader(code int) {
	w.mu.Lock()
	if !w.wrote {
		w.code = code
		w.wrote = true
		w.once.Do(func() { close(w.ready) })
	}
	w.mu.Unlock()
}

func (w *streamingResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeader(w.code)
	return w.writer.Write(data)
}

func (w *streamingResponseWriter) Flush() {
	w.WriteHeader(w.code)
}

func (s *directHTTPServer) postJSON(t *testing.T, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, s.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("request %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	return resp
}

func TestPhase2GatewayRoutesOpenAIAndAnthropicWithHeaders(t *testing.T) {
	preset := fixtures.MakePreset()
	upstreamCalls := []string{}
	upstream := directURL(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls = append(upstreamCalls, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("hello")))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance()
	inst.Addr = upstream
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{inst}}}
	server := newGatewayTestServer(t, preset, fleet, staticNodeResolver{agents: map[string]ports.NodeAgent{node.ID: mocks.NewNodeAgent(node)}})

	resp := server.postJSON(t, "/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("openai status = %s", resp.Status)
	}
	if resp.Header.Get(gateway.HeaderDecision) != string(domain.ActionWarmInstance) || resp.Header.Get(gateway.HeaderInstance) != inst.ID {
		t.Fatalf("openai headers = %+v", resp.Header)
	}

	resp = server.postJSON(t, "/v1/messages", `{"model":"qwen2.5-9b-instruct","max_tokens":1,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`)
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
	upstream := directURL(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\n"))
	}))

	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_cold"))
	inst.Addr = upstream
	allowLoad := make(chan struct{})
	released := false
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}}
	server := newGatewayTestServer(t, preset, fleet, staticNodeResolver{agents: map[string]ports.NodeAgent{
		node.ID: blockingLoadNode{node: node, inst: inst, allow: allowLoad},
	}})
	defer func() {
		if !released {
			close(allowLoad)
		}
	}()

	resp := server.postJSON(t, "/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`)
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type = %s", resp.Header.Get("Content-Type"))
	}
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read first stream line: %v", err)
	}
	if first != "event: loading\n" {
		t.Fatalf("first stream line = %q", first)
	}
	close(allowLoad)
	released = true
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	body := first + string(rest)
	if !strings.Contains(body, "event: ready") || !strings.Contains(body, `"instance_id":"inst_cold"`) || !strings.Contains(body, "data: done") {
		t.Fatalf("stream body = %q", body)
	}
}

func TestPhase2GatewayFailoverReportsFailedInstance(t *testing.T) {
	preset := fixtures.MakePreset()
	first := directURL(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dead", http.StatusInternalServerError)
	}))
	second := directURL(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatBody("rescued")))
	}))

	node := fixtures.MakeNode()
	instA := fixtures.MakeInstance(fixtures.WithInstanceID("inst_a"))
	instA.Addr = first
	instB := fixtures.MakeInstance(fixtures.WithInstanceID("inst_b"))
	instB.Addr = second
	excluded := map[string]bool{}
	reporter := &recordingFailureReporter{excluded: excluded}
	fleet := staticGatewayFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{instA, instB}}, excluded: excluded}
	server := newGatewayTestServerWithReporter(t, preset, fleet, staticNodeResolver{agents: map[string]ports.NodeAgent{node.ID: mocks.NewNodeAgent(node)}}, reporter)

	resp := server.postJSON(t, "/v1/chat/completions", `{"model":"qwen2.5-9b-instruct","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`)
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

func newGatewayTestServer(t *testing.T, preset domain.Preset, fleet gateway.FleetSource, nodes gateway.NodeResolver) *directHTTPServer {
	t.Helper()
	return newGatewayTestServerWithReporter(t, preset, fleet, nodes, nil)
}

func newGatewayTestServerWithReporter(t *testing.T, preset domain.Preset, fleet gateway.FleetSource, nodes gateway.NodeResolver, reporter gateway.FailureReporter) *directHTTPServer {
	t.Helper()
	clk := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	placer := scheduler.NewPlacer(
		estimate.NewInMemory(),
		lease.NewAllocator(),
		clk,
		preset,
	)
	owners, ok := nodes.(scheduler.AdmissionResolver)
	if !ok {
		t.Fatalf("test node resolver does not expose admission controllers")
	}
	handler := gateway.Server{Router: &gateway.Router{
		Placer:   placer,
		Fleet:    fleet,
		Nodes:    nodes,
		Presets:  gateway.NewPresetRegistry(preset),
		Client:   directClient(),
		Reporter: reporter,
		Runtime: &scheduler.Service{
			Placer:  placer,
			Fleet:   fleet,
			Nodes:   nodes,
			Owners:  owners,
			Queue:   scheduler.NewQueue(clk),
			Store:   &peerRuntimeStore{},
			Clock:   clk,
			Presets: map[string]domain.Preset{preset.ID: preset, preset.ModelRef: preset},
		},
		MaxTries: 2,
	}}
	return &directHTTPServer{URL: directURL(handler), client: directClient()}
}

type staticGatewayFleet struct {
	fleet    domain.FleetSnapshot
	excluded map[string]bool
}

func (s staticGatewayFleet) Snapshot(context.Context) (domain.FleetSnapshot, error) {
	fleet := s.fleet
	if len(s.excluded) == 0 {
		return fleet, nil
	}
	fleet.Instances = nil
	for _, inst := range s.fleet.Instances {
		if !s.excluded[inst.ID] {
			fleet.Instances = append(fleet.Instances, inst)
		}
	}
	return fleet, nil
}

type staticNodeResolver struct {
	agents     map[string]ports.NodeAgent
	admissions map[string]ports.AdmissionController
}

func (s staticNodeResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, ok := s.agents[nodeID]
	if !ok {
		return nil, domain.ErrUnreachable
	}
	return agent, nil
}

func (s staticNodeResolver) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	if admission, ok := s.admissions[nodeID]; ok {
		return admission, nil
	}
	if _, ok := s.agents[nodeID]; !ok {
		return nil, domain.ErrUnreachable
	}
	return &mocks.AdmissionController{}, nil
}

type loadOnlyNode struct {
	node domain.Node
	inst domain.ModelInstance
}

func (n loadOnlyNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n loadOnlyNode) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
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

type blockingLoadNode struct {
	node  domain.Node
	inst  domain.ModelInstance
	allow <-chan struct{}
}

func (n blockingLoadNode) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: n.node}, nil
}

func (n blockingLoadNode) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
	<-n.allow
	return n.inst, nil
}

func (n blockingLoadNode) Unload(context.Context, string) error {
	return nil
}

func (n blockingLoadNode) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (n blockingLoadNode) BeginRequest(context.Context, string) error {
	return nil
}

func (n blockingLoadNode) EndRequest(context.Context, string) error {
	return nil
}

type recordingFailureReporter struct {
	failed   []string
	excluded map[string]bool
}

func (r *recordingFailureReporter) ReportInstanceFailure(_ context.Context, instanceID string, _ error) error {
	r.failed = append(r.failed, instanceID)
	if r.excluded != nil {
		r.excluded[instanceID] = true
	}
	return nil
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
