package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/optimizer"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/pkg/api"
)

type FleetSource interface {
	Snapshot(ctx context.Context) (domain.FleetSnapshot, error)
}

type NodeResolver interface {
	NodeAgent(nodeID string) (ports.NodeAgent, error)
}

type FailureReporter interface {
	ReportInstanceFailure(ctx context.Context, instanceID string, err error) error
}

type PresetRegistry struct {
	byModel map[string]domain.Preset
	byID    map[string]domain.Preset
	all     []domain.Preset
}

func NewPresetRegistry(presets ...domain.Preset) PresetRegistry {
	r := PresetRegistry{byModel: map[string]domain.Preset{}, byID: map[string]domain.Preset{}, all: append([]domain.Preset(nil), presets...)}
	for _, preset := range presets {
		r.byID[preset.ID] = preset
		indexPresetModels(r.byModel, preset)
	}
	return r
}

func (r PresetRegistry) Resolve(model string) (domain.Preset, error) {
	if preset, ok := r.byID[model]; ok {
		return preset, nil
	}
	if preset, ok := r.byModel[model]; ok {
		return preset, nil
	}
	return domain.Preset{}, fmt.Errorf("unknown model %q", model)
}

func indexPresetModels(index map[string]domain.Preset, preset domain.Preset) {
	for _, model := range append([]string{preset.ModelRef}, preset.Aliases...) {
		if model == "" {
			continue
		}
		if _, exists := index[model]; !exists {
			index[model] = preset
		}
	}
}

func (r PresetRegistry) NextLargerContext(current domain.Preset) (domain.Preset, bool) {
	var best domain.Preset
	for _, preset := range r.all {
		if preset.ID == current.ID || preset.Backend != current.Backend || preset.ModelRef != current.ModelRef || preset.ContextLength <= current.ContextLength {
			continue
		}
		if best.ID == "" || preset.ContextLength < best.ContextLength {
			best = preset
		}
	}
	return best, best.ID != ""
}

type Router struct {
	Placer         ports.Placer
	Fleet          FleetSource
	Nodes          NodeResolver
	Presets        PresetRegistry
	Profiles       profiles.Registry
	Client         *http.Client
	Reporter       FailureReporter
	Runtime        *scheduler.Service
	Telemetry      ports.TelemetrySink
	Clock          ports.Clock
	Sticky         *StickyTable
	Projects       map[string]domain.Project
	DefaultProject string
	MaxTries       int
}

type RouteResponse struct {
	Status   int
	Header   http.Header
	Body     []byte
	Decision domain.PlacementDecision
	Instance domain.ModelInstance
	Profile  profiles.Profile
	Attempts int
	ColdLoad bool
}

func (r *Router) Route(ctx context.Context, req translate.IngressRequest) (RouteResponse, error) {
	if r.Placer == nil || r.Fleet == nil || r.Nodes == nil {
		return RouteResponse{}, fmt.Errorf("gateway router is not configured")
	}
	req, err := r.applyRequestDefaults(req)
	if err != nil {
		return RouteResponse{}, err
	}
	preset, err := r.Presets.Resolve(req.Model)
	if err != nil {
		return RouteResponse{}, err
	}
	profile, err := r.profileFor(preset)
	if err != nil {
		return RouteResponse{}, err
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return RouteResponse{}, err
	}
	tries := r.MaxTries
	if tries == 0 {
		tries = 2
	}
	var lastErr error
	for attempt := 1; attempt <= tries; attempt++ {
		job := r.jobFromIngress(req, attempt)
		clk := r.clock()
		loadStart := clk.Now()
		decision, inst, cold, err := r.placeStickyOrLoad(ctx, req, job, preset, fleet, nil)
		if err != nil {
			return RouteResponse{}, err
		}
		loadMS := 0
		if cold {
			loadMS = durationMS(loadStart, clk.Now())
		}
		endRequest, err := r.beginInstanceRequest(ctx, inst)
		if err != nil {
			return RouteResponse{}, err
		}
		route, err := translate.BuildUpstream(req, profile)
		if err != nil {
			endRequest()
			return RouteResponse{}, err
		}
		upstreamStart := clk.Now()
		resp, err := r.callUpstream(ctx, inst, route)
		upstreamEnd := clk.Now()
		endRequest()
		if err != nil || resp.Status >= 500 {
			if err == nil {
				err = fmt.Errorf("upstream returned %d", resp.Status)
			}
			lastErr = err
			if reportErr := r.reportFailure(ctx, inst.ID, err); reportErr != nil {
				return RouteResponse{}, reportErr
			}
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		if resp.Status >= 400 {
			bodyText := strings.TrimSpace(string(resp.Body))
			if optimizer.IsContextOverflow(preset.Backend, fmt.Errorf("%s", bodyText)) {
				next, ok := r.Presets.NextLargerContext(preset)
				if ok {
					lastErr = fmt.Errorf("context overflow on %s; retrying with %s", preset.ID, next.ID)
					req.Model = next.ID
					preset = next
					continue
				}
			}
			return RouteResponse{}, fmt.Errorf("upstream returned %d: %s", resp.Status, bodyText)
		}
		body, contentType, err := translate.TranslateResponse(req, route, resp.Body)
		if err != nil {
			return RouteResponse{}, err
		}
		r.recordMetric(ctx, job, preset, inst, body, metricTiming{
			Start:           upstreamStart,
			FirstByte:       upstreamEnd,
			End:             upstreamEnd,
			LoadWallClockMS: loadMS,
		})
		if r.Sticky != nil {
			r.Sticky.Put(req.ConversationKey, inst)
		}
		headers := cloneHeader(resp.Header)
		if contentType != "" {
			headers.Set("Content-Type", contentType)
		}
		writeDecisionHeaders(headers, decision, inst, profile, attempt)
		if cold && req.Stream {
			headers.Set("Content-Type", "text/event-stream")
			headers.Set("X-Accel-Buffering", "no")
			body = append(loadingEvent(decision, inst), body...)
		}
		return RouteResponse{
			Status:   resp.Status,
			Header:   headers,
			Body:     body,
			Decision: decision,
			Instance: inst,
			Profile:  profile,
			Attempts: attempt,
			ColdLoad: cold,
		}, nil
	}
	return RouteResponse{}, fmt.Errorf("gateway failover exhausted: %w", lastErr)
}

func (r *Router) Stream(ctx context.Context, req translate.IngressRequest, w http.ResponseWriter) error {
	if r.Placer == nil || r.Fleet == nil || r.Nodes == nil {
		return fmt.Errorf("gateway router is not configured")
	}
	req, err := r.applyRequestDefaults(req)
	if err != nil {
		return err
	}
	preset, err := r.Presets.Resolve(req.Model)
	if err != nil {
		return err
	}
	profile, err := r.profileFor(preset)
	if err != nil {
		return err
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return err
	}
	tries := r.MaxTries
	if tries == 0 {
		tries = 2
	}
	started := false
	var lastErr error
	for attempt := 1; attempt <= tries; attempt++ {
		job := r.jobFromIngress(req, attempt)
		clk := r.clock()
		loadStart := clk.Now()
		beforeCold := func(ctx context.Context, decision domain.PlacementDecision) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if started {
				return nil
			}
			headers := w.Header()
			headers.Set("Content-Type", "text/event-stream")
			headers.Set("X-Accel-Buffering", "no")
			writeDecisionHeaders(headers, decision, domain.ModelInstance{NodeID: decision.NodeID}, profile, attempt)
			w.WriteHeader(http.StatusOK)
			started = true
			if _, err := w.Write(loadingEvent(decision, domain.ModelInstance{NodeID: decision.NodeID})); err != nil {
				return err
			}
			flush(w)
			return nil
		}
		decision, inst, cold, err := r.placeStickyOrLoad(ctx, req, job, preset, fleet, beforeCold)
		if err != nil {
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		if started && cold {
			if _, err := w.Write(readyEvent(decision, inst)); err != nil {
				return err
			}
			flush(w)
		}
		loadMS := 0
		if cold {
			loadMS = durationMS(loadStart, clk.Now())
		}
		endRequest, err := r.beginInstanceRequest(ctx, inst)
		if err != nil {
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		route, err := translate.BuildUpstream(req, profile)
		if err != nil {
			endRequest()
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		if route.Translate {
			endRequest()
			err := fmt.Errorf("translated streaming responses are not supported")
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		upstreamStart := clk.Now()
		resp, err := r.doUpstream(ctx, inst, route)
		if err != nil {
			endRequest()
			lastErr = err
			if reportErr := r.reportFailure(ctx, inst.ID, err); reportErr != nil {
				err = reportErr
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			if err != lastErr {
				return err
			}
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		if resp.StatusCode >= 400 {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			endRequest()
			if readErr != nil {
				err = readErr
			} else {
				err = fmt.Errorf("upstream returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			if resp.StatusCode >= 500 {
				lastErr = err
				if reportErr := r.reportFailure(ctx, inst.ID, err); reportErr != nil {
					err = reportErr
				}
				if started {
					writeStreamError(w, err)
					return nil
				}
				if err != lastErr {
					return err
				}
				fleet = withoutInstance(fleet, inst.ID)
				continue
			}
			if optimizer.IsContextOverflow(preset.Backend, err) {
				next, ok := r.Presets.NextLargerContext(preset)
				if ok && !started {
					lastErr = fmt.Errorf("context overflow on %s; retrying with %s", preset.ID, next.ID)
					req.Model = next.ID
					preset = next
					continue
				}
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		if !started {
			headers := cloneHeader(resp.Header)
			if headers.Get("Content-Type") == "" {
				headers.Set("Content-Type", "text/event-stream")
			}
			headers.Set("X-Accel-Buffering", "no")
			writeResponseHeaders(w.Header(), headers)
			writeDecisionHeaders(w.Header(), decision, inst, profile, attempt)
			w.WriteHeader(resp.StatusCode)
			started = true
		}
		copied, copyErr := copyAndFlush(w, resp.Body, clk)
		_ = resp.Body.Close()
		endRequest()
		if copyErr != nil {
			return copyErr
		}
		r.recordMetric(ctx, job, preset, inst, copied.Body, metricTiming{
			Start:           upstreamStart,
			FirstByte:       copied.FirstByte,
			End:             copied.End,
			LoadWallClockMS: loadMS,
		})
		if r.Sticky != nil {
			r.Sticky.Put(req.ConversationKey, inst)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no stream placement attempts were made")
	}
	return fmt.Errorf("gateway failover exhausted: %w", lastErr)
}

func (r *Router) placeStickyOrLoad(ctx context.Context, req translate.IngressRequest, job domain.Job, preset domain.Preset, fleet domain.FleetSnapshot, beforeCold func(context.Context, domain.PlacementDecision) error) (domain.PlacementDecision, domain.ModelInstance, bool, error) {
	if r.Sticky != nil {
		if inst, ok := r.Sticky.Get(req.ConversationKey, preset, fleet); ok {
			return domain.PlacementDecision{
				JobID:          job.ID,
				InstanceID:     inst.ID,
				NodeID:         inst.NodeID,
				AcceleratorSet: append([]int(nil), inst.AcceleratorSet...),
				Claim:          inst.Claim,
				Action:         domain.ActionWarmInstance,
				Trace: []domain.TraceStep{{
					Step:   "sticky",
					Result: "conversation affinity selected warm instance",
				}},
			}, inst, false, nil
		}
	}
	return r.placeAndLoad(ctx, job, preset, fleet, beforeCold)
}

func (r *Router) placeAndLoad(ctx context.Context, job domain.Job, preset domain.Preset, fleet domain.FleetSnapshot, beforeCold func(context.Context, domain.PlacementDecision) error) (domain.PlacementDecision, domain.ModelInstance, bool, error) {
	if r.Runtime != nil {
		var hooks []scheduler.SubmitHooks
		if beforeCold != nil {
			hooks = append(hooks, scheduler.SubmitHooks{BeforeColdLoad: beforeCold})
		}
		result, err := r.Runtime.Submit(ctx, job, hooks...)
		if err != nil {
			return result.Decision, result.Instance, false, err
		}
		if result.Decision.Action == domain.ActionQueued {
			return result.Decision, result.Instance, false, fmt.Errorf("job %q queued: no instance available", job.ID)
		}
		cold := result.Decision.InstanceID == ""
		return result.Decision, result.Instance, cold, nil
	}
	decision, err := r.Placer.Place(ctx, job, fleet)
	if err != nil {
		return domain.PlacementDecision{}, domain.ModelInstance{}, false, err
	}
	if decision.InstanceID == "" && decision.Action != domain.ActionQueued && beforeCold != nil {
		if err := beforeCold(ctx, decision); err != nil {
			return decision, domain.ModelInstance{}, false, err
		}
	}
	inst, cold, err := r.resolveInstance(ctx, decision, preset, fleet)
	return decision, inst, cold, err
}

func (r *Router) jobFromIngress(req translate.IngressRequest, attempt int) domain.Job {
	project := req.Project
	if project == "" {
		project = r.DefaultProject
	}
	projectDefaults := domain.Project{}
	if project != "" {
		projectDefaults = r.Projects[project]
	}
	if project == "" {
		project = "gateway"
	}
	priority := req.Priority
	if priority == "" {
		priority = projectDefaults.Priority
	}
	if priority == "" {
		priority = domain.PriorityInteractive
	}
	speed := req.SpeedPref
	if speed == "" {
		speed = projectDefaults.SpeedPref
	}
	if speed == "" {
		speed = domain.SpeedThroughput
	}
	preemption := req.Preemption
	if preemption == "" {
		preemption = projectDefaults.Preemption
	}
	if preemption == "" || preemption == domain.PreemptInherit {
		preemption = domain.PreemptSoft
	}
	contextRequest := req.ContextRequest
	if contextRequest == 0 {
		contextRequest = projectDefaults.ContextCap
	}
	taskType := "chat"
	if req.Kind == translate.KindOpenAICompletion {
		taskType = "completion"
	}
	return domain.Job{
		ID:             fmt.Sprintf("gateway-%s-%d", req.Model, attempt),
		TaskType:       taskType,
		Model:          req.Model,
		Project:        project,
		Priority:       priority,
		SpeedPref:      speed,
		ContextRequest: contextRequest,
		Preemption:     preemption,
		Streaming:      req.Stream,
	}
}

func (r *Router) applyRequestDefaults(req translate.IngressRequest) (translate.IngressRequest, error) {
	project := req.Project
	if project == "" {
		project = r.DefaultProject
	}
	if req.Model == "" && project != "" {
		if defaults, ok := r.Projects[project]; ok && defaults.DefaultModel != "" {
			return translate.WithModel(req, defaults.DefaultModel)
		}
	}
	if req.Model == "" {
		return translate.IngressRequest{}, fmt.Errorf("model is required")
	}
	return req, nil
}

func (r *Router) profileFor(preset domain.Preset) (profiles.Profile, error) {
	registry := r.Profiles
	if registry.IsZero() {
		registry = profiles.DefaultRegistry()
	}
	return registry.ForBackend(preset.Backend)
}

func (r *Router) beginInstanceRequest(ctx context.Context, inst domain.ModelInstance) (func(), error) {
	agent, err := r.Nodes.NodeAgent(inst.NodeID)
	if err != nil {
		return func() {}, nil
	}
	if err := agent.BeginRequest(ctx, inst.ID); err != nil {
		return nil, err
	}
	return func() {
		_ = agent.EndRequest(context.Background(), inst.ID)
	}, nil
}

func (r *Router) resolveInstance(ctx context.Context, decision domain.PlacementDecision, preset domain.Preset, fleet domain.FleetSnapshot) (domain.ModelInstance, bool, error) {
	if decision.InstanceID != "" {
		for _, inst := range fleet.Instances {
			if inst.ID == decision.InstanceID {
				return inst, false, nil
			}
		}
		return domain.ModelInstance{}, false, fmt.Errorf("selected instance %q is missing from fleet snapshot", decision.InstanceID)
	}
	if decision.NodeID == "" {
		return domain.ModelInstance{}, false, fmt.Errorf("placement action %q did not select a node", decision.Action)
	}
	agent, err := r.Nodes.NodeAgent(decision.NodeID)
	if err != nil {
		return domain.ModelInstance{}, false, err
	}
	inst, err := agent.Load(ctx, preset)
	if err != nil {
		return domain.ModelInstance{}, false, err
	}
	return inst, true, nil
}

type upstreamResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r *Router) callUpstream(ctx context.Context, inst domain.ModelInstance, route translate.UpstreamRequest) (upstreamResponse, error) {
	resp, err := r.doUpstream(ctx, inst, route)
	if err != nil {
		return upstreamResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return upstreamResponse{}, err
	}
	return upstreamResponse{Status: resp.StatusCode, Header: resp.Header.Clone(), Body: body}, nil
}

func (r *Router) doUpstream(ctx context.Context, inst domain.ModelInstance, route translate.UpstreamRequest) (*http.Response, error) {
	client := r.Client
	if client == nil {
		client = http.DefaultClient
	}
	url := joinURL(inst.Addr, route.Path)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(route.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return client.Do(httpReq)
}

func (r *Router) reportFailure(ctx context.Context, instanceID string, err error) error {
	if r.Reporter != nil {
		return r.Reporter.ReportInstanceFailure(ctx, instanceID, err)
	}
	return nil
}

type metricTiming struct {
	Start           time.Time
	FirstByte       time.Time
	End             time.Time
	LoadWallClockMS int
}

func (r *Router) recordMetric(ctx context.Context, job domain.Job, preset domain.Preset, inst domain.ModelInstance, body []byte, timing metricTiming) {
	if r.Telemetry == nil {
		return
	}
	prompt, completion := usageFromBody(body)
	clk := r.clock()
	_ = r.Telemetry.Record(ctx, domain.RunMetric{
		JobID:           job.ID,
		InstanceID:      inst.ID,
		NodeID:          inst.NodeID,
		PresetID:        inst.PresetID,
		Backend:         preset.Backend,
		Project:         job.Project,
		ContextUsed:     prompt + completion,
		TokensPerSec:    tokensPerSecond(completion, timing.FirstByte, timing.End),
		TTFTms:          durationMS(timing.Start, timing.FirstByte),
		LoadWallClockMS: timing.LoadWallClockMS,
		At:              clk.Now().UTC(),
	})
}

func (r *Router) clock() ports.Clock {
	if r.Clock != nil {
		return r.Clock
	}
	return clock.System{}
}

func tokensPerSecond(tokens int, start, end time.Time) float64 {
	if tokens <= 0 || start.IsZero() || end.IsZero() || !end.After(start) {
		return 0
	}
	return float64(tokens) / end.Sub(start).Seconds()
}

func durationMS(start, end time.Time) int {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return int(end.Sub(start) / time.Millisecond)
}

func usageFromBody(body []byte) (int, int) {
	var openai api.OpenAIChatResponse
	if err := json.Unmarshal(body, &openai); err == nil && openai.Usage.TotalTokens != 0 {
		return openai.Usage.PromptTokens, openai.Usage.CompletionTokens
	}
	var anthropic api.AnthropicMessagesResponse
	if err := json.Unmarshal(body, &anthropic); err == nil && (anthropic.Usage.InputTokens != 0 || anthropic.Usage.OutputTokens != 0) {
		return anthropic.Usage.InputTokens, anthropic.Usage.OutputTokens
	}
	return 0, 0
}

func withoutInstance(fleet domain.FleetSnapshot, id string) domain.FleetSnapshot {
	out := domain.FleetSnapshot{Nodes: append([]domain.Node(nil), fleet.Nodes...)}
	for _, inst := range fleet.Instances {
		if inst.ID != id {
			out.Instances = append(out.Instances, inst)
		}
	}
	return out
}

func loadingEvent(decision domain.PlacementDecision, inst domain.ModelInstance) []byte {
	payload, err := json.Marshal(map[string]string{
		"action":      string(decision.Action),
		"node_id":     inst.NodeID,
		"instance_id": inst.ID,
	})
	if err != nil {
		panic(err)
	}
	return []byte("event: loading\ndata: " + string(payload) + "\n\n")
}

func readyEvent(decision domain.PlacementDecision, inst domain.ModelInstance) []byte {
	payload, err := json.Marshal(map[string]string{
		"action":      string(decision.Action),
		"node_id":     inst.NodeID,
		"instance_id": inst.ID,
	})
	if err != nil {
		panic(err)
	}
	return []byte("event: ready\ndata: " + string(payload) + "\n\n")
}

func writeStreamError(w http.ResponseWriter, err error) {
	payload, marshalErr := json.Marshal(map[string]string{"error": err.Error()})
	if marshalErr != nil {
		panic(marshalErr)
	}
	_, _ = w.Write([]byte("event: error\ndata: " + string(payload) + "\n\n"))
	flush(w)
}

type copyResult struct {
	Body      []byte
	FirstByte time.Time
	End       time.Time
}

func copyAndFlush(w http.ResponseWriter, r io.Reader, clk ports.Clock) (copyResult, error) {
	var body bytes.Buffer
	result := copyResult{}
	buf := make([]byte, 32*1024)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			now := clk.Now()
			if result.FirstByte.IsZero() {
				result.FirstByte = now
			}
			result.End = now
			chunk := buf[:n]
			body.Write(chunk)
			if _, err := w.Write(chunk); err != nil {
				result.Body = body.Bytes()
				return result, err
			}
			flush(w)
		}
		if readErr == io.EOF {
			if result.End.IsZero() {
				result.End = clk.Now()
			}
			result.Body = body.Bytes()
			return result, nil
		}
		if readErr != nil {
			result.Body = body.Bytes()
			return result, readErr
		}
	}
}

func writeResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func flush(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func joinURL(addr, path string) string {
	base := strings.TrimRight(addr, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return base + path
}

func cloneHeader(h http.Header) http.Header {
	if h == nil {
		return http.Header{}
	}
	out := h.Clone()
	out.Del("Content-Length")
	return out
}
