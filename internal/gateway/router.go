package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/ports"
)

type FleetSource interface {
	Snapshot(ctx context.Context) (domain.FleetSnapshot, error)
}

type NodeResolver interface {
	NodeAgent(nodeID string) (ports.NodeAgent, error)
}

type FailureReporter interface {
	ReportInstanceFailure(ctx context.Context, instanceID string, err error)
}

type PresetRegistry struct {
	byModel map[string]domain.Preset
	byID    map[string]domain.Preset
}

func NewPresetRegistry(presets ...domain.Preset) PresetRegistry {
	r := PresetRegistry{byModel: map[string]domain.Preset{}, byID: map[string]domain.Preset{}}
	for _, preset := range presets {
		r.byID[preset.ID] = preset
		r.byModel[preset.ModelRef] = preset
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

type Router struct {
	Placer   ports.Placer
	Fleet    FleetSource
	Nodes    NodeResolver
	Presets  PresetRegistry
	Profiles profiles.Registry
	Client   *http.Client
	Reporter FailureReporter
	MaxTries int
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
		decision, err := r.Placer.Place(ctx, domain.Job{
			ID:        fmt.Sprintf("gateway-%s-%d", req.Model, attempt),
			TaskType:  "chat",
			Model:     req.Model,
			Project:   "gateway",
			Priority:  domain.PriorityInteractive,
			SpeedPref: domain.SpeedThroughput,
			Streaming: req.Stream,
		}, fleet)
		if err != nil {
			return RouteResponse{}, err
		}
		inst, cold, err := r.resolveInstance(ctx, decision, preset, fleet)
		if err != nil {
			return RouteResponse{}, err
		}
		route, err := translate.BuildUpstream(req, profile)
		if err != nil {
			return RouteResponse{}, err
		}
		resp, err := r.callUpstream(ctx, inst, route)
		if err != nil || resp.Status >= 500 {
			if err == nil {
				err = fmt.Errorf("upstream returned %d", resp.Status)
			}
			lastErr = err
			r.reportFailure(ctx, inst.ID, err)
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		if resp.Status >= 400 {
			return RouteResponse{}, fmt.Errorf("upstream returned %d: %s", resp.Status, strings.TrimSpace(string(resp.Body)))
		}
		body, contentType, err := translate.TranslateResponse(req, route, resp.Body)
		if err != nil {
			return RouteResponse{}, err
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

func (r *Router) profileFor(preset domain.Preset) (profiles.Profile, error) {
	registry := r.Profiles
	if registry.IsZero() {
		registry = profiles.DefaultRegistry()
	}
	return registry.ForBackend(preset.Backend)
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
	client := r.Client
	if client == nil {
		client = http.DefaultClient
	}
	url := joinURL(inst.Addr, route.Path)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(route.Body))
	if err != nil {
		return upstreamResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
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

func (r *Router) reportFailure(ctx context.Context, instanceID string, err error) {
	if r.Reporter != nil {
		r.Reporter.ReportInstanceFailure(ctx, instanceID, err)
	}
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
