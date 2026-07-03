package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
	"mycelium/internal/gateway/translate"
	"mycelium/internal/modelruntime"
	"mycelium/internal/optimizer"
	"mycelium/internal/ports"
	"mycelium/internal/reconcile"
	"mycelium/internal/scheduler"
	"mycelium/pkg/api"
)

type FleetSource interface {
	Snapshot(ctx context.Context) (domain.FleetSnapshot, error)
}

type NodeResolver interface {
	NodeAgent(nodeID string) (ports.NodeAgent, error)
}

type TelemetryPeerResolver interface {
	PeerForNode(nodeID string) (domain.Peer, bool)
}

type InstanceMemorySampler interface {
	PeakMemoryMB(ctx context.Context, nodeID, instanceID string) (int, error)
}

type FailureReporter interface {
	ReportInstanceFailure(ctx context.Context, instanceID string, err error) error
}

type PresetRegistry struct {
	byModel map[string]domain.Preset
	byID    map[string]domain.Preset
	models  map[string][]domain.Preset
	all     []domain.Preset
}

func NewPresetRegistry(presets ...domain.Preset) PresetRegistry {
	r := PresetRegistry{byModel: map[string]domain.Preset{}, byID: map[string]domain.Preset{}, models: map[string][]domain.Preset{}, all: append([]domain.Preset(nil), presets...)}
	for _, preset := range presets {
		r.byID[preset.ID] = preset
		indexPresetModels(r.byModel, preset)
		indexPresetModelGroups(r.models, preset)
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

func (r PresetRegistry) ResolveAll(model string) []domain.Preset {
	if preset, ok := r.byID[model]; ok {
		return []domain.Preset{preset}
	}
	return append([]domain.Preset(nil), r.models[model]...)
}

func (r PresetRegistry) PublicModelIDs() []string {
	seen := map[string]struct{}{}
	var ids []string
	for _, preset := range r.all {
		addPublicModelID(&ids, seen, preset.ID)
		for _, alias := range preset.Aliases {
			addPublicModelID(&ids, seen, alias)
		}
		if len(preset.Aliases) == 0 && publicModelRef(preset.ModelRef) {
			addPublicModelID(&ids, seen, preset.ModelRef)
		}
	}
	return ids
}

func addPublicModelID(ids *[]string, seen map[string]struct{}, id string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	if _, ok := seen[id]; ok {
		return
	}
	seen[id] = struct{}{}
	*ids = append(*ids, id)
}

func publicModelRef(modelRef string) bool {
	if modelRef == "" || strings.ContainsAny(modelRef, `/\`) {
		return false
	}
	return true
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

func indexPresetModelGroups(index map[string][]domain.Preset, preset domain.Preset) {
	for _, model := range append([]string{preset.ModelRef}, preset.Aliases...) {
		if model == "" {
			continue
		}
		index[model] = append(index[model], preset)
	}
}

func (r PresetRegistry) NextLargerContext(current domain.Preset) (domain.Preset, bool) {
	return r.SmallestContextAtLeast(current, current.ContextLength+1)
}

func (r PresetRegistry) SmallestContextAtLeast(current domain.Preset, minContext int) (domain.Preset, bool) {
	var best domain.Preset
	for _, preset := range r.all {
		if preset.ID == current.ID || preset.Backend != current.Backend || preset.ModelRef != current.ModelRef || preset.ContextLength <= current.ContextLength || preset.ContextLength < minContext {
			continue
		}
		if best.ID == "" || preset.ContextLength < best.ContextLength {
			best = preset
		}
	}
	return best, best.ID != ""
}

func (r PresetRegistry) ContextLengths() []int {
	seen := map[int]struct{}{}
	var contexts []int
	for _, preset := range r.all {
		if preset.ContextLength <= 0 {
			continue
		}
		if _, ok := seen[preset.ContextLength]; ok {
			continue
		}
		seen[preset.ContextLength] = struct{}{}
		contexts = append(contexts, preset.ContextLength)
	}
	return contexts
}

type Router struct {
	Placer              ports.Placer
	Fleet               FleetSource
	Nodes               NodeResolver
	Presets             PresetRegistry
	Profiles            profiles.Registry
	Client              *http.Client
	Reporter            FailureReporter
	Runtime             *scheduler.Service
	Telemetry           ports.TelemetrySink
	TelemetryPeers      TelemetryPeerResolver
	TelemetryPeerClient ports.TelemetryPeerClient
	SelfNodeID          string
	MemorySampler       InstanceMemorySampler
	Clock               ports.Clock
	Projects            map[string]domain.Project
	DefaultProject      string
	MaxWaitDefaultMS    int
	MaxTries            int

	allowDirectPlacement bool
	jobPrefixOnce        sync.Once
	jobPrefix            string
	jobSeq               uint64
	sessionSeq           uint64
}

type RouteResponse struct {
	Status   int
	Header   http.Header
	Body     []byte
	Decision domain.PlacementDecision
	Instance domain.ModelInstance
	Lease    domain.Lease
	Profile  profiles.Profile
	Attempts int
	ColdLoad bool
}

// ExecutePlacedProvider runs a non-streaming request on an already placed
// instance. On success the caller owns terminal job completion/release so it can
// attach workload-specific result metadata first. On execution failure this
// method marks the placed job failed and releases its lease.
func (r *Router) ExecutePlacedProvider(ctx context.Context, req translate.IngressRequest, job domain.Job, decision domain.PlacementDecision, inst domain.ModelInstance, lease domain.Lease, attempt int, cold bool, loadMS int) (RouteResponse, error) {
	if req.Stream {
		return RouteResponse{}, fmt.Errorf("placed provider execution does not support streaming requests")
	}
	req, err := r.applyRequestDefaults(req)
	if err != nil {
		return RouteResponse{}, err
	}
	if err := r.validatePrivateRequest(req); err != nil {
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
	preset, profile, err = r.selectedPresetProfile(preset, profile, decision)
	if err != nil {
		return RouteResponse{}, err
	}
	route, err := translate.BuildUpstream(req, profile)
	if err != nil {
		return RouteResponse{}, err
	}
	recorder := r.newSessionRecorder()
	if err := recorder.emit(ctx, job, preset, inst, domain.TelemetryPhasePlaced, func(sample *domain.SessionMetric) {
		sample.BytesIn = len(req.Body)
		sample.LoadWallClockMS = loadMS
	}); err != nil {
		if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
			return RouteResponse{}, failErr
		}
		return RouteResponse{}, err
	}
	if cold {
		if err := recorder.emit(ctx, job, preset, inst, domain.TelemetryPhaseLoadReady, func(sample *domain.SessionMetric) {
			sample.BytesIn = len(req.Body)
			sample.LoadWallClockMS = loadMS
		}); err != nil {
			if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
				return RouteResponse{}, failErr
			}
			return RouteResponse{}, err
		}
	}
	endRequest, err := r.beginInstanceRequest(ctx, inst)
	if err != nil {
		if failErr := r.failPlacedJob(ctx, recorder, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, err); reportErr != nil {
			return RouteResponse{}, reportErr
		}
		return RouteResponse{}, err
	}
	clk := r.clock()
	upstreamStart := clk.Now()
	if err := recorder.emitAt(ctx, job, preset, inst, domain.TelemetryPhaseUpstreamStart, upstreamStart, func(sample *domain.SessionMetric) {
		sample.BytesIn = len(route.Body)
		sample.LoadWallClockMS = loadMS
	}); err != nil {
		endRequest()
		if failErr := r.failPlacedJob(ctx, nil, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		return RouteResponse{}, err
	}
	resp, err := r.callUpstream(ctx, inst, route)
	upstreamEnd := clk.Now()
	endRequest()
	if err != nil {
		if failErr := r.failPlacedJob(ctx, recorder, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, err); reportErr != nil {
			return RouteResponse{}, reportErr
		}
		return RouteResponse{}, err
	}
	headers := cloneHeader(resp.Header)
	if resp.Status >= 400 {
		bodyText := strings.TrimSpace(string(resp.Body))
		statusErr := fmt.Errorf("upstream returned %d: %s", resp.Status, bodyText)
		if failErr := r.failPlacedJob(ctx, recorder, job, preset, inst, lease, statusErr); failErr != nil {
			return RouteResponse{}, errors.Join(statusErr, failErr)
		}
		if resp.Status >= 500 {
			if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, statusErr); reportErr != nil {
				return RouteResponse{}, reportErr
			}
		}
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/json")
		}
		writeRouteDiagnostics(headers, routeDiagnosticsFor(req, preset, profile, cold, loadMS, 0, durationMS(upstreamStart, upstreamEnd)))
		writeDecisionHeaders(headers, decision, inst, profile, attempt)
		body := resp.Body
		if len(body) == 0 {
			body = []byte(fmt.Sprintf(`{"error":%q}`, statusErr.Error()))
		}
		return RouteResponse{Status: resp.Status, Header: headers, Body: body, Decision: decision, Instance: inst, Lease: lease, Profile: profile, Attempts: attempt, ColdLoad: cold}, statusErr
	}
	body, contentType, err := translate.TranslateResponse(req, route, resp.Body)
	if err != nil {
		if failErr := r.failPlacedJob(ctx, nil, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		return RouteResponse{}, err
	}
	body, err = normalizeOpenAIReasoningBoundary(req, preset, body)
	if err != nil {
		if failErr := r.failPlacedJob(ctx, nil, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		return RouteResponse{}, err
	}
	metric, err := r.recordMetric(ctx, job, preset, inst, body, metricTiming{
		Start:           upstreamStart,
		FirstByte:       upstreamEnd,
		End:             upstreamEnd,
		LoadWallClockMS: loadMS,
	})
	if err != nil {
		if failErr := r.failPlacedJob(ctx, nil, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		return RouteResponse{}, err
	}
	promptTokens, completionTokens := usageFromBody(body)
	if err := recorder.emitMetric(ctx, metric, domain.TelemetryPhaseComplete, len(route.Body), len(body), promptTokens, completionTokens); err != nil {
		if failErr := r.failPlacedJob(ctx, nil, job, preset, inst, lease, err); failErr != nil {
			return RouteResponse{}, errors.Join(err, failErr)
		}
		return RouteResponse{}, err
	}
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	writeRouteDiagnostics(headers, routeDiagnosticsFor(req, preset, profile, cold, loadMS, 0, durationMS(upstreamStart, upstreamEnd)))
	writeDecisionHeaders(headers, decision, inst, profile, attempt)
	return RouteResponse{Status: resp.Status, Header: headers, Body: body, Decision: decision, Instance: inst, Lease: lease, Profile: profile, Attempts: attempt, ColdLoad: cold}, nil
}

type noInstanceAvailableError struct {
	JobID          string
	RetryAfter     time.Duration
	Reason         string
	Decision       domain.PlacementDecision
	Instance       domain.ModelInstance
	Profile        profiles.Profile
	Attempts       int
	DemandRecorded bool
}

func (e noInstanceAvailableError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("job %q queued: %s", e.JobID, e.Reason)
	}
	return fmt.Sprintf("job %q queued: no instance available", e.JobID)
}

func (e noInstanceAvailableError) retryAfterSeconds() int {
	if e.RetryAfter <= 0 {
		return 1
	}
	return int((e.RetryAfter + time.Second - 1) / time.Second)
}

func (r *Router) recordNoInstanceDemand(ctx context.Context, miss noInstanceAvailableError) error {
	if miss.DemandRecorded {
		return nil
	}
	if r.Runtime == nil || r.Runtime.Store == nil {
		return nil
	}
	store, ok := r.Runtime.Store.(reconcile.DemandStore)
	if !ok {
		return nil
	}
	reason := firstString(miss.Reason, "gateway_no_instance")
	return reconcile.RecordDemand(ctx, store, miss.Decision, reason, r.clock().Now())
}

type leasedRouteError struct {
	status int
	msg    string
}

func (e leasedRouteError) Error() string {
	return e.msg
}

func (e leasedRouteError) StatusCode() int {
	if e.status == 0 {
		return http.StatusBadRequest
	}
	return e.status
}

type routeDiagnostics struct {
	Project                 string
	ColdLoad                bool
	LoadWallMS              int
	QueueWaitMS             int
	UpstreamWallMS          int
	StructuredOutput        string
	GuidedDecodingRequested bool
	ToolsRequested          bool
	ToolChoice              string
	VLLMToolParser          bool
	VLLMAutoToolChoice      bool
}

func (r *Router) Route(ctx context.Context, req translate.IngressRequest) (RouteResponse, error) {
	waitMS := r.effectiveMaxWaitMS(req)
	if waitMS <= 0 {
		return r.routeOnce(ctx, req, true)
	}
	return r.routeWithBoundedWait(ctx, req, time.Duration(waitMS)*time.Millisecond)
}

func (r *Router) routeWithBoundedWait(ctx context.Context, req translate.IngressRequest, maxWait time.Duration) (RouteResponse, error) {
	clk := r.clock()
	start := clk.Now()
	deadline := start.Add(maxWait)
	backoff := 500 * time.Millisecond
	recordDemand := true
	for {
		resp, err := r.routeOnce(ctx, req, recordDemand)
		if err == nil {
			queueWaitMS := durationMS(start, clk.Now())
			if queueWaitMS > 0 {
				if resp.Header == nil {
					resp.Header = http.Header{}
				}
				resp.Header.Set(HeaderQueueWaitMS, strconv.Itoa(queueWaitMS))
			}
			return resp, nil
		}
		var noInstance noInstanceAvailableError
		if !errors.As(err, &noInstance) {
			return resp, err
		}
		recordDemand = false
		now := clk.Now()
		if !now.Before(deadline) {
			return RouteResponse{}, noInstance
		}
		delay := backoff
		if remaining := deadline.Sub(now); delay > remaining {
			delay = remaining
		}
		timer := clk.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return RouteResponse{}, ctx.Err()
		case <-timer.C():
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}

func (r *Router) routeOnce(ctx context.Context, req translate.IngressRequest, recordDemand bool) (RouteResponse, error) {
	if r.Placer == nil || r.Fleet == nil || r.Nodes == nil {
		return RouteResponse{}, fmt.Errorf("gateway router is not configured")
	}
	req, err := r.applyRequestDefaults(req)
	if err != nil {
		return RouteResponse{}, err
	}
	if err := r.validatePrivateRequest(req); err != nil {
		return RouteResponse{}, err
	}
	if req.WorkloadID != "" || req.LeaseID != "" {
		return r.routeWithWorkloadLease(ctx, req)
	}
	preset, err := r.Presets.Resolve(req.Model)
	if err != nil {
		return RouteResponse{}, err
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return RouteResponse{}, err
	}
	recorder := r.newSessionRecorder()
	tries := r.MaxTries
	if tries == 0 {
		tries = 2
	}
	var lastErr error
	for attempt := 1; attempt <= tries; attempt++ {
		profile, err := r.profileFor(preset)
		if err != nil {
			return RouteResponse{}, err
		}
		if err := r.validateModelGroupProfile(req.Model, profile); err != nil {
			return RouteResponse{}, err
		}
		route, err := translate.BuildUpstream(req, profile)
		if err != nil {
			return RouteResponse{}, err
		}
		job := r.jobFromIngress(req, attempt)
		clk := r.clock()
		loadStart := clk.Now()
		decision, inst, lease, cold, err := r.placeAndLoad(ctx, job, req.Body, preset, fleet, nil)
		if err != nil {
			lastErr = err
			if decision.InstanceID != "" && attempt < tries {
				fleet = withoutInstance(fleet, decision.InstanceID)
				continue
			}
			var noInstance noInstanceAvailableError
			if errors.As(err, &noInstance) {
				noInstance.Profile = profile
				noInstance.Attempts = attempt
				if recordDemand {
					if demandErr := r.recordNoInstanceDemand(ctx, noInstance); demandErr != nil {
						return RouteResponse{}, errors.Join(noInstance, demandErr)
					}
				}
				return RouteResponse{}, noInstance
			}
			if placementNoFitError(err) {
				noInstance := noInstanceAvailableError{
					JobID:      job.ID,
					RetryAfter: time.Second,
					Reason:     err.Error(),
					Decision:   decision,
					Instance:   inst,
					Profile:    profile,
					Attempts:   attempt,
				}
				if recordDemand {
					if demandErr := r.recordNoInstanceDemand(ctx, noInstance); demandErr != nil {
						return RouteResponse{}, errors.Join(noInstance, demandErr)
					}
				}
				return RouteResponse{}, noInstance
			}
			return RouteResponse{}, err
		}
		placedPreset, placedProfile, err := r.selectedPresetProfile(preset, profile, decision)
		if err != nil {
			if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		loadMS := 0
		if cold {
			loadMS = durationMS(loadStart, clk.Now())
		}
		if err := recorder.emit(ctx, job, placedPreset, inst, domain.TelemetryPhasePlaced, func(sample *domain.SessionMetric) {
			sample.BytesIn = len(req.Body)
			sample.LoadWallClockMS = loadMS
		}); err != nil {
			if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
				return RouteResponse{}, failErr
			}
			return RouteResponse{}, err
		}
		if cold {
			if err := recorder.emit(ctx, job, placedPreset, inst, domain.TelemetryPhaseLoadReady, func(sample *domain.SessionMetric) {
				sample.BytesIn = len(req.Body)
				sample.LoadWallClockMS = loadMS
			}); err != nil {
				if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
					return RouteResponse{}, failErr
				}
				return RouteResponse{}, err
			}
		}
		endRequest, err := r.beginInstanceRequest(ctx, inst)
		if err != nil {
			if failErr := r.failPlacedJob(ctx, recorder, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			lastErr = err
			if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, err); reportErr != nil {
				return RouteResponse{}, reportErr
			}
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		upstreamStart := clk.Now()
		if err := recorder.emitAt(ctx, job, placedPreset, inst, domain.TelemetryPhaseUpstreamStart, upstreamStart, func(sample *domain.SessionMetric) {
			sample.BytesIn = len(route.Body)
			sample.LoadWallClockMS = loadMS
		}); err != nil {
			endRequest()
			if failErr := r.failPlacedJob(ctx, nil, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		resp, err := r.callUpstream(ctx, inst, route)
		upstreamEnd := clk.Now()
		endRequest()
		if err != nil {
			if failErr := r.failPlacedJob(ctx, recorder, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			lastErr = err
			if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, err); reportErr != nil {
				return RouteResponse{}, reportErr
			}
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		if resp.Status >= 400 {
			bodyText := strings.TrimSpace(string(resp.Body))
			statusErr := fmt.Errorf("upstream returned %d: %s", resp.Status, bodyText)
			if optimizer.IsContextOverflow(placedPreset.Backend, fmt.Errorf("%s", bodyText)) {
				next, ok := r.reactiveRequeuePreset(job, placedPreset, statusErr)
				if ok {
					if failErr := r.failPlacedJob(ctx, recorder, job, placedPreset, inst, lease, statusErr); failErr != nil {
						return RouteResponse{}, errors.Join(statusErr, failErr)
					}
					lastErr = fmt.Errorf("context overflow on %s; retrying with %s", placedPreset.ID, next.ID)
					req, err = translate.WithModel(req, next.ID)
					if err != nil {
						return RouteResponse{}, err
					}
					req.ContextRequest = modelruntime.EffectiveContextLength(next)
					preset = next
					continue
				}
			}
			if resp.Status >= 500 {
				if failErr := r.failPlacedJob(ctx, recorder, job, placedPreset, inst, lease, statusErr); failErr != nil {
					return RouteResponse{}, errors.Join(statusErr, failErr)
				}
				lastErr = statusErr
				if reportErr := r.reportFailure(cleanupContext(ctx), inst.ID, statusErr); reportErr != nil {
					return RouteResponse{}, reportErr
				}
				fleet = withoutInstance(fleet, inst.ID)
				continue
			}
			if failErr := r.failPlacedJob(ctx, recorder, job, placedPreset, inst, lease, statusErr); failErr != nil {
				return RouteResponse{}, errors.Join(statusErr, failErr)
			}
			headers := cloneHeader(resp.Header)
			if headers.Get("Content-Type") == "" {
				headers.Set("Content-Type", "application/json")
			}
			writeRouteDiagnostics(headers, routeDiagnosticsFor(req, placedPreset, placedProfile, cold, loadMS, 0, durationMS(upstreamStart, upstreamEnd)))
			writeDecisionHeaders(headers, decision, inst, placedProfile, attempt)
			body := resp.Body
			if len(body) == 0 {
				body = []byte(fmt.Sprintf(`{"error":%q}`, statusErr.Error()))
			}
			return RouteResponse{
				Status:   resp.Status,
				Header:   headers,
				Body:     body,
				Decision: decision,
				Instance: inst,
				Lease:    lease,
				Profile:  placedProfile,
				Attempts: attempt,
				ColdLoad: cold,
			}, nil
		}
		body, contentType, err := translate.TranslateResponse(req, route, resp.Body)
		if err != nil {
			if failErr := r.failPlacedJob(ctx, nil, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		body, err = normalizeOpenAIReasoningBoundary(req, placedPreset, body)
		if err != nil {
			if failErr := r.failPlacedJob(ctx, nil, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		metric, err := r.recordMetric(ctx, job, placedPreset, inst, body, metricTiming{
			Start:           upstreamStart,
			FirstByte:       upstreamEnd,
			End:             upstreamEnd,
			LoadWallClockMS: loadMS,
		})
		if err != nil {
			if failErr := r.failPlacedJob(ctx, nil, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		promptTokens, completionTokens := usageFromBody(body)
		if err := recorder.emitMetric(ctx, metric, domain.TelemetryPhaseComplete, len(route.Body), len(body), promptTokens, completionTokens); err != nil {
			if failErr := r.failPlacedJob(ctx, nil, job, placedPreset, inst, lease, err); failErr != nil {
				return RouteResponse{}, errors.Join(err, failErr)
			}
			return RouteResponse{}, err
		}
		if err := r.finishJob(ctx, job, lease, nil); err != nil {
			return RouteResponse{}, err
		}
		headers := cloneHeader(resp.Header)
		if contentType != "" {
			headers.Set("Content-Type", contentType)
		}
		writeRouteDiagnostics(headers, routeDiagnosticsFor(req, placedPreset, placedProfile, cold, loadMS, 0, durationMS(upstreamStart, upstreamEnd)))
		writeDecisionHeaders(headers, decision, inst, placedProfile, attempt)
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
			Lease:    lease,
			Profile:  placedProfile,
			Attempts: attempt,
			ColdLoad: cold,
		}, nil
	}
	return RouteResponse{}, fmt.Errorf("gateway failover exhausted: %w", lastErr)
}

func (r *Router) routeWithWorkloadLease(ctx context.Context, req translate.IngressRequest) (RouteResponse, error) {
	job, decision, inst, lease, err := r.resolveWorkloadLease(ctx, req)
	if err != nil {
		return RouteResponse{}, err
	}
	resp, err := r.ExecutePlacedProvider(ctx, req, job, decision, inst, lease, 1, false, 0)
	if err != nil {
		if resp.Status != 0 {
			return resp, nil
		}
		return RouteResponse{}, err
	}
	if req.HoldLease {
		// The caller (e.g. `myce pool proxy`) owns the workload lifecycle:
		// keep the worker running so the reserved lease serves more requests.
		if resp.Header == nil {
			resp.Header = http.Header{}
		}
		resp.Header.Set(HeaderLeaseHold, "true")
		return resp, nil
	}
	if err := r.finishJob(ctx, job, lease, nil); err != nil {
		return RouteResponse{}, err
	}
	return resp, nil
}

func (r *Router) resolveWorkloadLease(ctx context.Context, req translate.IngressRequest) (domain.Job, domain.PlacementDecision, domain.ModelInstance, domain.Lease, error) {
	if req.WorkloadID == "" || req.LeaseID == "" {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusBadRequest, msg: "both X-Myc-Workload and X-Myc-Lease are required for leased gateway execution"}
	}
	if req.Stream {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusBadRequest, msg: "leased gateway execution does not support streaming requests"}
	}
	if r.Runtime == nil || r.Runtime.Store == nil {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: "workload lease validation requires a configured runtime store"}
	}
	reader, ok := r.Runtime.Store.(scheduler.JobReader)
	if !ok {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: "runtime store cannot read workload jobs for lease validation"}
	}
	job, err := reader.Job(ctx, req.WorkloadID)
	if err != nil {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusNotFound, msg: fmt.Sprintf("workload %q was not found", req.WorkloadID)}
	}
	if job.Status != domain.JobRunning {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q is %q, not running", job.ID, job.Status)}
	}
	if job.Result == nil || job.Result.LeaseID == "" {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q has no active lease evidence", job.ID)}
	}
	if job.Result.Terminal {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q is already terminal", job.ID)}
	}
	if job.Result.LeaseID != req.LeaseID {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q lease mismatch", job.ID)}
	}
	if expected := r.projectForRequest(req); expected != "" && job.Project != "" && job.Project != expected {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusForbidden, msg: fmt.Sprintf("workload %q belongs to a different project", job.ID)}
	}
	if !r.requestCanUseWorkloadPreset(req.Model, job) {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q is not reserved for model %q", job.ID, req.Model)}
	}
	if req.ContextRequest > 0 && job.ContextRequest > 0 && req.ContextRequest > job.ContextRequest {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q context envelope %d is smaller than request %d", job.ID, job.ContextRequest, req.ContextRequest)}
	}
	lease, err := r.activeWorkloadLease(ctx, job)
	if err != nil {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, err
	}
	inst, err := r.workloadInstance(ctx, job, lease)
	if err != nil {
		return domain.Job{}, domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, err
	}
	decision := job.Result.Decision
	if decision.JobID == "" {
		decision.JobID = job.ID
	}
	if decision.Action == "" {
		decision.Action = nonEmptyAction(job.Result.Action, domain.ActionWarmInstance)
	}
	if decision.NodeID == "" {
		decision.NodeID = firstString(job.Result.NodeID, inst.NodeID, lease.NodeID)
	}
	if decision.InstanceID == "" {
		decision.InstanceID = firstString(inst.ID, lease.InstanceID)
	}
	if decision.Preset.ID == "" {
		presetID := firstString(job.Result.PresetID, job.PresetID, inst.PresetID)
		if presetID != "" {
			if preset, err := r.Presets.Resolve(presetID); err == nil {
				decision.Preset = preset
			}
		}
	}
	decision.Trace = append(decision.Trace, domain.TraceStep{Step: "workload-lease", Result: "using reserved workload lease"})
	return job, decision, inst, lease, nil
}

func (r *Router) activeWorkloadLease(ctx context.Context, job domain.Job) (domain.Lease, error) {
	leases, err := r.Runtime.Store.ListLeases(ctx)
	if err != nil {
		return domain.Lease{}, err
	}
	for _, lease := range leases {
		if lease.ID != job.Result.LeaseID {
			continue
		}
		if lease.JobID != "" && lease.JobID != job.ID {
			return domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("lease %q belongs to a different workload", lease.ID)}
		}
		if !lease.ExpiresAt.IsZero() && !lease.ExpiresAt.After(r.clock().Now()) {
			return domain.Lease{}, leasedRouteError{status: http.StatusGone, msg: fmt.Sprintf("workload %q lease %q is expired", job.ID, lease.ID)}
		}
		return lease, nil
	}
	return domain.Lease{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q lease %q is not active", job.ID, job.Result.LeaseID)}
}

func (r *Router) workloadInstance(ctx context.Context, job domain.Job, lease domain.Lease) (domain.ModelInstance, error) {
	inst := job.Result.Instance
	if inst.ID == "" {
		inst.ID = lease.InstanceID
	}
	if inst.NodeID == "" {
		inst.NodeID = firstString(job.Result.NodeID, lease.NodeID)
	}
	if inst.PresetID == "" {
		inst.PresetID = firstString(job.Result.PresetID, job.PresetID)
	}
	if inst.ID != "" && inst.Addr != "" && inst.NodeID != "" {
		return inst, nil
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	for _, candidate := range fleet.Instances {
		if candidate.ID == inst.ID && (inst.NodeID == "" || candidate.NodeID == inst.NodeID) {
			return candidate, nil
		}
	}
	return domain.ModelInstance{}, leasedRouteError{status: http.StatusConflict, msg: fmt.Sprintf("workload %q reserved instance %q is not visible", job.ID, inst.ID)}
}

func (r *Router) requestCanUseWorkloadPreset(model string, job domain.Job) bool {
	if model == "" {
		return false
	}
	if model == job.Model || model == job.PresetID {
		return true
	}
	presetID := ""
	if job.Result != nil {
		presetID = firstString(job.Result.PresetID, job.Result.Instance.PresetID, job.Result.Decision.Preset.ID)
	}
	if presetID == "" {
		presetID = job.PresetID
	}
	if model == presetID {
		return true
	}
	for _, preset := range r.Presets.ResolveAll(model) {
		if preset.ID == presetID {
			return true
		}
	}
	return false
}

func (r *Router) projectForRequest(req translate.IngressRequest) string {
	if req.Project != "" {
		return req.Project
	}
	return r.DefaultProject
}

func (r *Router) effectiveMaxWaitMS(req translate.IngressRequest) int {
	if req.HasMaxWait {
		return req.MaxWaitMS
	}
	project := r.projectForRequest(req)
	if project != "" {
		if defaults, ok := r.Projects[project]; ok && defaults.MaxWaitMS > 0 {
			return defaults.MaxWaitMS
		}
	}
	return r.MaxWaitDefaultMS
}

func nonEmptyAction(values ...domain.PlacementAction) domain.PlacementAction {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func placementNoFitError(err error) bool {
	if errors.Is(err, domain.ErrNoFit) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "no unit can fit") || strings.Contains(msg, "concurrency envelope is full")
}

func normalizeOpenAIReasoningBoundary(req translate.IngressRequest, preset domain.Preset, body []byte) ([]byte, error) {
	if req.Kind != translate.KindOpenAIChat || req.Stream || preset.Backend != domain.BackendVLLM || !hasConfiguredReasoningParser(preset) {
		return body, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("normalize reasoning boundary: decode response: %w", err)
	}
	choicesRaw, ok := root["choices"]
	if !ok {
		return body, nil
	}
	var choices []map[string]json.RawMessage
	if err := json.Unmarshal(choicesRaw, &choices); err != nil {
		return nil, fmt.Errorf("normalize reasoning boundary: decode choices: %w", err)
	}
	changed := false
	for i := range choices {
		if !jsonStringEquals(choices[i]["finish_reason"], "length") {
			continue
		}
		var msg map[string]json.RawMessage
		if err := json.Unmarshal(choices[i]["message"], &msg); err != nil {
			return nil, fmt.Errorf("normalize reasoning boundary: decode message: %w", err)
		}
		if !jsonNullOrMissing(msg["reasoning"]) || !jsonNullOrMissing(msg["reasoning_content"]) {
			continue
		}
		var content string
		if raw, ok := msg["content"]; !ok || jsonNull(raw) {
			continue
		} else if err := json.Unmarshal(raw, &content); err != nil {
			return nil, fmt.Errorf("normalize reasoning boundary: decode content: %w", err)
		}
		if content == "" {
			continue
		}
		reasoning, err := json.Marshal(content)
		if err != nil {
			return nil, fmt.Errorf("normalize reasoning boundary: encode reasoning: %w", err)
		}
		emptyContent, err := json.Marshal("")
		if err != nil {
			return nil, fmt.Errorf("normalize reasoning boundary: encode content: %w", err)
		}
		msg["reasoning"] = reasoning
		msg["reasoning_content"] = reasoning
		msg["content"] = emptyContent
		encoded, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("normalize reasoning boundary: encode message: %w", err)
		}
		choices[i]["message"] = encoded
		changed = true
	}
	if !changed {
		return body, nil
	}
	encodedChoices, err := json.Marshal(choices)
	if err != nil {
		return nil, fmt.Errorf("normalize reasoning boundary: encode choices: %w", err)
	}
	root["choices"] = encodedChoices
	normalized, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("normalize reasoning boundary: encode response: %w", err)
	}
	return normalized, nil
}

func hasConfiguredReasoningParser(preset domain.Preset) bool {
	for i, arg := range preset.LaunchArgs {
		if arg == "--reasoning-parser" && i+1 < len(preset.LaunchArgs) {
			return strings.TrimSpace(preset.LaunchArgs[i+1]) != ""
		}
		if value, ok := strings.CutPrefix(arg, "--reasoning-parser="); ok {
			return strings.TrimSpace(value) != ""
		}
	}
	return false
}

func jsonNullOrMissing(raw json.RawMessage) bool {
	return raw == nil || jsonNull(raw)
}

func jsonNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func jsonStringEquals(raw json.RawMessage, want string) bool {
	var got string
	return raw != nil && json.Unmarshal(raw, &got) == nil && got == want
}

func (r *Router) Stream(ctx context.Context, req translate.IngressRequest, w http.ResponseWriter) error {
	if r.Placer == nil || r.Fleet == nil || r.Nodes == nil {
		return fmt.Errorf("gateway router is not configured")
	}
	req, err := r.applyRequestDefaults(req)
	if err != nil {
		return err
	}
	if err := r.validatePrivateRequest(req); err != nil {
		return err
	}
	preset, err := r.Presets.Resolve(req.Model)
	if err != nil {
		return err
	}
	fleet, err := r.Fleet.Snapshot(ctx)
	if err != nil {
		return err
	}
	recorder := r.newSessionRecorder()
	tries := r.MaxTries
	if tries == 0 {
		tries = 2
	}
	started := false
	providerStarted := false
	var lastErr error
	for attempt := 1; attempt <= tries; attempt++ {
		profile, err := r.profileFor(preset)
		if err != nil {
			return err
		}
		if err := r.validateModelGroupProfile(req.Model, profile); err != nil {
			return err
		}
		route, err := translate.BuildUpstream(req, profile)
		if err != nil {
			return err
		}
		if route.Translate {
			return fmt.Errorf("translated streaming responses are not supported")
		}
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
			writeRouteDiagnostics(headers, routeDiagnosticsFor(req, preset, profile, true, 0, 0, -1))
			writeDecisionHeaders(headers, decision, domain.ModelInstance{NodeID: decision.NodeID}, profile, attempt)
			w.WriteHeader(http.StatusOK)
			started = true
			if _, err := w.Write(loadingEvent(decision, domain.ModelInstance{NodeID: decision.NodeID})); err != nil {
				return err
			}
			flush(w)
			return nil
		}
		decision, inst, lease, cold, err := r.placeAndLoad(ctx, job, req.Body, preset, fleet, beforeCold)
		if err != nil {
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		placedPreset, placedProfile, err := r.selectedPresetProfile(preset, profile, decision)
		if err != nil {
			if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
				err = releaseErr
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		if started && cold {
			if _, err := w.Write(readyEvent(decision, inst)); err != nil {
				_ = r.releaseAndFail(ctx, job, lease, err)
				return nil
			}
			flush(w)
		}
		loadMS := 0
		if cold {
			loadMS = durationMS(loadStart, clk.Now())
		}
		if err := recorder.emit(ctx, job, placedPreset, inst, domain.TelemetryPhasePlaced, func(sample *domain.SessionMetric) {
			sample.BytesIn = len(req.Body)
			sample.LoadWallClockMS = loadMS
		}); err != nil {
			if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
				err = releaseErr
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		if cold {
			if err := recorder.emit(ctx, job, placedPreset, inst, domain.TelemetryPhaseLoadReady, func(sample *domain.SessionMetric) {
				sample.BytesIn = len(req.Body)
				sample.LoadWallClockMS = loadMS
			}); err != nil {
				if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
					err = releaseErr
				}
				if started {
					writeStreamError(w, err)
					return nil
				}
				return err
			}
		}
		endRequest, err := r.beginInstanceRequest(ctx, inst)
		if err != nil {
			if sampleErr := recorder.emitError(ctx, job, placedPreset, inst, err); sampleErr != nil {
				err = sampleErr
			}
			if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
				err = releaseErr
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			lastErr = err
			if reportErr := r.reportFailure(ctx, inst.ID, err); reportErr != nil {
				return reportErr
			}
			fleet = withoutInstance(fleet, inst.ID)
			continue
		}
		upstreamStart := clk.Now()
		if err := recorder.emitAt(ctx, job, placedPreset, inst, domain.TelemetryPhaseUpstreamStart, upstreamStart, func(sample *domain.SessionMetric) {
			sample.BytesIn = len(route.Body)
			sample.LoadWallClockMS = loadMS
		}); err != nil {
			endRequest()
			if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
				err = releaseErr
			}
			if started {
				writeStreamError(w, err)
				return nil
			}
			return err
		}
		resp, err := r.doUpstream(ctx, inst, route)
		if err != nil {
			endRequest()
			if releaseErr := r.releaseAndFail(ctx, job, lease, err); releaseErr != nil {
				if started {
					writeStreamError(w, releaseErr)
					return nil
				}
				return releaseErr
			}
			if sampleErr := recorder.emitError(ctx, job, placedPreset, inst, err); sampleErr != nil {
				if started {
					writeStreamError(w, sampleErr)
					return nil
				}
				return sampleErr
			}
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
			body, readErr := readLimited(resp.Body, MaxUpstreamResponseBodyBytes, "upstream response body")
			_ = resp.Body.Close()
			endRequest()
			if readErr != nil {
				err = readErr
			} else {
				err = fmt.Errorf("upstream returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			if optimizer.IsContextOverflow(placedPreset.Backend, err) {
				next, ok := r.reactiveRequeuePreset(job, placedPreset, err)
				if ok && !providerStarted {
					if sampleErr := recorder.emitError(ctx, job, placedPreset, inst, err); sampleErr != nil {
						return sampleErr
					}
					if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
						return failErr
					}
					lastErr = fmt.Errorf("context overflow on %s; retrying with %s", placedPreset.ID, next.ID)
					req, err = translate.WithModel(req, next.ID)
					if err != nil {
						return err
					}
					req.ContextRequest = modelruntime.EffectiveContextLength(next)
					preset = next
					continue
				}
			}
			if resp.StatusCode >= 500 {
				if sampleErr := recorder.emitError(ctx, job, placedPreset, inst, err); sampleErr != nil {
					if started {
						writeStreamError(w, sampleErr)
						return nil
					}
					return sampleErr
				}
				if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
					if started {
						writeStreamError(w, failErr)
						return nil
					}
					return failErr
				}
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
			if sampleErr := recorder.emitError(ctx, job, placedPreset, inst, err); sampleErr != nil {
				if started {
					writeStreamError(w, sampleErr)
					return nil
				}
				return sampleErr
			}
			if failErr := r.releaseAndFail(ctx, job, lease, err); failErr != nil {
				if started {
					writeStreamError(w, failErr)
					return nil
				}
				return failErr
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
			writeRouteDiagnostics(w.Header(), routeDiagnosticsFor(req, placedPreset, placedProfile, cold, loadMS, 0, -1))
			writeDecisionHeaders(w.Header(), decision, inst, placedProfile, attempt)
			w.WriteHeader(resp.StatusCode)
			started = true
			providerStarted = true
		} else {
			providerStarted = true
		}
		firstByteSampled := false
		verifySSE := isSSEContentType(resp.Header.Get("Content-Type"))
		streamTelemetryCtx := cleanupContext(ctx)
		copied, copyErr := copyAndFlush(w, resp.Body, clk, verifySSE, func(copied copyResult) error {
			if !firstByteSampled && !copied.FirstByte.IsZero() {
				firstByteSampled = true
				if err := recorder.emitAt(streamTelemetryCtx, job, placedPreset, inst, domain.TelemetryPhaseFirstByte, copied.FirstByte, func(sample *domain.SessionMetric) {
					sample.BytesIn = len(route.Body)
					sample.BytesOut = copied.Bytes
					sample.LoadWallClockMS = loadMS
					sample.TTFTms = durationMS(upstreamStart, copied.FirstByte)
				}); err != nil {
					return err
				}
			}
			return recorder.emitAt(streamTelemetryCtx, job, placedPreset, inst, domain.TelemetryPhaseStreamChunk, copied.End, func(sample *domain.SessionMetric) {
				sample.BytesIn = len(route.Body)
				sample.BytesOut = copied.Bytes
				sample.LoadWallClockMS = loadMS
				sample.TTFTms = durationMS(upstreamStart, copied.FirstByte)
			})
		})
		_ = resp.Body.Close()
		endRequest()
		if copyErr != nil {
			r.failPlacedStream(ctx, recorder, job, placedPreset, inst, lease, w, copyErr, providerStarted)
			return nil
		}
		if verifySSE && !copied.SSETerminal {
			err := streamCopyError{
				kind: streamFailureUpstreamRead,
				err:  fmt.Errorf("upstream SSE stream ended without a terminal event"),
			}
			r.failPlacedStream(ctx, recorder, job, placedPreset, inst, lease, w, err, providerStarted)
			return nil
		}
		finishCtx := cleanupContext(ctx)
		metric, err := r.recordMetric(finishCtx, job, placedPreset, inst, copied.Body, metricTiming{
			Start:           upstreamStart,
			FirstByte:       copied.FirstByte,
			End:             copied.End,
			LoadWallClockMS: loadMS,
		})
		if err != nil {
			r.failPlacedStream(ctx, recorder, job, placedPreset, inst, lease, w, streamCopyError{kind: streamFailureTelemetry, err: err}, providerStarted)
			return nil
		}
		promptTokens, completionTokens := usageFromBody(copied.Body)
		if err := recorder.emitMetric(finishCtx, metric, domain.TelemetryPhaseComplete, len(route.Body), copied.Bytes, promptTokens, completionTokens); err != nil {
			r.failPlacedStream(ctx, recorder, job, placedPreset, inst, lease, w, streamCopyError{kind: streamFailureTelemetry, err: err}, providerStarted)
			return nil
		}
		if err := r.finishJob(finishCtx, job, lease, nil); err != nil {
			if !providerStarted {
				writeStreamError(w, err)
			}
			return nil
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no stream placement attempts were made")
	}
	return fmt.Errorf("gateway failover exhausted: %w", lastErr)
}

func (r *Router) placeAndLoad(ctx context.Context, job domain.Job, payload []byte, preset domain.Preset, fleet domain.FleetSnapshot, beforeCold func(context.Context, domain.PlacementDecision) error) (domain.PlacementDecision, domain.ModelInstance, domain.Lease, bool, error) {
	if r.Runtime != nil {
		hooks := []scheduler.SubmitHooks{{RejectQueued: true}}
		if beforeCold != nil {
			hooks = append(hooks, scheduler.SubmitHooks{BeforeColdLoad: beforeCold})
		}
		result, err := r.Runtime.SubmitWithPayload(ctx, job, payload, hooks...)
		if err != nil {
			if result.Decision.Action == domain.ActionQueued {
				return result.Decision, result.Instance, result.Lease, false, noInstanceAvailableError{JobID: job.ID, RetryAfter: time.Second, Decision: result.Decision, Instance: result.Instance, DemandRecorded: true}
			}
			return result.Decision, result.Instance, result.Lease, false, err
		}
		if result.Decision.Action == domain.ActionQueued {
			return result.Decision, result.Instance, result.Lease, false, noInstanceAvailableError{JobID: job.ID, RetryAfter: time.Second, Decision: result.Decision, Instance: result.Instance, DemandRecorded: true}
		}
		cold := result.Decision.InstanceID == ""
		return result.Decision, result.Instance, result.Lease, cold, nil
	}
	if !r.allowDirectPlacement {
		return domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, false, fmt.Errorf("gateway router requires coordinator runtime")
	}
	decision, err := r.Placer.Place(ctx, job, fleet)
	if err != nil {
		return domain.PlacementDecision{}, domain.ModelInstance{}, domain.Lease{}, false, err
	}
	if decision.Action == domain.ActionQueued {
		return decision, domain.ModelInstance{}, domain.Lease{}, false, noInstanceAvailableError{JobID: job.ID, RetryAfter: time.Second, Decision: decision}
	}
	if decision.InstanceID == "" && decision.Action != domain.ActionQueued && beforeCold != nil {
		if err := beforeCold(ctx, decision); err != nil {
			return decision, domain.ModelInstance{}, domain.Lease{}, false, err
		}
	}
	inst, cold, err := r.resolveInstance(ctx, job, decision, preset, fleet)
	return decision, inst, domain.Lease{}, cold, err
}

func (r *Router) jobFromIngress(req translate.IngressRequest, attempt int) domain.Job {
	seq := atomic.AddUint64(&r.jobSeq, 1)
	r.jobPrefixOnce.Do(func() {
		r.jobPrefix = fmt.Sprintf("%d", r.clock().Now().UnixNano())
	})
	prefix := r.jobPrefix
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
	if contextRequest == 0 {
		if group := r.Presets.ResolveAll(req.Model); len(group) > 1 {
			contextRequest = 0
		} else if preset, err := r.Presets.Resolve(req.Model); err == nil {
			contextRequest = modelruntime.EffectiveContextLength(preset)
		}
	}
	expectedConcurrency := projectDefaults.ExpectedConcurrency
	if expectedConcurrency == 0 {
		expectedConcurrency = 1
	}
	taskType := "chat"
	if req.Kind == translate.KindOpenAICompletion {
		taskType = "completion"
	}
	return domain.Job{
		ID:                  fmt.Sprintf("gateway-%s-%d-%d", prefix, seq, attempt),
		TaskType:            taskType,
		Model:               req.Model,
		Project:             project,
		Priority:            priority,
		SpeedPref:           speed,
		ContextRequest:      contextRequest,
		ExpectedConcurrency: expectedConcurrency,
		Preemption:          preemption,
		Streaming:           req.Stream,
		Submitter:           req.Submitter,
		Handling:            req.Handling,
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

func (r *Router) validatePrivateRequest(req translate.IngressRequest) error {
	if req.Handling != domain.HandlingPrivate {
		return nil
	}
	return fmt.Errorf("private handling is disabled until private job recovery is implemented")
}

func (r *Router) profileFor(preset domain.Preset) (profiles.Profile, error) {
	registry := r.Profiles
	if registry.IsZero() {
		registry = profiles.DefaultRegistry()
	}
	if preset.ProviderProfile != "" {
		return registry.ByID(preset.ProviderProfile)
	}
	return registry.ForBackend(preset.Backend)
}

func (r *Router) validateModelGroupProfile(model string, base profiles.Profile) error {
	for _, preset := range r.Presets.ResolveAll(model) {
		profile, err := r.profileFor(preset)
		if err != nil {
			return err
		}
		if !profilesWireCompatible(base, profile) {
			return fmt.Errorf("model %q maps to incompatible provider profiles %q and %q", model, base.ID, profile.ID)
		}
	}
	return nil
}

func (r *Router) selectedPresetProfile(initial domain.Preset, initialProfile profiles.Profile, decision domain.PlacementDecision) (domain.Preset, profiles.Profile, error) {
	selected := initial
	if decision.Preset.ID != "" {
		selected = decision.Preset
	}
	if selected.ID == initial.ID {
		return selected, initialProfile, nil
	}
	profile, err := r.profileFor(selected)
	if err != nil {
		return domain.Preset{}, profiles.Profile{}, err
	}
	if !profilesWireCompatible(initialProfile, profile) {
		return domain.Preset{}, profiles.Profile{}, fmt.Errorf("selected preset %q provider profile %q is incompatible with initial profile %q", selected.ID, profile.ID, initialProfile.ID)
	}
	return selected, profile, nil
}

func profilesWireCompatible(left, right profiles.Profile) bool {
	return left.Format == right.Format &&
		left.ChatPath == right.ChatPath &&
		left.CompletionPath == right.CompletionPath &&
		left.AnthropicPath == right.AnthropicPath &&
		left.SupportsChat == right.SupportsChat &&
		left.SupportsStream == right.SupportsStream
}

func (r *Router) beginInstanceRequest(ctx context.Context, inst domain.ModelInstance) (func(), error) {
	agent, err := r.Nodes.NodeAgent(inst.NodeID)
	if err != nil {
		return nil, err
	}
	if err := agent.BeginRequest(ctx, inst.ID); err != nil {
		return nil, err
	}
	return func() {
		_ = agent.EndRequest(context.Background(), inst.ID)
	}, nil
}

func (r *Router) resolveInstance(ctx context.Context, job domain.Job, decision domain.PlacementDecision, preset domain.Preset, fleet domain.FleetSnapshot) (domain.ModelInstance, bool, error) {
	if decision.InstanceID != "" {
		for _, inst := range fleet.Instances {
			if inst.ID == decision.InstanceID && (decision.NodeID == "" || inst.NodeID == decision.NodeID) {
				return inst, false, nil
			}
		}
		return domain.ModelInstance{}, false, fmt.Errorf("selected instance %q on node %q is missing from fleet snapshot", decision.InstanceID, decision.NodeID)
	}
	if decision.NodeID == "" {
		return domain.ModelInstance{}, false, fmt.Errorf("placement action %q did not select a node", decision.Action)
	}
	agent, err := r.Nodes.NodeAgent(decision.NodeID)
	if err != nil {
		return domain.ModelInstance{}, false, err
	}
	loadPreset := decision.Preset
	if loadPreset.ID == "" {
		loadPreset = preset
	}
	inst, err := agent.Load(ctx, domain.LoadRequest{
		JobID:          decision.JobID,
		Preset:         loadPreset,
		Claim:          decision.Claim,
		AcceleratorSet: append([]int(nil), decision.AcceleratorSet...),
		Priority:       job.Priority,
	})
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
	body, err := readLimited(resp.Body, MaxUpstreamResponseBodyBytes, "upstream response body")
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
	for key, value := range route.Headers {
		httpReq.Header.Set(key, value)
	}
	return client.Do(httpReq)
}

func (r *Router) reportFailure(ctx context.Context, instanceID string, err error) error {
	if r.Reporter != nil {
		return r.Reporter.ReportInstanceFailure(ctx, instanceID, err)
	}
	return nil
}

func (r *Router) releaseLease(ctx context.Context, lease domain.Lease) error {
	ctx = cleanupContext(ctx)
	if r.Runtime == nil {
		return nil
	}
	if lease.ID != "" || lease.JobID != "" {
		return r.Runtime.ReleaseJob(ctx, lease)
	}
	return nil
}

func (r *Router) completeJob(ctx context.Context, job domain.Job, lease domain.Lease) error {
	if r.Runtime == nil {
		return nil
	}
	return r.Runtime.CompleteJob(cleanupContext(ctx), job, lease)
}

func (r *Router) finishJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error {
	if r.Runtime == nil {
		return nil
	}
	return r.Runtime.FinishJob(cleanupContext(ctx), job, lease, cause)
}

func (r *Router) failJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error {
	if r.Runtime == nil {
		return nil
	}
	return r.Runtime.FailJob(cleanupContext(ctx), job, lease, cause)
}

func (r *Router) releaseAndFail(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error {
	return r.finishJob(ctx, job, lease, cause)
}

func (r *Router) failPlacedJob(ctx context.Context, recorder *sessionRecorder, job domain.Job, preset domain.Preset, inst domain.ModelInstance, lease domain.Lease, cause error) error {
	cleanup := cleanupContext(ctx)
	var errs []error
	if recorder != nil {
		if sampleErr := recorder.emitError(cleanup, job, preset, inst, cause); sampleErr != nil {
			errs = append(errs, sampleErr)
		}
	}
	if failErr := r.releaseAndFail(cleanup, job, lease, cause); failErr != nil {
		errs = append(errs, failErr)
	}
	return errors.Join(errs...)
}

func (r *Router) failPlacedStream(ctx context.Context, recorder *sessionRecorder, job domain.Job, preset domain.Preset, inst domain.ModelInstance, lease domain.Lease, w http.ResponseWriter, cause error, providerStarted bool) {
	cleanup := cleanupContext(ctx)
	terminalCause := cause
	if sampleErr := recorder.emitError(cleanup, job, preset, inst, cause); sampleErr != nil {
		terminalCause = errors.Join(terminalCause, sampleErr)
	}
	if failErr := r.releaseAndFail(cleanup, job, lease, terminalCause); failErr != nil {
		terminalCause = errors.Join(terminalCause, failErr)
	}
	if streamFailureIndicatesInstance(cause) {
		if reportErr := r.reportFailure(cleanup, inst.ID, cause); reportErr != nil {
			terminalCause = errors.Join(terminalCause, reportErr)
		}
	}
	if !providerStarted {
		writeStreamError(w, terminalCause)
	}
}

func cleanupContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

type metricTiming struct {
	Start           time.Time
	FirstByte       time.Time
	End             time.Time
	LoadWallClockMS int
}

type sessionRecorder struct {
	router    *Router
	sessionID string
	start     time.Time
	sequence  int
}

func (r *Router) newSessionRecorder() *sessionRecorder {
	clk := r.clock()
	start := clk.Now()
	return &sessionRecorder{
		router:    r,
		sessionID: fmt.Sprintf("gateway-session-%d-%d", start.UnixNano(), atomic.AddUint64(&r.sessionSeq, 1)),
		start:     start,
	}
}

func (s *sessionRecorder) emit(ctx context.Context, job domain.Job, preset domain.Preset, inst domain.ModelInstance, phase domain.TelemetryPhase, update func(*domain.SessionMetric)) error {
	return s.emitAt(ctx, job, preset, inst, phase, s.router.clock().Now(), update)
}

func (s *sessionRecorder) emitAt(ctx context.Context, job domain.Job, preset domain.Preset, inst domain.ModelInstance, phase domain.TelemetryPhase, at time.Time, update func(*domain.SessionMetric)) error {
	if s == nil || s.router == nil {
		return nil
	}
	if at.IsZero() {
		at = s.router.clock().Now()
	}
	s.sequence++
	presetID := inst.PresetID
	if presetID == "" {
		presetID = preset.ID
	}
	sample := domain.SessionMetric{
		SessionID:  s.sessionID,
		Sequence:   s.sequence,
		JobID:      job.ID,
		Phase:      phase,
		InstanceID: inst.ID,
		NodeID:     inst.NodeID,
		PresetID:   presetID,
		Backend:    preset.Backend,
		Project:    job.Project,
		ElapsedMS:  durationMS(s.start, at),
		At:         at.UTC(),
	}
	if update != nil {
		update(&sample)
	}
	return s.router.recordSample(ctx, sample)
}

func (s *sessionRecorder) emitMetric(ctx context.Context, metric domain.RunMetric, phase domain.TelemetryPhase, bytesIn, bytesOut, tokensIn, tokensOut int) error {
	if s == nil || s.router == nil {
		return nil
	}
	s.sequence++
	sample := domain.SessionMetric{
		SessionID:       s.sessionID,
		Sequence:        s.sequence,
		JobID:           metric.JobID,
		Phase:           phase,
		InstanceID:      metric.InstanceID,
		NodeID:          metric.NodeID,
		PresetID:        metric.PresetID,
		Backend:         metric.Backend,
		Project:         metric.Project,
		TokensIn:        tokensIn,
		TokensOut:       tokensOut,
		ContextUsed:     metric.ContextUsed,
		BytesIn:         bytesIn,
		BytesOut:        bytesOut,
		TokensPerSec:    metric.TokensPerSec,
		TTFTms:          metric.TTFTms,
		LoadWallClockMS: metric.LoadWallClockMS,
		PeakVRAMMB:      metric.PeakVRAMMB,
		ElapsedMS:       durationMS(s.start, metric.At),
		At:              metric.At,
	}
	return s.router.recordSample(ctx, sample)
}

func (s *sessionRecorder) emitError(ctx context.Context, job domain.Job, preset domain.Preset, inst domain.ModelInstance, err error) error {
	return s.emit(ctx, job, preset, inst, domain.TelemetryPhaseError, func(sample *domain.SessionMetric) {
		if err != nil {
			sample.Error = err.Error()
		}
	})
}

func (r *Router) recordMetric(ctx context.Context, job domain.Job, preset domain.Preset, inst domain.ModelInstance, body []byte, timing metricTiming) (domain.RunMetric, error) {
	if r.Telemetry == nil && r.TelemetryPeerClient == nil {
		return domain.RunMetric{}, nil
	}
	prompt, completion := usageFromBody(body)
	clk := r.clock()
	metric := domain.RunMetric{
		JobID:           job.ID,
		InstanceID:      inst.ID,
		NodeID:          inst.NodeID,
		PresetID:        inst.PresetID,
		Backend:         preset.Backend,
		Project:         job.Project,
		ContextUsed:     prompt + completion,
		TokensPerSec:    metricTokensPerSecond(completion, timing),
		TTFTms:          durationMS(timing.Start, timing.FirstByte),
		LoadWallClockMS: timing.LoadWallClockMS,
		At:              clk.Now().UTC(),
	}
	if r.MemorySampler != nil {
		peak, err := r.MemorySampler.PeakMemoryMB(ctx, inst.NodeID, inst.ID)
		if err != nil {
			return domain.RunMetric{}, err
		}
		metric.PeakVRAMMB = peak
	}
	if err := r.recordMetricValue(ctx, metric); err != nil {
		return domain.RunMetric{}, err
	}
	return metric, nil
}

func (r *Router) reactiveRequeuePreset(job domain.Job, preset domain.Preset, cause error) (domain.Preset, bool) {
	observed := observedOverflowTokens(cause)
	if observed == 0 {
		observed = job.ContextRequest
	}
	if observed <= 0 {
		return domain.Preset{}, false
	}
	plan, err := optimizer.PlanReactiveRequeue(job, preset, cause, observed, optimizer.ReactivePolicy{SharedContexts: r.Presets.ContextLengths()})
	if err != nil {
		return domain.Preset{}, false
	}
	next, ok := r.Presets.SmallestContextAtLeast(preset, plan.Preset.ContextLength)
	return next, ok
}

var overflowTokenCount = regexp.MustCompile(`(?i)(\d+)\s+tokens?`)

func observedOverflowTokens(err error) int {
	if err == nil {
		return 0
	}
	matches := overflowTokenCount.FindStringSubmatch(err.Error())
	if len(matches) != 2 {
		return 0
	}
	n, convErr := strconv.Atoi(matches[1])
	if convErr != nil || n <= 0 {
		return 0
	}
	return n
}

func (r *Router) recordMetricValue(ctx context.Context, metric domain.RunMetric) error {
	if r.Telemetry == nil && r.TelemetryPeerClient == nil {
		return nil
	}
	if (r.SelfNodeID != "" && metric.NodeID == r.SelfNodeID) || (r.SelfNodeID == "" && r.TelemetryPeers == nil && r.TelemetryPeerClient == nil) {
		if r.Telemetry == nil {
			return fmt.Errorf("local owner telemetry sink is not configured")
		}
		return r.Telemetry.Record(ctx, metric)
	}
	if r.TelemetryPeers == nil {
		return fmt.Errorf("remote owner telemetry peer resolver is not configured")
	}
	if r.TelemetryPeerClient == nil {
		return fmt.Errorf("remote owner telemetry client is not configured")
	}
	peer, ok := r.TelemetryPeers.PeerForNode(metric.NodeID)
	if !ok {
		return fmt.Errorf("telemetry owner peer for node %q is not known", metric.NodeID)
	}
	return r.TelemetryPeerClient.PushMetrics(ctx, peer, []domain.RunMetric{metric})
}

func (r *Router) recordSample(ctx context.Context, sample domain.SessionMetric) error {
	if r.Telemetry == nil && r.TelemetryPeerClient == nil {
		return nil
	}
	if sample.NodeID == "" {
		return fmt.Errorf("session telemetry owner node is required")
	}
	if (r.SelfNodeID != "" && sample.NodeID == r.SelfNodeID) || (r.SelfNodeID == "" && r.TelemetryPeers == nil && r.TelemetryPeerClient == nil) {
		if r.Telemetry == nil {
			return fmt.Errorf("local owner telemetry sink is not configured")
		}
		return r.Telemetry.RecordSample(ctx, sample)
	}
	if r.TelemetryPeers == nil {
		return fmt.Errorf("remote owner telemetry peer resolver is not configured")
	}
	if r.TelemetryPeerClient == nil {
		return fmt.Errorf("remote owner telemetry client is not configured")
	}
	peer, ok := r.TelemetryPeers.PeerForNode(sample.NodeID)
	if !ok {
		return fmt.Errorf("telemetry owner peer for node %q is not known", sample.NodeID)
	}
	return r.TelemetryPeerClient.PushSamples(ctx, peer, []domain.SessionMetric{sample})
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

func metricTokensPerSecond(tokens int, timing metricTiming) float64 {
	if timing.End.After(timing.FirstByte) {
		return tokensPerSecond(tokens, timing.FirstByte, timing.End)
	}
	return tokensPerSecond(tokens, timing.Start, timing.End)
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
	if prompt, completion, ok := usageFromSSE(body); ok {
		return prompt, completion
	}
	return 0, 0
}

func usageFromSSE(body []byte) (int, int, bool) {
	var usage api.OpenAIUsage
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(line[len("data:"):])
		if raw == "" || raw == "[DONE]" {
			continue
		}
		var chunk struct {
			Usage *api.OpenAIUsage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(raw), &chunk); err != nil || chunk.Usage == nil {
			continue
		}
		if chunk.Usage.TotalTokens != 0 || chunk.Usage.PromptTokens != 0 || chunk.Usage.CompletionTokens != 0 {
			usage = *chunk.Usage
		}
	}
	if usage.TotalTokens == 0 && usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
		return 0, 0, false
	}
	return usage.PromptTokens, usage.CompletionTokens, true
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
	Body        []byte
	Bytes       int
	FirstByte   time.Time
	End         time.Time
	SSETerminal bool
}

type streamFailureKind string

const (
	streamFailureClientWrite  streamFailureKind = "client_write"
	streamFailureUpstreamRead streamFailureKind = "upstream_read"
	streamFailureTelemetry    streamFailureKind = "telemetry"
)

type streamCopyError struct {
	kind streamFailureKind
	err  error
}

func (e streamCopyError) Error() string {
	return e.err.Error()
}

func (e streamCopyError) Unwrap() error {
	return e.err
}

func streamFailureIndicatesInstance(err error) bool {
	var copyErr streamCopyError
	return errors.As(err, &copyErr) && copyErr.kind == streamFailureUpstreamRead
}

func copyAndFlush(w http.ResponseWriter, r io.Reader, clk ports.Clock, verifySSE bool, onChunk func(copyResult) error) (copyResult, error) {
	var body bytes.Buffer
	result := copyResult{}
	var terminal sseTerminalTracker
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
			result.Bytes += n
			remaining := MaxStreamTelemetryBodyBytes - body.Len()
			if remaining > 0 {
				if n > remaining {
					body.Write(chunk[:remaining])
				} else {
					body.Write(chunk)
				}
			}
			if verifySSE {
				terminal.observe(chunk)
				result.SSETerminal = terminal.terminal()
			}
			if _, err := w.Write(chunk); err != nil {
				result.Body = body.Bytes()
				return result, streamCopyError{kind: streamFailureClientWrite, err: err}
			}
			flush(w)
			result.Body = body.Bytes()
			if onChunk != nil {
				if err := onChunk(result); err != nil {
					return result, streamCopyError{kind: streamFailureTelemetry, err: err}
				}
			}
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
			if verifySSE && result.SSETerminal {
				return result, nil
			}
			return result, streamCopyError{kind: streamFailureUpstreamRead, err: readErr}
		}
	}
}

type sseTerminalTracker struct {
	tail string
	done bool
}

func (t *sseTerminalTracker) observe(chunk []byte) {
	if t.done {
		return
	}
	t.tail += string(chunk)
	const maxTerminalTail = 64 << 10
	if len(t.tail) > maxTerminalTail {
		t.tail = t.tail[len(t.tail)-maxTerminalTail:]
	}
	t.done = sseTailHasTerminal(t.tail)
}

func (t *sseTerminalTracker) terminal() bool {
	return t.done
}

func isSSEContentType(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.HasPrefix(contentType, "text/event-stream")
}

func sseTailHasTerminal(tail string) bool {
	tail = strings.ReplaceAll(tail, "\r\n", "\n")
	for _, line := range strings.Split(tail, "\n") {
		line = strings.TrimSpace(line)
		lower := strings.ToLower(line)
		switch {
		case strings.HasPrefix(lower, "data:"):
			data := strings.TrimSpace(line[len("data:"):])
			if data == "[DONE]" || strings.Contains(data, `"type":"message_stop"`) || strings.Contains(data, `"event":"message_stop"`) {
				return true
			}
		case strings.HasPrefix(lower, "event:"):
			event := strings.TrimSpace(lower[len("event:"):])
			if event == "message_stop" || event == "done" || event == "completion_done" {
				return true
			}
		}
	}
	return false
}

func writeRouteDiagnostics(h http.Header, d routeDiagnostics) {
	if d.Project != "" {
		h.Set(HeaderProject, d.Project)
	}
	h.Set(HeaderColdLoad, strconv.FormatBool(d.ColdLoad))
	h.Set(HeaderQueueWaitMS, strconv.Itoa(d.QueueWaitMS))
	h.Set(HeaderStructuredOutput, d.StructuredOutput)
	h.Set(HeaderGuidedDecodingRequested, strconv.FormatBool(d.GuidedDecodingRequested))
	h.Set(HeaderToolsRequested, strconv.FormatBool(d.ToolsRequested))
	h.Set(HeaderToolChoice, d.ToolChoice)
	h.Set(HeaderVLLMToolParser, strconv.FormatBool(d.VLLMToolParser))
	h.Set(HeaderVLLMAutoToolChoice, strconv.FormatBool(d.VLLMAutoToolChoice))
	if d.LoadWallMS > 0 || d.ColdLoad {
		h.Set(HeaderLoadWallMS, strconv.Itoa(d.LoadWallMS))
	}
	if d.UpstreamWallMS >= 0 {
		h.Set(HeaderUpstreamWallMS, strconv.Itoa(d.UpstreamWallMS))
	}
}

func routeDiagnosticsFor(req translate.IngressRequest, preset domain.Preset, profile profiles.Profile, cold bool, loadMS, queueMS, upstreamMS int) routeDiagnostics {
	parser, auto := vllmToolConfig(preset)
	return routeDiagnostics{
		Project:                 req.Project,
		ColdLoad:                cold,
		LoadWallMS:              loadMS,
		QueueWaitMS:             queueMS,
		UpstreamWallMS:          upstreamMS,
		StructuredOutput:        structuredOutputMode(req),
		GuidedDecodingRequested: guidedDecodingRequested(req, preset, profile),
		ToolsRequested:          toolsRequested(req),
		ToolChoice:              toolChoiceMode(req),
		VLLMToolParser:          parser,
		VLLMAutoToolChoice:      auto,
	}
}

func structuredOutputMode(req translate.IngressRequest) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &fields); err != nil {
		return "none"
	}
	var modes []string
	if raw := fields["response_format"]; len(bytes.TrimSpace(raw)) > 0 && !jsonNull(raw) {
		var responseFormat struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &responseFormat); err == nil && responseFormat.Type != "" {
			modes = append(modes, responseFormat.Type)
		} else {
			modes = append(modes, "response_format")
		}
	}
	if raw := fields["structured_outputs"]; len(bytes.TrimSpace(raw)) > 0 && !jsonNull(raw) {
		modes = append(modes, "structured_outputs")
	}
	if len(modes) == 0 {
		return "none"
	}
	return strings.Join(modes, "+")
}

func toolsRequested(req translate.IngressRequest) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &fields); err != nil {
		return false
	}
	raw := bytes.TrimSpace(fields["tools"])
	return len(raw) > 0 && !jsonNull(raw) && string(raw) != "[]"
}

func toolChoiceMode(req translate.IngressRequest) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &fields); err != nil {
		return "none"
	}
	raw := bytes.TrimSpace(fields["tool_choice"])
	if len(raw) == 0 || jsonNull(raw) {
		if toolsRequested(req) {
			return "auto_default"
		}
		return "none"
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		text = strings.TrimSpace(text)
		if text == "" {
			return "none"
		}
		return text
	}
	var object struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &object); err == nil {
		if object.Type == "function" && object.Function.Name != "" {
			return "function:" + object.Function.Name
		}
		if object.Type != "" {
			return object.Type
		}
	}
	return "custom"
}

func vllmToolConfig(preset domain.Preset) (bool, bool) {
	if preset.Backend != domain.BackendVLLM {
		return false, false
	}
	hasParser := false
	hasAuto := false
	for i, arg := range preset.LaunchArgs {
		switch arg {
		case "--tool-call-parser":
			if i+1 < len(preset.LaunchArgs) && strings.TrimSpace(preset.LaunchArgs[i+1]) != "" {
				hasParser = true
			}
		case "--enable-auto-tool-choice":
			hasAuto = true
		}
		if strings.HasPrefix(arg, "--tool-call-parser=") && strings.TrimSpace(strings.TrimPrefix(arg, "--tool-call-parser=")) != "" {
			hasParser = true
		}
	}
	return hasParser, hasAuto
}

func guidedDecodingRequested(req translate.IngressRequest, preset domain.Preset, profile profiles.Profile) bool {
	if preset.Backend != domain.BackendVLLM || profile.Format != profiles.FormatOpenAI {
		return false
	}
	return structuredOutputMode(req) != "none"
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
