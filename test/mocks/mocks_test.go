package mocks

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
)

func TestBackendAdapterRecordsCallsAndFailures(t *testing.T) {
	backend := NewBackendAdapter()
	if backend.Name() != "mock" {
		t.Fatalf("name = %s", backend.Name())
	}
	handle, err := backend.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	backend.ReadyAfter = 1
	if err := backend.WaitReady(context.Background(), handle.Addr); err == nil {
		t.Fatal("first WaitReady should fail")
	}
	if err := backend.WaitReady(context.Background(), handle.Addr); err != nil {
		t.Fatalf("second WaitReady: %v", err)
	}
	if err := backend.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	dynamic, err := backend.LaunchDynamic(context.Background(), fixtures.MakePreset(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("LaunchDynamic: %v", err)
	}
	if strings.HasSuffix(dynamic.Addr, ":0") || !strings.HasPrefix(dynamic.Addr, "127.0.0.1:") {
		t.Fatalf("dynamic addr = %s", dynamic.Addr)
	}
	if _, err := backend.LaunchDynamic(context.Background(), fixtures.MakePreset(), "not-a-host-port"); err == nil {
		t.Fatal("invalid dynamic addr succeeded")
	}
	if len(backend.Calls) != 5 {
		t.Fatalf("calls = %+v", backend.Calls)
	}

	launchErr := errors.New("launch")
	backend = &BackendAdapter{LaunchErr: launchErr}
	if _, err := backend.Launch(context.Background(), fixtures.MakePreset(), "addr"); !errors.Is(err, launchErr) {
		t.Fatalf("launch error = %v", err)
	}
	if _, err := backend.LaunchDynamic(context.Background(), fixtures.MakePreset(), "127.0.0.1:0"); !errors.Is(err, launchErr) {
		t.Fatalf("dynamic launch error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.WaitReady(ctx, "addr"); err == nil {
		t.Fatal("WaitReady should return context error")
	}
	stopErr := errors.New("stop")
	backend.StopErr = stopErr
	if err := backend.Stop(context.Background(), ports.Handle{}); !errors.Is(err, stopErr) {
		t.Fatalf("stop error = %v", err)
	}
}

func TestNodeAgentRecordsLoadUnloadAndFailures(t *testing.T) {
	agent := NewNodeAgent(fixtures.MakeNode())
	if _, err := agent.Snapshot(context.Background()); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	preset := fixtures.MakePreset()
	inst, err := agent.Load(context.Background(), domain.LoadRequest{Preset: preset, Claim: fixtures.MakeClaim(1, 1), AcceleratorSet: []int{0}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if inst.ID == "" || len(agent.Instances) != 1 {
		t.Fatalf("load state = %+v", agent)
	}
	if err := agent.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("Unload: %v", err)
	}
	if len(agent.Instances) != 0 {
		t.Fatalf("instances = %+v", agent.Instances)
	}
	agent.Metadata = domain.ModelMetadata{ModelRef: preset.ModelRef, Format: "gguf", WeightsMB: 1, KVPerTokenMB: 0.1, ContextLength: 10}
	metadata, err := agent.InspectModel(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("InspectModel: %v", err)
	}
	if metadata.Format != "gguf" {
		t.Fatalf("metadata = %+v", metadata)
	}

	loadErr := errors.New("load")
	agent.LoadErr = loadErr
	if _, err := agent.Load(context.Background(), domain.LoadRequest{Preset: preset, Claim: fixtures.MakeClaim(1, 1), AcceleratorSet: []int{0}}); !errors.Is(err, loadErr) {
		t.Fatalf("load error = %v", err)
	}
	unloadErr := errors.New("unload")
	agent.UnloadErr = unloadErr
	if err := agent.Unload(context.Background(), "missing"); !errors.Is(err, unloadErr) {
		t.Fatalf("unload error = %v", err)
	}
	inspectErr := errors.New("inspect")
	agent.InspectErr = inspectErr
	if _, err := agent.InspectModel(context.Background(), fixtures.MakePreset()); !errors.Is(err, inspectErr) {
		t.Fatalf("inspect error = %v", err)
	}
	agent.InspectErr = nil
	agent.Metadata = domain.ModelMetadata{}
	if _, err := agent.InspectModel(context.Background(), fixtures.MakePreset()); err == nil || !strings.Contains(err.Error(), "no metadata") {
		t.Fatalf("missing metadata err = %v", err)
	}
}

func TestResourceEstimatorAllocatorTelemetryAndClock(t *testing.T) {
	estimator := &ResourceEstimator{Claim: fixtures.MakeClaim(1, 2)}
	claim, err := estimator.Estimate(context.Background(), fixtures.MakePreset(), 10, 3)
	if err != nil || claim != fixtures.MakeClaim(1, 2) || len(estimator.Calls) != 1 {
		t.Fatalf("estimate = %+v %v calls=%+v", claim, err, estimator.Calls)
	}
	estErr := errors.New("estimate")
	estimator.Err = estErr
	if _, err := estimator.Estimate(context.Background(), fixtures.MakePreset(), 10, 1); !errors.Is(err, estErr) {
		t.Fatalf("estimate error = %v", err)
	}

	allocator := &Allocator{FitsVal: true, CanStackLoadVal: true}
	if !allocator.Fits(fixtures.MakeNode(), []int{0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("Fits should return configured value")
	}
	if !allocator.CanStackLoad(fixtures.MakeNode(), []int{0}, nil) {
		t.Fatal("CanStackLoad should return configured value")
	}
	if allocator.FitsCalls != 1 || allocator.CanStackCalls != 1 {
		t.Fatalf("allocator calls = %d/%d", allocator.FitsCalls, allocator.CanStackCalls)
	}
	if allocator.Fits(fixtures.MakeNode(), []int{0}, nil, domain.Claim{WeightsMB: -1}) {
		t.Fatal("allocator accepted negative claim")
	}
	if allocator.Fits(fixtures.MakeNode(), []int{0, 0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("allocator accepted duplicate accelerator request")
	}
	if allocator.Fits(fixtures.MakeNode(), []int{99}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("allocator accepted unavailable accelerator request")
	}
	catastrophic := fixtures.MakeNode(fixtures.WithNodeID("cat"), fixtures.Catastrophic)
	loading := domain.ModelInstance{NodeID: "cat", AcceleratorSet: []int{1, 0}, Loading: true}
	if allocator.CanStackLoad(catastrophic, []int{0}, []domain.ModelInstance{loading}) {
		t.Fatal("catastrophic allocator accepted overlapping loading instance")
	}
	if !allocator.CanStackLoad(catastrophic, []int{2}, []domain.ModelInstance{loading}) {
		t.Fatal("catastrophic allocator rejected non-overlapping load")
	}

	sink := &TelemetrySink{}
	metric := domain.RunMetric{JobID: "job"}
	if err := sink.Record(context.Background(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	sample := domain.SessionMetric{SessionID: "session", JobID: "job", Phase: domain.TelemetryPhasePlaced}
	if err := sink.RecordSample(context.Background(), sample); err != nil {
		t.Fatalf("RecordSample: %v", err)
	}
	if len(sink.SamplesOut) != 1 {
		t.Fatalf("samples = %+v", sink.SamplesOut)
	}
	recordErr := errors.New("record")
	sink.Err = recordErr
	if err := sink.Record(context.Background(), metric); !errors.Is(err, recordErr) {
		t.Fatalf("record error = %v", err)
	}
	sampleErr := errors.New("sample")
	sink.SampleErr = sampleErr
	if err := sink.RecordSample(context.Background(), sample); !errors.Is(err, sampleErr) {
		t.Fatalf("sample error = %v", err)
	}
	wantCalls := []string{"record:job", "sample:job:placed", "record:job", "sample:job:placed"}
	if strings.Join(sink.Calls, "\n") != strings.Join(wantCalls, "\n") {
		t.Fatalf("telemetry calls = %+v", sink.Calls)
	}

	peerClient := &TelemetryPeerClient{
		MetricsByPeer: map[string][]domain.RunMetric{
			"peer-a": []domain.RunMetric{{JobID: "job-a"}},
		},
		SamplesByPeer: map[string][]domain.SessionMetric{
			"peer-a": []domain.SessionMetric{{SessionID: "session-a"}},
		},
		RecommendationsByPeer: map[string][]domain.RecommendationRecord{
			"peer-a": []domain.RecommendationRecord{{ID: "rec-a"}},
		},
	}
	peer := domain.Peer{ID: "peer-a"}
	metrics, err := peerClient.Metrics(context.Background(), peer)
	if err != nil || len(metrics) != 1 || metrics[0].JobID != "job-a" {
		t.Fatalf("peer metrics = %+v %v", metrics, err)
	}
	if err := peerClient.PushMetrics(context.Background(), peer, []domain.RunMetric{{JobID: "job-b"}}); err != nil {
		t.Fatalf("PushMetrics: %v", err)
	}
	samples, err := peerClient.Samples(context.Background(), peer, domain.SessionMetricQuery{})
	if err != nil || len(samples) != 1 || samples[0].SessionID != "session-a" {
		t.Fatalf("peer samples = %+v %v", samples, err)
	}
	if err := peerClient.PushSamples(context.Background(), peer, []domain.SessionMetric{{SessionID: "session-b"}}); err != nil {
		t.Fatalf("PushSamples: %v", err)
	}
	recs, err := peerClient.Recommendations(context.Background(), peer)
	if err != nil || len(recs) != 1 || recs[0].ID != "rec-a" {
		t.Fatalf("peer recommendations = %+v %v", recs, err)
	}
	if err := peerClient.PushRecommendations(context.Background(), peer, []domain.RecommendationRecord{{ID: "rec-b"}}); err != nil {
		t.Fatalf("PushRecommendations: %v", err)
	}
	if len(peerClient.PushedMetrics["peer-a"]) != 1 || len(peerClient.PushedSamples["peer-a"]) != 1 || len(peerClient.PushedRecommendations["peer-a"]) != 1 {
		t.Fatalf("pushed telemetry = %+v %+v %+v", peerClient.PushedMetrics, peerClient.PushedSamples, peerClient.PushedRecommendations)
	}

	clock := NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	timer := clock.NewTimer(time.Second)
	if !timer.Stop() {
		t.Fatal("fresh timer should stop")
	}
	if timer.Stop() {
		t.Fatal("stopped timer should not stop twice")
	}
	clock.Advance(time.Second)
	if !clock.Now().Equal(time.Date(2026, 5, 29, 12, 0, 1, 0, time.UTC)) {
		t.Fatalf("now = %s", clock.Now())
	}
	live := clock.NewTimer(time.Second)
	if clock.TimerCount() != 2 {
		t.Fatalf("timer count = %d", clock.TimerCount())
	}
	clock.Advance(time.Second)
	if fired := <-live.C(); !fired.Equal(clock.Now()) {
		t.Fatalf("fired at %s want %s", fired, clock.Now())
	}
}

func TestTunnelMockRecordsAndFails(t *testing.T) {
	node := domain.Node{ID: "node-a", Address: "127.0.0.1:1"}
	tunnel := &Tunnel{Addr: "127.0.0.1:6000"}
	addr, err := tunnel.Open(context.Background(), node)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if addr != tunnel.Addr {
		t.Fatalf("addr = %s", addr)
	}
	if err := tunnel.Close(context.Background(), node.ID); err != nil {
		t.Fatalf("Close: %v", err)
	}
	tunnel.Err = errors.New("boom")
	if _, err := tunnel.Open(context.Background(), node); err == nil {
		t.Fatal("expected open error")
	}
	if err := tunnel.Close(context.Background(), node.ID); err == nil {
		t.Fatal("expected close error")
	}
}

func TestBootstrapMocksRecordProgressAndRegistryState(t *testing.T) {
	host := domain.HostFacts{NodeID: "host-a", Platform: "linux/amd64"}
	hostDetector := &HostDetector{Host: host}
	detected, err := hostDetector.DetectHost(context.Background(), domain.Node{ID: "seed-a"})
	if err != nil || detected.NodeID != host.NodeID || len(hostDetector.Calls) != 1 || hostDetector.Calls[0].ID != "seed-a" {
		t.Fatalf("DetectHost = %+v err=%v calls=%+v", detected, err, hostDetector.Calls)
	}

	profiles := []domain.EngineProfile{{ID: "openvino", Backend: domain.BackendOpenVINO, Ready: true}}
	engineDetector := &EngineDetector{Profiles: profiles}
	gotProfiles, err := engineDetector.DetectEngines(context.Background(), host)
	if err != nil || len(gotProfiles) != 1 || gotProfiles[0].ID != "openvino" || len(engineDetector.Calls) != 1 {
		t.Fatalf("DetectEngines = %+v err=%v calls=%+v", gotProfiles, err, engineDetector.Calls)
	}
	gotProfiles[0].ID = "mutated"
	again, err := engineDetector.DetectEngines(context.Background(), host)
	if err != nil || again[0].ID != "openvino" {
		t.Fatalf("DetectEngines did not return a defensive copy: %+v err=%v", again, err)
	}

	req := domain.BootstrapRequest{ID: "bootstrap-host-a", RequestedEngines: []domain.Backend{domain.BackendOpenVINO}}
	plan := domain.BootstrapPlan{ID: "plan-a", Host: host, ResultingProfiles: profiles}
	planner := &BootstrapPlanner{Plan: plan}
	gotPlan, err := planner.PlanBootstrap(context.Background(), req, host, profiles)
	if err != nil || gotPlan.ID != plan.ID || len(planner.Calls) != 1 || planner.Calls[0].ID != req.ID {
		t.Fatalf("PlanBootstrap = %+v err=%v calls=%+v", gotPlan, err, planner.Calls)
	}

	result := domain.BootstrapResult{PlanID: "plan-a", Events: []domain.BootstrapEvent{{PlanID: "plan-a", Phase: "verify", Message: "ok"}}}
	installer := &EngineInstaller{Result: result}
	var events []domain.BootstrapEvent
	gotResult, err := installer.ApplyBootstrapPlan(context.Background(), plan, func(event domain.BootstrapEvent) {
		events = append(events, event)
	})
	if err != nil || gotResult.PlanID != result.PlanID || len(events) != 1 || events[0].Phase != "verify" || len(installer.Calls) != 1 {
		t.Fatalf("ApplyBootstrapPlan = %+v events=%+v err=%v calls=%+v", gotResult, events, err, installer.Calls)
	}
	if _, err := installer.ApplyBootstrapPlan(context.Background(), plan, nil); err != nil {
		t.Fatalf("ApplyBootstrapPlan nil progress: %v", err)
	}

	verification := domain.EngineVerification{ProfileID: "openvino", Ready: true}
	verifier := &EngineVerifier{Verification: verification}
	gotVerification, err := verifier.VerifyEngine(context.Background(), profiles[0])
	if err != nil || !gotVerification.Ready || len(verifier.Calls) != 1 || verifier.Calls[0].ID != profiles[0].ID {
		t.Fatalf("VerifyEngine = %+v err=%v calls=%+v", gotVerification, err, verifier.Calls)
	}

	registry := &EngineRegistry{}
	if err := registry.SaveEngineProfile(context.Background(), profiles[0]); err != nil {
		t.Fatalf("SaveEngineProfile: %v", err)
	}
	listed, err := registry.ListEngineProfiles(context.Background())
	if err != nil || len(listed) != 1 || listed[0].Ready != true {
		t.Fatalf("ListEngineProfiles = %+v err=%v", listed, err)
	}
	listed[0].ID = "mutated"
	againProfiles, err := registry.ListEngineProfiles(context.Background())
	if err != nil || againProfiles[0].ID != profiles[0].ID {
		t.Fatalf("ListEngineProfiles did not return a defensive copy: %+v err=%v", againProfiles, err)
	}
	if err := registry.MarkEngineProfileUnready(context.Background(), "openvino", "probe failed"); err != nil {
		t.Fatalf("MarkEngineProfileUnready existing: %v", err)
	}
	if err := registry.MarkEngineProfileUnready(context.Background(), "vllm", "missing binary"); err != nil {
		t.Fatalf("MarkEngineProfileUnready append: %v", err)
	}
	listed, err = registry.ListEngineProfiles(context.Background())
	if err != nil || len(listed) != 2 || listed[0].Ready || listed[0].UnreadyReason != "probe failed" || listed[1].ID != "vllm" {
		t.Fatalf("unready profiles = %+v err=%v", listed, err)
	}

	boom := errors.New("boom")
	for name, run := range map[string]func() error{
		"host": func() error {
			hostDetector.Err = boom
			_, err := hostDetector.DetectHost(context.Background(), domain.Node{})
			return err
		},
		"engine": func() error {
			engineDetector.Err = boom
			_, err := engineDetector.DetectEngines(context.Background(), host)
			return err
		},
		"planner": func() error {
			planner.Err = boom
			_, err := planner.PlanBootstrap(context.Background(), req, host, nil)
			return err
		},
		"installer": func() error {
			installer.Err = boom
			_, err := installer.ApplyBootstrapPlan(context.Background(), plan, nil)
			return err
		},
		"verifier": func() error {
			verifier.Err = boom
			_, err := verifier.VerifyEngine(context.Background(), profiles[0])
			return err
		},
		"registry": func() error {
			registry.Err = boom
			return registry.SaveEngineProfile(context.Background(), profiles[0])
		},
	} {
		if err := run(); !errors.Is(err, boom) {
			t.Fatalf("%s err = %v", name, err)
		}
	}
	if _, err := registry.ListEngineProfiles(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("registry list err = %v", err)
	}
	if err := registry.MarkEngineProfileUnready(context.Background(), "openvino", "failed"); !errors.Is(err, boom) {
		t.Fatalf("registry unready err = %v", err)
	}
}

func TestFederationMocksRecordAndFail(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	claim := fixtures.MakeClaim(3, 4)
	admission := &AdmissionController{}
	offer, err := admission.Offer(context.Background(), domain.AdmissionRequest{Job: job, Claim: claim})
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	if offer.JobID != job.ID || offer.Claim != claim {
		t.Fatalf("offer = %+v", offer)
	}
	if _, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := admission.Release(context.Background(), "lease-a"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := admission.Preempt(context.Background(), "lease-a", "test"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("Preempt err = %v", err)
	}
	if err := admission.PreemptForJob(context.Background(), job, "lease-a", "test"); err != nil {
		t.Fatalf("PreemptForJob: %v", err)
	}
	admission.LeaseForInstVal = domain.Lease{ID: "lease-a", InstanceID: "inst-a"}
	admission.LeaseForInstFound = true
	if got, found, err := admission.LeaseForInstance(context.Background(), "inst-a"); err != nil || !found || got.ID != "lease-a" {
		t.Fatalf("LeaseForInstance = %+v %v %v", got, found, err)
	}
	if err := admission.BindInstance(context.Background(), "lease-a", "inst-a"); err != nil {
		t.Fatalf("BindInstance: %v", err)
	}
	admission.LeaseForJobVal = domain.Lease{ID: "lease-job", JobID: job.ID}
	admission.LeaseForJobFound = true
	if got, found, err := admission.LeaseForJob(context.Background(), job.ID); err != nil || !found || got.ID != "lease-job" {
		t.Fatalf("LeaseForJob = %+v %v %v", got, found, err)
	}
	if _, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence+1); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("stale fence err = %v", err)
	}
	if _, err := admission.Commit(context.Background(), "missing-offer", 1); err == nil || !strings.Contains(err.Error(), "unknown offer") {
		t.Fatalf("unknown offer err = %v", err)
	}

	stale := errors.New("stale")
	admission.CommitErr = stale
	if _, err := admission.Commit(context.Background(), offer.OfferID, offer.Fence); !errors.Is(err, stale) {
		t.Fatalf("commit err = %v", err)
	}

	coordinator := &Coordinator{Decision: domain.PlacementDecision{JobID: job.ID}, Lease: domain.Lease{ID: "lease-a"}}
	if err := coordinator.ClaimJob(context.Background(), job.ID); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	decision, err := coordinator.Plan(context.Background(), job.ID)
	if err != nil || decision.JobID != job.ID {
		t.Fatalf("Plan = %+v %v", decision, err)
	}
	outcome, err := coordinator.Commit(context.Background(), decision)
	if err != nil || outcome.Lease.ID != "lease-a" {
		t.Fatalf("Commit = %+v %v", outcome, err)
	}
	if err := coordinator.MarkRunning(context.Background(), job.ID); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	if err := coordinator.Release(context.Background(), job.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := coordinator.Complete(context.Background(), job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := coordinator.Fail(context.Background(), job.ID, errors.New("failed upstream")); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	coordinator.PlanErr = stale
	if _, err := coordinator.Plan(context.Background(), job.ID); !errors.Is(err, stale) {
		t.Fatalf("plan err = %v", err)
	}
	coordinator.PlanErr = nil
	coordinator.CommitErr = stale
	if _, err := coordinator.Commit(context.Background(), decision); !errors.Is(err, stale) {
		t.Fatalf("coordinator commit err = %v", err)
	}
	coordinator.CommitErr = nil
	coordinator.RunningErr = stale
	if err := coordinator.MarkRunning(context.Background(), job.ID); !errors.Is(err, stale) {
		t.Fatalf("running err = %v", err)
	}
	coordinator.ReleaseErr = stale
	if err := coordinator.Release(context.Background(), job.ID); !errors.Is(err, stale) {
		t.Fatalf("release err = %v", err)
	}
	coordinator.CompleteErr = stale
	if err := coordinator.Complete(context.Background(), job.ID); !errors.Is(err, stale) {
		t.Fatalf("complete err = %v", err)
	}
	coordinator.FailErr = stale
	if err := coordinator.Fail(context.Background(), job.ID, nil); !errors.Is(err, stale) {
		t.Fatalf("fail err = %v", err)
	}

	registry := &JobRegistry{}
	record := fixtures.MakeJobRecord(fixtures.WithRecordJobID(job.ID))
	if err := registry.Put(context.Background(), record); err != nil {
		t.Fatalf("Put: %v", err)
	}
	records, err := registry.Snapshot(context.Background())
	if err != nil || len(records) != 1 || records[0].JobID != job.ID {
		t.Fatalf("Snapshot = %+v %v", records, err)
	}
	watch, err := registry.Watch(context.Background(), "")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case rec, ok := <-watch:
		if !ok || rec.JobID != job.ID {
			t.Fatalf("default watch replay = %+v open=%v", rec, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("default watch did not replay existing record")
	}
	cursorWatch, err := registry.Watch(context.Background(), record.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("cursor Watch: %v", err)
	}
	select {
	case rec, ok := <-cursorWatch:
		t.Fatalf("cursor watch produced %+v open=%v", rec, ok)
	default:
	}
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	cancelableWatch, err := registry.Watch(watchCtx, record.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("cancelable Watch: %v", err)
	}
	cancelWatch()
	select {
	case _, ok := <-cancelableWatch:
		if ok {
			t.Fatal("cancelable watch remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelable watch did not close")
	}
	canceledCtx, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if _, err := registry.Watch(canceledCtx, record.UpdatedAt.UTC().Format(time.RFC3339Nano)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled watch err = %v", err)
	}
	customWatch := make(chan domain.JobRecord, 1)
	registry.WatchErr = nil
	registry.WatchCh = customWatch
	if got, err := registry.Watch(context.Background(), record.UpdatedAt.UTC().Format(time.RFC3339Nano)); err != nil || got != customWatch {
		t.Fatalf("custom watch = %v %v", got, err)
	}
	registry.WatchCh = nil

	peers := &PeerDiscovery{}
	peer := fixtures.MakePeer(fixtures.WithPeerID("peer-a"))
	if err := peers.Advertise(context.Background(), peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	gotPeers, err := peers.Peers(context.Background())
	if err != nil || len(gotPeers) != 1 || gotPeers[0].ID != peer.ID {
		t.Fatalf("Peers = %+v %v", gotPeers, err)
	}
	peerWatch, err := peers.WatchPeers(context.Background())
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	select {
	case peer, ok := <-peerWatch:
		t.Fatalf("default peer watch channel produced %+v open=%v", peer, ok)
	default:
	}
	peerWatchCtx, cancelPeerWatch := context.WithCancel(context.Background())
	cancelablePeerWatch, err := peers.WatchPeers(peerWatchCtx)
	if err != nil {
		t.Fatalf("cancelable WatchPeers: %v", err)
	}
	cancelPeerWatch()
	select {
	case _, ok := <-cancelablePeerWatch:
		if ok {
			t.Fatal("cancelable peer watch remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelable peer watch did not close")
	}
	peerCanceledCtx, cancelPeerCanceled := context.WithCancel(context.Background())
	cancelPeerCanceled()
	if _, err := peers.WatchPeers(peerCanceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled peer watch err = %v", err)
	}
	customPeerWatch := make(chan domain.Peer, 1)
	peers.WatchCh = customPeerWatch
	if got, err := peers.WatchPeers(context.Background()); err != nil || got != customPeerWatch {
		t.Fatalf("custom peer watch = %v %v", got, err)
	}
	peers.WatchCh = nil

	boom := errors.New("boom")
	admission.OfferErr = boom
	if _, err := admission.Offer(context.Background(), domain.AdmissionRequest{Job: job, Claim: claim}); !errors.Is(err, boom) {
		t.Fatalf("offer err = %v", err)
	}
	registry.Err = boom
	if err := registry.Put(context.Background(), record); !errors.Is(err, boom) {
		t.Fatalf("put err = %v", err)
	}
	if _, err := registry.Snapshot(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("snapshot err = %v", err)
	}
	registry.WatchErr = boom
	if _, err := registry.Watch(context.Background(), record.UpdatedAt.UTC().Format(time.RFC3339Nano)); !errors.Is(err, boom) {
		t.Fatalf("watch err = %v", err)
	}
	peers.Err = boom
	if err := peers.Advertise(context.Background(), peer); !errors.Is(err, boom) {
		t.Fatalf("advertise err = %v", err)
	}
	if _, err := peers.Peers(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("peers err = %v", err)
	}
	peers.WatchErr = boom
	if _, err := peers.WatchPeers(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("watch peers err = %v", err)
	}
}

func TestJobRegistryPreservesOwnerRecordOrderingAndTerminalState(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	registry := &JobRegistry{}
	initial := domain.JobRecord{JobID: "job-a", Status: domain.JobRunning, Fence: 1, Coordinator: "coord-a", UpdatedAt: base, Request: []byte("first")}
	if err := registry.Put(context.Background(), initial); err != nil {
		t.Fatalf("Put initial: %v", err)
	}
	stale := initial
	stale.Fence = 0
	stale.Request = []byte("stale")
	if err := registry.Put(context.Background(), stale); err != nil {
		t.Fatalf("Put stale: %v", err)
	}
	terminal := initial
	terminal.Status = domain.JobDone
	terminal.Fence = 2
	terminal.Request = []byte("done")
	terminal.UpdatedAt = base.Add(time.Second)
	if err := registry.Put(context.Background(), terminal); err != nil {
		t.Fatalf("Put terminal: %v", err)
	}
	regression := terminal
	regression.Status = domain.JobRunning
	regression.Fence = 3
	regression.Request = []byte("running-again")
	regression.UpdatedAt = base.Add(2 * time.Second)
	if err := registry.Put(context.Background(), regression); err != nil {
		t.Fatalf("Put regression: %v", err)
	}

	records, err := registry.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(records) != 1 || records[0].Status != domain.JobDone || string(records[0].Request) != "done" {
		t.Fatalf("terminal record regressed: %+v", records)
	}
	records[0].Request[0] = 'X'
	again, err := registry.Snapshot(context.Background())
	if err != nil || string(again[0].Request) != "done" {
		t.Fatalf("snapshot did not clone request bytes: %+v err=%v", again, err)
	}

	sameTimeB := domain.JobRecord{JobID: "job-b", Status: domain.JobRunning, Fence: 1, UpdatedAt: base.Add(3 * time.Second)}
	sameTimeC := domain.JobRecord{JobID: "job-c", Status: domain.JobRunning, Fence: 1, UpdatedAt: base.Add(3 * time.Second)}
	older := domain.JobRecord{JobID: "job-old", Status: domain.JobRunning, Fence: 1, UpdatedAt: base.Add(-time.Second)}
	for _, rec := range []domain.JobRecord{sameTimeC, older, sameTimeB} {
		if err := registry.Put(context.Background(), rec); err != nil {
			t.Fatalf("Put %s: %v", rec.JobID, err)
		}
	}
	snapshot, err := registry.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot ordered: %v", err)
	}
	gotOrder := []string{snapshot[0].JobID, snapshot[1].JobID, snapshot[2].JobID, snapshot[3].JobID}
	wantOrder := []string{"job-old", "job-a", "job-b", "job-c"}
	if strings.Join(gotOrder, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("snapshot order = %v want %v", gotOrder, wantOrder)
	}

	watch, err := registry.Watch(context.Background(), base.Add(2*time.Second).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("Watch cursor: %v", err)
	}
	for _, want := range []string{"job-b", "job-c"} {
		select {
		case rec := <-watch:
			if rec.JobID != want {
				t.Fatalf("watch rec = %s want %s", rec.JobID, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("watch timed out waiting for %s", want)
		}
	}
}

func TestNodeHardwareAndTelemetryMockFailureBranches(t *testing.T) {
	boom := errors.New("boom")
	agent := NewNodeAgent(fixtures.MakeNode())
	if err := agent.BeginRequest(context.Background(), "inst-a"); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	if err := agent.EndRequest(context.Background(), "inst-a"); err != nil {
		t.Fatalf("EndRequest: %v", err)
	}
	agent.BeginErr = boom
	if err := agent.BeginRequest(context.Background(), "inst-a"); !errors.Is(err, boom) {
		t.Fatalf("begin err = %v", err)
	}
	agent.EndErr = boom
	if err := agent.EndRequest(context.Background(), "inst-a"); !errors.Is(err, boom) {
		t.Fatalf("end err = %v", err)
	}

	detector := &HardwareDetector{}
	seed := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	node, err := detector.Detect(context.Background(), seed)
	if err != nil || node.ID != seed.ID || len(node.Accelerators) == 0 {
		t.Fatalf("Detect = %+v %v", node, err)
	}
	emptySeed := domain.Node{ID: "empty-hardware"}
	empty, err := detector.Detect(context.Background(), emptySeed)
	if err != nil {
		t.Fatalf("Detect empty: %v", err)
	}
	if len(empty.Accelerators) != 0 || empty.UnifiedMemory {
		t.Fatalf("mock invented hardware facts: %+v", empty)
	}
	detector.Err = boom
	if _, err := detector.Detect(context.Background(), seed); !errors.Is(err, boom) {
		t.Fatalf("detect err = %v", err)
	}

	peer := domain.Peer{ID: "peer-a"}
	client := &TelemetryPeerClient{MetricsErr: boom}
	if _, err := client.Metrics(context.Background(), peer); !errors.Is(err, boom) {
		t.Fatalf("metrics err = %v", err)
	}
	client = &TelemetryPeerClient{PushMetricsErr: boom}
	if err := client.PushMetrics(context.Background(), peer, []domain.RunMetric{{JobID: "job"}}); !errors.Is(err, boom) {
		t.Fatalf("push metrics err = %v", err)
	}
	client = &TelemetryPeerClient{SamplesErr: boom}
	if _, err := client.Samples(context.Background(), peer, domain.SessionMetricQuery{}); !errors.Is(err, boom) {
		t.Fatalf("samples err = %v", err)
	}
	client = &TelemetryPeerClient{PushSamplesErr: boom}
	if err := client.PushSamples(context.Background(), peer, []domain.SessionMetric{{SessionID: "session"}}); !errors.Is(err, boom) {
		t.Fatalf("push samples err = %v", err)
	}
	client = &TelemetryPeerClient{RecommendationsErr: boom}
	if _, err := client.Recommendations(context.Background(), peer); !errors.Is(err, boom) {
		t.Fatalf("recommendations err = %v", err)
	}
	client = &TelemetryPeerClient{PushRecommendationsErr: boom}
	if err := client.PushRecommendations(context.Background(), peer, []domain.RecommendationRecord{{ID: "rec"}}); !errors.Is(err, boom) {
		t.Fatalf("push recommendations err = %v", err)
	}
}
