package mocks

import (
	"context"
	"errors"
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
	if len(backend.Calls) != 4 {
		t.Fatalf("calls = %+v", backend.Calls)
	}

	launchErr := errors.New("launch")
	backend = &BackendAdapter{LaunchErr: launchErr}
	if _, err := backend.Launch(context.Background(), fixtures.MakePreset(), "addr"); !errors.Is(err, launchErr) {
		t.Fatalf("launch error = %v", err)
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
	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
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
	metadata, err := agent.InspectModel(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("InspectModel: %v", err)
	}
	if metadata.Format != "gguf" {
		t.Fatalf("metadata = %+v", metadata)
	}

	loadErr := errors.New("load")
	agent.LoadErr = loadErr
	if _, err := agent.Load(context.Background(), fixtures.MakePreset()); !errors.Is(err, loadErr) {
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

	sink := &TelemetrySink{}
	metric := domain.RunMetric{JobID: "job"}
	if err := sink.Record(context.Background(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	recordErr := errors.New("record")
	sink.Err = recordErr
	if err := sink.Record(context.Background(), metric); !errors.Is(err, recordErr) {
		t.Fatalf("record error = %v", err)
	}

	peerClient := &TelemetryPeerClient{
		MetricsByPeer: map[string][]domain.RunMetric{
			"peer-a": []domain.RunMetric{{JobID: "job-a"}},
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
	recs, err := peerClient.Recommendations(context.Background(), peer)
	if err != nil || len(recs) != 1 || recs[0].ID != "rec-a" {
		t.Fatalf("peer recommendations = %+v %v", recs, err)
	}
	if err := peerClient.PushRecommendations(context.Background(), peer, []domain.RecommendationRecord{{ID: "rec-b"}}); err != nil {
		t.Fatalf("PushRecommendations: %v", err)
	}
	if len(peerClient.PushedMetrics["peer-a"]) != 1 || len(peerClient.PushedRecommendations["peer-a"]) != 1 {
		t.Fatalf("pushed telemetry = %+v %+v", peerClient.PushedMetrics, peerClient.PushedRecommendations)
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

func TestFederationMocksRecordAndFail(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	claim := fixtures.MakeClaim(3, 4)
	admission := &AdmissionController{}
	offer, err := admission.Offer(context.Background(), job, claim)
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
	if err := admission.Preempt(context.Background(), "lease-a", "test"); err != nil {
		t.Fatalf("Preempt: %v", err)
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
	lease, err := coordinator.Commit(context.Background(), decision)
	if err != nil || lease.ID != "lease-a" {
		t.Fatalf("Commit = %+v %v", lease, err)
	}
	if err := coordinator.Release(context.Background(), job.ID); err != nil {
		t.Fatalf("Release: %v", err)
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
	if _, ok := <-watch; ok {
		t.Fatal("default watch channel should be closed")
	}

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
	if _, ok := <-peerWatch; ok {
		t.Fatal("default peer watch channel should be closed")
	}

	boom := errors.New("boom")
	admission.OfferErr = boom
	if _, err := admission.Offer(context.Background(), job, claim); !errors.Is(err, boom) {
		t.Fatalf("offer err = %v", err)
	}
	registry.Err = boom
	if err := registry.Put(context.Background(), record); !errors.Is(err, boom) {
		t.Fatalf("put err = %v", err)
	}
	registry.WatchErr = boom
	if _, err := registry.Watch(context.Background(), "cursor"); !errors.Is(err, boom) {
		t.Fatalf("watch err = %v", err)
	}
	peers.Err = boom
	if err := peers.Advertise(context.Background(), peer); !errors.Is(err, boom) {
		t.Fatalf("advertise err = %v", err)
	}
	peers.WatchErr = boom
	if _, err := peers.WatchPeers(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("watch peers err = %v", err)
	}
}
