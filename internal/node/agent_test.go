package node

import (
	"context"
	"errors"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAgentConformance(t *testing.T) {
	contract.RunNodeAgentConformance(t, "node-agent",
		func() ports.NodeAgent {
			return NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
		},
		fixtures.MakePreset())
}

func loadReq(p domain.Preset, acc ...int) domain.LoadRequest {
	if len(acc) == 0 {
		acc = []int{0}
	}
	return domain.LoadRequest{
		Preset:         p,
		Claim:          domain.Claim{WeightsMB: p.EstWeightsMB, KVReservedMB: 1},
		AcceleratorSet: append([]int(nil), acc...),
	}
}

func TestLoadReadinessGatesInstanceAndUnloadStopsBackend(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithListenAddr("127.0.0.1:1234"), WithAllocator(lease.NewAllocator()))

	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if inst.State != domain.InstReady || inst.Loading || inst.Addr == "" {
		t.Fatalf("instance = %+v", inst)
	}
	if inst.Addr != "127.0.0.1:1234" {
		t.Fatalf("addr = %s", inst.Addr)
	}
	snap, err := agent.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].ID != inst.ID {
		t.Fatalf("snapshot = %+v", snap)
	}
	instances := agent.Instances()
	if len(instances) != 1 || instances[0].ID != inst.ID {
		t.Fatalf("instances = %+v", instances)
	}
	instances[0].ID = "mutated"
	if got := agent.Instances()[0].ID; got != inst.ID {
		t.Fatalf("Instances returned mutable backing state: %s", got)
	}
	if err := agent.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("Unload: %v", err)
	}
	if last := backend.Calls[len(backend.Calls)-1]; last.Op != "stop" {
		t.Fatalf("last backend call = %+v", last)
	}
}

func TestLoadAllocatesConcreteAddressPerPresetWhenListenPortIsZero(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithListenAddr("127.0.0.1:0"), WithAllocator(lease.NewAllocator()))
	firstPreset := fixtures.MakePreset(fixtures.WithPresetID("first"), fixtures.WithWeights(1), fixtures.WithKVPerToken(0.01))
	secondPreset := fixtures.MakePreset(fixtures.WithPresetID("second"), fixtures.WithWeights(1), fixtures.WithKVPerToken(0.01))

	first, err := agent.Load(context.Background(), loadReq(firstPreset))
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	second, err := agent.Load(context.Background(), loadReq(secondPreset))
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if first.Addr == second.Addr || strings.HasSuffix(first.Addr, ":0") || strings.HasSuffix(second.Addr, ":0") {
		t.Fatalf("allocated addrs first=%q second=%q", first.Addr, second.Addr)
	}
	var launchAddrs []string
	for _, call := range backend.Calls {
		if call.Op == "launch" {
			launchAddrs = append(launchAddrs, call.Addr)
		}
	}
	if len(launchAddrs) != 2 || launchAddrs[0] != first.Addr || launchAddrs[1] != second.Addr {
		t.Fatalf("launch addrs=%+v first=%s second=%s", launchAddrs, first.Addr, second.Addr)
	}
}

func TestLoadReusesReadyInstance(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset()

	first, err := agent.Load(context.Background(), loadReq(preset))
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	second, err := agent.Load(context.Background(), loadReq(preset))
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected same warm instance, got %s and %s", first.ID, second.ID)
	}
	if countCalls(backend.Calls, "launch") != 1 {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
}

func TestLoadUsesAcceleratorSetAsRuntimeUnit(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	node := fixtures.MakeNode()
	node.Accelerators = []domain.Accelerator{
		{Index: 0, Vendor: "nvidia", Kind: "rtx4090", VRAMTotalMB: 24576},
		{Index: 1, Vendor: "nvidia", Kind: "rtx4090", VRAMTotalMB: 24576},
	}
	agent := NewAgent(node, backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset()

	first, err := agent.Load(context.Background(), loadReq(preset, 0))
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	second, err := agent.Load(context.Background(), loadReq(preset, 1))
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	third, err := agent.Load(context.Background(), loadReq(preset, 1))
	if err != nil {
		t.Fatalf("third Load: %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("different accelerator sets reused one instance: first=%+v second=%+v", first, second)
	}
	if second.ID != third.ID {
		t.Fatalf("same accelerator set did not reuse warm instance: second=%+v third=%+v", second, third)
	}
	if !sameAcceleratorSet(first.AcceleratorSet, []int{0}) || !sameAcceleratorSet(second.AcceleratorSet, []int{1}) {
		t.Fatalf("accelerator sets not preserved: first=%+v second=%+v", first.AcceleratorSet, second.AcceleratorSet)
	}
	if countCalls(backend.Calls, "launch") != 2 {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
}

func TestConcurrentColdLoadsDeduplicate(t *testing.T) {
	backend := newBlockingBackend()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset()

	var wg sync.WaitGroup
	results := make(chan domain.ModelInstance, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst, err := agent.Load(context.Background(), loadReq(preset))
			if err != nil {
				t.Errorf("Load: %v", err)
				return
			}
			results <- inst
		}()
	}
	<-backend.waiting
	backend.release()
	wg.Wait()
	close(results)

	var ids []string
	for inst := range results {
		ids = append(ids, inst.ID)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("dedup ids = %+v", ids)
	}
	if backend.launches != 1 {
		t.Fatalf("launches = %d", backend.launches)
	}
}

func TestLoadFailureRemovesLoadingInstanceAndStopsHandle(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	backend.ReadyAfter = 1
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))

	_, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err == nil {
		t.Fatal("expected readiness error")
	}
	snap, _ := agent.Snapshot(context.Background())
	if len(snap.Instances) != 0 {
		t.Fatalf("failed load left instance: %+v", snap.Instances)
	}
	if countCalls(backend.Calls, "stop") != 1 {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
}

func TestLoadTimeoutUsesInjectedClock(t *testing.T) {
	backend := newBlockingBackend()
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	agent := NewAgent(fixtures.MakeNode(), backend, clock, WithAllocator(lease.NewAllocator()), WithLoadTimeout(time.Second))

	done := make(chan error, 1)
	go func() {
		_, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
		done <- err
	}()
	<-backend.waiting
	clock.Advance(time.Second)
	err := <-done
	if err == nil {
		t.Fatal("expected load timeout error")
	}
	snap, _ := agent.Snapshot(context.Background())
	if len(snap.Instances) != 0 {
		t.Fatalf("timed-out load left instance: %+v", snap.Instances)
	}
}

func TestUnloadUnknownAndStopFailure(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	if err := agent.Unload(context.Background(), "missing"); err == nil {
		t.Fatal("expected unknown instance error")
	}

	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	backend.StopErr = errors.New("stop failed")
	if err := agent.Unload(context.Background(), inst.ID); err == nil {
		t.Fatal("expected stop error")
	}
}

func TestShutdownUnloadsAllInstances(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	first, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset(fixtures.WithPresetID("first"))))
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	second, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset(fixtures.WithPresetID("second"))))
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if err := agent.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := countCalls(backend.Calls, "stop"); got != 2 {
		t.Fatalf("stop calls = %d calls=%+v", got, backend.Calls)
	}
	snap, err := agent.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 0 {
		t.Fatalf("shutdown left instances = %+v first=%s second=%s", snap.Instances, first.ID, second.ID)
	}
}

func TestShutdownReturnsUnloadErrors(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	if _, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset())); err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantErr := errors.New("stop failed")
	backend.StopErr = wantErr
	if err := agent.Shutdown(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Shutdown err = %v", err)
	}
}

func TestInFlightRequestsGuardUnload(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := agent.BeginRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- agent.Unload(context.Background(), inst.ID)
	}()
	for {
		agent.mu.Lock()
		stopping := agent.inflight[inst.ID].stopping
		agent.mu.Unlock()
		if stopping {
			break
		}
		runtime.Gosched()
	}
	select {
	case err := <-done:
		t.Fatalf("Unload finished with in-flight request: %v", err)
	default:
	}
	if err := agent.BeginRequest(context.Background(), inst.ID); err == nil || !strings.Contains(err.Error(), "stopping") {
		t.Fatalf("expected stopping error, got %v", err)
	}
	if err := agent.EndRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("EndRequest: %v", err)
	}
	unloaded := false
	for i := 0; i < 1000; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Unload: %v", err)
			}
			unloaded = true
		default:
			runtime.Gosched()
		}
		if unloaded {
			break
		}
	}
	if !unloaded {
		t.Fatal("Unload did not drain")
	}
	if countCalls(backend.Calls, "stop") != 1 {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
	if err := agent.EndRequest(context.Background(), inst.ID); err == nil {
		t.Fatal("expected extra EndRequest error")
	}
}

func TestSnapshotReportsInFlightRequests(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := agent.BeginRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	snap, err := agent.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].InFlight != 1 {
		t.Fatalf("snapshot = %+v", snap)
	}
	instances := agent.Instances()
	if len(instances) != 1 || instances[0].InFlight != 1 {
		t.Fatalf("instances = %+v", instances)
	}
	if err := agent.EndRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("EndRequest: %v", err)
	}
}

func TestBeginRequestRejectsMissingUnreadyAndCanceled(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	if err := agent.BeginRequest(context.Background(), "missing"); err == nil {
		t.Fatal("expected missing instance error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := agent.BeginRequest(ctx, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled begin err = %v", err)
	}
	loading := domain.ModelInstance{ID: "loading", State: domain.InstLoading}
	agent.mu.Lock()
	agent.instances[loading.ID] = loading
	agent.mu.Unlock()
	if err := agent.BeginRequest(context.Background(), loading.ID); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("loading begin err = %v", err)
	}
}

func TestLoadFailsLoudWithoutAllocatorOrCapacity(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	_, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err == nil || !strings.Contains(err.Error(), "allocator") {
		t.Fatalf("missing allocator err = %v", err)
	}
	if countCalls(backend.Calls, "launch") != 0 {
		t.Fatalf("backend launched despite missing allocator: %+v", backend.Calls)
	}

	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5))
	agent = NewAgent(node, backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	_, err = agent.Load(context.Background(), loadReq(fixtures.MakePreset(fixtures.WithWeights(1000), fixtures.WithKVPerToken(0))))
	if !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("saturation err = %v", err)
	}
}

func TestCatastrophicNodeShedsSecondColdLoadWhileFirstLoads(t *testing.T) {
	backend := newBlockingBackend()
	agent := NewAgent(fixtures.MakeSparkNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	firstPreset := fixtures.MakePreset(fixtures.WithPresetID("first"))
	secondPreset := fixtures.MakePreset(fixtures.WithPresetID("second"))

	done := make(chan error, 1)
	go func() {
		_, err := agent.Load(context.Background(), loadReq(firstPreset))
		done <- err
	}()
	<-backend.waiting
	_, err := agent.Load(context.Background(), loadReq(secondPreset))
	if !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("second load err = %v", err)
	}
	backend.release()
	if err := <-done; err != nil {
		t.Fatalf("first load err = %v", err)
	}
	if backend.launches != 1 {
		t.Fatalf("launches = %d", backend.launches)
	}
}

func TestRecordRunEmitsTelemetryWithNodeAndClock(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	sink := &mocks.TelemetrySink{}
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), clock, WithTelemetrySink(sink), WithAllocator(lease.NewAllocator()))
	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	err = agent.RecordRun(context.Background(), domain.RunMetric{
		JobID:      "job",
		InstanceID: inst.ID,
		Project:    "project",
	})
	if err != nil {
		t.Fatalf("RecordRun: %v", err)
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	got := sink.Metrics[0]
	if got.NodeID != "node_test" || got.PresetID != inst.PresetID || got.At != clock.Now() {
		t.Fatalf("metric = %+v", got)
	}
}

func TestRecordRunFailsWithoutSinkOrInstance(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	if err := agent.RecordRun(context.Background(), domain.RunMetric{InstanceID: "missing"}); err == nil {
		t.Fatal("expected missing sink error")
	}

	agent = NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithTelemetrySink(&mocks.TelemetrySink{}))
	if err := agent.RecordRun(context.Background(), domain.RunMetric{InstanceID: "missing"}); err == nil {
		t.Fatal("expected missing instance error")
	}
}

func TestProtectInstanceMarksPinnedReservation(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	inst, err := agent.Load(context.Background(), loadReq(fixtures.MakePreset()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := agent.ProtectInstance(inst.ID, "pin-a"); err != nil {
		t.Fatalf("ProtectInstance: %v", err)
	}
	snap, err := agent.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || !snap.Instances[0].Pinned || snap.Instances[0].ReservationID != "pin-a" {
		t.Fatalf("instances = %+v", snap.Instances)
	}
	if err := agent.ProtectInstance("", "pin-a"); err == nil {
		t.Fatal("empty instance id accepted")
	}
	if err := agent.ProtectInstance(inst.ID, ""); err == nil {
		t.Fatal("empty reservation id accepted")
	}
	if err := agent.ProtectInstance("missing", "pin-a"); err == nil {
		t.Fatal("missing instance accepted")
	}
	if err := agent.ProtectInstance(inst.ID, "pin-b"); err == nil {
		t.Fatal("conflicting reservation accepted")
	}
}

func TestInspectModelUsesConfiguredInspectorOrPresetMetadata(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithModelInspector(StaticInspector{
		Metadata: domain.ModelMetadata{ModelRef: "model", Format: "gguf", WeightsMB: 10, KVPerTokenMB: 0.1, ContextLength: 2048},
	}))
	metadata, err := agent.InspectModel(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("InspectModel configured: %v", err)
	}
	if metadata.Format != "gguf" || metadata.WeightsMB != 10 {
		t.Fatalf("metadata = %+v", metadata)
	}

	agent = NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	metadata, err = agent.InspectModel(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("InspectModel preset fallback: %v", err)
	}
	if metadata.Format != "preset" || metadata.WeightsMB == 0 {
		t.Fatalf("metadata = %+v", metadata)
	}

	_, err = agent.InspectModel(context.Background(), domain.Preset{ID: "empty"})
	if err == nil {
		t.Fatal("expected missing inspector error")
	}
}

func TestInspectModelPropagatesInspectorError(t *testing.T) {
	wantErr := errors.New("inspect")
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithModelInspector(StaticInspector{Err: wantErr}))
	if _, err := agent.InspectModel(context.Background(), fixtures.MakePreset()); !errors.Is(err, wantErr) {
		t.Fatalf("InspectModel err = %v", err)
	}
}

func TestParserInspectorUsesParser(t *testing.T) {
	parser := parserFunc(func(context.Context, string) (domain.ModelMetadata, error) {
		return domain.ModelMetadata{ModelRef: "model.gguf", WeightsMB: 7, KVPerTokenMB: 0.02}, nil
	})
	metadata, err := (ParserInspector{Parser: parser}).InspectModel(context.Background(), fixtures.MakePreset(fixtures.WithModelRef("model.gguf")))
	if err != nil {
		t.Fatalf("InspectModel: %v", err)
	}
	if metadata.WeightsMB != 7 {
		t.Fatalf("metadata = %+v", metadata)
	}
	if _, err := (ParserInspector{}).InspectModel(context.Background(), fixtures.MakePreset()); !errors.Is(err, domain.ErrUnsupported) {
		t.Fatalf("missing parser err = %v", err)
	}
}

type parserFunc func(context.Context, string) (domain.ModelMetadata, error)

func (f parserFunc) Parse(ctx context.Context, modelRef string) (domain.ModelMetadata, error) {
	return f(ctx, modelRef)
}

func TestWaitingLoadRespectsContextCancellation(t *testing.T) {
	op := &loadOp{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waitLoad(ctx, op); err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestWriteLoadingStateSetsNoBufferHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteLoadingState(rec, "loading model")
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("headers = %+v", rec.Header())
	}
	if !strings.Contains(rec.Body.String(), "loading model") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func countCalls(calls []mocks.BackendCall, op string) int {
	var count int
	for _, call := range calls {
		if call.Op == op {
			count++
		}
	}
	return count
}

type blockingBackend struct {
	mu       sync.Mutex
	launches int
	waiting  chan struct{}
	releaseC chan struct{}
}

func newBlockingBackend() *blockingBackend {
	return &blockingBackend{waiting: make(chan struct{}, 1), releaseC: make(chan struct{})}
}

func (b *blockingBackend) Name() string {
	return "blocking"
}

func (b *blockingBackend) Launch(context.Context, domain.Preset, string) (ports.Handle, error) {
	b.mu.Lock()
	b.launches++
	b.mu.Unlock()
	return ports.Handle{PID: 1, Addr: "127.0.0.1:1", Kind: "process", Ref: "1"}, nil
}

func (b *blockingBackend) WaitReady(ctx context.Context, _ string) error {
	b.waiting <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.releaseC:
		return nil
	}
}

func (b *blockingBackend) Stop(context.Context, ports.Handle) error {
	return nil
}

func (b *blockingBackend) release() {
	close(b.releaseC)
}

var _ ports.BackendAdapter = (*blockingBackend)(nil)
