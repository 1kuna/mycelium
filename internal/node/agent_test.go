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

func TestLoadReadinessGatesInstanceAndUnloadStopsBackend(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithListenAddr("127.0.0.1:1234"), WithAllocator(lease.NewAllocator()))

	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
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
	if err := agent.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("Unload: %v", err)
	}
	if last := backend.Calls[len(backend.Calls)-1]; last.Op != "stop" {
		t.Fatalf("last backend call = %+v", last)
	}
}

func TestLoadReusesReadyInstance(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset()

	first, err := agent.Load(context.Background(), preset)
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	second, err := agent.Load(context.Background(), preset)
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

func TestConcurrentColdLoadsDeduplicate(t *testing.T) {
	backend := newBlockingBackend()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	preset := fixtures.MakePreset()

	var wg sync.WaitGroup
	results := make(chan domain.ModelInstance, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst, err := agent.Load(context.Background(), preset)
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))

	_, err := agent.Load(context.Background(), fixtures.MakePreset())
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
		_, err := agent.Load(context.Background(), fixtures.MakePreset())
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	if err := agent.Unload(context.Background(), "missing"); err == nil {
		t.Fatal("expected unknown instance error")
	}

	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	backend.StopErr = errors.New("stop failed")
	if err := agent.Unload(context.Background(), inst.ID); err == nil {
		t.Fatal("expected stop error")
	}
}

func TestInFlightRequestsGuardUnload(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
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
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Unload: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unload did not drain")
	}
	if countCalls(backend.Calls, "stop") != 1 {
		t.Fatalf("backend calls = %+v", backend.Calls)
	}
	if err := agent.EndRequest(context.Background(), inst.ID); err == nil {
		t.Fatal("expected extra EndRequest error")
	}
}

func TestBeginRequestRejectsMissingUnreadyAndCanceled(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()))
	_, err := agent.Load(context.Background(), fixtures.MakePreset())
	if err == nil || !strings.Contains(err.Error(), "allocator") {
		t.Fatalf("missing allocator err = %v", err)
	}
	if countCalls(backend.Calls, "launch") != 0 {
		t.Fatalf("backend launched despite missing allocator: %+v", backend.Calls)
	}

	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5))
	agent = NewAgent(node, backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	_, err = agent.Load(context.Background(), fixtures.MakePreset(fixtures.WithWeights(1000), fixtures.WithKVPerToken(0)))
	if !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("saturation err = %v", err)
	}
}

func TestCatastrophicNodeShedsSecondColdLoadWhileFirstLoads(t *testing.T) {
	backend := newBlockingBackend()
	agent := NewAgent(fixtures.MakeSparkNode(), backend, mocks.NewFakeClock(time.Now()), WithAllocator(lease.NewAllocator()))
	firstPreset := fixtures.MakePreset(fixtures.WithPresetID("first"))
	secondPreset := fixtures.MakePreset(fixtures.WithPresetID("second"))

	done := make(chan error, 1)
	go func() {
		_, err := agent.Load(context.Background(), firstPreset)
		done <- err
	}()
	<-backend.waiting
	_, err := agent.Load(context.Background(), secondPreset)
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
	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
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
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()))
	if err := agent.RecordRun(context.Background(), domain.RunMetric{InstanceID: "missing"}); err == nil {
		t.Fatal("expected missing sink error")
	}

	agent = NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()), WithTelemetrySink(&mocks.TelemetrySink{}))
	if err := agent.RecordRun(context.Background(), domain.RunMetric{InstanceID: "missing"}); err == nil {
		t.Fatal("expected missing instance error")
	}
}

func TestInspectModelUsesConfiguredInspectorOrPresetMetadata(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()), WithModelInspector(StaticInspector{
		Metadata: domain.ModelMetadata{ModelRef: "model", Format: "gguf", WeightsMB: 10, KVPerTokenMB: 0.1, ContextLength: 2048},
	}))
	metadata, err := agent.InspectModel(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("InspectModel configured: %v", err)
	}
	if metadata.Format != "gguf" || metadata.WeightsMB != 10 {
		t.Fatalf("metadata = %+v", metadata)
	}

	agent = NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()))
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
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Now()), WithModelInspector(StaticInspector{Err: wantErr}))
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
