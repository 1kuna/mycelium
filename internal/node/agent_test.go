package node

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAgentConformance(t *testing.T) {
	contract.RunNodeAgentConformance(t, "node-agent",
		func() ports.NodeAgent {
			return NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
		},
		fixtures.MakePreset())
}

func TestLoadReadinessGatesInstanceAndUnloadStopsBackend(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))

	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if inst.State != domain.InstReady || inst.Loading || inst.Addr == "" {
		t.Fatalf("instance = %+v", inst)
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()))
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()))
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
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()))

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

func TestUnloadUnknownAndStopFailure(t *testing.T) {
	backend := mocks.NewBackendAdapter()
	agent := NewAgent(fixtures.MakeNode(), backend, mocks.NewFakeClock(time.Now()))
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
