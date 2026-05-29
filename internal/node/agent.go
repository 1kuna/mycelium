package node

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Agent struct {
	mu          sync.Mutex
	node        domain.Node
	backend     ports.BackendAdapter
	clock       ports.Clock
	telemetry   ports.TelemetrySink
	inspector   ModelInspector
	allocator   ports.Allocator
	listenAddr  string
	loadTimeout time.Duration
	nextID      int
	instances   map[string]domain.ModelInstance
	handles     map[string]ports.Handle
	loads       map[string]*loadOp
}

type loadOp struct {
	done chan struct{}
	inst domain.ModelInstance
	err  error
}

type Option func(*Agent)

func NewAgent(node domain.Node, backend ports.BackendAdapter, clock ports.Clock, opts ...Option) *Agent {
	agent := &Agent{
		node:        node,
		backend:     backend,
		clock:       clock,
		listenAddr:  "127.0.0.1:0",
		loadTimeout: 5 * time.Minute,
		instances:   map[string]domain.ModelInstance{},
		handles:     map[string]ports.Handle{},
		loads:       map[string]*loadOp{},
	}
	for _, opt := range opts {
		opt(agent)
	}
	return agent
}

func WithTelemetrySink(sink ports.TelemetrySink) Option {
	return func(a *Agent) {
		a.telemetry = sink
	}
}

func WithModelInspector(inspector ModelInspector) Option {
	return func(a *Agent) {
		a.inspector = inspector
	}
}

func WithListenAddr(addr string) Option {
	return func(a *Agent) {
		a.listenAddr = addr
	}
}

func WithAllocator(allocator ports.Allocator) Option {
	return func(a *Agent) {
		a.allocator = allocator
	}
}

func WithLoadTimeout(timeout time.Duration) Option {
	return func(a *Agent) {
		a.loadTimeout = timeout
	}
}

func (a *Agent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	node := a.node
	node.HeartbeatAt = a.clock.Now()
	instances := make([]domain.ModelInstance, 0, len(a.instances))
	for _, inst := range a.instances {
		instances = append(instances, inst)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	return domain.NodeSnapshot{Node: node, Instances: instances}, nil
}

func (a *Agent) Load(ctx context.Context, p domain.Preset) (domain.ModelInstance, error) {
	if inst, ok := a.readyInstance(p.ID); ok {
		return inst, nil
	}
	op, owner := a.beginLoad(p)
	if !owner {
		return waitLoad(ctx, op)
	}
	if err := a.admitLoad(p); err != nil {
		a.finishLoad(p.ID, op.inst.ID, ports.Handle{}, domain.ModelInstance{}, op, err)
		return waitLoad(ctx, op)
	}
	a.markLoading(op.inst)

	loadingID := op.inst.ID
	inst, handle, err := a.launchAndWait(ctx, p, op.inst)
	a.finishLoad(p.ID, loadingID, handle, inst, op, err)
	return waitLoad(ctx, op)
}

func (a *Agent) Unload(ctx context.Context, instanceID string) error {
	a.mu.Lock()
	inst, ok := a.instances[instanceID]
	handle := a.handles[instanceID]
	a.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown instance %q", instanceID)
	}

	if err := a.backend.Stop(ctx, handle); err != nil {
		return err
	}

	a.mu.Lock()
	delete(a.instances, inst.ID)
	delete(a.handles, inst.ID)
	a.mu.Unlock()
	return nil
}

func (a *Agent) InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	if a.inspector != nil {
		return a.inspector.InspectModel(ctx, p)
	}
	if p.EstWeightsMB <= 0 || p.KVPerTokenMB <= 0 || p.ContextLength <= 0 {
		return domain.ModelMetadata{}, fmt.Errorf("model inspector is not configured for preset %q", p.ID)
	}
	return domain.ModelMetadata{
		ModelRef:      p.ModelRef,
		Format:        "preset",
		WeightsMB:     p.EstWeightsMB,
		KVPerTokenMB:  p.KVPerTokenMB,
		ContextLength: p.ContextLength,
	}, nil
}

func (a *Agent) RecordRun(ctx context.Context, metric domain.RunMetric) error {
	if a.telemetry == nil {
		return fmt.Errorf("telemetry sink is not configured")
	}
	a.mu.Lock()
	inst, ok := a.instances[metric.InstanceID]
	a.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown instance %q", metric.InstanceID)
	}
	metric.NodeID = a.node.ID
	if metric.InstanceID == "" {
		metric.InstanceID = inst.ID
	}
	if metric.At.IsZero() {
		metric.At = a.clock.Now()
	}
	return a.telemetry.Record(ctx, metric)
}

func (a *Agent) readyInstance(presetID string) (domain.ModelInstance, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, inst := range a.instances {
		if inst.PresetID == presetID && inst.State == domain.InstReady {
			return inst, true
		}
	}
	return domain.ModelInstance{}, false
}

func (a *Agent) beginLoad(p domain.Preset) (*loadOp, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if op, ok := a.loads[p.ID]; ok {
		return op, false
	}

	inst := domain.ModelInstance{
		ID:             a.nextInstanceID(),
		PresetID:       p.ID,
		NodeID:         a.node.ID,
		AcceleratorSet: []int{0},
		Claim:          claimForPreset(p),
		State:          domain.InstLoading,
		Loading:        true,
	}
	op := &loadOp{done: make(chan struct{}), inst: inst}
	a.loads[p.ID] = op
	return op, true
}

func (a *Agent) markLoading(inst domain.ModelInstance) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.instances[inst.ID] = inst
}

func (a *Agent) admitLoad(p domain.Preset) error {
	if a.allocator == nil {
		return fmt.Errorf("allocator is not configured")
	}
	a.mu.Lock()
	node := a.node
	existing := a.instanceListLocked()
	a.mu.Unlock()
	acc := []int{0}
	if !a.allocator.CanStackLoad(node, acc, existing) {
		return fmt.Errorf("%w: load already in flight on node %q", domain.ErrNoFit, node.ID)
	}
	if !a.allocator.Fits(node, acc, existing, claimForPreset(p)) {
		return fmt.Errorf("%w: node %q is saturated", domain.ErrNoFit, node.ID)
	}
	return nil
}

func (a *Agent) instanceListLocked() []domain.ModelInstance {
	instances := make([]domain.ModelInstance, 0, len(a.instances))
	for _, inst := range a.instances {
		instances = append(instances, inst)
	}
	return instances
}

func claimForPreset(p domain.Preset) domain.Claim {
	kv := int(math.Ceil(float64(p.ContextLength) * p.KVPerTokenMB))
	return domain.Claim{WeightsMB: p.EstWeightsMB, KVReservedMB: kv}
}

func (a *Agent) finishLoad(presetID, instanceID string, handle ports.Handle, inst domain.ModelInstance, op *loadOp, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		delete(a.instances, instanceID)
	} else {
		a.instances[instanceID] = inst
		a.handles[instanceID] = handle
	}
	delete(a.loads, presetID)
	op.inst = inst
	op.err = err
	close(op.done)
}

func waitLoad(ctx context.Context, op *loadOp) (domain.ModelInstance, error) {
	select {
	case <-ctx.Done():
		return domain.ModelInstance{}, ctx.Err()
	case <-op.done:
		return op.inst, op.err
	}
}

var _ ports.NodeAgent = (*Agent)(nil)
