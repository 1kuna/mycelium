package node

import (
	"context"
	"errors"
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
	readiness   ports.EngineReadinessChecker
	listenAddr  string
	loadTimeout time.Duration
	nextID      int
	instances   map[string]domain.ModelInstance
	handles     map[string]ports.Handle
	loads       map[string]*loadOp
	inflight    map[string]*inflightState
}

type loadOp struct {
	done chan struct{}
	inst domain.ModelInstance
	err  error
}

type inflightState struct {
	count         int
	stopping      bool
	drained       chan struct{}
	drainedClosed bool
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
		inflight:    map[string]*inflightState{},
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

func WithEngineReadinessChecker(checker ports.EngineReadinessChecker) Option {
	return func(a *Agent) {
		a.readiness = checker
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
		if state := a.inflight[inst.ID]; state != nil {
			inst.InFlight = state.count
			if state.stopping {
				inst.State = domain.InstStopping
			}
		}
		instances = append(instances, inst)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	return domain.NodeSnapshot{Node: node, Instances: instances}, nil
}

func (a *Agent) Instances() []domain.ModelInstance {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.instanceListLocked()
}

func (a *Agent) ProtectInstance(instanceID, reservationID string) error {
	if instanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	if reservationID == "" {
		return fmt.Errorf("reservation id is required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	inst, ok := a.instances[instanceID]
	if !ok {
		return fmt.Errorf("unknown instance %q", instanceID)
	}
	if inst.ReservationID != "" && inst.ReservationID != reservationID {
		return fmt.Errorf("instance %q is already protected by reservation %q", instanceID, inst.ReservationID)
	}
	inst.ReservationID = reservationID
	inst.Pinned = true
	a.instances[instanceID] = inst
	return nil
}

func (a *Agent) Load(ctx context.Context, req domain.LoadRequest) (domain.ModelInstance, error) {
	if err := validateLoadRequest(req); err != nil {
		return domain.ModelInstance{}, err
	}
	if inst, ok := a.readyInstance(req.Preset.ID, req.AcceleratorSet); ok {
		return inst, nil
	}
	check, err := a.checkEngineReadiness(ctx, req.Preset)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	op, key, owner, err := a.beginLoad(req, check)
	if err != nil {
		return domain.ModelInstance{}, err
	}
	if !owner {
		return waitLoad(ctx, op)
	}

	loadingID := op.inst.ID
	inst, handle, err := a.launchAndWait(ctx, req.Preset, op.inst)
	a.finishLoad(key, loadingID, handle, inst, op, err)
	return waitLoad(ctx, op)
}

func (a *Agent) Unload(ctx context.Context, instanceID string) error {
	a.mu.Lock()
	inst, ok := a.instances[instanceID]
	handle := a.handles[instanceID]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("unknown instance %q", instanceID)
	}
	state := a.inflightStateLocked(instanceID)
	state.stopping = true
	inst.State = domain.InstStopping
	a.instances[instanceID] = inst
	a.closeDrainedIfReadyLocked(state)
	drained := state.drained
	a.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drained:
	}

	if err := a.backend.Stop(ctx, handle); err != nil {
		return err
	}

	a.mu.Lock()
	delete(a.instances, inst.ID)
	delete(a.handles, inst.ID)
	delete(a.inflight, inst.ID)
	a.mu.Unlock()
	return nil
}

func (a *Agent) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	ids := make([]string, 0, len(a.instances))
	for id := range a.instances {
		ids = append(ids, id)
	}
	a.mu.Unlock()
	sort.Strings(ids)
	var errs []error
	for _, id := range ids {
		if err := a.Unload(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *Agent) BeginRequest(ctx context.Context, instanceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	inst, ok := a.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q is not ready", instanceID)
	}
	state := a.inflight[instanceID]
	if state != nil && state.stopping {
		return fmt.Errorf("instance %q is stopping", instanceID)
	}
	if inst.State != domain.InstReady {
		return fmt.Errorf("instance %q is not ready", instanceID)
	}
	if state == nil {
		state = a.inflightStateLocked(instanceID)
	}
	state.count++
	return nil
}

func (a *Agent) EndRequest(_ context.Context, instanceID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.inflight[instanceID]
	if !ok || state.count == 0 {
		return fmt.Errorf("instance %q has no in-flight request", instanceID)
	}
	state.count--
	a.closeDrainedIfReadyLocked(state)
	return nil
}

func (a *Agent) InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	if a.inspector != nil {
		return a.inspector.InspectModel(ctx, p)
	}
	return domain.ModelMetadata{}, fmt.Errorf("model inspector is not configured for preset %q", p.ID)
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
	if metric.PresetID == "" {
		metric.PresetID = inst.PresetID
	}
	if metric.InstanceID == "" {
		metric.InstanceID = inst.ID
	}
	if metric.At.IsZero() {
		metric.At = a.clock.Now()
	}
	return a.telemetry.Record(ctx, metric)
}

func (a *Agent) readyInstance(presetID string, acc []int) (domain.ModelInstance, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, inst := range a.instances {
		if inst.PresetID == presetID && sameAcceleratorSet(inst.AcceleratorSet, acc) && inst.State == domain.InstReady {
			return inst, true
		}
	}
	return domain.ModelInstance{}, false
}

func (a *Agent) checkEngineReadiness(ctx context.Context, preset domain.Preset) (domain.EngineReadinessCheck, error) {
	if a.readiness == nil {
		return domain.EngineReadinessCheck{}, nil
	}
	return a.readiness.CheckEngineReadiness(ctx, a.node, preset)
}

func (a *Agent) beginLoad(req domain.LoadRequest, check domain.EngineReadinessCheck) (*loadOp, string, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := loadKey(req.Preset.ID, req.AcceleratorSet)
	if op, ok := a.loads[key]; ok {
		return op, key, false, nil
	}
	for _, inst := range a.instances {
		if inst.PresetID == req.Preset.ID && sameAcceleratorSet(inst.AcceleratorSet, req.AcceleratorSet) && inst.State == domain.InstReady {
			op := &loadOp{done: make(chan struct{}), inst: inst}
			close(op.done)
			return op, key, false, nil
		}
	}
	if a.allocator == nil {
		return nil, "", false, fmt.Errorf("allocator is not configured")
	}

	inst := domain.ModelInstance{
		ID:             a.nextInstanceID(),
		PresetID:       req.Preset.ID,
		NodeID:         a.node.ID,
		AcceleratorSet: append([]int(nil), req.AcceleratorSet...),
		Claim:          req.Claim,
		ReservationID:  req.ReservationID,
		Priority:       req.Priority,
		State:          domain.InstLoading,
		Loading:        true,
	}
	if check.Status != "" {
		inst.EngineProfileID = check.ProfileID
		inst.EngineReadiness = check.Status
		inst.EngineReadinessReason = check.Reason
	}
	existing := a.instanceListLocked()
	if !a.allocator.CanStackLoad(a.node, req.AcceleratorSet, existing) {
		return nil, "", false, fmt.Errorf("%w: load already in flight on node %q", domain.ErrNoFit, a.node.ID)
	}
	if !a.allocator.Fits(a.node, req.AcceleratorSet, existing, req.Claim) {
		return nil, "", false, fmt.Errorf("%w: node %q is saturated", domain.ErrNoFit, a.node.ID)
	}
	op := &loadOp{done: make(chan struct{}), inst: inst}
	a.loads[key] = op
	a.instances[inst.ID] = inst
	return op, key, true, nil
}

func (a *Agent) instanceListLocked() []domain.ModelInstance {
	instances := make([]domain.ModelInstance, 0, len(a.instances))
	for _, inst := range a.instances {
		if state := a.inflight[inst.ID]; state != nil {
			inst.InFlight = state.count
			if state.stopping {
				inst.State = domain.InstStopping
			}
		}
		instances = append(instances, inst)
	}
	return instances
}

func claimForPreset(p domain.Preset) domain.Claim {
	kv := int(math.Ceil(float64(p.ContextLength) * p.KVPerTokenMB))
	return domain.Claim{WeightsMB: p.EstWeightsMB, KVReservedMB: kv}
}

func (a *Agent) finishLoad(key, instanceID string, handle ports.Handle, inst domain.ModelInstance, op *loadOp, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		delete(a.instances, instanceID)
	} else {
		a.instances[instanceID] = inst
		a.handles[instanceID] = handle
	}
	delete(a.loads, key)
	op.inst = inst
	op.err = err
	close(op.done)
}

func validateLoadRequest(req domain.LoadRequest) error {
	if req.Preset.ID == "" {
		return fmt.Errorf("load preset id is required")
	}
	if len(req.AcceleratorSet) == 0 {
		return fmt.Errorf("load accelerator_set is required")
	}
	if req.Claim == (domain.Claim{}) {
		return fmt.Errorf("load claim is required")
	}
	return nil
}

func loadKey(presetID string, acc []int) string {
	return fmt.Sprintf("%s:%v", presetID, acc)
}

func sameAcceleratorSet(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (a *Agent) inflightStateLocked(instanceID string) *inflightState {
	state := a.inflight[instanceID]
	if state == nil {
		state = &inflightState{drained: make(chan struct{})}
		a.inflight[instanceID] = state
	}
	return state
}

func (a *Agent) closeDrainedIfReadyLocked(state *inflightState) {
	if state.stopping && state.count == 0 && !state.drainedClosed {
		close(state.drained)
		state.drainedClosed = true
	}
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
