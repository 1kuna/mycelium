package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestIdleReaperReapsIdleAutoscaleInstanceAfterTTL(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(11 * time.Second))
	inst := idleReapInstance("inst-auto", "node-a", start, start)
	store, fleet, nodes := idleReapHarness(inst)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Reaped != 1 || !nodes.unloaded("inst-auto") || store.hasInstance("inst-auto") || fleet.hasInstance("inst-auto") {
		t.Fatalf("idle instance was not fully reaped: report=%+v unloaded=%v store=%v fleet=%v", report, nodes.unloads, store.instances, fleet.snapshot.Instances)
	}
}

func TestIdleReaperKeepsRecentlyActiveInstance(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(20 * time.Second))
	inst := idleReapInstance("inst-active", "node-a", start, clk.Now().Add(-2*time.Second))
	store, fleet, nodes := idleReapHarness(inst)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Reaped != 0 || nodes.unloaded("inst-active") || !store.hasInstance("inst-active") {
		t.Fatalf("recently active instance was reaped: report=%+v unloaded=%v", report, nodes.unloads)
	}
}

func TestIdleReaperKeepsInflightInstancePastTTL(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(20 * time.Second))
	inst := idleReapInstance("inst-inflight", "node-a", start, start)
	inst.InFlight = 1
	store, fleet, nodes := idleReapHarness(inst)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Reaped != 0 || nodes.unloaded("inst-inflight") {
		t.Fatalf("in-flight instance was reaped: report=%+v unloaded=%v", report, nodes.unloads)
	}
}

func TestIdleReaperNeverReapsManualPinnedOrPoolInstances(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(20 * time.Second))
	manual := idleReapInstance("manual", "node-a", start, start)
	manual.Origin = ""
	pinned := idleReapInstance("pinned", "node-a", start, start)
	pinned.Pinned = true
	poolHeld := idleReapInstance("pool", "node-a", start, start)
	poolHeld.ReservationID = "pool-worker"
	store, fleet, nodes := idleReapHarness(manual, pinned, poolHeld)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Reaped != 0 || len(nodes.unloads) != 0 {
		t.Fatalf("protected instances were reaped: report=%+v unloaded=%v", report, nodes.unloads)
	}
}

func TestIdleReaperKeepsInstanceYoungerThanCooldown(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(3 * time.Second))
	inst := idleReapInstance("inst-young", "node-a", start, start)
	store, fleet, nodes := idleReapHarness(inst)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 1000, ScaleCooldownMS: 10000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if report.Reaped != 0 || nodes.unloaded("inst-young") {
		t.Fatalf("cooldown-young instance was reaped: report=%+v unloaded=%v", report, nodes.unloads)
	}
}

func TestIdleReaperReapThenDemandRegrowsThroughAutoScaler(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start)
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset-a"
		p.ModelRef = "model-a"
		p.ContextLength = 1024
	})
	store := newAutoScaleTestStore(demandTask("demand-a", "model-a", preset.ID, "node-a"))
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}}}
	runtime := &autoScaleTestRuntime{store: store, fleet: fleet, failNodes: map[string]error{}}
	scaler := testAutoScaler(store, runtime, fleet, clk, preset, domain.AutonomyPolicy{
		AutoScale:                  true,
		MaxReplicasPerModelPerNode: 1,
		MaxReplicasPerModel:        1,
		ScaleCooldownMS:            1000,
	})

	if report, err := scaler.Tick(ctx); err != nil || report.Scaled != 1 {
		t.Fatalf("scale tick = %+v, %v", report, err)
	}
	firstID := "inst-node-a-1"
	if !store.hasInstance(firstID) || !fleet.hasInstance(firstID) {
		t.Fatalf("autoscaler did not grow first instance: store=%v fleet=%v", store.instances, fleet.snapshot.Instances)
	}

	nodes := &idleReapTestNodes{fleet: fleet, agents: map[string]*idleReapTestAgent{"node-a": {nodeID: "node-a", fleet: fleet}}}
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})
	clk.Advance(6 * time.Second)
	if report, err := reaper.Tick(ctx); err != nil || report.Reaped != 1 {
		t.Fatalf("reap tick = %+v, %v", report, err)
	}
	if store.hasInstance(firstID) || fleet.hasInstance(firstID) {
		t.Fatalf("first instance survived reap: store=%v fleet=%v", store.instances, fleet.snapshot.Instances)
	}

	task := demandTask("demand-b", "model-a", preset.ID, "node-a")
	task.CreatedAt = clk.Now()
	task.UpdatedAt = clk.Now()
	task.LastSeen = clk.Now()
	store.tasks[task.ID] = task
	if report, err := scaler.Tick(ctx); err != nil || report.Scaled != 1 {
		t.Fatalf("regrow tick = %+v, %v", report, err)
	}
	if !store.hasInstance("inst-node-a-2") || !fleet.hasInstance("inst-node-a-2") {
		t.Fatalf("autoscaler did not regrow after reap: store=%v fleet=%v", store.instances, fleet.snapshot.Instances)
	}
}

func TestStartIdleReaperDisabledDoesNotRun(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(100, 0).UTC())
	store, fleet, nodes := idleReapHarness(idleReapInstance("inst-auto", "node-a", clk.Now(), clk.Now()))
	startIdleReaper(context.Background(), store, fleet, nodes, domain.AutonomyPolicy{IdleTTLMS: 0}, clk, time.Second)

	if clk.TimerCount() != 0 || len(nodes.unloads) != 0 {
		t.Fatalf("disabled reaper ran: timers=%d unloads=%v", clk.TimerCount(), nodes.unloads)
	}
}

func testIdleReaper(store *autoScaleTestStore, fleet *autoScaleTestFleet, nodes *idleReapTestNodes, clk *mocks.FakeClock, policy domain.AutonomyPolicy) *idleReaper {
	return &idleReaper{
		Store:  store,
		Fleet:  fleet,
		Nodes:  nodes,
		Clock:  clk,
		Policy: policy,
	}
}

func idleReapHarness(instances ...domain.ModelInstance) (*autoScaleTestStore, *autoScaleTestFleet, *idleReapTestNodes) {
	store := newAutoScaleTestStore()
	fleet := &autoScaleTestFleet{snapshot: domain.FleetSnapshot{Nodes: []domain.Node{autoscaleNode("node-a", 1000)}}}
	for _, inst := range instances {
		store.instances[inst.ID] = inst
		fleet.snapshot.Instances = append(fleet.snapshot.Instances, inst)
	}
	nodes := &idleReapTestNodes{fleet: fleet, agents: map[string]*idleReapTestAgent{"node-a": {nodeID: "node-a", fleet: fleet}}}
	return store, fleet, nodes
}

func idleReapInstance(id, nodeID string, created, activity time.Time) domain.ModelInstance {
	return fixtures.MakeInstance(fixtures.WithInstanceID(id), fixtures.OnNode(nodeID), func(i *domain.ModelInstance) {
		i.Origin = domain.InstanceOriginAutoscale
		i.CreatedAt = created
		i.LastActivityAt = activity
		i.State = domain.InstReady
	})
}

func (s *autoScaleTestStore) hasInstance(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.instances[id]
	return ok
}

func (f *autoScaleTestFleet) hasInstance(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, inst := range f.snapshot.Instances {
		if inst.ID == id {
			return true
		}
	}
	return false
}

type idleReapTestNodes struct {
	mu      sync.Mutex
	fleet   *autoScaleTestFleet
	agents  map[string]*idleReapTestAgent
	unloads []string
}

func (n *idleReapTestNodes) unloadCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.unloads)
}

func (n *idleReapTestNodes) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent := n.agents[nodeID]
	if agent == nil {
		return nil, fmt.Errorf("missing node %q", nodeID)
	}
	agent.nodes = n
	return agent, nil
}

func (n *idleReapTestNodes) unloaded(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, unload := range n.unloads {
		if unload == id {
			return true
		}
	}
	return false
}

type idleReapTestAgent struct {
	nodeID string
	fleet  *autoScaleTestFleet
	nodes  *idleReapTestNodes
}

func (a *idleReapTestAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	a.fleet.mu.Lock()
	defer a.fleet.mu.Unlock()
	var instances []domain.ModelInstance
	for _, inst := range a.fleet.snapshot.Instances {
		if inst.NodeID == a.nodeID {
			instances = append(instances, inst)
		}
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	return domain.NodeSnapshot{Node: autoscaleNode(a.nodeID, 1000), Instances: instances}, nil
}

func (a *idleReapTestAgent) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
	return domain.ModelInstance{}, fmt.Errorf("unexpected load")
}

func (a *idleReapTestAgent) Unload(_ context.Context, instanceID string) error {
	a.nodes.mu.Lock()
	a.nodes.unloads = append(a.nodes.unloads, instanceID)
	a.nodes.mu.Unlock()
	a.fleet.removeInstance(instanceID)
	return nil
}

func (a *idleReapTestAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, fmt.Errorf("unexpected inspect")
}

func (a *idleReapTestAgent) BeginRequest(context.Context, string) error {
	return fmt.Errorf("unexpected begin")
}

func (a *idleReapTestAgent) EndRequest(context.Context, string) error {
	return fmt.Errorf("unexpected end")
}

var _ scheduler.NodeResolver = (*idleReapTestNodes)(nil)
var _ ports.NodeAgent = (*idleReapTestAgent)(nil)

func TestIdleReaperReportShape(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	clk := mocks.NewFakeClock(start.Add(20 * time.Second))
	left := idleReapInstance("a", "node-a", start, start)
	right := idleReapInstance("b", "node-a", start, start)
	store, fleet, nodes := idleReapHarness(left, right)
	reaper := testIdleReaper(store, fleet, nodes, clk, domain.AutonomyPolicy{IdleTTLMS: 5000, ScaleCooldownMS: 1000})

	report, err := reaper.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if !reflect.DeepEqual(report, idleReapReport{Scanned: 2, Reaped: 2}) {
		t.Fatalf("report = %+v", report)
	}
}

func TestStartIdleReaperGuardsAndTicks(t *testing.T) {
	clk := mocks.NewFakeClock(time.Unix(300, 0).UTC())
	// Nil-dependency guards return without starting a loop.
	startIdleReaper(context.Background(), nil, nil, nil, domain.AutonomyPolicy{IdleTTLMS: 1000}, clk, 0)
	if clk.TimerCount() != 0 {
		t.Fatalf("guarded reaper armed a timer")
	}

	// Enabled: interval <= 0 falls back to the default and the loop ticks,
	// reaping an idle autoscale instance, then stops with the context.
	created := clk.Now().Add(-time.Hour)
	store, fleet, nodes := idleReapHarness(idleReapInstance("inst-auto", "node-a", created, created))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startIdleReaper(ctx, store, fleet, nodes, domain.AutonomyPolicy{IdleTTLMS: 1000, ScaleCooldownMS: 500}, clk, 0)
	deadline := time.After(5 * time.Second)
	for {
		clk.Advance(defaultIdleReaperInterval)
		if nodes.unloadCount() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("idle reaper loop never reaped")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
}

func TestPeerCatalogStageClientStageModel(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-stage"))

	// No reachable address fails loudly.
	if _, err := (peerCatalogStageClient{}).StageModel(context.Background(), domain.Peer{ID: "p"}, preset); err == nil {
		t.Fatalf("expected no-address error")
	}

	// Success path returns the staged locality and sends auth.
	var gotAuth string
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/catalog/stage" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"locality": domain.ModelLocality{ID: "loc-1", PresetID: preset.ID, NodeID: "node-a"}})
	}))
	defer ok.Close()
	client := peerCatalogStageClient{Client: ok.Client(), AuthToken: "rpc-secret"}
	locality, err := client.StageModel(context.Background(), domain.Peer{ID: "p", Addresses: []string{ok.URL}}, preset)
	if err != nil || locality.ID != "loc-1" {
		t.Fatalf("stage = %+v %v", locality, err)
	}
	if gotAuth != "Bearer rpc-secret" {
		t.Fatalf("auth = %q", gotAuth)
	}

	// Non-2xx, undecodable, and empty-locality responses fail loudly.
	for name, handler := range map[string]http.HandlerFunc{
		"refused": func(w http.ResponseWriter, r *http.Request) { http.Error(w, "disk floor", http.StatusConflict) },
		"garbage": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("{")) },
		"empty":   func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"locality":{}}`)) },
	} {
		server := httptest.NewServer(handler)
		if _, err := (peerCatalogStageClient{Client: server.Client()}).StageModel(context.Background(), domain.Peer{ID: name, Addresses: []string{server.URL}}, preset); err == nil {
			t.Fatalf("%s response should error", name)
		}
		server.Close()
	}
}
