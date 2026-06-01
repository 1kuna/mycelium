# Mycelium — Testing & Modularity Architecture (Document 2 of 3)

**Audience: the coding agent.** This document is the test and modularity contract. Build the test infrastructure in this document *before* writing application code — Phase 0 in Document 3 stands up contracts, mocks, fixtures, and the fake clock first, then the scheduler is written against them.

It cross-references Document 1 (the "what"): module boundaries are §5 of that doc; the domain data shapes are §3.8; the resource/lease/preemption model the scheduler must implement is §3.

**The load-bearing idea of this whole document:** every node, every backend engine, every GPU, and the system clock sit behind a Go interface with a hand-written mock. The scheduler/lease/safety core (Phase 0) is *pure logic over those interfaces*, so it builds and tests green on the local dev Mac against a **mock fleet + a fake clock**, with no Spark, no B70, no GPU powered on. Real engines and real machines appear only in the `smoke/` tier, run manually or on CI main.

---

## 1. Design principles

1. **Hard module boundaries.** Every seam in Document 1 §5 is a Go interface in `internal/ports`. Modules depend on interfaces from `internal/ports` and types from `internal/domain` — never on each other's concrete packages. `internal/domain` has no logic and no imports outside the standard library.
2. **Test-first, infra-first.** Contracts, mocks, fixtures, and the fake clock land before any implementation. The scheduler is written to satisfy a conformance suite that already exists.
3. **Mocks are first-class, hand-written, and recording.** Every external dependency (node agent, backend engine, resource estimator, clock, telemetry sink, discovery, tunnel, store) has a mock that satisfies the same interface as the real thing, records every call, and can inject failures. No code-generation for the behavioral mocks — hand-written gives the call recording and failure injection the tests need.
4. **Determinism.** No real time, no real network, no real hardware in `unit/`, `contract/`, `integration/`, or `e2e/`. Time comes from an injected `Clock`; tests drive aging, timeouts, TTLs, and backoff with a `FakeClock` they advance manually. A test that calls `time.Sleep` or `time.Now()` directly is a bug.
5. **Traceable.** Every scheduling decision emits a structured `PlacementDecision.Trace` (Document 1 §3.8). Longer multi-step operations emit a `Trace`. Agents debug by loading a trace and walking steps, not by reading printf output.
6. **Constructor injection only.** Dependencies enter through constructors. No package-level singletons, no `init()` wiring, no monkey-patching. If injection feels heavy, the module is too tightly coupled — split it.

### Go translation of the usual testing vocabulary

| Concept | Python idiom | Go idiom used here |
|---|---|---|
| Interface | `Protocol`, `@runtime_checkable` | `interface`; satisfaction is checked at **compile time** (stronger) |
| Mock conforms to real interface | runtime `isinstance` check | compile-time `var _ Iface = (*Mock)(nil)` **+** a shared behavioral **conformance suite** run against both impls |
| Test runner | pytest | `go test`, table-driven subtests |
| Avoid patching | "never `@patch`" | "never monkey-patch; inject via constructor" |
| Fake time | `freezegun` | injected `Clock` + `FakeClock` |

---

## 2. Module contracts

Domain types live in `internal/domain` (mirror of Document 1 §3.8). Interfaces live in `internal/ports`. Below are the load-bearing contracts; the same pattern extends to the rest of §5.

```go
// internal/domain/enums.go
package domain

type Priority string
const (
	PriorityInteractive Priority = "interactive"
	PriorityNormal      Priority = "normal"
	PriorityBackground  Priority = "background"
)

type SpeedPref string
const (
	SpeedThroughput SpeedPref = "throughput"
	SpeedLatency    SpeedPref = "latency"
	SpeedAuto       SpeedPref = "auto"
)

type Preemption string
const (
	PreemptInherit            Preemption = "inherit"
	PreemptSoft               Preemption = "soft"
	PreemptHardForInteractive Preemption = "hard_for_interactive"
	PreemptHard               Preemption = "hard"
)

type OOMSeverity string
const (
	OOMSoft         OOMSeverity = "soft"          // 4090: OOM crashes the program only
	OOMCatastrophic OOMSeverity = "catastrophic"  // DGX Spark: OOM forces a power cycle
)

type NodeStatus string
const (
	NodeReady       NodeStatus = "ready"
	NodeMaintenance NodeStatus = "maintenance"
	NodeDraining    NodeStatus = "draining"
	NodeUnreachable NodeStatus = "unreachable"
)
```

```go
// internal/domain/types.go
package domain

import "time"

type Accelerator struct {
	Index             int
	Vendor            string // nvidia | intel | apple | amd
	Kind              string // gb10 | rtx4090 | arc-pro-b70 | ...
	VRAMTotalMB       int
	VRAMUsedMB        int
	UnifiedMemory     bool   // Apple: one pressure domain with host RAM
	ComputeCapability string
	ArchFamily        string
}

type Node struct {
	ID            string
	Name          string
	Address       string // the peer's reachable LAN address (host:port)
	OS            string
	Labels        map[string]string
	MaxUtil       float64     // hard ceiling, e.g. 0.90 — never exceeded
	OOMSeverity   OOMSeverity
	Accelerators  []Accelerator
	UnifiedMemory bool
	SpeedClass    SpeedClass
	Status        NodeStatus
	HeartbeatAt   time.Time
}

type SpeedClass struct {
	TokensPerSecRef float64
	Source          string // "probe" | "class-default"
	ProbedAt        time.Time
}

type Preset struct {
	ID            string
	ModelRef      string
	Backend       string // llamacpp | vllm | mlx | custom
	ContextLength int
	Quant         string
	Capabilities  []string
	LaunchProfile string
	EstWeightsMB  int
	KVPerTokenMB  float64
}

type Claim struct {
	WeightsMB   int
	KVReservedMB int
}

type InstanceState string
const (
	InstLoading InstanceState = "loading"
	InstReady   InstanceState = "ready"
	InstStopping InstanceState = "stopping"
	InstError   InstanceState = "error"
)

type ModelInstance struct {
	ID             string
	PresetID       string
	NodeID         string
	AcceleratorSet []int
	Claim          Claim
	State          InstanceState
	Addr           string
	InFlight       int
}

type Job struct {
	ID             string
	TaskType       string // chat | embedding | vision | asr | ...
	Model          string // logical alias OR explicit preset id
	PresetID       string // optional pin
	Project        string
	Priority       Priority
	SpeedPref      SpeedPref
	ContextRequest int    // optional override of project/preset cap (0 = use default)
	Preemption     Preemption
	Streaming      bool
	DeadlineMS     int    // 0 = none
	ParentID       string // set on reverse-benchmark child jobs (fan-out)
	Status         string
}

type PlacementAction string
const (
	ActionWarmInstance   PlacementAction = "placed_on_warm_instance"
	ActionLoadedNew      PlacementAction = "loaded_new"
	ActionQueued         PlacementAction = "queued"
	ActionHardPreempted  PlacementAction = "hard_preempted_then_loaded"
	ActionDedicatedUnit  PlacementAction = "dedicated_unit"
)

type PlacementDecision struct {
	JobID            string
	InstanceID       string
	NodeID           string
	AcceleratorSet   []int
	Claim            Claim
	Action           PlacementAction
	SpeedPrefApplied SpeedPref
	Trace            []TraceStep
}

// Read-only view the Placer reasons over. The scheduler never mutates it.
type FleetSnapshot struct {
	Nodes     []Node
	Instances []ModelInstance
}

type NodeSnapshot struct {
	Node      Node
	Instances []ModelInstance
}

type RunMetric struct {
	JobID           string
	InstanceID      string
	NodeID          string
	Project         string
	TokensPerSec    float64
	TTFTms          int
	LoadWallClockMS int
	PeakVRAMMB      int
	ContextUsed     int
	At              time.Time
}

// --- Federation types (§3.12) ---

type Peer struct {
	ID         string
	Addresses  []string
	Compute    bool      // compute-on => owns accelerators, runs backends, joins group telemetry analysis
	LastSeen   time.Time // heartbeat
	Version    string
}

// LeaseOffer is an owner's conditional "I can run this" during planning.
type LeaseOffer struct {
	OfferID    string
	JobID      string
	NodeID     string
	Claim      Claim
	Fence      uint64 // the node's resource version this offer was made against
	ExpiresAt  time.Time
	Conditions string // e.g. "after evicting inst_x" | "reduced concurrency"
}

// JobRecord is one row of the replicated job registry (D16). Holds the request
// payload so a dead peer's job can be re-run (rescued), not just flagged lost.
type JobRecord struct {
	JobID       string
	Coordinator string // peer id currently responsible
	AssignedNode string
	Status      string // queued | placing | running | done | failed | orphaned
	Request     []byte // the submitted request, for rescue
	Fence       uint64
	UpdatedAt   time.Time
}
```

```go
// internal/domain/errors.go
package domain

import "errors"

var (
	ErrNoFit           = errors.New("no unit can fit this preset under its constraints")
	ErrContextOverflow = errors.New("request exceeded the preset's context cap")
	ErrPreempted       = errors.New("instance was preempted")
	ErrNotReady        = errors.New("backend did not become ready before timeout")
	ErrUnreachable     = errors.New("node is unreachable")
	ErrStaleFence      = errors.New("plan was built on a stale resource version; re-decide")
)
```

```go
// internal/ports/ports.go
package ports

import (
	"context"
	"time"

	"mycelium/internal/domain"
)

// Clock is injected everywhere time matters. Tests use FakeClock.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// ResourceEstimator computes a KV-aware, backend-aware claim for a preset.
type ResourceEstimator interface {
	Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error)
}

// Allocator owns the fit math and lease bookkeeping for a single unit set.
// usable_vram = vram_total*max_util - reserved_headroom; oom_severity tunes margin.
type Allocator interface {
	Fits(node domain.Node, acc []int, existing []domain.ModelInstance, want domain.Claim) bool
	// CanStackLoad reports whether a *new load* may begin on this unit now.
	// On catastrophic units it returns false while another load is in flight.
	CanStackLoad(node domain.Node, acc []int, existing []domain.ModelInstance) bool
}

// BackendAdapter is the node agent's view of an inference engine. This is the
// ONLY thing that touches a real engine; it is mocked everywhere except smoke/.
type BackendAdapter interface {
	Name() string
	Launch(ctx context.Context, p domain.Preset, addr string) (Handle, error)
	WaitReady(ctx context.Context, addr string) error
	Stop(ctx context.Context, h Handle) error
}

type Handle struct {
	PID  int
	Addr string
	Kind string // "process" | "container"
	Ref  string // container name or pid string used for Stop
}

// NodeAgent is a coordinator's view of one machine. Any peer coordinating a job
// talks to candidate nodes through this interface; in tests it is a MockNodeAgent.
type NodeAgent interface {
	Snapshot(ctx context.Context) (domain.NodeSnapshot, error)
	Load(ctx context.Context, p domain.Preset) (domain.ModelInstance, error)
	Unload(ctx context.Context, instanceID string) error
}

// Placer is the pure scheduling decision: given a job and a read-only fleet
// snapshot, decide where it goes (or that it queues / preempts). No I/O. It is
// peer-agnostic — identical code every coordinator runs over a snapshot.
type Placer interface {
	Place(ctx context.Context, job domain.Job, fleet domain.FleetSnapshot) (domain.PlacementDecision, error)
}

// AdmissionController is the OWNER side of authority (§3.12): the resource-owning
// peer is the single atomic authority for its own hardware. A coordinator proposes;
// the owner commits in a local transaction, enforcing max_util/oom_severity, and
// REJECTS a stale plan via a per-resource fence/version (optimistic concurrency).
type AdmissionController interface {
	Offer(ctx context.Context, req domain.Job, claim domain.Claim) (domain.LeaseOffer, error)
	Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error) // ErrStaleFence if version moved
	Release(ctx context.Context, leaseID string) error
	Preempt(ctx context.Context, leaseID, reason string) error
}

// Coordinator is the per-job role (whoever received the job). It is pure
// orchestration over Placer + NodeAgent + AdmissionController + JobRegistry; it owns
// no resources. Role lasts one job. No leader.
type Coordinator interface {
	ClaimJob(ctx context.Context, jobID string) error
	Plan(ctx context.Context, jobID string) (domain.PlacementDecision, error) // parallel snapshot → Placer
	Commit(ctx context.Context, plan domain.PlacementDecision) (domain.Lease, error)
	Release(ctx context.Context, jobID string) error
}

// JobRegistry is the small replicated, eventually-consistent record of which jobs
// exist and who owns them (§3.12, D16) — NOT a consensus event-log. Backs failure
// recovery: a dead peer's unfinished jobs are re-coordinated from registry state.
type JobRegistry interface {
	Put(ctx context.Context, rec domain.JobRecord) error
	Watch(ctx context.Context, fromCursor string) (<-chan domain.JobRecord, error)
	Snapshot(ctx context.Context) ([]domain.JobRecord, error)
}

// PeerDiscovery is LAN auto-discovery + peer advertisement (incl. the compute flag).
// Cross-NAT overlay slots in behind this same interface later (D12).
type PeerDiscovery interface {
	Advertise(ctx context.Context, self domain.Peer) error
	Peers(ctx context.Context) ([]domain.Peer, error)
	WatchPeers(ctx context.Context) (<-chan domain.Peer, error)
}

// TelemetrySink ingests per-run metrics. Wired from Phase 1, before the optimizer.
type TelemetrySink interface {
	Record(ctx context.Context, m domain.RunMetric) error
}
```

The same pattern covers the remaining ports (`ModelRegistry`, `Catalog`, `TelemetryStore`, `Optimizer`, `Tunnel`, `Store`, `Clock`). Define them in `internal/ports` before their implementations exist. The federation ports above (`AdmissionController`, `Coordinator`, `JobRegistry`, `PeerDiscovery`) each get a hand-written recording mock (`MockAdmissionController`, `MockJobRegistry`, `MockPeerDiscovery`, and a coordinator is tested over mocks of the others) and, where there is a real+mock pair, a conformance suite (§3) — so the whole federation layer is built and tested on the local dev Mac with a mock fleet + `FakeClock`, real peers only in `smoke/`.

### Compile-time satisfaction (do this for every interface/impl pair)

Go refuses to compile if an implementation drifts from its interface, which is stronger than a runtime check. Put one assertion next to each implementation **and** each mock:

```go
// in internal/backends/llamacpp/adapter.go
var _ ports.BackendAdapter = (*Adapter)(nil)

// in test/mocks/backend.go
var _ ports.BackendAdapter = (*BackendAdapter)(nil)
```

Compile-time satisfaction catches *shape* drift. **Behavioral** drift (the mock acts differently from the real thing) is caught by conformance suites — §3.

---

## 3. Conformance tests — the most important category

For each interface with both a real and a mock implementation, write **one behavioral suite** and run it against both. The mock runs it in `contract/` (fast, every change); the real implementation runs the *same* suite in `smoke/` against a real engine. If they ever diverge, a suite fails.

```go
// test/contract/backendadapter_conformance.go
package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

// RunBackendAdapterConformance asserts the behavioral contract every
// BackendAdapter must honor. newAdapter must return a fresh adapter whose
// next Launch+WaitReady will succeed for the given preset.
func RunBackendAdapterConformance(t *testing.T, name string, newAdapter func() ports.BackendAdapter, p domain.Preset) {
	t.Run(name+"/launch_wait_stop_happy_path", func(t *testing.T) {
		a := newAdapter()
		h, err := a.Launch(context.Background(), p, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Launch: %v", err)
		}
		if err := a.WaitReady(context.Background(), h.Addr); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
		if err := a.Stop(context.Background(), h); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run(name+"/stop_is_idempotent", func(t *testing.T) {
		a := newAdapter()
		h, _ := a.Launch(context.Background(), p, "127.0.0.1:0")
		_ = a.WaitReady(context.Background(), h.Addr)
		_ = a.Stop(context.Background(), h)
		if err := a.Stop(context.Background(), h); err != nil {
			t.Fatalf("second Stop should be a no-op, got %v", err)
		}
	})

	t.Run(name+"/waitready_respects_context_cancel", func(t *testing.T) {
		a := newAdapter()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := a.WaitReady(ctx, "127.0.0.1:1"); err == nil {
			t.Fatalf("WaitReady should error on cancelled context")
		}
	})
}
```

```go
// test/contract/backend_mock_conformance_test.go   (fast tier, runs always)
package contract

import (
	"testing"

	"mycelium/test/fixtures"
	"mycelium/test/mocks"
	"mycelium/internal/ports"
)

func TestMockBackendAdapter_Conformance(t *testing.T) {
	RunBackendAdapterConformance(t, "mock",
		func() ports.BackendAdapter { return mocks.NewBackendAdapter() },
		fixtures.MakePreset())
}
```

```go
// test/smoke/backend_llamacpp_conformance_test.go   (smoke tier, real engine)
//go:build smoke

package smoke

import (
	"testing"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
)

func TestLlamaCpp_Conformance(t *testing.T) {
	contract.RunBackendAdapterConformance(t, "llamacpp",
		func() ports.BackendAdapter { return llamacpp.NewAdapter(llamacpp.DefaultConfig()) },
		fixtures.MakePreset(fixtures.WithRealGGUF(t))) // needs a real model file
}
```

Write a conformance suite for every dual-implemented interface (`BackendAdapter`, `NodeAgent`, `ResourceEstimator`, `Allocator`). Contract tests are the category that prevents the "mock proved nothing because it lied" failure.

---

## 4. Test hierarchy

```
test/
├── unit/            # one module, all deps mocked, fake clock        <5s   every change
├── contract/        # conformance suites vs mocks                    <2s   any iface/mock change
├── integration/     # 2+ real modules wired, external deps mocked    <10s  any module change
├── e2e/             # full peer (gateway+coordinator+node), mock fleet + fake clock     <30s  before commit
├── smoke/           # real engines / real machines (build tag smoke) 1-5m  manual / CI main
├── fixtures/        # factories + golden config/data
└── mocks/           # hand-written mocks for every port
```

| Category | Deps | Real HW? | Speed | When |
|---|---|---|---|---|
| `unit/` | none (mocked) | no | <5s | every file change |
| `contract/` | none | no | <2s | any Protocol/mock change |
| `integration/` | real modules, mocked externals | no | <10s | any module change |
| `e2e/` | whole peer, mock node agents + fake clock | no | <30s | before commit |
| `smoke/` | real engines & machines (`//go:build smoke`) | **yes** | 1-5m | manual / CI main only |

Everything except `smoke/` runs on the local dev Mac with nothing powered on. `smoke/` is the only place the Spark/B70/4090/Mac engines appear, and it is gated behind the `smoke` build tag so normal `go test ./...` never touches hardware.

Coverage targets: 85%+ lines per module; **100% on `internal/scheduler`, `internal/lease`, and all conformance suites and fixture factories**; every error path tested; every public method tested.

---

## 5. Fixture factories

Go has no kwargs, so factories take functional options, return valid instances with sensible defaults, and ship convenience variants. Test the factories themselves.

```go
// test/fixtures/nodes.go
package fixtures

import "mycelium/internal/domain"

func MakeNode(opts ...func(*domain.Node)) domain.Node {
	n := domain.Node{
		ID:          "node_test",
		Name:        "test-node",
		Address:     "127.0.0.1:50000",
		OS:          "linux",
		Labels:      map[string]string{"gpu.vendor": "nvidia"},
		MaxUtil:     0.90,
		OOMSeverity: domain.OOMSoft,
		Status:      domain.NodeReady,
		Accelerators: []domain.Accelerator{{
			Index: 0, Vendor: "nvidia", Kind: "rtx4090", VRAMTotalMB: 24576,
		}},
		SpeedClass: domain.SpeedClass{TokensPerSecRef: 90, Source: "class-default"},
	}
	for _, o := range opts {
		o(&n)
	}
	return n
}

func WithVRAM(mb int) func(*domain.Node) {
	return func(n *domain.Node) { n.Accelerators[0].VRAMTotalMB = mb }
}
func WithUsedVRAM(mb int) func(*domain.Node) {
	return func(n *domain.Node) { n.Accelerators[0].VRAMUsedMB = mb }
}
func WithMaxUtil(u float64) func(*domain.Node) { return func(n *domain.Node) { n.MaxUtil = u } }
func Catastrophic(n *domain.Node)              { n.OOMSeverity = domain.OOMCatastrophic }
func Maintenance(n *domain.Node)               { n.Status = domain.NodeMaintenance }

// Convenience variants for the real fleet shapes.
func MakeSparkNode(opts ...func(*domain.Node)) domain.Node {
	base := []func(*domain.Node){
		func(n *domain.Node) {
			n.ID, n.Name = "node_spark", "dgx-spark"
			n.OOMSeverity = domain.OOMCatastrophic
			n.UnifiedMemory = true
			n.Labels = map[string]string{"gpu.vendor": "nvidia", "gpu.kind": "gb10", "memory.class": "huge"}
			n.Accelerators = []domain.Accelerator{{Index: 0, Vendor: "nvidia", Kind: "gb10", VRAMTotalMB: 131072, UnifiedMemory: true}}
		},
	}
	return MakeNode(append(base, opts...)...)
}

func Make4090Node(opts ...func(*domain.Node)) domain.Node {
	return MakeNode(append([]func(*domain.Node){
		func(n *domain.Node) { n.ID, n.Name = "node_4090a", "rtx4090-box" },
	}, opts...)...)
}
```

```go
// test/fixtures/jobs.go
package fixtures

import "mycelium/internal/domain"

func MakeJob(opts ...func(*domain.Job)) domain.Job {
	j := domain.Job{
		ID: "job_test", TaskType: "chat", Model: "qwen2.5-9b-instruct",
		Project: "project-test", Priority: domain.PriorityNormal,
		SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptInherit,
		Streaming: true, Status: "queued",
	}
	for _, o := range opts {
		o(&j)
	}
	return j
}

func Interactive(j *domain.Job)            { j.Priority = domain.PriorityInteractive }
func Background(j *domain.Job)             { j.Priority = domain.PriorityBackground }
func Latency(j *domain.Job)               { j.SpeedPref = domain.SpeedLatency }
func HardForInteractive(j *domain.Job)    { j.Preemption = domain.PreemptHardForInteractive }
func WithContext(n int) func(*domain.Job) { return func(j *domain.Job) { j.ContextRequest = n } }

func MakePreset(opts ...func(*domain.Preset)) domain.Preset {
	p := domain.Preset{
		ID: "preset_test", ModelRef: "qwen2.5-9b-instruct", Backend: "llamacpp",
		ContextLength: 8000, Quant: "Q4_K_M", Capabilities: []string{"chat"},
		LaunchProfile: "llamacpp-cuda", EstWeightsMB: 5600, KVPerTokenMB: 0.18,
	}
	for _, o := range opts {
		o(&p)
	}
	return p
}
```

```go
// test/fixtures/fixtures_test.go  — factories are tested too
package fixtures

import "testing"

func TestMakeNodeDefaultsValid(t *testing.T) {
	n := MakeNode()
	if n.MaxUtil <= 0 || n.MaxUtil > 1 {
		t.Fatalf("MaxUtil out of range: %v", n.MaxUtil)
	}
	if len(n.Accelerators) == 0 {
		t.Fatal("default node must have an accelerator")
	}
}

func TestSparkIsCatastrophicAndUnified(t *testing.T) {
	n := MakeSparkNode()
	if n.OOMSeverity != "catastrophic" || !n.UnifiedMemory {
		t.Fatalf("spark fixture wrong: %+v", n)
	}
}
```

---

## 6. Mock architecture

Rules: a mock implements the same interface as the real thing, records every call in a `Calls` slice, exposes failure-injection fields, returns deterministic data, and never touches filesystem/network/hardware/real time.

```go
// test/mocks/backend.go
package mocks

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type BackendCall struct {
	Op    string // "launch" | "waitready" | "stop"
	Addr  string
	Preset domain.Preset
}

type BackendAdapter struct {
	NameVal    string
	LaunchErr  error
	StopErr    error
	// ReadyAfter: number of WaitReady calls that fail before one succeeds.
	// 0 => ready immediately. Models slow cold starts deterministically.
	ReadyAfter int
	readyCalls int
	Calls      []BackendCall
}

func NewBackendAdapter() *BackendAdapter { return &BackendAdapter{NameVal: "mock"} }

func (m *BackendAdapter) Name() string { return m.NameVal }

func (m *BackendAdapter) Launch(_ context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	m.Calls = append(m.Calls, BackendCall{Op: "launch", Addr: addr, Preset: p})
	if m.LaunchErr != nil {
		return ports.Handle{}, m.LaunchErr
	}
	return ports.Handle{PID: 4242, Addr: addr, Kind: "process", Ref: "4242"}, nil
}

func (m *BackendAdapter) WaitReady(ctx context.Context, addr string) error {
	m.Calls = append(m.Calls, BackendCall{Op: "waitready", Addr: addr})
	if err := ctx.Err(); err != nil {
		return err
	}
	m.readyCalls++
	if m.readyCalls <= m.ReadyAfter {
		return fmt.Errorf("%w: not ready yet (call %d)", domain.ErrNotReady, m.readyCalls)
	}
	return nil
}

func (m *BackendAdapter) Stop(_ context.Context, h ports.Handle) error {
	m.Calls = append(m.Calls, BackendCall{Op: "stop", Addr: h.Addr})
	return m.StopErr // nil => idempotent no-op on repeat
}

var _ ports.BackendAdapter = (*BackendAdapter)(nil)
```

```go
// test/mocks/nodeagent.go
package mocks

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type NodeAgent struct {
	NodeVal   domain.Node
	Instances []domain.ModelInstance
	LoadErr   error
	UnloadErr error
	Calls     []string
	nextID    int
}

func NewNodeAgent(node domain.Node) *NodeAgent { return &NodeAgent{NodeVal: node} }

func (m *NodeAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	m.Calls = append(m.Calls, "snapshot")
	return domain.NodeSnapshot{Node: m.NodeVal, Instances: m.Instances}, nil
}

func (m *NodeAgent) Load(_ context.Context, p domain.Preset) (domain.ModelInstance, error) {
	m.Calls = append(m.Calls, "load:"+p.ID)
	if m.LoadErr != nil {
		return domain.ModelInstance{}, m.LoadErr
	}
	m.nextID++
	inst := domain.ModelInstance{
		ID: fmt.Sprintf("inst_%d", m.nextID), PresetID: p.ID,
		NodeID: m.NodeVal.ID, State: domain.InstReady,
		Claim: domain.Claim{WeightsMB: p.EstWeightsMB},
	}
	m.Instances = append(m.Instances, inst)
	return inst, nil
}

func (m *NodeAgent) Unload(_ context.Context, id string) error {
	m.Calls = append(m.Calls, "unload:"+id)
	if m.UnloadErr != nil {
		return m.UnloadErr
	}
	out := m.Instances[:0]
	for _, in := range m.Instances {
		if in.ID != id {
			out = append(out, in)
		}
	}
	m.Instances = out
	return nil
}

var _ ports.NodeAgent = (*NodeAgent)(nil)
```

```go
// test/mocks/clock.go  — deterministic time for aging/timeouts/TTL/backoff
package mocks

import (
	"sync"
	"time"

	"mycelium/internal/ports"
)

type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{ch: make(chan time.Time, 1), fireAt: c.now.Add(d)}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves time forward and fires any timers now due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.fired && !c.now.Before(t.fireAt) {
			t.fired = true
			t.ch <- c.now
		}
	}
}

type fakeTimer struct {
	ch     chan time.Time
	fireAt time.Time
	fired  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool          { return !t.fired }

var _ ports.Clock = (*FakeClock)(nil)
```

Every mock needs `should_fail`-style injection (`LaunchErr`, `LoadErr`, `ReadyAfter`, …) and every module needs at least one test exercising a failure path. The `ReadyAfter` field is what lets unit tests prove the readiness gate, cold-start dedup, and load-timeout behavior with zero real processes.

---

## 7. Dependency injection

Every module takes its dependencies through its constructor. The scheduler is the canonical example — it is pure logic over `Placer`/`Estimator`/`Allocator`/`Clock` plus injected node agents, which is exactly why the Phase-0 brain is fully testable on the Mac.

```go
// internal/scheduler/scheduler.go
package scheduler

import (
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Scheduler struct {
	placer  ports.Placer
	clock   ports.Clock
	agents  map[string]ports.NodeAgent   // node id -> agent
	tel     ports.TelemetrySink
}

func New(placer ports.Placer, clock ports.Clock, agents map[string]ports.NodeAgent, tel ports.TelemetrySink) *Scheduler {
	return &Scheduler{placer: placer, clock: clock, agents: agents, tel: tel}
}
```

Production wiring (`cmd/mycelium/peer.go`):

```go
sch := scheduler.New(
	scheduler.NewPlacer(estimate.NewGGUF(cfg.GGUFParserPath), lease.NewAllocator(), clock.System{}),
	clock.System{},
	liveAgents,                       // real HTTP-backed NodeAgent per registered node
	telemetry.NewSQLiteSink(db),
)
```

Test wiring (`test/e2e/...`):

```go
clk := mocks.NewFakeClock(t0)
agents := map[string]ports.NodeAgent{
	"node_spark":  mocks.NewNodeAgent(fixtures.MakeSparkNode()),
	"node_4090a":  mocks.NewNodeAgent(fixtures.Make4090Node()),
}
sch := scheduler.New(
	scheduler.NewPlacer(&mocks.Estimator{Claim: domain.Claim{WeightsMB: 5600, KVReservedMB: 1476}}, lease.NewAllocator(), clk),
	clk, agents, &mocks.TelemetrySink{},
)
```

Never import a dependency at package scope; never reach for a global; never monkey-patch. If a test needs to replace behavior, it injects a mock.

---

## 8. Trace system

Two layers. (1) Every `PlacementDecision` carries a `Trace` of the scheduler's reasoning (Document 1 §3.8) so the agent can see exactly why a job landed where it did. (2) Longer multi-step operations (install, drain, benchmark fan-out) emit a generic `Trace`.

```go
// internal/domain/trace.go
package domain

type TraceStep struct {
	Step   string         `json:"step"`
	Result string         `json:"result,omitempty"`
	Data   map[string]any `json:"data,omitempty"`
}
```

```go
// internal/trace/trace.go
package trace

import "time"

type Step struct {
	Operation  string         `json:"operation"`
	Input      map[string]any `json:"input,omitempty"`
	Status     string         `json:"status"` // pending | success | error
	Error      string         `json:"error,omitempty"`
	DurationMS float64        `json:"duration_ms"`
}

type Trace struct {
	Steps []Step `json:"steps"`
	clock func() time.Time
}

func New(now func() time.Time) *Trace { return &Trace{clock: now} }

// Do runs fn as a recorded step. On error the step is marked "error" and the
// error is both recorded and returned.
func (t *Trace) Do(op string, input map[string]any, fn func() error) error {
	s := Step{Operation: op, Input: input, Status: "pending"}
	start := t.clock()
	err := fn()
	s.DurationMS = float64(t.clock().Sub(start).Microseconds()) / 1000.0
	if err != nil {
		s.Status, s.Error = "error", err.Error()
	} else {
		s.Status = "success"
	}
	t.Steps = append(t.Steps, s)
	return err
}
```

Example placement trace (the agent's debugging surface; identical shape to Document 1 §3.8):

```json
"trace": [
  { "step": "estimate", "result": "weights=5600MB kv=1476MB @ctx8000" },
  { "step": "filter",   "data": { "kept": ["node_spark","node_4090a"], "dropped": { "node_4070ti": "fit", "node_b70": "label.vendor" } } },
  { "step": "select",   "data": { "candidates": ["spark:warm-qwen9b","4090a:cold"], "speed_pref": "auto" } },
  { "step": "score",    "result": "winner=spark:warm-qwen9b (warm instance + most available)" },
  { "step": "admit",    "result": "batched onto warm instance, no eviction" }
]
```

Because `trace.Trace` takes its clock as `func() time.Time`, tests pass `FakeClock.Now` and get deterministic durations.

---

## 9. CI configuration

```yaml
# .github/workflows/ci.yml
name: ci
on: [push, pull_request]

jobs:
  fast:
    runs-on: ubuntu-latest          # no GPU; everything here is mocked
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.23' }
      - run: go vet ./...
      - run: go build ./...
      - name: unit + contract + integration + e2e (no hardware)
        run: go test ./... -race -covermode=atomic -coverprofile=cover.out
      - name: coverage gate
        run: |
          go run ./tools/covergate -profile cover.out \
            -min 0.85 \
            -require internal/scheduler=1.0 \
            -require internal/lease=1.0 \
            -require internal/peer/coordinator=1.0 \
            -require internal/peer/recovery=1.0 \
            -require internal/node/admission=1.0
  smoke:
    if: github.ref == 'refs/heads/main'
    runs-on: [self-hosted, gpu]      # a real machine in the fleet
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.23' }
      - name: smoke (real engines / machines)
        run: go test -tags smoke ./test/smoke/... -timeout 20m
```

`go test ./...` (the default everywhere, including the local dev Mac) never compiles the `smoke` files because they carry `//go:build smoke`. Hardware is touched only by the gated `smoke` job and by a developer explicitly running `go test -tags smoke`.

---

## 10. Coverage targets and pitfalls

Targets: 85%+ lines per module; 100% on `internal/scheduler`, `internal/lease`, the federation authority/recovery packages (`internal/node/admission`, `internal/peer/coordinator`, `internal/peer/recovery`), conformance suites, and fixture factories; every error path covered; every public method covered.

Avoid (Go-specific reading of the standard pitfalls):

1. **No contracts before implementation** — write the `internal/ports` interface and its conformance suite before the impl.
2. **Mocks that drift** — every mock gets a compile-time `var _ Iface` assertion *and* runs the conformance suite. Both, not one.
3. **Vibes-based gates** — a gate is `go test ./... -race` passing and the coverage gate passing, never "looks good."
4. **Monolithic phases** — if a phase is more than ~a day of agent work, split it (Document 3 already does).
5. **Import-time / global deps** — no package-level singletons, no `init()` wiring. Constructor injection only; it is what makes the Mac-only build possible.
6. **No "when stuck" guidance** — Document 3 grants explicit permission to ship a gate-passing 80% solution with TODOs.
7. **Real tools in fast tests** — every external (engine, gguf-parser, network, clock) has a mock; real ones live only in `smoke/`.
8. **No error-path tests** — every mock has failure injection; every module has at least one failure test (e.g. `BackendAdapter.LaunchErr`, `NodeAgent.LoadErr`, `ReadyAfter` exceeding the load timeout).
9. **Non-deterministic time** — never `time.Now()`/`time.Sleep` in code under test; inject `Clock`, drive with `FakeClock`.
10. **Flat structure** — keep the deep `internal/<module>` tree from Document 1 §5 so the agent navigates by path.
11. **Untested concurrency/federation invariants** — the federation safety properties are not "looks right," they are tests: two coordinators racing one resource (the `FakeClock`-driven optimistic-concurrency commit, exactly one wins, the loser gets `ErrStaleFence`), a stale-fence commit rejected, a partitioned peer cannot claim unreachable remote capacity, and a dead peer's unfinished jobs are re-coordinated from the registry. Drive them deterministically with mocks + `FakeClock`; never rely on real timing.
