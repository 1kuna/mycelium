# Mycelium — Development Guide (Document 3 of 3)

**Audience: the coding agent.** This is the execution plan. Build it one phase at a time, in order. A phase is done only when its **gate** passes — gates are concrete commands with pass criteria, never "looks good." Do not start phase N+1 until phase N's gate is green.

This guide assumes Document 1 (the spec — *what* to build, esp. §3 the core model, §5 the repo tree, §6 the locked decisions) and Document 2 (the testing & modularity architecture — the Go contracts, mocks, fixtures, `FakeClock`, conformance suites, and CI tiers). When this guide says "implement the `Placer`," the *shape* is in Document 2 §2 and the *behavior* is in Document 1 §3.

## How to use this guide

- **Gates are runnable.** Each phase ends with exact `go test` / coverage commands and a pass bar. Run them. If they don't pass, the phase isn't done.
- **"Your call" sections are yours.** Where you see *Your call*, the implementation decision is genuinely yours to make — pick what you think is best within the stated hard requirements. The hard requirements are non-negotiable; the approach is not dictated.
- **Parallel-OK** marks work that can proceed concurrently once the phase's contracts exist.
- **When stuck:** ship the part that passes the gate and leave a `// TODO(phase-N):` for the rest. A gate-passing 80% beats a blocked 100%. Note what you deferred at the top of the phase's PR/commit.
- **Do not reintroduce rejected designs.** Document 1 §6 lists the locked decisions with their rejected alternatives. If a simpler-looking path contradicts one — naive FIFO, model-as-unit, hard-default preemption, weights-only fit, in-process engines, Docker Mac workers, **a single fleet leader / elected scheduler host (D14), a replicated consensus event-log as source of truth (D15/D16), cross-coordinator pre-send negotiation (D15), SSH-based peer transport (D12), or sharding one model across machines / MLX-distributed (D17)** — it was already ruled out. Don't "improve" back into it.
- **Iterate freely inside the fast tiers; the gate is the floor.** The `unit`/`contract`/`integration`/`e2e` tiers are mock-only, deterministic, and hardware-free — that is your sandbox to perfect the implementation. Write a test, run it, refine, re-run, as many loops as it takes; tighten beyond the gate where it makes the code better. The only place *not* to grind is a smoke/hardware test that needs hardware you don't have — that's a defer-and-log, not a puzzle to solve (see AGENTS.md → Autonomous operation).
- **Self-test cadence.** Run the fast gates after every unit of work; run the full phase gate at each boundary. Never advance on a red gate. The gates and conformance suites are how you prove your own work without a human checking.
- **Spec is read-only; log decisions and blockers.** Don't edit these three documents. Record every *Your call* you resolve in `DECISIONS.md` and everything you set aside in `BLOCKERS.md`. If you think the spec is wrong, add a `PROPOSED SPEC CHANGE` to `DECISIONS.md` and keep building to spec — don't silently diverge.

The phase order follows the standard dependency spine: scaffold → contracts/types → test infra → most-depended module (the scheduler core) → its consumers (node agent + owner-authority, gateway) → integrations → peer membership → optimizer → federated coordination & resilience. The scheduler/Placer is peer-agnostic (pure logic over a snapshot), so the same brain serves one coordinator in early phases and any coordinator once federation lands. Real specialty hardware appears only at phase-boundary smoke gates; everything else is built and verified on the local dev Mac against the mock fleet.

---

## Phase 0 — Scaffold, contracts, test infra, and the scheduler/lease/safety core

**Goal:** stand up the module skeleton and *all* of Document 2's test machinery, then build the novel core — the scheduler, lease/allocation, and safety model — entirely against mocks. This is the riskiest, highest-value phase and it touches no hardware.

**Depends on:** nothing.

**Build (in this sub-order):**

1. **Scaffold** the repo tree from Document 1 §5: `go.mod` (`module mycelium`, Go 1.23), `cmd/mycelium/` with `main.go` dispatching `run` (the peer daemon — `peer.go`) and `ctl` (the `myce` surface), and empty package dirs under `internal/` and `test/`. There is no `server`/`node` subcommand — one peer daemon, with `compute` on/off via config/flag.
2. **`internal/domain`** — all types and enums and typed errors from Document 2 §2 (`Job`, `Node`, `Accelerator`, `Preset`, `Claim`, `ModelInstance`, `Lease`, `Reservation`, `PlacementDecision`, `FleetSnapshot`, `NodeSnapshot`, `RunMetric`, `Project`, the federation types `Peer`/`LeaseOffer`/`JobRecord`, and the `domain.Err*` set incl. `ErrStaleFence`). No logic, std-lib only.
3. **`internal/ports`** — every interface from Document 2 §2 (`Clock`, `ResourceEstimator`, `Allocator`, `BackendAdapter`, `NodeAgent`, `Placer`, `TelemetrySink`, plus the federation ports `AdmissionController`/`Coordinator`/`JobRegistry`/`PeerDiscovery` and stubs for `ModelRegistry`, `Catalog`, `TelemetryStore`, `Optimizer`, `Tunnel`, `Store`). Define the interface now even where the implementation is a later phase — contracts before implementations.
4. **Test infra** *(Parallel-OK once ports exist — three separate workstreams):*
   - **`test/mocks`** — hand-written `BackendAdapter` (with `ReadyAfter`/`LaunchErr`/`StopErr`), `NodeAgent` (with `LoadErr`/`UnloadErr`), `ResourceEstimator`, `TelemetrySink`, and `FakeClock` (with `Advance`). Each gets a `var _ ports.X = (*Mock)(nil)` assertion.
   - **`test/fixtures`** — functional-option factories `MakeNode`/`MakeSparkNode`/`Make4090Node`/`MakeJob`/`MakePreset`/`MakeClaim`, plus their self-tests.
   - **`internal/trace`** + `domain.TraceStep` — the `Trace.Do` recorder taking an injected clock.
5. **Conformance suites** (`test/contract`) for the dual-implemented interfaces (`BackendAdapter`, `NodeAgent`, `ResourceEstimator`, `Allocator`) and the fast-tier tests that run them against the mocks.
6. **`internal/estimate`** — a *mockable* `ResourceEstimator`. For Phase 0, a deterministic in-memory estimator is enough (`weights + ctx*concurrency*kv_per_token`); the real GGUF-preflight path is Phase 1. Hard requirement: it implements `ports.ResourceEstimator` and passes the estimator conformance suite.
7. **`internal/lease`** — `Allocator` (`usable_vram = vram_total*max_util - reserved_headroom`), `safety.go` (`max_util` hard ceiling + `oom_severity`: catastrophic ⇒ extra margin **and** `CanStackLoad` returns false while a load is in flight), `reservation.go` (headroom + pinned). 100% coverage required.
8. **`internal/scheduler`** — the brain: `filter` → `selector` (speed-pref-aware) → `scorer` → `scheduler` (admit/queue) → `preempt` (soft default; hard ladder; fit-forced reallocation = same priority test, candidate set pinned to the one unit). Implements `ports.Placer`. Keep it **peer-agnostic** — a pure function over a `FleetSnapshot`, no I/O, no knowledge of who is coordinating — so the identical brain serves any coordinator once federation lands (Phase 6). 100% coverage required.

**Your call (Phase 0):**
- *Queue structure and aging.* Heap, sorted slice, whatever. Hard requirements: priority order honored; **deterministic under `FakeClock`**; no starvation (aging must let a long-waiting background job eventually beat fresh higher-priority arrivals — you choose the aging curve); every dequeue decision is traceable.
- *Scoring weights.* The relative weight of warm-instance locality vs fit-tightness (packing) vs speed-pref alignment is yours to tune. Hard requirements: deterministic, and every score appears in the `PlacementDecision.Trace`.
- *Preemption victim selection & tie-breaks.* When several instances are eligible to be displaced, which one. Hard requirements: never violates `max_util`; prefers lowest-priority then (your tie-break, e.g. least-in-flight or most-recently-loaded); the displaced instance re-places if it fits else re-queues at its own priority.

**When stuck:** get soft-preempt + the common hard-preempt path green first; if the exotic fit-forced-reallocation tie-breaks (multi-unit, multi-victim) are gnarly, ship the single-victim case and `// TODO(phase-0):` the rest. The gate below does not require the exotic cases — but it does require soft queueing, single-victim hard preempt, and the §3.6 worked example.

**Also produce in Phase 0:** `AGENTS.md` at the repo root (runtime rules for agents working in this repo) and the three skill files from Document 1 §4. `AGENTS.md` must state: the build/test commands; "fast tiers touch no hardware/network/real-time — inject `Clock`, never call `time.Now()`/`time.Sleep`"; "every port impl and mock carries a `var _ ports.X` assertion and every dual-impl interface has a conformance suite"; the coverage gates; "navigate by the `internal/<module>` path"; "when stuck, ship the gate-passing 80% with a TODO"; and "do not reintroduce a Document 1 §6 rejected alternative."

**Gate (Phase 0):**

```bash
go build ./...                                   # clean
go vet ./...                                     # clean
go test ./... -race                              # ALL green (unit+contract+integration+e2e; smoke excluded by build tag)
go test ./internal/scheduler/... ./internal/lease/... -race -covermode=atomic -coverprofile=core.out
go tool cover -func=core.out | grep total        # internal/scheduler AND internal/lease == 100.0%
go test ./... -covermode=atomic -coverprofile=all.out
go tool cover -func=all.out | grep total         # overall >= 85.0%
```

Plus one named behavioral check that must pass — the Document 1 §3.6 worked example, as an `e2e` test against a mock fleet + `FakeClock`:

> Given a free `MakeSparkNode()` co-hosting 3× interactive 9B + a 1B ASR + a background 27B (within KV), a new **interactive 120B** job flagged `hard_for_interactive` (a) hard-preempts the lowest-priority occupant (the background 27B), (b) re-places that 27B onto a `Make4090Node()` if it fits there else re-queues it at background priority, and (c) loads the 120B on the Spark — and the resulting `PlacementDecision.Trace` shows the estimate/filter/select/score/preempt steps. `max_util` is never exceeded on any node at any step.

If all of the above pass, Phase 0 is done. **You now have the entire control-plane brain, provably correct, with nothing powered on.**

---

## Phase 1 — Node agent, the llama.cpp backend, and telemetry wiring

**Goal:** make a real machine do work. Build the per-node lifecycle, the first real backend adapter, and wire the telemetry substrate from the start (it must emit before the optimizer exists). Validate on the local dev Mac (llama.cpp Metal) and a second peer as a real remote peer.

**Depends on:** Phase 0 (ports, mocks, scheduler).

**Build:**

1. **`internal/node`** — `agent.go` (owns lifecycle, sends heartbeats via injected `Clock`, enforces leases locally), **`admission.go` (OWNER AUTHORITY, Doc 1 §3.12): a local transactional lease store that is the single truth for what runs on this machine — `Offer`/`Commit`/`Release`/`Preempt`, enforcing `max_util`/`oom_severity` in the commit transaction, stamping a per-resource fence/version, and returning `ErrStaleFence` when a coordinator's plan was built on an old version**, `process.go` (spawn/stop via `cmd`/`cmdStop`, readiness gate — no routing until health passes, cold-start dedup so two requests for the same cold preset wait on one load), `instance.go` (per-instance state machine), `loadingstate.go` (SSE loading-state during cold load), and a **startup reaper** that cleans up orphaned backend processes/containers left by a previous (crashed) run so none keep holding VRAM (Doc 1 §3.10 — llama-swap lacks this; build it in). The agent **sheds** (429-style) when saturated rather than building a local queue — all queueing lives in the coordinator. Implements `ports.NodeAgent` and `ports.AdmissionController`. *(Parallel-OK with step 2.)*
2. **`internal/backends/llamacpp`** — real `BackendAdapter`: launch the llama.cpp server subprocess (Metal on Mac, CUDA elsewhere) from a command template, poll its health endpoint for `WaitReady` (context-cancel-respecting), idempotent `Stop`. Must pass the `BackendAdapter` conformance suite against a real model in `smoke/`. *(Parallel-OK with step 1.)*
3. **`internal/estimate/gguf.go`** — replace Phase 0's in-memory estimator with the real GGUF-preflight path (shell out to gguf-parser). Keep the in-memory one available for tests. **Node-side parsing:** when a preset's model file is not local to the coordinator (the common case in a heterogeneous fleet), the estimator delegates inspection to the owning `NodeAgent` (a `ParseModel`/`InspectModel` call) and trusts the structured metadata it returns (Doc 1 §3.10) — the coordinator never needs local access to the file.
4. **`internal/telemetry`** — `sink.go` (ingest `RunMetric`), `store.go` (SQLite), `rollup.go` (per-project rolling distributions). Wire the node agent to emit a `RunMetric` after every run (tokens/sec, TTFT, load wall-clock, peak VRAM, context used). *(Parallel-OK with steps 1–2.)*
5. **Reactive overflow-requeue** (the resilience half of the optimizer, lands here, not Phase 5): when a backend signals context overflow, classify the error (per-backend), and requeue the job with a larger preset (observed-max × buffer, snapped to the next shared value that fits). The proactive/recommend half is Phase 5.

**Your call (Phase 1):**
- *llama.cpp launch flags & health-poll strategy.* Which flags, what health path, poll interval/backoff. Hard requirements: readiness-gated (no traffic pre-health), `WaitReady` respects context cancellation and a load timeout, `Stop` is idempotent, exact tuning flags are passable through the preset's `launch_profile`, and any **scheduler-computed tuning** (GGUF offload layers, multi-GPU tensor-split) is actually injected into the launch command, not dropped (Doc 1 §3.10).
- *Local lease store & fence semantics.* How the owner persists leases and versions its resource state (SQLite transaction, an in-process guarded store, etc.). Hard requirements: a `Commit` is atomic and serialized (two concurrent commits can't both succeed past `max_util`); the fence/version is monotonic; a commit referencing a stale fence returns `ErrStaleFence` rather than over-committing; and the store is the *only* authority for this machine's occupancy (no coordinator writes it directly). Must be testable with `FakeClock` and a mock — no real DB required in the fast tiers (an in-memory impl behind the same interface is fine; SQLite is the smoke/real path).
- *Reaper strategy.* How you identify orphaned backends from a prior run — pid file, process-name match, container label, a tracked-instances file. Hard requirement: after agent startup, no inference server from a previous run is left running and untracked.
- *Heartbeat interval & unreachable threshold.* You pick the numbers. Hard requirement: driven by the injected `Clock` so it's testable with `FakeClock`; a missed-heartbeat count flips the node to `unreachable` and the scheduler stops placing on it.
- *Per-backend overflow classification.* How you detect "context overflow" vs other errors from llama.cpp output. Hard requirements: a non-overflow error must NOT trigger a silent requeue (fail loudly instead); and a **failed resource estimate** is never placed on a guess — surface it, don't deploy (Doc 1 §3.11).

**When stuck:** the fast-tier work (node agent + telemetry against the mock backend) and the smoke-tier work (real llama.cpp) are separable — get the mock-backed lifecycle and telemetry green first, then bring up the real adapter behind the conformance suite.

**Gate (Phase 1):**

```bash
go test ./... -race                              # Phase 0 gates still green + new node/backend/telemetry unit+integration
go tool cover -func=all.out | grep total         # overall still >= 85.0%
# Smoke — REAL HARDWARE. Split into local (run yourself) and multi-node (needs a 2nd machine):
go test -tags smoke ./test/smoke/... -run 'Local'  -timeout 20m   # local dev Mac + a small GGUF — DO THIS
go test -tags smoke ./test/smoke/... -run 'Fleet'  -timeout 20m   # needs a second peer address — defer-and-log if absent
```

The **local smoke** (runnable by the agent on a local dev Mac with a small GGUF model) must demonstrate: (1) the llama.cpp conformance suite passes against a real model on Metal; (2) a full load → ready-gate → serve → graceful-stop cycle; (3) a `RunMetric` with real numbers lands in the telemetry store; (4) a request exceeding the preset context cap triggers a reactive requeue to a larger preset and then succeeds; (5) the startup reaper cleans up a deliberately-orphaned backend process. The **fleet smoke** (needs a second machine — provide its address, else defer-and-log per AGENTS.md): a second peer registers as a remote node over LAN with a loopback tunnel and the scheduler places and runs a job on it. (A minimal manual join is fine here; polished onboarding is Phase 4.)

---

## Phase 2 — Gateway, routing, and the OpenAI/Anthropic surface

**Goal:** one endpoint that hides the fleet. Apps speak OpenAI or Anthropic; the gateway extracts intent, asks the scheduler, routes to a live instance, and translates only when needed.

**Depends on:** Phase 1 (live instances to route to).

**Build:** `internal/gateway/` — `server.go` (HTTP), `router.go` (model-aware routing to a live instance, failover to another replica on error), `profiles/` (provider-profile-as-data + per-provider parser, Olla-style), `translate/` (passthrough when the backend already speaks the client's wire format; Anthropic↔OpenAI translation otherwise), `headers.go` (`X-Myc-*` decision/observability headers). `pkg/api/` gets the `openai.go`/`anthropic.go` request/response types.

**Your call (Phase 2):**
- *Passthrough-vs-translate detection.* How you decide a backend can take the client's format directly. Hard requirement: passthrough is preferred (cheaper); translation is lossless for the supported fields and explicitly errors on an unsupported field rather than silently dropping it.
- *Failover policy.* Retry count, which replica next, when to give up. Hard requirement: a failed instance is reported to the scheduler (so it can stop routing there) and the client sees a clean error after retries are exhausted.

**Gate (Phase 2):**

```bash
go test ./internal/gateway/... -race
go test ./... -race                              # everything still green
```

Plus an `e2e` test (mock node tier, fast): an OpenAI-shaped request and an Anthropic-shaped request both reach a (mock) instance through the scheduler; the response carries `X-Myc-*` headers; a cold target streams a loading-state SSE; killing the chosen instance mid-flight triggers failover to another replica. A smoke variant proves the same path end-to-end against a real llama.cpp instance.

---

## Phase 3 — Catalog, install, and presets

**Goal:** "add a model = one line." Turn a catalog item or a source URI into a materialized, ready-to-load preset.

**Depends on:** Phase 1 (presets must be loadable to be meaningful).

**Build:** `internal/catalog/` — `gallery.go` (catalog item → materialized `Preset`), `importers/` (`hf://`, OCI, local path → draft preset), `install.go` (async install jobs with progress, reusing the Job/telemetry plumbing), `provenance.go` (record where a preset came from). Surface it on the CLI as `myce add-model …`.

**Your call (Phase 3):**
- *Importer source support order.* Which of `hf://`/OCI/local you implement first and how deeply. Hard requirement: at least one importer is end-to-end real; the rest may be `// TODO(phase-3):` stubs that fail cleanly, not half-working.
- *Install concurrency & resumability.* Your design. Hard requirement: an interrupted install leaves no half-materialized preset registered as usable.

**Gate (Phase 3):**

```bash
go test ./internal/catalog/... -race
go test ./... -race
```

Plus: `myce add-model <something>` materializes a preset that Phase 1 can actually load (mock install + mock load in the fast tier; a small real model end-to-end in smoke), with provenance recorded and progress reported.

---

## Phase 4 — Peer membership and onboarding

**Goal:** "a peer joins the hive." One command on a new machine and it is discovered by the others on the LAN, advertises itself (including its `compute` flag), and is reachable — no central server to register with.

**Depends on:** Phase 1 (a node agent to onboard).

**Build:** `internal/peer/` (discovery side) + `internal/membership/` — `discovery_lan.go` (LAN auto-discovery via mDNS/DNS-SD, implementing `ports.PeerDiscovery`: advertise/peers/watch, carrying the `compute` flag), `token.go` (shared join token — membership only, rotatable/revocable), `tunnel.go` (loopback tunnel allocation), `rpc.go` (authenticated peer transport — **no SSH**). `discovery_overlay.go` (cross-NAT) is a **roadmap** stub behind the same `PeerDiscovery` interface — leave it unimplemented but interface-shaped, not half-built.

**Your call (Phase 4):**
- *LAN discovery mechanism.* mDNS, UDP broadcast, a seed address — your pick, behind `ports.PeerDiscovery`. Hard requirement: a `mycelium --join <token>` started on the same LAN is discovered by existing peers, appears in every peer's view, advertises its `compute` flag, and is reachable via an allocated loopback tunnel, with no manual address wrangling and no central server to register against.
- *Join-token handling.* The token gets a peer into the fleet; it is membership, not per-operation auth (LocalAI's "P2P token ≠ service auth" lesson). Support rotating and revoking the token so a leaked one can be invalidated without re-onboarding every peer, and don't treat token possession as authorization for anything beyond joining.

**Gate (Phase 4):**

```bash
go test ./internal/peer/... ./internal/membership/... -race
go test ./... -race
```

Plus a smoke check on a **second real machine** (a second peer): `mycelium --join <token>` brings it online in one command, the existing peer discovers it and lists it as `ready` with `compute` advertised, and a job placed from either machine runs on whichever the Placer selects.

---

## Phase 5 — The self-optimizer

**Goal:** turn accumulated telemetry into recommendations (and, behind a gate, automatic adjustments) that reduce reloads and preset fragmentation without violating fit/cap/latency targets. The reactive half already shipped in Phase 1; this is the proactive half.

**Depends on:** Phase 1 (telemetry history) — ideally several phases of real usage data first.

**Build:** `internal/optimizer/` — `presets.go` (the consolidation cost function: minimize reload/switch frequency + preset fragmentation, subject to fit/cap/latency, weighed against tokens/sec + load wall-clock; **backend-aware reload cost** — a context change is a reload for GGUF, ≈ free for vLLM within its launched max), `recommend.go` (distribution-shift detection on the per-project rolling stats → emit a `context_cap_recommendation` like Document 1 §3.7), `apply.go` (auto-apply **only** when the per-project toggle is on; default off).

**Your call (Phase 5):**
- *Anomaly / shift detection method.* Rolling-percentile thresholds, EWMA, CUSUM, your choice. Hard requirements: **deterministic and explainable — no ML model**; every recommendation carries the observed stats and a human-readable rationale (so the user can see *why*); it only ever *recommends* unless the project's auto-apply toggle is explicitly on.

**When stuck:** ship `recommend.go` (the valuable part) fully and gate-passing; `apply.go`'s auto-application can be the deferred piece (`// TODO(phase-5):`) since it's off by default anyway.

**Gate (Phase 5):**

```bash
go test ./internal/optimizer/... -race
go test ./... -race
```

Plus behavioral checks (fast tier, synthetic telemetry): (1) feeding a project's stats that have shifted (avg 4k, p95 12k against a 16k cap, with 6k already used by another project) produces a `context_cap_recommendation` recommending ~6k with the correct rationale fields; (2) with the auto-apply toggle **off**, nothing changes; with it **on**, the preset cap is updated and the change is logged; (3) the consolidation cost function, given two presets that could share a context value, recommends collapsing them and the math is asserted.

---

## Phase 6 — Federated coordination & resilience

**Goal:** make coordination peer-distributed and the fleet failure-tolerant. Any peer can coordinate a job (parallel-poll the fleet → run the Placer → commit on the owner with optimistic concurrency → relay the result); jobs survive in a replicated registry; a dead peer's unfinished work is rescued onto live peers; and the compute peers analyze telemetry as a group. This is the layer that delivers "no leader, submit anywhere, one machine down is business as usual." It builds on Phase 1's owner-authority (`AdmissionController`) and Phase 4's discovery (`PeerDiscovery`), and it reuses the **peer-agnostic Placer from Phase 0 unchanged**.

**Depends on:** Phase 1 (owner-authority commit), Phase 4 (peer discovery + RPC), Phase 5 (the optimizer the group analysis round drives). Effectively the capstone.

**Build:** `internal/peer/` —
1. **`coordinator.go`** (`ports.Coordinator`) — the per-job role. `ClaimJob` → record in the registry; `Plan` → snapshot candidate peers **in parallel** (via `NodeAgent.Snapshot` over RPC) and run the Placer; `Commit` → call the chosen owner's `AdmissionController.Commit` with the fence from its offer, and on `ErrStaleFence` **re-plan against fresh truth** (optimistic-concurrency retry, bounded); then proxy + **relay the response back through the coordinator** to the client. `Release` on completion/failure. No self-preference — the local machine is just one candidate.
2. **`registry.go`** (`ports.JobRegistry`) — the small replicated, eventually-consistent job record (`JobRecord`, incl. the request payload for rescue). It is **not** a consensus event-log; authority stays at the owner.
3. **`heartbeat.go`** — liveness over `PeerDiscovery`/RPC, `Clock`-driven; a missed-beat threshold marks a peer dead.
4. **`recovery.go`** — on a peer death, find its unfinished `JobRecord`s and re-coordinate them onto live peers, **double-checking the relevant owner before acting** (the owner may report the job already finished — ignore the stale registry row).
5. **`internal/telemetry/group.go`** — the periodic **group analysis round** across **compute-enabled** peers: whichever compute peer's timer fires gathers the fleet's telemetry and runs the Phase 5 optimizer on a compute node with spare CPU, no permanent owner; thin/compute-off peers never run it.

**Your call (Phase 6):**
- *Registry replication mechanism.* How the `JobRecord` set is replicated across peers (gossip, a small CRDT-ish last-writer-wins table, periodic pull-merge). Hard requirements: survives any single peer death; **no leader and no strong-consensus dependency**; eventually-consistent is acceptable *because* the owner remains the commit authority; a stale row can only cause a redundant owner re-check, never a double-booked resource.
- *Optimistic-concurrency retry policy.* Re-plan attempts and backoff on `ErrStaleFence`. Hard requirements: bounded (no infinite re-plan loop); deterministic under `FakeClock`; on exhaustion the job queues rather than force-commits.
- *Group-analysis election-free turn-taking.* How a compute peer decides it's "the one" this interval (timer jitter, a claim row in the registry, hashed interval). Hard requirements: at most one analysis round runs per interval; it runs only on a compute-on peer; if that peer dies mid-round another simply runs the next interval (no promotion, no stuck state).
- *Snapshot freshness vs. fan-out cost.* How fresh the parallel snapshot must be and how many peers to poll. Hard requirement: the authoritative check is always the owner's commit, so a slightly stale snapshot is fine — never treat the snapshot as authority.

**When stuck:** land the **happy-path coordinator + owner-commit + registry + single-peer-death recovery** first and gate it; if the trickier cases (simultaneous multi-peer death, registry merge conflicts under partition) are gnarly, ship the single-death case and `// TODO(phase-6):` the rest. The gate below requires the race, the stale-fence rejection, and single-peer-death recovery — not the exotic multi-failure cases.

**Gate (Phase 6):**

```bash
go test ./internal/peer/... -race -covermode=atomic -coverprofile=fed.out
go tool cover -func=fed.out | grep total          # internal/peer/coordinator + recovery == 100.0%
go test ./test/e2e/... -run Peer -race            # federation behavioral checks on a mock fleet + FakeClock
go test ./... -race                               # everything still green
# Smoke — REAL multi-peer (needs the second peer address; defer-and-log if absent):
go test -tags smoke ./test/smoke/... -run Federation -timeout 20m
```

Behavioral checks (fast tier, mock fleet + `FakeClock` — deterministic, no real timing):
1. **Submit-anywhere:** a job submitted to peer A and a job submitted to peer B each get coordinated by their submitter; both appear in the shared registry.
2. **Race resolves at the owner:** two coordinators plan against the same unit from the same snapshot and both attempt commit; **exactly one succeeds, the other gets `ErrStaleFence` and re-plans** — the unit is never double-booked, `max_util` never exceeded.
3. **Stale fence rejected:** a commit carrying an out-of-date fence is rejected even with capacity nominally free in the stale view.
4. **Owner adjudicates contention:** a higher-priority job (hard-for-interactive) proposed by one coordinator preempts a lower-priority occupant placed by another, via the owner's preemption ladder; the displaced job re-coordinates elsewhere or re-queues.
5. **No self-preference:** a job submitted from a compute-on peer is placed on a *different* peer when that peer is the better host (e.g. already warm with the model), proving the local machine isn't favored.
6. **Death recovery:** a peer running unfinished jobs is marked dead (advance `FakeClock` past the heartbeat threshold); its jobs are re-coordinated onto a live peer, and a job the dead peer had actually finished is *not* spuriously re-run (owner re-check wins over the stale registry).
7. **Partition safety:** a coordinator that cannot reach a peer does not place on it and cannot claim its capacity; reachable owners still serve.

The **smoke** variant (needs a second real machine): submit from the local dev Mac and from the second peer, confirm jobs run on whichever the Placer picks with results relayed back through the submitter, then kill one peer mid-job and confirm the other rescues the unfinished work.



These are designed-for in Documents 1–2 but deliberately out of the MVP build. Pick them up when there's bandwidth; the hooks are already paid.

- **Reverse benchmarking** — `internal/bench/`: fan out a `(prompt × model)` parent job into background children, write one output file per model + the objective metrics, stop. Judging is entirely the user's, entirely outside Mycelium (Document 1 §3.9, D13). No scorer, no agent integration. Hooks already paid: the fan-out-capable `Job` model and telemetry.
- **libp2p / cross-NAT overlay** — implement `discovery_overlay.go` behind the existing `PeerDiscovery`/`Tunnel` interfaces for membership beyond the LAN.
- **Auto-calibration** — grow the join-time tokens/sec probe into continuous self-benchmarking that refines `speed_class` and the optimizer's cost model.
- **Auto-apply on by default** — only after `recommend.go` is observed behaving in practice.
- **Auto engine/param selection** — "best engine + params for this model on this machine," informed by accumulated telemetry (and, if built, reverse-benchmark results). LocalAI's meta-backend resolution (a logical "llama-cpp" resolving to the best concrete backend for the host) is the mechanism shape.
- **Sticky / KV-cache-affinity routing** — route a conversation's follow-up requests back to the same warm instance to reuse its KV cache (a selector decorator over a conversation→instance TTL map, Olla-style). Big chat latency win; not MVP gateway core.
- **Privacy / sensitive-data handling** — a per-job handling class (e.g. PII / must-stay-encrypted). Its own scoping effort: it reshapes how peers store and move data (encryption in transit *and* at rest, what the job registry may replicate — today the request payload is replicated for rescue, which changes — and whether/how telemetry and recovery work for protected jobs). Functionality first; this is a deliberate later body of work, not a flag bolted on (Doc 1 §9).
- **Multi-user priority & permissions** — cross-submitter priority tiers and access control. Today priority is per-job and contention resolves equal-rights at the owner (Doc 1 §3.12); this adds a shared priority policy every owner applies identically, plus who-may-do-what.
- **Cross-coordinator pre-send negotiation** — coordinators coordinating *before* dispatch to optimize globally, vs. today's decide-then-let-the-owner-arbitrate. Reintroduces coordinator-to-coordinator coordination, so deferred until the simple model's limits are felt (Doc 1 §3.12, D15).

---

## The one-line summary for each phase

| Phase | Ships | Hardware? | Gate headline |
|---|---|---|---|
| 0 | scaffold + contracts + test infra + scheduler/lease/safety core | none | `go test ./... -race` green; scheduler+lease 100%; §3.6 example passes on mock fleet |
| 1 | node agent + owner-authority + llama.cpp + telemetry + reactive requeue | smoke only | smoke on local dev Mac (+ a second peer): real load/serve/stop, real metric, real requeue, atomic local commit |
| 2 | gateway + routing + OpenAI/Anthropic + `X-Myc-*` | smoke only | both wire formats route + translate + failover + loading-state |
| 3 | catalog + install + presets (`myce add-model`) | smoke only | one-liner materializes a loadable preset with provenance |
| 4 | peer membership + onboarding (token + LAN auto-discovery) | smoke only | `mycelium --join` brings a 2nd real machine online in one command, discovered by peers |
| 5 | optimizer (proactive recommend + gated auto-apply) | none | shift → correct recommendation; auto-apply respects the toggle |
| 6 | federated coordination + job registry + recovery + group telemetry | smoke only | submit-anywhere; race resolves at owner (`ErrStaleFence`); dead-peer jobs rescued; no leader |

Build Phase 0 first and completely. It is the part nobody else has built, and it is the part you can finish on the local dev Mac with nothing plugged in. Phase 6 is the federation capstone — it makes "submit anywhere, no leader, one machine down is business as usual" real, and most of it is testable on the local dev Mac against a mock fleet with `FakeClock`.
