# CODEX Findings

Audit date: 2026-06-09
Triage pass: 2026-06-09

Scope: `01-project-spec.md`, `02-testing-architecture.md`, and `03-development-guide.md` only. `04-engine-bootstrap.md` was intentionally ignored. This review used five subagents split across scheduler/lease, node/backends/telemetry, gateway/API, peer/federation, and tests/phase completeness, then consolidated and rechecked the claims against the code.

This triaged version removes findings that were too speculative, too defensive, or mostly product-policy questions. The remaining items are concrete SSOT drift, realistic runtime failure modes, or phase-gate gaps that can hide incomplete implementation.

Severity key:

- P1: realistic-use bug or direct SSOT drift that can break expected operation.
- P2: incomplete implementation/gate or realistic edge case that can hide or cause wrong behavior.
- P3: lower-blast-radius drift, known spec-ratification gap, or resilience issue worth fixing after the P1/P2 set.

## Summary

The implementation has substantial coverage of the SSOT model, especially around admission fences, owner-committed leases, node-side inspection, process identity/reaping, and fail-loud placement failures. The remaining high-signal drift clusters are:

- queue draining is not fit-aware even though the SSOT says the next freed slot goes to the highest-priority job that fits;
- real backend co-residency is blocked by fixed process ports;
- peer discovery/snapshot failures can collapse or misdrive fleet behavior instead of degrading capacity;
- gateway failover does not cover the request-registration race;
- reactive overflow handling is incomplete in streaming and bypasses the planner;
- several phase gates are present as smoke tests but do not prove the behavior the SSOT names.

## Fix Pass Status

Implemented on 2026-06-09:

- F-01 fixed: real process backends now implement dynamic per-instance launch addresses.
- F-02 fixed for the local drain path: the scheduler can dequeue the first currently runnable job instead of churning only the head item.
- F-03 fixed: bad discovered peers degrade to unreachable evidence where possible, and combined local/peer fleet snapshots preserve partial capacity.
- F-04 fixed: pre-provider `BeginRequest` failures now participate in gateway failover.
- F-05 fixed: cold streaming context overflow can reactive-requeue after Mycelium loading frames, as long as provider bytes have not started.
- F-06 fixed for heartbeat death classification: known peers missing from discovery are directly probed before misses accrue when a probe is configured.
- F-07 fixed: runtime overflow requeue now uses the reactive planner and shared registered context lengths.
- F-12 fixed: runtime source adoption saves parsed GGUF weights, KV-per-token, artifact size, model ref, and context metadata.
- F-14 fixed for peer startup: optimizer turn-taking now passes the local compute node ID into node selection.
- F-16 fixed: recovery continues across independent records and returns an aggregate error after the pass.

Still open after this bug-fix pass:

- F-08, F-09, and F-11 are smoke-gate proof gaps requiring real/phase-boundary orchestration.
- F-13 is a broader conformance-suite expansion, not a single runtime bug.
- F-15 is an SSOT ratification/product-decision item, not a code bug.

## Findings

### F-01 [P1] Real backend co-residency is blocked by fixed backend ports

SSOT: `01-project-spec.md:78` makes co-residency and preset collapse central to the resource model. `03-development-guide.md:79` explicitly puts coexistence on the Phase 1 path.

Code: `cmd/mycelium/config.go:191-194` defaults every compute node to `127.0.0.1:51848`; `cmd/mycelium/compute.go:87-97` wires that one listen address into the node agent; `internal/node/process.go:39-45` only supports `:0` when the backend implements `ports.DynamicBackendAdapter`; `internal/backends/llamacpp/adapter.go:109-142` implements only fixed-address `Launch`, and `rg DynamicBackendAdapter` shows no real backend implementation.

Impact: a second cold model instance on the same node either collides on the fixed port or fails with "requires a backend that reports dynamic ports" if configured with `:0`. This undermines the SSOT default that a unit can host multiple instances until VRAM/fit says otherwise.

Fix direction: implement real per-instance port allocation for process backends, return the resolved address in the handle, and add a smoke or real conformance case that loads two distinct cold presets on one node.

### F-02 [P1] Queue draining can block fitting work behind one no-fit job

SSOT: `01-project-spec.md:106` says the next freed slot goes to the highest-priority job that fits. `03-development-guide.md:43` calls out no starvation in Phase 0.

Code: `internal/scheduler/queue.go:54-63` always pops the aged-priority head; `internal/scheduler/service.go:355-379` drains only that popped job; when placement queues it again, `internal/scheduler/service.go:106-112` re-enqueues it.

Impact: one huge interactive job that still cannot fit can churn at the head of the queue and prevent smaller fitting jobs from running. This is a normal saturation scenario, not an exotic edge case.

Fix direction: make drain fit-aware. Scan the priority-ordered queue for the first job that fits the current snapshot, preserve enqueue time/aging for skipped jobs, and avoid consuming drain capacity on no-fit retries.

### F-03 [P1] One bad discovered peer can fail the whole fleet snapshot

SSOT: `01-project-spec.md:315` and `03-development-guide.md:240` require partition/unreachable cases to shrink capacity, not stop unrelated reachable work.

Code: `internal/gateway/peer_directory.go:67-70` returns immediately on `agentFor` errors such as missing addresses or tunnel-open failures. `cmd/mycelium/peer.go:1155-1163` makes `combinedFleet.Snapshot` return as soon as either local or peer-side snapshotting errors.

Impact: a single stale/bad discovered peer can make placement fail entirely even when local compute or other peers are still usable.

Fix direction: convert `agentFor` failures into `NodeUnreachable` evidence and continue. In `combinedFleet`, gather both sides and return reachable capacity with explicit unreachable records unless both sides are unusable.

### F-04 [P1] Gateway does not fail over when `BeginRequest` loses the lifecycle race

SSOT: `01-project-spec.md:545` describes serving through a selected owner with fallback/failover behavior. `03-development-guide.md:114` and `03-development-guide.md:118` put failover and graceful-stop race protection in the gateway phase.

Code: `internal/gateway/router.go:198-204` and `internal/gateway/router.go:438-448` release/fail and return immediately if `beginInstanceRequest` fails. The later upstream transport paths do remove the failed instance and continue (`internal/gateway/router.go:219-231`, `internal/gateway/router.go:481-493`), but request registration is outside that failover path.

Impact: if a warm instance disappears or becomes unready between placement and request registration during graceful stop, preemption, or backend crash, the gateway aborts instead of trying another replica before any provider bytes were sent.

Fix direction: treat pre-provider `BeginRequest` failures like upstream transport/5xx failures: finish/release the job, report the instance failure, remove it from the attempt fleet, and retry within the existing attempt budget.

### F-05 [P1] Cold streaming overflow cannot reactive-requeue after loading-state SSE starts

SSOT: `01-project-spec.md:138` requires reactive requeue after context overflow. `03-development-guide.md:83` puts reactive requeue in Phase 1, and `03-development-guide.md:127` includes loading-state SSE in Phase 2.

Code: for cold streaming, `internal/gateway/router.go:372-389` writes loading SSE and sets `started = true` before the upstream provider starts. Later, `internal/gateway/router.go:504-520` only requeues overflow when `ok && !started`.

Impact: a common path, cold model plus streaming chat plus too-long prompt, emits an error instead of retrying with a larger preset. Loading-state SSE is treated like provider-started output even though it is only Mycelium progress output.

Fix direction: distinguish "gateway loading event sent" from "provider bytes sent"; allow internal overflow requeue until provider output starts, or delay the requeue-inhibiting state transition.

### F-06 [P2] Discovery absence can count as peer death without direct health proof

SSOT: `01-project-spec.md:309-315` says the registry is visibility/rescue, partitions should degrade capacity, and stale registry state must cause owner checks, not corruption. `03-development-guide.md:239-240` makes peer death and partition handling part of the Phase 6 gate.

Code: `internal/peer/heartbeat.go:74-83` increments misses for known peers that disappear from discovery without probing the last known address. `internal/membership/peer_cache.go:57-63` stops its update watcher permanently if the upstream channel closes, and `internal/membership/peer_cache.go:115-130` returns cached peers until TTL expiry without refreshing upstream. `internal/membership/peer_discovery_lan.go:149-153` closes the watch channel on read error.

Impact: if discovery/watch is disrupted while peer RPC would still work, heartbeat can eventually treat a live peer as dead and run recovery against jobs the owner may still be serving.

Fix direction: before counting an absent known peer as missed, probe its last known RPC address. Treat discovery absence as weaker evidence than failed health RPC, and restart or refresh discovery after transient watch failures.

### F-07 [P2] Runtime overflow requeue bypasses the optimizer planner

SSOT: `01-project-spec.md:138` describes reactive optimization as measuring context usage, applying a buffer, snapping to shared contexts, and producing a new preset recommendation.

Code: `internal/optimizer/reactive.go:24` implements `PlanReactiveRequeue`, but the gateway uses `Presets.NextLargerContext` directly in both non-streaming and streaming paths (`internal/gateway/router.go:236-251`, `internal/gateway/router.go:504-520`).

Impact: the gateway can pick a larger registered preset, but it does not use observed tokens plus buffer and shared-context snapping. If the next preset is still too small, or no larger preset is pre-registered, the SSOT behavior is not delivered.

Fix direction: wire `PlanReactiveRequeue` into the runtime path using observed context usage and shared context lengths, then route/place/load the planned preset.

### F-08 [P2] Phase 6 smoke does not prove federation rescue or relay after peer death

SSOT: `03-development-guide.md:242` says the Phase 6 smoke must submit from either peer, confirm placement and relay, kill one peer mid-job, and confirm rescue.

Code: `test/smoke/phase6_federation_smoke_test.go:12-38` only sends one request through gateway A and one through gateway B, with optional expected-node assertions. It does not kill a peer mid-job, assert registry recovery, or prove relay through the original submitter after failure.

Impact: the Phase 6 gate can pass while the most important federation failure mode remains untested.

Fix direction: expand the smoke to orchestrate two real peers, run a long-enough job, terminate one peer, and assert registry recovery plus a successful client response through the surviving submitter.

### F-09 [P2] Phase 1 smoke does not prove its named reactive requeue behavior

SSOT: `03-development-guide.md:83` and `03-development-guide.md:104` put reactive context-overflow requeue in Phase 1 and require the local smoke gate to demonstrate load, serve, telemetry, requeue, and reaper.

Code: `test/smoke/phase1_local_smoke_test.go:24-84` sends one small completion, then manually calls `agent.RecordRun` at `test/smoke/phase1_local_smoke_test.go:66-76`. It never sends an over-context request that retries and succeeds.

Impact: the smoke can pass while the required reactive path is broken.

Fix direction: add a real over-context request that retries and succeeds on a larger preset. Keep telemetry assertions tied to the production serving path rather than direct manual metric insertion where possible.

### F-11 [P2] Phase 4 join smoke and startup path do not prove true one-command join

SSOT: `01-project-spec.md:313`, `01-project-spec.md:501`, and `03-development-guide.md:156-173` require no manual address wrangling and a Phase 4 smoke where a second machine runs `mycelium --join <token>` and appears in the fleet.

Code: `test/smoke/phase4_join_smoke_test.go:49-82` starts two local processes from generated configs and explicitly wires discovery addresses, including `nodeDiscoveryAddr` into the gateway config. Runtime startup only enables discovery when `join_token` is configured (`cmd/mycelium/peer.go:204-214`), and CLI `--join` parsing appends a seed peer from the join URI (`cmd/mycelium/peer.go:117-124`).

Impact: the automated smoke can pass without proving a real second-machine join, LAN discovery, or no manual address plumbing. A seed-address join may be acceptable, but that is narrower than the current SSOT wording.

Fix direction: keep the local two-process smoke as a dev check, but make the Phase 4 gate require the real second-peer join path and assert discovered peer metadata including `compute`. If a seed address is required, record the spec deviation explicitly.

### F-12 [P2] Catalog runtime source adoption parses GGUF metadata but discards it

SSOT: `03-development-guide.md:135-150` expects model catalog staging to materialize loadable presets with provenance/locality. `01-project-spec.md:88` and `01-project-spec.md:271-274` require node-side parsing/estimation facts to feed placement.

Code: `cmd/mycelium/peer.go:725-747` parses a local runtime source and validates `metadata.WeightsMB > 0`, but returns only `(localSource, true, nil)`. The adoption path then saves `req.Preset` unchanged at `cmd/mycelium/peer.go:566-580`.

Impact: a local GGUF can be accepted as ready while the saved preset still lacks weights, KV-per-token, context length, or artifact size derived from the parser. Later placement may fail, under-estimate, or rely on caller-supplied estimates instead of the parsed source of truth.

Fix direction: materialize a staged preset from parsed metadata: set model ref/source, estimated weights, KV/context facts where available, artifact size, node locality, and provenance.

### F-13 [P2] Conformance coverage has not kept up with implemented ports

SSOT: `02-testing-architecture.md:15`, `02-testing-architecture.md:374-386`, and `02-testing-architecture.md:390-487` require hand-written mocks and behavioral conformance for external dependencies and real/mock interface pairs.

Code: `test/contract/mock_conformance_test.go:12-74` covers backend, node, estimator, allocator, admission, registry, discovery, tunnel, and hardware mocks. `test/contract/real_conformance_test.go:12-26` covers only real job registries. Implemented phase-critical ports now include catalog staging/materialization, telemetry stores/peer sync, optimizer recommendations, real `PeerLANDiscovery`, and real `LANTunnel` surfaces without matching shared conformance coverage.

Impact: interface shape is compile-time checked in some implementations, but behavior drift between mock and real implementations can pass because the shared contract suite is missing.

Fix direction: add focused mocks and conformance suites for the phase-critical implemented ports above. Avoid expanding this into conformance for purely internal helper interfaces.

### F-14 [P2] Group optimizer turn-taking compares node IDs to peer IDs

SSOT: `01-project-spec.md:311` and `03-development-guide.md:217` define group analysis as running on compute-enabled peers, with election-free turn-taking and at most one active compute peer per interval.

Code: `cmd/mycelium/peer.go:2108-2123` selects a node from `FleetSnapshot.Nodes` and compares `selected.ID` to `selfID`. `selfID` is the peer ID passed by `startOptimizerEvaluator`, while `selected.ID` is a node ID. `config_validate.go` does not enforce `PeerConfig.ID == ComputeConfig.ID`; defaults often make them match, but valid config can diverge.

Impact: a compute peer with distinct peer/node IDs can skip its rightful optimizer turn. Divergent peer snapshots can also select different nodes, causing missed or duplicate rounds.

Fix direction: select over compute peer identities, or make node-to-peer identity mapping explicit and use a registry-backed interval claim to enforce one active runner.

### F-15 [P3] Scheduler disk filtering is an unratified SSOT change

SSOT: `01-project-spec.md:183` does not make disk fields part of the core node descriptor; `01-project-spec.md:97` and `03-development-guide.md:40` describe scheduling filters around max-util, labels/capabilities, OOM severity, and fit.

Code: `internal/scheduler/disk.go:9-31` applies disk-headroom placement filters, and `internal/scheduler/filter.go:128-130` drops nodes on those reasons. `DECISIONS.md:142` records a proposed spec change to make disk headroom a first-class placement constraint.

Impact: this looks intentional and may be the right product direction, but against docs `01-03` it is still a drift until the SSOT is ratified. A peer snapshot without disk facts can be dropped before accelerator fit is checked.

Fix direction: either ratify disk headroom in `01-03`, or change scheduler behavior so unknown disk does not drop otherwise-valid nodes.

### F-16 [P3] Recovery stops processing after the first bad recoverable record

SSOT: `01-project-spec.md:309-315` and `03-development-guide.md:239-240` describe recovery as fleet resilience work over unfinished jobs.

Code: `internal/peer/recovery.go:50-88` iterates candidate records but returns immediately on the first decision, cleanup, or rescue error.

Impact: one malformed/stale job record or one temporarily unreachable owner can prevent later independent jobs for the same dead peer from being rescued in that tick. This is not the top recovery risk, but it can turn one bad record into broader delayed recovery.

Fix direction: continue across records, record per-job failure evidence in the registry/log, and return an aggregate error after attempting all candidates.

## Removed During Triage

These were removed from the primary finding list because they were intentional, too speculative, or not clearly a useful implementation task:

- GitHub Actions CI disabled: intentional due runner credits.
- Production `X-Myc-*` request control headers ignored: this is security-sensitive and currently treated as trusted test mode, not clear SSOT drift.
- Local scheduler stale-fence terminal failure: production peer routing uses the coordinator path; the local-only path is not a typical product use.
- Standalone node-agent telemetry sink: gateway-owned routing already pushes run metrics to the owner store; remaining telemetry issues are covered by the Phase 1 smoke/reactive finding.
- `auto` speed preference cold-loads before warm reuse: too product-policy-dependent without a sharper SSOT rule for `auto`.
- Node-agent accelerator set ordering: low-likelihood internal boundary issue; not worth primary audit space unless external node RPC callers become supported.

## Checked But Not Counted As Primary Findings

- Owner telemetry is not entirely coordinator-local: `internal/gateway/router.go:1031-1084` routes metrics/samples to the owner telemetry peer when the selected node is remote.
- Private/redacted payload handling is intentionally rejected at ingress today (`internal/gateway/router.go:765-770` and `internal/gateway/server.go:146-152`), which is safer than accepting private jobs without recoverability.
- UDP broadcast discovery is allowed by `03-development-guide.md:162-163` even though `01-project-spec.md:313` names mDNS/DNS-SD. The issue is the join/gate proof and liveness behavior, not the protocol choice by itself.
