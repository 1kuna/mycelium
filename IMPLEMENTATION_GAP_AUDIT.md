# Implementation Gap Audit

Date: 2026-05-29

## Completion Addendum - 2026-05-29

This audit is preserved as the original checklist. A subsequent implementation
pass closed the non-hardware gaps it named: durable SQLite control state,
config-backed `mycelium server` / `mycelium node`, scheduler runtime queueing,
lease lifecycle, startup reaping, hardware discovery, GGUF inspection wiring,
node 429 shedding, gateway request controls, streaming proxying with loading
SSE, gateway telemetry, reactive overflow retry, catalog-backed local/HF/OCI
imports, resumable installs, loopback LAN tunnels, persisted join-token state,
optimizer rollups/recommendations, speed calibration, sticky routing,
benchmarks, CI/covergate, and the standalone `myce` control binary.

Current verified gates after the completion pass:

- `make ci`: passes; covergate reports total coverage 87.3%, with
  `internal/scheduler`, `internal/lease`, `test/contract`, and `test/fixtures`
  at 100%.
- `MYCELIUM_LLAMA_CPP_BINARY=$(command -v llama-server) MYCELIUM_LLAMA_CPP_MODEL=<repo>/.smoke-models/stories260K.gguf make smoke-local SMOKE_JSON=/tmp/mycelium-smoke-local-final.json`:
  `smoke ok: 8 passed, 0 skipped, 0 failed`.
- `MYCELIUM_REMOTE_PEER_ADDR=192.0.2.63:51847 MYCELIUM_REMOTE_PEER_MODEL=$HOME/.mycelium/models/stories260K.gguf make smoke-fleet SMOKE_JSON=/tmp/mycelium-smoke-fleet-final.json`:
  `smoke ok: 1 passed, 0 skipped, 0 failed`.

Remaining open items are hardware/engine-gated and are tracked in
`BLOCKERS.md`: real MLX, vLLM, CUDA/NVIDIA, and cross-machine
MLX-distributed smoke proof. `command -v mlx_lm.server`, `command -v vllm`,
and `command -v nvidia-smi` are empty on this dev host.

Scope: Compared `AGENTS.md`, `01-project-spec.md`, `02-testing-architecture.md`,
`03-development-guide.md`, and `skills/*.md` against the current code under
`cmd/`, `internal/`, `pkg/`, and `test/`. This audit is intentionally biased
toward naming missing integration work, even when a narrow unit-testable slice
exists.

## Current Verified Baseline

These checks passed during the audit:

- `gofmt -l .`
- `go build ./...`
- `go vet ./...`
- `go test ./... -race`
- `go test ./internal/scheduler/... ./internal/lease/... -race -covermode=atomic -coverprofile=core.out`
- `go tool cover -func=core.out | tail -n 20`: total 100.0% for scheduler and lease
- `go test ./... -covermode=atomic -coverprofile=all.out`
- `go tool cover -func=all.out | tail -n 30`: total 85.5%

Interpretation: the repo has green fast gates and phase-slice implementations,
but it is not production-complete Mycelium. Several pieces are test-only,
decision-only, manually wired, or intentionally stubbed.

## Product Runtime Gaps

### Server Is Still A Thin Single-Model Runtime

The real `mycelium server` path is not a durable fleet/catalog-backed control
plane yet.

- It requires `--model` and creates one ad hoc preset from CLI flags.
- It uses `estimate.NewInMemory()` instead of the GGUF estimator or a catalog
  registry.
- It has no persisted project defaults, fleet config, catalog loading, optimizer
  loop, or durable store wiring.

Evidence:

- `cmd/mycelium/server.go:40`
- `cmd/mycelium/server.go:88`
- `cmd/mycelium/server.go:98`
- `internal/ports/ports.go:85`

### Node Binary Hard-Codes The Local Machine Shape

`mycelium node` does not discover hardware or describe heterogeneous machines.

- It hard-codes one Apple unified-memory accelerator.
- It defaults to Darwin/Apple labels.
- It does not probe actual RAM/VRAM, Metal/CUDA availability, multiple
  accelerators, or speed class.
- It does not wire telemetry, a model inspector, or startup reaping in the CLI
  path.

Evidence:

- `cmd/mycelium/node.go:77`
- `cmd/mycelium/node.go:105`
- `cmd/mycelium/node.go:124`

### Scheduler Queue Is Not Integrated

The queue exists as a tested data structure, but placement is stateless.

- `Placer.Place` can return `ActionQueued`, but there is no persistent scheduler
  service that enqueues, dequeues, ages, and retries jobs.
- There is no later admission loop that drains queued jobs when capacity changes.
- The gateway does not own or consult a queue after `ActionQueued`.

Evidence:

- `internal/scheduler/placer.go:12`
- `internal/scheduler/placer.go:121`
- `internal/scheduler/queue.go:25`
- `internal/gateway/router.go:167`

### Preemption Is Decision-Only

The scheduler can decide that a victim should be preempted, but the runtime does
not enact the decision.

- Gateway loads the selected target but does not call `Unload` on preempted
  instances.
- Displaced work is returned in `Requeued`, but nothing actually requeues or
  restarts that work.
- Preemption is single-victim and single-accelerator shaped, not full
  multi-victim/multi-unit fit-forced reallocation.

Evidence:

- `internal/scheduler/preempt.go:17`
- `internal/scheduler/preempt.go:31`
- `internal/scheduler/preempt.go:50`
- `internal/scheduler/filter.go:27`
- `internal/gateway/router.go:167`

### Lease And Reservation Lifecycle Is Incomplete

The domain contains `Lease` and `Reservation`, and the allocator supports static
reserved headroom, but the full lifecycle is not present.

- No component grants, persists, expires, or releases `domain.Lease` objects.
- Pinned reservations are defined but not implemented beyond the enum.
- Reservations are not loaded from config/store or attached to projects.

Evidence:

- `internal/domain/types.go:129`
- `internal/domain/types.go:138`
- `internal/domain/enums.go:98`
- `internal/lease/reservation.go:5`
- `internal/lease/safety.go:42`

### Startup Reaper Exists But Is Not Wired

The reaper code and tests exist, but real node startup does not call it.

- Backend process references are not persisted during launch.
- `mycelium node` does not call `NewReaper(...).Reap(...)` before serving.
- The Phase 1 smoke proves the reaper helper manually, not startup behavior.

Evidence:

- `internal/node/reaper.go:28`
- `internal/node/reaper.go:50`
- `internal/backends/llamacpp/adapter.go:100`
- `cmd/mycelium/node.go:19`
- `test/smoke/phase1_local_smoke_test.go:110`

### Run Telemetry Is Not Emitted By The Real Request Path

Telemetry storage exists, and `Agent.RecordRun` exists, but real serving does
not automatically produce `RunMetric` records.

- Gateway does not record tokens/sec, TTFT, load wall-clock, peak VRAM, or
  context used after upstream calls.
- Node HTTP API has no route for completed run metrics.
- The Phase 1 smoke manually calls `agent.RecordRun`, with some fields supplied
  by the test instead of the runtime.

Evidence:

- `internal/node/agent.go:156`
- `internal/gateway/router.go:196`
- `internal/node/http.go:19`
- `test/smoke/phase1_local_smoke_test.go:66`

### Node Shedding Is Not HTTP 429-Style

The agent returns loud `ErrNoFit`-style errors when saturated, but the node HTTP
transport maps all load errors to HTTP 500.

Evidence:

- `internal/node/agent.go:214`
- `internal/node/http.go:43`
- `internal/node/http.go:147`

## Backend And Estimation Gaps

### Only llama.cpp Has A Real Adapter

`vllm`, `mlx`, and `custom` exist as enum/profile/cost-model concepts, not real
backend adapters.

Evidence:

- `internal/domain/enums.go:44`
- `internal/backends/llamacpp/adapter.go:1`

### GGUF Estimation Exists But Is Not Production-Wired

The estimator and command parser exist, but runtime paths do not use them.

- `mycelium server` uses `estimate.NewInMemory()`.
- Catalog install estimates weights from file size and hard-codes
  `KVPerTokenMB: 0.01`.
- `mycelium node` does not wire a real model inspector/parser.

Evidence:

- `internal/estimate/gguf.go:20`
- `internal/estimate/gguf.go:79`
- `cmd/mycelium/server.go:98`
- `internal/catalog/install.go:124`
- `cmd/mycelium/node.go:124`

### Scheduler-Computed Launch Tuning Is Not Computed

The llama.cpp adapter does thread `Preset.LaunchArgs` into the launch command,
but the scheduler does not compute offload layers or tensor splits.

Evidence:

- `internal/backends/llamacpp/adapter.go:106`
- `internal/backends/llamacpp/adapter.go:115`
- `internal/scheduler/placer.go:35`

### Graceful Stop / In-Flight Request Guard Is Missing

The docs call out the race where graceful stop can outrun request in-flight
registration. There is no per-instance in-flight wait group or boundary guard in
the runtime.

Evidence:

- `internal/domain/types.go:72`
- `internal/node/agent.go:120`
- `internal/gateway/router.go:196`

## Gateway And API Gaps

### Gateway Intent Extraction Is Hard-Coded

Every gateway request becomes one interactive, throughput-oriented `chat` job in
project `gateway`.

- Project defaults are not loaded or merged.
- Priority, speed preference, context cap, preemption, task type, and per-request
  overrides are not parsed from headers or config.
- Completion and Anthropic requests do not become distinct job intents.

Evidence:

- `internal/gateway/router.go:98`

### Streaming Is Buffered, Not Proxied

The gateway reads the full upstream response body into memory. For streaming
requests, it prepends a loading event after the upstream body has been read; it
does not proxy chunks as they arrive.

Evidence:

- `internal/gateway/router.go:140`
- `internal/gateway/router.go:196`
- `internal/gateway/router.go:212`

### API Surface Is Narrow

The OpenAI/Anthropic structs cover a minimal text-chat/completion subset.

- No OpenAI tools/function calls, tool choice, images, embeddings, audio, or
  richer message content.
- No Anthropic tool use/tool result/image/document blocks.
- Anthropic streaming to OpenAI is explicitly unsupported.
- Strict decoding is fail-loud, which is good, but it also shows these fields
  are not yet supported.

Evidence:

- `pkg/api/openai.go:3`
- `pkg/api/anthropic.go:3`
- `internal/gateway/translate/translate.go:80`
- `internal/gateway/translate/translate.go:99`
- `internal/gateway/translate/translate.go:180`

### Real Gateway Smoke Coverage Is Narrow

Fast e2e covers OpenAI, Anthropic, headers, loading-state, and failover with
mocks. Smoke covers only a local OpenAI-shaped llama.cpp path.

- No real Anthropic smoke.
- No real failover smoke.
- No real loading-state streaming smoke.

Evidence:

- `test/e2e/phase2_gateway_test.go:23`
- `test/smoke/phase2_gateway_smoke_test.go:26`

### Sticky / KV-Cache Affinity Is Not Implemented

This is explicitly post-MVP roadmap, not part of the current phase gates.

Evidence:

- `03-development-guide.md:209`

## Catalog Gaps

### Remote Importers Are Explicit Stubs

Only local file import is real. `hf://` and `oci://` fail loudly.

Evidence:

- `internal/catalog/importers/importers.go:23`
- `internal/catalog/importers/importers.go:25`
- `internal/catalog/importers/importers.go:27`

### Catalog Is Not Server-Backed

`myce add-model` materializes preset/provenance JSON, but `mycelium server` does
not load those presets as its registry.

Evidence:

- `cmd/mycelium/ctl.go:26`
- `internal/catalog/gallery.go:11`
- `cmd/mycelium/server.go:88`

### Install Jobs Are Minimal

The installer has an async wrapper and progress slice, but it is not a durable
job system.

- Progress is returned after the install, not streamed or queryable.
- There is no resume support beyond avoiding half-materialized presets.
- Fast tests do not wire the materialized preset into a mock node load path.

Evidence:

- `internal/catalog/install.go:25`
- `internal/catalog/install.go:43`
- `internal/catalog/install.go:76`
- `cmd/mycelium/ctl.go:50`

## Membership And Onboarding Gaps

### LAN Discovery Is Seed-Address Join, Not Discovery

The MVP docs allowed a LAN mechanism, but the current implementation is a
`mycjoin://server?token=...` seed address flow. There is no mDNS or broadcast
discovery.

Evidence:

- `internal/membership/discovery_lan.go:26`
- `internal/membership/discovery_lan.go:69`

### Tunnel Is Not A Loopback Tunnel

`LANTunnel.Open` records and returns the node's advertised address. It does not
allocate a local loopback endpoint or tunnel traffic.

Evidence:

- `internal/membership/tunnel.go:21`
- `DECISIONS.md:28`

### Overlay Discovery Is Explicitly Unimplemented

This is an allowed roadmap stub in Phase 4.

Evidence:

- `internal/membership/discovery_overlay.go:11`
- `internal/membership/discovery_overlay.go:13`
- `03-development-guide.md:159`

### Membership State Is In-Memory

Token rotation/revocation works in memory, but there is no durable membership
store.

Evidence:

- `internal/membership/token.go:11`
- `internal/membership/registry.go:15`
- `internal/ports/ports.go:85`

### Phase 4 Smoke Is Still A Manual Harness

The smoke test expects the gateway and joined node to already be running; it
does not automate `mycelium node --join <token>` on the second machine.

Evidence:

- `test/smoke/phase4_join_smoke_test.go:15`

## Optimizer Gaps

### Optimizer Is Not Wired Into Runtime State

The optimizer package produces recommendations and apply results, but no runtime
component periodically reads telemetry and persists project/preset changes.

Evidence:

- `internal/optimizer/recommend.go:41`
- `internal/optimizer/apply.go:16`
- `cmd/mycelium/server.go:40`

### Stats Provider Is Not Connected To Telemetry Rollups

`telemetry.RollupContext` and `optimizer.StatsProvider` are separate pieces; no
adapter wires SQLite metrics into optimizer project stats.

Evidence:

- `internal/telemetry/rollup.go:17`
- `internal/optimizer/recommend.go:37`

### Reactive Requeue Is Planning-Only In Runtime

Reactive overflow requeue logic exists, but gateway/node request handling does
not classify a real backend overflow, choose a larger preset, and retry through
the normal scheduler path.

Evidence:

- `internal/optimizer/reactive.go:24`
- `internal/gateway/router.go:118`
- `test/smoke/phase1_local_smoke_test.go:86`

### Cost Model Is Synthetic

The consolidation function is deterministic and tested, but it is not yet
calibrated from observed load wall-clock, tokens/sec, or backend-specific real
reload data.

Evidence:

- `internal/optimizer/presets.go:26`
- `internal/optimizer/presets.go:56`

## Testing, CI, And Proof Gaps

### No CI Or Covergate Tool Exists

The testing architecture sketches CI and `tools/covergate`, but the repo does
not contain them.

Evidence:

- `02-testing-architecture.md:881`
- no `.github/workflows/`
- no `tools/`

### Per-Module Coverage Gate Is Not Met

Total coverage is above 85%, but several packages are below the documented
per-module 85% target.

Observed audit values:

- `cmd/mycelium`: 69.6%
- `internal/catalog`: 78.0%
- `internal/catalog/importers`: 83.3%
- `internal/clock`: 75.0%
- `internal/gateway`: 74.1%
- `internal/gateway/profiles`: 68.8%
- `internal/gateway/translate`: 59.5%
- `internal/membership`: 82.5%
- `test/contract`: 70.1%

### Conformance Suites Are Thin

Conformance suites exist and run, but they do not yet satisfy the documented
spirit of broad behavioral parity for every dual implementation.

Evidence:

- `test/contract/backendadapter_conformance.go:15`
- `test/contract/nodeagent_conformance.go:10`
- `test/contract/estimator_conformance.go:10`
- `test/contract/allocator_conformance.go:8`

### Smoke Tests Are Environment-Gated

Smoke tests skip unless real hardware/model environment variables are set.
Manual success was recorded earlier, but fast green does not prove those paths.

Evidence:

- `test/smoke/phase1_local_smoke_test.go:25`
- `test/smoke/fleet_smoke_test.go:15`
- `test/smoke/phase2_gateway_smoke_test.go:26`
- `test/smoke/phase3_catalog_smoke_test.go:18`
- `test/smoke/phase4_join_smoke_test.go:15`
- `BLOCKERS.md:7`

### Phase Smoke Proofs Are Shallow In Places

- Phase 1 local smoke manually records telemetry instead of proving automatic
  runtime emission.
- Phase 1 reactive requeue smoke only calls the planner.
- Phase 1 fleet smoke snapshots/loads/unloads a remote node, not gateway routing
  through normal scheduler placement.
- Phase 2 smoke is local OpenAI-only.
- Phase 3 fast tests do not prove mock load of materialized presets.
- Phase 4 smoke assumes a manually started joined node.

Evidence:

- `test/smoke/phase1_local_smoke_test.go:66`
- `test/smoke/phase1_local_smoke_test.go:86`
- `test/smoke/fleet_smoke_test.go:15`
- `test/smoke/phase2_gateway_smoke_test.go:26`
- `test/smoke/phase3_catalog_smoke_test.go:18`
- `test/smoke/phase4_join_smoke_test.go:15`

## Explicit Post-MVP Roadmap Not Implemented

These are not phase-gate misses; they are documented roadmap work.

- Reverse benchmarking: `internal/bench`, fan-out background jobs, output files
  per model, objective metrics, optional user pick.
- libp2p/cross-NAT overlay discovery and tunnel implementation.
- Continuous auto-calibration of speed class and optimizer cost model.
- Auto-apply-on-by-default after observation.
- Auto engine/parameter selection from telemetry and benchmark outcomes.
- Sticky/KV-cache-affinity routing.
- Cross-machine model sharding, including MLX-distributed across Macs.

Evidence:

- `03-development-guide.md:200`
- `01-project-spec.md:277`
- `01-project-spec.md:515`
