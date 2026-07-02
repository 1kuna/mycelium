# BLOCKERS

This file is public. Keep entries generic, product-facing, and free of private
consumer names, hostnames, private IPs, local user paths, token names, artifact
paths, run IDs, or operational journal narrative. Private operator evidence
belongs in ignored `ops/private/*.local.md` notes or an out-of-repo private
handoff.

## Active

- 2026-06-30: Reachable peers and single-request canaries do not by themselves
  prove live allocator behavior under pressure. The OpenAI-compatible gateway
  currently requires synchronous ownership for chat requests and returns
  `503 Service Unavailable` with `Retry-After` when work would queue. The
  authenticated workload RPC and
  `myce jobs admit/status/drain/finish` now provide explicit queued admission,
  scheduler drain, scheduler release/reclaim, redacted payload/result metadata,
  registry replayability classification, executable-payload contract
  classification, execution-state evidence, terminal evidence, and result
  artifact manifests for repo-local allocator proofs.
  Workload status now says whether a job is queued, running, terminal
  manifest-only, or terminal replayable, whether the stored payload is an
  executable gateway-request envelope, and whether provider output was captured
  automatically. `myce jobs drain --execute` now provides the first automatic
  queued provider-output path for non-streaming registry-replayable executable
  payloads: it runs the placed workload through the gateway/provider seam,
  stores the provider response body, and finishes/releases the job through the
  scheduler. Explicit stored result bodies can still be attached manually at
  workload finish time and retrieved through the separate `jobs result` path
  without printing raw output in status/debug surfaces. The fast suite now
  covers queued admission, multi-owner drain, explicit provider execution,
  terminal success/failure, result artifact manifest metadata, explicit stored
  result-body replay, payload replayability evidence, executable-payload
  contract evidence, execution evidence, and lease reclaim through the public
  workload HTTP surface. The remaining blockers are streaming queued replay and
  a live pressure proof that shows queue/preemption/reclaim under real fleet
  load.
- 2026-06-29: A low-context tool-call route can prove transport and provider
  parser wiring without being a valid route for reasoning-heavy tool-agent
  workloads. Public Mycelium evidence now exposes this as a consumer-contract
  issue: `myce gateway routes --min-context-cap`, `myce gateway preflight
  --context-cap`, and `myce gateway preflight --min-context-cap` classify
  undersized routes as `undersized_context` /
  `consumer_contract_context_too_small`. The remaining product work is to
  expose or provision a genuinely high-context, tool-capable route when
  hardware and provider configuration allow it.
- 2026-06-29: Some deployed gateways may lag the current catalog-discovery and
  route-header code until the operator activates a new binary through the
  documented service path. Use `myce gateway routes --live` to distinguish
  configured routes from the live `/v1/models` catalog. Do not claim live
  catalog support from config alone.
- 2026-06-29: macOS user LaunchAgent activation can be blocked by binary
  identity / Local Network permission behavior. The repo provides generic
  service plan/install/status/rollback surfaces, but a live gateway binary swap
  should be performed only in an operator-approved maintenance window with a
  rollback-ready known-good binary.

## Resolved

- 2026-06-29: The repo coverage gate is green under `make ci`.
- 2026-06-29: Gateway preflight, debug job, debug bundle, owner telemetry, and
  optimization summaries can join secret-safe request evidence to route,
  timing, usage, structured-output, tool-call, and consumer outcome fields.
- 2026-06-27: The repo-local cold-load/client-timeout stale-job lifecycle bug is
  fixed and covered in tests. Live proof still depends on activating a fixed
  gateway binary through the safe service path.
- 2026-06-27: Peer registry snapshots are compacted to rescue-relevant job
  records so historical terminal jobs do not exceed the peer RPC body limit.
- 2026-06-26: Local smoke was re-proven with an already-present local
  llama.cpp-compatible GGUF that has enough context for the overflow requeue
  branch.
