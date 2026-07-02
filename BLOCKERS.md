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
  scheduler. Workload admission now persists non-private payloads as
  registry-replayable records when the local store supports the job registry,
  so the ordinary `jobs admit --payload-kind` path can feed
  `jobs drain --execute`. Explicit stored result bodies can still be attached
  manually at workload finish time and retrieved through the separate
  `jobs result` path without printing raw output in status/debug surfaces. The
  fast suite now covers queued admission, multi-owner drain, explicit provider
  execution, terminal success/failure, result artifact manifest metadata,
  explicit stored result-body replay, payload replayability evidence,
  executable-payload contract evidence, execution evidence, and lease reclaim
  through the public workload HTTP surface. Workload admission can now be
  explicitly queued without immediate placement, and already-running queued
  workloads can be executed through the same provider/result-capture path when
  the periodic drainer has placed them first. Finish/release now handles stale
  coordinator claims and owner leases that were already released, so operator
  cleanup can terminalize restart-straddling workload records without manual
  store edits. `make smoke-workload-executor` proves the local operator path
  with a loopback peer and fake provider. The
  calibration surface now refuses to turn simulation into optimizer priors:
  `calibrate apply-priors` imports only live benchmark metric rows and updates
  speed-class priors through the optimizer calibration path. The remaining
  blockers are streaming queued replay and a live pressure proof that shows
  queue/preemption/reclaim under real fleet load. The project-free live
  benchmark now proves cold placement and warm reuse for one owner through the
  synchronous gateway. The explicit workload lane has also been live-proven for
  queued admission, owner placement, provider execution, result capture/replay,
  terminal finish, and lease release on two owner/backend classes. Full
  pressure remains open: the synchronous gateway intentionally does not trust
  per-request control headers on ordinary deployments, and the workload proof
  has not yet shown concurrent preemption or saturation behavior under a clean
  fleet window. File-backed SQLite control stores now open with explicit busy
  timeout, foreign keys, and WAL mode through driver pragmas. Coordinated
  post-commit failures now record terminal failed registry evidence before
  releasing owner capacity, keeping the registry/rescue view consistent with
  workload status after canceled or failed loads. A provider-specific route can
  still fail during backend load; treat that as route/provider readiness, not
  allocator success. The next gate is a clean-window explicit workload pressure
  proof that captures admission, queue/preemption/drain, provider execution,
  result capture, release/reclaim, and owner telemetry across the selected
  routes.
- 2026-06-29: A low-context tool-call route can prove transport and provider
  parser wiring without being a valid route for reasoning-heavy tool-agent
  workloads. Public Mycelium evidence now exposes this as a consumer-contract
  issue: `myce gateway routes --min-context-cap`, `myce gateway preflight
  --context-cap`, and `myce gateway preflight --min-context-cap` classify
  undersized routes as `undersized_context` /
  `consumer_contract_context_too_small`. The remaining product work is to
  expose or provision a genuinely high-context, tool-capable route when
  hardware and provider configuration allow it.
- 2026-07-02: Full-document OCR readiness still requires a complete-document
  client proof or explicit all-page chunking. A page-limited or single-image
  canary proves OpenAI-compatible vision transport and provider readiness, not
  complete document coverage.
- 2026-07-02: Same-model multi-owner vision/OCR allocation is not yet
  autonomous. Two owners can be independently certified for the same consumer
  model class, but the current gateway/scheduler preset lookup resolves a model
  alias to the first matching preset. That makes a single normal gateway model
  name effectively single-owner until Mycelium grows an explicit multi-owner
  route class or equivalent scheduler contract. Do not present per-owner
  certification as automatic cross-owner distribution.
- 2026-06-29: Some deployed gateways may lag the current catalog-discovery and
  route-header code until the operator activates a new binary through the
  documented service path. Use `myce gateway routes --live` to distinguish
  configured routes from the live `/v1/models` catalog. Do not claim live
  catalog support from config alone.
- 2026-06-29: macOS user LaunchAgent activation can be blocked by binary
  identity / Local Network permission behavior. The repo provides generic
  service plan/install/status/rollback surfaces plus
  `docs/runbooks/service-upgrade.md`, but a live gateway binary swap should be
  performed only in an operator-approved maintenance window with a
  rollback-ready known-good binary.

## Resolved

- 2026-07-02: The large vision/OCR vLLM route reached live route certification
  for provider launch/readiness. The root cause was a private operator preset
  pointing at a model path visible on the host but not inside the configured
  vLLM container. After correcting the private runtime config and restarting
  the established services, an OpenAI-compatible gateway request with image
  content returned HTTP 200, route headers, non-empty final content, and usage.
- 2026-07-02: A second vision/OCR owner reached live provider readiness at a
  smaller context envelope. Current primary runtime docs and live hardware
  checks confirmed the owner has an Intel XPU-capable vLLM path. The public
  model artifact was absent from that owner's configured vLLM cache, so it was
  staged into the existing cache mount. A high-context launch started vLLM but
  did not become ready in an operator-useful window; the smaller envelope
  returned HTTP 200 with image content, owner/backend headers, non-empty final
  content, and usage, then served a warm follow-up request from the same
  instance.
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
